package downloader

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

func TestTrackerLogIDRedactsPathAndQuery(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "http passkey path and query",
			raw:  "https://tracker.example.com/abc123passkey/announce?token=secret",
			want: "https://tracker.example.com",
		},
		{
			name: "host port retained",
			raw:  "http://tracker.example.com:8080/announce?passkey=secret",
			want: "http://tracker.example.com:8080",
		},
		{
			name: "udp tracker",
			raw:  "udp://tracker.example.com:6969/announce",
			want: "udp://tracker.example.com:6969",
		},
		{
			name: "invalid",
			raw:  "://bad tracker",
			want: "invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trackerLogID(tt.raw); got != tt.want {
				t.Fatalf("trackerLogID(%q) = %q; want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestTrackerLogErrRedactsURLErrorURL(t *testing.T) {
	err := trackerLogErr(&url.Error{
		Op:  "Get",
		URL: "https://tracker.example.com/passkey/announce?token=secret",
		Err: errors.New("connection refused"),
	})
	got := err.Error()
	if strings.Contains(got, "passkey") || strings.Contains(got, "token=secret") {
		t.Fatalf("trackerLogErr leaked sensitive URL detail: %q", got)
	}
	if !strings.Contains(got, "connection refused") {
		t.Fatalf("trackerLogErr lost useful error detail: %q", got)
	}
}

// newTrackerTestSession builds an unstarted magnet-style session (nil storage)
// announcing to trackers.
func newTrackerTestSession(t *testing.T, name string, trackers ...string) *Session {
	t.Helper()
	tor := &torrent.Torrent{
		Name:     name,
		InfoHash: sha1.Sum([]byte(name)),
		Trackers: trackers,
	}
	sess, err := NewSession(tor, nil, [20]byte{}, 6881, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(sess.Close)
	return sess
}

// blockingTracker answers announces only once release is closed, tracking
// how many requests are in flight at once.
type blockingTracker struct {
	release  chan struct{}
	inflight atomic.Int32
	peak     atomic.Int32
	total    atomic.Int32
}

func newBlockingTracker() *blockingTracker {
	return &blockingTracker{release: make(chan struct{})}
}

func (b *blockingTracker) handler(w http.ResponseWriter, r *http.Request) {
	b.total.Add(1)
	n := b.inflight.Add(1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	defer b.inflight.Add(-1)
	select {
	case <-b.release:
	case <-r.Context().Done():
		return
	}
	_, _ = w.Write([]byte("d8:intervali1800ee"))
}

// serveTrackers starts n tracker servers sharing h and returns two announce
// URLs per server (maxTrackersPerHost allows two paths per host).
func serveTrackers(t *testing.T, n int, h http.HandlerFunc) []string {
	t.Helper()
	var urls []string
	for i := 0; i < n; i++ {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL+"/announce/a", srv.URL+"/announce/b")
	}
	return urls
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestSanitizeTrackersDedupesAndCaps(t *testing.T) {
	raw := []string{
		"http://tracker.example/announce",
		"HTTP://Tracker.Example:80/announce", // same tracker, other spelling
		" http://tracker.example/announce ",
		"http://tracker.example/other/announce",
		"http://tracker.example/third/announce",   // over the per-host cap
		"http://TRACKER.example./fourth/announce", // same host, root-dot spelling
		"udp://tracker.example:6969/announce",
		"https://tracker.example/announce",
		"wss://tracker.example/announce",
		"httpx://tracker.example/announce",
		"udpfoo://tracker.example:1/announce",
		"http:///announce",
		"",
	}
	got := sanitizeTrackers(raw)
	var urls []string
	for _, tg := range got {
		urls = append(urls, tg.url)
	}
	want := []string{
		"http://tracker.example/announce",
		"http://tracker.example/other/announce",
		"udp://tracker.example:6969/announce",
		"https://tracker.example/announce",
	}
	if strings.Join(urls, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sanitizeTrackers =\n%v\nwant\n%v", urls, want)
	}
	if !got[2].udp || got[0].udp {
		t.Fatal("udp flag not derived from the parsed scheme")
	}

	var many []string
	for i := 0; i < 5000; i++ {
		many = append(many, fmt.Sprintf("udp://t%d.example:6969/announce", i))
	}
	if n := len(sanitizeTrackers(many)); n != maxSessionTrackers {
		t.Fatalf("kept %d trackers, want cap %d", n, maxSessionTrackers)
	}
}

// TestAnnounceDedupesRepeatedTracker reproduces a torrent whose announce-list
// repeats one URL: every copy used to be announced concurrently.
func TestAnnounceDedupesRepeatedTracker(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("d8:intervali1800ee"))
	}))
	defer srv.Close()
	trackers := make([]string, 1500)
	for i := range trackers {
		trackers[i] = srv.URL + "/announce"
	}
	sess := newTrackerTestSession(t, "dedupe", trackers...)
	sess.announceAndConnect()
	if got := hits.Load(); got != 1 {
		t.Fatalf("1500 copies of one tracker produced %d announces, want 1", got)
	}
}

func TestAnnounceFanOutIsBoundedPerSession(t *testing.T) {
	t.Cleanup(swapDuration(&trackerAnnounceTimeout, 10*time.Second))
	bt := newBlockingTracker()
	urls := serveTrackers(t, 20, bt.handler) // 40 trackers
	sess := newTrackerTestSession(t, "bounded", urls...)

	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.announceAndConnect()
	}()
	waitFor(t, "workers to fill", 5*time.Second, func() bool { return bt.inflight.Load() >= trackerAnnounceWorkers })
	time.Sleep(100 * time.Millisecond)
	if peak := bt.peak.Load(); peak > trackerAnnounceWorkers {
		t.Fatalf("%d concurrent announces from one session, want at most %d", peak, trackerAnnounceWorkers)
	}
	close(bt.release)
	<-done
	if got := bt.total.Load(); got != int32(len(urls)) {
		t.Fatalf("announced to %d trackers, want all %d", got, len(urls))
	}
}

func TestAnnounceFanOutIsBoundedAcrossSessions(t *testing.T) {
	t.Cleanup(swapDuration(&trackerAnnounceTimeout, 10*time.Second))
	bt := newBlockingTracker()
	var urls []string
	for i := 0; i < trackerAnnounceWorkers; i++ {
		srv := httptest.NewServer(http.HandlerFunc(bt.handler))
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL+"/announce")
	}
	// Enough sessions that their pools together exceed the process-wide cap,
	// which must win.
	sessions := maxConcurrentTrackerRequests/trackerAnnounceWorkers + 2
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		sess := newTrackerTestSession(t, fmt.Sprintf("global-%d", i), urls...)
		wg.Go(sess.announceAndConnect)
	}
	waitFor(t, "requests to approach the global cap", 5*time.Second, func() bool {
		return bt.inflight.Load() >= maxConcurrentTrackerRequests-10
	})
	time.Sleep(100 * time.Millisecond)
	peak := bt.peak.Load()
	close(bt.release)
	wg.Wait()
	if peak > maxConcurrentTrackerRequests {
		t.Fatalf("%d tracker requests in flight across sessions, want at most %d", peak, maxConcurrentTrackerRequests)
	}
	if got := bt.total.Load(); got != int32(sessions*len(urls)) {
		t.Fatalf("announced %d times, want %d", got, sessions*len(urls))
	}
}

// TestSlowTrackersDoNotStallOtherSessions reproduces a restart with several
// torrents whose lists hold dead trackers (each holds a request slot until it
// times out). Every session's live tracker must still be asked at once rather
// than queue behind other sessions' timeouts: the old loop dialed a torrent's
// first peers within one announce timeout, so a smaller process-wide budget
// would slow startup.
func TestSlowTrackersDoNotStallOtherSessions(t *testing.T) {
	t.Cleanup(swapDuration(&trackerAnnounceTimeout, 5*time.Second))
	slow := newBlockingTracker()
	release := sync.OnceFunc(func() { close(slow.release) })
	t.Cleanup(release)
	dead := serveTrackers(t, 6, slow.handler) // 12 per session, within its pool
	var liveHits atomic.Int32
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits.Add(1)
		_, _ = w.Write([]byte("d8:intervali1800ee"))
	}))
	defer live.Close()

	const sessions = 10
	var wg sync.WaitGroup
	for i := 0; i < sessions; i++ {
		trackers := append(append([]string(nil), dead...), live.URL+"/announce")
		sess := newTrackerTestSession(t, fmt.Sprintf("restart-%d", i), trackers...)
		wg.Go(sess.announceAndConnect)
	}
	waitFor(t, "every session's live tracker", 2*time.Second, func() bool {
		return liveHits.Load() == sessions
	})
	release()
	wg.Wait()
}

// TestAnnounceDialsPeersBeforeSlowTrackerAnswers checks the first peers are
// dialed as soon as any tracker answers; the old loop waited for the slowest
// tracker (up to its full timeout) before dialing anyone.
func TestAnnounceDialsPeersBeforeSlowTrackerAnswers(t *testing.T) {
	t.Cleanup(swapDuration(&trackerAnnounceTimeout, 10*time.Second))
	peerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peerLn.Close()
	dialed := make(chan struct{}, 1)
	go func() {
		c, err := peerLn.Accept()
		if err != nil {
			return
		}
		c.Close()
		dialed <- struct{}{}
	}()
	peerPort := uint16(peerLn.Addr().(*net.TCPAddr).Port)

	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compact := []byte{127, 0, 0, 1, byte(peerPort >> 8), byte(peerPort)}
		_, _ = fmt.Fprintf(w, "d8:intervali1800e5:peers%d:%se", len(compact), compact)
	}))
	defer fast.Close()
	slow := newBlockingTracker()
	slowSrv := httptest.NewServer(http.HandlerFunc(slow.handler))
	defer slowSrv.Close()

	sess := newTrackerTestSession(t, "streaming", slowSrv.URL+"/announce", fast.URL+"/announce")
	done := make(chan struct{})
	go func() {
		defer close(done)
		sess.announceAndConnect()
	}()
	select {
	case <-dialed:
	case <-time.After(3 * time.Second):
		t.Fatal("peer from the fast tracker was not dialed while the slow tracker was pending")
	}
	close(slow.release)
	<-done
}

func TestTrackerFailureBackoff(t *testing.T) {
	t.Cleanup(swapDuration(&trackerRetryBaseDelay, 200*time.Millisecond))
	var deadHits, liveHits atomic.Int32
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadHits.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer dead.Close()
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveHits.Add(1)
		_, _ = w.Write([]byte("d8:intervali1800ee"))
	}))
	defer live.Close()

	sess := newTrackerTestSession(t, "backoff", dead.URL+"/announce", live.URL+"/announce")
	sched := &trackerSchedule{}
	sess.announceDue(sched)
	sess.announceDue(sched) // immediately again: nothing is due
	if deadHits.Load() != 1 || liveHits.Load() != 1 {
		t.Fatalf("hits dead=%d live=%d after two rounds, want 1/1", deadHits.Load(), liveHits.Load())
	}
	if err := sess.LastError(); err != nil {
		t.Fatalf("a healthy tracker should clear the tracker error, got %v", err)
	}

	time.Sleep(250 * time.Millisecond) // first retry after 200ms
	sess.announceDue(sched)
	sess.announceDue(sched)
	if deadHits.Load() != 2 || liveHits.Load() != 1 {
		t.Fatalf("hits dead=%d live=%d, want 2/1", deadHits.Load(), liveHits.Load())
	}
	if st := sched.states[0]; st.fails != 2 || time.Until(st.next) > 400*time.Millisecond {
		t.Fatalf("dead tracker fails=%d next in %v, want 2 and <= 400ms", st.fails, time.Until(st.next))
	}
	for fails, want := range map[int]time.Duration{
		1:  trackerRetryBaseDelay,
		2:  2 * trackerRetryBaseDelay,
		3:  4 * trackerRetryBaseDelay,
		40: trackerRetryMaxDelay,
	} {
		if got := trackerRetryDelay(fails); got != want {
			t.Fatalf("trackerRetryDelay(%d) = %v, want %v", fails, got, want)
		}
	}
}

// TestTrackerIntervalIsPerTracker reproduces one tracker answering interval=1
// to drive re-announces to every other tracker every 5s.
func TestTrackerIntervalIsPerTracker(t *testing.T) {
	sched := &trackerSchedule{}
	sched.build([]string{"http://evil.example/announce", "http://victim.example/announce"})
	now := time.Now()
	sched.record(0, trackerAnnounceResult{interval: 1}, now)
	sched.record(1, trackerAnnounceResult{interval: 1800, minInterval: 900}, now)

	check := func(at time.Duration, needPeers bool, want ...int) {
		t.Helper()
		got := sched.due(now.Add(at), false, needPeers)
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("due(+%v, needPeers=%v) = %v, want %v", at, needPeers, got, want)
		}
	}
	check(0, false)             // nothing is due right after answering
	check(5*time.Second, false) // interval=1 is floored...
	check(5*time.Second, true)  // ...even when short of peers
	check(trackerMinInterval+time.Second, false, 0)
	check(10*time.Minute, true, 0)       // victim's min interval (900s) beats early re-announce
	check(901*time.Second, true, 0, 1)   // short of peers: early, but not before min interval
	check(901*time.Second, false, 0)     // enough peers: victim waits its full interval
	check(1801*time.Second, false, 0, 1) // regular interval
	if got := sched.due(now, true, false); fmt.Sprint(got) != "[0 1]" {
		t.Fatalf("pending event: due = %v, want both healthy trackers", got)
	}

	// Oversized intervals clamp instead of overflowing into a negative Duration.
	sched.record(0, trackerAnnounceResult{interval: math.MaxInt32, minInterval: math.MaxInt32}, now)
	if got := sched.states[0].next.Sub(now); got != trackerMaxInterval {
		t.Fatalf("huge interval scheduled in %v, want %v", got, trackerMaxInterval)
	}
	sched.record(0, trackerAnnounceResult{}, now)
	if got := sched.states[0].next.Sub(now); got != trackerDefaultInterval {
		t.Fatalf("missing interval scheduled in %v, want %v", got, trackerDefaultInterval)
	}
	if d := sched.sleep(now, false, false, false); d <= 0 || d > trackerIdlePoll {
		t.Fatalf("sleep = %v, want within (0, %v]", d, trackerIdlePoll)
	}
}

func TestAnnounceRequiresHTTP200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("d8:intervali7ee"))
	}))
	defer srv.Close()
	sess := newTrackerTestSession(t, "status", srv.URL+"/announce")
	sess.announceAndConnect()
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.lastTrackerErr == nil || !strings.Contains(sess.lastTrackerErr.Error(), "HTTP 500") {
		t.Fatalf("lastTrackerErr = %v, want HTTP 500 failure", sess.lastTrackerErr)
	}
	if len(sess.trackerEvents) != 1 || sess.trackerEvents[0] != "started" {
		t.Fatalf("events = %v, want started kept for retry", sess.trackerEvents)
	}
}

func TestAnnounceCapsResponseBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("d8:intervali60e5:peers" + strconv.Itoa(300*1024) + ":"))
		_, _ = w.Write(make([]byte, 300*1024))
		_, _ = w.Write([]byte("e"))
	}))
	defer srv.Close()
	sess := newTrackerTestSession(t, "bodycap", srv.URL+"/announce")
	sess.announceAndConnect()
	if err := sess.LastError(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("LastError = %v, want body cap failure", err)
	}
}

func TestAnnounceDispatchesOnParsedScheme(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte("d8:intervali1800ee"))
	}))
	defer srv.Close()
	hostPort := strings.TrimPrefix(srv.URL, "http://")

	sess := newTrackerTestSession(t, "upper", "HTTP://"+hostPort+"/announce")
	sess.announceAndConnect()
	if hits.Load() != 1 || sess.LastError() != nil {
		t.Fatalf("uppercase scheme: hits=%d err=%v", hits.Load(), sess.LastError())
	}

	sess = newTrackerTestSession(t, "httpx", "httpx://"+hostPort+"/announce")
	sess.announceAndConnect()
	if hits.Load() != 1 {
		t.Fatal("httpx:// tracker was sent down the HTTP path")
	}
}

// TestAnnounceRefusesLocalServiceTracker reproduces a torrent whose tracker
// is a loopback admin endpoint, directly or through a tracker's redirect. An
// "/announce" suffix must not help: a prefix-routed admin handler still
// serves "/admin/reboot/announce".
func TestAnnounceRefusesLocalServiceTracker(t *testing.T) {
	var internalHits atomic.Int32
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		internalHits.Add(1)
	}))
	defer internal.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, internal.URL+"/admin/reboot/announce?confirm=yes", http.StatusFound)
	}))
	defer redirector.Close()

	sess := newTrackerTestSession(t, "ssrf",
		internal.URL+"/admin/reboot?confirm=yes&token=abc",
		internal.URL+"/admin/reboot/announce?confirm=yes&token=abc",
		redirector.URL+"/announce",
	)
	sess.announceAndConnect()
	if got := internalHits.Load(); got != 0 {
		t.Fatalf("local admin endpoint received %d requests", got)
	}
	if sess.LastError() == nil {
		t.Fatal("refused trackers should surface a tracker error")
	}
}

func TestAnnounceKeysIPv6PeersWithBrackets(t *testing.T) {
	// [::1]:1 from a loopback tracker: allowed by netpolicy, and the dial
	// fails fast (refused, or no IPv6) without leaving the machine.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer := string(net.IPv6loopback) + "\x00\x01"
		_, _ = fmt.Fprintf(w, "d8:intervali1800e6:peers6%d:%se", len(peer), peer)
	}))
	defer srv.Close()
	sess := newTrackerTestSession(t, "ipv6", srv.URL+"/announce")
	sess.announceAndConnect()

	const key = "[::1]:1"
	sess.mu.RLock()
	_, ok := sess.Peers[key]
	sess.mu.RUnlock()
	if !ok {
		t.Fatalf("IPv6 tracker peer not recorded under %q (err=%v)", key, sess.LastError())
	}
	// connectToPeer must use the same key, or the dial never clears Dialing.
	waitFor(t, "IPv6 dial to finish", 10*time.Second, func() bool {
		sess.mu.RLock()
		defer sess.mu.RUnlock()
		return !sess.Peers[key].Dialing
	})
}

func TestFreshTrackerPeersAppliesNetpolicy(t *testing.T) {
	peers := []tracker.Peer{
		{IP: net.ParseIP("8.8.8.8"), Port: 6881},
		{IP: net.ParseIP("8.8.8.8").To4(), Port: 6881}, // duplicate in another form
		{IP: net.ParseIP("127.0.0.1"), Port: 22},
		{IP: net.ParseIP("192.168.1.1"), Port: 80},
		{IP: net.ParseIP("169.254.169.254"), Port: 80},
		{IP: net.ParseIP("1.2.3.4"), Port: 0},
		{IP: net.IPv4zero, Port: 6881},
		{IP: net.ParseIP("2001:db8::1"), Port: 6881},
	}
	got := freshTrackerPeers(peers, netip.Addr{}, map[netip.AddrPort]struct{}{})
	if fmt.Sprint(got) != "[8.8.8.8:6881 [2001:db8::1]:6881]" {
		t.Fatalf("public tracker peers = %v", got)
	}
	got = freshTrackerPeers(peers, netip.MustParseAddr("127.0.0.1"), map[netip.AddrPort]struct{}{})
	if fmt.Sprint(got) != "[8.8.8.8:6881 127.0.0.1:22 192.168.1.1:80 169.254.169.254:80 [2001:db8::1]:6881]" {
		t.Fatalf("loopback tracker peers = %v", got)
	}
}

// TestAnnounceWithEventReachesTrackersInParallel checks the shutdown "stopped"
// announce reaches every tracker within its budget; it used to walk the list
// one tracker at a time and ran out of budget after a few slow ones.
func TestAnnounceWithEventReachesTrackersInParallel(t *testing.T) {
	var stopped atomic.Int32
	urls := serveTrackers(t, 10, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		if r.URL.Query().Get("event") == "stopped" {
			stopped.Add(1)
		}
		_, _ = w.Write([]byte("d8:intervali1800ee"))
	})
	sess := newTrackerTestSession(t, "stopped", urls...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if !sess.announceWithEvent(ctx, "stopped") {
		t.Fatal("stopped announce reported failure")
	}
	if got := stopped.Load(); got != int32(len(urls)) {
		t.Fatalf("stopped reached %d of %d trackers", got, len(urls))
	}
}
