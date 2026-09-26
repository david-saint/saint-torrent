package downloader

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

// multiPieceWebseedTorrent is a single-file torrent of pieces pieces of
// pieceLen bytes served by the given webseeds.
func multiPieceWebseedTorrent(name string, pieces, pieceLen int, webseeds []string) (*torrent.Torrent, []byte) {
	data := make([]byte, pieces*pieceLen)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	hashes := make([][20]byte, pieces)
	for i := range hashes {
		hashes[i] = sha1.Sum(data[i*pieceLen : (i+1)*pieceLen])
	}
	return &torrent.Torrent{
		Name:        name,
		InfoHash:    sha1.Sum([]byte(name)),
		WebSeeds:    webseeds,
		PieceLength: int64(pieceLen),
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{name}}},
	}, data
}

// pathCounter counts requests per URL path.
type pathCounter struct {
	mu     sync.Mutex
	counts map[string]int
	total  int
}

func (c *pathCounter) add(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = make(map[string]int)
	}
	c.counts[path]++
	c.total++
}

func (c *pathCounter) snapshot() (map[string]int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.counts))
	for k, v := range c.counts {
		out[k] = v
	}
	return out, c.total
}

func TestWebseedSourcesDedupeAndWorkersBounded(t *testing.T) {
	seeds := []string{
		"http://seed.example/f.bin?v=1",
		"http://seed.example/f.bin?v=2", // same source, other query
		"HTTP://SEED.example:80/f.bin",  // same source, other spelling
		"http://seed.example/f.bin#frag",
		"http://alice:pw@seed.example/g.bin", // credentials: rejected
		"ftp://seed.example/f.bin",
	}
	for i := 0; i < 10; i++ {
		seeds = append(seeds, fmt.Sprintf("http://m%d.example/f.bin", i))
	}
	tor, _ := multiPieceWebseedTorrent("dedupe.bin", 2, 16, seeds)
	sess := newWebseedTestSession(t, tor)
	workers := sess.webseedSpecsForStart()
	if len(workers) != maxWebseedWorkers {
		t.Fatalf("%d webseed workers, want %d", len(workers), maxWebseedWorkers)
	}
	pool := workers[0].pool
	for _, w := range workers {
		if w.pool != pool {
			t.Fatal("webseed workers do not share one pool")
		}
	}
	if len(pool.sources) != 11 {
		var got []string
		for _, s := range pool.sources {
			got = append(got, s.display)
		}
		t.Fatalf("pool has %d sources, want 11: %v", len(pool.sources), got)
	}
	if got := pool.sources[0].display; got != "http://seed.example/f.bin" {
		t.Fatalf("display = %q, want no query or fragment", got)
	}
}

// TestWebseedWorkersBoundedAndRotateThroughList reproduces a url-list with
// many entries: each one used to get its own goroutine, connection and
// full-piece buffer at once.
func TestWebseedWorkersBoundedAndRotateThroughList(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	release := make(chan struct{})
	var inflight, peak atomic.Int32
	var paths pathCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.add(r.URL.Path)
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	var seeds []string
	for i := 0; i < 10; i++ {
		seeds = append(seeds, fmt.Sprintf("%s/m%d/f.bin", srv.URL, i))
	}
	tor, _ := multiPieceWebseedTorrent("rotate.bin", 8, 16, seeds)
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitFor(t, "webseed workers to start", 3*time.Second, func() bool { return inflight.Load() >= maxWebseedWorkers })
	time.Sleep(100 * time.Millisecond)
	if got := peak.Load(); got > maxWebseedWorkers {
		t.Fatalf("%d concurrent webseed requests, want at most %d", got, maxWebseedWorkers)
	}
	close(release)

	// Every source answers 404 and is retired; workers rotate to the next
	// until the whole list has been tried exactly once.
	waitFor(t, "rotation through every webseed", 3*time.Second, func() bool {
		_, total := paths.snapshot()
		return total >= len(seeds)
	})
	time.Sleep(100 * time.Millisecond)
	counts, total := paths.snapshot()
	if total != len(seeds) || len(counts) != len(seeds) {
		t.Fatalf("requests = %v (total %d), want each of %d webseeds once", counts, total, len(seeds))
	}
}

func TestWebseedRetiresOnPermanentFailures(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryBaseDelay, 5*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryMaxDelay, 10*time.Millisecond))
	statuses := map[string]int{
		"/s404": http.StatusNotFound,
		"/s410": http.StatusGone,
		"/s401": http.StatusUnauthorized,
		"/s403": http.StatusForbidden,
		"/s416": http.StatusRequestedRangeNotSatisfiable,
		"/s200": http.StatusOK, // Range ignored
	}
	var paths pathCounter
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.add(r.URL.Path)
		w.WriteHeader(statuses[r.URL.Path])
		_, _ = w.Write([]byte("not a range"))
	}))
	defer srv.Close()

	var seeds []string
	for p := range statuses {
		seeds = append(seeds, srv.URL+p)
	}
	tor, _ := multiPieceWebseedTorrent("permanent.bin", 2, 16, seeds)
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitFor(t, "every webseed to be tried", 3*time.Second, func() bool {
		_, total := paths.snapshot()
		return total >= len(seeds)
	})
	time.Sleep(200 * time.Millisecond) // many retry periods
	counts, total := paths.snapshot()
	if total != len(seeds) {
		t.Fatalf("requests = %v, want each permanently failing webseed exactly once", counts)
	}
	if err := sess.LastError(); err == nil || !strings.Contains(err.Error(), "returned") {
		t.Fatalf("LastError = %v", err)
	}
}

func TestWebseedRetiresAfterRepeatedHashFailures(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryBaseDelay, 5*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryMaxDelay, 10*time.Millisecond))
	good := []byte("verified webseed payload")
	bad := bytes.Repeat([]byte("x"), len(good))
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		serveRange(t, w, r, bad)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "corrupt.bin",
		InfoHash:    sha1.Sum([]byte("corrupt-forever")),
		WebSeeds:    []string{srv.URL + "/corrupt.bin"},
		PieceLength: int64(len(good)),
		PieceHashes: [][20]byte{sha1.Sum(good)},
		Files:       []torrent.File{{Length: int64(len(good)), Path: []string{"corrupt.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitFor(t, "hash failures", 3*time.Second, func() bool { return requests.Load() >= webseedMaxHashFailures })
	time.Sleep(200 * time.Millisecond)
	if got := requests.Load(); got != webseedMaxHashFailures {
		t.Fatalf("corrupt webseed fetched %d times, want retirement after %d", got, webseedMaxHashFailures)
	}
	if states := sess.GetPieceStates(); states[0] != PieceEmpty {
		t.Fatalf("piece state = %v, want empty for other sources", states[0])
	}
}

func TestWebseedHonoursRetryAfter(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryBaseDelay, 10*time.Millisecond))
	data := []byte("payload after a busy spell")
	var mu sync.Mutex
	var times []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		times = append(times, time.Now())
		first := len(times) == 1
		mu.Unlock()
		if first {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		serveRange(t, w, r, data)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "busy.bin",
		InfoHash:    sha1.Sum([]byte("busy-webseed")),
		WebSeeds:    []string{srv.URL + "/busy.bin"},
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"busy.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)
	waitForWebseedState(t, sess, 0, PieceCompleted)

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 2 {
		t.Fatalf("%d requests, want 2", len(times))
	}
	if gap := times[1].Sub(times[0]); gap < 900*time.Millisecond {
		t.Fatalf("retried after %v, want Retry-After's 1s", gap)
	}
}

func TestWebseedRetryAfterAndBackoffPolicy(t *testing.T) {
	now := time.Now()
	for v, want := range map[string]time.Duration{
		"":                     0,
		"garbage":              0,
		"-5":                   0,
		"120":                  2 * time.Minute,
		"600":                  webseedMaxRetryAfter, // capped
		"99999999999999999999": webseedMaxRetryAfter,
		now.Add(90 * time.Second).UTC().Format(http.TimeFormat): 90 * time.Second,
	} {
		got := parseRetryAfter(v, now)
		if d := got - want; d < -time.Second || d > time.Second {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", v, got, want)
		}
	}

	pool := &webseedPool{}
	src := &webseedSource{}
	transient := &webseedStatusError{code: http.StatusBadGateway}
	if got := pool.restAfterError(src, transient); got != webseedRetryBaseDelay {
		t.Fatalf("first transient rest = %v", got)
	}
	if got := pool.restAfterError(src, transient); got != 2*webseedRetryBaseDelay {
		t.Fatalf("second transient rest = %v, want doubled", got)
	}
	if got := pool.restAfterError(src, &webseedStatusError{code: 503, retryAfter: 2 * time.Minute}); got != 2*time.Minute {
		t.Fatalf("Retry-After rest = %v", got)
	}
	pool.succeeded(src)
	if got := pool.restAfterError(src, transient); got != webseedRetryBaseDelay {
		t.Fatalf("rest after success = %v, want backoff reset", got)
	}
	if got := pool.restAfterError(src, &webseedStatusError{code: 404, permanent: true}); got != webseedRetireDelay {
		t.Fatalf("permanent rest = %v, want retirement", got)
	}
}

// TestWebseedRejectsCredentialsAndRedactsErrors reproduces credentials and
// signed-URL tokens in a url-list leaking into LastError (shown by the TUI and
// the stats API) and being sent as Basic auth.
func TestWebseedRejectsCredentialsAndRedactsErrors(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()
	hostPort := strings.TrimPrefix(srv.URL, "http://")

	tor, _ := multiPieceWebseedTorrent("creds.bin", 1, 16, []string{"http://alice:S3cretPassw0rd@" + hostPort + "/f.bin"})
	if specs := newWebseedTestSession(t, tor).webseedSpecsForStart(); len(specs) != 0 {
		t.Fatal("webseed with credentials was accepted")
	}

	tor, _ = multiPieceWebseedTorrent("token.bin", 1, 16, []string{srv.URL + "/f.bin?token=SIGNEDTOKEN123"})
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)
	waitFor(t, "webseed error", 3*time.Second, func() bool { return sess.LastError() != nil })
	if got := sess.LastError().Error(); strings.Contains(got, "SIGNEDTOKEN123") || !strings.Contains(got, "/f.bin") {
		t.Fatalf("LastError = %q, want the redacted URL", got)
	}
	if requests.Load() != 0 {
		t.Fatal("local webseed with a query string was contacted")
	}

	if got := redactedURL(&url.URL{Scheme: "https", User: url.UserPassword("a", "b"), Host: "cdn.example", Path: "/f.bin", RawQuery: "sig=x", Fragment: "y"}); got != "https://cdn.example/f.bin" {
		t.Fatalf("redactedURL = %q", got)
	}
}

func TestWebseedRedirectToLocalServiceRefused(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryBaseDelay, 5*time.Millisecond))
	t.Cleanup(swapDuration(&webseedRetryMaxDelay, 10*time.Millisecond))
	var internalHits, mirrorHits atomic.Int32
	internal := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		internalHits.Add(1)
	}))
	defer internal.Close()
	target := strings.Replace(internal.URL, "http://", "http://admin:admin@", 1) + "/apply.cgi?dns=6.6.6.6"
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mirrorHits.Add(1)
		http.Redirect(w, r, target, http.StatusFound)
	}))
	defer mirror.Close()

	tor, _ := multiPieceWebseedTorrent("redirect.bin", 1, 16, []string{mirror.URL + "/f.bin"})
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)
	waitFor(t, "webseed error", 3*time.Second, func() bool { return sess.LastError() != nil })
	time.Sleep(100 * time.Millisecond) // many retry periods
	if internalHits.Load() != 0 {
		t.Fatal("webseed redirect reached a credentialed local target")
	}
	if got := mirrorHits.Load(); got != 1 {
		t.Fatalf("mirror redirecting to a refused target was asked %d times, want 1 (retired)", got)
	}
	if got := sess.LastError().Error(); strings.Contains(got, "admin") || strings.Contains(got, "dns=") {
		t.Fatalf("LastError leaks the redirect target: %q", got)
	}
}

func TestWebseedFetchUsesPooledPieceBuffer(t *testing.T) {
	tor, data := multiPieceWebseedTorrent("pooled.bin", 2, 32, nil)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(t, w, r, data)
	}))
	defer srv.Close()
	tor.WebSeeds = []string{srv.URL + "/pooled.bin"}
	sess := newWebseedTestSession(t, tor)
	pool := sess.webseedSpecsForStart()[0].pool

	piece, ok, _ := sess.claimWebseedPiece()
	if !ok {
		t.Fatal("no piece to claim")
	}
	got, buf, err := sess.fetchWebseedPiece(context.Background(), tracker.HTTPClient, pool, pool.sources[0], piece, &PeerState{})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if buf == nil || &(*buf)[0] != &got[0] {
		t.Fatal("piece data is not backed by a pooled piece buffer")
	}
	if !bytes.Equal(got, data[piece.absoluteStart:piece.absoluteStart+piece.length]) {
		t.Fatal("piece data mismatch")
	}
}

func TestWebseedAcceptsWholeFileOK(t *testing.T) {
	t.Cleanup(swapDuration(&webseedIdleDelay, 10*time.Millisecond))
	data := []byte("small file inside one piece")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(data) // ignores Range
	}))
	defer srv.Close()
	tor := &torrent.Torrent{
		Name:        "whole.bin",
		InfoHash:    sha1.Sum([]byte("whole-file-200")),
		WebSeeds:    []string{srv.URL + "/whole.bin"},
		PieceLength: 1 << 14,
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"whole.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)
	waitForWebseedState(t, sess, 0, PieceCompleted)
}
