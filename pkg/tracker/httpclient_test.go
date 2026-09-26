package tracker

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDestinationAllowed(t *testing.T) {
	public := netip.Addr{}
	loop := netip.MustParseAddr("127.0.0.1")
	lan := netip.MustParseAddr("192.168.1.5")
	cgnat := netip.MustParseAddr("100.64.1.2")
	cases := []struct {
		dst    string
		source netip.Addr
		want   bool
	}{
		{"8.8.8.8", public, true},
		{"2001:4860::8888", public, true},
		{"127.0.0.1", public, false},
		{"::1", public, false},
		{"10.0.0.1", public, false},
		{"fd00::1", public, false},
		{"127.0.0.1", loop, true},
		{"10.0.0.1", loop, true},
		{"10.0.0.1", lan, true},
		{"127.0.0.1", lan, false}, // a LAN URL cannot reach loopback
		// Carrier-grade NAT space (ISP services, Tailscale's 100.100.100.100
		// and tailnet nodes) is private, as in libtorrent's is_local.
		{"100.100.100.100", public, false},
		{"100.64.1.2", lan, true},
		{"100.64.1.2", loop, true},
		{"100.64.1.3", cgnat, true},
		{"10.0.0.1", cgnat, true},
		{"127.0.0.1", cgnat, false},
		{"fec0::1", public, false},
		// Link-local (cloud metadata) and non-unicast are refused whatever
		// the source claims.
		{"169.254.169.254", public, false},
		{"169.254.169.254", loop, false},
		{"fe80::1", loop, false},
		{"0.0.0.0", loop, false},
		{"224.0.0.1", loop, false},
		{"255.255.255.255", lan, false},
		// NAT64 and the other IPv6 forms of IPv4 addresses are judged by the
		// IPv4 address: a translator forwards them there.
		{"64:ff9b::a9fe:a9fe", public, false},
		{"64:ff9b::a9fe:a9fe", loop, false},
		{"64:ff9b::a00:5", public, false},
		{"64:ff9b::a00:5", lan, true},
		{"64:ff9b:1::a00:5", public, false},
		{"2002:a00:5::1", public, false},
		{"64:ff9b::808:808", public, true},
	}
	for _, c := range cases {
		if got := DestinationAllowed(netip.MustParseAddr(c.dst), c.source); got != c.want {
			t.Errorf("DestinationAllowed(%s, %v) = %v, want %v", c.dst, c.source, got, c.want)
		}
	}
}

func TestNewRequestAppliesDestinationPolicy(t *testing.T) {
	cases := []struct {
		name    string
		purpose Purpose
		url     string
		refused bool
	}{
		{"public tracker", PurposeAnnounce, "http://tracker.example/x/y?passkey=1", false},
		{"loopback announce", PurposeAnnounce, "http://127.0.0.1:6969/announce", false},
		{"localhost announce", PurposeAnnounce, "http://localhost:6969/announce", false},
		{"loopback announce.php", PurposeAnnounce, "http://127.0.0.1:6969/announce.php?passkey=1", false},
		{"loopback admin path", PurposeAnnounce, "http://127.0.0.1:8080/admin/reboot?confirm=yes", true},
		{"ipv6 loopback admin path", PurposeAnnounce, "http://[::1]:8080/cgi-bin/x", true},
		// A suffix rule let these reach the admin handler (libtorrent checks
		// the prefix); dot segments are normalised away by the server.
		{"loopback admin path ending in announce", PurposeAnnounce, "http://127.0.0.1:8080/admin/reboot/announce?confirm=yes", true},
		{"loopback PATH_INFO announce", PurposeAnnounce, "http://localhost:8080/admin.php/announce", true},
		{"loopback dot segments", PurposeAnnounce, "http://127.0.0.1:8080/announce/../admin/reboot", true},
		{"loopback encoded dot segments", PurposeAnnounce, "http://127.0.0.1:8080/announce/%2e%2e/admin", true},
		{"loopback dot-semicolon segment", PurposeAnnounce, "http://127.0.0.1:8080/announce/..;/admin", true},
		{"loopback backslash", PurposeAnnounce, "http://127.0.0.1:8080/announce%5c..%5cadmin", true},
		{"loopback scrape", PurposeScrape, "http://127.0.0.1:6969/scrape", false},
		{"loopback scrape bad path", PurposeScrape, "http://127.0.0.1:6969/announce", true},
		{"loopback scrape suffix only", PurposeScrape, "http://127.0.0.1:6969/admin/scrape", true},
		{"cloud metadata", PurposeAnnounce, "http://169.254.169.254/latest/meta-data/announce", true},
		{"ipv6 link-local", PurposeWebseed, "http://[fe80::1]/f.bin", true},
		{"unspecified", PurposeAnnounce, "http://0.0.0.0/announce", true},
		{"multicast", PurposeWebseed, "http://224.0.0.1/f.bin", true},
		{"userinfo", PurposeAnnounce, "http://user:pw@tracker.example/announce", true},
		{"lan tracker", PurposeAnnounce, "http://192.168.1.1/announce.php", false},
		{"loopback webseed", PurposeWebseed, "http://127.0.0.1:8000/f.bin", false},
		{"lan webseed with query", PurposeWebseed, "http://192.168.1.1/apply.cgi?dns=6.6.6.6", true},
		{"cgnat tracker", PurposeAnnounce, "http://100.64.1.2/announce.php", false},
		{"cgnat webseed with query", PurposeWebseed, "http://100.100.100.100/apply.cgi?dns=6.6.6.6", true},
		{"public webseed with query", PurposeWebseed, "https://cdn.example/f.bin?sig=abc", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewRequest(context.Background(), c.purpose, c.url)
			if refused := errors.Is(err, ErrDestinationRefused); refused != c.refused {
				t.Fatalf("NewRequest(%q) err = %v, refused = %v, want %v", c.url, err, refused, c.refused)
			}
		})
	}

	if _, err := NewRequest(context.Background(), PurposeAnnounce, "ftp://tracker.example/announce"); err == nil {
		t.Fatal("NewRequest accepted an ftp URL")
	}
}

// countingServer counts every request it receives.
func countingServer(t *testing.T, h http.HandlerFunc) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if h != nil {
			h(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func redirectTo(target string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target, http.StatusFound)
	}
}

func TestHTTPClientRefusesRedirectToLocalService(t *testing.T) {
	internal, internalHits := countingServer(t, nil)

	for _, target := range []string{
		// A loopback tracker redirecting to a non-announce path on another
		// local service (verifier repro: GET /admin/reboot?confirm=yes).
		internal.URL + "/admin/reboot?confirm=yes&token=abc",
		// The same with an "/announce" suffix, which prefix routing on the
		// local service still sends to the admin handler.
		internal.URL + "/admin/reboot/announce?confirm=yes&token=abc",
		// Credentials in a redirect would become a Basic auth header.
		strings.Replace(internal.URL, "http://", "http://admin:admin@", 1) + "/announce",
		// Link-local is never reachable.
		"http://169.254.169.254/latest/meta-data/announce",
	} {
		tracker, _ := countingServer(t, redirectTo(target))
		req, err := NewRequest(context.Background(), PurposeAnnounce, tracker.URL+"/announce")
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		resp, err := HTTPClient.Do(req)
		if err == nil {
			resp.Body.Close()
			t.Fatalf("redirect to %q was followed", target)
		}
		if !errors.Is(err, ErrDestinationRefused) {
			t.Fatalf("redirect to %q: err = %v, want ErrDestinationRefused", target, err)
		}
	}
	if got := internalHits.Load(); got != 0 {
		t.Fatalf("internal service received %d requests", got)
	}
}

func TestHTTPClientRequestWithoutPolicyIsStrict(t *testing.T) {
	srv, hits := countingServer(t, nil)
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/announce", nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp, err := HTTPClient.Do(req); err == nil {
		resp.Body.Close()
		t.Fatal("request without a policy reached a loopback server")
	}
	if hits.Load() != 0 {
		t.Fatal("loopback server was contacted")
	}
}

// TestDialRefusesLocalAddressForPublicOrigin covers DNS rebinding and
// redirects from a public tracker: the address actually dialed is checked, so
// a name that resolves to loopback is refused unless the request itself
// started from a literal local URL.
func TestDialRefusesLocalAddressForPublicOrigin(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	publicCtx := context.WithValue(context.Background(), policyKey{}, &requestPolicy{purpose: PurposeAnnounce})
	for _, addr := range []string{"localhost:" + port, "127.0.0.1:" + port} {
		if c, err := dialWithPolicy(publicCtx, "tcp", addr); err == nil {
			c.Close()
			t.Fatalf("public-origin dial to %s succeeded", addr)
		} else if !errors.Is(err, ErrDestinationRefused) {
			t.Fatalf("public-origin dial to %s: err = %v, want ErrDestinationRefused", addr, err)
		}
	}

	localCtx := context.WithValue(context.Background(), policyKey{}, &requestPolicy{
		purpose: PurposeAnnounce, origin: netip.MustParseAddr("127.0.0.1"),
	})
	c, err := dialWithPolicy(localCtx, "tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("loopback-origin dial to localhost: %v", err)
	}
	c.Close()

	// A hostname hop never inherits a local origin: control with a public
	// source refuses loopback whatever the name was.
	control := DestinationControl(netip.Addr{})
	if err := control(context.Background(), "tcp", "127.0.0.1:80", nil); !errors.Is(err, ErrDestinationRefused) {
		t.Fatalf("control(127.0.0.1) = %v, want refusal", err)
	}
	if err := control(context.Background(), "tcp", "[::ffff:10.0.0.1]:80", nil); !errors.Is(err, ErrDestinationRefused) {
		t.Fatalf("control(mapped 10.0.0.1) = %v, want refusal", err)
	}
	if err := control(context.Background(), "tcp", "93.184.216.34:80", nil); err != nil {
		t.Fatalf("control(public) = %v", err)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) && accepted.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	if got := accepted.Load(); got != 1 {
		t.Fatalf("listener accepted %d connections, want only the loopback-origin one", got)
	}
}

// fakeResolver answers every A query with a and every other query with no
// records, over in-memory connections, so a hostname can be made to resolve
// anywhere without touching the network.
func fakeResolver(a [4]byte) *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go serveFakeDNS(server, a)
			return client, nil
		},
	}
}

// serveFakeDNS answers one DNS query on c. The Go resolver frames messages
// with a two-byte length (as over TCP) on a conn that is not a PacketConn.
func serveFakeDNS(c net.Conn, a [4]byte) {
	defer c.Close()
	var n [2]byte
	if _, err := io.ReadFull(c, n[:]); err != nil {
		return
	}
	q := make([]byte, binary.BigEndian.Uint16(n[:]))
	if _, err := io.ReadFull(c, q); err != nil || len(q) < 12 {
		return
	}
	// Echo the header and question, dropping the query's EDNS record.
	end := 12
	for end < len(q) && q[end] != 0 {
		end += int(q[end]) + 1
	}
	end += 5 // root label, QTYPE, QCLASS
	if end > len(q) {
		return
	}
	resp := append([]byte(nil), q[:end]...)
	// Response, recursion desired and available, NOERROR; no answer,
	// authority or additional records unless the query is for an A record.
	resp[2], resp[3] = 0x81, 0x80
	clear(resp[6:12])
	if qtype := binary.BigEndian.Uint16(q[end-4:]); qtype == 1 {
		resp[7] = 1
		// Name (pointer to the question), A, IN, TTL 60, RDLENGTH 4, address.
		resp = append(resp, 0xc0, 12, 0, 1, 0, 1, 0, 0, 0, 60, 0, 4)
		resp = append(resp, a[:]...)
	}
	_, _ = c.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(resp))), resp...))
}

// TestHostnameResolvingToCGNATIsRefused: a webseed hostname, or any hostname
// on a redirect hop, must resolve to a public address. Carrier-grade NAT space
// (100.64.0.0/10) holds ISP-internal services and Tailscale's 100.100.100.100
// resolver and tailnet nodes, so a name resolving there is refused like one
// resolving to a LAN address, on the address actually dialed. (A tracker's own
// hostname on the first hop may resolve there; see
// TestTrackerHostnameReachesLocalTracker.)
func TestHostnameResolvingToCGNATIsRefused(t *testing.T) {
	saved := baseDialer
	baseDialer.Resolver = fakeResolver([4]byte{100, 100, 100, 100})
	// Bound to loopback, a dial the policy wrongly allowed fails locally
	// instead of leaving the machine.
	baseDialer.LocalAddr = &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	t.Cleanup(func() { baseDialer = saved })
	addrs, err := baseDialer.Resolver.LookupNetIP(context.Background(), "ip4", "cgnat-tracker.test")
	if err != nil || len(addrs) != 1 || addrs[0] != netip.MustParseAddr("100.100.100.100") {
		t.Skipf("the in-memory resolver is not used on this platform: %v, %v", addrs, err)
	}

	for _, c := range []struct {
		purpose Purpose
		url     string
	}{
		{PurposeAnnounce, "http://cgnat-tracker.test/announce"},
		{PurposeWebseed, "http://cgnat-seed.test/files/f.bin"},
	} {
		req, err := NewRequest(context.Background(), c.purpose, c.url)
		if err != nil {
			t.Fatalf("NewRequest(%q) = %v; a hostname is judged when dialed", c.url, err)
		}
		ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
		conn, err := dialWithPolicy(ctx, "tcp", net.JoinHostPort(req.URL.Hostname(), "80"))
		cancel()
		if err == nil {
			conn.Close()
			t.Fatalf("dial for %q connected to 100.100.100.100", c.url)
		}
		if !errors.Is(err, ErrDestinationRefused) || !strings.Contains(err.Error(), "100.100.100.100") {
			t.Fatalf("dial for %q: err = %v, want the resolved address 100.100.100.100 refused", c.url, err)
		}
	}
}

// TestTrackerHostnameReach: as in libtorrent, a tracker named by a hostname
// may resolve to a private address, where LAN, company and tailnet trackers
// live, and to loopback when its path is a tracker endpoint. Webseed
// hostnames, and literal hosts (judged by checkURL), get public only.
func TestTrackerHostnameReach(t *testing.T) {
	for _, c := range []struct {
		purpose Purpose
		url     string
		want    hostReach
	}{
		{PurposeAnnounce, "http://tracker.lan:6969/announce", reachLoopback},
		{PurposeAnnounce, "http://tracker.lan:6969/announce.php?passkey=1", reachLoopback},
		{PurposeAnnounce, "http://tracker.lan/tr/announce", reachPrivate},
		{PurposeAnnounce, "http://tracker.lan/announce/../admin", reachPrivate},
		{PurposeScrape, "http://tracker.lan/scrape", reachLoopback},
		{PurposeScrape, "http://tracker.lan/announce", reachPrivate},
		{PurposeWebseed, "http://mirror.lan/announce", reachPublic},
		{PurposeAnnounce, "http://192.168.1.10/announce", reachPublic},
		{PurposeAnnounce, "http://localhost:6969/announce", reachPublic},
	} {
		u, err := url.Parse(c.url)
		if err != nil {
			t.Fatal(err)
		}
		if got := trackerHostnameReach(c.purpose, u); got != c.want {
			t.Errorf("trackerHostnameReach(%d, %q) = %d, want %d", c.purpose, c.url, got, c.want)
		}
	}

	for _, c := range []struct {
		reach hostReach
		dst   string
		want  bool
	}{
		{reachPublic, "93.184.216.34", true},
		{reachPublic, "192.168.1.10", false},
		{reachPublic, "127.0.0.1", false},
		{reachPrivate, "192.168.1.10", true},
		{reachPrivate, "10.0.0.5", true},
		{reachPrivate, "100.100.100.100", true},
		{reachPrivate, "fd00::1", true},
		{reachPrivate, "127.0.0.1", false},
		{reachPrivate, "169.254.169.254", false},
		{reachLoopback, "127.0.0.1", true},
		{reachLoopback, "::1", true},
		{reachLoopback, "10.0.0.5", true},
		{reachLoopback, "169.254.169.254", false},
		{reachLoopback, "fe80::1", false},
		{reachLoopback, "0.0.0.0", false},
		{reachLoopback, "224.0.0.1", false},
	} {
		if got := c.reach.allows(netip.MustParseAddr(c.dst)); got != c.want {
			t.Errorf("reach %d allows(%s) = %v, want %v", c.reach, c.dst, got, c.want)
		}
	}
}

// TestTrackerHostnameReachesLocalTracker: a tracker whose hostname resolves
// to a local address (here loopback, as a name in /etc/hosts would) is
// announced to and scraped on its tracker endpoints. A non-endpoint path on
// it, a webseed on the same host, and a redirect from it to another path are
// refused, none of them reusing the tracker's pooled connection.
func TestTrackerHostnameReachesLocalTracker(t *testing.T) {
	saved := baseDialer
	baseDialer.Resolver = fakeResolver([4]byte{127, 0, 0, 1})
	t.Cleanup(func() { baseDialer = saved })
	addrs, err := baseDialer.Resolver.LookupNetIP(context.Background(), "ip4", "lan-tracker.test")
	if err != nil || len(addrs) != 1 || addrs[0] != netip.MustParseAddr("127.0.0.1") {
		t.Skipf("the in-memory resolver is not used on this platform: %v, %v", addrs, err)
	}

	var mu sync.Mutex
	var paths []string
	var redirectTarget string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/announce-redirect" {
			http.Redirect(w, r, redirectTarget, http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "d8:intervali60ee")
	}))
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	base := "http://lan-tracker.test:" + port
	redirectTarget = base + "/admin/reboot"

	// A client of its own, without a proxy from the environment, so the
	// hostname is dialed directly.
	client := newHTTPClient()
	pt := client.Transport.(*policyTransport)
	for _, tr := range []*http.Transport{pt.base, pt.private, pt.loopback} {
		tr.Proxy = nil
	}
	t.Cleanup(client.CloseIdleConnections)
	get := func(purpose Purpose, rawURL string) error {
		req, err := NewRequest(context.Background(), purpose, rawURL)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil
	}

	for _, c := range []struct {
		purpose Purpose
		path    string
	}{
		{PurposeAnnounce, "/announce?info_hash=x"},
		{PurposeScrape, "/scrape?info_hash=x"},
		{PurposeAnnounce, "/announce?info_hash=y"}, // on the pooled connection
	} {
		if err := get(c.purpose, base+c.path); err != nil {
			t.Fatalf("GET %s: %v, want the local tracker reached", c.path, err)
		}
	}
	for _, c := range []struct {
		purpose Purpose
		path    string
	}{
		{PurposeAnnounce, "/admin/reboot"},
		{PurposeScrape, "/admin/scrape"},
		{PurposeWebseed, "/f.bin"},
		{PurposeAnnounce, "/announce-redirect"}, // the redirect hop is refused
	} {
		if err := get(c.purpose, base+c.path); !errors.Is(err, ErrDestinationRefused) {
			t.Fatalf("GET %s (purpose %d): err = %v, want ErrDestinationRefused", c.path, c.purpose, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"/announce", "/scrape", "/announce", "/announce-redirect"}
	if strings.Join(paths, " ") != strings.Join(want, " ") {
		t.Fatalf("tracker saw %v, want %v", paths, want)
	}
}

// TestUDPTrackerHostnameReachesLAN: a UDP tracker named by a hostname may
// resolve to a private address. Its path is not sent, so it cannot vouch for
// loopback, which takes a literal address.
func TestUDPTrackerHostnameReachesLAN(t *testing.T) {
	for _, c := range []struct {
		host, dst string
		want      bool
	}{
		{"tracker.lan", "192.168.1.10:6969", true},
		{"tracker.lan", "100.64.1.2:6969", true},
		{"tracker.lan", "93.184.216.34:6969", true},
		{"tracker.lan", "127.0.0.1:6969", false},
		{"tracker.lan", "169.254.169.254:6969", false},
		{"127.0.0.1", "127.0.0.1:6969", true},
		{"localhost", "127.0.0.1:6969", true},
		{"192.168.1.10", "192.168.1.10:6969", true},
		{"192.168.1.10", "127.0.0.1:6969", false},
	} {
		err := udpDialControl(c.host)(context.Background(), "udp", c.dst, nil)
		if allowed := err == nil; allowed != c.want {
			t.Errorf("UDP tracker %s dialing %s: err = %v, want allowed %v", c.host, c.dst, err, c.want)
		}
	}
}

// TestCGNATLiteralTreatedLikeLANLiteral: a URL naming a CGNAT address
// literally follows exactly the rules for an RFC 1918 literal: allowed as the
// request's own origin, able to reach local (not loopback) addresses, and
// refused as a hop of a request that started from a public URL.
func TestCGNATLiteralTreatedLikeLANLiteral(t *testing.T) {
	const lan, cgnat = "192.168.1.2", "100.64.1.2"
	for _, purpose := range []Purpose{PurposeAnnounce, PurposeScrape, PurposeWebseed} {
		for _, rest := range []string{"/announce", "/announce.php?passkey=1", "/scrape", ":8080/admin/reboot?confirm=yes", "/f.bin", "/apply.cgi?dns=6.6.6.6"} {
			_, lanErr := NewRequest(context.Background(), purpose, "http://"+lan+rest)
			_, cgnatErr := NewRequest(context.Background(), purpose, "http://"+cgnat+rest)
			if (lanErr == nil) != (cgnatErr == nil) || errors.Is(lanErr, ErrDestinationRefused) != errors.Is(cgnatErr, ErrDestinationRefused) {
				t.Errorf("purpose %d, %q: LAN literal err = %v, CGNAT literal err = %v, want the same outcome", purpose, rest, lanErr, cgnatErr)
			}
		}
	}

	public := &requestPolicy{purpose: PurposeAnnounce}
	for _, host := range []string{lan, cgnat} {
		origin := localAddr(host)
		if !origin.IsValid() {
			t.Fatalf("localAddr(%s) = zero, want the literal as a local origin", host)
		}
		control := DestinationControl(hopSource(host, &requestPolicy{purpose: PurposeAnnounce, origin: origin}))
		for _, dst := range []string{"100.64.9.9:80", "192.168.9.9:80", "203.0.113.5:80"} {
			if err := control(context.Background(), "tcp", dst, nil); err != nil {
				t.Errorf("request from %s: dial %s = %v, want allowed", host, dst, err)
			}
		}
		for _, dst := range []string{"127.0.0.1:80", "169.254.169.254:80"} {
			if err := control(context.Background(), "tcp", dst, nil); !errors.Is(err, ErrDestinationRefused) {
				t.Errorf("request from %s: dial %s = %v, want refused", host, dst, err)
			}
		}
		// A public tracker redirecting to the literal cannot reach it.
		u, err := url.Parse("http://" + host + "/announce")
		if err != nil {
			t.Fatal(err)
		}
		if err := public.checkURL(u); !errors.Is(err, ErrDestinationRefused) {
			t.Errorf("hop to %s from a public origin = %v, want refused", host, err)
		}
	}
}

func TestCheckRedirectPolicy(t *testing.T) {
	mk := func(raw string) *http.Request {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Request{URL: u}
	}
	via := []*http.Request{mk("https://tracker.example/announce")}
	if err := checkRedirect(mk("http://tracker.example/announce"), via); !errors.Is(err, ErrDestinationRefused) {
		t.Fatalf("https->http redirect: err = %v", err)
	}
	if err := checkRedirect(mk("https://other.example/announce"), via); err != nil {
		t.Fatalf("https->https redirect refused: %v", err)
	}
	long := []*http.Request{mk("http://a/"), mk("http://b/"), mk("http://c/"), mk("http://d/")}
	if err := checkRedirect(mk("http://e/"), long); err == nil {
		t.Fatal("fourth redirect was allowed")
	}
	if err := checkRedirect(mk("http://d/"), long[:3]); err != nil {
		t.Fatalf("third redirect refused: %v", err)
	}
}

func TestHTTPClientCapsResponseHeaders(t *testing.T) {
	srv, _ := countingServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", "/"+strings.Repeat("a/", 40<<10))
		w.WriteHeader(http.StatusFound)
	})
	req, err := NewRequest(context.Background(), PurposeAnnounce, srv.URL+"/announce")
	if err != nil {
		t.Fatal(err)
	}
	resp, err := HTTPClient.Do(req)
	if err == nil {
		resp.Body.Close()
		t.Fatal("80 KiB response header was accepted")
	}
}

func TestProxyAddrsFromEnvironment(t *testing.T) {
	env := map[string]string{
		"HTTP_PROXY":  "127.0.0.1:3128",
		"https_proxy": "https://[::1]",
		"HTTPS_PROXY": "socks5://proxy.corp",
	}
	got := proxyAddrsFrom(func(k string) string { return env[k] })
	for _, want := range []string{"127.0.0.1:3128", "[::1]:443", "proxy.corp:1080"} {
		if !got[want] {
			t.Errorf("proxy address %s missing from %v", want, got)
		}
	}
	if len(got) != 3 {
		t.Errorf("proxy addresses = %v, want 3 entries", got)
	}
}
