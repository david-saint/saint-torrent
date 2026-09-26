package tracker

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDestinationAllowed(t *testing.T) {
	public := netip.Addr{}
	loop := netip.MustParseAddr("127.0.0.1")
	lan := netip.MustParseAddr("192.168.1.5")
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
		// Link-local (cloud metadata) and non-unicast are refused whatever
		// the source claims.
		{"169.254.169.254", public, false},
		{"169.254.169.254", loop, false},
		{"fe80::1", loop, false},
		{"0.0.0.0", loop, false},
		{"224.0.0.1", loop, false},
		{"255.255.255.255", lan, false},
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
