package tracker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"sainttorrent/pkg/netpolicy"
)

// Tracker and webseed URLs come from untrusted torrents and magnet links, so
// every request they cause goes through HTTPClient, which refuses to aim the
// client at loopback, LAN or link-local services (SSRF). The rules follow
// libtorrent's ssrf_mitigation:
//
//   - A destination may be at most as local as the URL that led to it. A
//     loopback or private address is reachable only when the request's original
//     URL named a literal local IP or "localhost", and only through a hop whose
//     own host is such a literal: a hostname must resolve to a public address.
//     The check runs on the address actually dialed, so DNS rebinding and
//     redirects cannot get around it.
//   - Link-local (including 169.254.169.254 cloud metadata), unspecified,
//     multicast and broadcast destinations are always refused.
//   - A loopback tracker's path must end in /announce (or /scrape for its
//     derived scrape URL), and a local webseed may not carry a query string.
//   - At most maxHTTPRedirects redirects, never from https to http, and never
//     to a URL with userinfo (which Go would turn into Basic auth).

const (
	// maxHTTPRedirects bounds the redirect chain of one request.
	maxHTTPRedirects = 3
	// maxResponseHeaderBytes caps response headers. Tracker and webseed replies
	// carry a few hundred header bytes; the cap bounds a hostile Location or
	// header flood long before net/http's 10 MiB default.
	maxResponseHeaderBytes = 64 << 10
	// httpMaxIdleConnsPerHost lets a torrent's parallel webseed workers keep
	// their connections to one mirror alive (DefaultTransport keeps only 2).
	httpMaxIdleConnsPerHost = 8
)

// Purpose says what an outbound HTTP request is for. It selects the extra
// rules applied when the request targets the local machine or network.
type Purpose uint8

const (
	// PurposeAnnounce is a tracker announce; a loopback tracker's path must end
	// in /announce.
	PurposeAnnounce Purpose = iota
	// PurposeScrape is a tracker scrape; a loopback tracker's path must end in
	// /scrape.
	PurposeScrape
	// PurposeWebseed is a BEP 19 webseed range request; a local webseed URL may
	// not carry a query string.
	PurposeWebseed
)

// ErrDestinationRefused marks a request refused by the SSRF policy.
var ErrDestinationRefused = errors.New("destination refused")

// requestPolicy travels in the request context so the transport and dialer
// can apply it to every redirect hop.
type requestPolicy struct {
	purpose Purpose
	// origin is the original URL's own address when its host is a literal
	// loopback or private IP or "localhost"; the zero Addr (public) otherwise.
	origin netip.Addr
}

type policyKey struct{}

// strictPolicy applies to requests built without NewRequest: public origin, so
// no local destination is reachable.
var strictPolicy = &requestPolicy{purpose: PurposeWebseed}

// HTTPClient is the hardened client shared by tracker announces, stopped
// announces, scrapes and webseed requests. Build its requests with NewRequest
// so each carries its destination policy; a request without one is held to the
// strictest policy. Request lifetime is bounded by the request context.
var HTTPClient = newHTTPClient()

func newHTTPClient() *http.Client {
	var t *http.Transport
	if dt, ok := http.DefaultTransport.(*http.Transport); ok {
		t = dt.Clone()
	} else {
		t = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		}
	}
	t.DialContext = dialWithPolicy
	t.MaxResponseHeaderBytes = maxResponseHeaderBytes
	t.MaxIdleConnsPerHost = httpMaxIdleConnsPerHost
	return &http.Client{
		Transport:     &policyTransport{base: t},
		CheckRedirect: checkRedirect,
	}
}

// NewRequest builds a GET for rawURL whose destination policy is derived from
// rawURL itself. It fails early when rawURL already breaks the policy.
func NewRequest(ctx context.Context, purpose Purpose, rawURL string) (*http.Request, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	p := &requestPolicy{purpose: purpose, origin: localAddr(u.Hostname())}
	if err := p.checkURL(u); err != nil {
		return nil, err
	}
	return http.NewRequestWithContext(context.WithValue(ctx, policyKey{}, p), http.MethodGet, rawURL, nil)
}

func policyFrom(ctx context.Context) *requestPolicy {
	if p, ok := ctx.Value(policyKey{}).(*requestPolicy); ok {
		return p
	}
	return strictPolicy
}

// literalAddr returns the address a URL host names without DNS: an IP literal,
// or loopback for "localhost".
func literalAddr(host string) (netip.Addr, bool) {
	if strings.EqualFold(strings.TrimSuffix(host, "."), "localhost") {
		return netip.AddrFrom4([4]byte{127, 0, 0, 1}), true
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return a.Unmap(), true
}

// localAddr returns the loopback or private address host names literally, or
// the zero Addr when host is a hostname or any other kind of address.
// Link-local is deliberately excluded: it never vouches for anything.
func localAddr(host string) netip.Addr {
	if a, ok := literalAddr(host); ok {
		switch netpolicy.Classify(a) {
		case netpolicy.ScopeLoopback, netpolicy.ScopePrivate:
			return a
		}
	}
	return netip.Addr{}
}

// hopSource returns the address a destination reached through host is judged
// against: the request's local origin when host is itself a literal local
// address, otherwise the zero Addr (public). A hostname can therefore only
// ever reach public addresses, whatever it resolves to.
func hopSource(host string, p *requestPolicy) netip.Addr {
	if localAddr(host).IsValid() {
		return p.origin
	}
	return netip.Addr{}
}

// DestinationAllowed reports whether a connection to dst is allowed for a URL
// supplied by source (the zero Addr for a public or unknown source). Public
// destinations are always allowed; loopback only from a loopback source;
// private only from a loopback or private source; link-local, unspecified,
// multicast and broadcast never.
func DestinationAllowed(dst, source netip.Addr) bool {
	src := netpolicy.Classify(source)
	switch netpolicy.Classify(dst) {
	case netpolicy.ScopeGlobal:
		return true
	case netpolicy.ScopeLoopback:
		return src == netpolicy.ScopeLoopback
	case netpolicy.ScopePrivate:
		return src == netpolicy.ScopeLoopback || src == netpolicy.ScopePrivate
	}
	return false
}

// checkURL applies the URL-level rules to one hop. Hosts that are literal
// addresses are judged here, before a pooled connection could be reused;
// hostnames are judged at dial time on the resolved address.
func (p *requestPolicy) checkURL(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("%w: URL carries credentials", ErrDestinationRefused)
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("URL has no host")
	}
	a, ok := literalAddr(host)
	if !ok {
		return nil
	}
	if !DestinationAllowed(a, hopSource(host, p)) {
		return fmt.Errorf("%w: %s is not reachable from this URL", ErrDestinationRefused, a)
	}
	switch scope := netpolicy.Classify(a); {
	case scope == netpolicy.ScopeLoopback && p.purpose == PurposeAnnounce && !strings.HasSuffix(u.Path, "/announce"):
		return fmt.Errorf("%w: loopback tracker path must end in /announce", ErrDestinationRefused)
	case scope == netpolicy.ScopeLoopback && p.purpose == PurposeScrape && !strings.HasSuffix(u.Path, "/scrape"):
		return fmt.Errorf("%w: loopback tracker path must end in /scrape", ErrDestinationRefused)
	case scope != netpolicy.ScopeGlobal && p.purpose == PurposeWebseed && u.RawQuery != "":
		return fmt.Errorf("%w: local webseed URL may not carry a query", ErrDestinationRefused)
	}
	return nil
}

// policyTransport checks every hop, including each redirect, before handing
// it to the real transport.
type policyTransport struct {
	base *http.Transport
}

func (t *policyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := policyFrom(req.Context()).checkURL(req.URL); err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}
	return t.base.RoundTrip(req)
}

// CloseIdleConnections lets http.Client.CloseIdleConnections reach the pool.
func (t *policyTransport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
}

func checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) > maxHTTPRedirects {
		return fmt.Errorf("stopped after %d redirects", maxHTTPRedirects)
	}
	if via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return fmt.Errorf("%w: redirect from https to %s", ErrDestinationRefused, req.URL.Scheme)
	}
	// The destination itself is checked by policyTransport and the dialer.
	return nil
}

var baseDialer = net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}

// dialWithPolicy is the transport's dialer. addr is the host:port of the hop
// being dialed (or of the configured proxy) before DNS resolution; the Control
// hook then checks each resolved address actually connected to.
func dialWithPolicy(ctx context.Context, network, addr string) (net.Conn, error) {
	d := baseDialer
	// A user-configured proxy is trusted even on loopback (privoxy, corporate
	// agents); the URL-level rules still apply to every proxied hop.
	if !proxyDialAddrs()[addr] {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		d.ControlContext = DestinationControl(hopSource(host, policyFrom(ctx)))
	}
	return d.DialContext(ctx, network, addr)
}

// DestinationControl returns a net.Dialer ControlContext hook that refuses any
// address DestinationAllowed rejects for source. Pass the zero Addr for a
// public or unknown source.
func DestinationControl(source netip.Addr) func(context.Context, string, string, syscall.RawConn) error {
	return func(_ context.Context, _, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return fmt.Errorf("%w: unparsable address %q", ErrDestinationRefused, address)
		}
		if dst := ap.Addr().Unmap(); !DestinationAllowed(dst, source) {
			return fmt.Errorf("%w: %s", ErrDestinationRefused, dst)
		}
		return nil
	}
}

// proxyDialAddrs lists the host:port values the transport dials when it uses
// a proxy from the environment (net/http reads the same variables once).
var proxyDialAddrs = sync.OnceValue(func() map[string]bool {
	return proxyAddrsFrom(os.Getenv)
})

func proxyAddrsFrom(getenv func(string) string) map[string]bool {
	addrs := make(map[string]bool)
	for _, key := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"} {
		raw := getenv(key)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			// net/http retries bare "host:port" values with an http:// prefix.
			if u, err = url.Parse("http://" + raw); err != nil || u.Host == "" {
				continue
			}
		}
		port := u.Port()
		if port == "" {
			switch u.Scheme {
			case "https":
				port = "443"
			case "socks5", "socks5h":
				port = "1080"
			default:
				port = "80"
			}
		}
		addrs[net.JoinHostPort(u.Hostname(), port)] = true
	}
	return addrs
}
