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
//     loopback or private (RFC 1918, carrier-grade NAT, unique-local) address
//     is reachable when the request's original URL named a literal local IP
//     or "localhost", through a hop whose own host is such a literal. A
//     tracker named by a hostname may resolve to a private address too, and
//     to loopback under the path rule below, as in libtorrent: LAN, company
//     and tailnet trackers are usually reached by name. Any other hostname,
//     a webseed's or a redirect's, must resolve to a public address.
//     The check runs on the address actually dialed, so DNS rebinding and
//     redirects cannot get around it.
//   - Link-local (including 169.254.169.254 cloud metadata), unspecified,
//     multicast and broadcast destinations are always refused.
//   - A loopback tracker's path must start with /announce (or /scrape for its
//     derived scrape URL) and hold no dot segments, and a local webseed may
//     not carry a query string.
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
	// PurposeAnnounce is a tracker announce; a loopback tracker's path must
	// start with /announce.
	PurposeAnnounce Purpose = iota
	// PurposeScrape is a tracker scrape; a loopback tracker's path must start
	// with /scrape.
	PurposeScrape
	// PurposeWebseed is a BEP 19 webseed range request; a local webseed URL may
	// not carry a query string.
	PurposeWebseed
)

// MaxWebseedURLLength bounds a webseed request URL, the url-list entry with
// the file's escaped path appended. Longer URLs are refused before anything
// is sent. It is twice torrent.MaxWebSeedURLLength, the bound on the entry
// itself, so even the longest entry leaves 4 KiB for the path, whose
// non-ASCII bytes triple when escaped; nginx and Apache accept request lines
// of about this size by default.
const MaxWebseedURLLength = 8192

// ErrDestinationRefused marks a request refused by the SSRF policy.
var ErrDestinationRefused = errors.New("destination refused")

// ErrURLTooLong marks a tracker or webseed URL refused for its length.
var ErrURLTooLong = errors.New("URL too long")

// requestPolicy travels in the request context so the transport and dialer
// can apply it to every redirect hop.
type requestPolicy struct {
	purpose Purpose
	// origin is the original URL's own address when its host is a literal
	// loopback or private IP or "localhost"; the zero Addr (public) otherwise.
	origin netip.Addr
	// hostnameReach is what the original URL's host may resolve to on the
	// request's first hop when it is a hostname (see trackerHostnameReach).
	// Redirect hops never get it.
	hostnameReach hostReach
}

// hostReach is what a hostname may resolve to.
type hostReach uint8

const (
	// reachPublic allows public addresses only.
	reachPublic hostReach = iota
	// reachPrivate adds private addresses (RFC 1918, carrier-grade NAT,
	// unique-local).
	reachPrivate
	// reachLoopback adds private and loopback addresses.
	reachLoopback
)

// allows reports whether a hostname with this reach may resolve to dst.
// Link-local, unspecified, multicast and broadcast never qualify.
func (r hostReach) allows(dst netip.Addr) bool {
	switch netpolicy.Classify(dst) {
	case netpolicy.ScopeGlobal:
		return true
	case netpolicy.ScopePrivate:
		return r >= reachPrivate
	case netpolicy.ScopeLoopback:
		return r >= reachLoopback
	}
	return false
}

// trackerHostnameReach is what the host of a tracker URL u for purpose may
// resolve to on the request's first hop, as libtorrent allows: a private
// address, where LAN, company and tailnet trackers live, and loopback only
// when u's path is a tracker endpoint (see trackerEndpointPath). Webseed
// hostnames, and hosts that are literal addresses (judged by checkURL), get
// reachPublic.
func trackerHostnameReach(purpose Purpose, u *url.URL) hostReach {
	if _, literal := literalAddr(u.Hostname()); literal {
		return reachPublic
	}
	switch purpose {
	case PurposeAnnounce:
		if trackerEndpointPath(u.Path, "/announce") {
			return reachLoopback
		}
		return reachPrivate
	case PurposeScrape:
		if trackerEndpointPath(u.Path, "/scrape") {
			return reachLoopback
		}
		return reachPrivate
	}
	return reachPublic
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
		Transport:     newPolicyTransport(t),
		CheckRedirect: checkRedirect,
	}
}

// NewRequest builds a GET for rawURL whose destination policy is derived from
// rawURL itself. It fails early when rawURL already breaks the policy, or is
// a webseed URL longer than MaxWebseedURLLength.
func NewRequest(ctx context.Context, purpose Purpose, rawURL string) (*http.Request, error) {
	if purpose == PurposeWebseed && len(rawURL) > MaxWebseedURLLength {
		return nil, fmt.Errorf("%w: webseed URL is %d bytes, more than the maximum of %d", ErrURLTooLong, len(rawURL), MaxWebseedURLLength)
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	p := &requestPolicy{purpose: purpose, origin: localAddr(u.Hostname()), hostnameReach: trackerHostnameReach(purpose, u)}
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
// address, otherwise the zero Addr (public). A hostname hop can therefore only
// reach public addresses, whatever it resolves to; the one exception, a
// tracker's own hostname on the first hop, is dialed by its own transport
// (see policyTransport).
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
	case scope == netpolicy.ScopeLoopback && p.purpose == PurposeAnnounce && !trackerEndpointPath(u.Path, "/announce"):
		return fmt.Errorf("%w: loopback tracker path must start with /announce", ErrDestinationRefused)
	case scope == netpolicy.ScopeLoopback && p.purpose == PurposeScrape && !trackerEndpointPath(u.Path, "/scrape"):
		return fmt.Errorf("%w: loopback tracker path must start with /scrape", ErrDestinationRefused)
	case scope != netpolicy.ScopeGlobal && p.purpose == PurposeWebseed && u.RawQuery != "":
		return fmt.Errorf("%w: local webseed URL may not carry a query", ErrDestinationRefused)
	}
	return nil
}

// trackerEndpointPath reports whether a loopback tracker's (decoded) path is
// a conventional tracker endpoint. Like libtorrent's ssrf_mitigation it must
// start with prefix, so the request can only reach the tracker handler: with
// a suffix rule, "/admin/reboot/announce" or "/admin.php/announce" still land
// in whatever serves the prefix. Dot segments (including Tomcat's "..;") and
// backslashes are refused because servers normalise them into another path
// ("/announce/../admin" is "/admin").
func trackerEndpointPath(path, prefix string) bool {
	if !strings.HasPrefix(path, prefix) || strings.ContainsRune(path, '\\') {
		return false
	}
	for _, seg := range strings.Split(path, "/") {
		seg, _, _ = strings.Cut(seg, ";")
		if seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// policyTransport checks every hop, including each redirect, before handing
// it to the real transport. The first hop of a tracker request naming a
// hostname goes through a transport of its own for that hostname's reach
// (private, or private and loopback), whose dialer allows those addresses:
// separate connection pools, so a connection to a local tracker is never
// reused by a request that could not have dialed it itself (a redirect, a
// webseed, or a tracker path that fails the loopback path rule).
type policyTransport struct {
	base     *http.Transport // every other hop: dialWithPolicy
	private  *http.Transport // tracker hostnames with reachPrivate
	loopback *http.Transport // tracker hostnames with reachLoopback
}

func newPolicyTransport(base *http.Transport) *policyTransport {
	t := &policyTransport{base: base, private: base.Clone(), loopback: base.Clone()}
	t.private.DialContext = hostnameDialer(reachPrivate)
	t.loopback.DialContext = hostnameDialer(reachLoopback)
	return t
}

func (t *policyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	p := policyFrom(req.Context())
	if err := p.checkURL(req.URL); err != nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}
	// Response is set on the requests a client creates to follow redirects,
	// and only on those: nil means the request's own URL.
	if req.Response == nil {
		switch p.hostnameReach {
		case reachPrivate:
			return t.private.RoundTrip(req)
		case reachLoopback:
			return t.loopback.RoundTrip(req)
		}
	}
	return t.base.RoundTrip(req)
}

// CloseIdleConnections lets http.Client.CloseIdleConnections reach the pools.
func (t *policyTransport) CloseIdleConnections() {
	t.base.CloseIdleConnections()
	t.private.CloseIdleConnections()
	t.loopback.CloseIdleConnections()
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

// hostnameDialer is the dialer of the transport for a tracker hostname's
// first hop: it applies reach to each resolved address actually connected to.
// Only policyTransport sends requests through it, for hops whose host is a
// hostname.
func hostnameDialer(reach hostReach) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := baseDialer
		if !proxyDialAddrs()[addr] {
			d.ControlContext = controlAllowing(reach.allows)
		}
		return d.DialContext(ctx, network, addr)
	}
}

// DestinationControl returns a net.Dialer ControlContext hook that refuses any
// address DestinationAllowed rejects for source. Pass the zero Addr for a
// public or unknown source.
func DestinationControl(source netip.Addr) func(context.Context, string, string, syscall.RawConn) error {
	return controlAllowing(func(dst netip.Addr) bool { return DestinationAllowed(dst, source) })
}

// controlAllowing returns a net.Dialer ControlContext hook that refuses any
// address allowed rejects.
func controlAllowing(allowed func(netip.Addr) bool) func(context.Context, string, string, syscall.RawConn) error {
	return func(_ context.Context, _, address string, _ syscall.RawConn) error {
		ap, err := netip.ParseAddrPort(address)
		if err != nil {
			return fmt.Errorf("%w: unparsable address %q", ErrDestinationRefused, address)
		}
		if dst := ap.Addr().Unmap(); !allowed(dst) {
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
