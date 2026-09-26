package downloader

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/huin/goupnp"
	"github.com/huin/goupnp/dcps/internetgateway2"
)

const (
	// maxNATResponseBytes caps every UPnP device description and SOAP reply.
	// Real gateways send a few KiB; the cap stops a hostile or broken one from
	// streaming until the process runs out of memory.
	maxNATResponseBytes = 256 << 10
	// maxNATResponseHeaderBytes caps UPnP response headers.
	maxNATResponseHeaderBytes = 16 << 10
	// ssdpSearchWait is how long discovery listens for M-SEARCH replies.
	ssdpSearchWait = 2 * time.Second
	// maxSSDPLocations bounds the device descriptions one discovery fetches; a
	// gateway announces one or two.
	maxSSDPLocations = 4
	// maxUPnPServiceProbes bounds the SOAP probes one description can trigger.
	maxUPnPServiceProbes = 8
)

var errNATResponseTooLarge = errors.New("UPnP response exceeds size limit")

// ssdpSearchTargets are the M-SEARCH targets an IGD answers. Any reply carries
// the root description URL, which lists every service on the device.
var ssdpSearchTargets = [...]string{
	"urn:schemas-upnp-org:device:InternetGatewayDevice:1",
	"urn:schemas-upnp-org:device:InternetGatewayDevice:2",
	"urn:schemas-upnp-org:device:WANConnectionDevice:1",
	"urn:schemas-upnp-org:device:WANConnectionDevice:2",
}

// natHTTPClient is the only HTTP client that talks to UPnP gateways, for
// device descriptions (through goupnp.HTTPClientDefault) and SOAP calls. It
// never follows redirects, never uses a proxy, never asks for gzip (so there
// is no decompression bomb) and caps every response body.
var natHTTPClient = &http.Client{
	Timeout: natOperationTimeout,
	Transport: natTransport{base: &http.Transport{
		DialContext:            (&net.Dialer{Timeout: natOperationTimeout}).DialContext,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxResponseHeaderBytes: maxNATResponseHeaderBytes,
		ResponseHeaderTimeout:  natOperationTimeout,
	}},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func init() {
	// goupnp fetches device descriptions through this variable, which defaults
	// to http.DefaultClient: no size cap, redirects followed, gzip decoded.
	goupnp.HTTPClientDefault = natHTTPClient
}

// natTransport refuses anything but plain http to an IP literal (discovery
// only builds URLs on the gateway's address) and caps response bodies.
type natTransport struct {
	base http.RoundTripper
}

func (t natTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" || net.ParseIP(req.URL.Hostname()) == nil {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, fmt.Errorf("UPnP request to %q refused", req.URL.Redacted())
	}
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	resp.Body = &cappedBody{ReadCloser: resp.Body, left: maxNATResponseBytes}
	return resp, nil
}

// cappedBody fails, rather than silently truncating, once more than left
// bytes arrive.
type cappedBody struct {
	io.ReadCloser
	left int64
}

func (b *cappedBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		// A body of exactly the cap still ends cleanly.
		var probe [1]byte
		if n, err := b.ReadCloser.Read(probe[:]); n > 0 {
			return 0, errNATResponseTooLarge
		} else if err != nil {
			return 0, err
		}
		return 0, nil
	}
	if int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	b.left -= int64(n)
	return n, err
}

// checkGatewayURL accepts only a plain http URL on the gateway's own address,
// so a description or control URL can aim the client neither at itself nor at
// another host.
func checkGatewayURL(u *url.URL, gateway net.IP) error {
	if u.Scheme != "http" || u.User != nil || u.Opaque != "" {
		return fmt.Errorf("UPnP URL %q is not plain http", u.Redacted())
	}
	if ip := net.ParseIP(u.Hostname()); ip == nil || !ip.Equal(gateway) {
		return fmt.Errorf("UPnP URL %q is not on gateway %s", u.Redacted(), gateway)
	}
	return nil
}

// parseSSDPLocation returns the description URL of one M-SEARCH reply.
func parseSSDPLocation(packet []byte, gateway net.IP) (*url.URL, error) {
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(packet)), nil)
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("SSDP reply status %d", resp.StatusCode)
	}
	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		return nil, err
	}
	if err := checkGatewayURL(loc, gateway); err != nil {
		return nil, err
	}
	return loc, nil
}

// ssdpSearch sends M-SEARCH to dst from local and returns the distinct
// description URLs that gateway itself announced. Replies from any other
// address are dropped unparsed, and at most maxSSDPLocations are kept, so a
// LAN flood costs a bounded wait and nothing else.
func ssdpSearch(ctx context.Context, local, gateway net.IP, dst *net.UDPAddr, wait time.Duration) ([]*url.URL, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: local})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline := time.Now().Add(wait)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Now()) })
	defer stop()

	// Twice, because multicast over Wi-Fi is lossy.
	for range 2 {
		for _, target := range ssdpSearchTargets {
			msg := "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\n" +
				"MAN: \"ssdp:discover\"\r\nMX: 1\r\nST: " + target + "\r\n\r\n"
			if _, err := conn.WriteToUDP([]byte(msg), dst); err != nil {
				return nil, err
			}
		}
	}

	var locations []*url.URL
	buf := make([]byte, 2048)
	for len(locations) < maxSSDPLocations {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			break // deadline reached or ctx done
		}
		if !from.IP.Equal(gateway) {
			continue
		}
		loc, err := parseSSDPLocation(buf[:n], gateway)
		if err != nil || containsURL(locations, loc) {
			continue
		}
		locations = append(locations, loc)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return locations, nil
}

func containsURL(urls []*url.URL, u *url.URL) bool {
	for _, have := range urls {
		if have.String() == u.String() {
			return true
		}
	}
	return false
}

// discoverUPnP finds a WAN connection service with NAT enabled on the
// gateway's IGD.
func discoverUPnP(ctx context.Context, gateway, local net.IP, ssdpAddr *net.UDPAddr, wait time.Duration) (portMapper, error) {
	locations, err := ssdpSearch(ctx, local, gateway, ssdpAddr, wait)
	if err != nil {
		return nil, err
	}
	err = errors.New("no UPnP reply from the gateway")
	for _, loc := range locations {
		root, rootErr := goupnp.DeviceByURLCtx(ctx, loc)
		if rootErr != nil {
			err = rootErr
			continue
		}
		if mapper := newUPnPMapper(ctx, root, gateway, local); mapper != nil {
			return mapper, nil
		}
		err = errors.New("gateway offers no WAN connection service with NAT")
	}
	return nil, err
}

// upnpClient is what upnpMapper needs from goupnp's WANIPConnection and
// WANPPPConnection clients. Only the context-taking methods are used.
type upnpClient interface {
	GetNATRSIPStatusCtx(ctx context.Context) (rsipAvailable, natEnabled bool, err error)
	GetExternalIPAddressCtx(ctx context.Context) (string, error)
	AddPortMappingCtx(ctx context.Context, remoteHost string, externalPort uint16, protocol string,
		internalPort uint16, internalClient string, enabled bool, description string, leaseSeconds uint32) error
	DeletePortMappingCtx(ctx context.Context, remoteHost string, externalPort uint16, protocol string) error
}

// newUPnPMapper returns a mapper for the first WAN connection service in root
// that reports NAT enabled, or nil. Services whose control URL is not on the
// gateway are skipped whatever URLBase says.
func newUPnPMapper(ctx context.Context, root *goupnp.RootDevice, gateway, local net.IP) *upnpMapper {
	var found *upnpMapper
	probes := 0
	root.Device.VisitServices(func(srv *goupnp.Service) {
		if found != nil || probes >= maxUPnPServiceProbes || ctx.Err() != nil {
			return
		}
		if !srv.ControlURL.Ok || checkGatewayURL(&srv.ControlURL.URL, gateway) != nil {
			return
		}
		sc := goupnp.ServiceClient{SOAPClient: srv.NewSOAPClient(), RootDevice: root, Service: srv}
		sc.SOAPClient.HTTPClient = *natHTTPClient
		var client upnpClient
		var typ string
		switch srv.ServiceType {
		case internetgateway2.URN_WANIPConnection_2:
			client, typ = &internetgateway2.WANIPConnection2{ServiceClient: sc}, "UPnP (IP2)"
		case internetgateway2.URN_WANIPConnection_1:
			client, typ = &internetgateway2.WANIPConnection1{ServiceClient: sc}, "UPnP (IP1)"
		case internetgateway2.URN_WANPPPConnection_1:
			client, typ = &internetgateway2.WANPPPConnection1{ServiceClient: sc}, "UPnP (PPP1)"
		default:
			return
		}
		probes++
		if _, natEnabled, err := client.GetNATRSIPStatusCtx(ctx); err == nil && natEnabled {
			found = &upnpMapper{client: client, typ: typ, internalClient: local.String()}
		}
	})
	return found
}

// upnpMapper maps ports through one WAN connection service of the gateway's
// IGD. Every call is bounded by its context and by natHTTPClient.
type upnpMapper struct {
	client         upnpClient
	typ            string
	internalClient string // our address on the gateway's LAN
}

func (u *upnpMapper) Type() string { return u.typ }

func (u *upnpMapper) GetExternalAddress(ctx context.Context) (net.IP, error) {
	s, err := u.client.GetExternalIPAddressCtx(ctx)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(s)
	if ip == nil {
		return nil, errors.New("gateway returned an invalid external address")
	}
	return ip, nil
}

func (u *upnpMapper) AddPortMapping(ctx context.Context, protocol string, internalPort, externalPort int,
	description string, lifetime time.Duration) (int, error) {
	proto, err := upnpProtocol(protocol)
	if err != nil {
		return 0, err
	}
	if !validNATPort(internalPort) {
		return 0, fmt.Errorf("invalid internal port %d", internalPort)
	}
	lease := uint32(lifetime / time.Second)
	// Renew (or share) the port we were given first, then try random ones.
	candidates := [...]int{externalPort, randomNATPort(), randomNATPort(), randomNATPort()}
	err = errors.New("no external port available")
	for _, port := range candidates {
		if !validNATPort(port) {
			continue
		}
		err = u.client.AddPortMappingCtx(ctx, "", uint16(port), proto, uint16(internalPort),
			u.internalClient, true, description, lease)
		if err == nil {
			return port, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return 0, err
}

func (u *upnpMapper) DeletePortMapping(ctx context.Context, protocol string, _, externalPort int) error {
	proto, err := upnpProtocol(protocol)
	if err != nil {
		return err
	}
	if !validNATPort(externalPort) {
		return fmt.Errorf("invalid external port %d", externalPort)
	}
	return u.client.DeletePortMappingCtx(ctx, "", uint16(externalPort), proto)
}

func upnpProtocol(protocol string) (string, error) {
	switch protocol {
	case "tcp":
		return "TCP", nil
	case "udp":
		return "UDP", nil
	}
	return "", fmt.Errorf("unknown protocol %q", protocol)
}
