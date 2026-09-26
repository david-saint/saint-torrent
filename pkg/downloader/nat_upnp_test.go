package downloader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natpmp "github.com/jackpal/go-nat-pmp"
)

var loopbackGateway = net.IPv4(127, 0, 0, 1)

// fakeIGDConfig is fixed before the fake starts serving.
type fakeIGDConfig struct {
	// locations returns the LOCATION headers sent for each M-SEARCH; nil
	// means the IGD's own description URL.
	locations func(base string) []string
	// controlURL is the description's controlURL; empty means "/ctl".
	controlURL string
	// urlBase, when set, is the description's URLBase.
	urlBase string
	// desc may answer a description request itself by returning true.
	desc func(w http.ResponseWriter, r *http.Request) bool
	// soap may answer a SOAP action itself by returning true.
	soap func(action string, w http.ResponseWriter, r *http.Request) bool
	// addPortMappingFault, when set, returns the UPnP error code with which
	// to refuse an AddPortMapping asking for leaseSeconds, or 0 to accept.
	addPortMappingFault func(leaseSeconds string) int
}

// fakeIGD is a loopback UPnP gateway: an SSDP responder plus the HTTP server
// for its device description and SOAP control URL.
type fakeIGD struct {
	cfg  fakeIGDConfig
	http *httptest.Server
	ssdp *net.UDPConn

	mu              sync.Mutex
	calls           []string
	acceptEncodings []string
	// leases records "PROTO port lease" for every AddPortMapping.
	leases []string
}

func newFakeIGD(t *testing.T, cfg fakeIGDConfig) *fakeIGD {
	t.Helper()
	g := &fakeIGD{cfg: cfg}
	g.http = httptest.NewServer(http.HandlerFunc(g.serveHTTP))
	t.Cleanup(g.http.Close)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: loopbackGateway})
	if err != nil {
		t.Fatalf("listen SSDP: %v", err)
	}
	g.ssdp = conn
	t.Cleanup(func() { _ = conn.Close() })
	go g.serveSSDP()
	return g
}

// discovery returns a natDiscovery that searches this fake, believes gateway
// is the default gateway, and finds no NAT-PMP service.
func (g *fakeIGD) discovery(gateway net.IP) *natDiscovery {
	return &natDiscovery{
		gateway:    func() (net.IP, error) { return gateway, nil },
		ssdpAddr:   g.ssdp.LocalAddr().(*net.UDPAddr),
		searchWait: 300 * time.Millisecond,
		newPMP:     func(net.IP) natpmpClient { return &fakePMPClient{err: errors.New("no NAT-PMP")} },
	}
}

func (g *fakeIGD) record(call string) {
	g.mu.Lock()
	g.calls = append(g.calls, call)
	g.mu.Unlock()
}

func (g *fakeIGD) recorded() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.calls...)
}

func (g *fakeIGD) waitForCall(t *testing.T, call string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !containsString(g.recorded(), call) {
		if time.Now().After(deadline) {
			t.Fatalf("gateway never saw %q; calls=%v", call, g.recorded())
		}
		time.Sleep(time.Millisecond)
	}
}

func (g *fakeIGD) serveSSDP() {
	buf := make([]byte, 2048)
	for {
		n, from, err := g.ssdp.ReadFromUDP(buf)
		if err != nil {
			return
		}
		st := ""
		for _, line := range strings.Split(string(buf[:n]), "\r\n") {
			if v, ok := strings.CutPrefix(line, "ST: "); ok {
				st = v
			}
		}
		locations := []string{g.http.URL + "/desc.xml"}
		if g.cfg.locations != nil {
			locations = g.cfg.locations(g.http.URL)
		}
		for _, loc := range locations {
			reply := "HTTP/1.1 200 OK\r\nCACHE-CONTROL: max-age=120\r\nEXT:\r\n" +
				"ST: " + st + "\r\nUSN: uuid:fake::" + st + "\r\nLOCATION: " + loc + "\r\n\r\n"
			_, _ = g.ssdp.WriteToUDP([]byte(reply), from)
		}
	}
}

const fakeIGDDescription = `<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
<specVersion><major>1</major><minor>0</minor></specVersion>%s
<device>
<deviceType>urn:schemas-upnp-org:device:InternetGatewayDevice:1</deviceType>
<deviceList><device>
<deviceType>urn:schemas-upnp-org:device:WANDevice:1</deviceType>
<deviceList><device>
<deviceType>urn:schemas-upnp-org:device:WANConnectionDevice:1</deviceType>
<serviceList><service>
<serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
<serviceId>urn:upnp-org:serviceId:WANIPConn1</serviceId>
<SCPDURL>/scpd.xml</SCPDURL>
<controlURL>%s</controlURL>
<eventSubURL>/evt</eventSubURL>
</service></serviceList>
</device></deviceList>
</device></deviceList>
</device>
</root>`

func (g *fakeIGD) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		g.record("GET " + r.URL.Path)
		if g.cfg.desc != nil && g.cfg.desc(w, r) {
			return
		}
		controlURL := g.cfg.controlURL
		if controlURL == "" {
			controlURL = "/ctl"
		}
		urlBase := ""
		if g.cfg.urlBase != "" {
			urlBase = "\n<URLBase>" + g.cfg.urlBase + "</URLBase>"
		}
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, fakeIGDDescription, urlBase, controlURL)
		return
	}

	soapAction := strings.Trim(r.Header.Get("SOAPACTION"), `"`)
	urn, action, _ := strings.Cut(soapAction, "#")
	var env struct {
		Body struct {
			Action struct {
				Protocol      string `xml:"NewProtocol"`
				ExternalPort  string `xml:"NewExternalPort"`
				LeaseDuration string `xml:"NewLeaseDuration"`
			} `xml:",any"`
		} `xml:"Body"`
	}
	_ = xml.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&env)
	args := env.Body.Action
	call := strings.TrimSpace(strings.Join([]string{action, args.Protocol, args.ExternalPort}, " "))
	g.mu.Lock()
	g.calls = append(g.calls, call)
	g.acceptEncodings = append(g.acceptEncodings, r.Header.Get("Accept-Encoding"))
	if action == "AddPortMapping" {
		g.leases = append(g.leases, args.Protocol+" "+args.ExternalPort+" "+args.LeaseDuration)
	}
	g.mu.Unlock()
	if g.cfg.soap != nil && g.cfg.soap(action, w, r) {
		return
	}
	if action == "AddPortMapping" && g.cfg.addPortMappingFault != nil {
		if code := g.cfg.addPortMappingFault(args.LeaseDuration); code != 0 {
			writeUPnPFault(w, code)
			return
		}
	}

	inner := ""
	switch action {
	case "GetNATRSIPStatus":
		inner = "<NewRSIPAvailable>0</NewRSIPAvailable><NewNATEnabled>1</NewNATEnabled>"
	case "GetExternalIPAddress":
		inner = "<NewExternalIPAddress>203.0.113.7</NewExternalIPAddress>"
	}
	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0"?>`+
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">`+
		`<s:Body><u:%sResponse xmlns:u="%s">%s</u:%sResponse></s:Body></s:Envelope>`,
		action, urn, inner, action)
}

// writeUPnPFault answers a SOAP action with a UPnP error, as an IGD does.
func writeUPnPFault(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", `text/xml; charset="utf-8"`)
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0"?>`+
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">`+
		`<s:Body><s:Fault><faultcode>s:Client</faultcode><faultstring>UPnPError</faultstring>`+
		`<detail><UPnPError xmlns="urn:schemas-upnp-org:control-1-0"><errorCode>%d</errorCode>`+
		`<errorDescription>refused</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>`, code)
}

func (g *fakeIGD) recordedLeases() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.leases...)
}

// discoverFakeUPnP runs discovery against g and returns the UPnP mapper.
func discoverFakeUPnP(t *testing.T, g *fakeIGD) *upnpMapper {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	mapper, err := g.discovery(loopbackGateway).discover(ctx)
	if err != nil {
		t.Fatalf("discovery failed: %v (calls=%v)", err, g.recorded())
	}
	u, ok := mapper.(*upnpMapper)
	if !ok {
		t.Fatalf("expected a UPnP mapper, got %T", mapper)
	}
	return u
}

// countingServer stands in for a service the gateway must not be able to aim
// the client at.
func countingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestUPnPMappingDeletesTCPAndUDPOnSharedPort(t *testing.T) {
	g := newFakeIGD(t, fakeIGDConfig{})
	stubNATDiscovery(t, g.discovery(loopbackGateway).discover)

	mgr := NewTorrentManager()
	t.Cleanup(mgr.Close)
	if err := mgr.StartNATTraversal(51413, 51413); err != nil {
		t.Fatalf("failed to start NAT traversal: %v", err)
	}
	waitForNATStatus(t, mgr, func(s NATStatus) bool { return s.TCPMapped && s.UDPMapped })
	status := mgr.NATStatus()
	if status.ExternalIP != "203.0.113.7" || status.Protocol != "UPnP (IP1)" {
		t.Fatalf("unexpected NAT status: %+v", status)
	}
	mgr.Close()

	calls := g.recorded()
	external := ""
	for _, c := range calls {
		if port, ok := strings.CutPrefix(c, "AddPortMapping TCP "); ok {
			external = port
			break
		}
	}
	if external == "" {
		t.Fatalf("no TCP mapping; calls=%v", calls)
	}
	// go-nat kept one external port per internal port, so with TCP and UDP
	// on the same local port only the TCP mapping was ever deleted.
	for _, want := range []string{
		"AddPortMapping TCP " + external,
		"AddPortMapping UDP " + external,
		"DeletePortMapping TCP " + external,
		"DeletePortMapping UDP " + external,
	} {
		if !containsString(calls, want) {
			t.Fatalf("gateway never saw %q; calls=%v", want, calls)
		}
	}
}

// onlyPermanentLeases refuses every timed lease the way IGDv1 routers that
// support only permanent mappings do.
func onlyPermanentLeases(lease string) int {
	if lease != "0" {
		return upnpErrOnlyPermanentLeases
	}
	return 0
}

// A gateway that supports only permanent leases answers every timed mapping
// with error 725, so such routers never mapped at all. The mapping must be
// retried on the same port with lease 0, and still be deleted on shutdown.
func TestUPnPOnlyPermanentLeasesMapsWithLeaseZero(t *testing.T) {
	g := newFakeIGD(t, fakeIGDConfig{addPortMappingFault: onlyPermanentLeases})
	stubNATDiscovery(t, g.discovery(loopbackGateway).discover)

	mgr := NewTorrentManager()
	t.Cleanup(mgr.Close)
	if err := mgr.StartNATTraversal(51413, 51413); err != nil {
		t.Fatalf("failed to start NAT traversal: %v", err)
	}
	waitForNATStatus(t, mgr, func(s NATStatus) bool { return s.TCPMapped && s.UDPMapped })
	if status := mgr.NATStatus(); status.LastError != "" {
		t.Fatalf("mapping reported an error: %+v", status)
	}
	leases := g.recordedLeases()
	if len(leases) != 3 {
		t.Fatalf("AddPortMapping calls = %v, want TCP timed, TCP permanent, UDP permanent", leases)
	}
	var port string
	if _, err := fmt.Sscanf(leases[0], "TCP %s", &port); err != nil {
		t.Fatalf("first mapping %q: %v", leases[0], err)
	}
	want := []string{
		"TCP " + port + " " + fmt.Sprint(int(natMappingLifetime/time.Second)),
		"TCP " + port + " 0", // the same port again, with lease 0
		"UDP " + port + " 0", // remembered: no timed attempt first
	}
	for i := range want {
		if leases[i] != want[i] {
			t.Fatalf("AddPortMapping calls = %v, want %v", leases, want)
		}
	}

	mgr.Close()
	calls := g.recorded()
	for _, c := range []string{"DeletePortMapping TCP " + port, "DeletePortMapping UDP " + port} {
		if !containsString(calls, c) {
			t.Fatalf("permanent mapping not deleted on shutdown: no %q in %v", c, calls)
		}
	}
}

// Renewals of a permanent mapping ask for lease 0 directly, and any other
// UPnP error (a port conflict here) still moves on to another port with the
// timed lease.
func TestUPnPPermanentLeaseOnlyAfterError725(t *testing.T) {
	var conflicts atomic.Int32
	g := newFakeIGD(t, fakeIGDConfig{addPortMappingFault: func(lease string) int {
		if conflicts.Add(-1) >= 0 {
			return 718 // ConflictInMappingEntry
		}
		return onlyPermanentLeases(lease)
	}})
	mapper := discoverFakeUPnP(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	timed := fmt.Sprint(int(natMappingLifetime / time.Second))

	conflicts.Store(1)
	port, err := mapper.AddPortMapping(ctx, "tcp", 51413, 40000, natMappingDescription, natMappingLifetime)
	if err != nil {
		t.Fatalf("AddPortMapping: %v", err)
	}
	leases := g.recordedLeases()
	if len(leases) != 3 || leases[0] != "TCP 40000 "+timed ||
		leases[1] != fmt.Sprintf("TCP %d %s", port, timed) || leases[2] != fmt.Sprintf("TCP %d 0", port) {
		t.Fatalf("AddPortMapping calls = %v; want a conflict on 40000, then a timed and a permanent try on %d", leases, port)
	}

	renewed, err := mapper.AddPortMapping(ctx, "tcp", 51413, port, natMappingDescription, natMappingLifetime)
	if err != nil || renewed != port {
		t.Fatalf("renewal = %d, %v; want %d", renewed, err, port)
	}
	if leases = g.recordedLeases(); len(leases) != 4 || leases[3] != fmt.Sprintf("TCP %d 0", port) {
		t.Fatalf("renewal calls = %v, want one lease-0 request", leases[3:])
	}
}

func TestUPnPHangingGatewayDoesNotBlockClose(t *testing.T) {
	release := make(chan struct{})
	g := newFakeIGD(t, fakeIGDConfig{
		soap: func(action string, _ http.ResponseWriter, r *http.Request) bool {
			if action != "GetExternalIPAddress" {
				return false
			}
			// Accept the call and never answer it.
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return true
		},
	})
	t.Cleanup(func() { close(release) })
	stubNATDiscovery(t, g.discovery(loopbackGateway).discover)

	mgr := NewTorrentManager()
	t.Cleanup(mgr.Close)
	if err := mgr.StartNATTraversal(51413, 51413); err != nil {
		t.Fatalf("failed to start NAT traversal: %v", err)
	}
	g.waitForCall(t, "GetExternalIPAddress")

	closed := make(chan struct{})
	go func() {
		mgr.Close()
		close(closed)
	}()
	// Well inside natOperationTimeout: Close must not wait out the call.
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked on a gateway that never answers GetExternalIPAddress")
	}
	// The mappings were made before the hang, so cleanup must still run.
	calls := g.recorded()
	for _, prefix := range []string{"DeletePortMapping TCP ", "DeletePortMapping UDP "} {
		found := false
		for _, c := range calls {
			found = found || strings.HasPrefix(c, prefix)
		}
		if !found {
			t.Fatalf("no %q after the hung call; calls=%v", prefix, calls)
		}
	}
}

func TestUPnPSOAPResponseIsCapped(t *testing.T) {
	var written atomic.Int64
	g := newFakeIGD(t, fakeIGDConfig{
		soap: func(action string, w http.ResponseWriter, _ *http.Request) bool {
			if action != "GetExternalIPAddress" {
				return false
			}
			_, _ = io.WriteString(w, `<?xml version="1.0"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">`+
				`<s:Body><u:GetExternalIPAddressResponse xmlns:u="urn:schemas-upnp-org:service:WANIPConnection:1">`+
				`<NewExternalIPAddress>`)
			chunk := bytes.Repeat([]byte("A"), 32<<10)
			for written.Load() < 64<<20 {
				n, err := w.Write(chunk)
				written.Add(int64(n))
				if err != nil {
					break
				}
			}
			return true
		},
	})
	mapper := discoverFakeUPnP(t, g)

	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	ip, err := mapper.GetExternalAddress(ctx)
	if err == nil || !strings.Contains(err.Error(), errNATResponseTooLarge.Error()) {
		t.Fatalf("expected the size cap to stop an endless reply, got ip=%d bytes err=%v", len(ip), err)
	}
	g.mu.Lock()
	encodings := append([]string(nil), g.acceptEncodings...)
	g.mu.Unlock()
	if len(encodings) == 0 {
		t.Fatal("gateway saw no SOAP requests")
	}
	for _, enc := range encodings {
		if enc != "" {
			t.Fatalf("SOAP request asked for %q; a compressed reply would bypass the cap", enc)
		}
	}
}

func TestNATHTTPClientDoesNotDecodeGzip(t *testing.T) {
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	_, _ = zw.Write(bytes.Repeat([]byte("A"), 4*maxNATResponseBytes))
	_ = zw.Close()
	var acceptEncoding atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		acceptEncoding.Store(r.Header.Get("Accept-Encoding"))
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer srv.Close()

	resp, err := natHTTPClient.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || !bytes.Equal(body, compressed.Bytes()) {
		t.Fatalf("expected the raw %d compressed bytes, got %d bytes, err=%v", compressed.Len(), len(body), err)
	}
	if enc := acceptEncoding.Load(); enc != "" {
		t.Fatalf("request asked for Accept-Encoding %q", enc)
	}
}

func TestSSDPIgnoresRepliesFromNonGateway(t *testing.T) {
	gateway := net.IPv4(127, 0, 0, 2)
	// The fake answers from 127.0.0.1, which is not the gateway here, while
	// claiming a description on the gateway's address.
	g := newFakeIGD(t, fakeIGDConfig{
		locations: func(string) []string { return []string{"http://127.0.0.2:5000/rootDesc.xml"} },
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	locations, err := ssdpSearch(ctx, loopbackGateway, gateway, g.ssdp.LocalAddr().(*net.UDPAddr), 300*time.Millisecond)
	if err != nil {
		t.Fatalf("ssdpSearch: %v", err)
	}
	if len(locations) != 0 {
		t.Fatalf("accepted a reply from a host that is not the gateway: %v", locations)
	}
}

func TestNATTransportRefusesHostnames(t *testing.T) {
	srv, hits := countingServer(t)
	for _, raw := range []string{
		strings.Replace(srv.URL, "127.0.0.1", "localhost", 1),
		strings.Replace(srv.URL, "http://", "https://", 1),
	} {
		if resp, err := natHTTPClient.Get(raw); err == nil {
			_ = resp.Body.Close()
			t.Fatalf("natHTTPClient fetched %s", raw)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("server saw %d requests", n)
	}
}

func TestSSDPRejectsLocationsOffGateway(t *testing.T) {
	g := newFakeIGD(t, fakeIGDConfig{
		locations: func(base string) []string {
			port := base[strings.LastIndex(base, ":")+1:]
			return []string{
				"http://localhost:" + port + "/desc.xml",
				"https://127.0.0.1:" + port + "/desc.xml",
				"http://user:pw@127.0.0.1:" + port + "/desc.xml",
				"http://127.0.0.2:" + port + "/desc.xml",
				"http://[::1]:" + port + "/desc.xml",
				"file:///etc/passwd",
			}
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	if mapper, err := g.discovery(loopbackGateway).discover(ctx); err == nil {
		t.Fatalf("accepted %T from an off-gateway LOCATION", mapper)
	}
	if calls := g.recorded(); len(calls) != 0 {
		t.Fatalf("fetched an off-gateway LOCATION: %v", calls)
	}
}

func TestSSDPFloodFetchesBoundedDescriptions(t *testing.T) {
	g := newFakeIGD(t, fakeIGDConfig{
		locations: func(base string) []string {
			locs := make([]string, 64)
			for i := range locs {
				locs[i] = fmt.Sprintf("%s/desc-%d.xml", base, i)
			}
			return locs
		},
		desc: func(w http.ResponseWriter, _ *http.Request) bool {
			http.NotFound(w, nil)
			return true
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	if _, err := g.discovery(loopbackGateway).discover(ctx); err == nil {
		t.Fatal("discovery succeeded without a usable description")
	}
	if calls := g.recorded(); len(calls) == 0 || len(calls) > maxSSDPLocations {
		t.Fatalf("expected 1..%d description fetches, got %d: %v", maxSSDPLocations, len(calls), calls)
	}
}

func TestUPnPControlURLOffGatewayIsRefused(t *testing.T) {
	victim, hits := countingServer(t)
	victimPort := victim.URL[strings.LastIndex(victim.URL, ":")+1:]
	g := newFakeIGD(t, fakeIGDConfig{
		// URLBase claims the gateway; the absolute controlURL points away.
		controlURL: "http://localhost:" + victimPort + "/ctl",
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	if mapper, err := g.discovery(loopbackGateway).discover(ctx); err == nil {
		t.Fatalf("accepted %T with an off-gateway control URL", mapper)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("sent %d SOAP requests to a host other than the gateway", n)
	}
}

func TestUPnPURLBaseOffGatewayIsRefused(t *testing.T) {
	victim, hits := countingServer(t)
	g := newFakeIGD(t, fakeIGDConfig{
		urlBase:    strings.Replace(victim.URL, "127.0.0.1", "localhost", 1),
		controlURL: "/ctl",
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	if mapper, err := g.discovery(loopbackGateway).discover(ctx); err == nil {
		t.Fatalf("accepted %T with an off-gateway URLBase", mapper)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("sent %d requests to the URLBase host", n)
	}
}

func TestUPnPDescriptionRedirectIsNotFollowed(t *testing.T) {
	victim, hits := countingServer(t)
	g := newFakeIGD(t, fakeIGDConfig{
		desc: func(w http.ResponseWriter, r *http.Request) bool {
			http.Redirect(w, r, victim.URL+"/desc.xml", http.StatusFound)
			return true
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	if mapper, err := g.discovery(loopbackGateway).discover(ctx); err == nil {
		t.Fatalf("accepted %T through a redirect", mapper)
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("followed the description redirect (%d requests)", n)
	}
}

func TestNATDiscoveryFallsBackToNATPMP(t *testing.T) {
	g := newFakeIGD(t, fakeIGDConfig{
		locations: func(string) []string { return nil },
	})
	d := g.discovery(loopbackGateway)
	d.newPMP = func(net.IP) natpmpClient {
		return &fakePMPClient{external: [4]byte{203, 0, 113, 9}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
	defer cancel()
	mapper, err := d.discover(ctx)
	if err != nil {
		t.Fatalf("discovery failed: %v", err)
	}
	if mapper.Type() != "NAT-PMP" {
		t.Fatalf("expected NAT-PMP, got %s", mapper.Type())
	}
}

func TestParseSSDPLocation(t *testing.T) {
	reply := func(status, location string) []byte {
		return []byte("HTTP/1.1 " + status + "\r\nST: upnp:rootdevice\r\nLOCATION: " + location + "\r\n\r\n")
	}
	for _, tc := range []struct {
		name   string
		packet []byte
		ok     bool
	}{
		{"gateway", reply("200 OK", "http://127.0.0.1:5000/rootDesc.xml"), true},
		{"default port", reply("200 OK", "http://127.0.0.1/rootDesc.xml"), true},
		{"other host", reply("200 OK", "http://127.0.0.2:5000/rootDesc.xml"), false},
		{"hostname", reply("200 OK", "http://localhost:5000/rootDesc.xml"), false},
		{"https", reply("200 OK", "https://127.0.0.1:5000/rootDesc.xml"), false},
		{"userinfo", reply("200 OK", "http://a:b@127.0.0.1:5000/rootDesc.xml"), false},
		{"not ok", reply("404 Not Found", "http://127.0.0.1:5000/rootDesc.xml"), false},
		{"no location", []byte("HTTP/1.1 200 OK\r\n\r\n"), false},
		{"garbage", []byte("\x1b]0;title\x07"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSSDPLocation(tc.packet, loopbackGateway)
			if (err == nil) != tc.ok {
				t.Fatalf("ok=%v, err=%v", tc.ok, err)
			}
		})
	}
}

func TestCappedBody(t *testing.T) {
	read := func(n int) error {
		body := &cappedBody{ReadCloser: io.NopCloser(bytes.NewReader(make([]byte, n))), left: 16}
		_, err := io.ReadAll(body)
		return err
	}
	if err := read(16); err != nil {
		t.Fatalf("a body of exactly the cap failed: %v", err)
	}
	if err := read(17); !errors.Is(err, errNATResponseTooLarge) {
		t.Fatalf("expected errNATResponseTooLarge past the cap, got %v", err)
	}
}

func TestCheckGatewayURLRejectsOtherHosts(t *testing.T) {
	for _, raw := range []string{
		"http://10.0.0.2:5000/ctl",
		"http://gateway.lan:5000/ctl",
		"https://10.0.0.1:5000/ctl",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if checkGatewayURL(u, net.IPv4(10, 0, 0, 1)) == nil {
			t.Fatalf("accepted %s for gateway 10.0.0.1", raw)
		}
	}
	u, _ := url.Parse("http://10.0.0.1:5000/ctl")
	if err := checkGatewayURL(u, net.IPv4(10, 0, 0, 1)); err != nil {
		t.Fatalf("rejected the gateway's own URL: %v", err)
	}
}

// fakePMPClient is a NAT-PMP gateway at the client API.
type fakePMPClient struct {
	mu       sync.Mutex
	err      error
	block    chan struct{}
	external [4]byte
	grant    uint16
	calls    []string
}

func (f *fakePMPClient) GetExternalAddress() (*natpmp.GetExternalAddressResult, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &natpmp.GetExternalAddressResult{ExternalIPAddress: f.external}, nil
}

func (f *fakePMPClient) AddPortMapping(protocol string, internalPort, requestedExternalPort, lifetime int) (*natpmp.AddPortMappingResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, fmt.Sprintf("%s %d %d %d", protocol, internalPort, requestedExternalPort, lifetime))
	f.mu.Unlock()
	if f.block != nil {
		<-f.block
	}
	if f.err != nil {
		return nil, f.err
	}
	res := &natpmp.AddPortMappingResult{InternalPort: uint16(internalPort)}
	if lifetime > 0 {
		res.MappedExternalPort = f.grant
		res.PortMappingLifetimeInSeconds = uint32(lifetime)
	}
	return res, nil
}
