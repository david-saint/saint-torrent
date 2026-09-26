package downloader

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"sainttorrent/pkg/logging"
)

const (
	natMappingDescription = "saintTorrent"
	natMappingLifetime    = 30 * time.Minute
	natRenewInterval      = 15 * time.Minute
	natRetryInterval      = 5 * time.Minute
	natOperationTimeout   = 5 * time.Second
	// maxNATErrorBytes bounds NATStatus.LastError, which the TUI and the
	// stats API show. Its text can come from the gateway (a SOAP fault of up
	// to maxNATResponseBytes).
	maxNATErrorBytes = 256
)

// portMapper is one gateway's port-mapping service. Every method must return
// once ctx is done, so the NAT loop, and with it TorrentManager.Close, can
// never be held up by a slow or hostile gateway.
type portMapper interface {
	Type() string
	GetExternalAddress(ctx context.Context) (net.IP, error)
	// AddPortMapping maps internalPort, asking for externalPort when it is
	// non-zero, and returns the external port the gateway actually mapped.
	AddPortMapping(ctx context.Context, protocol string, internalPort, externalPort int,
		description string, lifetime time.Duration) (int, error)
	// DeletePortMapping removes the mapping AddPortMapping returned
	// externalPort for.
	DeletePortMapping(ctx context.Context, protocol string, internalPort, externalPort int) error
}

// natDiscovery finds the port-mapping service on the default gateway. The
// fields describe the network environment so tests can substitute loopback
// fakes.
type natDiscovery struct {
	gateway    func() (net.IP, error)
	ssdpAddr   *net.UDPAddr
	searchWait time.Duration
	newPMP     func(gateway net.IP) natpmpClient
}

var discoverNATGateway = (&natDiscovery{
	gateway:    defaultGatewayIPv4,
	ssdpAddr:   &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900},
	searchWait: ssdpSearchWait,
	newPMP:     newNATPMPClient,
}).discover

// discover returns a UPnP IGD or NAT-PMP mapper on the default gateway, and
// only there: any LAN host can answer SSDP, and whoever supplies the device
// description decides where the SOAP calls go and which port is advertised.
// A mapping on any other box would not carry our traffic anyway.
func (d *natDiscovery) discover(ctx context.Context) (portMapper, error) {
	gateway, err := d.gateway()
	if err != nil {
		return nil, fmt.Errorf("default gateway: %w", err)
	}
	local, err := localAddrToward(gateway)
	if err != nil {
		return nil, err
	}

	// NAT-PMP answers in one round trip; probe it while SSDP runs and use it
	// only when the gateway has no UPnP IGD.
	pmp := make(chan portMapper, 1)
	go func() {
		client := d.newPMP(gateway)
		if _, err := callWithContext(ctx, client.GetExternalAddress); err != nil {
			pmp <- nil
			return
		}
		pmp <- &natpmpMapper{client: client}
	}()

	mapper, upnpErr := discoverUPnP(ctx, gateway, local, d.ssdpAddr, d.searchWait)
	if mapper != nil {
		return mapper, nil
	}
	// callWithContext returns once ctx is done, so this never outlives it.
	if mapper := <-pmp; mapper != nil {
		return mapper, nil
	}
	return nil, fmt.Errorf("no UPnP IGD or NAT-PMP service on gateway %s: %w", gateway, upnpErr)
}

// defaultGatewayIPv4 lives in nat_route_netroute.go, or nat_route_stub.go on
// platforms go-netroute does not support.

// localAddrToward returns the local address the kernel uses to reach gateway:
// the address SSDP is sent from and mappings point at. Connecting a UDP socket
// sends nothing.
func localAddrToward(gateway net.IP) (net.IP, error) {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: gateway, Port: 1900})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || addr.IP.To4() == nil {
		return nil, fmt.Errorf("no local IPv4 address toward gateway %s", gateway)
	}
	return addr.IP.To4(), nil
}

// callWithContext runs call, a library call that takes no context, in its own
// goroutine and stops waiting for it when ctx is done. The call itself is
// bounded by its client's timeout; the caller just no longer waits for it.
func callWithContext[T any](ctx context.Context, call func() (T, error)) (T, error) {
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := call()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func validNATPort(port int) bool {
	return port > 0 && port <= 65535
}

// randomNATPort picks an external port the way go-nat did, so several clients
// behind one NAT rarely ask for the same one.
func randomNATPort() int {
	return 10000 + rand.IntN(65535-10000)
}

// NATStatus describes the current automatic port-mapping state.
type NATStatus struct {
	Enabled        bool
	Protocol       string
	ExternalIP     string
	ListenPort     uint16
	AdvertisedPort uint16
	TCPMapped      bool
	UDPMapped      bool
	LastError      string
}

// StartNATTraversal starts asynchronous UPnP IGD/NAT-PMP discovery and mapping.
// Failure is non-fatal because a stable local port can still be forwarded manually.
func (m *TorrentManager) StartNATTraversal(tcpPort, udpPort uint16) error {
	if tcpPort == 0 {
		return fmt.Errorf("TCP listen port is required for NAT traversal")
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("torrent manager is closed")
	}
	if m.natStarted {
		m.mu.Unlock()
		return nil
	}
	m.natStarted = true
	m.natStatus.Enabled = true
	m.natStatus.ListenPort = tcpPort
	if m.natStatus.AdvertisedPort == 0 {
		m.natStatus.AdvertisedPort = tcpPort
	}
	m.wg.Add(1)
	m.mu.Unlock()

	if logging.Enabled() {
		logging.Info("nat_traversal_started",
			logging.Uint16("tcp_port", tcpPort),
			logging.Uint16("udp_port", udpPort),
		)
	}
	go m.natTraversalLoop(tcpPort, udpPort)
	return nil
}

// NATStatus returns a snapshot of the current automatic mapping state.
func (m *TorrentManager) NATStatus() NATStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.natStatus
}

func (m *TorrentManager) natTraversalLoop(tcpPort, udpPort uint16) {
	defer m.wg.Done()

	for {
		if m.ctx.Err() != nil {
			return
		}

		discoveryCtx, cancel := context.WithTimeout(m.ctx, natOperationTimeout)
		gateway, err := discoverNATGateway(discoveryCtx)
		cancel()
		if err != nil {
			m.recordNATFailure(err)
			if !waitForContext(m.ctx, natRetryInterval) {
				return
			}
			continue
		}
		if logging.Enabled() {
			logging.Info("nat_gateway_discovered",
				logging.String("protocol", gateway.Type()),
			)
		}

		if m.maintainNATMappings(gateway, tcpPort, udpPort) {
			return
		}
		if !waitForContext(m.ctx, natRetryInterval) {
			return
		}
	}
}

// maintainNATMappings returns true when the manager context was cancelled.
func (m *TorrentManager) maintainNATMappings(gateway portMapper, tcpPort, udpPort uint16) bool {
	// External ports the gateway granted, 0 while unmapped. They live here, per
	// protocol, so TCP and UDP on the same internal port stay two mappings that
	// are renewed and deleted separately.
	tcpExternal := 0
	udpExternal := 0

	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
		defer cancel()
		// Delete concurrently so a slow gateway costs one round trip, not two,
		// of the shutdown budget.
		var wg sync.WaitGroup
		deleteMapping := func(protocol string, internalPort uint16, externalPort int) {
			if externalPort == 0 {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = gateway.DeletePortMapping(cleanupCtx, protocol, int(internalPort), externalPort)
			}()
		}
		deleteMapping("tcp", tcpPort, tcpExternal)
		deleteMapping("udp", udpPort, udpExternal)
		wg.Wait()

		m.mu.Lock()
		if m.natGateway == gateway {
			m.natGateway = nil
			m.natStatus.TCPMapped = false
			m.natStatus.UDPMapped = false
		}
		m.mu.Unlock()
		if logging.Enabled() {
			logging.Info("nat_mapping_cleaned_up",
				logging.String("protocol", gateway.Type()),
				logging.Bool("tcp_mapped", tcpExternal != 0),
				logging.Bool("udp_mapped", udpExternal != 0),
			)
		}
	}
	defer cleanup()

	mapPorts := func() error {
		mapCtx, cancel := context.WithTimeout(m.ctx, natOperationTimeout)
		defer cancel()

		externalTCPPort, err := gateway.AddPortMapping(
			mapCtx, "tcp", int(tcpPort), tcpExternal, natMappingDescription, natMappingLifetime,
		)
		if err != nil {
			return fmt.Errorf("%s TCP mapping failed: %w", gateway.Type(), err)
		}
		if !validNATPort(externalTCPPort) {
			return fmt.Errorf("%s returned invalid external TCP port %d", gateway.Type(), externalTCPPort)
		}
		tcpExternal = externalTCPPort

		udpMapped := false
		if udpPort != 0 {
			preferred := udpExternal
			if preferred == 0 && udpPort == tcpPort {
				// Keep UDP (DHT, uTP) on the external port peers are told.
				preferred = externalTCPPort
			}
			externalUDPPort, err := gateway.AddPortMapping(
				mapCtx, "udp", int(udpPort), preferred, natMappingDescription, natMappingLifetime,
			)
			if err == nil && validNATPort(externalUDPPort) {
				udpExternal = externalUDPPort
				udpMapped = true
			}
		}

		externalIP := ""
		if ip, err := gateway.GetExternalAddress(mapCtx); err == nil && ip != nil {
			externalIP = ip.String()
		}

		m.mu.Lock()
		m.natGateway = gateway
		m.natStatus.Protocol = gateway.Type()
		m.natStatus.ExternalIP = externalIP
		m.natStatus.TCPMapped = true
		m.natStatus.UDPMapped = udpMapped
		m.natStatus.LastError = ""
		m.mu.Unlock()
		m.setAdvertisedPeerPort(uint16(externalTCPPort))
		if logging.Enabled() {
			logging.Info("nat_mapping_active",
				logging.String("protocol", gateway.Type()),
				logging.String("external_ip", externalIP),
				logging.Int("external_tcp_port", externalTCPPort),
				logging.Uint16("local_tcp_port", tcpPort),
				logging.Uint16("local_udp_port", udpPort),
				logging.Bool("udp_mapped", udpMapped),
			)
		}
		return nil
	}

	if err := mapPorts(); err != nil {
		m.recordNATFailure(err)
		m.setAdvertisedPeerPort(tcpPort)
		return m.ctx.Err() != nil
	}

	ticker := time.NewTicker(natRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return true
		case <-ticker.C:
			if err := mapPorts(); err != nil {
				m.recordNATFailure(err)
				m.setAdvertisedPeerPort(tcpPort)
				return false
			}
		}
	}
}

func (m *TorrentManager) recordNATFailure(err error) {
	msg := sanitizeNATError(err)
	m.mu.Lock()
	m.natStatus.TCPMapped = false
	m.natStatus.UDPMapped = false
	m.natStatus.LastError = msg
	m.mu.Unlock()
	if logging.Enabled() {
		logging.Warn("nat_mapping_failed",
			logging.String("error", msg),
		)
	}
}

// sanitizeNATError renders err for NATStatus.LastError. The text can carry a
// gateway's SOAP fault or description, so runes that are not printable
// (control characters, including ESC; format characters such as bidi
// overrides; invalid UTF-8) become '?', and the result is cut on a rune
// boundary to at most maxNATErrorBytes, ending in an ellipsis when cut.
func sanitizeNATError(err error) string {
	if err == nil {
		return ""
	}
	const ellipsis = "…"
	msg := err.Error()
	out := make([]byte, 0, min(len(msg), maxNATErrorBytes))
	cut := -1 // where to cut if the rest does not fit
	for i := 0; i < len(msg); {
		r, size := utf8.DecodeRuneInString(msg[i:])
		i += size
		if (r == utf8.RuneError && size == 1) || !unicode.IsPrint(r) {
			r = '?'
		}
		n := utf8.RuneLen(r)
		if cut < 0 && len(out)+n > maxNATErrorBytes-len(ellipsis) {
			cut = len(out)
		}
		if len(out)+n > maxNATErrorBytes {
			return string(out[:cut]) + ellipsis
		}
		out = utf8.AppendRune(out, r)
	}
	return string(out)
}

func waitForContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
