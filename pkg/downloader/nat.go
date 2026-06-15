package downloader

import (
	"context"
	"fmt"
	"net"
	"time"

	gonat "github.com/libp2p/go-nat"
)

const (
	natMappingDescription = "saintTorrent"
	natMappingLifetime    = 30 * time.Minute
	natRenewInterval      = 15 * time.Minute
	natRetryInterval      = 5 * time.Minute
	natOperationTimeout   = 5 * time.Second
)

type portMapper interface {
	Type() string
	GetExternalAddress() (net.IP, error)
	AddPortMapping(context.Context, string, int, string, time.Duration) (int, error)
	DeletePortMapping(context.Context, string, int) error
}

var discoverNATGateway = func(ctx context.Context) (portMapper, error) {
	return gonat.DiscoverGateway(ctx)
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
	tcpMapped := false
	udpMapped := false

	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), natOperationTimeout)
		defer cancel()
		if tcpMapped {
			_ = gateway.DeletePortMapping(cleanupCtx, "tcp", int(tcpPort))
		}
		if udpMapped {
			_ = gateway.DeletePortMapping(cleanupCtx, "udp", int(udpPort))
		}
		m.mu.Lock()
		if m.natGateway == gateway {
			m.natGateway = nil
			m.natStatus.TCPMapped = false
			m.natStatus.UDPMapped = false
		}
		m.mu.Unlock()
	}
	defer cleanup()

	mapPorts := func() error {
		mapCtx, cancel := context.WithTimeout(m.ctx, natOperationTimeout)
		defer cancel()

		externalTCPPort, err := gateway.AddPortMapping(
			mapCtx, "tcp", int(tcpPort), natMappingDescription, natMappingLifetime,
		)
		if err != nil {
			return fmt.Errorf("%s TCP mapping failed: %w", gateway.Type(), err)
		}
		if externalTCPPort <= 0 || externalTCPPort > 65535 {
			return fmt.Errorf("%s returned invalid external TCP port %d", gateway.Type(), externalTCPPort)
		}
		tcpMapped = true

		if udpPort != 0 {
			if _, err := gateway.AddPortMapping(
				mapCtx, "udp", int(udpPort), natMappingDescription, natMappingLifetime,
			); err == nil {
				udpMapped = true
			}
		}

		externalIP := ""
		if ip, err := gateway.GetExternalAddress(); err == nil && ip != nil {
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
	m.mu.Lock()
	m.natStatus.TCPMapped = false
	m.natStatus.UDPMapped = false
	m.natStatus.LastError = err.Error()
	m.mu.Unlock()
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
