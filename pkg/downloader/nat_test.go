package downloader

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/torrent"
)

type fakePortMapper struct {
	mu      sync.Mutex
	adds    []string
	deletes []string
}

func (f *fakePortMapper) Type() string { return "test-NAT" }

func (f *fakePortMapper) GetExternalAddress() (net.IP, error) {
	return net.ParseIP("203.0.113.10"), nil
}

func (f *fakePortMapper) AddPortMapping(
	_ context.Context,
	protocol string,
	_ int,
	_ string,
	_ time.Duration,
) (int, error) {
	f.mu.Lock()
	f.adds = append(f.adds, protocol)
	f.mu.Unlock()
	if protocol == "tcp" {
		return 62000, nil
	}
	return 62001, nil
}

func (f *fakePortMapper) DeletePortMapping(
	_ context.Context,
	protocol string,
	_ int,
) error {
	f.mu.Lock()
	f.deletes = append(f.deletes, protocol)
	f.mu.Unlock()
	return nil
}

func TestNATMappingUpdatesAdvertisedPortAndCleansUp(t *testing.T) {
	oldDiscover := discoverNATGateway
	mapper := &fakePortMapper{}
	discoverNATGateway = func(context.Context) (portMapper, error) {
		return mapper, nil
	}
	defer func() { discoverNATGateway = oldDiscover }()

	mgr := NewTorrentManager()
	if err := mgr.StartPeerListener(0); err != nil {
		t.Fatalf("failed to start peer listener: %v", err)
	}

	tor := &torrent.Torrent{Name: "mapped", InfoHash: [20]byte{1}}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	mgr.AddSession("01", sess)

	if err := mgr.StartNATTraversal(mgr.PeerListenPort(), mgr.PeerListenPort()); err != nil {
		t.Fatalf("failed to start NAT traversal: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for mgr.AdvertisedPeerPort() != 62000 {
		if time.Now().After(deadline) {
			t.Fatalf("expected mapped port 62000, got %d; status=%+v",
				mgr.AdvertisedPeerPort(), mgr.NATStatus())
		}
		time.Sleep(time.Millisecond)
	}
	sess.mu.RLock()
	sessionPort := sess.Port
	sess.mu.RUnlock()
	if sessionPort != 62000 {
		t.Fatalf("expected session to advertise mapped port 62000, got %d", sessionPort)
	}
	status := mgr.NATStatus()
	if !status.TCPMapped || !status.UDPMapped || status.Protocol != "test-NAT" {
		t.Fatalf("unexpected NAT status: %+v", status)
	}

	mgr.Close()

	mapper.mu.Lock()
	defer mapper.mu.Unlock()
	if len(mapper.adds) != 2 {
		t.Fatalf("expected TCP and UDP mappings, got %v", mapper.adds)
	}
	if len(mapper.deletes) != 2 {
		t.Fatalf("expected TCP and UDP cleanup, got %v", mapper.deletes)
	}
}
