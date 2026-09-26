package downloader

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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

func (f *fakePortMapper) GetExternalAddress(context.Context) (net.IP, error) {
	return net.ParseIP("203.0.113.10"), nil
}

func (f *fakePortMapper) AddPortMapping(
	_ context.Context,
	protocol string,
	_ int,
	externalPort int,
	_ string,
	_ time.Duration,
) (int, error) {
	f.mu.Lock()
	f.adds = append(f.adds, fmt.Sprintf("%s:%d", protocol, externalPort))
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
	externalPort int,
) error {
	f.mu.Lock()
	f.deletes = append(f.deletes, fmt.Sprintf("%s:%d", protocol, externalPort))
	f.mu.Unlock()
	return nil
}

func stubNATDiscovery(t *testing.T, discover func(context.Context) (portMapper, error)) {
	t.Helper()
	old := discoverNATGateway
	discoverNATGateway = discover
	t.Cleanup(func() { discoverNATGateway = old })
}

func waitForNATStatus(t *testing.T, mgr *TorrentManager, ok func(NATStatus) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok(mgr.NATStatus()) {
		if time.Now().After(deadline) {
			t.Fatalf("NAT status never reached the expected state: %+v", mgr.NATStatus())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNATMappingUpdatesAdvertisedPortAndCleansUp(t *testing.T) {
	mapper := &fakePortMapper{}
	stubNATDiscovery(t, func(context.Context) (portMapper, error) {
		return mapper, nil
	})

	mgr := NewTorrentManager()
	t.Cleanup(mgr.Close)
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
	// TCP asks for any port; UDP on the same local port asks for TCP's.
	if len(mapper.adds) != 2 || mapper.adds[0] != "tcp:0" || mapper.adds[1] != "udp:62000" {
		t.Fatalf("expected TCP then UDP on TCP's external port, got %v", mapper.adds)
	}
	// Each protocol's own mapping is deleted, even though the local ports match.
	if len(mapper.deletes) != 2 || !containsString(mapper.deletes, "tcp:62000") ||
		!containsString(mapper.deletes, "udp:62001") {
		t.Fatalf("expected TCP and UDP cleanup of the granted ports, got %v", mapper.deletes)
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// concurrentDeleteMapper records whether the TCP and UDP deletes overlapped:
// each waits (briefly) for the other to start.
type concurrentDeleteMapper struct {
	fakePortMapper
	started    sync.WaitGroup
	overlapped atomic.Int32
}

func (f *concurrentDeleteMapper) DeletePortMapping(ctx context.Context, protocol string, internalPort, externalPort int) error {
	f.started.Done()
	both := make(chan struct{})
	go func() {
		f.started.Wait()
		close(both)
	}()
	select {
	case <-both:
		f.overlapped.Add(1)
	case <-time.After(2 * time.Second):
	}
	return f.fakePortMapper.DeletePortMapping(ctx, protocol, internalPort, externalPort)
}

func TestNATCleanupDeletesTCPAndUDPConcurrently(t *testing.T) {
	mapper := &concurrentDeleteMapper{}
	mapper.started.Add(2)
	stubNATDiscovery(t, func(context.Context) (portMapper, error) {
		return mapper, nil
	})

	mgr := NewTorrentManager()
	t.Cleanup(mgr.Close)
	if err := mgr.StartNATTraversal(51413, 51413); err != nil {
		t.Fatalf("failed to start NAT traversal: %v", err)
	}
	waitForNATStatus(t, mgr, func(s NATStatus) bool { return s.TCPMapped && s.UDPMapped })

	start := time.Now()
	mgr.Close()
	if got := mapper.overlapped.Load(); got != 2 {
		t.Fatalf("TCP and UDP deletes ran one after the other (overlapped=%d, took %v)",
			got, time.Since(start))
	}
}
