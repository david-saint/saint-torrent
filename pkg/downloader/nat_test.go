package downloader

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

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

// hostileGatewayError is what a hostile or broken gateway can put in a SOAP
// fault, only larger than maxNATResponseBytes allows: 300 KiB of terminal
// escape sequences, bidi overrides and invalid UTF-8.
func hostileGatewayError() error {
	unit := "\x1b]0;owned\x07\x1b[2J\u202eevil\xff "
	return errors.New(strings.Repeat(unit, (300<<10)/len(unit)+1))
}

// checkSanitizedNATError fails unless s is a bounded, printable rendering
// that starts with prefix and was cut.
func checkSanitizedNATError(t *testing.T, s, prefix string) {
	t.Helper()
	if s == "" || len(s) > maxNATErrorBytes {
		t.Fatalf("LastError is %d bytes, want 1..%d", len(s), maxNATErrorBytes)
	}
	if !utf8.ValidString(s) || !strings.HasPrefix(s, prefix) || !strings.HasSuffix(s, "…") {
		t.Fatalf("LastError = %q, want valid UTF-8 starting %q and ending in an ellipsis", s, prefix)
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			t.Fatalf("LastError keeps unprintable %U: %q", r, s)
		}
	}
}

func TestSanitizeNATError(t *testing.T) {
	for _, tc := range []struct {
		in, want string
	}{
		{"", ""},
		{"UPnP (IP1) TCP mapping failed: ConflictInMappingEntry", "UPnP (IP1) TCP mapping failed: ConflictInMappingEntry"},
		{"a\x1b[31mb\x07c\r\nd\te\x7f", "a?[31mb?c??d?e?"},
		{"right\u202eleft\u200bzw", "right?left?zw"},
		{"bad\xff\xfeutf8", "bad??utf8"},
		{"café 名前", "café 名前"},
		{strings.Repeat("a", maxNATErrorBytes), strings.Repeat("a", maxNATErrorBytes)},
		{strings.Repeat("a", maxNATErrorBytes+1), strings.Repeat("a", maxNATErrorBytes-len("…")) + "…"},
		// Two-byte runes: the cut must not split one.
		{strings.Repeat("é", maxNATErrorBytes), strings.Repeat("é", (maxNATErrorBytes-len("…"))/2) + "…"},
	} {
		var err error
		if tc.in != "" {
			err = errors.New(tc.in)
		}
		if got := sanitizeNATError(err); got != tc.want {
			t.Errorf("sanitizeNATError(%.40q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	checkSanitizedNATError(t, sanitizeNATError(hostileGatewayError()), "?]0;owned?")
}

// failingPortMapper refuses every mapping with err.
type failingPortMapper struct {
	fakePortMapper
	err error
}

func (f *failingPortMapper) AddPortMapping(context.Context, string, int, int, string, time.Duration) (int, error) {
	return 0, f.err
}

// NATStatus.LastError (shown by the TUI and the stats API) used to hold the
// gateway's whole error text: up to 256 KiB, escape sequences included.
func TestNATLastErrorFromGatewayIsSanitizedAndBounded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		discover func(context.Context) (portMapper, error)
		prefix   string
	}{
		{"mapping", func(context.Context) (portMapper, error) {
			return &failingPortMapper{err: hostileGatewayError()}, nil
		}, "test-NAT TCP mapping failed: ?]0;owned?"},
		{"discovery", func(context.Context) (portMapper, error) {
			return nil, hostileGatewayError()
		}, "?]0;owned?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stubNATDiscovery(t, tc.discover)
			mgr := NewTorrentManager()
			t.Cleanup(mgr.Close)
			if err := mgr.StartNATTraversal(51413, 51413); err != nil {
				t.Fatalf("failed to start NAT traversal: %v", err)
			}
			waitForNATStatus(t, mgr, func(s NATStatus) bool { return s.LastError != "" })
			checkSanitizedNATError(t, mgr.NATStatus().LastError, tc.prefix)
		})
	}
}

// StubNATDiscoveryErrorForTest makes NAT discovery fail with err until t
// ends. With HostileGatewayErrorForTest it is exported to the external test
// package (piece_counts_test.go), which checks what the stats API shows.
func StubNATDiscoveryErrorForTest(t *testing.T, err error) {
	t.Helper()
	stubNATDiscovery(t, func(context.Context) (portMapper, error) { return nil, err })
}

var HostileGatewayErrorForTest = hostileGatewayError
