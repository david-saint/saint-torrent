package tracker

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// mockUDPServer starts a UDP server that responds to connect and announce requests.
// It returns the server address and a cleanup function.
// The peers parameter specifies which peers the announce response should contain.
func mockUDPServer(t *testing.T, peers []Peer) (string, func()) {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return // connection closed
			}

			if n < 12 {
				continue
			}

			action := binary.BigEndian.Uint32(buf[8:12])

			switch action {
			case actionConnect:
				if n < udpConnectRequestSize {
					continue
				}
				magic := binary.BigEndian.Uint64(buf[0:8])
				if magic != udpConnectMagic {
					continue
				}
				txnID := binary.BigEndian.Uint32(buf[12:16])

				var resp [16]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				binary.BigEndian.PutUint64(resp[8:16], 0xDEADBEEFCAFEBABE) // connection_id
				pc.WriteTo(resp[:], addr)

			case actionAnnounce:
				if n < udpAnnounceRequestSize {
					continue
				}
				txnID := binary.BigEndian.Uint32(buf[12:16])

				// Build announce response
				respSize := 20 + len(peers)*6
				resp := make([]byte, respSize)
				binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				binary.BigEndian.PutUint32(resp[8:12], 1800) // interval
				binary.BigEndian.PutUint32(resp[12:16], 5)   // leechers
				binary.BigEndian.PutUint32(resp[16:20], 100) // seeders
				for i, p := range peers {
					offset := 20 + i*6
					copy(resp[offset:offset+4], p.IP.To4())
					binary.BigEndian.PutUint16(resp[offset+4:offset+6], p.Port)
				}
				pc.WriteTo(resp[:], addr)
			}
		}
	}()

	cleanup := func() {
		pc.Close()
		<-done
	}

	return pc.LocalAddr().String(), cleanup
}

func TestUDPAnnounce_FullFlow(t *testing.T) {
	expectedPeers := []Peer{
		{IP: net.IPv4(192, 168, 1, 1).To4(), Port: 6881},
		{IP: net.IPv4(10, 0, 0, 5).To4(), Port: 8080},
		{IP: net.IPv4(172, 16, 0, 1).To4(), Port: 51413},
	}

	addr, cleanup := mockUDPServer(t, expectedPeers)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	infoHash := [20]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
		0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14,
	}
	peerID := [20]byte{
		'-', 'S', 'T', '0', '0', '0', '1', '-', '1', '2',
		'3', '4', '5', '6', '7', '8', '9', '0', 'A', 'B',
	}

	resp, err := UDPAnnounce(ctx, "udp://"+addr+"/announce", infoHash, peerID, 6881, 0, 0, 1000, "")
	if err != nil {
		t.Fatalf("UDPAnnounce failed: %v", err)
	}

	if resp.Interval != 1800 {
		t.Errorf("expected interval 1800, got %d", resp.Interval)
	}
	if resp.Complete != 100 {
		t.Errorf("expected seeders (complete) 100, got %d", resp.Complete)
	}
	if resp.Incomplete != 5 {
		t.Errorf("expected leechers (incomplete) 5, got %d", resp.Incomplete)
	}

	if len(resp.Peers) != len(expectedPeers) {
		t.Fatalf("expected %d peers, got %d", len(expectedPeers), len(resp.Peers))
	}

	for i, expected := range expectedPeers {
		got := resp.Peers[i]
		if !got.IP.Equal(expected.IP) {
			t.Errorf("peer %d: expected IP %v, got %v", i, expected.IP, got.IP)
		}
		if got.Port != expected.Port {
			t.Errorf("peer %d: expected port %d, got %d", i, expected.Port, got.Port)
		}
	}
}

func TestUDPAnnounce_Timeout(t *testing.T) {
	// Start a UDP server that never responds.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer pc.Close()

	// Use a very short context deadline to make the test fast.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	infoHash := [20]byte{}
	peerID := [20]byte{}

	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestUDPAnnounce_CancelWithoutDeadlineReturnsPromptly(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer pc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- func() error {
			_, err := UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 1, "")
			return err
		}()
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error, got nil")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("UDPAnnounce did not return promptly after cancellation")
	}
}

func TestUDPAnnounce_InvalidScheme(t *testing.T) {
	ctx := context.Background()
	infoHash := [20]byte{}
	peerID := [20]byte{}

	_, err := UDPAnnounce(ctx, "http://example.com/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected error for HTTP scheme, got nil")
	}
}

func TestUDPAnnounce_InvalidURL(t *testing.T) {
	ctx := context.Background()
	infoHash := [20]byte{}
	peerID := [20]byte{}

	_, err := UDPAnnounce(ctx, "://bad-url", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}

// TestUDPConnect_TransactionIDMismatch verifies that a response with a
// mismatched transaction ID is rejected.
func TestUDPConnect_TransactionIDMismatch(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < udpConnectRequestSize {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			if action == actionConnect {
				txnID := binary.BigEndian.Uint32(buf[12:16])
				// Respond with a WRONG transaction ID
				var resp [16]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], txnID+1) // mismatch!
				binary.BigEndian.PutUint64(resp[8:16], 0xDEADBEEFCAFEBABE)
				pc.WriteTo(resp[:], addr)
			}
		}
	}()
	defer func() {
		pc.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	infoHash := [20]byte{}
	peerID := [20]byte{}
	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected transaction ID mismatch error, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestUDPConnect_InvalidResponse verifies that a truncated connect response
// is rejected.
func TestUDPConnect_InvalidResponse(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < udpConnectRequestSize {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			if action == actionConnect {
				// Send a truncated response (only 8 bytes instead of 16)
				var resp [8]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], binary.BigEndian.Uint32(buf[12:16]))
				pc.WriteTo(resp[:], addr)
			}
		}
	}()
	defer func() {
		pc.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	infoHash := [20]byte{}
	peerID := [20]byte{}
	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected error for truncated connect response, got nil")
	}
	t.Logf("got expected error: %v", err)
}

// TestUDPConnect_ErrorResponse verifies that a tracker error action is
// properly reported.
func TestUDPConnect_ErrorResponse(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < udpConnectRequestSize {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			if action == actionConnect {
				txnID := binary.BigEndian.Uint32(buf[12:16])
				errMsg := []byte("connection refused")
				resp := make([]byte, 8+len(errMsg))
				binary.BigEndian.PutUint32(resp[0:4], actionError)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				copy(resp[8:], errMsg)
				pc.WriteTo(resp, addr)
			}
		}
	}()
	defer func() {
		pc.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	infoHash := [20]byte{}
	peerID := [20]byte{}
	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err == nil {
		t.Fatal("expected tracker error, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestUDPAnnounce_NoPeers(t *testing.T) {
	// Announce returning zero peers should still succeed.
	addr, cleanup := mockUDPServer(t, nil)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	infoHash := [20]byte{}
	peerID := [20]byte{}

	resp, err := UDPAnnounce(ctx, "udp://"+addr+"/announce", infoHash, peerID, 6881, 0, 0, 0, "")
	if err != nil {
		t.Fatalf("UDPAnnounce failed: %v", err)
	}
	if len(resp.Peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(resp.Peers))
	}
}

func TestParseUDPAnnounceResponse_TooShort(t *testing.T) {
	data := make([]byte, 10) // too short
	_, err := parseUDPAnnounceResponse(data)
	if err == nil {
		t.Fatal("expected error for too-short response, got nil")
	}
}

func TestParseUDPAnnounceResponse_BadPeerLength(t *testing.T) {
	// 20 byte header + 7 bytes of peer data (not a multiple of 6)
	data := make([]byte, 27)
	binary.BigEndian.PutUint32(data[0:4], actionAnnounce)
	binary.BigEndian.PutUint32(data[8:12], 1800) // interval
	binary.BigEndian.PutUint32(data[12:16], 0)   // leechers
	binary.BigEndian.PutUint32(data[16:20], 0)   // seeders

	_, err := parseUDPAnnounceResponse(data)
	if err == nil {
		t.Fatal("expected error for non-multiple-of-6 peer data, got nil")
	}
}

func TestUDPTimeout(t *testing.T) {
	tests := []struct {
		n        int
		expected time.Duration
	}{
		{0, 15 * time.Second},
		{1, 30 * time.Second},
		{2, 60 * time.Second},
		{3, 120 * time.Second},
		{8, 3840 * time.Second},
	}
	for _, tt := range tests {
		got := udpTimeout(tt.n)
		if got != tt.expected {
			t.Errorf("udpTimeout(%d) = %v, want %v", tt.n, got, tt.expected)
		}
	}
}

func TestUDPAnnounce_WithEvent(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	receivedEvent := make(chan uint32, 1)
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			if action == actionConnect {
				txnID := binary.BigEndian.Uint32(buf[12:16])
				var resp [16]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				binary.BigEndian.PutUint64(resp[8:16], 0x1122334455667788)
				pc.WriteTo(resp[:], addr)
			} else if action == actionAnnounce {
				if n >= udpAnnounceRequestSize {
					select {
					case receivedEvent <- binary.BigEndian.Uint32(buf[80:84]):
					default:
					}
					txnID := binary.BigEndian.Uint32(buf[12:16])
					var resp [20]byte
					binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
					binary.BigEndian.PutUint32(resp[4:8], txnID)
					binary.BigEndian.PutUint32(resp[8:12], 1800)
					pc.WriteTo(resp[:], addr)
				}
			}
		}
	}()
	defer func() {
		pc.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 0, "completed")
	if err != nil {
		t.Fatalf("UDPAnnounce failed: %v", err)
	}

	select {
	case got := <-receivedEvent:
		if got != 1 {
			t.Errorf("expected received event ID 1 (completed), got %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for announce event")
	}
}

func TestUDPAnnounce_SendsNumWant(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	done := make(chan struct{})
	receivedNumWant := make(chan int32, 1)
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			n, addr, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			action := binary.BigEndian.Uint32(buf[8:12])
			if action == actionConnect {
				txnID := binary.BigEndian.Uint32(buf[12:16])
				var resp [16]byte
				binary.BigEndian.PutUint32(resp[0:4], actionConnect)
				binary.BigEndian.PutUint32(resp[4:8], txnID)
				binary.BigEndian.PutUint64(resp[8:16], 0x1122334455667788)
				pc.WriteTo(resp[:], addr)
			} else if action == actionAnnounce {
				if n >= udpAnnounceRequestSize {
					select {
					case receivedNumWant <- int32(binary.BigEndian.Uint32(buf[92:96])):
					default:
					}
					txnID := binary.BigEndian.Uint32(buf[12:16])
					var resp [20]byte
					binary.BigEndian.PutUint32(resp[0:4], actionAnnounce)
					binary.BigEndian.PutUint32(resp[4:8], txnID)
					binary.BigEndian.PutUint32(resp[8:12], 1800)
					pc.WriteTo(resp[:], addr)
				}
			}
		}
	}()
	defer func() {
		pc.Close()
		<-done
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = UDPAnnounce(ctx, "udp://"+pc.LocalAddr().String()+"/announce", [20]byte{}, [20]byte{}, 6881, 0, 0, 0, "", 123)
	if err != nil {
		t.Fatalf("UDPAnnounce failed: %v", err)
	}

	select {
	case got := <-receivedNumWant:
		if got != 123 {
			t.Errorf("expected num_want 123, got %d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for announce num_want")
	}
}
