package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func init() {
	dht.DefaultBootstrapHosts = nil
}

// newDHTPortTestSession builds a minimal single-piece session backed by real
// storage, for exercising the BEP 5 PORT exchange through runPeerMessageLoop.
func newDHTPortTestSession(t *testing.T) *Session {
	t.Helper()
	data := []byte("port-test-block!")
	tempDir := t.TempDir()
	tor := &torrent.Torrent{
		Name:        "port.bin",
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"port.bin"}}},
	}
	st, err := storage.NewStorage(tempDir, []storage.FileInfo{{
		Path:   "port.bin",
		Length: int64(len(data)),
	}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, tempDir)
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	return sess
}

// TestPeerLoopSendsPortToDHTCapablePeer verifies that after the handshake, a peer
// that advertised the DHT reserved bit receives a PORT message carrying our DHT
// listen port (BEP 5).
func TestPeerLoopSendsPortToDHTCapablePeer(t *testing.T) {
	sess := newDHTPortTestSession(t)
	defer sess.Close()

	sessionDHT, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start session DHT: %v", err)
	}
	defer sessionDHT.Close()
	sess.mu.Lock()
	sess.DHT = sessionDHT
	sess.mu.Unlock()

	clientConn, remoteConn := net.Pipe()
	defer remoteConn.Close()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)

	var dhtReserved [8]byte
	dhtReserved[7] |= 0x01 // peer advertises DHT support

	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:7000", "127.0.0.1", 7000, dhtReserved, true)
		close(done)
	}()

	_ = remoteConn.SetDeadline(time.Now().Add(2 * time.Second))

	// The peer first receives interested (no pieces complete), then the PORT
	// message. Scan messages until we find PORT.
	var portMsg *peer.Message
	for i := 0; i < 5 && portMsg == nil; i++ {
		msg, err := peer.ParseMessage(remoteConn)
		if err != nil {
			t.Fatalf("failed to read message: %v", err)
		}
		if msg != nil && msg.ID == peer.MsgPort {
			portMsg = msg
		}
	}
	if portMsg == nil {
		t.Fatal("did not receive a PORT message")
	}
	if len(portMsg.Payload) != 2 {
		t.Fatalf("expected 2-byte PORT payload, got %d", len(portMsg.Payload))
	}
	if got := binary.BigEndian.Uint16(portMsg.Payload); got != sessionDHT.Port() {
		t.Errorf("PORT advertised %d, expected DHT port %d", got, sessionDHT.Port())
	}

	remoteConn.Close()
	<-done
}

// TestPeerLoopIngestsInboundPort verifies that an inbound PORT message feeds the
// advertised endpoint into the DHT routing table, growing it (BEP 5). A second
// live DHT instance plays the advertised node and answers the resulting ping.
func TestPeerLoopIngestsInboundPort(t *testing.T) {
	sess := newDHTPortTestSession(t)
	defer sess.Close()

	sessionDHT, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start session DHT: %v", err)
	}
	defer sessionDHT.Close()
	sess.mu.Lock()
	sess.DHT = sessionDHT
	sess.mu.Unlock()

	// The node the peer "advertises" via PORT: a real DHT that answers pings.
	advertised, err := dht.NewDHT(t.TempDir(), 0)
	if err != nil {
		t.Fatalf("failed to start advertised DHT: %v", err)
	}
	defer advertised.Close()
	advertisedPort := advertised.Port()
	if advertisedPort == 0 {
		t.Fatal("advertised DHT has no listen port")
	}

	if sessionDHT.NodesCount() != 0 {
		t.Fatalf("expected empty routing table, got %d nodes", sessionDHT.NodesCount())
	}

	clientConn, remoteConn := net.Pipe()
	defer remoteConn.Close()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)

	done := make(chan struct{})
	go func() {
		// peerReserved without the DHT bit so the loop does not also send its own
		// PORT (keeps the message stream we drain below clean).
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:7100", "127.0.0.1", 7100, [8]byte{}, true)
		close(done)
	}()

	_ = remoteConn.SetDeadline(time.Now().Add(3 * time.Second))
	// Drain the loop's initial interested message so the pipe does not deadlock.
	if _, err := peer.ParseMessage(remoteConn); err != nil {
		t.Fatalf("failed to read initial message: %v", err)
	}

	// Send an inbound PORT advertising the live DHT's loopback endpoint.
	portPayload := make([]byte, 2)
	binary.BigEndian.PutUint16(portPayload, advertisedPort)
	if _, err := remoteConn.Write((&peer.Message{ID: peer.MsgPort, Payload: portPayload}).Serialize()); err != nil {
		t.Fatalf("failed to send PORT message: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for !sessionDHT.HasNodeAddress(net.ParseIP("127.0.0.1"), advertisedPort) {
		select {
		case <-deadline:
			t.Fatal("routing table did not receive the expected advertised node after inbound PORT")
		case <-time.After(20 * time.Millisecond):
		}
	}

	remoteConn.Close()
	<-done
}
