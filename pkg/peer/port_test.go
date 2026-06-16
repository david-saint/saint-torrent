package peer

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// TestSendPortMessage verifies SendPort frames a BEP 5 PORT message (id 9) with
// the DHT UDP port as a 2-byte big-endian payload.
func TestSendPortMessage(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	var infoHash, peerID [20]byte
	client := NewClient(clientConn, infoHash, peerID)

	const dhtPort uint16 = 51413
	errChan := make(chan error, 1)
	go func() {
		errChan <- client.SendPort(dhtPort)
	}()

	serverConn.SetDeadline(time.Now().Add(2 * time.Second))
	msg, err := ParseMessage(serverConn)
	if err != nil {
		t.Fatalf("failed to parse PORT message: %v", err)
	}
	if err := <-errChan; err != nil {
		t.Fatalf("SendPort returned error: %v", err)
	}
	if msg == nil {
		t.Fatal("expected a PORT message, got keep-alive")
	}
	if msg.ID != MsgPort {
		t.Errorf("expected message ID %d (MsgPort), got %d", MsgPort, msg.ID)
	}
	if len(msg.Payload) != 2 {
		t.Fatalf("expected 2-byte payload, got %d bytes", len(msg.Payload))
	}
	if got := binary.BigEndian.Uint16(msg.Payload); got != dhtPort {
		t.Errorf("expected port %d, got %d", dhtPort, got)
	}
}

// TestHandshakeSetsDHTReservedBit verifies the outgoing handshake advertises DHT
// support (BEP 5, reserved[7] bit 0x01) alongside the existing extension-protocol
// bit (BEP 10, reserved[5] = 0x10).
func TestHandshakeSetsDHTReservedBit(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	var infoHash, peerID [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i)
	}

	hsChan := make(chan *Handshake, 1)
	errChan := make(chan error, 1)
	go func() {
		serverConn.SetDeadline(time.Now().Add(2 * time.Second))
		// Read the client's handshake to inspect its reserved bytes.
		hs, err := ParseHandshake(serverConn)
		if err != nil {
			errChan <- err
			return
		}
		hsChan <- hs
		// Reply so the client's Handshake() call returns cleanly.
		resp := &Handshake{Pstr: "BitTorrent protocol", InfoHash: infoHash}
		_, _ = serverConn.Write(resp.Serialize())
	}()

	client := NewClient(clientConn, infoHash, peerID)
	if _, err := client.Handshake(); err != nil {
		t.Fatalf("client handshake failed: %v", err)
	}

	select {
	case err := <-errChan:
		t.Fatalf("server failed to parse handshake: %v", err)
	case hs := <-hsChan:
		if hs.Reserved[7]&0x01 == 0 {
			t.Errorf("DHT reserved bit not set: reserved=%v", hs.Reserved)
		}
		if hs.Reserved[5]&0x10 == 0 {
			t.Errorf("BEP 10 extension bit not set: reserved=%v", hs.Reserved)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handshake")
	}
}
