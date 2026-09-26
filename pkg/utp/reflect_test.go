package utp

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// TestUnsolicitedResetsAreRateLimited sends many packets for connections that
// do not exist, as a sender spoofing a victim's address would to bounce
// traffic off us. Every one used to draw a RESET; now at most
// maxResetsPerSecond do.
func TestUnsolicitedResetsAreRateLimited(t *testing.T) {
	server := newServerSocket(t)
	peer := newRawPeer(t, server)
	from := peer.conn.LocalAddr().(*net.UDPAddr)

	// Driven through handleUTPPacket, the read loop's own entry point, so no
	// datagram is lost before it is counted.
	const strays = 3 * maxResetsPerSecond
	for i := 0; i < strays; i++ {
		p := packet{typ: packetTypeData, connID: uint16(20000 + i), seqNr: uint16(i), ackNr: 7, payload: []byte("x")}
		server.handleUTPPacket(p.marshal(), from)
	}
	resets := 0
	for {
		p, ok := peer.recv(100 * time.Millisecond)
		if !ok {
			break
		}
		if p.typ != packetTypeReset || p.ackNr != uint16(resets) || p.connID != uint16(20000+resets) {
			t.Fatalf("reply %d: type=%d connID=%d ack=%d, want RESET connID=%d ack=%d", resets, p.typ, p.connID, p.ackNr, 20000+resets, resets)
		}
		resets++
	}
	if resets != maxResetsPerSecond {
		t.Fatalf("%d stray packets drew %d RESETs, want the budget of %d", strays, resets, maxResetsPerSecond)
	}
}

// TestDHTDatagramCopyIsCapped checks that an oversized non-uTP datagram is
// handed to the DHT truncated to what the DHT reads, instead of being copied
// and queued whole.
func TestDHTDatagramCopyIsCapped(t *testing.T) {
	socket := newServerSocket(t)
	raw, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("raw udp: %v", err)
	}
	defer raw.Close()

	// 8000 bytes: past maxDHTDatagram, not a uTP header, and under macOS's
	// default net.inet.udp.maxdgram (9216), past which sendto fails.
	big := bytes.Repeat([]byte("d1:ae"), 1600)
	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(socket.Port())}
	if _, err := raw.WriteToUDP(big, target); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	select {
	case pkt := <-socket.DHTConn().incoming:
		if len(pkt.data) != maxDHTDatagram || cap(pkt.data) >= 2*maxDHTDatagram {
			t.Fatalf("queued datagram len=%d cap=%d, want len %d", len(pkt.data), cap(pkt.data), maxDHTDatagram)
		}
		if !bytes.Equal(pkt.data, big[:maxDHTDatagram]) {
			t.Fatal("queued datagram is not the leading bytes of what was sent")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("datagram was not delivered to the DHT queue")
	}
}
