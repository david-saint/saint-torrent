package utp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// acceptRaw completes an inbound handshake from a libutp-style raw initiator
// and returns the accepted conn and the SYN-ACK's seq_nr, which is the seq_nr
// of the conn's first DATA.
func acceptRaw(t *testing.T, server *Socket, peer *rawPeer, syn packet) (*Conn, uint16) {
	t.Helper()
	ln := server.Listen()
	accepted := acceptAsync(ln)
	synAck := peer.synAck(syn)
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1})
	return expectAccept(t, accepted, "raw initiator").(*Conn), synAck.seqNr
}

func newServerSocket(t *testing.T) *Socket {
	t.Helper()
	s, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// expectOpen checks that c neither reached EOF nor failed: a read with a
// short deadline must time out.
func expectOpen(t *testing.T, c net.Conn, what string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var b [1]byte
	n, err := c.Read(b[:])
	var ne net.Error
	if n != 0 || !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("%s: read got n=%d err=%v, want a timeout on a live conn", what, n, err)
	}
}

// TestBlindResetIsIgnored checks that a RESET is honoured only when its
// ack_nr names a packet we sent. Guessing the connection id alone must not
// be enough to tear a connection down.
func TestBlindResetIsIgnored(t *testing.T) {
	server := newServerSocket(t)
	peer := newRawPeer(t, server)
	syn := packet{connID: 8100, seqNr: 500}
	c, r := acceptRaw(t, server, peer, syn)

	for _, ack := range []uint16{r + 1, r + 20000, r - ackNrSlack - 2, r - 30000} {
		peer.send(packet{typ: packetTypeReset, connID: syn.connID + 1, seqNr: 9, ackNr: ack})
	}
	expectOpen(t, c, "after forged RESETs")

	// The RESET a peer that lost our connection sends in reply to our STATE
	// acks the STATE's seq_nr, localSeq.
	peer.send(packet{typ: packetTypeReset, connID: syn.connID + 1, seqNr: 9, ackNr: r})
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, errReset) {
		t.Fatalf("read after plausible RESET: %v, want %v", err, errReset)
	}
}

// TestBlindFinIsIgnored checks that a FIN ends the stream only at the next
// expected seq_nr and with a plausible ack_nr. Any older FIN used to close
// the read side, so half the sequence space was a valid blind guess.
func TestBlindFinIsIgnored(t *testing.T) {
	server := newServerSocket(t)
	peer := newRawPeer(t, server)
	syn := packet{connID: 8200, seqNr: 700}
	c, r := acceptRaw(t, server, peer, syn)
	next := syn.seqNr + 1

	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next - 20000, ackNr: r - 1})
	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next - 1, ackNr: r - 1})
	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next + maxReorderDistance, ackNr: r - 1})
	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next, ackNr: r + 20000})
	expectOpen(t, c, "after forged FINs")
	if p, ok := peer.recv(50 * time.Millisecond); ok {
		t.Fatalf("forged FIN drew a reply: type=%d ack=%d", p.typ, p.ackNr)
	}

	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next, ackNr: r - 1})
	if ack := peer.mustRecv("ack of FIN"); ack.typ != packetTypeState || ack.ackNr != next {
		t.Fatalf("ack of FIN: type=%d ack=%d, want STATE ack=%d", ack.typ, ack.ackNr, next)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read after FIN: %v, want EOF", err)
	}
	// A retransmit of the applied FIN is re-acked.
	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: next, ackNr: r - 1})
	if ack := peer.mustRecv("re-ack of FIN"); ack.typ != packetTypeState || ack.ackNr != next {
		t.Fatalf("re-ack of FIN: type=%d ack=%d, want STATE ack=%d", ack.typ, ack.ackNr, next)
	}
}

// TestImplausibleAckDoesNotCompleteWrite reproduces the blind ack sweep: a
// STATE acking data we never sent used to wake every write waiter, so our
// writes completed toward a sender that received nothing. Such acks are now
// ignored, while in-order payload on a packet with a bad ack still arrives.
func TestImplausibleAckDoesNotCompleteWrite(t *testing.T) {
	server := newServerSocket(t)
	peer := newRawPeer(t, server)
	syn := packet{connID: 8300, seqNr: 900}
	c, r := acceptRaw(t, server, peer, syn)

	writeDone := make(chan error, 1)
	go func() {
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := c.Write([]byte("reflected"))
		writeDone <- err
	}()
	if data := peer.mustRecv("our DATA"); data.typ != packetTypeData || data.seqNr != r {
		t.Fatalf("our DATA: type=%d seq=%d, want DATA seq=%d", data.typ, data.seqNr, r)
	}

	for _, ack := range []uint16{r + 1, r + 2, r + 500, r + 20000, r + 32767} {
		peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: ack})
	}
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: r + 5000, payload: []byte("kept")})
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "kept" {
		t.Fatalf("read %q err=%v, want the in-order payload despite its bad ack", got, err)
	}
	select {
	case err := <-writeDone:
		t.Fatalf("write completed on acks for data never sent (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}

	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: r})
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("genuine ack did not complete the write")
	}
}

// TestReorderDistanceIsCapped checks that out-of-order packets are buffered
// only within maxReorderDistance of the next expected seq_nr, so a peer cannot
// grow the pending map with tiny packets spread across the sequence space.
func TestReorderDistanceIsCapped(t *testing.T) {
	server := newServerSocket(t)
	peer := newRawPeer(t, server)
	syn := packet{connID: 8400, seqNr: 100}
	c, r := acceptRaw(t, server, peer, syn)
	next := syn.seqNr + 1

	for i := uint16(0); i < 64; i++ {
		peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: next + maxReorderDistance + i, ackNr: r - 1})
		peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: next + 20000 + i, ackNr: r - 1, payload: []byte{1}})
	}
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: next + maxReorderDistance - 1, ackNr: r - 1, payload: []byte("far")})
	// Only the last packet was within reach, and only it draws an ack.
	if ack := peer.mustRecv("ack of buffered packet"); ack.typ != packetTypeState || ack.ackNr != syn.seqNr {
		t.Fatalf("ack: type=%d ack=%d, want STATE ack=%d", ack.typ, ack.ackNr, syn.seqNr)
	}
	c.mu.Lock()
	pending, pendingBytes := len(c.pending), c.pendingBytes
	c.mu.Unlock()
	if pending != 1 || pendingBytes != 3 {
		t.Fatalf("pending entries=%d bytes=%d, want only the packet within the reorder window", pending, pendingBytes)
	}
}

// TestDialIgnoresSynAckForAnotherSyn checks that only a STATE acking our
// SYN's seq_nr establishes an outbound conn.
func TestDialIgnoresSynAckForAnotherSyn(t *testing.T) {
	acceptor, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("raw acceptor: %v", err)
	}
	defer acceptor.Close()
	client := newServerSocket(t)

	dialed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := client.DialContext(ctx, acceptor.LocalAddr().String())
		if err == nil {
			defer c.Close()
		}
		dialed <- err
	}()

	_ = acceptor.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1500)
	n, addr, err := acceptor.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read SYN: %v", err)
	}
	syn, err := parsePacket(buf[:n])
	if err != nil || syn.typ != packetTypeSyn {
		t.Fatalf("first packet: %+v err=%v, want SYN", syn, err)
	}
	send := func(ack uint16) {
		p := packet{typ: packetTypeState, connID: syn.connID, timestamp: uint32(time.Now().UnixMicro()), seqNr: 4000, ackNr: ack}
		if _, err := acceptor.WriteToUDP(p.marshal(), addr); err != nil {
			t.Fatalf("send STATE: %v", err)
		}
	}
	send(syn.seqNr - 1)
	send(syn.seqNr + 1)
	select {
	case err := <-dialed:
		t.Fatalf("dial completed on a STATE that does not ack our SYN (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}
	send(syn.seqNr)
	select {
	case err := <-dialed:
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SYN-ACK did not complete the dial")
	}
}
