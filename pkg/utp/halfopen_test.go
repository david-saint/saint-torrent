package utp

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// rawPeer is a bare UDP endpoint that speaks uTP by hand, for tests that need
// exact control over what reaches a Socket and what it sends back.
type rawPeer struct {
	t      *testing.T
	conn   *net.UDPConn
	target *net.UDPAddr
	buf    []byte
}

func newRawPeer(t *testing.T, target *Socket) *rawPeer {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw peer: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &rawPeer{
		t:      t,
		conn:   conn,
		target: &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(target.Port())},
		buf:    make([]byte, 64*1024),
	}
}

func (r *rawPeer) send(p packet) {
	r.t.Helper()
	p.timestamp = uint32(time.Now().UnixMicro())
	if _, err := r.conn.WriteToUDP(p.marshal(), r.target); err != nil {
		r.t.Fatalf("send packet type %d: %v", p.typ, err)
	}
}

// recv returns the next packet sent to the peer, or ok=false if none arrives
// within d.
func (r *rawPeer) recv(d time.Duration) (packet, bool) {
	r.t.Helper()
	_ = r.conn.SetReadDeadline(time.Now().Add(d))
	n, _, err := r.conn.ReadFromUDP(r.buf)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return packet{}, false
		}
		r.t.Fatalf("raw peer read: %v", err)
	}
	p, err := parsePacket(r.buf[:n])
	if err != nil {
		r.t.Fatalf("raw peer parse: %v", err)
	}
	p.payload = append([]byte(nil), p.payload...)
	return p, true
}

func (r *rawPeer) mustRecv(what string) packet {
	r.t.Helper()
	p, ok := r.recv(2 * time.Second)
	if !ok {
		r.t.Fatalf("no %s within 2s", what)
	}
	return p
}

// mustRecvAfter is mustRecv for a peer that has already been sent DATA up to
// nextSeq-1: retransmits of that DATA, which a loaded machine can send before
// the peer's ack is processed, are skipped.
func (r *rawPeer) mustRecvAfter(what string, nextSeq uint16) packet {
	r.t.Helper()
	for {
		p := r.mustRecv(what)
		if p.typ == packetTypeData && seqLT(p.seqNr, nextSeq) {
			continue
		}
		return p
	}
}

// synAck sends a SYN and returns the SYN-ACK it draws.
func (r *rawPeer) synAck(syn packet) packet {
	r.t.Helper()
	syn.typ = packetTypeSyn
	r.send(syn)
	p := r.mustRecv("SYN-ACK")
	if p.typ != packetTypeState || p.connID != syn.connID || p.ackNr != syn.seqNr {
		r.t.Fatalf("SYN-ACK: type=%d connID=%d ack=%d, want STATE connID=%d ack=%d", p.typ, p.connID, p.ackNr, syn.connID, syn.seqNr)
	}
	return p
}

func acceptAsync(ln *Listener) <-chan net.Conn {
	ch := make(chan net.Conn, 4)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			ch <- c
		}
	}()
	return ch
}

func expectNoAccept(t *testing.T, ch <-chan net.Conn, what string) {
	t.Helper()
	select {
	case c := <-ch:
		_ = c.Close()
		t.Fatalf("%s was handed to Accept", what)
	case <-time.After(150 * time.Millisecond):
	}
}

func expectAccept(t *testing.T, ch <-chan net.Conn, what string) net.Conn {
	t.Helper()
	select {
	case c := <-ch:
		t.Cleanup(func() { _ = c.Close() })
		return c
	case <-time.After(2 * time.Second):
		t.Fatalf("%s was not handed to Accept", what)
		return nil
	}
}

func socketState(s *Socket) (conns, halfOpen int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns), len(s.halfOpen)
}

// TestBlindInboundSessionIsNeverAccepted reproduces the spoofed-source uTP
// session: a sender that never sees our SYN-ACK sends its SYN, in-order DATA
// carrying a payload it wants processed, and a sweep of guessed acks. None of
// it may create a Conn, reach Accept or have its payload buffered; only a
// packet acknowledging the SYN-ACK's random seq_nr completes the handshake.
func TestBlindInboundSessionIsNeverAccepted(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	peer := newRawPeer(t, server)
	syn := packet{connID: 5000, seqNr: 1000}
	synAck := peer.synAck(syn)

	// What a blind sender can do: in-order DATA with a payload, then a sweep
	// of STATEs guessing the ack. The guesses skip the two values that prove
	// receipt of the SYN-ACK, which a blind sender hits only by luck.
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr + 2, payload: []byte("blind-handshake")})
	for guess := 0; guess < 1<<16; guess += 251 {
		ack := uint16(guess)
		if ack == synAck.seqNr || ack == synAck.seqNr-1 {
			continue
		}
		peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: ack})
	}
	peer.send(packet{typ: packetTypeFin, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: synAck.seqNr})

	expectNoAccept(t, accepted, "blind session")
	if p, ok := peer.recv(50 * time.Millisecond); ok {
		t.Fatalf("blind session drew a reply beyond the SYN-ACK: type=%d seq=%d ack=%d", p.typ, p.seqNr, p.ackNr)
	}
	if conns, halfOpen := socketState(server); conns != 0 || halfOpen != 1 {
		t.Fatalf("socket state after blind session: conns=%d halfOpen=%d, want 0 and 1", conns, halfOpen)
	}

	// The genuine initiator, which did see the SYN-ACK, completes the
	// handshake; the blind payload was never buffered.
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr, payload: []byte("real")})
	c := expectAccept(t, accepted, "acknowledged session")
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 4)
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "real" {
		t.Fatalf("read %q, want the acknowledged payload", got)
	}
	if conns, halfOpen := socketState(server); conns != 1 || halfOpen != 0 {
		t.Fatalf("socket state after promotion: conns=%d halfOpen=%d, want 1 and 0", conns, halfOpen)
	}
}

// TestHalfOpenPromotionAcceptsBothAckConventions pins which acks complete an
// inbound handshake: libutp and libtorrent initiators ack the SYN-ACK's
// seq_nr-1, earlier saintTorrent initiators ack seq_nr. Either may arrive on a
// DATA or on a bare STATE.
func TestHalfOpenPromotionAcceptsBothAckConventions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   packetType
		delta uint16 // subtracted from the SYN-ACK seq_nr
	}{
		{"data-libutp", packetTypeData, 1},
		{"data-legacy", packetTypeData, 0},
		{"state-libutp", packetTypeState, 1},
		{"state-legacy", packetTypeState, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, err := NewSocket(0)
			if err != nil {
				t.Fatalf("server socket: %v", err)
			}
			defer server.Close()
			ln := server.Listen()
			defer ln.Close()
			accepted := acceptAsync(ln)

			peer := newRawPeer(t, server)
			syn := packet{connID: 700, seqNr: 42}
			synAck := peer.synAck(syn)
			peer.send(packet{typ: tc.typ, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - tc.delta})
			expectAccept(t, accepted, "acknowledged session")
		})
	}
}

// TestHalfOpenSynFloodIsBounded floods the listener with SYNs on distinct
// connection ids. Each draws exactly one SYN-ACK and a bounded table entry;
// none reaches Accept or allocates a Conn, and a genuine initiator arriving
// after the flood still completes its handshake.
func TestHalfOpenSynFloodIsBounded(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	flood := newRawPeer(t, server)
	const (
		floodSize = maxHalfOpen + 96
		batch     = 32 // small enough that loopback never drops a reply
	)
	for i := 0; i < floodSize; i += batch {
		for j := i; j < i+batch; j++ {
			flood.send(packet{typ: packetTypeSyn, connID: uint16(10000 + 2*j), seqNr: uint16(j)})
		}
		for j := i; j < i+batch; j++ {
			p := flood.mustRecv("SYN-ACK for flooded SYN")
			if p.typ != packetTypeState || p.connID != uint16(10000+2*j) {
				t.Fatalf("flood reply %d: type=%d connID=%d, want one SYN-ACK per SYN", j, p.typ, p.connID)
			}
		}
	}
	if p, ok := flood.recv(50 * time.Millisecond); ok {
		t.Fatalf("flood drew an extra reply: type=%d", p.typ)
	}
	if conns, halfOpen := socketState(server); conns != 0 || halfOpen != maxHalfOpen {
		t.Fatalf("socket state after flood: conns=%d halfOpen=%d, want 0 and %d", conns, halfOpen, maxHalfOpen)
	}
	expectNoAccept(t, accepted, "flooded SYN")

	// The oldest entries were evicted to make room: acknowledging one of
	// them no longer completes anything.
	server.mu.Lock()
	_, firstLive := server.halfOpen[newConnKey(flood.conn.LocalAddr().(*net.UDPAddr), 10000+1)]
	server.mu.Unlock()
	if firstLive {
		t.Fatal("oldest flood entry was not evicted")
	}

	peer := newRawPeer(t, server)
	syn := packet{connID: 3, seqNr: 9}
	synAck := peer.synAck(syn)
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1, payload: []byte("x")})
	expectAccept(t, accepted, "initiator arriving after the flood")
}

// TestHalfOpenEntryExpiresSilently checks that an unacknowledged entry is
// forgotten after halfOpenTimeout: a late acknowledgement no longer promotes
// it, and the expiry itself sends nothing.
func TestHalfOpenEntryExpiresSilently(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	peer := newRawPeer(t, server)
	syn := packet{connID: 900, seqNr: 77}
	synAck := peer.synAck(syn)

	server.mu.Lock()
	for _, h := range server.halfOpen {
		h.created = h.created.Add(-halfOpenTimeout)
	}
	server.mu.Unlock()

	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1, payload: []byte("late")})
	expectNoAccept(t, accepted, "expired half-open entry")
	// The late packet now belongs to no connection, like any stray packet.
	if p := peer.mustRecv("reply to late packet"); p.typ != packetTypeReset {
		t.Fatalf("reply to packet for expired entry: type=%d, want RESET", p.typ)
	}
	if conns, halfOpen := socketState(server); conns != 0 || halfOpen != 0 {
		t.Fatalf("socket state after expiry: conns=%d halfOpen=%d, want 0 and 0", conns, halfOpen)
	}
}

// TestHalfOpenSynWithNewSeqReplacesEntry checks that a SYN reusing a live
// entry's connection id with a different seq_nr is a fresh attempt: it gets a
// SYN-ACK acking its own seq_nr rather than the stale one.
func TestHalfOpenSynWithNewSeqReplacesEntry(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	peer := newRawPeer(t, server)
	stale := peer.synAck(packet{connID: 60, seqNr: 100})
	syn := packet{connID: 60, seqNr: 5000}
	synAck := peer.synAck(syn)
	if conns, halfOpen := socketState(server); conns != 0 || halfOpen != 1 {
		t.Fatalf("socket state: conns=%d halfOpen=%d, want 0 and 1", conns, halfOpen)
	}
	if d := stale.seqNr - synAck.seqNr; d != 0 && d != 1 {
		// Only the new entry's seq_nr may complete the handshake.
		peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: stale.seqNr - 1})
		expectNoAccept(t, accepted, "ack of the replaced SYN-ACK")
	}
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1, payload: []byte("new")})
	c := expectAccept(t, accepted, "fresh attempt")
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 3)
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, []byte("new")) {
		t.Fatalf("read %q err=%v, want new", got, err)
	}
}
