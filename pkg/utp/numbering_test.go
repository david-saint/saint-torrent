package utp

import (
	"io"
	"testing"
	"time"
)

// TestInboundSequenceNumberingMatchesLibutp pins the acceptor's on-wire
// numbering against a libutp-style initiator: the SYN-ACK's seq_nr is our
// first DATA seq_nr, a STATE carries the next seq_nr without consuming it,
// and every packet acks the last in-order seq_nr received. libutp discards a
// STATE whose seq_nr looks old, and delivers DATA only in seq order from the
// SYN-ACK's seq_nr, so any other numbering stalls the stream.
func TestInboundSequenceNumberingMatchesLibutp(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	peer := newRawPeer(t, server)
	syn := packet{connID: 4000, seqNr: 1000}
	synAck := peer.synAck(syn)
	r := synAck.seqNr

	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: r - 1, payload: []byte("hi")})
	c := expectAccept(t, accepted, "libutp-style initiator")
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	got := make([]byte, 2)
	if _, err := io.ReadFull(c, got); err != nil || string(got) != "hi" {
		t.Fatalf("read %q err=%v, want hi", got, err)
	}
	if ack := peer.mustRecv("ack of first DATA"); ack.typ != packetTypeState || ack.seqNr != r || ack.ackNr != syn.seqNr+1 {
		t.Fatalf("ack of first DATA: type=%d seq=%d ack=%d, want STATE seq=%d ack=%d", ack.typ, ack.seqNr, ack.ackNr, r, syn.seqNr+1)
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := c.Write([]byte("yo"))
		writeDone <- err
	}()
	data := peer.mustRecv("our first DATA")
	if data.typ != packetTypeData || data.seqNr != r || data.ackNr != syn.seqNr+1 || string(data.payload) != "yo" {
		t.Fatalf("our first DATA: type=%d seq=%d ack=%d payload=%q, want DATA seq=%d ack=%d yo", data.typ, data.seqNr, data.ackNr, data.payload, r, syn.seqNr+1)
	}
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: r})
	if err := <-writeDone; err != nil {
		t.Fatalf("write: %v", err)
	}

	go func() {
		_, err := c.Write([]byte("z"))
		writeDone <- err
	}()
	if next := peer.mustRecv("our second DATA"); next.typ != packetTypeData || next.seqNr != r+1 {
		t.Fatalf("our second DATA: type=%d seq=%d, want DATA seq=%d", next.typ, next.seqNr, r+1)
	}
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: r + 1})
	if err := <-writeDone; err != nil {
		t.Fatalf("second write: %v", err)
	}

	// Data from the peer is acked by a STATE carrying our next seq_nr.
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 2, ackNr: r + 1, payload: []byte("!")})
	if _, err := io.ReadFull(c, got[:1]); err != nil {
		t.Fatalf("read: %v", err)
	}
	if ack := peer.mustRecv("ack of second peer DATA"); ack.typ != packetTypeState || ack.seqNr != r+2 || ack.ackNr != syn.seqNr+2 {
		t.Fatalf("ack of second peer DATA: type=%d seq=%d ack=%d, want STATE seq=%d ack=%d", ack.typ, ack.seqNr, ack.ackNr, r+2, syn.seqNr+2)
	}
}

// TestInboundLegacyInitiatorNumbering pins compatibility with earlier
// saintTorrent initiators, which treated the SYN-ACK as consuming its seq_nr:
// they ack seq_nr itself and expect our first DATA one past it.
func TestInboundLegacyInitiatorNumbering(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)

	peer := newRawPeer(t, server)
	syn := packet{connID: 4100, seqNr: 3000}
	synAck := peer.synAck(syn)
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr})
	c := expectAccept(t, accepted, "legacy initiator")
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))

	writeDone := make(chan error, 1)
	go func() {
		_, err := c.Write([]byte("hello"))
		writeDone <- err
	}()
	data := peer.mustRecv("our first DATA")
	if data.typ != packetTypeData || data.seqNr != synAck.seqNr+1 || data.ackNr != syn.seqNr {
		t.Fatalf("our first DATA: type=%d seq=%d ack=%d, want DATA seq=%d ack=%d", data.typ, data.seqNr, data.ackNr, synAck.seqNr+1, syn.seqNr)
	}
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: data.seqNr})
	if err := <-writeDone; err != nil {
		t.Fatalf("write: %v", err)
	}
}
