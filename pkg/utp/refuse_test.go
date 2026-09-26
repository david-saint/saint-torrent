package utp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// TestRefuseIncomingResetsSyn checks SetRefuseIncoming at the SYN: a refused
// SYN draws a RESET a libtorrent or libutp initiator matches to its
// connection (the SYN's connection id, acking the SYN's seq_nr) and records
// nothing, even with the unsolicited-RESET budget spent. Once refusal is
// lifted the same initiator is accepted.
func TestRefuseIncomingResetsSyn(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)
	server.SetRefuseIncoming(true)

	peer := newRawPeer(t, server)
	expectRefused := func(syn packet) {
		t.Helper()
		syn.typ = packetTypeSyn
		peer.send(syn)
		p := peer.mustRecv("RESET for refused SYN")
		if p.typ != packetTypeReset || p.connID != syn.connID || p.ackNr != syn.seqNr {
			t.Fatalf("reply to refused SYN: type=%d connID=%d ack=%d, want RESET connID=%d ack=%d", p.typ, p.connID, p.ackNr, syn.connID, syn.seqNr)
		}
		if conns, halfOpen := socketState(server); conns != 0 || halfOpen != 0 {
			t.Fatalf("socket state after refused SYN: conns=%d halfOpen=%d, want 0 and 0", conns, halfOpen)
		}
	}
	expectRefused(packet{connID: 5000, seqNr: 1000})

	// Stray packets cannot use up the RESETs refused initiators rely on.
	server.mu.Lock()
	server.resetWindow = time.Now()
	server.resetsSent = maxResetsPerSecond
	server.mu.Unlock()
	expectRefused(packet{connID: 5002, seqNr: 2000})
	expectNoAccept(t, accepted, "refused SYN")

	server.SetRefuseIncoming(false)
	syn := packet{connID: 5004, seqNr: 3000}
	synAck := peer.synAck(syn)
	peer.send(packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1, payload: []byte("x")})
	expectAccept(t, accepted, "SYN after refusal was lifted")
}

// TestRefuseIncomingResetsPendingHandshake checks an initiator whose SYN was
// answered before refusal began: the packet that would have completed its
// handshake draws a RESET carrying the SYN's connection id (the id its conn
// is keyed by) and never reaches Accept.
func TestRefuseIncomingResetsPendingHandshake(t *testing.T) {
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
	server.SetRefuseIncoming(true)

	data := packet{typ: packetTypeData, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1, payload: []byte("handshake")}
	peer.send(data)
	p := peer.mustRecv("RESET for refused handshake")
	if p.typ != packetTypeReset || p.connID != syn.connID || p.ackNr != data.seqNr {
		t.Fatalf("reply to refused handshake: type=%d connID=%d ack=%d, want RESET connID=%d ack=%d", p.typ, p.connID, p.ackNr, syn.connID, data.seqNr)
	}
	expectNoAccept(t, accepted, "refused handshake")
	if conns, halfOpen := socketState(server); conns != 0 || halfOpen != 0 {
		t.Fatalf("socket state after refused handshake: conns=%d halfOpen=%d, want 0 and 0", conns, halfOpen)
	}
}

// TestRefuseIncomingFailsDialAtOnce runs refusal between two Sockets: a dial
// to a refusing socket fails with a reset rather than running into its
// timeout, while connections it already accepted keep working and its own
// dials still connect.
func TestRefuseIncomingFailsDialAtOnce(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	serverLn := server.Listen()
	defer serverLn.Close()
	serverAccepted := acceptAsync(serverLn)

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()
	clientLn := client.Listen()
	defer clientLn.Close()
	clientAccepted := acceptAsync(clientLn)

	dial := func(from, to *Socket) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return from.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", to.Port()))
	}

	established, err := dial(client, server)
	if err != nil {
		t.Fatalf("dial before refusal: %v", err)
	}
	defer established.Close()
	serverSide := expectAccept(t, serverAccepted, "connection dialed before refusal")

	server.SetRefuseIncoming(true)
	if c, err := dial(client, server); !errors.Is(err, errReset) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("dial to a refusing socket: err=%v, want a reset", err)
	}
	expectNoAccept(t, serverAccepted, "refused dial")

	// The connection accepted before refusal still carries data both ways.
	_ = established.SetDeadline(time.Now().Add(2 * time.Second))
	_ = serverSide.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := established.Write([]byte("ping")); err != nil {
		t.Fatalf("write on established conn: %v", err)
	}
	got := make([]byte, 4)
	if _, err := io.ReadFull(serverSide, got); err != nil || string(got) != "ping" {
		t.Fatalf("read on accepted conn: %q err=%v", got, err)
	}
	if _, err := serverSide.Write([]byte("pong")); err != nil {
		t.Fatalf("write on accepted conn: %v", err)
	}
	if _, err := io.ReadFull(established, got); err != nil || string(got) != "pong" {
		t.Fatalf("read on established conn: %q err=%v", got, err)
	}

	// Refusal covers inbound connections only.
	out, err := dial(server, client)
	if err != nil {
		t.Fatalf("dial from a refusing socket: %v", err)
	}
	_ = out.Close()
	expectAccept(t, clientAccepted, "connection dialed by the refusing socket")
}
