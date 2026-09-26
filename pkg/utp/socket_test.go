package utp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestDialAcceptStreamRoundTrip(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()

	ln := server.Listen()
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	clientConn, err := client.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", server.Port()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not accept uTP connection")
	}
	defer serverConn.Close()

	payload := bytes.Repeat([]byte("utp-fragmentation-"), 512)
	serverDone := make(chan error, 1)
	go func() {
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(serverConn, buf); err != nil {
			serverDone <- err
			return
		}
		if !bytes.Equal(buf, payload) {
			serverDone <- fmt.Errorf("payload mismatch")
			return
		}
		_, err := serverConn.Write([]byte("ack"))
		serverDone <- err
	}()

	if n, err := clientConn.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("client write got n=%d err=%v", n, err)
	}

	ack := make([]byte, 3)
	if _, err := io.ReadFull(clientConn, ack); err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if string(ack) != "ack" {
		t.Fatalf("unexpected ack %q", ack)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("server side failed: %v", err)
	}
}

func TestOutboundConnectionIDsMatchBEP29(t *testing.T) {
	rawPeer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw peer: %v", err)
	}
	defer rawPeer.Close()

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()

	serverDone := make(chan error, 1)
	go func() {
		_ = rawPeer.SetDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1500)
		n, addr, err := rawPeer.ReadFromUDP(buf)
		if err != nil {
			serverDone <- err
			return
		}
		syn, err := parsePacket(buf[:n])
		if err != nil {
			serverDone <- err
			return
		}
		if syn.typ != packetTypeSyn {
			serverDone <- fmt.Errorf("first packet type = %d, want SYN", syn.typ)
			return
		}

		serverSeq := uint16(900)
		state := packet{
			typ:       packetTypeState,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     serverSeq,
			ackNr:     syn.seqNr,
		}
		if _, err := rawPeer.WriteToUDP(state.marshal(), addr); err != nil {
			serverDone <- err
			return
		}

		// The dialer completes the handshake with a STATE of its own. As in
		// libutp, the SYN consumed its seq_nr, the STATE carries the next one
		// without consuming it, and serverSeq-1 acks the SYN-ACK: its seq_nr
		// is the first DATA seq_nr the acceptor will send.
		n, _, err = rawPeer.ReadFromUDP(buf)
		if err != nil {
			serverDone <- err
			return
		}
		ackOfSynAck, err := parsePacket(buf[:n])
		if err != nil {
			serverDone <- err
			return
		}
		if ackOfSynAck.typ != packetTypeState || ackOfSynAck.connID != syn.connID+1 ||
			ackOfSynAck.seqNr != syn.seqNr+1 || ackOfSynAck.ackNr != serverSeq-1 {
			serverDone <- fmt.Errorf("handshake ACK type=%d connID=%d seq=%d ack=%d, want STATE connID=%d seq=%d ack=%d",
				ackOfSynAck.typ, ackOfSynAck.connID, ackOfSynAck.seqNr, ackOfSynAck.ackNr, syn.connID+1, syn.seqNr+1, serverSeq-1)
			return
		}

		n, _, err = rawPeer.ReadFromUDP(buf)
		if err != nil {
			serverDone <- err
			return
		}
		data, err := parsePacket(buf[:n])
		if err != nil {
			serverDone <- err
			return
		}
		if data.typ != packetTypeData {
			serverDone <- fmt.Errorf("second packet type = %d, want DATA", data.typ)
			return
		}
		if data.connID != syn.connID+1 {
			serverDone <- fmt.Errorf("DATA connID = %d, want SYN connID+1 %d", data.connID, syn.connID+1)
			return
		}
		if data.seqNr != syn.seqNr+1 || data.ackNr != serverSeq-1 {
			serverDone <- fmt.Errorf("first DATA seq=%d ack=%d, want seq=%d ack=%d", data.seqNr, data.ackNr, syn.seqNr+1, serverSeq-1)
			return
		}
		if string(data.payload) != "x" {
			serverDone <- fmt.Errorf("DATA payload = %q, want x", data.payload)
			return
		}

		ack := packet{
			typ:       packetTypeState,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     serverSeq,
			ackNr:     data.seqNr,
		}
		_, err = rawPeer.WriteToUDP(ack.marshal(), addr)
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := client.DialContext(ctx, rawPeer.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if n, err := conn.Write([]byte("x")); err != nil || n != 1 {
		t.Fatalf("write got n=%d err=%v", n, err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("raw peer failed: %v", err)
	}
}

func TestInboundConnectionIDsMatchBEP29(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(2 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	syn := packet{typ: packetTypeSyn, connID: 1234, timestamp: uint32(time.Now().UnixMicro()), seqNr: 77}
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}

	buf := make([]byte, 1500)
	n, _, err := rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read STATE: %v", err)
	}
	state, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse STATE: %v", err)
	}
	if state.typ != packetTypeState || state.connID != syn.connID || state.ackNr != syn.seqNr {
		t.Fatalf("unexpected STATE: type=%d connID=%d ack=%d", state.typ, state.connID, state.ackNr)
	}

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     state.seqNr - 1,
		payload:   []byte("hello"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	got := make([]byte, len(data.payload))
	_ = accepted.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read accepted payload: %v", err)
	}
	if !bytes.Equal(got, data.payload) {
		t.Fatalf("accepted payload = %q, want %q", got, data.payload)
	}
}

func TestOutOfOrderFINIsAppliedAfterMissingData(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(2 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	syn := packet{typ: packetTypeSyn, connID: 2200, timestamp: uint32(time.Now().UnixMicro()), seqNr: 10}
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}
	buf := make([]byte, 1500)
	n, _, err := rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read STATE: %v", err)
	}
	state, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse STATE: %v", err)
	}
	// The first DATA acknowledges the SYN-ACK and completes the handshake.
	first := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     state.seqNr - 1,
		payload:   []byte("a"),
	}
	if _, err := rawClient.WriteToUDP(first.marshal(), target); err != nil {
		t.Fatalf("send first DATA: %v", err)
	}
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	fin := packet{
		typ:       packetTypeFin,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 3,
		ackNr:     state.seqNr - 1,
	}
	if _, err := rawClient.WriteToUDP(fin.marshal(), target); err != nil {
		t.Fatalf("send FIN: %v", err)
	}
	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 2,
		ackNr:     state.seqNr - 1,
		payload:   []byte("z"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}

	_ = accepted.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 2)
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != "az" {
		t.Fatalf("payload = %q, want az", got)
	}
	if n, err := accepted.Read(got); err != io.EOF || n != 0 {
		t.Fatalf("second read got n=%d err=%v, want EOF", n, err)
	}
}

func TestDeliverDropsWhenDHTQueueFull(t *testing.T) {
	socket, err := NewSocket(0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer socket.Close()

	pc := socket.DHTConn()
	// Saturate the DHT queue with no consumer draining it.
	for i := 0; i < dhtQueueSize; i++ {
		pc.incoming <- udpPacket{data: []byte("x")}
	}

	// deliver runs on the shared UDP read loop, so it must drop rather than
	// block when the queue is full.
	done := make(chan struct{})
	go func() {
		pc.deliver(udpPacket{data: []byte("dropme")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deliver blocked on a full DHT queue")
	}
	if got := pc.DroppedPackets(); got != 1 {
		t.Fatalf("DroppedPackets() = %d, want 1", got)
	}
}

func TestUTPHandshakeSurvivesFullDHTQueue(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	// Saturate the DHT queue and leave it undrained so every further non-uTP
	// packet the read loop delivers must be dropped, never block.
	pc := server.DHTConn()
	for i := 0; i < dhtQueueSize; i++ {
		pc.incoming <- udpPacket{data: []byte("x")}
	}

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(2 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}

	// Non-uTP traffic that would head-of-line block a blocking deliver, sent
	// ahead of the uTP SYN on the same ordered loopback path.
	for i := 0; i < 8; i++ {
		if _, err := rawClient.WriteToUDP([]byte("d1:t2:aa1:y1:qe"), target); err != nil {
			t.Fatalf("write dht packet: %v", err)
		}
	}
	syn := packet{typ: packetTypeSyn, connID: 4242, timestamp: uint32(time.Now().UnixMicro()), seqNr: 5}
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}

	// A blocked read loop would never emit the STATE reply, tripping the deadline.
	buf := make([]byte, 1500)
	n, _, err := rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read STATE (read loop head-of-line blocked?): %v", err)
	}
	state, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse STATE: %v", err)
	}
	if state.typ != packetTypeState || state.connID != syn.connID || state.ackNr != syn.seqNr {
		t.Fatalf("unexpected STATE: type=%d connID=%d ack=%d", state.typ, state.connID, state.ackNr)
	}
}

func TestLargeTransferWithCoalescedAcks(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()

	ln := server.Listen()
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		if conn, err := ln.Accept(); err == nil {
			accepted <- conn
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientConn, err := client.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", server.Port()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	var serverConn net.Conn
	select {
	case serverConn = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not accept uTP connection")
	}
	defer serverConn.Close()

	// Push several send windows' worth of data in one direction (the shape of
	// a real download) so the sender slides its window many times. This
	// exercises ack coalescing on the receiver, the seq-ordered ack walk over
	// a full window of waiters on the sender, and the pooled marshal buffers.
	const size = 4 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*7 + 3)
	}

	readErr := make(chan error, 1)
	go func() {
		got := make([]byte, size)
		if _, err := io.ReadFull(serverConn, got); err != nil {
			readErr <- err
			return
		}
		if !bytes.Equal(got, payload) {
			readErr <- fmt.Errorf("payload mismatch")
			return
		}
		readErr <- nil
	}()

	if n, err := clientConn.Write(payload); err != nil || n != size {
		t.Fatalf("client write got n=%d err=%v", n, err)
	}

	select {
	case err := <-readErr:
		if err != nil {
			t.Fatalf("receiver: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("receiver did not drain payload")
	}
}

func TestListenerCloseClosesQueuedConns(t *testing.T) {
	s, err := NewSocket(0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer s.Close()

	ln := s.Listen()

	// A conn accepted into the queue but never handed to a caller must be
	// closed by Listener.Close rather than lingering with its receive buffer.
	queued := newInboundConn(s, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 65000}, 4242, 1, 100)
	if !ln.enqueue(queued) {
		t.Fatal("failed to enqueue conn")
	}

	if err := ln.Close(); err != nil {
		t.Fatalf("listener close: %v", err)
	}

	if _, err := queued.Read(make([]byte, 1)); err == nil {
		t.Fatal("queued conn was not closed by Listener.Close")
	}
}

func TestSocketDemuxesDHTPackets(t *testing.T) {
	socket, err := NewSocket(0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer socket.Close()

	raw, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw udp: %v", err)
	}
	defer raw.Close()

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(socket.Port())}
	dhtPayload := []byte("d1:t2:aa1:y1:qe")
	if _, err := raw.WriteToUDP(dhtPayload, target); err != nil {
		t.Fatalf("write dht packet: %v", err)
	}

	buf := make([]byte, 64)
	n, addr, err := socket.DHTConn().ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read dht packet: %v", err)
	}
	if !bytes.Equal(buf[:n], dhtPayload) {
		t.Fatalf("DHT payload mismatch: got %q want %q", buf[:n], dhtPayload)
	}
	if addr == nil || addr.Port != raw.LocalAddr().(*net.UDPAddr).Port {
		t.Fatalf("unexpected source address %v", addr)
	}

	syn := packet{typ: packetTypeSyn, connID: 42, timestamp: socket.nowMicros(), seqNr: 7}.marshal()
	if _, err := raw.WriteToUDP(syn, target); err != nil {
		t.Fatalf("write uTP packet: %v", err)
	}

	delivered := make(chan []byte, 1)
	go func() {
		n, _, err := socket.DHTConn().ReadFromUDP(buf)
		if err == nil {
			delivered <- append([]byte(nil), buf[:n]...)
		}
	}()

	select {
	case pkt := <-delivered:
		t.Fatalf("uTP packet leaked into DHT path: %x", pkt)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCollidingSynDoesNotHijackOutboundConn(t *testing.T) {
	rawPeer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw peer: %v", err)
	}
	defer rawPeer.Close()
	_ = rawPeer.SetDeadline(time.Now().Add(5 * time.Second))

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()
	ln := client.Listen()
	defer ln.Close()

	const peerSeq = uint16(900)
	type handshake struct {
		syn  packet
		addr *net.UDPAddr
		err  error
	}
	handshakeCh := make(chan handshake, 1)
	go func() {
		buf := make([]byte, 1500)
		n, addr, err := rawPeer.ReadFromUDP(buf)
		if err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		syn, err := parsePacket(buf[:n])
		if err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		if syn.typ != packetTypeSyn {
			handshakeCh <- handshake{err: fmt.Errorf("first packet type = %d, want SYN", syn.typ)}
			return
		}
		state := packet{
			typ:       packetTypeState,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     peerSeq,
			ackNr:     syn.seqNr,
		}
		if _, err := rawPeer.WriteToUDP(state.marshal(), addr); err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		if err := readHandshakeAck(rawPeer, buf); err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		handshakeCh <- handshake{syn: syn, addr: addr}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialed, err := client.DialContext(ctx, rawPeer.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer dialed.Close()
	conn := dialed.(*Conn)

	hs := <-handshakeCh
	if hs.err != nil {
		t.Fatalf("raw peer handshake: %v", hs.err)
	}

	conn.mu.Lock()
	localSeq, remoteSeq := conn.localSeq, conn.remoteSeq
	conn.mu.Unlock()

	acceptCh := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			acceptCh <- c
		}
	}()

	// A SYN whose connID is the outbound conn's recvID-1 derives the very key
	// the established outbound conn is registered under.
	stray := packet{
		typ:       packetTypeSyn,
		connID:    conn.recvID - 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     40000,
	}
	if _, err := rawPeer.WriteToUDP(stray.marshal(), hs.addr); err != nil {
		t.Fatalf("send colliding SYN: %v", err)
	}

	select {
	case c := <-acceptCh:
		t.Fatalf("colliding SYN was accepted as an inbound conn (is the dialed conn: %v)", c == net.Conn(conn))
	case <-time.After(200 * time.Millisecond):
	}

	buf := make([]byte, 1500)
	n, _, err := rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read reply to colliding SYN: %v", err)
	}
	reply, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}
	if reply.typ != packetTypeReset || reply.connID != stray.connID {
		t.Fatalf("reply to colliding SYN: type=%d connID=%d, want RESET connID=%d", reply.typ, reply.connID, stray.connID)
	}

	conn.mu.Lock()
	gotLocal, gotRemote := conn.localSeq, conn.remoteSeq
	conn.mu.Unlock()
	if gotLocal != localSeq || gotRemote != remoteSeq {
		t.Fatalf("sequence state after colliding SYN: localSeq %d->%d remoteSeq %d->%d", localSeq, gotLocal, remoteSeq, gotRemote)
	}

	// The refused SYN must not have registered anything: the socket still
	// holds exactly the dialed conn. The read loop mutates this map, so the
	// check takes the socket lock.
	client.mu.Lock()
	registered := len(client.conns)
	var only *Conn
	for _, c := range client.conns {
		only = c
	}
	client.mu.Unlock()
	if registered != 1 || only != conn {
		t.Fatalf("socket conns after colliding SYN: len=%d onlyIsDialed=%v, want len=1 holding the dialed conn", registered, only == conn)
	}

	writeErr := make(chan error, 1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		_, err := conn.Write([]byte("ping"))
		writeErr <- err
	}()

	n, _, err = rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read DATA after colliding SYN: %v", err)
	}
	data, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse DATA: %v", err)
	}
	if data.typ != packetTypeData || data.connID != hs.syn.connID+1 {
		t.Fatalf("DATA after colliding SYN: type=%d connID=%d, want DATA connID=%d", data.typ, data.connID, hs.syn.connID+1)
	}
	if data.seqNr != localSeq || data.ackNr != remoteSeq {
		t.Fatalf("DATA seq=%d ack=%d, want seq=%d ack=%d", data.seqNr, data.ackNr, localSeq, remoteSeq)
	}
	if string(data.payload) != "ping" {
		t.Fatalf("DATA payload = %q, want ping", data.payload)
	}
	ack := packet{
		typ:       packetTypeState,
		connID:    hs.syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     peerSeq,
		ackNr:     data.seqNr,
	}
	if _, err := rawPeer.WriteToUDP(ack.marshal(), hs.addr); err != nil {
		t.Fatalf("send DATA ack: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write after colliding SYN: %v", err)
	}

	inbound := packet{
		typ:       packetTypeData,
		connID:    hs.syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     peerSeq, // the SYN-ACK seq_nr is the first DATA seq_nr
		ackNr:     data.seqNr,
		payload:   []byte("pong"),
	}
	if _, err := rawPeer.WriteToUDP(inbound.marshal(), hs.addr); err != nil {
		t.Fatalf("send inbound DATA: %v", err)
	}
	got := make([]byte, len(inbound.payload))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read after colliding SYN: %v", err)
	}
	if !bytes.Equal(got, inbound.payload) {
		t.Fatalf("read %q, want %q", got, inbound.payload)
	}
}

func TestSynRetransmitReusesPendingInboundConn(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(5 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	syn := packet{typ: packetTypeSyn, connID: 4321, timestamp: uint32(time.Now().UnixMicro()), seqNr: 55}
	buf := make([]byte, 1500)
	readState := func(what string) packet {
		t.Helper()
		n, _, err := rawClient.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
		p, err := parsePacket(buf[:n])
		if err != nil {
			t.Fatalf("parse %s: %v", what, err)
		}
		if p.typ != packetTypeState || p.connID != syn.connID {
			t.Fatalf("%s: type=%d connID=%d, want STATE connID=%d", what, p.typ, p.connID, syn.connID)
		}
		return p
	}

	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}
	first := readState("STATE for first SYN")
	if first.ackNr != syn.seqNr {
		t.Fatalf("first STATE ack = %d, want %d", first.ackNr, syn.seqNr)
	}
	// The conn is still half-open. A retransmit arriving in that window must
	// be answered with the identical SYN-ACK: no reset (readState rejects any
	// other packet type), no fresh sequence number, no conn handed out.
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("resend SYN while half-open: %v", err)
	}
	queued := readState("STATE for SYN retransmit while half-open")
	if queued.seqNr != first.seqNr || queued.ackNr != first.ackNr {
		t.Fatalf("STATE for retransmit while half-open seq=%d ack=%d, want seq=%d ack=%d", queued.seqNr, queued.ackNr, first.seqNr, first.ackNr)
	}

	acceptCh := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			acceptCh <- c
		}
	}()
	select {
	case <-acceptCh:
		t.Fatal("half-open conn was handed to Accept before its SYN-ACK was acknowledged")
	case <-time.After(100 * time.Millisecond):
	}

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     first.seqNr - 1,
		payload:   []byte("hello"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}
	var accepted net.Conn
	select {
	case accepted = <-acceptCh:
	case <-time.After(2 * time.Second):
		t.Fatal("acknowledging DATA did not hand the conn to Accept")
	}
	defer accepted.Close()

	got := make([]byte, len(data.payload))
	_ = accepted.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read accepted payload: %v", err)
	}
	if !bytes.Equal(got, data.payload) {
		t.Fatalf("accepted payload = %q, want %q", got, data.payload)
	}
	if ack := readState("STATE for DATA"); ack.ackNr != data.seqNr {
		t.Fatalf("DATA ack = %d, want %d", ack.ackNr, data.seqNr)
	}

	// A late SYN retransmit arriving after data is re-acked by the promoted
	// conn, must not rewind remoteSeq and must not produce a second conn.
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("resend SYN after data: %v", err)
	}
	third := readState("STATE for late SYN retransmit")
	if third.seqNr != first.seqNr || third.ackNr != data.seqNr {
		t.Fatalf("late retransmit STATE seq=%d ack=%d, want seq=%d ack=%d", third.seqNr, third.ackNr, first.seqNr, data.seqNr)
	}
	select {
	case <-acceptCh:
		t.Fatal("SYN retransmit produced a second accepted conn")
	case <-time.After(100 * time.Millisecond):
	}

	more := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 2,
		ackNr:     first.seqNr - 1,
		payload:   []byte("world"),
	}
	if _, err := rawClient.WriteToUDP(more.marshal(), target); err != nil {
		t.Fatalf("send second DATA: %v", err)
	}
	got = make([]byte, len(more.payload))
	_ = accepted.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read second payload: %v", err)
	}
	if !bytes.Equal(got, more.payload) {
		t.Fatalf("second payload = %q, want %q", got, more.payload)
	}
}

func TestSynRetransmitAfterListenerCloseKeepsInboundConn(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(5 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	syn := packet{typ: packetTypeSyn, connID: 6543, timestamp: uint32(time.Now().UnixMicro()), seqNr: 21}
	buf := make([]byte, 1500)
	readPacket := func(what string) packet {
		t.Helper()
		n, _, err := rawClient.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
		p, err := parsePacket(buf[:n])
		if err != nil {
			t.Fatalf("parse %s: %v", what, err)
		}
		return p
	}

	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}
	first := readPacket("STATE for first SYN")
	if first.typ != packetTypeState || first.connID != syn.connID || first.ackNr != syn.seqNr {
		t.Fatalf("first reply: type=%d connID=%d ack=%d, want STATE connID=%d ack=%d", first.typ, first.connID, first.ackNr, syn.connID, syn.seqNr)
	}
	// A bare STATE acknowledging the SYN-ACK completes the handshake.
	handshakeAck := packet{
		typ:       packetTypeState,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     first.seqNr - 1,
	}
	if _, err := rawClient.WriteToUDP(handshakeAck.marshal(), target); err != nil {
		t.Fatalf("send handshake ACK: %v", err)
	}

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	// TorrentManager.Close closes the uTP listener while sessions still hold
	// the conns it handed out. A SYN retransmit arriving after that must still
	// be routed to the inbound conn: a RESET carries the initiator's connID,
	// so at the peer it keys to the connection it opened and kills a live
	// stream.
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("resend SYN after listener close: %v", err)
	}
	second := readPacket("reply to SYN retransmit after listener close")
	if second.typ == packetTypeReset {
		t.Fatal("SYN retransmit after listener close was answered with a RESET")
	}
	if second.typ != packetTypeState || second.connID != syn.connID {
		t.Fatalf("retransmit reply: type=%d connID=%d, want STATE connID=%d", second.typ, second.connID, syn.connID)
	}
	if second.seqNr != first.seqNr || second.ackNr != first.ackNr {
		t.Fatalf("retransmit STATE seq=%d ack=%d, want seq=%d ack=%d", second.seqNr, second.ackNr, first.seqNr, first.ackNr)
	}

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     first.seqNr - 1,
		payload:   []byte("after-close"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA after listener close: %v", err)
	}
	got := make([]byte, len(data.payload))
	_ = accepted.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read accepted payload after listener close: %v", err)
	}
	if !bytes.Equal(got, data.payload) {
		t.Fatalf("accepted payload = %q, want %q", got, data.payload)
	}
}

// TestSynMatchingOutboundRecvIDOpensNewInboundConn pins the rule that a SYN's
// key is derived from connID+1 before any map lookup. A SYN whose connID equals
// an outbound conn's recvID would, keyed directly, land on that established
// conn; it must instead open a brand-new inbound conn one id higher.
func TestSynMatchingOutboundRecvIDOpensNewInboundConn(t *testing.T) {
	rawPeer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw peer: %v", err)
	}
	defer rawPeer.Close()
	_ = rawPeer.SetDeadline(time.Now().Add(5 * time.Second))

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()
	ln := client.Listen()
	defer ln.Close()

	const peerSeq = uint16(1200)
	type handshake struct {
		syn  packet
		addr *net.UDPAddr
		err  error
	}
	handshakeCh := make(chan handshake, 1)
	go func() {
		buf := make([]byte, 1500)
		n, addr, err := rawPeer.ReadFromUDP(buf)
		if err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		syn, err := parsePacket(buf[:n])
		if err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		if syn.typ != packetTypeSyn {
			handshakeCh <- handshake{err: fmt.Errorf("first packet type = %d, want SYN", syn.typ)}
			return
		}
		state := packet{
			typ:       packetTypeState,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     peerSeq,
			ackNr:     syn.seqNr,
		}
		if _, err := rawPeer.WriteToUDP(state.marshal(), addr); err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		if err := readHandshakeAck(rawPeer, buf); err != nil {
			handshakeCh <- handshake{err: err}
			return
		}
		handshakeCh <- handshake{syn: syn, addr: addr}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dialed, err := client.DialContext(ctx, rawPeer.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer dialed.Close()
	conn := dialed.(*Conn)

	hs := <-handshakeCh
	if hs.err != nil {
		t.Fatalf("raw peer handshake: %v", hs.err)
	}

	conn.mu.Lock()
	localSeq, remoteSeq := conn.localSeq, conn.remoteSeq
	conn.mu.Unlock()

	syn := packet{
		typ:       packetTypeSyn,
		connID:    conn.recvID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     31000,
	}
	if _, err := rawPeer.WriteToUDP(syn.marshal(), hs.addr); err != nil {
		t.Fatalf("send SYN on the outbound conn's recvID: %v", err)
	}

	buf := make([]byte, 1500)
	n, _, err := rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read reply to SYN: %v", err)
	}
	reply, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}
	if reply.typ != packetTypeState || reply.connID != syn.connID || reply.ackNr != syn.seqNr {
		t.Fatalf("reply to SYN: type=%d connID=%d ack=%d, want STATE connID=%d ack=%d", reply.typ, reply.connID, reply.ackNr, syn.connID, syn.seqNr)
	}
	handshakeAck := packet{
		typ:       packetTypeState,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     reply.seqNr - 1,
	}
	if _, err := rawPeer.WriteToUDP(handshakeAck.marshal(), hs.addr); err != nil {
		t.Fatalf("send handshake ACK: %v", err)
	}

	inbound, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer inbound.Close()
	inboundConn := inbound.(*Conn)
	if inboundConn == conn {
		t.Fatal("SYN on the outbound conn's recvID was accepted as the outbound conn")
	}
	if !inboundConn.inbound || inboundConn.recvID != syn.connID+1 {
		t.Fatalf("accepted conn: inbound=%v recvID=%d, want inbound recvID=%d", inboundConn.inbound, inboundConn.recvID, syn.connID+1)
	}

	conn.mu.Lock()
	gotLocal, gotRemote := conn.localSeq, conn.remoteSeq
	conn.mu.Unlock()
	if gotLocal != localSeq || gotRemote != remoteSeq {
		t.Fatalf("outbound sequence state after SYN: localSeq %d->%d remoteSeq %d->%d", localSeq, gotLocal, remoteSeq, gotRemote)
	}

	writeErr := make(chan error, 1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		_, err := conn.Write([]byte("ping"))
		writeErr <- err
	}()

	n, _, err = rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read DATA from the outbound conn: %v", err)
	}
	data, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse DATA: %v", err)
	}
	if data.typ != packetTypeData || data.connID != hs.syn.connID+1 {
		t.Fatalf("DATA: type=%d connID=%d, want DATA connID=%d", data.typ, data.connID, hs.syn.connID+1)
	}
	if data.seqNr != localSeq || data.ackNr != remoteSeq {
		t.Fatalf("DATA seq=%d ack=%d, want seq=%d ack=%d", data.seqNr, data.ackNr, localSeq, remoteSeq)
	}
	if string(data.payload) != "ping" {
		t.Fatalf("DATA payload = %q, want ping", data.payload)
	}
	ack := packet{
		typ:       packetTypeState,
		connID:    hs.syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     peerSeq,
		ackNr:     data.seqNr,
	}
	if _, err := rawPeer.WriteToUDP(ack.marshal(), hs.addr); err != nil {
		t.Fatalf("send DATA ack: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write on the outbound conn: %v", err)
	}

	toOutbound := packet{
		typ:       packetTypeData,
		connID:    hs.syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     peerSeq, // the SYN-ACK seq_nr is the first DATA seq_nr
		ackNr:     data.seqNr,
		payload:   []byte("pong"),
	}
	if _, err := rawPeer.WriteToUDP(toOutbound.marshal(), hs.addr); err != nil {
		t.Fatalf("send DATA to the outbound conn: %v", err)
	}
	got := make([]byte, len(toOutbound.payload))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read on the outbound conn: %v", err)
	}
	if !bytes.Equal(got, toOutbound.payload) {
		t.Fatalf("read %q, want %q", got, toOutbound.payload)
	}
}

// TestSynMatchingInboundRecvIDOpensSecondInboundConn is the mirror of the
// outbound case: a SYN whose connID equals an existing inbound conn's recvID
// keys one higher and opens a second, distinct inbound conn instead of being
// routed into the first one.
func TestSynMatchingInboundRecvIDOpensSecondInboundConn(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()

	rawClient, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw client: %v", err)
	}
	defer rawClient.Close()
	_ = rawClient.SetDeadline(time.Now().Add(5 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	buf := make([]byte, 1500)
	readState := func(what string, wantConnID uint16) packet {
		t.Helper()
		n, _, err := rawClient.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
		p, err := parsePacket(buf[:n])
		if err != nil {
			t.Fatalf("parse %s: %v", what, err)
		}
		if p.typ != packetTypeState || p.connID != wantConnID {
			t.Fatalf("%s: type=%d connID=%d, want STATE connID=%d", what, p.typ, p.connID, wantConnID)
		}
		return p
	}

	firstSyn := packet{typ: packetTypeSyn, connID: 7000, timestamp: uint32(time.Now().UnixMicro()), seqNr: 11}
	if _, err := rawClient.WriteToUDP(firstSyn.marshal(), target); err != nil {
		t.Fatalf("send first SYN: %v", err)
	}
	firstState := readState("STATE for first SYN", firstSyn.connID)
	sendHandshakeAck(t, rawClient, target, firstSyn, firstState)

	acceptedA, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	defer acceptedA.Close()
	connA := acceptedA.(*Conn)
	if connA.recvID != firstSyn.connID+1 {
		t.Fatalf("first inbound recvID = %d, want %d", connA.recvID, firstSyn.connID+1)
	}

	secondSyn := packet{typ: packetTypeSyn, connID: connA.recvID, timestamp: uint32(time.Now().UnixMicro()), seqNr: 12000}
	if _, err := rawClient.WriteToUDP(secondSyn.marshal(), target); err != nil {
		t.Fatalf("send SYN on the first conn's recvID: %v", err)
	}
	secondState := readState("STATE for SYN on the first conn's recvID", secondSyn.connID)
	if secondState.ackNr != secondSyn.seqNr {
		t.Fatalf("second STATE ack = %d, want %d", secondState.ackNr, secondSyn.seqNr)
	}
	sendHandshakeAck(t, rawClient, target, secondSyn, secondState)

	acceptedB, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept second: %v", err)
	}
	defer acceptedB.Close()
	connB := acceptedB.(*Conn)
	if connB == connA {
		t.Fatal("SYN on the first conn's recvID was routed into the first conn")
	}
	if !connB.inbound || connB.recvID != secondSyn.connID+1 {
		t.Fatalf("second accepted conn: inbound=%v recvID=%d, want inbound recvID=%d", connB.inbound, connB.recvID, secondSyn.connID+1)
	}

	// The first conn is untouched and still carries data on its own recvID.
	data := packet{
		typ:       packetTypeData,
		connID:    connA.recvID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     firstSyn.seqNr + 1,
		ackNr:     firstState.seqNr - 1,
		payload:   []byte("hello"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA to the first conn: %v", err)
	}
	got := make([]byte, len(data.payload))
	_ = acceptedA.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(acceptedA, got); err != nil {
		t.Fatalf("read first conn payload: %v", err)
	}
	if !bytes.Equal(got, data.payload) {
		t.Fatalf("first conn payload = %q, want %q", got, data.payload)
	}
}

// readHandshakeAck reads the STATE a dialer sends once its SYN is
// acknowledged, the third leg of the uTP handshake.
func readHandshakeAck(conn *net.UDPConn, buf []byte) error {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return err
	}
	p, err := parsePacket(buf[:n])
	if err != nil {
		return err
	}
	if p.typ != packetTypeState {
		return fmt.Errorf("handshake ACK type = %d, want STATE", p.typ)
	}
	return nil
}

// sendHandshakeAck completes a raw inbound handshake: a bare STATE that
// acknowledges synAck is what promotes the half-open conn to Accept.
func sendHandshakeAck(t *testing.T, conn *net.UDPConn, target *net.UDPAddr, syn, synAck packet) {
	t.Helper()
	ack := packet{
		typ:       packetTypeState,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     synAck.seqNr - 1,
	}
	if _, err := conn.WriteToUDP(ack.marshal(), target); err != nil {
		t.Fatalf("send handshake ACK: %v", err)
	}
}

// faultyReader is a Socket.readFrom that fails with each error queued in errs
// before reading from conn, and reports every call on calls.
type faultyReader struct {
	conn  *net.UDPConn
	errs  chan error
	calls chan readCall
}

type readCall struct {
	at     time.Time
	failed bool
}

func (r *faultyReader) readFrom(b []byte) (int, *net.UDPAddr, error) {
	call := readCall{at: time.Now()}
	var err error
	select {
	case err = <-r.errs:
		call.failed = true
	default:
	}
	select {
	case r.calls <- call:
	default:
	}
	if err != nil {
		return 0, nil, err
	}
	return r.conn.ReadFromUDP(b)
}

// nextCall returns the read loop's next call to readFrom.
func (r *faultyReader) nextCall(t *testing.T) readCall {
	t.Helper()
	select {
	case c := <-r.calls:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("read loop stopped reading")
		return readCall{}
	}
}

// startFaultySocket runs a Socket on a loopback UDP socket whose reads first
// fail with errs, in order. The returned channel is closed when the read loop
// exits.
func startFaultySocket(t *testing.T, errs ...error) (*Socket, *faultyReader, <-chan struct{}) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("udp socket: %v", err)
	}
	r := &faultyReader{conn: conn, errs: make(chan error, 16), calls: make(chan readCall, 1024)}
	for _, e := range errs {
		r.errs <- e
	}
	s := newSocket(conn, r.readFrom)
	exited := make(chan struct{})
	go func() {
		s.readLoop()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = s.Close()
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Error("read loop did not exit after Close")
		}
	})
	return s, r, exited
}

func transientReadError(errno syscall.Errno) error {
	return &net.OpError{Op: "read", Net: "udp", Err: os.NewSyscallError("recvfrom", errno)}
}

func socketClosed(s *Socket) bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// TestReadLoopSurvivesTransientReadErrors checks that a read error other than
// a closed socket (ENOBUFS or ENOMEM under memory pressure, an ICMP-induced
// error) no longer closes the shared socket, which took every uTP conn, the
// listener and the DHT down with it until restart. uTP and DHT datagrams
// that arrive afterwards are still handled.
func TestReadLoopSurvivesTransientReadErrors(t *testing.T) {
	s, r, exited := startFaultySocket(t,
		transientReadError(syscall.ENOBUFS),
		transientReadError(syscall.ENOMEM),
		transientReadError(syscall.ECONNREFUSED),
	)
	ln := s.Listen()
	peer := newRawPeer(t, s)

	dhtPayload := []byte("d1:t2:aa1:y1:qe")
	if _, err := peer.conn.WriteToUDP(dhtPayload, peer.target); err != nil {
		t.Fatalf("send DHT datagram: %v", err)
	}
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 64)
		if n, _, err := s.DHTConn().ReadFromUDP(buf); err == nil {
			got <- buf[:n]
		}
	}()
	select {
	case b := <-got:
		if !bytes.Equal(b, dhtPayload) {
			t.Fatalf("DHT datagram %q, want %q", b, dhtPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DHT datagram was not delivered after transient read errors")
	}

	accepted := acceptAsync(ln)
	syn := packet{connID: 7300, seqNr: 40}
	synAck := peer.synAck(syn)
	peer.send(packet{typ: packetTypeState, connID: syn.connID + 1, seqNr: syn.seqNr + 1, ackNr: synAck.seqNr - 1})
	expectAccept(t, accepted, "uTP conn after transient read errors")

	failed := 0
	for len(r.calls) > 0 {
		if (<-r.calls).failed {
			failed++
		}
	}
	if failed != 3 {
		t.Fatalf("read loop saw %d failed reads, want 3", failed)
	}
	if socketClosed(s) {
		t.Fatal("a transient read error closed the socket")
	}
	select {
	case <-exited:
		t.Fatal("a transient read error ended the read loop")
	default:
	}
}

// TestReadLoopStopsWhenSocketCloses checks that the read loop still ends at
// once when the socket is gone: on net.ErrClosed from a UDP socket closed
// underneath it, and on Close while it waits out a read-error backoff.
func TestReadLoopStopsWhenSocketCloses(t *testing.T) {
	t.Run("ErrClosed", func(t *testing.T) {
		s, _, exited := startFaultySocket(t, &net.OpError{Op: "read", Net: "udp", Err: net.ErrClosed})
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Fatal("read loop kept running after net.ErrClosed")
		}
		// Nothing more can be read, so the Socket is closed rather than
		// leaving DHT reads and Accept waiting on it forever.
		if !socketClosed(s) {
			t.Fatal("socket left open after its UDP socket was closed")
		}
		if _, _, err := s.DHTConn().ReadFromUDP(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("DHT read after net.ErrClosed: %v, want %v", err, net.ErrClosed)
		}
	})

	t.Run("Close during backoff", func(t *testing.T) {
		// The pause after the sixth consecutive failure reaches
		// readErrorBackoffMax, so the seventh is followed by a long wait.
		const failures = 7
		errs := make([]error, failures)
		for i := range errs {
			errs[i] = transientReadError(syscall.ENOBUFS)
		}
		s, r, exited := startFaultySocket(t, errs...)
		for seen := 0; seen < failures; {
			if r.nextCall(t).failed {
				seen++
			}
		}
		closedAt := time.Now()
		_ = s.Close()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Fatal("read loop did not exit after Close")
		}
		// Had the wait ignored Close, the loop would have read once more
		// after it before noticing.
		for len(r.calls) > 0 {
			if c := <-r.calls; c.at.After(closedAt) {
				t.Fatalf("read loop read again %v after Close instead of ending its backoff", c.at.Sub(closedAt))
			}
		}
	})
}

// TestReadErrorBackoffIsBounded checks the pause after consecutive failed
// reads: readErrorBackoffMin at first, doubling up to readErrorBackoffMax,
// and back to the minimum once a read succeeds.
func TestReadErrorBackoffIsBounded(t *testing.T) {
	want := []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
		160 * time.Millisecond, 250 * time.Millisecond, 250 * time.Millisecond,
	}
	var d time.Duration
	for i, w := range want {
		if d = nextReadErrorBackoff(d); d != w {
			t.Fatalf("pause after failure %d = %v, want %v", i+1, d, w)
		}
	}

	errs := make([]error, len(want))
	for i := range errs {
		errs[i] = transientReadError(syscall.ENOBUFS)
	}
	s, r, _ := startFaultySocket(t, errs...)
	prev := r.nextCall(t)
	for i, w := range want {
		next := r.nextCall(t)
		if !prev.failed {
			t.Fatalf("call %d succeeded before the queued failures ran out", i+1)
		}
		// Timers never fire early, so each gap is at least its pause. The
		// last pause would be twice readErrorBackoffMax without the cap.
		gap := next.at.Sub(prev.at)
		if gap < w {
			t.Fatalf("read %v after failure %d, want a pause of at least %v", gap, i+1, w)
		}
		if i == len(want)-1 && gap >= 2*readErrorBackoffMax {
			t.Fatalf("read %v after failure %d, want the pause capped at %v", gap, i+1, readErrorBackoffMax)
		}
		prev = next
	}
	if prev.failed {
		t.Fatal("read after the queued failures failed")
	}

	// prev is the loop's blocking read. A datagram completes it, and the
	// failure queued before that follows a success, so it pauses the minimum
	// again rather than readErrorBackoffMax.
	r.errs <- transientReadError(syscall.ENOBUFS)
	if _, err := r.conn.WriteToUDP([]byte("d1:ae"), r.conn.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatalf("send datagram: %v", err)
	}
	failedCall := r.nextCall(t)
	next := r.nextCall(t)
	if !failedCall.failed || next.failed {
		t.Fatalf("calls after the datagram failed=%v,%v, want true,false", failedCall.failed, next.failed)
	}
	if gap := next.at.Sub(failedCall.at); gap < readErrorBackoffMin || gap >= readErrorBackoffMax {
		t.Fatalf("read %v after a failure that followed a success, want a pause of %v", gap, readErrorBackoffMin)
	}
	if socketClosed(s) {
		t.Fatal("read errors closed the socket")
	}
}

// dialRaw dials peer from client, answers the SYN by hand and returns the
// established conn once its handshake STATE has reached the peer.
func dialRaw(t *testing.T, client *Socket, peer *rawPeer) *Conn {
	t.Helper()
	type dialResult struct {
		c   *Conn
		err error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := client.dialContext(ctx, peer.conn.LocalAddr().(*net.UDPAddr))
		dialed <- dialResult{c, err}
	}()
	syn := peer.mustRecv("SYN")
	if syn.typ != packetTypeSyn {
		t.Fatalf("first packet type %d, want SYN", syn.typ)
	}
	peer.send(packet{typ: packetTypeState, connID: syn.connID, seqNr: 900, ackNr: syn.seqNr})
	var res dialResult
	select {
	case res = <-dialed:
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not complete")
	}
	if res.err != nil {
		t.Fatalf("dial: %v", res.err)
	}
	t.Cleanup(func() { _ = res.c.Close() })
	for {
		// SYN retransmits sent before the SYN-ACK arrived are skipped.
		if p := peer.mustRecv("handshake STATE"); p.typ == packetTypeState {
			return res.c
		}
	}
}

// TestResetCarryingOurSendIDClosesConn checks that a RESET carrying our send
// id reaches its conn. libutp (uTorrent, Transmission) answers a packet for a
// connection it has lost with a RESET that echoes the packet's connection id,
// our send id, while conns are keyed by recv id, so such RESETs used to be
// dropped and the dead conn lingered until the peer-wire timeouts. A RESET
// whose id merely neighbours a conn's recv id without being its send id, or
// whose ack_nr names nothing we sent, is ignored, and none is answered.
func TestResetCarryingOurSendIDClosesConn(t *testing.T) {
	cases := []struct {
		name string
		open func(t *testing.T, s *Socket, peer *rawPeer) *Conn
	}{
		{"dialed", dialRaw},
		{"accepted", func(t *testing.T, s *Socket, peer *rawPeer) *Conn {
			c, _ := acceptRaw(t, s, peer, packet{connID: 8200, seqNr: 300})
			return c
		}},
		// Recv id 0, send id 0xffff: the lookup one above wraps.
		{"accepted at wrap", func(t *testing.T, s *Socket, peer *rawPeer) *Conn {
			c, _ := acceptRaw(t, s, peer, packet{connID: 0xffff, seqNr: 300})
			return c
		}},
		// Recv id 0xffff, send id 0: the lookup one below wraps. Registered
		// by hand because dial picks its ids at random.
		{"dialed at wrap", func(t *testing.T, s *Socket, peer *rawPeer) *Conn {
			c := newOutboundConn(s, peer.conn.LocalAddr().(*net.UDPAddr), 0xffff)
			if err := s.register(c); err != nil {
				t.Fatalf("register: %v", err)
			}
			t.Cleanup(func() { c.closeWithError(net.ErrClosed, false) })
			return c
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newServerSocket(t)
			peer := newRawPeer(t, s)
			from := peer.conn.LocalAddr().(*net.UDPAddr)
			c := tc.open(t, s, peer)
			c.mu.Lock()
			ack := c.localSeq // what the RESET for our latest packet acks
			c.mu.Unlock()
			// Driven through handleUTPPacket, the read loop's own entry
			// point, so each RESET has been handled when it returns.
			reset := func(id, ack uint16) {
				s.handleUTPPacket(packet{typ: packetTypeReset, connID: id, seqNr: 77, ackNr: ack}.marshal(), from)
			}

			// sendID±2 puts c's recv id among the ids tried either way.
			for _, id := range []uint16{c.sendID + 2, c.sendID - 2, c.sendID + 1000} {
				reset(id, ack)
			}
			reset(c.sendID, ack+20000)
			if err := c.errIfClosed(); err != nil {
				t.Fatalf("conn closed by a RESET that does not name it: %v", err)
			}

			reset(c.sendID, ack)
			if err := c.errIfClosed(); !errors.Is(err, errReset) {
				t.Fatalf("conn after a RESET carrying its send id: err=%v, want %v", err, errReset)
			}
			if conns, _ := socketState(s); conns != 0 {
				t.Fatalf("%d conns still registered after the RESET", conns)
			}
			if p, ok := peer.recv(150 * time.Millisecond); ok {
				t.Fatalf("RESETs drew a reply of type %d", p.typ)
			}
		})
	}
}
