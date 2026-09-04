package utp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
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

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     state.seqNr,
		payload:   []byte("hello"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}

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
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	fin := packet{
		typ:       packetTypeFin,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 2,
		ackNr:     state.seqNr,
	}
	if _, err := rawClient.WriteToUDP(fin.marshal(), target); err != nil {
		t.Fatalf("send FIN: %v", err)
	}
	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     state.seqNr,
		payload:   []byte("z"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}

	_ = accepted.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, 1)
	if _, err := io.ReadFull(accepted, got); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(got) != "z" {
		t.Fatalf("payload = %q, want z", got)
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
	queued := newInboundConn(s, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 65000}, 4242, 1)
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
	accepted := conn.accepted
	conn.mu.Unlock()
	if accepted {
		t.Fatal("outbound conn was marked accepted by the handshake")
	}

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
	gotAccepted := conn.accepted
	conn.mu.Unlock()
	if gotLocal != localSeq || gotRemote != remoteSeq {
		t.Fatalf("sequence state after colliding SYN: localSeq %d->%d remoteSeq %d->%d", localSeq, gotLocal, remoteSeq, gotRemote)
	}
	if gotAccepted {
		t.Fatal("colliding SYN marked the outbound conn accepted")
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
		seqNr:     peerSeq + 1,
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
	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	// A retransmitted SYN must be answered with the same STATE by the same
	// Conn: no reset, no second accept, no second localSeq increment.
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("resend SYN: %v", err)
	}
	second := readState("STATE for SYN retransmit")
	if second.seqNr != first.seqNr || second.ackNr != first.ackNr {
		t.Fatalf("STATE for retransmit seq=%d ack=%d, want seq=%d ack=%d", second.seqNr, second.ackNr, first.seqNr, first.ackNr)
	}

	acceptCh := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			acceptCh <- c
		}
	}()
	select {
	case <-acceptCh:
		t.Fatal("SYN retransmit produced a second accepted conn")
	case <-time.After(200 * time.Millisecond):
	}

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     first.seqNr,
		payload:   []byte("hello"),
	}
	if _, err := rawClient.WriteToUDP(data.marshal(), target); err != nil {
		t.Fatalf("send DATA: %v", err)
	}
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

	// A late SYN retransmit arriving after data must not rewind remoteSeq.
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("resend SYN after data: %v", err)
	}
	third := readState("STATE for late SYN retransmit")
	if third.seqNr != first.seqNr || third.ackNr != data.seqNr {
		t.Fatalf("late retransmit STATE seq=%d ack=%d, want seq=%d ack=%d", third.seqNr, third.ackNr, first.seqNr, data.seqNr)
	}

	more := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 2,
		ackNr:     first.seqNr,
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
