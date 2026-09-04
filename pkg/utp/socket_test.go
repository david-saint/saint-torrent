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

func snapshotConn(c *Conn) (localSeq, remoteSeq uint16, accepted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.localSeq, c.remoteSeq, c.accepted
}

func TestCollidingSynDoesNotRewriteOutboundConn(t *testing.T) {
	rawPeer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("raw peer: %v", err)
	}
	defer rawPeer.Close()
	_ = rawPeer.SetDeadline(time.Now().Add(3 * time.Second))

	client, err := NewSocket(0)
	if err != nil {
		t.Fatalf("client socket: %v", err)
	}
	defer client.Close()
	ln := client.Listen()
	defer ln.Close()

	acceptedCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			acceptedCh <- c
		}
	}()

	dialed := make(chan struct {
		conn net.Conn
		err  error
	}, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn, err := client.DialContext(ctx, rawPeer.LocalAddr().String())
		dialed <- struct {
			conn net.Conn
			err  error
		}{conn, err}
	}()

	buf := make([]byte, 1500)
	n, addr, err := rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read SYN: %v", err)
	}
	syn, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse SYN: %v", err)
	}
	if syn.typ != packetTypeSyn {
		t.Fatalf("first packet type = %d, want SYN", syn.typ)
	}

	serverSeq := uint16(1000)
	state := packet{
		typ:       packetTypeState,
		connID:    syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     serverSeq,
		ackNr:     syn.seqNr,
	}
	if _, err := rawPeer.WriteToUDP(state.marshal(), addr); err != nil {
		t.Fatalf("write STATE: %v", err)
	}

	var conn net.Conn
	select {
	case r := <-dialed:
		if r.err != nil {
			t.Fatalf("dial: %v", r.err)
		}
		conn = r.conn
	case <-time.After(2 * time.Second):
		t.Fatal("dial timed out")
	}
	defer conn.Close()
	utpConn := conn.(*Conn)

	localSeq, remoteSeq, accepted := snapshotConn(utpConn)
	if accepted {
		t.Fatal("outbound conn marked accepted after dial")
	}

	colliding := packet{
		typ:       packetTypeSyn,
		connID:    syn.connID - 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     40000,
	}
	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(client.Port())}
	if _, err := rawPeer.WriteToUDP(colliding.marshal(), target); err != nil {
		t.Fatalf("send colliding SYN: %v", err)
	}

	n, _, err = rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read RESET: %v", err)
	}
	reply, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse RESET: %v", err)
	}
	if reply.typ != packetTypeReset {
		t.Fatalf("colliding SYN reply type = %d, want RESET", reply.typ)
	}

	select {
	case got := <-acceptedCh:
		if got == conn {
			t.Fatal("Accept() returned the dialed outbound conn")
		}
		t.Fatalf("Accept() returned unexpected conn %v", got)
	default:
	}

	local2, remote2, accepted2 := snapshotConn(utpConn)
	if local2 != localSeq || remote2 != remoteSeq || accepted2 != accepted {
		t.Fatalf("outbound seq state changed: localSeq %d->%d remoteSeq %d->%d accepted %v->%v",
			localSeq, local2, remoteSeq, remote2, accepted, accepted2)
	}

	writeErr := make(chan error, 1)
	go func() {
		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		_, err := conn.Write([]byte("ping"))
		writeErr <- err
	}()

	n, _, err = rawPeer.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read DATA: %v", err)
	}
	data, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse DATA: %v", err)
	}
	if data.typ != packetTypeData || string(data.payload) != "ping" {
		t.Fatalf("unexpected DATA type=%d payload=%q", data.typ, data.payload)
	}
	ack := packet{
		typ:       packetTypeState,
		connID:    syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     serverSeq,
		ackNr:     data.seqNr,
	}
	if _, err := rawPeer.WriteToUDP(ack.marshal(), addr); err != nil {
		t.Fatalf("ack DATA: %v", err)
	}
	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("client write after colliding SYN: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client write did not complete")
	}

	pong := packet{
		typ:       packetTypeData,
		connID:    syn.connID,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     serverSeq + 1,
		ackNr:     data.seqNr,
		payload:   []byte("pong"),
	}
	if _, err := rawPeer.WriteToUDP(pong.marshal(), addr); err != nil {
		t.Fatalf("send pong: %v", err)
	}
	got := make([]byte, 4)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if string(got) != "pong" {
		t.Fatalf("payload = %q, want pong", got)
	}
}

func TestPendingInboundSynRetransmitDedupes(t *testing.T) {
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
	_ = rawClient.SetDeadline(time.Now().Add(3 * time.Second))

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

	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("retransmit SYN: %v", err)
	}
	n, _, err = rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read retransmit reply: %v", err)
	}
	reply, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse retransmit reply: %v", err)
	}
	if reply.typ == packetTypeReset {
		t.Fatal("SYN retransmit was RESET")
	}
	if reply.typ != packetTypeState || reply.seqNr != state.seqNr || reply.ackNr != syn.seqNr {
		t.Fatalf("retransmit reply type=%d seq=%d ack=%d, want STATE seq=%d ack=%d",
			reply.typ, reply.seqNr, reply.ackNr, state.seqNr, syn.seqNr)
	}

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()

	second := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			second <- c
		}
	}()
	select {
	case <-second:
		t.Fatal("SYN retransmit delivered a second Accept()")
	default:
	}

	utpConn := accepted.(*Conn)
	_, remoteBefore, _ := snapshotConn(utpConn)

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

	n, _, err = rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read DATA ack: %v", err)
	}
	dataAck, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse DATA ack: %v", err)
	}
	if dataAck.typ != packetTypeState {
		t.Fatalf("DATA ack type = %d, want STATE", dataAck.typ)
	}

	localAfterData, remoteAfterData, _ := snapshotConn(utpConn)
	if remoteAfterData == remoteBefore {
		t.Fatal("remoteSeq did not advance after DATA")
	}

	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("post-data SYN retransmit: %v", err)
	}
	n, _, err = rawClient.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("read post-data SYN reply: %v", err)
	}
	post, err := parsePacket(buf[:n])
	if err != nil {
		t.Fatalf("parse post-data SYN reply: %v", err)
	}
	if post.typ == packetTypeReset {
		t.Fatal("post-data SYN retransmit was RESET")
	}
	if post.typ != packetTypeState {
		t.Fatalf("post-data SYN reply type = %d, want STATE", post.typ)
	}

	localAfterSyn, remoteAfterSyn, _ := snapshotConn(utpConn)
	if localAfterSyn != localAfterData || remoteAfterSyn != remoteAfterData {
		t.Fatalf("inbound seq rewritten by SYN retransmit: localSeq %d->%d remoteSeq %d->%d",
			localAfterData, localAfterSyn, remoteAfterData, remoteAfterSyn)
	}

	select {
	case <-second:
		t.Fatal("post-data SYN retransmit delivered a second Accept()")
	default:
	}
}
