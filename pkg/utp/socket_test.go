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

func seqState(c *Conn) (local, remote uint16, accepted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.localSeq, c.remoteSeq, c.accepted
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

	const serverSeq = uint16(900)

	proceed := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
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

		<-proceed

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
		if data.typ != packetTypeData || data.connID != syn.connID+1 {
			serverDone <- fmt.Errorf("DATA type/connID = %d/%d, want DATA/%d", data.typ, data.connID, syn.connID+1)
			return
		}
		ack := packet{
			typ:       packetTypeState,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     serverSeq,
			ackNr:     data.seqNr,
		}
		if _, err := rawPeer.WriteToUDP(ack.marshal(), addr); err != nil {
			serverDone <- err
			return
		}
		reply := packet{
			typ:       packetTypeData,
			connID:    syn.connID,
			timestamp: uint32(time.Now().UnixMicro()),
			seqNr:     serverSeq + 1,
			ackNr:     data.seqNr,
			payload:   []byte("inbound-after-collision"),
		}
		_, err = rawPeer.WriteToUDP(reply.marshal(), addr)
		serverDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := client.DialContext(ctx, rawPeer.LocalAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	dialed, ok := conn.(*Conn)
	if !ok {
		t.Fatalf("dialed conn has type %T, want *utp.Conn", conn)
	}

	localBefore, remoteBefore, acceptedBefore := seqState(dialed)
	if acceptedBefore {
		t.Fatal("outbound conn is marked accepted before any SYN")
	}

	// A SYN from the peer whose connID collides with the outbound conn's
	// recvID (derived fallback key addr, connID+1 == recvID).
	stray := packet{
		typ:       packetTypeSyn,
		connID:    dialed.recvID - 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     40000,
	}
	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(client.Port())}
	if _, err := rawPeer.WriteToUDP(stray.marshal(), target); err != nil {
		t.Fatalf("send stray SYN: %v", err)
	}

	// The stray SYN is refused with a RESET; its arrival also proves the
	// packet was processed before the state assertions below.
	resetBuf := make([]byte, 1500)
	n, _, err := rawPeer.ReadFromUDP(resetBuf)
	if err != nil {
		t.Fatalf("read RESET for stray SYN: %v", err)
	}
	reset, err := parsePacket(resetBuf[:n])
	if err != nil {
		t.Fatalf("parse RESET: %v", err)
	}
	if reset.typ != packetTypeReset || reset.connID != stray.connID {
		t.Fatalf("stray SYN reply type/connID = %d/%d, want RESET/%d", reset.typ, reset.connID, stray.connID)
	}

	if localAfter, remoteAfter, acceptedAfter := seqState(dialed); localAfter != localBefore || remoteAfter != remoteBefore || acceptedAfter {
		t.Fatalf("outbound seq state changed: local %d->%d remote %d->%d accepted %v->%v",
			localBefore, localAfter, remoteBefore, remoteAfter, acceptedBefore, acceptedAfter)
	}

	client.mu.Lock()
	conns := len(client.conns)
	only := true
	for _, c := range client.conns {
		only = only && c == dialed
	}
	client.mu.Unlock()
	if conns != 1 || !only {
		t.Fatalf("socket holds %d conns after stray SYN, want exactly the dialed conn", conns)
	}

	acceptResult := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			acceptResult <- c
		}
	}()
	select {
	case c := <-acceptResult:
		t.Fatalf("Accept returned conn %p, want nothing (dialed %p)", c, dialed)
	case <-time.After(200 * time.Millisecond):
	}

	// The outbound conn must still carry data both ways afterwards.
	close(proceed)
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if n, err := conn.Write([]byte("outbound-after-collision")); err != nil || n != len("outbound-after-collision") {
		t.Fatalf("write after stray SYN got n=%d err=%v", n, err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len("inbound-after-collision"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read after stray SYN: %v", err)
	}
	if string(got) != "inbound-after-collision" {
		t.Fatalf("payload after stray SYN = %q", got)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("raw peer failed: %v", err)
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
	_ = rawClient.SetDeadline(time.Now().Add(2 * time.Second))

	target := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(server.Port())}
	syn := packet{typ: packetTypeSyn, connID: 5100, timestamp: uint32(time.Now().UnixMicro()), seqNr: 33}
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("send SYN: %v", err)
	}

	buf := make([]byte, 1500)
	readState := func(what string) packet {
		t.Helper()
		n, _, err := rawClient.ReadFromUDP(buf)
		if err != nil {
			t.Fatalf("read %s: %v", what, err)
		}
		state, err := parsePacket(buf[:n])
		if err != nil {
			t.Fatalf("parse %s: %v", what, err)
		}
		if state.typ != packetTypeState || state.connID != syn.connID || state.ackNr != syn.seqNr {
			t.Fatalf("%s: type=%d connID=%d ack=%d, want STATE/%d/%d",
				what, state.typ, state.connID, state.ackNr, syn.connID, syn.seqNr)
		}
		return state
	}
	state := readState("first STATE")

	accepted, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer accepted.Close()
	inbound, ok := accepted.(*Conn)
	if !ok {
		t.Fatalf("accepted conn has type %T, want *utp.Conn", accepted)
	}
	localBefore, remoteBefore, _ := seqState(inbound)

	// Retransmitted SYN for the still-pending inbound conn must re-ack
	// without touching sequence state and without a second Accept.
	if _, err := rawClient.WriteToUDP(syn.marshal(), target); err != nil {
		t.Fatalf("retransmit SYN: %v", err)
	}
	readState("retransmit STATE")

	if localAfter, remoteAfter, _ := seqState(inbound); localAfter != localBefore || remoteAfter != remoteBefore {
		t.Fatalf("retransmit rewrote seq state: local %d->%d remote %d->%d",
			localBefore, localAfter, remoteBefore, remoteAfter)
	}
	if _, remote, _ := seqState(inbound); remote != syn.seqNr {
		t.Fatalf("remoteSeq = %d, want SYN seq %d", remote, syn.seqNr)
	}

	secondAccept := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			secondAccept <- c
		}
	}()
	select {
	case c := <-secondAccept:
		t.Fatalf("second Accept returned conn %p, want exactly one delivery", c)
	case <-time.After(200 * time.Millisecond):
	}

	_ = rawClient.SetDeadline(time.Now().Add(150 * time.Millisecond))
	if _, _, err := rawClient.ReadFromUDP(buf); err == nil {
		t.Fatal("unexpected packet after SYN retransmit, want only the STATE re-ack")
	} else if nerr, ok := err.(net.Error); !ok || !nerr.Timeout() {
		t.Fatalf("read after retransmit: %v, want a timeout (no RESET)", err)
	}
	_ = rawClient.SetDeadline(time.Now().Add(2 * time.Second))

	data := packet{
		typ:       packetTypeData,
		connID:    syn.connID + 1,
		timestamp: uint32(time.Now().UnixMicro()),
		seqNr:     syn.seqNr + 1,
		ackNr:     state.seqNr,
		payload:   []byte("hello-again"),
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
