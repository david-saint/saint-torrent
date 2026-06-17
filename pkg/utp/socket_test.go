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
