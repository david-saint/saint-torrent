package utp

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// BenchmarkLoopbackTransfer measures one-directional stream throughput over
// a dialed and accepted loopback pair: the per-packet receive path (routing,
// ack validation, in-order delivery, coalesced acks) and the sender's ack
// walk.
func BenchmarkLoopbackTransfer(b *testing.B) {
	server, err := NewSocket(0)
	if err != nil {
		b.Fatalf("server socket: %v", err)
	}
	defer server.Close()
	client, err := NewSocket(0)
	if err != nil {
		b.Fatalf("client socket: %v", err)
	}
	defer client.Close()
	ln := server.Listen()
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			accepted <- c
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, err := client.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", server.Port()))
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer src.Close()
	var dst net.Conn
	select {
	case dst = <-accepted:
	case <-time.After(5 * time.Second):
		b.Fatal("accept timed out")
	}
	defer dst.Close()

	const chunk = 1 << 20
	payload := make([]byte, chunk)
	sink := make([]byte, chunk)
	b.SetBytes(chunk)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readErr := make(chan error, 1)
		go func() {
			_, err := io.ReadFull(dst, sink)
			readErr <- err
		}()
		if _, err := src.Write(payload); err != nil {
			b.Fatalf("write: %v", err)
		}
		if err := <-readErr; err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}
