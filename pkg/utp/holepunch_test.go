package utp

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func localAddrPort(t *testing.T, s *Socket) netip.AddrPort {
	t.Helper()
	return netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), s.Port())
}

func dialSocket(t *testing.T, from, to *Socket, timeout time.Duration) (net.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := from.DialContext(ctx, localAddrPort(t, to).String())
	if err == nil {
		t.Cleanup(func() { _ = conn.Close() })
	}
	return conn, err
}

// TestExpectIncomingExemptsFromRefusal checks that a socket refusing inbound
// connections still accepts one from an endpoint ExpectIncoming named, and
// only from that endpoint and only until it expires.
func TestExpectIncomingExemptsFromRefusal(t *testing.T) {
	server, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ln := server.Listen()
	defer ln.Close()
	accepted := acceptAsync(ln)
	server.SetRefuseIncoming(true)

	expected, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer expected.Close()
	stranger, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()

	server.ExpectIncoming(localAddrPort(t, expected), time.Minute)

	if _, err := dialSocket(t, stranger, server, 2*time.Second); err == nil || !strings.Contains(err.Error(), "reset") {
		t.Fatalf("dial from an endpoint not expected: %v, want a reset", err)
	}
	conn, err := dialSocket(t, expected, server, 2*time.Second)
	if err != nil {
		t.Fatalf("dial from the expected endpoint: %v", err)
	}
	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}
	expectAccept(t, accepted, "expected endpoint")

	// An expired exemption refuses again.
	late, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Close()
	server.ExpectIncoming(localAddrPort(t, late), time.Nanosecond)
	time.Sleep(time.Millisecond)
	if _, err := dialSocket(t, late, server, 2*time.Second); err == nil || !strings.Contains(err.Error(), "reset") {
		t.Fatalf("dial after the exemption expired: %v, want a reset", err)
	}
}

// TestExpectIncomingIsBounded checks the exemption table never grows past
// maxExpectedIncoming, replacing the entry closest to expiry.
func TestExpectIncomingIsBounded(t *testing.T) {
	s, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	base := netip.MustParseAddr("10.0.0.1")
	for i := 0; i < maxExpectedIncoming+50; i++ {
		s.ExpectIncoming(netip.AddrPortFrom(base, uint16(1000+i)), time.Duration(i+1)*time.Minute)
	}
	s.mu.Lock()
	n := len(s.expected)
	_, firstKept := s.expected[netip.AddrPortFrom(base, 1000)]
	_, lastKept := s.expected[netip.AddrPortFrom(base, uint16(1000+maxExpectedIncoming+49))]
	s.mu.Unlock()
	if n != maxExpectedIncoming {
		t.Fatalf("expected table holds %d entries, want %d", n, maxExpectedIncoming)
	}
	if firstKept || !lastKept {
		t.Fatalf("first entry kept=%v, newest kept=%v; want the oldest replaced", firstKept, lastKept)
	}
}

// TestEmulateNATSimultaneousOpen checks the NAT emulation the holepunch tests
// rely on: a socket behind it cannot be dialed first, but once both sides
// dial each other at once each one's SYN opens its own filter to the other,
// and the dials connect.
func TestEmulateNATSimultaneousOpen(t *testing.T) {
	a, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := NewSocket(0)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a.EmulateNAT()
	b.EmulateNAT()
	lnA, lnB := a.Listen(), b.Listen()
	defer lnA.Close()
	defer lnB.Close()

	// One-sided: b's filter drops a's SYNs, so the dial times out.
	if _, err := dialSocket(t, a, b, 700*time.Millisecond); err == nil {
		t.Fatal("dial into an emulated NAT succeeded")
	}

	acceptedA, acceptedB := acceptAsync(lnA), acceptAsync(lnB)
	type result struct {
		conn net.Conn
		err  error
	}
	fromB := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := b.DialContext(ctx, localAddrPort(t, a).String())
		fromB <- result{conn, err}
	}()
	connA, errA := dialSocket(t, a, b, 5*time.Second)
	resB := <-fromB
	if errA != nil || resB.err != nil {
		t.Fatalf("simultaneous dials: a->b %v, b->a %v", errA, resB.err)
	}
	defer resB.conn.Close()
	// Each side also accepted the other's connection.
	if _, err := connA.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := resB.conn.Write([]byte("y")); err != nil {
		t.Fatal(err)
	}
	expectAccept(t, acceptedA, "b's connection at a")
	expectAccept(t, acceptedB, "a's connection at b")
}
