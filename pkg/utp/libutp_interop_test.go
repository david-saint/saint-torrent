package utp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// These tests run a real libutp endpoint against this package. They need
// libutp's ucat tool, which is not part of the build:
//
//	git clone https://github.com/bittorrent/libutp && make -C libutp ucat-static
//	SAINTTORRENT_UCAT=$PWD/libutp/ucat-static go test -run LibutpInterop ./pkg/utp
//
// ucat copies stdin to the uTP stream and the stream to stdout, so each test
// pushes data both ways and compares what arrived. The streams are sized to
// fit a default UDP receive buffer: our sender has no congestion control, so
// a larger burst on loopback overflows ucat's socket and the test would time
// the retransmits rather than the protocol.

func ucatPath(t *testing.T) string {
	t.Helper()
	path := os.Getenv("SAINTTORRENT_UCAT")
	if path == "" {
		t.Skip("SAINTTORRENT_UCAT is not set; see libutp_interop_test.go")
	}
	return path
}

type ucatProcess struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser

	mu   sync.Mutex
	out  []byte // what ucat received over uTP, drained from its stdout
	grew chan struct{}
}

func startUcat(t *testing.T, args ...string) *ucatProcess {
	t.Helper()
	p := &ucatProcess{cmd: exec.Command(ucatPath(t), args...), grew: make(chan struct{}, 1)}
	var err error
	if p.stdin, err = p.cmd.StdinPipe(); err != nil {
		t.Fatalf("ucat stdin: %v", err)
	}
	stdout, err := p.cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("ucat stdout: %v", err)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatalf("start ucat: %v", err)
	}
	// ucat writes stdout synchronously from its network loop, so it is
	// drained continuously or a full pipe would stall the uTP stream.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		buf := make([]byte, 32*1024)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				p.mu.Lock()
				p.out = append(p.out, buf[:n]...)
				p.mu.Unlock()
				select {
				case p.grew <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = p.cmd.Process.Kill()
		<-drained
		_ = p.cmd.Wait()
	})
	return p
}

// received waits until ucat has received n bytes over uTP and returns them.
// ucat's exit is no end-of-stream signal: it only notices stdin EOF by chance.
func (p *ucatProcess) received(t *testing.T, n int) []byte {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		p.mu.Lock()
		got := append([]byte(nil), p.out...)
		p.mu.Unlock()
		if len(got) >= n {
			return got
		}
		select {
		case <-p.grew:
		case <-deadline:
			t.Fatalf("ucat received %d bytes, want %d", len(got), n)
		}
	}
}

func interopPayload(tag string, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = tag[i%len(tag)] + byte(i/len(tag))
	}
	return b
}

// exchange sends a first flight from us, then streams both ways at once and
// closes our side. Upstream libutp refuses writes on an accepted socket until
// the initiator's first DATA arrives and never tells ucat when that changes,
// so a listening ucat must not be given data before our first flight; a
// BitTorrent initiator always speaks first anyway.
func exchange(t *testing.T, conn net.Conn, ucat *ucatProcess, toPeer, fromPeer []byte) {
	t.Helper()
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	const first = 1000
	if _, err := conn.Write(toPeer[:first]); err != nil {
		t.Fatalf("write first flight to libutp: %v", err)
	}
	toPeer = toPeer[first:]

	go func() {
		_, _ = ucat.stdin.Write(fromPeer)
	}()
	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(toPeer)
		writeErr <- err
	}()
	got := make([]byte, len(fromPeer))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read from libutp: %v", err)
	}
	if !bytes.Equal(got, fromPeer) {
		t.Fatal("stream from libutp was corrupted")
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write to libutp: %v", err)
	}
	_ = conn.Close()
}

func TestLibutpInteropDial(t *testing.T) {
	probe, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("probe port: %v", err)
	}
	port := probe.LocalAddr().(*net.UDPAddr).Port
	_ = probe.Close()
	ucat := startUcat(t, "-l", "-p", fmt.Sprint(port))

	s, err := NewSocket(0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer s.Close()

	var conn net.Conn
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		conn, err = s.DialContext(ctx, fmt.Sprintf("127.0.0.1:%d", port))
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial libutp: %v", err)
		}
	}
	toPeer := interopPayload("to-libutp", 64_000)
	exchange(t, conn, ucat, toPeer, interopPayload("from-libutp", 48_000))
	if out := ucat.received(t, len(toPeer)); !bytes.Equal(out, toPeer) {
		t.Fatalf("libutp received %d bytes, want %d intact", len(out), len(toPeer))
	}
}

func TestLibutpInteropAccept(t *testing.T) {
	s, err := NewSocket(0)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	defer s.Close()
	ln := s.Listen()
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		if c, err := ln.Accept(); err == nil {
			accepted <- c
		}
	}()
	ucat := startUcat(t, "127.0.0.1", fmt.Sprint(s.Port()))
	// Our listener hands the conn out only once libutp's first packet has
	// acknowledged our SYN-ACK; with nothing to send yet, upstream libutp
	// sends nothing, so the first flight is fed before Accept can return.
	fromPeer := interopPayload("from-libutp", 48_000)
	toPeer := interopPayload("to-libutp", 64_000)
	result := make(chan net.Conn, 1)
	go func() {
		select {
		case c := <-accepted:
			result <- c
		case <-time.After(5 * time.Second):
			result <- nil
		}
	}()
	if _, err := ucat.stdin.Write(fromPeer[:1000]); err != nil {
		t.Fatalf("feed ucat: %v", err)
	}
	conn := <-result
	if conn == nil {
		t.Fatal("libutp initiator was not accepted")
	}
	got := make([]byte, 1000)
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := io.ReadFull(conn, got); err != nil || !bytes.Equal(got, fromPeer[:1000]) {
		t.Fatalf("read first flight from libutp: %v", err)
	}
	exchange(t, conn, ucat, toPeer, fromPeer[1000:])
	if out := ucat.received(t, len(toPeer)); !bytes.Equal(out, toPeer) {
		t.Fatalf("libutp received %d bytes, want %d intact", len(out), len(toPeer))
	}
}
