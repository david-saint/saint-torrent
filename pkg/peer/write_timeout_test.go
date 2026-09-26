package peer

import (
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// deadlineCountingConn counts the write deadlines set on a connection.
type deadlineCountingConn struct {
	net.Conn
	writeDeadlines atomic.Int32
}

func (c *deadlineCountingConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadlines.Add(1)
	return c.Conn.SetWriteDeadline(t)
}

// A peer that stops reading must not block a send forever: the write times out,
// the send reports it, and the connection is closed so the reader ends too.
func TestSendFailsWhenPeerStopsReading(t *testing.T) {
	local, remote := net.Pipe() // unbuffered: every write waits for the remote to read
	defer remote.Close()
	c := NewClient(local, [20]byte{}, [20]byte{})
	c.SetWriteTimeout(100 * time.Millisecond)

	sent := make(chan error, 1)
	go func() { sent <- c.SendPiece(0, 0, make([]byte, 16*1024)) }()
	select {
	case err := <-sent:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("SendPiece returned %v, want a deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SendPiece still blocked on a peer that never reads")
	}

	_ = remote.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := remote.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("remote read after the timeout returned %v, want EOF from the closed connection", err)
	}
	if err := c.SendHave(1); err == nil {
		t.Fatal("send on a timed-out connection succeeded")
	}
}

// The deadline is armed only for writes that can reach the socket and is moved
// at most once per half timeout, so block traffic does not reset a timer per
// message. A handshake leaves no stale deadline behind.
func TestWriteDeadlineArmedLazily(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	conn := &deadlineCountingConn{Conn: local}
	c := NewClient(conn, [20]byte{1}, [20]byte{2})

	// The remote answers the handshake, then drains everything.
	go func() {
		if _, err := ParseHandshake(remote); err != nil {
			return
		}
		resp := &Handshake{Pstr: "BitTorrent protocol", InfoHash: [20]byte{1}, PeerID: [20]byte{3}}
		if _, err := remote.Write(resp.Serialize()); err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, remote)
	}()

	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	if c.RemotePeerID != ([20]byte{3}) {
		t.Fatalf("RemotePeerID = %x, want the peer's handshake ID", c.RemotePeerID)
	}
	_ = conn.SetDeadline(time.Time{}) // what the session does once the handshake is done
	conn.writeDeadlines.Store(0)

	if err := c.Flush(); err != nil {
		t.Fatalf("empty flush: %v", err)
	}
	if n := conn.writeDeadlines.Load(); n != 0 {
		t.Fatalf("an empty flush set %d write deadlines, want 0", n)
	}

	block := make([]byte, 16*1024)
	for i := 0; i < 1000; i++ {
		if err := c.SendPiece(uint32(i), 0, block); err != nil {
			t.Fatalf("SendPiece: %v", err)
		}
		for j := 0; j < 8; j++ {
			if err := c.WriteRequest(uint32(i), uint32(j*16*1024), 16*1024); err != nil {
				t.Fatalf("WriteRequest: %v", err)
			}
		}
		if err := c.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if n := conn.writeDeadlines.Load(); n != 1 {
		t.Fatalf("1000 sends set %d write deadlines, want exactly 1", n)
	}
}
