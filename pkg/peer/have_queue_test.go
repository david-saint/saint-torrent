package peer

import (
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// writeCountingConn counts the writes that reach a connection.
type writeCountingConn struct {
	net.Conn
	writes atomic.Int32
}

func (c *writeCountingConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

// Queued Haves wake the owner once and go out in order with a single write.
func TestQueuedHavesAreCoalesced(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	conn := &writeCountingConn{Conn: local}
	c := NewClient(conn, [20]byte{}, [20]byte{})

	for i := uint32(0); i < 100; i++ {
		c.QueueHave(i)
	}
	select {
	case <-c.Notified():
	default:
		t.Fatal("QueueHave did not wake the owner")
	}
	select {
	case <-c.Notified():
		t.Fatal("a burst of QueueHave calls should coalesce into one wakeup")
	default:
	}

	received := make(chan []uint32, 1)
	go func() {
		var got []uint32
		_ = remote.SetReadDeadline(time.Now().Add(5 * time.Second))
		for len(got) < 100 {
			msg, err := ParseMessage(remote)
			if err != nil || msg == nil || msg.ID != MsgHave || len(msg.Payload) != 4 {
				break
			}
			got = append(got, binary.BigEndian.Uint32(msg.Payload))
		}
		received <- got
	}()
	if err := c.SendQueuedHaves(); err != nil {
		t.Fatalf("SendQueuedHaves: %v", err)
	}
	got := <-received
	if len(got) != 100 {
		t.Fatalf("received %d Haves, want 100", len(got))
	}
	for i, idx := range got {
		if idx != uint32(i) {
			t.Fatalf("Have %d is for piece %d, want %d", i, idx, i)
		}
	}
	if n := conn.writes.Load(); n != 1 {
		t.Fatalf("100 queued Haves took %d writes, want 1", n)
	}

	// Dropped Haves are never sent.
	c.QueueHave(7)
	c.DropQueuedHaves()
	if err := c.SendQueuedHaves(); err != nil {
		t.Fatalf("SendQueuedHaves after drop: %v", err)
	}
	if n := conn.writes.Load(); n != 1 {
		t.Fatalf("dropped Haves were written (%d writes)", n)
	}
}
