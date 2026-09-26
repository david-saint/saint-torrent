package downloader

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/peer"
)

// livenessPeerA1 is the remote end of a runPeerMessageLoop connection over
// net.Pipe that, unlike wirePeer, counts the keep-alives it receives and can run
// the loop as an outbound connection. It drains everything the loop sends.
type livenessPeerA1 struct {
	t          *testing.T
	addr       string
	remote     net.Conn
	keepAlives atomic.Int32
	in         chan *peer.Message
	done       chan struct{}
}

func startLivenessPeerA1(t *testing.T, sess *Session, port uint16, outbound bool) *livenessPeerA1 {
	t.Helper()
	local, remote := net.Pipe()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	p := &livenessPeerA1{
		t:      t,
		addr:   fmt.Sprintf("127.0.0.1:%d", port),
		remote: remote,
		in:     make(chan *peer.Message, 4096),
		done:   make(chan struct{}),
	}
	go func() {
		sess.runPeerMessageLoop(client, local, p.addr, "127.0.0.1", port, fastReserved(), outbound)
		close(p.done)
	}()
	go func() {
		r := bufio.NewReader(remote)
		for {
			msg, err := peer.ParseMessage(r)
			if err != nil {
				return
			}
			if msg == nil {
				p.keepAlives.Add(1)
				continue
			}
			select {
			case p.in <- msg:
			default: // nobody is looking; never block the loop's writes
			}
		}
	}()
	t.Cleanup(func() {
		_ = remote.Close()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("peer loop did not exit")
		}
	})
	return p
}

// write sends raw bytes and reports whether the loop took them all.
func (p *livenessPeerA1) write(b []byte) bool {
	_ = p.remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := p.remote.Write(b)
	return err == nil
}

func (p *livenessPeerA1) closedWithin(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// disconnectReasonsA1 routes the package log to a file and returns a function
// that reports the reason logged when the connection at addr ended ("" if it
// has not). Call it before starting the connection.
func disconnectReasonsA1(t *testing.T) func(addr string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer.log")
	if err := logging.Configure(logging.Config{Path: path, Level: logging.LevelInfo}); err != nil {
		t.Fatalf("configure logging: %v", err)
	}
	t.Cleanup(func() { _ = logging.Close() })
	return func(addr string) string {
		t.Helper()
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open log: %v", err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var entry struct {
				Event  string         `json:"event"`
				Fields map[string]any `json:"fields"`
			}
			if json.Unmarshal(scanner.Bytes(), &entry) != nil || entry.Event != "peer_disconnected" {
				continue
			}
			if entry.Fields["peer"] == addr {
				reason, _ := entry.Fields["reason"].(string)
				return reason
			}
		}
		return ""
	}
}

// A message whose length its type does not allow drops the peer. One too large
// for a pooled buffer is refused from its header, without reading or allocating
// the payload (only the header is sent here); a small one is read, then refused.
func TestOversizedMessageDisconnects(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     byte
		length uint32
	}{
		{"1 MiB unknown id", 30, 1 << 20},
		{"512 KiB unknown id", 30, 512 << 10},
		{"64 KiB have", byte(peer.MsgHave), 64 << 10},
		{"oversized piece", byte(peer.MsgPiece), 9 + BlockSize + 1},
		{"long have", byte(peer.MsgHave), 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reasons := disconnectReasonsA1(t)
			sess := settled(newWireTestSession(t, 4, 16*1024))
			p := startLivenessPeerA1(t, sess, 7703, false)
			frame := make([]byte, 5)
			if tc.length <= 9+BlockSize {
				frame = make([]byte, 4+tc.length)
			}
			binary.BigEndian.PutUint32(frame, tc.length)
			frame[4] = tc.id
			p.write(frame)
			if !p.closedWithin(3 * time.Second) {
				t.Fatal("peer was not dropped")
			}
			if got := reasons(p.addr); got != "invalid_message_length" {
				t.Fatalf("disconnect reason %q, want invalid_message_length", got)
			}
		})
	}
}
