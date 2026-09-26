package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// newWireTestSession builds a downloading session of numPieces pieces of pieceLen
// bytes each, backed by in-memory storage, for driving runPeerMessageLoop.
func newWireTestSession(t testing.TB, numPieces int, pieceLen int64) *Session {
	t.Helper()
	hashes := make([][20]byte, numPieces)
	for i := range hashes {
		hashes[i] = sha1.Sum([]byte(fmt.Sprintf("wire-piece-%d", i)))
	}
	total := int64(numPieces) * pieceLen
	tor := &torrent.Torrent{
		Name:        "wire.bin",
		InfoHash:    sha1.Sum([]byte("wire-test")),
		PieceLength: pieceLen,
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: total, Path: []string{"wire.bin"}}},
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: "wire.bin", Length: total}}, pieceLen)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

// wirePeer is the remote end of a runPeerMessageLoop connection over net.Pipe.
// Everything the loop sends is collected on in, so the loop never blocks on a
// write the test has not read yet.
type wirePeer struct {
	t      *testing.T
	remote net.Conn
	in     chan *peer.Message
	done   chan struct{}
}

func fastReserved() [8]byte {
	var reserved [8]byte
	peer.EnableFastExtension(&reserved)
	return reserved
}

func startWirePeer(t *testing.T, sess *Session, port uint16, reserved [8]byte) *wirePeer {
	t.Helper()
	clientConn, remoteConn := net.Pipe()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
	w := &wirePeer{
		t:      t,
		remote: remoteConn,
		in:     make(chan *peer.Message, 4096),
		done:   make(chan struct{}),
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	go func() {
		sess.runPeerMessageLoop(client, clientConn, addr, "127.0.0.1", port, reserved, false)
		close(w.done)
	}()
	go func() {
		defer close(w.in)
		for {
			msg, err := peer.ParseMessage(remoteConn)
			if err != nil {
				return
			}
			if msg == nil {
				continue // keep-alive
			}
			w.in <- msg
		}
	}()
	t.Cleanup(w.close)
	return w
}

func (w *wirePeer) send(m *peer.Message) {
	w.t.Helper()
	_ = w.remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := w.remote.Write(m.Serialize()); err != nil {
		w.t.Fatalf("write %v: %v", m.ID, err)
	}
}

func (w *wirePeer) sendRequest(index, begin, length uint32) {
	w.t.Helper()
	w.send(&peer.Message{ID: peer.MsgRequest, Payload: blockPayload(index, begin, length)})
}

func blockPayload(index, begin, length uint32) []byte {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	return payload
}

// expect skips messages until one with the given id arrives and returns it.
func (w *wirePeer) expect(id peer.MessageID, timeout time.Duration) *peer.Message {
	w.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				w.t.Fatalf("connection closed while waiting for message %d", id)
			}
			if msg.ID == id {
				return msg
			}
		case <-deadline:
			w.t.Fatalf("timed out waiting for message %d", id)
		}
	}
}

// barrier waits until the loop has processed everything sent before it. A request
// for a piece index that cannot exist always draws a reject_request from a
// fast-extension connection, and messages are handled in order, so seeing that
// reject proves every earlier message was consumed. Messages that arrive while
// waiting are returned.
func (w *wirePeer) barrier() []*peer.Message {
	w.t.Helper()
	w.sendRequest(0xffffffff, 0, 1)
	var seen []*peer.Message
	deadline := time.After(5 * time.Second)
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				w.t.Fatal("connection closed before the barrier")
			}
			if msg.ID == peer.MsgRejectRequest && binary.BigEndian.Uint32(msg.Payload[0:4]) == 0xffffffff {
				return seen
			}
			seen = append(seen, msg)
		case <-deadline:
			w.t.Fatal("timed out waiting for the barrier reject")
		}
	}
}

func (w *wirePeer) close() {
	_ = w.remote.Close()
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		w.t.Error("peer loop did not exit")
	}
}

func availabilitySnapshot(sess *Session) []int {
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	return append([]int(nil), sess.pieceAvailability...)
}
