package downloader

import (
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// countPickerScans wraps selectNeededPiece to count its calls. The cleanup is
// registered before any peer loop is started, so it runs after they have exited.
func countPickerScans(t *testing.T) *atomic.Int64 {
	t.Helper()
	var scans atomic.Int64
	orig := selectNeededPiece
	selectNeededPiece = func(s *Session, hasPiece func(int64) bool) int {
		scans.Add(1)
		return orig(s, hasPiece)
	}
	t.Cleanup(func() { selectNeededPiece = orig })
	return &scans
}

func havePayload(index uint32) []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return payload
}

// expectRequestFor waits for a block request for piece index.
func (w *wirePeer) expectRequestFor(index uint32, timeout time.Duration) {
	w.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				w.t.Fatalf("connection closed while waiting for a request for piece %d", index)
			}
			if msg.ID == peer.MsgRequest && binary.BigEndian.Uint32(msg.Payload[0:4]) == index {
				return
			}
		case <-deadline:
			w.t.Fatalf("timed out waiting for a request for piece %d", index)
		}
	}
}

// TestPeerWithNothingWeNeedIsNotRescannedPerMessage is the regression test for
// the picker scan on every message: a peer that unchoked us but has none of the
// pieces we need made each message it sent (a 4-byte keep-alive, or its own
// block requests when it downloads from us) run a scan of every needed piece
// under the session write lock, about 20 ms per message with 100k pieces. An
// empty pick is now trusted until something changes, and a piece the peer then
// announces is still asked for at once.
func TestPeerWithNothingWeNeedIsNotRescannedPerMessage(t *testing.T) {
	scans := countPickerScans(t)
	sess := newWireTestSession(t, 64, 16)
	w := startWirePeer(t, sess, 6250, fastReserved())
	w.send(&peer.Message{ID: peer.MsgHaveNone})
	w.send(&peer.Message{ID: peer.MsgUnchoke})
	w.barrier()

	before := scans.Load()
	keepAlive := make([]byte, 4)
	for i := 0; i < 200; i++ {
		if _, err := w.remote.Write(keepAlive); err != nil {
			t.Fatalf("write keep-alive: %v", err)
		}
	}
	w.barrier()
	if n := scans.Load() - before; n > 1 {
		t.Fatalf("200 keep-alives from a peer with nothing we need ran the picker %d times", n)
	}

	// Flipping choke and unchoke does not force a scan per flip either: the
	// empty pick was made unchoked, and an unchoke adds nothing to it.
	before = scans.Load()
	for i := 0; i < 100; i++ {
		w.send(&peer.Message{ID: peer.MsgChoke})
		w.send(&peer.Message{ID: peer.MsgUnchoke})
	}
	w.barrier()
	if n := scans.Load() - before; n > 1 {
		t.Fatalf("100 choke/unchoke flips ran the picker %d times", n)
	}

	w.send(&peer.Message{ID: peer.MsgHave, Payload: havePayload(9)})
	w.expectRequestFor(9, 2*time.Second)
}

// TestRestrictedEmptyPickIsRedoneOnUnchoke checks the other side of the flip
// rule: an empty pick that set aside pieces the peer rejected is redone when an
// unchoke clears the rejections, at once rather than after pickRetryInterval.
func TestRestrictedEmptyPickIsRedoneOnUnchoke(t *testing.T) {
	sess := newWireTestSession(t, 1, 2*BlockSize)
	w := startWirePeer(t, sess, 6252, fastReserved())
	w.send(&peer.Message{ID: peer.MsgHaveAll})
	w.send(&peer.Message{ID: peer.MsgUnchoke})
	w.expectRequestFor(0, 2*time.Second)

	// The peer rejects a block: the piece is dropped and set aside for this
	// peer, so the next pick finds nothing.
	w.send(&peer.Message{ID: peer.MsgRejectRequest, Payload: blockPayload(0, 0, BlockSize)})
	w.barrier()

	w.send(&peer.Message{ID: peer.MsgUnchoke})
	w.expectRequestFor(0, 2*time.Second)
}

// TestEmptyPickFollowsSessionChanges checks the empty-pick cache against changes
// on the session side: a piece another peer releases, and endgame starting, make
// a piece this peer has pickable again, and the next message picks it.
func TestEmptyPickFollowsSessionChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(sess *Session)
	}{
		{"released", func(sess *Session) {
			sess.setPieceStateLocked(5, PieceEmpty) // its downloader was choked
		}},
		{"endgame", func(sess *Session) {
			for i := range sess.PieceStates {
				if i != 5 {
					sess.setPieceStateLocked(i, PieceDownloading) // the last pieces are claimed
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scans := countPickerScans(t)
			sess := newWireTestSession(t, 8, 16)
			sess.mu.Lock()
			sess.setPieceStateLocked(5, PieceDownloading) // in flight on another peer
			sess.mu.Unlock()
			w := startWirePeer(t, sess, 6251, fastReserved())
			w.send(&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0x04}}) // piece 5 only
			w.send(&peer.Message{ID: peer.MsgUnchoke})
			w.barrier()
			if scans.Load() == 0 {
				t.Fatal("the unchoke did not run the picker")
			}

			sess.mu.Lock()
			tc.change(sess)
			sess.mu.Unlock()
			if _, err := w.remote.Write(make([]byte, 4)); err != nil { // keep-alive
				t.Fatalf("write keep-alive: %v", err)
			}
			w.expectRequestFor(5, 2*time.Second)
		})
	}
}
