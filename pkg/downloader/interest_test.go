package downloader

import (
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// TestPeerLoopRepeatedInterestedIsNoOp is the regression test for the Interested
// scan: every Interested, including repeats, took the session write lock and
// walked the whole Peers map, and a repeat could even undo the choker by
// unchoking the peer again. A repeat must now do nothing.
func TestPeerLoopRepeatedInterestedIsNoOp(t *testing.T) {
	sess := newWireTestSession(t, 4, 16)
	w := startWirePeer(t, sess, 6240, fastReserved())

	w.send(&peer.Message{ID: peer.MsgInterested})
	w.expect(peer.MsgUnchoke, 2*time.Second)

	// The choke round chokes the (still interested) peer.
	sess.mu.Lock()
	sess.Peers["127.0.0.1:6240"].AmChoking = true
	sess.mu.Unlock()

	for i := 0; i < 10; i++ {
		w.send(&peer.Message{ID: peer.MsgInterested})
	}
	for _, msg := range w.barrier() {
		if msg.ID == peer.MsgUnchoke {
			t.Fatal("a repeated Interested unchoked the peer again")
		}
	}
	sess.mu.RLock()
	choking := sess.Peers["127.0.0.1:6240"].AmChoking
	sess.mu.RUnlock()
	if !choking {
		t.Fatal("a repeated Interested cleared AmChoking")
	}
}
