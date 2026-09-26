package downloader

import (
	"encoding/binary"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// pieceBegin returns the index and begin offset of a piece or reject_request
// message.
func pieceBegin(msg *peer.Message) (uint32, uint32) {
	return binary.BigEndian.Uint32(msg.Payload[0:4]), binary.BigEndian.Uint32(msg.Payload[4:8])
}

// unchokedUploadPeer starts a fast-extension peer on sess and gets it unchoked, with
// the upload limit set so queued requests are served slowly (about 1 KB/s).
func unchokedUploadPeer(t *testing.T, sess *Session, port uint16) *wirePeer {
	t.Helper()
	w := startWirePeer(t, sess, port, fastReserved())
	w.send(&peer.Message{ID: peer.MsgInterested})
	w.expect(peer.MsgUnchoke, 2*time.Second)
	sess.SetUploadLimit(1000)
	// Start from an empty bucket however long setup took.
	sess.UploadLimiter.mu.Lock()
	sess.UploadLimiter.tokens = 0
	sess.UploadLimiter.lastRefill = time.Now()
	sess.UploadLimiter.mu.Unlock()
	return w
}

// TestUploadQueueHonoursCancel is the regression test for ignored cancels: there
// was no MsgCancel case, so a cancelled block stayed queued and was still read
// from disk and sent. It must be dropped (and, under BEP 6, rejected).
func TestUploadQueueHonoursCancel(t *testing.T) {
	sess, _ := newSeedingWireTestSession(t, 40, 1024)
	w := unchokedUploadPeer(t, sess, 6260)

	w.sendRequest(3, 0, 200)   // served after ~0.2 s of tokens
	w.sendRequest(3, 200, 200) // would follow ~0.2 s later
	w.send(&peer.Message{ID: peer.MsgCancel, Payload: blockPayload(3, 200, 200)})

	reject := w.expect(peer.MsgRejectRequest, 2*time.Second)
	if idx, begin := pieceBegin(reject); idx != 3 || begin != 200 {
		t.Fatalf("reject for %d/%d, want the cancelled block 3/200", idx, begin)
	}
	first := w.expect(peer.MsgPiece, 3*time.Second)
	if idx, begin := pieceBegin(first); idx != 3 || begin != 0 {
		t.Fatalf("served %d/%d, want 3/0", idx, begin)
	}
	for _, msg := range w.collect(800 * time.Millisecond) {
		if msg.ID == peer.MsgPiece {
			idx, begin := pieceBegin(msg)
			t.Fatalf("served %d/%d after it was cancelled", idx, begin)
		}
	}
}

// TestUploadQueueDroppedOnChoke checks that when the choker chokes a peer, its
// queued requests are dropped (with a reject each, BEP 6) instead of being served
// after the choke; the queue used to be kept and drained in full.
func TestUploadQueueDroppedOnChoke(t *testing.T) {
	const numPieces = 40
	sess, _ := newSeedingWireTestSession(t, numPieces, 1024)
	// Use a piece outside this peer's allowed-fast set, which may still be served.
	fast := map[int]bool{}
	for _, idx := range allowedFastSet(sess.Torrent.InfoHash, "127.0.0.1", numPieces, allowedFastSetSize) {
		fast[idx] = true
	}
	piece := -1
	for i := 0; i < numPieces && piece < 0; i++ {
		if !fast[i] {
			piece = i
		}
	}
	w := unchokedUploadPeer(t, sess, 6261)

	w.sendRequest(uint32(piece), 0, 200)
	w.sendRequest(uint32(piece), 200, 200)
	w.barrier()

	// The choke round chokes the peer while both requests wait for bandwidth.
	sess.mu.Lock()
	sess.Peers["127.0.0.1:6261"].AmChoking = true
	sess.mu.Unlock()

	rejected := 0
	for _, msg := range w.barrier() {
		switch msg.ID {
		case peer.MsgRejectRequest:
			rejected++
		case peer.MsgPiece:
			t.Fatal("a queued block was served after the choke")
		}
	}
	if rejected != 2 {
		t.Fatalf("got %d rejects for the dropped queue, want 2", rejected)
	}
	for _, msg := range w.collect(800 * time.Millisecond) {
		if msg.ID == peer.MsgPiece {
			t.Fatal("a queued block was served after the choke")
		}
	}
}
