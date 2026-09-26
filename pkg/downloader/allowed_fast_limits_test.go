package downloader

import (
	"encoding/binary"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

func allowedFastOffer(index uint32) *peer.Message {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, index)
	return &peer.Message{ID: peer.MsgAllowedFast, Payload: payload}
}

// TestAllowedFastSetCappedAfterMetadata is the regression test for the unbounded
// post-metadata allowed_fast set: before metadata offers were capped at
// pendingAllowedFastCap, but afterwards every valid index was kept, so a peer
// could grow the set to the piece count and make hasAllowedFastWork scan it on
// every message while it chokes us. Offers beyond the cap are now ignored.
//
// The peer offers indices from the top down, so with the cap in place the lowest
// indices are the ones dropped. The picker prefers the lowest index among equally
// available pieces, so the piece we ask for next shows whether they were kept.
func TestAllowedFastSetCappedAfterMetadata(t *testing.T) {
	defer swapDuration(&blockRequestTimeout, time.Hour)()
	const numPieces = pendingAllowedFastCap + 44
	sess := newWireTestSession(t, numPieces, 16)
	sess.pipelineBudget = newPipelineByteBudget(16) // one request in flight at a time
	w := startWirePeer(t, sess, 6250, fastReserved())

	w.send(&peer.Message{ID: peer.MsgHaveAll})
	for i := numPieces - 1; i >= 0; i-- {
		w.send(allowedFastOffer(uint32(i)))
	}
	first := w.expect(peer.MsgRequest, 2*time.Second)
	if got := binary.BigEndian.Uint32(first.Payload[0:4]); got != numPieces-1 {
		t.Fatalf("first allowed-fast request for piece %d, want %d", got, numPieces-1)
	}
	w.barrier()

	// While the first request was in flight the loop already opened the next piece
	// (offered second) and is waiting for budget to request it. Reject both; the
	// piece opened after that is the lowest-index one still allowed.
	w.send(&peer.Message{ID: peer.MsgRejectRequest, Payload: first.Payload})
	second := w.expect(peer.MsgRequest, 2*time.Second)
	w.send(&peer.Message{ID: peer.MsgRejectRequest, Payload: second.Payload})
	next := w.expect(peer.MsgRequest, 2*time.Second)
	lowestKept := uint32(numPieces - pendingAllowedFastCap)
	if got := binary.BigEndian.Uint32(next.Payload[0:4]); got != lowestKept {
		t.Fatalf("allowed-fast request for piece %d, want %d (offers past the cap were kept)", got, lowestKept)
	}
}

// TestAllowedFastServeCappedWhileChoking is the regression test for the
// allowed-fast choke bypass: a peer we choke could request its allowed-fast
// pieces any number of times. As in libtorrent, each piece is now served at most
// allowedFastServeRounds times over while we choke; later requests are rejected.
func TestAllowedFastServeCappedWhileChoking(t *testing.T) {
	sess, _ := newSeedingWireTestSession(t, 1, 16) // one piece of one block: fast set {0}
	w := startWirePeer(t, sess, 6251, fastReserved())
	w.expect(peer.MsgAllowedFast, 2*time.Second)

	for i := 0; i < allowedFastServeRounds+2; i++ {
		w.sendRequest(0, 0, 16)
	}
	served, rejected := 0, 0
	for _, msg := range w.barrier() {
		switch msg.ID {
		case peer.MsgPiece:
			served++
		case peer.MsgRejectRequest:
			rejected++
		}
	}
	if served != allowedFastServeRounds || rejected != 2 {
		t.Fatalf("while choking: served %d, rejected %d; want %d served, 2 rejected", served, rejected, allowedFastServeRounds)
	}

	// Once unchoked the peer is served normally again.
	w.send(&peer.Message{ID: peer.MsgInterested})
	w.expect(peer.MsgUnchoke, 2*time.Second)
	w.sendRequest(0, 0, 16)
	w.expect(peer.MsgPiece, 2*time.Second)
}
