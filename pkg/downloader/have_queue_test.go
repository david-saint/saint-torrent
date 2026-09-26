package downloader

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// broadcastHave used to start one goroutine per active peer per completed piece,
// each blocking forever on a peer that stopped reading. It must only queue.
func TestBroadcastHaveStartsNoGoroutinesForStalledPeer(t *testing.T) {
	sess := newWireTestSession(t, 8, 16*1024)
	local, remote := net.Pipe() // nobody reads remote, so any write blocks
	defer remote.Close()
	defer local.Close()
	client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
	sess.mu.Lock()
	sess.activePeers["127.0.0.1:7500"] = client
	sess.mu.Unlock()

	before := runtime.NumGoroutine()
	for i := 0; i < 1000; i++ {
		sess.broadcastHave(uint32(i % 8))
	}
	if grown := runtime.NumGoroutine() - before; grown > 5 {
		t.Fatalf("1000 Haves for a stalled peer left %d extra goroutines, want none", grown)
	}
}

// Completed pieces reach a connected peer as Haves, sent by its own loop.
func TestCompletedPiecesReachPeerAsHaves(t *testing.T) {
	sess := newWireTestSession(t, 8, 16*1024)
	w := startWirePeer(t, sess, 7501, fastReserved())
	w.barrier()

	for i := int64(0); i < 5; i++ {
		sess.markPieceCompleted(i)
	}
	for want := uint32(0); want < 5; want++ {
		msg := w.expect(peer.MsgHave, 2*time.Second)
		if got := binary.BigEndian.Uint32(msg.Payload); got != want {
			t.Fatalf("Have for piece %d, want %d", got, want)
		}
	}
}

// The choker only records the decision; the peer's loop sends the choke.
func TestChokerDecisionIsSentByPeerLoop(t *testing.T) {
	sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
	w := startWirePeer(t, sess, 7502, fastReserved())
	w.send(&peer.Message{ID: peer.MsgInterested})
	w.expect(peer.MsgUnchoke, 2*time.Second)

	// Once the peer loses interest the next choke round chokes it.
	w.send(&peer.Message{ID: peer.MsgNotInterested})
	w.barrier()
	var optimistic string
	sess.recalculateChoking(&optimistic)
	w.expect(peer.MsgChoke, 2*time.Second)
}

// A reconnecting peer's Peers entry still holds the last connection's choke and
// interest state. The new connection starts choked and uninterested, so its first
// Interested is answered with an unchoke.
func TestReconnectedPeerStartsChoked(t *testing.T) {
	sess, _ := newSeedingWireTestSession(t, 4, 16*1024)
	sess.mu.Lock()
	sess.Peers["127.0.0.1:7503"] = &PeerState{IP: "127.0.0.1", Port: 7503, Dialable: true, Interested: true, AmChoking: false}
	sess.mu.Unlock()

	w := startWirePeer(t, sess, 7503, fastReserved())
	w.send(&peer.Message{ID: peer.MsgInterested})
	w.expect(peer.MsgUnchoke, 2*time.Second)
}

// When a choke drops queued uploads, the peer hears the choke first and then the
// rejects for what it had asked, in that order on the wire.
func TestChokeGoesOutBeforeItsRejects(t *testing.T) {
	const numPieces = 40
	sess, _ := newSeedingWireTestSession(t, numPieces, 1024)
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
	w := unchokedUploadPeer(t, sess, 7504, 200) // queued requests wait for bandwidth
	w.sendRequest(uint32(piece), 0, 200)
	w.sendRequest(uint32(piece), 200, 200)
	w.barrier()

	sess.mu.Lock()
	sess.Peers["127.0.0.1:7504"].AmChoking = true
	client := sess.activePeers["127.0.0.1:7504"]
	sess.mu.Unlock()
	client.Notify() // what the choker does

	var order []peer.MessageID
	for _, msg := range w.barrier() {
		if msg.ID == peer.MsgChoke || msg.ID == peer.MsgRejectRequest {
			order = append(order, msg.ID)
		}
	}
	want := []peer.MessageID{peer.MsgChoke, peer.MsgRejectRequest, peer.MsgRejectRequest}
	if len(order) != len(want) {
		t.Fatalf("got %v, want a choke followed by two rejects", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("got %v, want a choke followed by two rejects", order)
		}
	}
}

// BenchmarkBroadcastHave measures what completing a piece costs the completing
// goroutine with 200 connected peers whose loops send the queued Haves.
func BenchmarkBroadcastHave(b *testing.B) {
	sess := newWireTestSession(b, 8, 16*1024)
	stop := make(chan struct{})
	defer close(stop)
	for i := 0; i < 200; i++ {
		local, remote := net.Pipe()
		defer local.Close()
		go func() { _, _ = io.Copy(io.Discard, remote) }()
		client := peer.NewClient(local, sess.Torrent.InfoHash, sess.PeerID)
		sess.activePeers[fmt.Sprintf("10.0.%d.%d:6881", i/250, i%250)] = client
		go func() { // the part of the peer loop that sends queued Haves
			for {
				select {
				case <-client.Notified():
					_ = client.SendQueuedHaves()
				case <-stop:
					return
				}
			}
		}()
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.broadcastHave(uint32(i % 8))
	}
}
