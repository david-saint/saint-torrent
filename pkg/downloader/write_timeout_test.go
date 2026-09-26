package downloader

import (
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// A peer that asks for a block and then stops reading used to wedge its loop in
// the write for good, holding its connection slot and activePeers entry until the
// session stopped. The write timeout must fail the write and tear the connection
// down.
func TestPeerThatStopsReadingIsDropped(t *testing.T) {
	sess, _ := newSeedingWireTestSession(t, 4, 64*1024)
	clientConn, remote := net.Pipe() // unbuffered: a write waits for the remote to read
	defer remote.Close()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
	client.SetWriteTimeout(200 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:7400", "127.0.0.1", 7400, fastReserved(), false)
		close(done)
	}()

	// Read what the loop sends until it unchokes us, then never read again.
	unchoked := make(chan struct{})
	go func() {
		for {
			msg, err := peer.ParseMessage(remote)
			if err != nil {
				return
			}
			if msg != nil && msg.ID == peer.MsgUnchoke {
				close(unchoked)
				return
			}
		}
	}()
	_ = remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := remote.Write((&peer.Message{ID: peer.MsgInterested}).Serialize()); err != nil {
		t.Fatalf("send interested: %v", err)
	}
	select {
	case <-unchoked:
	case <-time.After(5 * time.Second):
		t.Fatal("never unchoked")
	}
	req := &peer.Message{ID: peer.MsgRequest, Payload: blockPayload(0, 0, BlockSize)}
	if _, err := remote.Write(req.Serialize()); err != nil {
		t.Fatalf("send request: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("peer loop still blocked writing to a peer that stopped reading")
	}
	sess.mu.RLock()
	active := len(sess.activePeers)
	sess.mu.RUnlock()
	if active != 0 {
		t.Fatalf("%d active peers after the wedged peer was dropped, want 0", active)
	}
}
