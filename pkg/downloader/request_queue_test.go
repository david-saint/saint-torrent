package downloader

import (
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// TestDynamicWindowHonoursPeerRequestQueue drives the same fast-peer growth as
// TestDynamicWindowStartupProbeAndRaisedCap, after LimitWindowBlocks, and checks
// the window never passes the peer's advertised queue.
func TestDynamicWindowHonoursPeerRequestQueue(t *testing.T) {
	now := time.Unix(100, 0)
	p := newPeerPipelineController(testPipelineConfig())
	p.LimitWindowBlocks(250)

	now = now.Add(250 * time.Millisecond)
	p.OnWindowLimited(now)
	for i := 0; i < 200; i++ {
		req := &blockRequest{length: BlockSize, requested: true, requestedAt: now}
		p.OnRequestSent(req, now)
		now = now.Add(1 * time.Millisecond)
		p.OnBlockAccepted(req, BlockSize, now)
		p.OnWindowLimited(now)
		if got := p.WindowBlocks(now); got > 250 {
			t.Fatalf("window = %d, past the peer's reqq of 250", got)
		}
	}

	// A tiny queue is honoured too, below our usual minimum window.
	p.LimitWindowBlocks(2)
	if got := p.WindowBlocks(now); got != 2 {
		t.Fatalf("window = %d after reqq 2, want 2", got)
	}
	// A queue at or above our maximum changes nothing.
	q := newPeerPipelineController(testPipelineConfig())
	q.LimitWindowBlocks(5000)
	if q.cfg.MaxWindowBlocks != testPipelineConfig().MaxWindowBlocks {
		t.Fatalf("reqq above our maximum changed it to %d", q.cfg.MaxWindowBlocks)
	}
}

// TestPeerLoopHonoursRequestQueue is the regression test for ignoring BEP 10
// reqq: the window starts at 64 requests and grows to 1024, while clients that
// advertise a smaller queue reject or silently drop the excess (losing the piece
// or stalling it for the 20 s request timeout). We also advertise our own queue.
func TestPeerLoopHonoursRequestQueue(t *testing.T) {
	const reqq = 8
	sess := newWireTestSession(t, 128, 16) // one block per piece
	reserved := fastReserved()
	reserved[5] |= 0x10 // extension protocol
	w := startWirePeer(t, sess, 6290, reserved)

	ours := w.expect(peer.MsgExtended, 2*time.Second)
	hs, err := peer.ParseExtensionHandshake(ours.Payload[1:])
	if err != nil {
		t.Fatalf("parse our handshake: %v", err)
	}
	if hs.RequestQueue != maxUploadQueue {
		t.Fatalf("we advertised reqq %d, want %d", hs.RequestQueue, maxUploadQueue)
	}

	payload, err := (&peer.ExtensionHandshake{RequestQueue: reqq}).Serialize()
	if err != nil {
		t.Fatalf("serialize handshake: %v", err)
	}
	w.sendExtended(peer.ExtHandshake, payload)
	w.send(&peer.Message{ID: peer.MsgHaveAll})
	w.send(&peer.Message{ID: peer.MsgUnchoke})
	requests := 0
	for _, msg := range w.barrier() {
		if msg.ID == peer.MsgRequest {
			requests++
		}
	}
	if requests != reqq {
		t.Fatalf("sent %d outstanding requests, want the peer's reqq of %d", requests, reqq)
	}
}
