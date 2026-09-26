package downloader

import (
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// expectDownloadStart waits for the messages a connection opened before
// metadata sends once metadata is in: our (empty) availability, then our
// interest. Extension messages sent before that are skipped; anything else
// ahead of the availability (a Have, say) fails the test.
func (w *wirePeer) expectDownloadStart(timeout time.Duration) {
	w.t.Helper()
	deadline := time.After(timeout)
	sawAvailability := false
	for {
		select {
		case msg, ok := <-w.in:
			if !ok {
				w.t.Fatal("connection closed before the download started")
			}
			switch {
			case msg.ID == peer.MsgExtended && !sawAvailability:
			case msg.ID == peer.MsgHaveNone && !sawAvailability:
				sawAvailability = true
			case msg.ID == peer.MsgInterested && sawAvailability:
				return
			default:
				w.t.Fatalf("got message %d before our have_none and interested", msg.ID)
			}
		case <-deadline:
			w.t.Fatalf("no have_none and interested within %v of metadata completing", timeout)
		}
	}
}

// TestQuietSeedGetsInterestWhenMetadataLands: a seed that sends have_all and
// the info dict has nothing more to say until we are interested, so once
// metadata is in every connection opened before it must send its bitfield and
// interest at once, not after the peer's next message (which may never come,
// or come only after the stall reaper or the inactivity drop). Covers the
// connection that delivered the metadata, inbound and outbound, and a second
// seed that stays silent throughout.
func TestQuietSeedGetsInterestWhenMetadataLands(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outbound bool
	}{
		{"inbound", false},
		{"outbound", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess, info := newMagnetTestSession(t, 1)
			start := startWirePeer
			if tc.outbound {
				start = startOutboundWirePeer
			}

			// A second seed that announces have_all and the metadata size and
			// then says nothing at all, not even to our metadata requests.
			silent := start(t, sess, 6302, fastReserved())
			silent.send(&peer.Message{ID: peer.MsgHaveAll})
			silent.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))

			delivering := start(t, sess, 6301, fastReserved())
			delivering.send(&peer.Message{ID: peer.MsgHaveAll})
			delivering.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
			delivering.expectMetadataRequests(3, 0)
			delivering.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
			waitMetadataComplete(t, sess)

			// Both wait well inside the stall reaper and liveness tick periods.
			delivering.expectDownloadStart(2 * time.Second)
			silent.expectDownloadStart(2 * time.Second)

			// The buffered have_all was replayed: an unchoke draws requests.
			for _, w := range []*wirePeer{delivering, silent} {
				w.send(&peer.Message{ID: peer.MsgUnchoke})
				w.expect(peer.MsgRequest, 2*time.Second)
			}
		})
	}
}
