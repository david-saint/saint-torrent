package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/torrent"
)

// TestPeerLoopIgnoresRepeatedAvailabilityAnnouncements is the regression test for
// the have_all/have_none flip DoS: every bitfield-class message used to rewrite
// the peer's whole availability contribution under the session write lock, so a
// peer alternating them held s.mu for O(pieces) per 5-byte message. Only the first
// announcement may count; later bitfield, have_all and have_none are ignored.
func TestPeerLoopIgnoresRepeatedAvailabilityAnnouncements(t *testing.T) {
	const numPieces = 64
	sess := newWireTestSession(t, numPieces, 16)
	w := startWirePeer(t, sess, 6200, fastReserved())

	partial := make([]byte, numPieces/8)
	setBit(partial, 0)
	setBit(partial, 2)
	w.send(&peer.Message{ID: peer.MsgBitfield, Payload: partial})
	for i := 0; i < 50; i++ {
		w.send(&peer.Message{ID: peer.MsgHaveAll})
		w.send(&peer.Message{ID: peer.MsgHaveNone})
		w.send(&peer.Message{ID: peer.MsgBitfield, Payload: fullPieceBitfield(numPieces)})
		w.send(&peer.Message{ID: peer.MsgBitfield, Payload: make([]byte, numPieces/8)})
	}
	w.barrier()

	got := availabilitySnapshot(sess)
	for i, avail := range got {
		want := 0
		if i == 0 || i == 2 {
			want = 1
		}
		if avail != want {
			t.Fatalf("availability[%d] = %d, want %d (a repeated announcement was applied): %v", i, avail, want, got)
		}
	}

	// A Have after the announcement still counts.
	have := make([]byte, 4)
	binary.BigEndian.PutUint32(have, 5)
	w.send(&peer.Message{ID: peer.MsgHave, Payload: have})
	w.barrier()
	if got := availabilitySnapshot(sess); got[5] != 1 {
		t.Fatalf("availability[5] = %d after Have, want 1", got[5])
	}

	// Disconnecting removes exactly what was counted.
	w.close()
	for i, avail := range availabilitySnapshot(sess) {
		if avail != 0 {
			t.Fatalf("availability[%d] = %d after disconnect, want 0", i, avail)
		}
	}
}

// TestPeerLoopSeedLeavesAvailabilityUntouched checks the O(1) seed path: a have_all
// (or full bitfield) peer is not folded into per-piece availability, which would
// cost O(pieces) under the write lock on connect and again on disconnect, yet we
// still request pieces from it. Haves that preceded the announcement are withdrawn
// so the peer is not counted twice.
func TestPeerLoopSeedLeavesAvailabilityUntouched(t *testing.T) {
	const numPieces = 16
	for _, tc := range []struct {
		name     string
		announce *peer.Message
	}{
		{"have_all", &peer.Message{ID: peer.MsgHaveAll}},
		{"full_bitfield", &peer.Message{ID: peer.MsgBitfield, Payload: fullPieceBitfield(numPieces)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newWireTestSession(t, numPieces, 16)
			w := startWirePeer(t, sess, 6201, fastReserved())

			have := make([]byte, 4)
			binary.BigEndian.PutUint32(have, 3)
			w.send(&peer.Message{ID: peer.MsgHave, Payload: have})
			w.barrier()
			if got := availabilitySnapshot(sess); got[3] != 1 {
				t.Fatalf("availability[3] = %d after Have, want 1", got[3])
			}

			w.send(tc.announce)
			w.barrier()
			for i, avail := range availabilitySnapshot(sess) {
				if avail != 0 {
					t.Fatalf("availability[%d] = %d after seed announcement, want 0", i, avail)
				}
			}

			// The seed is still a usable source.
			w.send(&peer.Message{ID: peer.MsgUnchoke})
			if req := w.expect(peer.MsgRequest, 2*time.Second); binary.BigEndian.Uint32(req.Payload[0:4]) >= numPieces {
				t.Fatalf("request for out-of-range piece %d", binary.BigEndian.Uint32(req.Payload[0:4]))
			}

			w.close()
			for i, avail := range availabilitySnapshot(sess) {
				if avail != 0 {
					t.Fatalf("availability[%d] = %d after seed disconnect, want 0", i, avail)
				}
			}
		})
	}
}

// TestPeerLoopReplaysPreMetadataBitfield covers the magnet path: a partial seed's
// bitfield that arrives before metadata used to be dropped (only have_all was
// buffered), so the peer contributed no availability after metadata landed. With
// availability announcements accepted once per connection, it must be buffered and
// replayed or the peer's pieces would be lost for good.
func TestPeerLoopReplaysPreMetadataBitfield(t *testing.T) {
	const numPieces = 12
	const pieceLength = 4
	info := map[string]interface{}{
		"name":         "premeta-bitfield.bin",
		"piece length": int64(pieceLength),
		"pieces":       string(make([]byte, 20*numPieces)),
		"length":       int64(pieceLength * numPieces),
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	tor := &torrent.Torrent{Name: "premeta-bitfield", InfoHash: sha1.Sum(infoBytes)}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	w := startWirePeer(t, sess, 6202, fastReserved())
	bf := make([]byte, (numPieces+7)/8)
	setBit(bf, 1)
	setBit(bf, 10)
	w.send(&peer.Message{ID: peer.MsgBitfield, Payload: bf})
	// A later have_all must not override the buffered announcement.
	w.send(&peer.Message{ID: peer.MsgHaveAll})
	w.barrier()

	if err := sess.onMetadataDownloaded(infoBytes); err != nil {
		t.Fatalf("onMetadataDownloaded: %v", err)
	}
	// The next message triggers the post-metadata initialisation and replay.
	w.barrier()

	got := availabilitySnapshot(sess)
	if len(got) != numPieces {
		t.Fatalf("availability has %d entries, want %d", len(got), numPieces)
	}
	for i, avail := range got {
		want := 0
		if i == 1 || i == 10 {
			want = 1
		}
		if avail != want {
			t.Fatalf("availability[%d] = %d, want %d: %v", i, avail, want, got)
		}
	}
}

func TestBitfieldHelpers(t *testing.T) {
	for _, n := range []int{1, 7, 8, 9, 15, 16, 17, 100} {
		full := fullPieceBitfield(n)
		if len(full) != (n+7)/8 {
			t.Fatalf("fullPieceBitfield(%d) len = %d", n, len(full))
		}
		for i := 0; i < len(full)*8; i++ {
			if got, want := bitfieldHas(full, i), i < n; got != want {
				t.Fatalf("fullPieceBitfield(%d) bit %d = %v, want %v", n, i, got, want)
			}
		}
		if !bitfieldComplete(full, n) {
			t.Fatalf("bitfieldComplete(full(%d)) = false", n)
		}
		missing := append([]byte(nil), full...)
		missing[(n-1)/8] &^= 1 << (7 - uint((n-1)%8))
		if bitfieldComplete(missing, n) {
			t.Fatalf("bitfieldComplete with piece %d missing = true", n-1)
		}
		if bitfieldAny(make([]byte, len(full))) || !bitfieldAny(missing) && n > 1 {
			t.Fatalf("bitfieldAny wrong for n=%d", n)
		}
	}
	if bitfieldComplete(nil, 0) || bitfieldComplete([]byte{0xff}, 9) {
		t.Fatal("bitfieldComplete accepted a short or empty bitfield")
	}
}

// BenchmarkApplyBitfieldAvailability measures folding a sparse partial bitfield
// into a large torrent's availability, as a new partial-seed peer does.
func BenchmarkApplyBitfieldAvailability(b *testing.B) {
	const numPieces = 100000
	sess := newWireTestSession(b, numPieces, 16)
	bf := make([]byte, (numPieces+7)/8)
	for i := 0; i < numPieces; i += 97 {
		setBit(bf, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.applyBitfieldAvailability(nil, bf)
		sess.removePeerAvailability(bf)
	}
}
