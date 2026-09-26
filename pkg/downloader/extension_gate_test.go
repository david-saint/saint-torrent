package downloader

import (
	"bytes"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// flatListPayload returns a bencoded list of n empty lists ("llelele...e"): cheap
// on the wire, but each element used to become a heap-allocated node when a peer's
// extension message was decoded.
func flatListPayload(n int) []byte {
	return append(append([]byte{'l'}, bytes.Repeat([]byte("le"), n)...), 'e')
}

// TestPeerLoopDropsOversizedExtensionMessages is the regression test for the
// extension decode amplification: handshake, ut_metadata and ut_pex payloads were
// bencode-decoded up to the 2 MiB wire cap (~50x heap blow-up, hundreds of ms of
// CPU each). They are now size-checked before decoding and the peer is dropped.
func TestPeerLoopDropsOversizedExtensionMessages(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extID byte
		size  int
	}{
		{"handshake", peer.ExtHandshake, peer.MaxExtHandshakeSize},
		{"ut_metadata", peer.LocalMetadataExtID, peer.MaxMetadataMessageSize},
		{"ut_pex", peer.LocalPEXExtID, peer.MaxPEXMessageSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newWireTestSession(t, 4, 16)
			w := startWirePeer(t, sess, 6210, fastReserved())

			// At the cap the message is decoded (and rejected as malformed)
			// without costing the connection.
			w.sendExtended(tc.extID, flatListPayload((tc.size-2)/2))
			w.barrier()

			w.sendExtended(tc.extID, flatListPayload(tc.size/2))
			w.waitClosed(2 * time.Second)
		})
	}
}

// TestPeerLoopCapsRepeatedExtensionHandshakes checks that only the first
// maxExtHandshakesPerConn extension handshakes are acted on: each one is decoded
// and can restart work such as metadata requests, so a peer must not be able to
// stream them.
func TestPeerLoopCapsRepeatedExtensionHandshakes(t *testing.T) {
	sess := newWireTestSession(t, 4, 16)
	sess.mu.Lock()
	sess.Torrent.InfoBytes = []byte("d4:name4:teste")
	sess.mu.Unlock()
	w := startWirePeer(t, sess, 6211, fastReserved())

	handshake := func(utMetadataID int) []byte {
		t.Helper()
		payload, err := peer.SerializeExtensionHandshakeWithExtensions(map[string]int{peer.ExtNameMetadata: utMetadataID}, 0)
		if err != nil {
			t.Fatalf("serialize handshake: %v", err)
		}
		return payload
	}
	for id := 1; id <= maxExtHandshakesPerConn; id++ {
		w.sendExtended(peer.ExtHandshake, handshake(id))
	}
	// Past the cap: ignored, so its new ut_metadata id must not take effect.
	w.sendExtended(peer.ExtHandshake, handshake(99))

	request, err := peer.SerializeMetadataRequest(0)
	if err != nil {
		t.Fatalf("serialize request: %v", err)
	}
	w.sendExtended(peer.LocalMetadataExtID, request)
	reply := w.expect(peer.MsgExtended, 2*time.Second)
	if got := reply.Payload[0]; got != byte(maxExtHandshakesPerConn) {
		t.Fatalf("metadata reply used extended id %d, want %d (handshake past the cap was applied)", got, maxExtHandshakesPerConn)
	}
}
