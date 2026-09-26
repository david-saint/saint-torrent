package downloader

import (
	"crypto/sha1"
	"errors"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// newMagnetTestSession returns a metadata-mode session for an info dict that
// spans numBlocks ut_metadata blocks, plus the dict itself.
func newMagnetTestSession(t *testing.T, numBlocks int) (*Session, []byte) {
	t.Helper()
	sess, info := newUnclosedMagnetTestSession(t, numBlocks)
	t.Cleanup(func() { sess.Close() })
	return sess, info
}

// newUnclosedMagnetTestSession is newMagnetTestSession without the Close cleanup,
// for tests where a wedged session must not hang the test binary.
func newUnclosedMagnetTestSession(t *testing.T, numBlocks int) (*Session, []byte) {
	t.Helper()
	// Piece hashes dominate the dict: aim for the middle of the last block.
	numPieces := ((numBlocks-1)*peer.MetadataBlockSize + peer.MetadataBlockSize/2) / 20
	info := map[string]interface{}{
		"name":         "magnet-fetch.bin",
		"piece length": int64(16),
		"pieces":       string(make([]byte, 20*numPieces)),
		"length":       int64(16 * numPieces),
	}
	infoBytes, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("marshal info: %v", err)
	}
	if got := (len(infoBytes) + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize; got != numBlocks {
		t.Fatalf("fixture info dict spans %d metadata blocks, want %d", got, numBlocks)
	}
	tor := &torrent.Torrent{Name: "magnet-fetch", InfoHash: sha1.Sum(infoBytes)}
	sess, err := NewSession(tor, nil, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	return sess, infoBytes
}

func extHandshakePayload(t *testing.T, utMetadataID, metadataSize int) []byte {
	t.Helper()
	payload, err := peer.SerializeExtensionHandshakeWithExtensions(map[string]int{peer.ExtNameMetadata: utMetadataID}, metadataSize)
	if err != nil {
		t.Fatalf("serialize handshake: %v", err)
	}
	return payload
}

// metadataDataPayload builds a ut_metadata data message (msg_type 1) for block
// piece of info.
func metadataDataPayload(t *testing.T, info []byte, piece int) []byte {
	t.Helper()
	dict, err := bencode.Marshal(map[string]interface{}{
		"msg_type":   int64(peer.MetadataData),
		"piece":      int64(piece),
		"total_size": int64(len(info)),
	})
	if err != nil {
		t.Fatalf("marshal data dict: %v", err)
	}
	end := min((piece+1)*peer.MetadataBlockSize, len(info))
	return append(dict, info[piece*peer.MetadataBlockSize:end]...)
}

// expectMetadataRequests reads messages until it has seen a ut_metadata request
// for every block in want, sent on extended id extID.
func (w *wirePeer) expectMetadataRequests(extID byte, want ...int) {
	w.t.Helper()
	pending := make(map[int]bool, len(want))
	for _, p := range want {
		pending[p] = true
	}
	deadline := time.After(3 * time.Second)
	for len(pending) > 0 {
		select {
		case msg, ok := <-w.in:
			if !ok {
				w.t.Fatal("connection closed while waiting for metadata requests")
			}
			if msg.ID != peer.MsgExtended || len(msg.Payload) < 2 || msg.Payload[0] != extID {
				continue
			}
			m, err := peer.ParseMetadataMessage(msg.Payload[1:])
			if err != nil || m.MsgType != peer.MetadataRequest {
				continue
			}
			delete(pending, m.Piece)
		case <-deadline:
			w.t.Fatalf("timed out waiting for metadata requests; still missing %v", pending)
		}
	}
}

func waitMetadataComplete(t *testing.T, sess *Session) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		sess.mu.RLock()
		done := !sess.metadataMode
		lastErr := sess.lastErr
		sess.mu.RUnlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metadata never completed (last error: %v)", lastErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestMetadataRequestLoopSurvivesConcurrentReset is the regression test for the
// ut_metadata request-loop panic: the loop indexed s.metadataPieces with a bound
// taken before it started, dropping s.mu (and blocking on a socket write) between
// blocks. A concurrent failed assembly nils the slice, so the next index panicked
// under s.mu; with no recover anywhere that either killed the process or, as here,
// wedged it when a deferred cleanup re-took s.mu. net.Pipe makes each request
// write block until the test reads it, so the reset lands mid-loop.
func TestMetadataRequestLoopSurvivesConcurrentReset(t *testing.T) {
	// Three blocks: whichever side wins the race to the second block, the old loop
	// reads the third only after the test has reset the accumulator.
	sess, info := newUnclosedMagnetTestSession(t, 3)
	clientConn, remote := net.Pipe()
	defer remote.Close()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:6220", "127.0.0.1", 6220, fastReserved(), false)
		close(done)
	}()
	loopExited := false
	t.Cleanup(func() {
		if loopExited {
			sess.Close() // a wedged session would block Close forever
		}
	})
	_ = remote.SetDeadline(time.Now().Add(5 * time.Second))

	hs := (&peer.Message{ID: peer.MsgExtended, Payload: append([]byte{peer.ExtHandshake}, extHandshakePayload(t, 3, len(info))...)}).Serialize()
	if _, err := remote.Write(hs); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	first, err := peer.ParseMessage(remote)
	if err != nil || first == nil || first.ID != peer.MsgExtended {
		t.Fatalf("expected the first metadata request, got %#v (%v)", first, err)
	}

	// The loop is now blocked writing the second request. A failed assembly
	// elsewhere discards the accumulator in the meantime.
	if err := sess.onMetadataDownloaded([]byte("not the info dict")); err == nil {
		t.Fatal("bogus metadata passed the infohash check")
	}

	// Keep draining; the loop must keep serving the connection.
	go func() {
		for {
			if _, err := peer.ParseMessage(remote); err != nil {
				return
			}
		}
	}()
	barrier := (&peer.Message{ID: peer.MsgRequest, Payload: blockPayload(0xffffffff, 0, 1)}).Serialize()
	if _, err := remote.Write(barrier); err != nil {
		t.Fatalf("peer loop stopped reading after the reset: %v", err)
	}
	_ = remote.Close()
	select {
	case <-done:
		loopExited = true
	case <-time.After(5 * time.Second):
		t.Fatal("peer loop wedged after the metadata reset (stale-index panic under s.mu)")
	}
}

// TestMetadataIgnoresUnsolicitedBlocks covers ut_metadata poisoning: a data block
// used to be accepted from any peer for any missing piece, so a peer we never
// asked could slip a bogus block into the assembly an honest peer was feeding and
// make it fail the infohash check. Only blocks asked of that peer now count.
func TestMetadataIgnoresUnsolicitedBlocks(t *testing.T) {
	sess, info := newMagnetTestSession(t, 2)
	honest := startWirePeer(t, sess, 6221, fastReserved())
	attacker := startWirePeer(t, sess, 6222, fastReserved())

	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	honest.expectMetadataRequests(3, 0, 1)

	// The attacker never advertised ut_metadata and was never asked for anything.
	bogus := make([]byte, len(info))
	attacker.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 0))
	attacker.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 1))
	attacker.barrier()

	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)
}

// TestMetadataResetReRequestsConnectedPeers checks recovery from a poisoned
// assembly: after a failed infohash check discards the accumulator, peers that are
// already connected must be asked again. Requests used to go out only when a
// peer's extension handshake arrived, so honest peers already connected were never
// re-asked and the fetch waited for new connections.
func TestMetadataResetReRequestsConnectedPeers(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	honest := startWirePeer(t, sess, 6223, fastReserved())
	attacker := startWirePeer(t, sess, 6224, fastReserved())

	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	honest.expectMetadataRequests(3, 0, 1)
	attacker.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, len(info)))
	attacker.expectMetadataRequests(4, 0, 1)

	// The attacker answers first with a full, correctly sized, wrong dict.
	bogus := make([]byte, len(info))
	attacker.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 0))
	attacker.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 1))

	// The honest peer is asked again without sending anything itself.
	honest.expectMetadataRequests(3, 0, 1)
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)
}

// TestMetadataNoRefetchLoopOnStorageFailure checks the new-round re-requests stop
// while the session has a blocking error: when storage cannot be set up, every
// completed fetch fails and resets the accumulator, and re-asking connected peers
// each time would re-download the metadata in an endless loop.
func TestMetadataNoRefetchLoopOnStorageFailure(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	sess.storageFactory = func(string, []storage.FileInfo, int64) (storage.Storage, error) {
		return nil, errors.New("disk unavailable")
	}
	w := startWirePeer(t, sess, 6225, fastReserved())
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	w.expectMetadataRequests(3, 0, 1)
	w.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	w.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	// The barrier's own message already runs the new-round check.
	after := w.barrier()

	sess.mu.RLock()
	statusErr := sess.statusErr
	sess.mu.RUnlock()
	if statusErr == nil {
		t.Fatal("expected the storage failure to be reported")
	}
	after = append(after, w.collect(300*time.Millisecond)...)
	for _, msg := range after {
		if msg.ID == peer.MsgExtended {
			t.Fatal("peer was asked for metadata again after a storage failure")
		}
	}
}

// TestMetadataStalledSizeIsReplaced covers a peer that is first to advertise a
// bogus metadata_size and then never answers. The accumulator took its size, and
// peers advertising the true size were never asked, so the magnet stalled for
// good (an inbound peer is not even reaped). Once the accumulator has gone
// metadataSizeStallTimeout without a block, an honest peer's size takes over.
func TestMetadataStalledSizeIsReplaced(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataSizeStallTimeout, 300*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)

	attacker := startWirePeer(t, sess, 6226, fastReserved())
	attacker.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, len(info)+peer.MetadataBlockSize))
	attacker.expectMetadataRequests(4, 0, 1, 2)
	// The attacker never answers; its messages pile up unread in attacker.in.

	honest := startWirePeer(t, sess, 6227, fastReserved())
	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	// Answer every request the honest peer gets until the fetch completes.
	deadline := time.After(5 * time.Second)
	for {
		sess.mu.RLock()
		done := !sess.metadataMode
		sess.mu.RUnlock()
		if done {
			return
		}
		select {
		case msg, ok := <-honest.in:
			if !ok {
				t.Fatal("honest peer was disconnected")
			}
			if msg.ID != peer.MsgExtended || len(msg.Payload) < 2 || msg.Payload[0] != 3 {
				continue
			}
			m, err := peer.ParseMetadataMessage(msg.Payload[1:])
			if err != nil || m.MsgType != peer.MetadataRequest {
				continue
			}
			honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, m.Piece))
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatal("the honest peer's metadata_size never replaced the stalled one")
		}
	}
}
