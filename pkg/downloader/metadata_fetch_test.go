package downloader

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
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
	// A session whose metadata cannot be used drops its peers (see
	// metadataStalledLocked); until then it must not ask again either.
	after := w.collect(300 * time.Millisecond)
	w.waitClosed(2 * time.Second)

	sess.mu.RLock()
	statusErr := sess.statusErr
	sess.mu.RUnlock()
	if statusErr == nil {
		t.Fatal("expected the storage failure to be reported")
	}
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

// startWirePeerAtA2 is startWirePeer for a connection from ip (a documentation
// address such as 203.0.113.x), so admission treats it as a remote host: its
// blocks are attributed to it and it can be banned. Loopback is exempt.
func startWirePeerAtA2(t *testing.T, sess *Session, ip string, port uint16) *wirePeer {
	t.Helper()
	clientConn, remoteConn := net.Pipe()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
	w := &wirePeer{
		t:      t,
		remote: remoteConn,
		in:     make(chan *peer.Message, 4096),
		done:   make(chan struct{}),
	}
	addr := net.JoinHostPort(ip, strconv.Itoa(int(port)))
	go func() {
		sess.runPeerMessageLoop(client, clientConn, addr, ip, port, fastReserved(), false)
		close(w.done)
	}()
	go func() {
		defer close(w.in)
		for {
			msg, err := peer.ParseMessage(remoteConn)
			if err != nil {
				return
			}
			if msg != nil {
				w.in <- msg
			}
		}
	}()
	t.Cleanup(w.close)
	return w
}

// metadataReplyA2 builds a ut_metadata message of msgType for block piece,
// carrying that block of info for a data message. It is safe off the test
// goroutine.
func metadataReplyA2(msgType, piece int, info []byte) []byte {
	dict := map[string]interface{}{"msg_type": int64(msgType), "piece": int64(piece)}
	if msgType == peer.MetadataData {
		dict["total_size"] = int64(len(info))
	}
	payload, _ := bencode.Marshal(dict)
	if msgType == peer.MetadataData {
		start := min(piece*peer.MetadataBlockSize, len(info))
		payload = append(payload, info[start:min(start+peer.MetadataBlockSize, len(info))]...)
	}
	return append([]byte{peer.LocalMetadataExtID}, payload...)
}

// metadataRequestPiece returns the block msg asks for when it is a ut_metadata
// request on extID.
func metadataRequestPieceA2(msg *peer.Message, extID byte) (int, bool) {
	if msg.ID != peer.MsgExtended || len(msg.Payload) < 2 || msg.Payload[0] != extID {
		return 0, false
	}
	m, err := peer.ParseMetadataMessage(msg.Payload[1:])
	if err != nil || m.MsgType != peer.MetadataRequest {
		return 0, false
	}
	return m.Piece, true
}

// answerMetadataA2 answers every ut_metadata request w receives on extID with
// the block of info (junk or not) it asks for, until the connection closes.
// Nothing else may read w.in afterwards.
func answerMetadataA2(w *wirePeer, extID byte, info []byte) {
	go func() {
		for msg := range w.in {
			piece, ok := metadataRequestPieceA2(msg, extID)
			if !ok {
				continue
			}
			_ = w.remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := w.remote.Write((&peer.Message{ID: peer.MsgExtended, Payload: metadataReplyA2(peer.MetadataData, piece, info)}).Serialize()); err != nil {
				return
			}
		}
	}()
}

// noMetadataRequestsA2 fails if w is asked for metadata on extID within idle.
func noMetadataRequestsA2(t *testing.T, w *wirePeer, extID byte, idle time.Duration, what string) {
	t.Helper()
	for _, msg := range w.collect(idle) {
		if piece, ok := metadataRequestPieceA2(msg, extID); ok {
			t.Fatalf("%s: asked for metadata block %d", what, piece)
		}
	}
}

func waitMetadataCompleteWithinA2(t *testing.T, sess *Session, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
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

// TestMetadataSoleJunkSupplierIsBannedA2 covers ut_metadata poisoning by a peer
// that advertises the true size and answers first with junk. A failed assembly
// was blamed on nobody, so it was asked again every round and stalled the magnet
// for good. An assembly fed entirely by one host proves that host lied: it is
// banned and disconnected, and an honest peer then completes the fetch.
func TestMetadataSoleJunkSupplierIsBannedA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	const junkIP = "203.0.113.10"

	junk := startWirePeerAtA2(t, sess, junkIP, 6301)
	junk.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, len(info)))
	junk.expectMetadataRequests(4, 0, 1)
	bogus := make([]byte, len(info))
	junk.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 0))
	junk.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 1))
	junk.waitClosed(2 * time.Second)

	sess.mu.RLock()
	banned := sess.refusesIncomingLocked(&net.TCPAddr{IP: net.ParseIP(junkIP), Port: 7000})
	sess.mu.RUnlock()
	if !banned {
		t.Fatal("the sole supplier of a poisoned info dict was not banned")
	}
	startWirePeerAtA2(t, sess, junkIP, 6302).waitClosed(2 * time.Second)

	honest := startWirePeerAtA2(t, sess, "203.0.113.11", 6303)
	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	honest.expectMetadataRequests(3, 0, 1)
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)

	sess.mu.RLock()
	defer sess.mu.RUnlock()
	if sess.metadataSuspects != nil || sess.metadataFailStreak != 0 || sess.metadataSolo {
		t.Fatal("blame state survived a verified info dict")
	}
}

// TestMetadataMixedRoundGoesSoloA2 covers a junk peer answering the same size as
// an honest one: every peer was asked for every block, the first answer won, so
// the junk peer spoiled a block of each round and was never pinned down. A
// round fed by several hosts makes them all suspects and runs the next round
// from a single connection, whose failure is attributable, so the magnet
// resolves.
func TestMetadataMixedRoundGoesSoloA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataSizeStallTimeout, 300*time.Millisecond)()
	defer swapDuration(&metadataRoundBackoffBase, 50*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	bogus := make([]byte, len(info))

	honest := startWirePeerAtA2(t, sess, "203.0.113.20", 6311)
	junk := startWirePeerAtA2(t, sess, "203.0.113.21", 6312)
	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	honest.expectMetadataRequests(3, 0, 1)
	junk.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, len(info)))
	junk.expectMetadataRequests(4, 0, 1)

	junk.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 0))
	junk.barrier()
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	honest.barrier()

	sess.mu.RLock()
	solo, suspects, strikes := sess.metadataSolo, len(sess.metadataSuspects), len(sess.admission.strikes)
	sess.mu.RUnlock()
	if !solo || suspects != 2 || strikes != 0 {
		t.Fatalf("after a mixed failed round: solo %v, %d suspects, %d strike entries; want solo, 2, 0", solo, suspects, strikes)
	}

	// From here each peer answers whatever it is asked, the junk peer with junk.
	answerMetadataA2(honest, 3, info)
	answerMetadataA2(junk, 4, bogus)
	waitMetadataCompleteWithinA2(t, sess, 5*time.Second)
}

// TestMetadataBogusSizerWaitsAfterFailedRoundA2 covers a poisoner advertising a
// bogus metadata_size: after its round failed, its own loop re-sized the new
// round at once, before any honest loop woke, so the true size never got a
// round. Hosts that sized or fed a failed round now wait metadataSizeStallTimeout
// before sizing again, and the honest size wins.
func TestMetadataBogusSizerWaitsAfterFailedRoundA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataSizeStallTimeout, 2*time.Second)()
	sess, info := newMagnetTestSession(t, 2)
	bogusSize := len(info) + peer.MetadataBlockSize
	bogus := make([]byte, bogusSize)

	p1 := startWirePeerAtA2(t, sess, "203.0.113.30", 6321)
	p1.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, bogusSize))
	p1.expectMetadataRequests(4, 0, 1, 2)
	p2 := startWirePeerAtA2(t, sess, "203.0.113.31", 6322)
	p2.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 5, bogusSize))
	p2.expectMetadataRequests(5, 0, 1, 2)
	honest := startWirePeerAtA2(t, sess, "203.0.113.32", 6323)
	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))

	p1.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 0))
	p1.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 1))
	p1.barrier()
	failedAt := time.Now()
	p2.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, bogus, 2))
	// Both poisoners' loops run the new-round check on their next message.
	p2.barrier()
	p1.barrier()

	honest.expectMetadataRequests(3, 0, 1)
	if el := time.Since(failedAt); el >= metadataSizeStallTimeout {
		t.Fatalf("the true size took over only after %v", el)
	}
	sess.mu.RLock()
	size, sizedBy := sess.metadataSize, sess.metadataSizedBy
	sess.mu.RUnlock()
	if size != len(info) || sizedBy != "203.0.113.32" {
		t.Fatalf("new round sized %d by %q, want %d by the honest peer", size, sizedBy, len(info))
	}
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)
	noMetadataRequestsA2(t, p1, 4, 50*time.Millisecond, "a suspect sizer")
	noMetadataRequestsA2(t, p2, 5, 50*time.Millisecond, "a suspect sizer")
}

// TestMetadataDripSizerIsTakenOverA2 covers a peer that sizes the round with a
// bogus size and then answers one block just inside every stall timeout: the
// stall clock never ran out, so peers advertising the true size were never
// asked for hours. A round fed slower than one block per metadataMinBlockPeriod
// (after its grace period) is taken over, and its sizer becomes a suspect.
func TestMetadataDripSizerIsTakenOverA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataSizeStallTimeout, time.Second)()
	defer swapDuration(&metadataMinBlockPeriod, 100*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	const blocks = 10
	bogusSize := len(info) + (blocks-2)*peer.MetadataBlockSize
	bogus := make([]byte, bogusSize)

	drip := startWirePeerAtA2(t, sess, "203.0.113.40", 6331)
	drip.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, bogusSize))
	want := make([]int, blocks)
	for i := range want {
		want[i] = i
	}
	drip.expectMetadataRequests(4, want...)
	honest := startWirePeerAtA2(t, sess, "203.0.113.41", 6332)
	honest.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for i := 0; i < blocks; i++ {
			select {
			case <-time.After(700 * time.Millisecond): // inside the 1 s stall timeout
			case <-stop:
				return
			}
			_ = drip.remote.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := drip.remote.Write((&peer.Message{ID: peer.MsgExtended, Payload: metadataReplyA2(peer.MetadataData, i, bogus)}).Serialize()); err != nil {
				return
			}
		}
	}()

	honest.expectMetadataRequests(3, 0, 1)
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	honest.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)
}

// TestMetadataRejectedAndUnansweredBlocksAreReaskedA2 covers a single source
// that rejects one request (as libtorrent does while rate limiting) and drops
// another: neither block was asked for again that round, so the magnet stalled
// until the connection was replaced. An unanswered block is asked again after
// metadataRequestTimeout, a rejected one after metadataRejectRetryAfter.
func TestMetadataRejectedAndUnansweredBlocksAreReaskedA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataRequestTimeout, 200*time.Millisecond)()
	defer swapDuration(&metadataRejectRetryAfter, 600*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)

	w := startWirePeerAtA2(t, sess, "203.0.113.50", 6341)
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	w.expectMetadataRequests(3, 0, 1)
	start := time.Now()
	w.send(&peer.Message{ID: peer.MsgExtended, Payload: metadataReplyA2(peer.MetadataReject, 0, nil)})

	reasked := make(map[int]time.Duration)
	deadline := time.After(3 * time.Second)
	for len(reasked) < 2 {
		select {
		case msg, ok := <-w.in:
			if !ok {
				t.Fatal("connection closed")
			}
			if piece, ok := metadataRequestPieceA2(msg, 3); ok {
				if _, seen := reasked[piece]; !seen {
					reasked[piece] = time.Since(start)
				}
			}
		case <-deadline:
			t.Fatalf("blocks never asked again: %v", reasked)
		}
	}
	if reasked[1] < metadataRequestTimeout {
		t.Fatalf("unanswered block asked again after %v, want at least %v", reasked[1], metadataRequestTimeout)
	}
	if reasked[0] < metadataRejectRetryAfter {
		t.Fatalf("rejected block asked again after %v, want at least %v", reasked[0], metadataRejectRetryAfter)
	}
	w.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	w.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 1))
	waitMetadataComplete(t, sess)
}

// TestMetadataSoloOwnerHandoverA2 checks a solo round survives its owner going
// quiet: once it has taken no block for metadataSizeStallTimeout another
// connection owns the round, and what the old owner supplied is dropped so the
// round keeps a single supplier.
func TestMetadataSoloOwnerHandoverA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	defer swapDuration(&metadataSizeStallTimeout, 300*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 3)
	sess.mu.Lock()
	sess.metadataSolo = true
	sess.mu.Unlock()

	quiet := startWirePeerAtA2(t, sess, "203.0.113.60", 6351)
	quiet.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 4, len(info)))
	quiet.expectMetadataRequests(4, 0, 1, 2)
	other := startWirePeerAtA2(t, sess, "203.0.113.61", 6352)
	other.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	noMetadataRequestsA2(t, other, 3, 100*time.Millisecond, "a connection that does not own the solo round")

	quiet.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, 0))
	other.expectMetadataRequests(3, 0, 1, 2)
	sess.mu.RLock()
	fromQuiet := slices.Contains(sess.metadataFrom, "203.0.113.60")
	sess.mu.RUnlock()
	if fromQuiet {
		t.Fatal("the old owner's block stayed in the handed-over round")
	}
	for i := 0; i < 3; i++ {
		other.sendExtended(peer.LocalMetadataExtID, metadataDataPayload(t, info, i))
	}
	waitMetadataComplete(t, sess)
}

// TestMetadataRoundBackoffA2 checks failed rounds in a row back off new rounds:
// none after the first failure, then metadataRoundBackoffBase doubling up to
// metadataRoundBackoffMax, and no peer sizes a round before it is due.
func TestMetadataRoundBackoffA2(t *testing.T) {
	defer swapDuration(&metadataRetryInterval, 20*time.Millisecond)()
	sess, info := newMagnetTestSession(t, 2)
	now := time.Now()
	fail := func() time.Duration {
		sess.metadataPieces = []bool{true, true}
		sess.metadataFrom = []string{"", ""}
		sess.noteMetadataRoundFailedLocked(now)
		if sess.metadataNextRoundAt.IsZero() {
			return 0
		}
		return sess.metadataNextRoundAt.Sub(now)
	}
	sess.mu.Lock()
	got := []time.Duration{fail(), fail(), fail()}
	for i := 0; i < 40; i++ {
		fail()
	}
	got = append(got, fail())
	sess.metadataPieces, sess.metadataFrom = nil, nil
	sess.metadataNextRoundAt = time.Now().Add(300 * time.Millisecond)
	sess.mu.Unlock()
	want := []time.Duration{0, metadataRoundBackoffBase, 2 * metadataRoundBackoffBase, metadataRoundBackoffMax}
	if !slices.Equal(got, want) {
		t.Fatalf("round backoff after 1, 2, 3 and 44 failures = %v, want %v", got, want)
	}

	start := time.Now()
	w := startWirePeerAtA2(t, sess, "203.0.113.70", 6361)
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 3, len(info)))
	w.expectMetadataRequests(3, 0, 1)
	if el := time.Since(start); el < 300*time.Millisecond {
		t.Fatalf("a round started %v into a 300ms backoff", el)
	}
}

// TestMetadataSuspectsStayBoundedA2 checks the suspect set cannot grow without
// bound, forgets entries after metadataSuspectTTL, and never holds loopback.
func TestMetadataSuspectsStayBoundedA2(t *testing.T) {
	sess, _ := newMagnetTestSession(t, 1)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	now := time.Now()
	const hosts = 3 * maxMetadataSuspects
	for i := 0; i < hosts; i++ {
		sess.addMetadataSuspectLocked(fmt.Sprintf("198.51.100.%d", i), now.Add(time.Duration(i)*time.Millisecond))
	}
	if n := len(sess.metadataSuspects); n != maxMetadataSuspects {
		t.Fatalf("suspect set holds %d hosts, want %d", n, maxMetadataSuspects)
	}
	later := now.Add(time.Second)
	if !sess.metadataSuspectLocked(fmt.Sprintf("198.51.100.%d", hosts-1), later) {
		t.Fatal("the newest suspect was evicted")
	}
	if sess.metadataSuspectLocked("198.51.100.0", later) {
		t.Fatal("the oldest suspect was kept over newer ones")
	}
	if sess.metadataSuspectLocked(fmt.Sprintf("198.51.100.%d", hosts-1), now.Add(metadataSuspectTTL+time.Second)) {
		t.Fatal("a suspect outlived metadataSuspectTTL")
	}
	sess.addMetadataSuspectLocked("", now)
	if sess.metadataSuspectLocked("", now) || len(sess.metadataSuspects) > maxMetadataSuspects {
		t.Fatal("loopback became a suspect")
	}
}
