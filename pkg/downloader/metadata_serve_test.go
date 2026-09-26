package downloader

import (
	"bytes"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// metadataReply reads the next extended message and parses it as a ut_metadata
// message sent on extID.
func (w *wirePeer) metadataReply(extID byte) *peer.MetadataMessage {
	w.t.Helper()
	msg := w.expect(peer.MsgExtended, 2*time.Second)
	if msg.Payload[0] != extID {
		w.t.Fatalf("reply on extended id %d, want %d", msg.Payload[0], extID)
	}
	m, err := peer.ParseMetadataMessage(msg.Payload[1:])
	if err != nil {
		w.t.Fatalf("parse metadata reply: %v", err)
	}
	return m
}

func (w *wirePeer) requestMetadata(piece int) {
	w.t.Helper()
	payload, err := peer.SerializeMetadataRequest(piece)
	if err != nil {
		w.t.Fatalf("serialize request: %v", err)
	}
	w.sendExtended(peer.LocalMetadataExtID, payload)
}

// newMetadataServingSession is a seeding-capable session that knows its info dict.
func newMetadataServingSession(t *testing.T, info []byte, private bool) *Session {
	t.Helper()
	sess := newWireTestSession(t, 4, 16)
	sess.mu.Lock()
	sess.Torrent.InfoBytes = info
	sess.Torrent.Private = private
	sess.mu.Unlock()
	return sess
}

// TestMetadataServeIsBudgetedAndCounted is the regression test for unmetered
// ut_metadata serving: every request was answered inline with a 16 KiB block, with
// no per-peer limit and no upload accounting, so a peer could re-request forever.
// Serving is now capped at metadataServeRequestsPerBlock x blocks per window and
// counted as upload.
func TestMetadataServeIsBudgetedAndCounted(t *testing.T) {
	info := bytes.Repeat([]byte("i"), 100) // one block
	sess := newMetadataServingSession(t, info, false)
	w := startWirePeer(t, sess, 6230, fastReserved())
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 5, 0))

	for i := 0; i < metadataServeRequestsPerBlock; i++ {
		w.requestMetadata(0)
		if reply := w.metadataReply(5); reply.MsgType != peer.MetadataData || !bytes.Equal(reply.Data, info) {
			t.Fatalf("request %d: got msg_type %d, want the data block", i, reply.MsgType)
		}
	}
	w.requestMetadata(0)
	if reply := w.metadataReply(5); reply.MsgType != peer.MetadataReject {
		t.Fatalf("request past the budget got msg_type %d, want reject", reply.MsgType)
	}

	want := int64(metadataServeRequestsPerBlock * len(info))
	if got := sess.Uploaded.Load(); got != want {
		t.Fatalf("session Uploaded = %d, want %d", got, want)
	}
	sess.mu.RLock()
	pState := sess.Peers["127.0.0.1:6230"]
	sess.mu.RUnlock()
	if got := atomic.LoadInt64(&pState.Uploaded); got != want {
		t.Fatalf("peer Uploaded = %d, want %d", got, want)
	}
}

// TestMetadataServeHonoursUploadLimit checks that served metadata is charged to the
// upload limiter without blocking the loop: a block the limiter cannot cover right
// now is rejected instead of bypassing the user's limit.
func TestMetadataServeHonoursUploadLimit(t *testing.T) {
	info := bytes.Repeat([]byte("j"), 2000)
	sess := newMetadataServingSession(t, info, false)
	sess.SetUploadLimit(1000) // the bucket starts (nearly) empty and fills at 1 KB/s
	w := startWirePeer(t, sess, 6231, fastReserved())
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 5, 0))

	w.requestMetadata(0)
	if reply := w.metadataReply(5); reply.MsgType != peer.MetadataReject {
		t.Fatalf("block the upload limiter cannot cover got msg_type %d, want reject", reply.MsgType)
	}

	// Once the bucket holds enough tokens the block is served and charged.
	sess.UploadLimiter.mu.Lock()
	sess.UploadLimiter.tokens = sess.UploadLimiter.maxTokens
	sess.UploadLimiter.mu.Unlock()
	w.requestMetadata(0)
	if reply := w.metadataReply(5); reply.MsgType != peer.MetadataData {
		t.Fatalf("block within the upload limit got msg_type %d, want data", reply.MsgType)
	}
	sess.UploadLimiter.mu.Lock()
	left := sess.UploadLimiter.tokens
	maxTokens := sess.UploadLimiter.maxTokens
	sess.UploadLimiter.mu.Unlock()
	if left > maxTokens-float64(len(info))+100 { // allow for refill during the test
		t.Fatalf("limiter tokens %v of %v after serving %d bytes; the block was not charged", left, maxTokens, len(info))
	}
}

// TestPrivateTorrentDoesNotShareMetadata covers BEP 27: ut_metadata used to be
// advertised (with metadata_size) and served for private torrents, handing the
// private info dict to anyone who knew the infohash.
func TestPrivateTorrentDoesNotShareMetadata(t *testing.T) {
	info := []byte("d4:name7:private7:privatei1ee")
	sess := newMetadataServingSession(t, info, true)
	reserved := fastReserved()
	reserved[5] |= 0x10 // extension protocol, so we send our handshake
	w := startWirePeer(t, sess, 6232, reserved)

	ours := w.expect(peer.MsgExtended, 2*time.Second)
	hs, err := peer.ParseExtensionHandshake(ours.Payload[1:])
	if err != nil {
		t.Fatalf("parse our handshake: %v", err)
	}
	if _, ok := hs.Extensions[peer.ExtNameMetadata]; ok || hs.MetadataSize != 0 {
		t.Fatalf("private torrent advertised ut_metadata %v with metadata_size %d", hs.Extensions, hs.MetadataSize)
	}

	// A peer asking anyway (e.g. using an id from an earlier handshake) is refused.
	w.sendExtended(peer.ExtHandshake, extHandshakePayload(t, 5, 0))
	w.requestMetadata(0)
	if reply := w.metadataReply(5); reply.MsgType != peer.MetadataReject {
		t.Fatalf("private metadata request got msg_type %d, want reject", reply.MsgType)
	}
}
