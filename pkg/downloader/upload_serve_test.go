package downloader

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TestUploadLimiterDoesNotBlockMessageLoop is the regression test for issue #59: with
// an upload limit configured, serving a throttled block must NOT stall the peer message
// loop. We seed a two-block piece to a mock peer under a tight upload limit and drive
// three requests through runPeerMessageLoop:
//
//   - block 0 — served at once from the fresh limiter burst;
//   - block 1 — throttled: its bytes cannot be reserved for ~2s;
//   - an out-of-range request — whose fast-extension reject is produced independently
//     of the upload limiter.
//
// The reject must come back promptly even while block 1 sits waiting for bandwidth.
// Under the old blocking UploadLimiter.Wait the loop parked inside the limiter serving
// block 1 and could not process the third request, so its reject was delayed by the
// whole throttle interval — the download-pump stall #59 reports. The throttled block
// must still arrive correctly once its tokens accrue.
func TestUploadLimiterDoesNotBlockMessageLoop(t *testing.T) {
	const numBlocks = 2
	pieceLen := int64(numBlocks * BlockSize)
	data := make([]byte, pieceLen)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}

	tor := &torrent.Torrent{
		Name:        "upload.bin",
		PieceLength: pieceLen,
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: pieceLen, Path: []string{"upload.bin"}}},
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: "upload.bin", Length: pieceLen}}, pieceLen)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}
	defer sess.Close()

	// Seed the piece: write its blocks and mark it complete so requests are served.
	if err := sess.Storage.WriteBlock(0, 0, data); err != nil {
		t.Fatalf("failed to seed piece data: %v", err)
	}
	sess.mu.Lock()
	sess.setPieceStateLocked(0, PieceCompleted)
	sess.mu.Unlock()

	// Tight upload limit: the fresh burst covers exactly one block, so block 0 serves
	// immediately but block 1 must wait ~2s for tokens — long enough that a blocking
	// serve would visibly delay the unrelated reject below.
	sess.SetUploadLimit(BlockSize / 2)

	clientConn, remoteConn := net.Pipe()
	defer remoteConn.Close()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)

	var reserved [8]byte
	peer.EnableFastExtension(&reserved) // so the out-of-range request is answered with a reject

	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:7300", "127.0.0.1", 7300, reserved, false)
		close(done)
	}()

	// Drain the loop's initial burst (HaveAll / NotInterested / any AllowedFast) until
	// the pipe goes quiet, so the message stream we assert on starts clean.
	drainInitialPeerMessages(t, remoteConn)
	_ = remoteConn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// Writer goroutine: unchoke us, then fire the three requests. Writes block until the
	// loop reads them (net.Pipe is synchronous); the main goroutine drains responses
	// concurrently so nothing deadlocks.
	go func() {
		_, _ = remoteConn.Write((&peer.Message{ID: peer.MsgInterested}).Serialize())
		sendBlockRequest(remoteConn, 0, 0, BlockSize)         // block 0: served from burst
		sendBlockRequest(remoteConn, 0, BlockSize, BlockSize) // block 1: throttled ~2s
		sendBlockRequest(remoteConn, 99, 0, BlockSize)        // out of range: fast reject
	}()

	reqStart := time.Now()
	var (
		gotReject   bool
		rejectAfter time.Duration
		block0      []byte
		block1      []byte
	)
	for block0 == nil || block1 == nil || !gotReject {
		msg, err := peer.ParseMessage(remoteConn)
		if err != nil {
			t.Fatalf("reading response failed (reject=%v, block0=%v, block1=%v): %v",
				gotReject, block0 != nil, block1 != nil, err)
		}
		if msg == nil {
			continue // keep-alive
		}
		switch msg.ID {
		case peer.MsgPiece:
			index, begin, payload := readPieceFields(t, msg)
			if index != 0 {
				t.Fatalf("piece for unexpected index %d", index)
			}
			switch begin {
			case 0:
				block0 = payload
			case BlockSize:
				block1 = payload
			default:
				t.Fatalf("piece for unexpected offset %d", begin)
			}
		case peer.MsgRejectRequest:
			if !gotReject {
				gotReject = true
				rejectAfter = time.Since(reqStart)
			}
		}
	}

	// The reject is not subject to the upload limiter, so it must return promptly even
	// though block 1 is still waiting on bandwidth. A delay near the ~2s throttle means
	// the loop was parked inside the limiter serving block 1 (the #59 regression).
	if rejectAfter > time.Second {
		t.Fatalf("fast-extension reject took %v; the upload limiter is blocking the message loop (issue #59)", rejectAfter)
	}

	// And the throttled block is still served correctly once its tokens accrue.
	if !bytes.Equal(block0, data[:BlockSize]) {
		t.Fatal("block 0 payload mismatch")
	}
	if !bytes.Equal(block1, data[BlockSize:]) {
		t.Fatal("block 1 (throttled) payload mismatch")
	}

	remoteConn.Close()
	<-done
}

// drainInitialPeerMessages reads and discards the messages runPeerMessageLoop sends
// right after startup, stopping once the pipe goes quiet (a read deadline fires with
// nothing pending). The connection stays usable afterward.
func drainInitialPeerMessages(t *testing.T, c net.Conn) {
	t.Helper()
	for {
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if _, err := peer.ParseMessage(c); err != nil {
			return
		}
	}
}

func sendBlockRequest(c net.Conn, index, begin, length uint32) {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[0:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:12], length)
	_, _ = c.Write((&peer.Message{ID: peer.MsgRequest, Payload: payload}).Serialize())
}

func readPieceFields(t *testing.T, msg *peer.Message) (index, begin uint32, payload []byte) {
	t.Helper()
	if len(msg.Payload) < 8 {
		t.Fatalf("short piece payload: %d bytes", len(msg.Payload))
	}
	index = binary.BigEndian.Uint32(msg.Payload[0:4])
	begin = binary.BigEndian.Uint32(msg.Payload[4:8])
	return index, begin, msg.Payload[8:]
}
