package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TestRemoveDownloadShrinksAndCompletesOutOfOrder exercises removeDownload's
// slice-shrink path (#63) with several pieces in flight at once, completed out
// of order, so the fix (nil-ing the vacated tail slot after the append-based
// shift) runs repeatedly against a shrinking/growing activeDownloads slice
// instead of just the trivial single-entry case. It also keeps the connection
// open after the torrent finishes — mimicking an idle seeding connection — to
// confirm the completed download's activeDownload entries are gone and the
// connection keeps working normally afterward (no corruption from the shift).
func TestRemoveDownloadShrinksAndCompletesOutOfOrder(t *testing.T) {
	const pieceLen = BlockSize // one block per piece keeps request/response 1:1
	const numPieces = 5
	total := pieceLen * numPieces

	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i*13 + 1)
	}
	hashes := make([][20]byte, numPieces)
	for p := 0; p < numPieces; p++ {
		hashes[p] = sha1.Sum(data[p*pieceLen : (p+1)*pieceLen])
	}

	tor := &torrent.Torrent{
		Name:        "removedl.bin",
		PieceLength: int64(pieceLen),
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(total), Path: []string{"removedl.bin"}}},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		hs, err := peer.ParseHandshake(conn)
		if err != nil {
			return
		}
		var pid [20]byte
		copy(pid[:], "-MOCK01-removedltest")
		_, _ = conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize())
		// Bitfield advertising all 5 pieces: bits 0..4 set -> 0b11111000.
		_, _ = conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0xF8}}).Serialize())

		serve := func(index, begin, length uint32) {
			block := data[int(index)*pieceLen+int(begin) : int(index)*pieceLen+int(begin)+int(length)]
			payload := make([]byte, 8+len(block))
			binary.BigEndian.PutUint32(payload[0:4], index)
			binary.BigEndian.PutUint32(payload[4:8], begin)
			copy(payload[8:], block)
			_, _ = conn.Write((&peer.Message{ID: peer.MsgPiece, Payload: payload}).Serialize())
		}

		type req struct{ index, begin, length uint32 }
		var queue []req

		// Once requests for all 5 pieces have arrived, serve them out of
		// order (reversed) so removeDownload shrinks activeDownloads from
		// varying positions rather than always the head or tail.
		for {
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				return
			}
			if msg == nil {
				continue
			}
			switch msg.ID {
			case peer.MsgInterested:
				_, _ = conn.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			case peer.MsgRequest:
				if len(msg.Payload) != 12 {
					continue
				}
				index := binary.BigEndian.Uint32(msg.Payload[0:4])
				begin := binary.BigEndian.Uint32(msg.Payload[4:8])
				length := binary.BigEndian.Uint32(msg.Payload[8:12])
				queue = append(queue, req{index, begin, length})
				if len(queue) >= numPieces {
					for i := len(queue) - 1; i >= 0; i-- {
						serve(queue[i].index, queue[i].begin, queue[i].length)
					}
					queue = nil
				}
			}
		}
	}()

	dir := t.TempDir()
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "removedl.bin", Length: int64(total)}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	var pid [20]byte
	copy(pid[:], "-ST0001-removedltest")
	sess, err := NewSession(tor, st, pid, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	completed := false
	for i := 0; i < 200; i++ {
		if sess.PercentComplete() == 100.0 {
			completed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("download did not complete; %.1f%% done", sess.PercentComplete())
	}

	got, err := os.ReadFile(filepath.Join(dir, "removedl.bin"))
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("downloaded content does not match source")
	}

	// The connection should still be alive and functional now that the
	// torrent is complete (the peer loop keeps running for seeding); confirm
	// the session still reports the peer as connected instead of having
	// panicked or wedged from a corrupted activeDownloads slice.
	stats := sess.GetUploadPeerStats()
	if stats.Connected < 1 {
		t.Fatalf("expected peer connection to remain alive after completion, stats=%+v", stats)
	}
}
