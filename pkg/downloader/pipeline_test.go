package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TestMultiPiecePipelineSpansPieces proves the per-peer request window spans
// multiple pieces rather than being capped at a single piece's block count.
//
// The torrent uses single-block pieces. The mock peer withholds all piece data
// until it has received requests for at least two DISTINCT pieces (with a 1s
// grace fallback so the test fails on the assertion instead of hanging). The old
// one-piece-per-peer downloader could only ever have one piece's blocks in flight,
// so it would request piece 0's single block and then stall waiting for a reply —
// never reaching two concurrent pieces. The new pipeline opens several pieces at
// once, so the peer observes concurrent requests and the download completes.
func TestMultiPiecePipelineSpansPieces(t *testing.T) {
	const pieceLen = BlockSize // one 16 KB block per piece
	const numPieces = 6
	total := pieceLen * numPieces

	data := make([]byte, total)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	hashes := make([][20]byte, numPieces)
	for p := 0; p < numPieces; p++ {
		hashes[p] = sha1.Sum(data[p*pieceLen : (p+1)*pieceLen])
	}

	tor := &torrent.Torrent{
		Name:        "multipiece.bin",
		PieceLength: int64(pieceLen),
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(total), Path: []string{"multipiece.bin"}}},
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	var sawConcurrentPieces atomic.Bool

	go func() {
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
		copy(pid[:], "-MOCK01-multipiece!!")
		_, _ = conn.Write((&peer.Handshake{Pstr: "BitTorrent protocol", InfoHash: hs.InfoHash, PeerID: pid}).Serialize())
		// Bitfield advertising all 6 pieces: bits 0..5 set -> 0b11111100.
		_, _ = conn.Write((&peer.Message{ID: peer.MsgBitfield, Payload: []byte{0xFC}}).Serialize())

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
		distinct := make(map[uint32]bool)
		served := false
		flush := func() {
			for _, r := range queue {
				serve(r.index, r.begin, r.length)
			}
			queue = nil
			served = true
		}

		for {
			_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			msg, err := peer.ParseMessage(conn)
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					// Grace period expired without seeing concurrent pieces:
					// serve what we have so the download still completes (and the
					// assertion below fails) rather than deadlocking.
					if !served && len(queue) > 0 {
						flush()
					}
					continue
				}
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
				if served {
					serve(index, begin, length)
					continue
				}
				queue = append(queue, req{index, begin, length})
				distinct[index] = true
				if len(distinct) >= 2 {
					sawConcurrentPieces.Store(true)
					flush()
				}
			}
		}
	}()

	dir := t.TempDir()
	st, err := storage.NewStorage(dir, []storage.FileInfo{{Path: "multipiece.bin", Length: int64(total)}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}

	var pid [20]byte
	copy(pid[:], "-ST0001-pipeline123")
	sess, err := NewSession(tor, st, pid, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	sess.Start()
	defer sess.Close()

	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	sess.AddPeerFromDiscovery("127.0.0.1:" + portStr)

	completed := false
	for i := 0; i < 100; i++ {
		if sess.PercentComplete() == 100.0 {
			completed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !completed {
		t.Fatalf("download did not complete; %.1f%% done", sess.PercentComplete())
	}
	if !sawConcurrentPieces.Load() {
		t.Fatal("peer never saw requests for two pieces at once: the pipeline is still capped at one piece per peer")
	}

	got, err := os.ReadFile(filepath.Join(dir, "multipiece.bin"))
	if err != nil {
		t.Fatalf("read result: %v", err)
	}
	if string(got) != string(data) {
		t.Fatal("downloaded content does not match source")
	}
}

// TestSessionCountersConcurrentAtomic exercises the lock-free byte counters: many
// goroutines bump the session and per-peer counters on the hot path (no s.mu)
// while readers take snapshots. It checks the arithmetic is exact and, under
// -race, that the lock-free writes don't race the snapshot reads.
func TestSessionCountersConcurrentAtomic(t *testing.T) {
	tor := &torrent.Torrent{Name: "counters"} // PieceLength 0 -> metadata mode; no storage needed
	var pid [20]byte
	sess, err := NewSession(tor, nil, pid, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	const numPeers = 4
	peers := make([]*PeerState, numPeers)
	sess.mu.Lock()
	for i := 0; i < numPeers; i++ {
		addr := fmt.Sprintf("10.0.0.%d:%d", i, 1000+i)
		ps := &PeerState{IP: fmt.Sprintf("10.0.0.%d", i), Port: uint16(1000 + i), Active: true}
		sess.Peers[addr] = ps
		peers[i] = ps
	}
	sess.mu.Unlock()

	const perGoroutine = 5000
	const inc = int64(BlockSize)

	var wg sync.WaitGroup
	for i := 0; i < numPeers; i++ {
		wg.Add(1)
		go func(ps *PeerState) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				sess.Downloaded.Add(inc)
				atomic.AddInt64(&ps.Downloaded, inc)
				sess.Uploaded.Add(inc)
				atomic.AddInt64(&ps.Uploaded, inc)
			}
		}(peers[i])
	}

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 3; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = sess.GetActivePeers()
				_ = sess.DownloadedBytes()
				_ = sess.UploadedBytes()
				_ = sess.DownloadSpeed()
			}
		}()
	}

	wg.Wait()
	close(stop)
	readers.Wait()

	wantSession := inc * perGoroutine * numPeers
	if got := sess.Downloaded.Load(); got != wantSession {
		t.Fatalf("session Downloaded = %d, want %d", got, wantSession)
	}
	if got := sess.Uploaded.Load(); got != wantSession {
		t.Fatalf("session Uploaded = %d, want %d", got, wantSession)
	}
	wantPeer := inc * perGoroutine
	for i, ps := range peers {
		if got := atomic.LoadInt64(&ps.Downloaded); got != wantPeer {
			t.Fatalf("peer %d Downloaded = %d, want %d", i, got, wantPeer)
		}
		if got := atomic.LoadInt64(&ps.Uploaded); got != wantPeer {
			t.Fatalf("peer %d Uploaded = %d, want %d", i, got, wantPeer)
		}
	}
}
