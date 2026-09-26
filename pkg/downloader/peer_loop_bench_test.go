package downloader

import (
	"crypto/sha1"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// Peer-loop throughput benchmarks: one runPeerMessageLoop connection over
// net.Pipe, driven by a remote that answers as fast as it can. They cover the
// per-message hot path of both directions (block receipt and piece completion,
// request queueing and block serving) and exist to catch regressions from work
// added to that path.

const (
	benchPieceLen  = 256 * 1024
	benchNumPieces = 64 // 16 MiB per iteration
)

func benchTorrentData(pieceLen, numPieces int) ([]byte, [][20]byte) {
	data := make([]byte, pieceLen*numPieces)
	for i := range data {
		data[i] = byte(i*31 + 11)
	}
	hashes := make([][20]byte, numPieces)
	for i := range hashes {
		hashes[i] = sha1.Sum(data[i*pieceLen : (i+1)*pieceLen])
	}
	return data, hashes
}

func benchSession(b *testing.B, data []byte, hashes [][20]byte, pieceLen int, seed bool) *Session {
	b.Helper()
	tor := &torrent.Torrent{
		Name:        "bench.bin",
		InfoHash:    sha1.Sum([]byte("peer-loop-bench")),
		PieceLength: int64(pieceLen),
		PieceHashes: hashes,
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"bench.bin"}}},
	}
	st, err := storage.NewMemStorage(b.TempDir(), []storage.FileInfo{{Path: "bench.bin", Length: int64(len(data))}}, int64(pieceLen))
	if err != nil {
		b.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, b.TempDir())
	if err != nil {
		b.Fatalf("session: %v", err)
	}
	if seed {
		for i := range hashes {
			if err := st.WriteBlock(int64(i), 0, data[i*pieceLen:(i+1)*pieceLen]); err != nil {
				b.Fatalf("seed: %v", err)
			}
		}
		sess.mu.Lock()
		for i := range hashes {
			sess.setPieceStateLocked(i, PieceCompleted)
		}
		sess.mu.Unlock()
	}
	return sess
}

func benchRunLoop(sess *Session, reserved [8]byte) (net.Conn, chan struct{}) {
	clientConn, remote := net.Pipe()
	client := peer.NewClient(clientConn, sess.Torrent.InfoHash, sess.PeerID)
	done := make(chan struct{})
	go func() {
		sess.runPeerMessageLoop(client, clientConn, "127.0.0.1:6999", "127.0.0.1", 6999, reserved, false)
		close(done)
	}()
	return remote, done
}

// BenchmarkPeerLoopDownload downloads a 16 MiB torrent from one seed peer.
func BenchmarkPeerLoopDownload(b *testing.B) {
	benchPeerLoopDownload(b, benchPieceLen, benchNumPieces)
}

// BenchmarkPeerLoopDownloadLargePieces downloads 128 MiB in 16 MiB pieces, where
// peerOpenPieceBytesCap keeps the connection to four open pieces.
func BenchmarkPeerLoopDownloadLargePieces(b *testing.B) {
	benchPeerLoopDownload(b, 16<<20, 8)
}

func benchPeerLoopDownload(b *testing.B, pieceLen, numPieces int) {
	data, hashes := benchTorrentData(pieceLen, numPieces)
	var reserved [8]byte
	peer.EnableFastExtension(&reserved)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		sess := benchSession(b, data, hashes, pieceLen, false)
		remote, done := benchRunLoop(sess, reserved)
		b.StartTimer()

		// net.Pipe is synchronous, so the remote reads and writes on separate
		// goroutines with a queue between them, like a real socket's buffers.
		requests := make(chan []byte, 4096)
		go func() {
			defer close(requests)
			for {
				msg, err := peer.ParseMessage(remote)
				if err != nil {
					return
				}
				if msg != nil && msg.ID == peer.MsgRequest {
					requests <- msg.Payload
				}
			}
		}()
		go func() {
			_, _ = remote.Write((&peer.Message{ID: peer.MsgHaveAll}).Serialize())
			_, _ = remote.Write((&peer.Message{ID: peer.MsgUnchoke}).Serialize())
			for req := range requests {
				index := binary.BigEndian.Uint32(req[0:4])
				begin := binary.BigEndian.Uint32(req[4:8])
				length := binary.BigEndian.Uint32(req[8:12])
				off := int(index)*pieceLen + int(begin)
				frame := make([]byte, 13, 13+length)
				binary.BigEndian.PutUint32(frame[0:4], 9+length)
				frame[4] = byte(peer.MsgPiece)
				copy(frame[5:13], req[0:8])
				if _, err := remote.Write(append(frame, data[off:off+int(length)]...)); err != nil {
					return
				}
			}
		}()
		for !sess.IsCompleted() {
			time.Sleep(200 * time.Microsecond)
		}

		b.StopTimer()
		_ = remote.Close()
		<-done
		sess.Close()
		b.StartTimer()
	}
}

// BenchmarkPeerLoopUpload serves a 16 MiB torrent to one leecher that keeps
// maxUploadQueue/2 requests in flight.
func BenchmarkPeerLoopUpload(b *testing.B) {
	data, hashes := benchTorrentData(benchPieceLen, benchNumPieces)
	var reserved [8]byte
	peer.EnableFastExtension(&reserved)
	const blocksPerPiece = benchPieceLen / BlockSize
	const totalBlocks = benchNumPieces * blocksPerPiece
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		sess := benchSession(b, data, hashes, benchPieceLen, true)
		remote, done := benchRunLoop(sess, reserved)
		unchoked := make(chan struct{})
		served := make(chan struct{}, totalBlocks)
		go func() {
			closedUnchoke := false
			for {
				msg, err := peer.ParseMessage(remote)
				if err != nil {
					return
				}
				switch {
				case msg == nil:
				case msg.ID == peer.MsgUnchoke && !closedUnchoke:
					closedUnchoke = true
					close(unchoked)
				case msg.ID == peer.MsgPiece:
					served <- struct{}{}
				}
			}
		}()
		_, _ = remote.Write((&peer.Message{ID: peer.MsgInterested}).Serialize())
		<-unchoked
		b.StartTimer()

		go func() {
			req := make([]byte, 12)
			for i := 0; i < totalBlocks; i++ {
				if i >= maxUploadQueue/2 {
					<-served // keep the window below the queue limit
				}
				binary.BigEndian.PutUint32(req[0:4], uint32(i/blocksPerPiece))
				binary.BigEndian.PutUint32(req[4:8], uint32(i%blocksPerPiece*BlockSize))
				binary.BigEndian.PutUint32(req[8:12], BlockSize)
				if _, err := remote.Write((&peer.Message{ID: peer.MsgRequest, Payload: req}).Serialize()); err != nil {
					return
				}
			}
			for i := 0; i < maxUploadQueue/2; i++ {
				<-served
			}
			_ = remote.Close()
		}()
		<-done

		b.StopTimer()
		sess.Close()
		b.StartTimer()
	}
}
