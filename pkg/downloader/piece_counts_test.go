// External tests of what the stats API reads from the downloader.
package downloader_test

import (
	"crypto/sha1"
	"testing"

	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestSessionPieceCountsMatchHandBuiltStates(t *testing.T) {
	const pieces = 9
	tor := &torrent.Torrent{
		Name:        "counts.bin",
		InfoHash:    sha1.Sum([]byte("piece-counts")),
		PieceLength: 2,
		PieceHashes: make([][20]byte, pieces),
		Files:       []torrent.File{{Length: 2 * pieces, Path: []string{"counts.bin"}}},
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{{Path: "counts.bin", Length: 2 * pieces}}, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := downloader.NewSession(tor, st, [20]byte{}, 0, t.TempDir())
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	defer sess.Close()

	states := []downloader.PieceState{
		downloader.PieceCompleted, downloader.PieceEmpty, downloader.PieceDownloading,
		downloader.PieceCompleted, downloader.PieceUnverified, downloader.PieceState(-1),
		downloader.PieceCompleted, downloader.PieceState(42), downloader.PieceEmpty,
	}
	copy(sess.PieceStates, states)
	want := downloader.PieceCounts{Total: 9, Empty: 2, Downloading: 1, Completed: 3, Unverified: 1, Unknown: 2}
	if got := sess.PieceCounts(); got != want {
		t.Fatalf("PieceCounts() = %+v, want %+v", got, want)
	}
	if allocs := testing.AllocsPerRun(10, func() { _ = sess.PieceCounts() }); allocs != 0 {
		t.Fatalf("PieceCounts allocates %.0f times, want 0", allocs)
	}
}
