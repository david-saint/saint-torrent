// External tests of what the stats API reads from the downloader: they need
// package httpapi, which imports downloader.
package downloader_test

import (
	"crypto/sha1"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"unicode"

	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/httpapi"
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

// A gateway's error text reaches the stats API through NATStatus.LastError;
// it must arrive there bounded and printable, not as the 300 KiB of escape
// sequences the gateway sent.
func TestStatsAPIShowsSanitizedNATError(t *testing.T) {
	downloader.StubNATDiscoveryErrorForTest(t, downloader.HostileGatewayErrorForTest())
	mgr := downloader.NewTorrentManager()
	t.Cleanup(mgr.Close)
	if err := mgr.StartNATTraversal(51413, 51413); err != nil {
		t.Fatalf("StartNATTraversal: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for mgr.NATStatus().LastError == "" {
		if time.Now().After(deadline) {
			t.Fatal("NAT failure was never recorded")
		}
		time.Sleep(time.Millisecond)
	}

	rec := httptest.NewRecorder()
	httpapi.NewHandler(mgr).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/stats", nil))
	var stats httpapi.Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode /stats: %v", err)
	}
	got := stats.Manager.NAT.LastError
	if got == "" || got != mgr.NATStatus().LastError || len(got) > 256 {
		t.Fatalf("/stats nat.last_error is %d bytes (%.80q), want the stored 1..256-byte text", len(got), got)
	}
	for _, r := range got {
		if !unicode.IsPrint(r) {
			t.Fatalf("/stats nat.last_error keeps unprintable %U: %q", r, got)
		}
	}
	if len(rec.Body.Bytes()) > 4<<10 {
		t.Fatalf("/stats body is %d bytes for one idle manager", rec.Body.Len())
	}
}
