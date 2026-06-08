package downloader

// Benchmarks that measure the two latencies we care about for saintTorrent:
// cold startup (restoring a saved library to the point the UI appears) and shutdown
// (tearing the manager down, including the "stopped" tracker announces).
//
// They are deterministic and CI-friendly: seedState builds a synthetic on-disk
// library that mirrors real persistence (session.json + cached .torrent files +
// fast-resume .state files + real data files), so EnablePersistence exercises the
// exact same code path it does in production.
//
//   go test -bench=BenchmarkColdStartup -benchmem ./pkg/downloader
//   go test -bench=BenchmarkShutdown            ./pkg/downloader
//
// Capture these numbers on the current tree first; they are the denominator for
// the >=5x goal.

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// seedConfig describes a synthetic, mostly-complete library to benchmark against.
type seedConfig struct {
	numTorrents     int
	bytesPerTorrent int64
	pieceLength     int64
	trackerURL      string // announce baked into each torrent (a blocking server for shutdown)
}

func (c seedConfig) name() string {
	return fmt.Sprintf("torrents=%d/size=%dKiB", c.numTorrents, c.bytesPerTorrent>>10)
}

// seededLibrary holds the on-disk paths produced by seedState.
type seededLibrary struct {
	stateDir    string // config dir: session.json + torrents/
	downloadDir string // data files + .state files
}

// seedState writes a complete, restorable library to a fresh temp dir and returns
// the paths to hand to EnablePersistence. Every piece hashes correctly and every
// fast-resume file validates, so restore is exercised exactly as in production (piece
// verification itself is deferred to a background pass that Start() kicks off).
func seedState(tb testing.TB, cfg seedConfig) seededLibrary {
	return seedStateInto(tb, tb.TempDir(), cfg)
}

// seedStateInto is seedState with an explicit root directory, used by TestSeedFixture to
// persist a library for manual headless runs of the real binary.
func seedStateInto(tb testing.TB, root string, cfg seedConfig) seededLibrary {
	tb.Helper()
	stateDir := filepath.Join(root, "config")
	downloadDir := filepath.Join(root, "downloads")
	if err := os.MkdirAll(filepath.Join(stateDir, "torrents"), 0o755); err != nil {
		tb.Fatal(err)
	}
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		tb.Fatal(err)
	}

	persisted := PersistedState{Version: 1}

	for i := 0; i < cfg.numTorrents; i++ {
		name := fmt.Sprintf("bench-%d.bin", i)

		// 1. Deterministic data + per-piece SHA-1 hashes (so verification passes).
		data := make([]byte, cfg.bytesPerTorrent)
		for j := range data {
			data[j] = byte((j*31 + i*7) & 0xff)
		}
		numPieces := int((cfg.bytesPerTorrent + cfg.pieceLength - 1) / cfg.pieceLength)
		pieces := make([]byte, 0, numPieces*20)
		for p := 0; p < numPieces; p++ {
			start := int64(p) * cfg.pieceLength
			end := min(start+cfg.pieceLength, cfg.bytesPerTorrent)
			h := sha1.Sum(data[start:end])
			pieces = append(pieces, h[:]...)
		}

		// 2. Build a real single-file .torrent and parse it back for the canonical info hash.
		info := map[string]interface{}{
			"name":         name,
			"piece length": cfg.pieceLength,
			"pieces":       string(pieces),
			"length":       cfg.bytesPerTorrent,
		}
		torDict := map[string]interface{}{"info": info}
		if cfg.trackerURL != "" {
			torDict["announce"] = cfg.trackerURL
		}
		torBytes, err := bencode.Marshal(torDict)
		if err != nil {
			tb.Fatal(err)
		}
		tor, err := torrent.Parse(torBytes)
		if err != nil {
			tb.Fatalf("seed torrent failed to parse: %v", err)
		}
		infoHashHex := fmt.Sprintf("%x", tor.InfoHash)

		// 3. Cache the .torrent where EnablePersistence looks for it.
		cachedPath := filepath.Join(stateDir, "torrents", infoHashHex+".torrent")
		if err := os.WriteFile(cachedPath, torBytes, 0o644); err != nil {
			tb.Fatal(err)
		}

		// 4. Write the full data file.
		if err := os.WriteFile(filepath.Join(downloadDir, name), data, 0o644); err != nil {
			tb.Fatal(err)
		}

		// 5. Produce a valid fast-resume .state marking every piece complete.
		st, err := storage.NewStorage(downloadDir, []storage.FileInfo{{Path: name, Length: cfg.bytesPerTorrent}}, cfg.pieceLength)
		if err != nil {
			tb.Fatal(err)
		}
		complete := make([]int, numPieces)
		for p := range complete {
			complete[p] = p
		}
		if err := st.SaveState(infoHashHex, complete); err != nil {
			tb.Fatal(err)
		}

		persisted.Torrents = append(persisted.Torrents, PersistedTorrent{
			InfoHashHex: infoHashHex,
			DownloadDir: downloadDir,
		})
	}

	stateJSON, err := json.Marshal(persisted)
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "session.json"), stateJSON, 0o644); err != nil {
		tb.Fatal(err)
	}

	return seededLibrary{stateDir: stateDir, downloadDir: downloadDir}
}

// BenchmarkColdStartup measures restoring a saved library via EnablePersistence — the
// work that runs before the UI appears. Piece verification is deliberately NOT included:
// it is deferred to a background pass (kicked off by Session.Start, not called here),
// which is exactly the optimization this benchmark is meant to capture. The baseline it
// is compared against did this work synchronously, so the delta reflects that change.
func BenchmarkColdStartup(b *testing.B) {
	profiles := []seedConfig{
		{numTorrents: 5, bytesPerTorrent: 16 << 20, pieceLength: 1 << 18},   // few large, mostly-complete
		{numTorrents: 50, bytesPerTorrent: 256 << 10, pieceLength: 1 << 15}, // many small
	}
	for _, prof := range profiles {
		b.Run(prof.name(), func(b *testing.B) {
			lib := seedState(b, prof) // seeded once, restored read-only each iteration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				mgr := NewTorrentManager()
				if _, err := mgr.EnablePersistence(lib.stateDir); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				mgr.Close() // untimed teardown (Start is not called, so no verification runs)
				b.StartTimer()
			}
		})
	}
}

// BenchmarkShutdown measures mgr.Close() for a started library whose tracker never
// responds — reproducing the "stopped" announce stall that dominates close time.
func BenchmarkShutdown(b *testing.B) {
	// A tracker that accepts the request and blocks until the client gives up.
	block := make(chan struct{})
	b.Cleanup(func() { close(block) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	b.Cleanup(srv.Close)

	for _, n := range []int{3} {
		b.Run(fmt.Sprintf("torrents=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				lib := seedState(b, seedConfig{
					numTorrents:     n,
					bytesPerTorrent: 1 << 20,
					pieceLength:     1 << 18,
					trackerURL:      srv.URL,
				})
				mgr := NewTorrentManager()
				if _, err := mgr.EnablePersistence(lib.stateDir); err != nil {
					b.Fatal(err)
				}
				for _, s := range mgr.ListSessions() {
					s.Start() // fires the (blocking) "started" announce in the background
				}
				b.StartTimer()
				mgr.Close() // includes the "stopped" announces we want to bound
			}
		})
	}
}

// TestSeedFixture writes a benchmark library to $ST_SEED_DIR so the real binary can be
// measured end-to-end (e.g. SAINTTORRENT_BENCH=1 ./sainttorrent -d <dir>/downloads
// -c <dir>/config). Skipped unless ST_SEED_DIR is set.
func TestSeedFixture(t *testing.T) {
	dir := os.Getenv("ST_SEED_DIR")
	if dir == "" {
		t.Skip("set ST_SEED_DIR to seed a fixture for manual headless runs")
	}
	lib := seedStateInto(t, dir, seedConfig{numTorrents: 5, bytesPerTorrent: 16 << 20, pieceLength: 1 << 18})
	t.Logf("seeded few-large library: config=%s downloads=%s", lib.stateDir, lib.downloadDir)
}
