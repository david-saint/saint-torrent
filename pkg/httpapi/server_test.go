package httpapi

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestStatsHandlerReturnsManagerAndTorrentSnapshot(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	mgr.SetGlobalDownloadLimit(1234)
	mgr.SetGlobalUploadLimit(5678)

	sess := newHTTPTestSession(t)
	infoHash := fmt.Sprintf("%x", sess.Torrent.InfoHash)
	mgr.AddSession(infoHash, sess)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec := httptest.NewRecorder()

	NewHandler(mgr).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}

	var stats Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw stats: %v", err)
	}

	if stats.Version != statsVersion {
		t.Fatalf("version = %d, want %d", stats.Version, statsVersion)
	}
	if stats.Manager.TorrentCount != 1 {
		t.Fatalf("torrent count = %d, want 1", stats.Manager.TorrentCount)
	}
	if stats.Manager.DownloadLimitBytesPerSecond != 1234 || stats.Manager.UploadLimitBytesPerSecond != 5678 {
		t.Fatalf("unexpected limits: %+v", stats.Manager)
	}
	if stats.Manager.DownloadedBytes != 2048 || stats.Manager.UploadedBytes != 512 {
		t.Fatalf("unexpected manager byte totals: %+v", stats.Manager)
	}
	if len(stats.Torrents) != 1 {
		t.Fatalf("torrent snapshots = %d, want 1", len(stats.Torrents))
	}

	got := stats.Torrents[0]
	if got.Name != "stats-test" || got.InfoHash != infoHash || got.Status != "Downloading" {
		t.Fatalf("unexpected torrent identity/status: %+v", got)
	}
	if got.TotalSizeBytes != 4 || got.DownloadedBytes != 2048 || got.UploadedBytes != 512 {
		t.Fatalf("unexpected torrent byte stats: %+v", got)
	}
	if got.Pieces.Total != 2 || got.Pieces.Completed != 1 || got.Pieces.Downloading != 1 {
		t.Fatalf("unexpected piece stats: %+v", got.Pieces)
	}
	if len(got.Files) != 1 || got.Files[0].Path != "stats-test/file.bin" || got.Files[0].Priority != "normal" {
		t.Fatalf("unexpected file stats: %+v", got.Files)
	}

	managerRaw, ok := raw["manager"].(map[string]any)
	if !ok {
		t.Fatalf("manager JSON is not an object: %#v", raw["manager"])
	}
	natRaw, ok := managerRaw["nat"].(map[string]any)
	if !ok {
		t.Fatalf("nat JSON is not an object: %#v", managerRaw["nat"])
	}
	for _, key := range []string{"enabled", "protocol", "external_ip", "listen_port", "advertised_port", "tcp_mapped", "udp_mapped"} {
		if _, ok := natRaw[key]; !ok {
			t.Fatalf("nat JSON missing snake_case key %q: %#v", key, natRaw)
		}
	}
	for _, key := range []string{"Enabled", "Protocol", "ExternalIP", "ListenPort", "AdvertisedPort", "TCPMapped", "UDPMapped", "LastError"} {
		if _, ok := natRaw[key]; ok {
			t.Fatalf("nat JSON leaked Go field key %q: %#v", key, natRaw)
		}
	}
}

func TestHealthzAndMethodHandling(t *testing.T) {
	handler := NewHandler(nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", rec.Code, http.StatusOK)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/stats", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /stats status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func TestStartServesHealthzAndShutdown(t *testing.T) {
	server, err := Start("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}

	resp, err := http.Get("http://" + server.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown server: %v", err)
	}
}

func TestStartConfiguresWriteAndIdleTimeouts(t *testing.T) {
	server, err := Start("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	if server.server.WriteTimeout != 10*time.Second {
		t.Fatalf("WriteTimeout = %s, want %s", server.server.WriteTimeout, 10*time.Second)
	}
	if server.server.IdleTimeout != 60*time.Second {
		t.Fatalf("IdleTimeout = %s, want %s", server.server.IdleTimeout, 60*time.Second)
	}
	if server.server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.server.ReadHeaderTimeout, 5*time.Second)
	}
}

func TestSnapshotAtUsesProvidedTimestampAndEmptyManager(t *testing.T) {
	ts := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)

	stats := SnapshotAt(nil, ts)
	if !stats.GeneratedAt.Equal(ts) {
		t.Fatalf("generated_at = %s, want %s", stats.GeneratedAt, ts)
	}
	if stats.Version != statsVersion || len(stats.Torrents) != 0 {
		t.Fatalf("unexpected empty snapshot: %+v", stats)
	}
}

func newHTTPTestSession(t *testing.T) *downloader.Session {
	t.Helper()

	piece0 := []byte("ab")
	piece1 := []byte("cd")
	tor := &torrent.Torrent{
		Name:        "stats-test",
		InfoHash:    sha1.Sum([]byte("stats-test")),
		PieceLength: 2,
		PieceHashes: [][20]byte{
			sha1.Sum(piece0),
			sha1.Sum(piece1),
		},
		Files: []torrent.File{
			{Length: 4, Path: []string{"stats-test", "file.bin"}},
		},
	}
	st, err := storage.NewMemStorage(t.TempDir(), []storage.FileInfo{
		{Path: filepath.Join("stats-test", "file.bin"), Length: 4},
	}, tor.PieceLength)
	if err != nil {
		t.Fatalf("new storage: %v", err)
	}
	sess, err := downloader.NewSession(tor, st, [20]byte{}, 51413, t.TempDir())
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	sess.PieceStates[0] = downloader.PieceCompleted
	sess.PieceStates[1] = downloader.PieceDownloading
	sess.Downloaded.Store(2048)
	sess.Uploaded.Store(512)
	return sess
}
