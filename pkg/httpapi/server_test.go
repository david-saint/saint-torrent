package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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

	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/stats", nil)
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", rec.Code, http.StatusOK)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "http://127.0.0.1:16666/stats", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /stats status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow = %q, want GET", got)
	}
}

func TestStartServesHealthzAndShutdown(t *testing.T) {
	server, err := Start("127.0.0.1:0", nil, Options{})
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
	server, err := Start("127.0.0.1:0", nil, Options{})
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
	if server.server.ReadTimeout != 10*time.Second {
		t.Fatalf("ReadTimeout = %s, want %s", server.server.ReadTimeout, 10*time.Second)
	}
	if server.server.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.server.ReadHeaderTimeout, 5*time.Second)
	}
}

// The API reads only bare GETs; net/http's 1 MiB default header allowance let
// each of the maxConns slots pin a megabyte.
func TestStartCapsRequestHeaderBytes(t *testing.T) {
	server, err := Start("127.0.0.1:0", nil, Options{})
	if err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer server.Shutdown(t.Context())

	if server.server.MaxHeaderBytes != 8<<10 {
		t.Fatalf("MaxHeaderBytes = %d, want %d", server.server.MaxHeaderBytes, 8<<10)
	}

	conn, err := net.Dial("tcp", server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	req := "GET /healthz HTTP/1.1\r\nHost: " + server.Addr() + "\r\nX-Pad: " + strings.Repeat("a", 64<<10) + "\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		t.Fatalf("write: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("64 KiB header: status %d, want %d", resp.StatusCode, http.StatusRequestHeaderFieldsTooLarge)
	}
}

// countStatsBuilds swaps statsSnapshot for one that counts its calls and, when
// gate is non-nil, reports each start and waits for gate to close.
func countStatsBuilds(t *testing.T, started chan<- struct{}, gate <-chan struct{}) *atomic.Int32 {
	t.Helper()
	var builds atomic.Int32
	old := statsSnapshot
	statsSnapshot = func(m *downloader.TorrentManager) Stats {
		builds.Add(1)
		if gate != nil {
			started <- struct{}{}
			<-gate
		}
		return old(m)
	}
	t.Cleanup(func() { statsSnapshot = old })
	return &builds
}

// fakeStatsClock is a settable clock for statsHandler.
type fakeStatsClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeStatsClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeStatsClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func getStats(t *testing.T, h http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/stats", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /stats: status %d; body=%s", rec.Code, rec.Body.String())
	}
	return rec
}

// A client polling /stats in a loop used to take every session's lock and copy
// every piece-state vector per request. Within statsCacheTTL the encoded body
// is served again; after it the snapshot is rebuilt.
func TestStatsCacheServesOneBuildPerTTL(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	sess := newHTTPTestSession(t)
	mgr.AddSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), sess)
	builds := countStatsBuilds(t, nil, nil)
	clock := &fakeStatsClock{now: time.Now()}
	h := newStatsHandler(mgr)
	h.now = clock.Now

	first := getStats(t, h)
	sess.Downloaded.Store(4096) // visible only after a rebuild
	clock.Advance(statsCacheTTL - time.Millisecond)
	second := getStats(t, h)
	if got := builds.Load(); got != 1 {
		t.Fatalf("%d snapshots built for two requests within the TTL, want 1", got)
	}
	if !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatal("second request within the TTL got a different body")
	}
	for _, rec := range []*httptest.ResponseRecorder{first, second} {
		if rec.Header().Get("Content-Type") != "application/json" || rec.Header().Get("Cache-Control") != "no-store" ||
			rec.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("cached response headers = %v", rec.Header())
		}
	}
	var want bytes.Buffer
	var stats Stats
	if err := json.Unmarshal(first.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	_ = json.NewEncoder(&want).Encode(stats)
	if !bytes.Equal(first.Body.Bytes(), want.Bytes()) {
		t.Fatalf("cached body differs from writeJSON's encoding:\n got %q\nwant %q", first.Body.String(), want.String())
	}

	clock.Advance(time.Millisecond)
	third := getStats(t, h)
	if got := builds.Load(); got != 2 {
		t.Fatalf("%d snapshots built after the TTL, want 2", got)
	}
	if err := json.Unmarshal(third.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if stats.Manager.DownloadedBytes != 4096 {
		t.Fatalf("rebuilt snapshot downloaded = %d, want 4096", stats.Manager.DownloadedBytes)
	}
}

// The cached body lists every file of every torrent, so a large library's runs
// to megabytes. It used to stay in memory from one request until the next,
// however long the API then sat idle; it is now released once it is stale.
func TestStatsCacheReleasesStaleBody(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	sess := newHTTPTestSession(t)
	mgr.AddSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), sess)
	clock := &fakeStatsClock{now: time.Now()}
	h := newStatsHandler(mgr)
	h.now = clock.Now
	cached := func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.body != nil
	}

	getStats(t, h)
	clock.Advance(statsCacheTTL - time.Millisecond)
	h.dropStale()
	if !cached() {
		t.Fatal("a body younger than the TTL was released")
	}
	clock.Advance(time.Millisecond)
	h.dropStale()
	if cached() {
		t.Fatal("a stale body was kept")
	}

	// On the real clock the release runs by itself once the TTL has passed.
	h = newStatsHandler(mgr)
	getStats(t, h)
	deadline := time.Now().Add(5 * time.Second)
	for cached() {
		if time.Now().After(deadline) {
			t.Fatal("stale body still held 5 s after the request")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// And the next request simply builds a fresh one.
	if body := getStats(t, h).Body.Bytes(); len(body) == 0 || !cached() {
		t.Fatalf("request after the release: body %d bytes, cached %v", len(body), cached())
	}
}

// Requests that arrive while a snapshot is being built wait for it and share
// its bytes instead of each building their own.
func TestStatsConcurrentRequestsShareOneBuild(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	started := make(chan struct{}, 1)
	gate := make(chan struct{})
	builds := countStatsBuilds(t, started, gate)
	clock := &fakeStatsClock{now: time.Now()}
	h := newStatsHandler(mgr)
	h.now = clock.Now

	const n = 16
	bodies := make([][]byte, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:16666/stats", nil))
			bodies[i] = rec.Body.Bytes()
		}()
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no snapshot build started")
	}
	close(gate)
	wg.Wait()

	if got := builds.Load(); got != 1 {
		t.Fatalf("%d concurrent requests built %d snapshots, want 1", n, got)
	}
	for i, body := range bodies {
		if len(body) == 0 || !bytes.Equal(body, bodies[0]) {
			t.Fatalf("request %d got body %q, want the shared %q", i, body, bodies[0])
		}
	}
}

// The piece summary comes from Session.PieceCounts (one pass, no copy of the
// state vector) and must match the states.
func TestStatsPieceCountsMatchSessionStates(t *testing.T) {
	mgr := downloader.NewTorrentManager()
	defer mgr.Close()
	sess := newHTTPTestSession(t)
	mgr.AddSession(fmt.Sprintf("%x", sess.Torrent.InfoHash), sess)

	for _, tc := range []struct {
		states []downloader.PieceState
		want   PieceStats
	}{
		{[]downloader.PieceState{downloader.PieceCompleted, downloader.PieceDownloading}, PieceStats{Total: 2, Completed: 1, Downloading: 1}},
		{[]downloader.PieceState{downloader.PieceUnverified, downloader.PieceEmpty}, PieceStats{Total: 2, Unverified: 1, Empty: 1}},
		{[]downloader.PieceState{downloader.PieceState(9), downloader.PieceCompleted}, PieceStats{Total: 2, Completed: 1, Unknown: 1}},
	} {
		copy(sess.PieceStates, tc.states)
		var stats Stats
		if err := json.Unmarshal(getStats(t, NewHandler(mgr)).Body.Bytes(), &stats); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(stats.Torrents) != 1 || stats.Torrents[0].Pieces != tc.want {
			t.Fatalf("states %v: pieces = %+v, want %+v", tc.states, stats.Torrents, tc.want)
		}
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
