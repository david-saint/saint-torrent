package downloader

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func TestWebseedDownloadsSingleFileViaHTTPRange(t *testing.T) {
	defer swapDuration(&webseedIdleDelay, 10*time.Millisecond)()

	data := bytes.Repeat([]byte("0123456789abcdef"), 2000) // spans two webseed blocks
	requests := makeRangeRecorder()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r.URL.Path, r.Header.Get("Range"))
		serveRange(t, w, r, data)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "seed.bin",
		InfoHash:    sha1.Sum([]byte("single-webseed")),
		WebSeeds:    []string{srv.URL + "/seed.bin"},
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"seed.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitForWebseedState(t, sess, 0, PieceCompleted)

	got := make([]byte, len(data))
	if _, err := sess.Storage.ReadBlock(0, 0, got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("stored data does not match webseed payload")
	}
	if got := sess.DownloadedBytes(); got != int64(len(data)) {
		t.Fatalf("DownloadedBytes = %d, want %d", got, len(data))
	}
	if got := webseedPeerDownloaded(t, sess); got != int64(len(data)) {
		t.Fatalf("webseed peer downloaded = %d, want %d", got, len(data))
	}
	wantRequests := []string{
		"/seed.bin bytes=0-16383",
		fmt.Sprintf("/seed.bin bytes=16384-%d", len(data)-1),
	}
	if got := requests.snapshot(); !reflect.DeepEqual(got, wantRequests) {
		t.Fatalf("range requests = %v, want %v", got, wantRequests)
	}
}

func TestWebseedDownloadsMultiFilePieceAcrossBoundary(t *testing.T) {
	defer swapDuration(&webseedIdleDelay, 10*time.Millisecond)()

	fileA := []byte("alpha")
	fileB := []byte("bravo-charlie")
	piece := append(append([]byte(nil), fileA...), fileB...)
	files := map[string][]byte{
		"/mirror/root/a.bin":     fileA,
		"/mirror/root/dir/b.bin": fileB,
	}
	requests := makeRangeRecorder()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.record(r.URL.Path, r.Header.Get("Range"))
		data, ok := files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		serveRange(t, w, r, data)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "root",
		InfoHash:    sha1.Sum([]byte("multi-webseed")),
		WebSeeds:    []string{srv.URL + "/mirror"},
		PieceLength: int64(len(piece)),
		PieceHashes: [][20]byte{sha1.Sum(piece)},
		Files: []torrent.File{
			{Length: int64(len(fileA)), Path: []string{"root", "a.bin"}},
			{Length: int64(len(fileB)), Path: []string{"root", "dir", "b.bin"}},
		},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitForWebseedState(t, sess, 0, PieceCompleted)

	got := make([]byte, len(piece))
	if _, err := sess.Storage.ReadBlock(0, 0, got); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !bytes.Equal(got, piece) {
		t.Fatal("stored data does not match multi-file webseed payload")
	}
	wantRequests := []string{
		"/mirror/root/a.bin bytes=0-4",
		fmt.Sprintf("/mirror/root/dir/b.bin bytes=0-%d", len(fileB)-1),
	}
	if got := requests.snapshot(); !reflect.DeepEqual(got, wantRequests) {
		t.Fatalf("range requests = %v, want %v", got, wantRequests)
	}
}

func TestWebseedHTTPErrorReturnsPieceToNeededSet(t *testing.T) {
	defer swapDuration(&webseedIdleDelay, 10*time.Millisecond)()

	data := []byte("payload never served")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no mirror today", http.StatusBadGateway)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "missing.bin",
		InfoHash:    sha1.Sum([]byte("http-error-webseed")),
		WebSeeds:    []string{srv.URL + "/missing.bin"},
		PieceLength: int64(len(data)),
		PieceHashes: [][20]byte{sha1.Sum(data)},
		Files:       []torrent.File{{Length: int64(len(data)), Path: []string{"missing.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitForWebseedErrorFallback(t, sess, 0, "502 Bad Gateway")

	sess.mu.RLock()
	_, needed := sess.neededPieces[0]
	sess.mu.RUnlock()
	if !needed {
		t.Fatal("HTTP-failed webseed piece was not returned to the needed set")
	}
	if status := sess.Status(); status != "Downloading" {
		t.Fatalf("Status = %q, want Downloading", status)
	}
}

func TestWebseedCorruptBytesReturnPieceToNeededSet(t *testing.T) {
	defer swapDuration(&webseedIdleDelay, 10*time.Millisecond)()

	good := []byte("verified webseed payload")
	bad := []byte("corrupt! webseed payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveRange(t, w, r, bad)
	}))
	defer srv.Close()

	tor := &torrent.Torrent{
		Name:        "bad.bin",
		InfoHash:    sha1.Sum([]byte("bad-webseed")),
		WebSeeds:    []string{srv.URL + "/bad.bin"},
		PieceLength: int64(len(good)),
		PieceHashes: [][20]byte{sha1.Sum(good)},
		Files:       []torrent.File{{Length: int64(len(good)), Path: []string{"bad.bin"}}},
	}
	sess := newWebseedTestSession(t, tor)
	startWebseedsForTest(t, sess)

	waitForWebseedCorruptFallback(t, sess, 0)

	sess.mu.RLock()
	_, needed := sess.neededPieces[0]
	sess.mu.RUnlock()
	if !needed {
		t.Fatal("corrupt webseed piece was not returned to the needed set")
	}
	if status := sess.Status(); status != "Downloading" {
		t.Fatalf("Status = %q, want Downloading", status)
	}
	if err := sess.LastError(); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("LastError = %v, want corrupt webseed error", err)
	}
}

func newWebseedTestSession(t *testing.T, tor *torrent.Torrent) *Session {
	t.Helper()
	dir := t.TempDir()
	files := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		files[i] = storage.FileInfo{Path: filepath.Join(f.Path...), Length: f.Length}
	}
	st, err := storage.NewMemStorage(dir, files, tor.PieceLength)
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	sess, err := NewSession(tor, st, [20]byte{}, 0, dir)
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	t.Cleanup(func() { sess.Close() })
	return sess
}

func startWebseedsForTest(t *testing.T, sess *Session) {
	t.Helper()
	specs := sess.webseedSpecsForStart()
	if len(specs) == 0 {
		t.Fatal("expected at least one webseed spec")
	}
	for _, spec := range specs {
		spec := spec
		sess.wg.Add(1)
		go sess.webseedLoop(spec)
	}
}

func waitForWebseedState(t *testing.T, sess *Session, piece int, want PieceState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		states := sess.GetPieceStates()
		if piece >= 0 && piece < len(states) && states[piece] == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("piece %d did not reach state %v; states=%v err=%v", piece, want, sess.GetPieceStates(), sess.LastError())
}

func waitForWebseedCorruptFallback(t *testing.T, sess *Session, piece int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		states := sess.GetPieceStates()
		err := sess.LastError()
		if piece >= 0 && piece < len(states) && states[piece] == PieceEmpty &&
			err != nil && strings.Contains(err.Error(), "corrupt") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("piece %d did not return to empty with corrupt error; states=%v err=%v", piece, sess.GetPieceStates(), sess.LastError())
}

func waitForWebseedErrorFallback(t *testing.T, sess *Session, piece int, errText string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		states := sess.GetPieceStates()
		err := sess.LastError()
		if piece >= 0 && piece < len(states) && states[piece] == PieceEmpty &&
			err != nil && strings.Contains(err.Error(), errText) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("piece %d did not return to empty with %q error; states=%v err=%v", piece, errText, sess.GetPieceStates(), sess.LastError())
}

func webseedPeerDownloaded(t *testing.T, sess *Session) int64 {
	t.Helper()
	sess.mu.RLock()
	defer sess.mu.RUnlock()
	for addr, pState := range sess.Peers {
		if strings.HasPrefix(addr, "webseed:") {
			return atomic.LoadInt64(&pState.Downloaded)
		}
	}
	t.Fatal("webseed peer state not found")
	return 0
}

type rangeRecorder struct {
	mu       sync.Mutex
	requests []string
}

func makeRangeRecorder() *rangeRecorder {
	return &rangeRecorder{}
}

func (r *rangeRecorder) record(path, header string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, path+" "+header)
}

func (r *rangeRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.requests...)
}

func serveRange(t *testing.T, w http.ResponseWriter, r *http.Request, data []byte) {
	t.Helper()
	rangeHeader := r.Header.Get("Range")
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		http.Error(w, "missing range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	parts := strings.SplitN(strings.TrimPrefix(rangeHeader, "bytes="), "-", 2)
	if len(parts) != 2 {
		http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		http.Error(w, "bad range start", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		http.Error(w, "bad range end", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if start < 0 || end < start || end >= len(data) {
		http.Error(w, "range outside file", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(data[start : end+1])
}
