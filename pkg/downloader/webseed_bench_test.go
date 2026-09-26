package downloader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"sainttorrent/pkg/storage"
)

// Webseed throughput benchmarks: a webseed-only download from several
// loopback mirrors, each limited to benchMirrorRate. The download is bound by
// how many mirrors are used at once, so a worker cap below the mirror count
// shows up directly (four workers took 67% longer from eight mirrors).

const benchMirrorRate = 2 << 20 // bytes per second, per mirror

// BenchmarkWebseedMirrors2: 16 MiB of 256 KiB pieces from two mirrors.
func BenchmarkWebseedMirrors2(b *testing.B) { benchWebseedMirrors(b, 2) }

// BenchmarkWebseedMirrors8: the same from eight mirrors.
func BenchmarkWebseedMirrors8(b *testing.B) { benchWebseedMirrors(b, 8) }

func benchWebseedMirrors(b *testing.B, mirrors int) {
	const pieceLen, numPieces = 256 << 10, 64
	_, data := multiPieceWebseedTorrent("mirrors.bin", numPieces, pieceLen, nil)
	var seeds []string
	for i := 0; i < mirrors; i++ {
		srv := rateLimitedMirror(data, benchMirrorRate)
		b.Cleanup(srv.Close)
		seeds = append(seeds, srv.URL+"/mirrors.bin")
	}
	tor, _ := multiPieceWebseedTorrent("mirrors.bin", numPieces, pieceLen, seeds)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		st, err := storage.NewMemStorage(b.TempDir(), []storage.FileInfo{{Path: "mirrors.bin", Length: int64(len(data))}}, pieceLen)
		if err != nil {
			b.Fatalf("storage: %v", err)
		}
		sess, err := NewSession(tor, st, [20]byte{}, 0, b.TempDir())
		if err != nil {
			b.Fatalf("session: %v", err)
		}
		sess.mu.Lock()
		sess.verifying = false
		sess.verifyFullScan = false
		sess.verifyDone = nil
		sess.mu.Unlock()
		b.StartTimer()
		start := time.Now()
		for _, spec := range sess.webseedSpecsForStart() {
			sess.wg.Add(1)
			go sess.webseedLoop(spec)
		}
		for !sess.IsCompleted() {
			if time.Since(start) > 2*time.Minute {
				b.Fatalf("webseed download incomplete: %.1f%%", sess.PercentComplete())
			}
			time.Sleep(time.Millisecond)
		}
		b.StopTimer()
		sess.Close()
		b.StartTimer()
	}
}

// rateLimitedMirror serves data, honouring single Range requests, at no more
// than bytesPerSec per response.
func rateLimitedMirror(data []byte, bytesPerSec int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := 0, len(data)-1
		if rg, ok := strings.CutPrefix(r.Header.Get("Range"), "bytes="); ok {
			first, last, _ := strings.Cut(rg, "-")
			start, _ = strconv.Atoi(first)
			if last != "" {
				end, _ = strconv.Atoi(last)
			}
			if start < 0 || end >= len(data) || start > end {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		}
		body := data[start : end+1]
		const chunk = 16 << 10
		began := time.Now()
		for sent := 0; sent < len(body); {
			n := min(chunk, len(body)-sent)
			if _, err := w.Write(body[sent : sent+n]); err != nil {
				return
			}
			sent += n
			due := began.Add(time.Duration(float64(sent) / float64(bytesPerSec) * float64(time.Second)))
			if wait := time.Until(due); wait > 0 {
				time.Sleep(wait)
			}
		}
	}))
}
