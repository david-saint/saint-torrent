package downloader

// Opt-in benchmarks for the recheck scheduler. Nothing here runs unless the
// corresponding environment variable is set, so `go test ./...` is unaffected.
//
// Both harnesses A/B the scheduling policy inside a single build by swapping the
// two package variables PR #108 introduced, so comparing policies needs no
// worktree juggling:
//
//   current  - as shipped: paused rechecks serialized to one slot, each yielding
//              2x the last piece's time while any transfer is above 512 KiB/s
//   pre108   - the policy before PR #108: paused rechecks share the core-bounded
//              gate and never yield
//
// Synthetic corpus (writes its own files, safe anywhere):
//
//	ST_RECHECK_DIR=/Volumes/SAINT/bench ST_RECHECK_MIB=2048 \
//	  go test -count=1 -run TestRecheckPolicies -timeout 120m ./pkg/downloader -v
//
// -count=1 matters: without it Go serves a cached result and you measure nothing.

import (
	"context"
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

func benchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// recheckPolicy names a scheduling policy and installs it for the duration of a
// subtest. The swap is safe because every policy change happens before the recheck
// goroutines start and is undone after they are joined.
type recheckPolicy struct {
	name  string
	apply func(t *testing.T)
}

func restorePolicy(t *testing.T) {
	t.Helper()
	gate, yield := pausedVerifyGate, pausedRecheckYield
	t.Cleanup(func() {
		pausedVerifyGate = gate
		pausedRecheckYield = yield
	})
}

func recheckPolicies() []recheckPolicy {
	return []recheckPolicy{
		{name: "current", apply: func(t *testing.T) { restorePolicy(t) }},
		{name: "pre108", apply: func(t *testing.T) {
			restorePolicy(t)
			pausedVerifyGate = make(chan struct{}, max(1, runtime.GOMAXPROCS(0)))
			pausedRecheckYield = func(context.Context, time.Duration) {}
		}},
	}
}

// benchTorrent is one torrent's metadata plus the storage layout it maps to.
type benchTorrent struct {
	tor   *torrent.Torrent
	files []storage.FileInfo
	dir   string
	bytes int64
}

// recheckOne drives a full recheck through the real session code. It deliberately
// calls runVerification rather than verifyResume: verifyResume ends in finishVerify,
// which persists fast-resume state, and this harness must never write to a library
// it is only measuring.
func recheckOne(t *testing.T, item benchTorrent, paused bool) {
	t.Helper()
	st, err := storage.NewFileStorage(item.dir, item.files, item.tor.PieceLength)
	if err != nil {
		t.Errorf("%s: open storage: %v", item.tor.Name, err)
		return
	}
	defer func() { _ = st.Close() }()
	sess, err := newSession(item.tor, st, [20]byte{}, 0, item.dir, true)
	if err != nil {
		t.Errorf("%s: new session: %v", item.tor.Name, err)
		return
	}
	defer sess.cancel()
	if paused {
		sess.mu.Lock()
		sess.paused = true
		sess.mu.Unlock()
	}
	if !sess.runVerification(sess.ctx) {
		t.Errorf("%s: verification did not run to completion", item.tor.Name)
	}
}

// recheckAll runs every torrent's recheck concurrently, the way a restored library
// does, and returns how long the slowest one took.
func recheckAll(t *testing.T, lib []benchTorrent, paused bool) time.Duration {
	t.Helper()
	start := time.Now()
	var wg sync.WaitGroup
	for _, item := range lib {
		wg.Add(1)
		go func(item benchTorrent) {
			defer wg.Done()
			recheckOne(t, item, paused)
		}(item)
	}
	wg.Wait()
	return time.Since(start)
}

// evictLibrary drops every payload file from the page cache where the platform
// supports it, so a second policy is not simply reading the first one's cache.
func evictLibrary(lib []benchTorrent) {
	for _, item := range lib {
		for _, f := range item.files {
			h, err := os.Open(filepath.Join(item.dir, f.Path))
			if err != nil {
				continue
			}
			evictFromCache(h)
			_ = h.Close()
		}
	}
}

// rateWriter models the download side: sequential writes paced to a target rate.
// It reports what it actually achieved and the worst single-write stall, which is
// what a user perceives as a download stuttering.
type rateWriter struct {
	written  atomic.Int64
	maxStall atomic.Int64
	stop     chan struct{}
	done     chan struct{}
}

func startRateWriter(t *testing.T, dir string, targetMBps float64) *rateWriter {
	t.Helper()
	w := &rateWriter{stop: make(chan struct{}), done: make(chan struct{})}
	if targetMBps <= 0 {
		close(w.done)
		return w
	}
	path := filepath.Join(dir, ".sainttorrent-bench-writer.tmp")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create writer file: %v", err)
	}
	chunk := make([]byte, 4<<20)
	targetBps := targetMBps * 1e6
	go func() {
		defer close(w.done)
		defer func() {
			_ = f.Close()
			_ = os.Remove(path)
		}()
		start := time.Now()
		var off int64
		for {
			select {
			case <-w.stop:
				return
			default:
			}
			if want := time.Duration(float64(off) / targetBps * float64(time.Second)); want > time.Since(start) {
				select {
				case <-time.After(want - time.Since(start)):
				case <-w.stop:
					return
				}
			}
			began := time.Now()
			n, err := f.WriteAt(chunk, off%(4<<30))
			stall := time.Since(began).Nanoseconds()
			for {
				prev := w.maxStall.Load()
				if stall <= prev || w.maxStall.CompareAndSwap(prev, stall) {
					break
				}
			}
			if err != nil {
				return
			}
			off += int64(n)
			w.written.Add(int64(n))
		}
	}()
	return w
}

func (w *rateWriter) stopAndReport(elapsed time.Duration) (mbps float64, maxStall time.Duration) {
	select {
	case <-w.done:
	default:
		close(w.stop)
		<-w.done
	}
	if elapsed <= 0 {
		return 0, 0
	}
	return float64(w.written.Load()) / elapsed.Seconds() / 1e6, time.Duration(w.maxStall.Load())
}

// report prints one policy's result in a form that is easy to diff across runs.
func report(t *testing.T, policy string, totalBytes int64, elapsed time.Duration, w *rateWriter) {
	t.Helper()
	wmbps, stall := w.stopAndReport(elapsed)
	line := fmt.Sprintf("POLICY=%-8s recheck=%6.1fs %8.1f MB/s",
		policy, elapsed.Seconds(), float64(totalBytes)/elapsed.Seconds()/1e6)
	if wmbps > 0 {
		line += fmt.Sprintf("  | writer %6.1f MB/s maxStall=%s", wmbps, stall.Round(time.Millisecond))
	}
	t.Log(line)
}

// buildSyntheticLibrary writes n single-file torrents of mib MiB each. Files that
// already exist at the right size are reused so repeat runs skip the corpus build.
func buildSyntheticLibrary(t *testing.T, dir string, n, mib int, pieceLen int64) []benchTorrent {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	size := int64(mib) << 20
	out := make([]benchTorrent, 0, n)
	block := make([]byte, 1<<20)
	for i := range n {
		name := fmt.Sprintf("bench-%d.bin", i)
		path := filepath.Join(dir, name)
		if st, err := os.Stat(path); err != nil || st.Size() != size {
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			for w := int64(0); w < size; w += int64(len(block)) {
				for j := range block {
					block[j] = byte((int(w>>20)*31 + j*7 + i*13) & 0xff)
				}
				if _, err := f.Write(block); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}
		tor := hashIntoTorrent(t, dir, name, size, pieceLen)
		out = append(out, benchTorrent{
			tor:   tor,
			files: []storage.FileInfo{{Path: name, Length: size}},
			dir:   dir,
			bytes: size,
		})
	}
	return out
}

// hashIntoTorrent builds a real single-file torrent whose piece hashes match what
// is on disk, so every piece of the synthetic corpus verifies.
func hashIntoTorrent(t *testing.T, dir, name string, size, pieceLen int64) *torrent.Torrent {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	numPieces := int((size + pieceLen - 1) / pieceLen)
	pieces := make([]byte, 0, numPieces*20)
	buf := make([]byte, pieceLen)
	for p := range numPieces {
		off := int64(p) * pieceLen
		n := min(pieceLen, size-off)
		if _, err := f.ReadAt(buf[:n], off); err != nil {
			t.Fatal(err)
		}
		sum := sha1.Sum(buf[:n])
		pieces = append(pieces, sum[:]...)
	}
	raw, err := bencode.Marshal(map[string]any{
		"info": map[string]any{
			"name":         name,
			"piece length": pieceLen,
			"pieces":       string(pieces),
			"length":       size,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tor, err := torrent.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return tor
}

// TestRecheckPolicies compares the scheduling policies over a synthetic corpus.
func TestRecheckPolicies(t *testing.T) {
	dir := os.Getenv("ST_RECHECK_DIR")
	if dir == "" {
		t.Skip("set ST_RECHECK_DIR to run the synthetic recheck benchmark")
	}
	n := benchEnvInt("ST_RECHECK_TORRENTS", 4)
	mib := benchEnvInt("ST_RECHECK_MIB", 512)
	pieceLen := int64(benchEnvInt("ST_RECHECK_PIECE_MIB", 4)) << 20
	writeMBps := float64(benchEnvInt("ST_RECHECK_WRITE_MBPS", 0))
	paused := benchEnvInt("ST_RECHECK_PAUSED", 1) == 1

	lib := buildSyntheticLibrary(t, dir, n, mib, pieceLen)
	var total int64
	for _, item := range lib {
		total += item.bytes
	}
	t.Logf("synthetic: %d torrents, %.2f GiB, piece=%dMiB, paused=%v, cpus=%d, cache-eviction=%v",
		len(lib), float64(total)/(1<<30), pieceLen>>20, paused, runtime.NumCPU(), cacheEvictionSupported)

	for _, policy := range recheckPolicies() {
		t.Run(policy.name, func(t *testing.T) {
			policy.apply(t)
			evictLibrary(lib)
			liveTransfers.Store(0)
			w := startRateWriter(t, dir, writeMBps)
			if writeMBps > 0 {
				liveTransfers.Store(1)
			}
			elapsed := recheckAll(t, lib, paused)
			liveTransfers.Store(0)
			report(t, policy.name, total, elapsed, w)
		})
	}
}
