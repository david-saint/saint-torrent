package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// hintBenchHotFiles is how many payload files a download dirties between two hint
// writes (about one state-persist tick of a busy transfer).
const hintBenchHotFiles = 128

const benchBaseMtime int64 = 1_700_000_000_000_000_000

// newHintBenchStorage builds a FileStorage over n layouts without creating n files:
// the hint path only reads layout paths and lengths, and statting a payload file
// that is absent costs the same syscall as statting a present one. Only the hot
// files that the benchmark keeps dirtying exist on disk, so the setup stays small
// however many files are modeled. The state file goes to dir.
func newHintBenchStorage(tb testing.TB, dir string, n int, withCheckpoint bool) (*FileStorage, []*fileLayout) {
	tb.Helper()
	root, err := OpenDownloadRoot(dir)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = root.Close() })
	resolver, err := NewPathResolver(dir)
	if err != nil {
		tb.Fatal(err)
	}
	const fileLength = 1 << 10
	const pieceLength = 1 << 18
	layouts := make([]*fileLayout, n)
	stateMt := make(map[string]int64, n)
	var offset int64
	for i := range layouts {
		path := fmt.Sprintf("d%04d/payload-file-%07d.bin", i%1000, i)
		layouts[i] = &fileLayout{path: path, idx: i, downloadRoot: root, length: fileLength, startOffset: offset, endOffset: offset + fileLength}
		offset += fileLength
		stateMt[path] = benchBaseMtime + int64(i)
	}
	s := &FileStorage{
		resolver:        resolver,
		baseDir:         resolver.BaseDir(),
		files:           layouts,
		pieceLength:     pieceLength,
		totalSize:       offset,
		stateFileMt:     stateMt,
		stateFileInfo:   make(map[string]os.FileInfo),
		initialInfo:     make(map[string]os.FileInfo),
		trustedIdentity: make(map[string]string),
		dirty:           make(map[*fileLayout]dirtyState),
		downloadRoot:    root,
	}
	var hot []*fileLayout
	step := n / hintBenchHotFiles
	if step < 1 {
		step = 1
	}
	for i := 0; i < n && len(hot) < hintBenchHotFiles; i += step {
		file := layouts[i]
		full := filepath.Join(dir, file.path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			tb.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0o644); err != nil {
			tb.Fatal(err)
		}
		hot = append(hot, file)
	}
	if withCheckpoint {
		checkpoint := resumeCheckpoint{
			identities: make([]string, n),
			mtimes:     make([]int64, n),
			valid:      true,
			complete:   true,
		}
		for i := range layouts {
			checkpoint.identities[i] = fmt.Sprintf("16777232:%d:%d", 20_000_000+i, benchBaseMtime+int64(i))
			checkpoint.mtimes[i] = benchBaseMtime + int64(i)
		}
		s.checkpoint = checkpoint
	}
	return s, hot
}

// hintBenchPieces returns the verified and unverified piece lists a download
// holding every other piece verified would pass to the hint.
func hintBenchPieces(s *FileStorage) (verified, unverified []int) {
	for i := 0; i < s.pieceCount(); i++ {
		if i%2 == 0 {
			verified = append(verified, i)
		} else {
			unverified = append(unverified, i)
		}
	}
	return verified, unverified
}

// BenchmarkResumeHint measures one non-durable SaveResumeState at 100k and 500k
// files while a writer goroutine does exactly what WriteBlock's dirty marking
// does (take mtMu, mark the file dirty) and records how long it waited for the
// lock. The longest wait is the longest stretch the hint held mtMu. Run it
// explicitly; it is not part of the default test run:
//
//	go test -run '^$' -bench BenchmarkResumeHint -benchtime=20x ./pkg/storage
func BenchmarkResumeHint(b *testing.B) {
	for _, n := range []int{100_000, 500_000} {
		for _, withCheckpoint := range []bool{false, true} {
			name := fmt.Sprintf("files=%d/checkpoint=%t", n, withCheckpoint)
			b.Run(name, func(b *testing.B) { benchmarkResumeHint(b, n, withCheckpoint) })
		}
	}
}

func benchmarkResumeHint(b *testing.B, n int, withCheckpoint bool) {
	dir := b.TempDir()
	s, hot := newHintBenchStorage(b, dir, n, withCheckpoint)
	verified, unverified := hintBenchPieces(s)
	const infoHash = "0123456789abcdef0123456789abcdef01234567"
	// Prime once so the cost of the first, uncached hint is not averaged in.
	if err := s.SaveResumeState(infoHash, verified, unverified, false); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "."+infoHash+".state"))
	if err != nil {
		b.Fatal(err)
	}

	var (
		stop  = make(chan struct{})
		done  sync.WaitGroup
		mu    sync.Mutex
		waits []time.Duration
	)
	done.Add(1)
	go func() {
		defer done.Done()
		local := make([]time.Duration, 0, 1<<16)
		next := 0
		for {
			select {
			case <-stop:
				mu.Lock()
				waits = append(waits, local...)
				mu.Unlock()
				return
			default:
			}
			file := hot[next%len(hot)]
			next++
			start := time.Now()
			s.mtMu.Lock()
			wait := time.Since(start)
			s.dirty[file] |= dirtyMeta | dirtySync
			s.allCreated = false
			s.mtMu.Unlock()
			if len(local) < cap(local) {
				local = append(local, wait)
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := s.SaveResumeState(infoHash, verified, unverified, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
	done.Wait()

	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	if len(waits) > 0 {
		var total time.Duration
		for _, w := range waits {
			total += w
		}
		b.ReportMetric(float64(waits[len(waits)-1])/1e6, "max-mtMu-wait-ms")
		b.ReportMetric(float64(waits[len(waits)*99/100])/1e6, "p99-mtMu-wait-ms")
		b.ReportMetric(float64(total)/float64(len(waits))/1e6, "mean-mtMu-wait-ms")
	}
	b.ReportMetric(float64(info.Size())/(1<<20), "state-MiB")
}
