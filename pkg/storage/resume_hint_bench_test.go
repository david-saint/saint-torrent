package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func makeHintBenchFiles(fileCount int) []FileInfo {
	files := make([]FileInfo, fileCount)
	for i := range files {
		length := int64(0)
		if i < 128 {
			length = 4
		}
		files[i] = FileInfo{
			Path:   filepath.Join(fmt.Sprintf("d%d", i/1000), fmt.Sprintf("f%d", i)),
			Length: length,
		}
	}
	return files
}

func BenchmarkResumeHint(b *testing.B) {
	for _, fileCount := range []int{100_000, 500_000} {
		name := fmt.Sprintf("%dk", fileCount/1000)
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			files := makeHintBenchFiles(fileCount)
			const pieceLength = 4
			st, err := NewFileStorage(dir, files, pieceLength)
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()

			hash := "0123456789abcdef0123456789abcdef01234567"
			pieceCount := st.pieceCount()
			verified := make([]int, pieceCount)
			for i := range verified {
				verified[i] = i
			}

			// Establish durable checkpoint baseline
			if err := st.SaveResumeState(hash, verified, nil, true); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				// Dirty ~128 files before the hint tick
				dirtyCount := 128
				if dirtyCount > fileCount {
					dirtyCount = fileCount
				}
				for i := 0; i < dirtyCount; i++ {
					if err := st.WriteBlock(int64(i), 0, []byte{9, 9, 9, 9}); err != nil {
						b.Fatal(err)
					}
				}

				var maxWait atomic.Int64
				done := make(chan struct{})
				go func() {
					for {
						select {
						case <-done:
							return
						default:
						}
						t0 := time.Now()
						st.mtMu.Lock()
						wait := time.Since(t0).Nanoseconds()
						st.mtMu.Unlock()
						for {
							cur := maxWait.Load()
							if wait <= cur || maxWait.CompareAndSwap(cur, wait) {
								break
							}
						}
						runtime.Gosched()
					}
				}()

				b.StartTimer()
				t0 := time.Now()
				err := st.SaveResumeState(hash, verified, nil, false)
				hintDuration := time.Since(t0)
				close(done)
				if err != nil {
					b.Fatal(err)
				}

				statePath := filepath.Join(st.BaseDir(), "."+hash+".state")
				info, err := os.Stat(statePath)
				if err != nil {
					b.Fatal(err)
				}

				longestWait := time.Duration(maxWait.Load())
				b.ReportMetric(float64(hintDuration.Milliseconds()), "ms/hint")
				b.ReportMetric(float64(longestWait.Microseconds())/1000.0, "ms/mtMu-wait")
				b.ReportMetric(float64(info.Size())/(1024*1024), "MiB/state")
				b.Logf("[%s] Hint write: %v | Longest wait for mtMu: %v | State file: %.1f MiB",
					name, hintDuration, longestWait, float64(info.Size())/(1024*1024))
			}
		})
	}
}
