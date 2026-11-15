package storage

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkSaveResumeHint measures active-download resume hint writes
// (SaveResumeState with durable=false) and concurrent mtMu wait times across
// 100k and 500k files while ~128 files are dirtied per tick.
func BenchmarkSaveResumeHint(b *testing.B) {
	const (
		fileLen  = 128
		pieceLen = 16 << 10 // 128 files per piece
	)
	infoHash := "0123456789abcdef0123456789abcdef01234567"
	pieceBuf := make([]byte, pieceLen)

	for _, fileCount := range []int{100_000, 500_000} {
		b.Run(fmt.Sprintf("files=%d", fileCount), func(b *testing.B) {
			files := make([]FileInfo, fileCount)
			for i := range files {
				files[i] = FileInfo{
					Path:   filepath.Join(fmt.Sprintf("d%d", i/1000), fmt.Sprintf("f%d", i)),
					Length: fileLen,
				}
			}

			st, err := NewFileStorage(b.TempDir(), files, pieceLen)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = st.Close() }()

			totalPieces := st.pieceCount()
			verified := make([]int, totalPieces/2)
			for i := range verified {
				verified[i] = i
			}
			if err := st.SaveResumeState(infoHash, verified, nil, true); err != nil {
				b.Fatal(err)
			}

			probeStart := make(chan struct{})
			probeDone := make(chan int64)
			var probing atomic.Bool
			done := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					case <-probeStart:
						var maxWait int64
						for probing.Load() {
							t0 := time.Now()
							st.mtMu.Lock()
							wait := time.Since(t0).Nanoseconds()
							st.mtMu.Unlock()
							if wait > maxWait {
								maxWait = wait
							}
							runtime.Gosched()
						}
						probeDone <- maxWait
					}
				}
			}()
			defer func() {
				close(done)
				wg.Wait()
			}()

			var (
				iter         int
				hintTotal    time.Duration
				sumMaxWaitNs int64
				worstWaitNs  int64
			)
			for b.Loop() {
				b.StopTimer()
				piece := int64(totalPieces/2 + (iter % max(1, totalPieces/2)))
				iter++
				if err := st.WriteBlock(piece, 0, pieceBuf); err != nil {
					b.Fatal(err)
				}
				unverified := []int{int(piece)}

				probing.Store(true)
				probeStart <- struct{}{}
				b.StartTimer()
				t0 := time.Now()
				err := st.SaveResumeState(infoHash, verified, unverified, false)
				elapsed := time.Since(t0)
				b.StopTimer()
				probing.Store(false)
				maxWait := <-probeDone
				if err != nil {
					b.Fatal(err)
				}
				hintTotal += elapsed
				sumMaxWaitNs += maxWait
				if maxWait > worstWaitNs {
					worstWaitNs = maxWait
				}
				b.StartTimer()
			}

			if b.N > 0 {
				b.ReportMetric(float64(hintTotal.Nanoseconds())/float64(b.N)/1e6, "hint-ms/op")
				b.ReportMetric(float64(sumMaxWaitNs)/float64(b.N)/1e6, "mtMu-wait-avg-ms")
				b.ReportMetric(float64(worstWaitNs)/1e6, "mtMu-wait-max-ms")
			}
		})
	}
}
