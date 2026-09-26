package storage

import (
	"fmt"
	"path/filepath"
	"testing"
)

// blockBenchLayout lays out fileCount equal files covering 64 MiB, so a 16 KiB
// block read touches the same bytes whatever the file count and only the cost
// of finding the files overlapping it changes. fileCount must divide 64 MiB.
func blockBenchLayout(fileCount int) []FileInfo {
	const total = 64 << 20
	files := make([]FileInfo, fileCount)
	for i := range files {
		files[i] = FileInfo{Path: filepath.Join(fmt.Sprintf("d%d", i/1000), fmt.Sprintf("f%d", i)), Length: total / int64(fileCount)}
	}
	return files
}

type blockBenchStorage interface {
	ReadBlock(int64, int64, []byte) (int, error)
	WriteBlock(int64, int64, []byte) error
	Close() error
}

func benchmarkBlocks(b *testing.B, open func(string, []FileInfo, int64) (blockBenchStorage, error)) {
	const pieceLength = 256 << 10
	for _, fileCount := range []int{1, 1 << 10, 1 << 14} {
		st, err := open(b.TempDir(), blockBenchLayout(fileCount), pieceLength)
		if err != nil {
			b.Fatal(err)
		}
		lastPiece := int64(64<<20)/pieceLength - 1
		block := make([]byte, 16<<10)
		piece := make([]byte, pieceLength)
		if err := st.WriteBlock(lastPiece, 0, piece); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("files=%d/ReadBlock16KiB", fileCount), func(b *testing.B) {
			b.SetBytes(int64(len(block)))
			for b.Loop() {
				if _, err := st.ReadBlock(lastPiece, pieceLength-int64(len(block)), block); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("files=%d/WriteBlock16KiB", fileCount), func(b *testing.B) {
			b.SetBytes(int64(len(block)))
			for b.Loop() {
				if err := st.WriteBlock(lastPiece, pieceLength-int64(len(block)), block); err != nil {
					b.Fatal(err)
				}
			}
		})
		// Many peers reading the same file at once: the seed path's shared state
		// (the storage lock and the file's cached handle) under contention.
		b.Run(fmt.Sprintf("files=%d/ReadBlock16KiBParallel", fileCount), func(b *testing.B) {
			b.SetBytes(int64(len(block)))
			b.RunParallel(func(pb *testing.PB) {
				buf := make([]byte, len(block))
				for pb.Next() {
					if _, err := st.ReadBlock(lastPiece, pieceLength-int64(len(buf)), buf); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
		_ = st.Close()
	}
}

// BenchmarkFileStorageBlocks measures the per-block cost on the seed (ReadBlock)
// and piece-write (WriteBlock) paths against the file count; the block always
// sits at the end of the torrent, where a linear scan is at its worst.
func BenchmarkFileStorageBlocks(b *testing.B) {
	benchmarkBlocks(b, func(dir string, files []FileInfo, pieceLength int64) (blockBenchStorage, error) {
		return NewFileStorage(dir, files, pieceLength)
	})
}
