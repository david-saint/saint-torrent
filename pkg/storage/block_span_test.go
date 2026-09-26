package storage

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/rand"
	"testing"
)

// checkBlockSpans drives random block writes and reads over a layout full of
// empty files, including ones on block and piece boundaries, and compares every
// read and piece hash against a flat reference copy. It pins the binary-search
// lookup of overlapping files to exactly what a scan of every file selected.
func checkBlockSpans(t *testing.T, open func(string, []FileInfo, int64) (Storage, error)) {
	t.Helper()
	rng := rand.New(rand.NewSource(1))
	var files []FileInfo
	var total int64
	for i := range 64 {
		length := int64(0)
		if i%3 != 0 {
			length = int64(rng.Intn(40))
		}
		files = append(files, FileInfo{Path: fmt.Sprintf("f%02d", i), Length: length})
		total += length
	}
	files = append(files, FileInfo{Path: "last", Length: 7})
	total += 7
	const pieceLength = 32
	st, err := open(t.TempDir(), files, pieceLength)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	want := make([]byte, total)
	pieces := (total + pieceLength - 1) / pieceLength
	for range 2000 {
		piece := rng.Int63n(pieces)
		pieceLen := st.PieceLength(piece)
		offset := rng.Int63n(pieceLen)
		length := 1 + rng.Int63n(pieceLen-offset)
		start := piece*pieceLength + offset
		if rng.Intn(2) == 0 {
			block := make([]byte, length)
			rng.Read(block)
			if err := st.WriteBlock(piece, offset, block); err != nil {
				t.Fatalf("WriteBlock(%d, %d, %d): %v", piece, offset, length, err)
			}
			copy(want[start:], block)
			continue
		}
		got := make([]byte, length)
		if _, err := st.ReadBlock(piece, offset, got); err != nil {
			t.Fatalf("ReadBlock(%d, %d, %d): %v", piece, offset, length, err)
		}
		if !bytes.Equal(got, want[start:start+length]) {
			t.Fatalf("ReadBlock(%d, %d, %d) = %x, want %x", piece, offset, length, got, want[start:start+length])
		}
	}
	for piece := range pieces {
		start := piece * pieceLength
		expected := sha1.Sum(want[start : start+st.PieceLength(piece)])
		if ok, err := st.VerifyPiece(piece, expected); err != nil || !ok {
			t.Fatalf("VerifyPiece(%d) = %v, %v; want true", piece, ok, err)
		}
	}
}

func TestFileStorageBlockSpans(t *testing.T) {
	checkBlockSpans(t, func(dir string, files []FileInfo, pieceLength int64) (Storage, error) {
		return NewStorage(dir, files, pieceLength)
	})
}
