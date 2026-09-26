//go:build !windows

package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestMMapStorageSurvivesTruncatedMapping: a mapped file that shrinks under its
// mapping (a second torrent naming the same path used to truncate it on add)
// raised SIGBUS on the next copy or hash, a fatal runtime error that took the
// whole client down on the first peer request. Each access path now returns an
// error instead, and a later write grows the file back and reports the repair.
func TestMMapStorageSurvivesTruncatedMapping(t *testing.T) {
	const size, pieceLength = 1 << 20, 256 << 10
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.bin")
	st, err := NewMMapStorage(dir, []FileInfo{{Path: "shared.bin", Length: size}}, pieceLength)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	piece := bytes.Repeat([]byte{7}, pieceLength)
	mapThenTruncate := func() {
		t.Helper()
		for i := range int64(size / pieceLength) {
			if err := st.WriteBlock(i, 0, piece); err != nil && !errors.Is(err, ErrFileRepaired) {
				t.Fatal(err)
			}
		}
		// Only whole pages past the new end fault; the first one stays readable.
		if err := os.Truncate(path, 1); err != nil {
			t.Fatal(err)
		}
	}
	block := make([]byte, 16<<10)

	mapThenTruncate()
	if _, err := st.ReadBlock(3, 0, block); !errors.Is(err, errMappingFault) {
		t.Fatalf("ReadBlock over a truncated mapping = %v, want errMappingFault", err)
	}
	// The faulted mapping is dropped: the next access reports the short file
	// through the size check instead of faulting again.
	if _, err := st.ReadBlock(3, 0, block); err == nil || errors.Is(err, errMappingFault) {
		t.Fatalf("ReadBlock after the fault = %v, want the size-mismatch error", err)
	}

	mapThenTruncate()
	if _, err := st.VerifyPiece(2, sha1.Sum(piece)); !errors.Is(err, errMappingFault) {
		t.Fatalf("VerifyPiece over a truncated mapping = %v, want errMappingFault", err)
	}

	mapThenTruncate()
	if err := st.WriteBlock(1, 0, piece); !errors.Is(err, ErrFileRepaired) {
		t.Fatalf("WriteBlock over a truncated mapping = %v, want ErrFileRepaired", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != size {
		t.Fatalf("file after the repairing write: %v, %v; want %d bytes", fi, err, size)
	}
	if _, err := st.ReadBlock(1, 0, block); err != nil || !bytes.Equal(block, piece[:len(block)]) {
		t.Fatalf("ReadBlock after the repairing write = %v, data intact %v", err, bytes.Equal(block, piece[:len(block)]))
	}
}

// TestRecoverMappingFaultRepanicsOtherPanics keeps the fault guard from
// swallowing genuine bugs: only a panic carrying a fault address is converted.
func TestRecoverMappingFaultRepanicsOtherPanics(t *testing.T) {
	defer func() {
		if r := recover(); r != "boom" {
			t.Fatalf("recovered %v, want the original panic re-raised", r)
		}
	}()
	_ = func() (err error) {
		defer recoverMappingFault(&err)
		panic("boom")
	}()
	t.Fatal("recoverMappingFault swallowed a panic that was not a memory fault")
}
