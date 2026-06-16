package storage

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestStorage_SingleFile(t *testing.T) {
	// Create temporary base directory
	tmpDir, err := os.MkdirTemp("", "torrent-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files := []FileInfo{
		{
			Path:   "single_file.bin",
			Length: 100,
		},
	}
	pieceLength := int64(50)

	s, err := NewStorage(tmpDir, files, pieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Verify file was created and is of correct size
	filePath := filepath.Join(tmpDir, "single_file.bin")
	fi, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if fi.Size() != 100 {
		t.Errorf("expected file size 100, got %d", fi.Size())
	}

	// Write block to piece 0
	data1 := []byte("hello world")
	err = s.WriteBlock(0, 5, data1)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	// Read block back
	readBuf := make([]byte, len(data1))
	n, err := s.ReadBlock(0, 5, readBuf)
	if err != nil {
		t.Fatalf("failed to read block: %v", err)
	}
	if n != len(data1) {
		t.Errorf("expected to read %d bytes, got %d", len(data1), n)
	}
	if !bytes.Equal(readBuf, data1) {
		t.Errorf("expected data %q, got %q", data1, readBuf)
	}

	// Write block to piece 1
	data2 := []byte("torrent testing")
	err = s.WriteBlock(1, 10, data2)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	// Read block 2 back
	readBuf2 := make([]byte, len(data2))
	n, err = s.ReadBlock(1, 10, readBuf2)
	if err != nil {
		t.Fatalf("failed to read block: %v", err)
	}
	if n != len(data2) {
		t.Errorf("expected to read %d bytes, got %d", len(data2), n)
	}
	if !bytes.Equal(readBuf2, data2) {
		t.Errorf("expected data %q, got %q", data2, readBuf2)
	}
}

func TestStorage_MultiFile_BoundaryCrossing(t *testing.T) {
	// Create temporary base directory
	tmpDir, err := os.MkdirTemp("", "torrent-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Two files: file1 (length 30), file2 (length 40)
	// Piece length: 25. Total torrent length: 70.
	files := []FileInfo{
		{
			Path:   "dir1/file1.bin",
			Length: 30,
		},
		{
			Path:   "dir2/file2.bin",
			Length: 40,
		},
	}
	pieceLength := int64(25)

	s, err := NewStorage(tmpDir, files, pieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Verify both files were created with correct sizes
	fi1, err := os.Stat(filepath.Join(tmpDir, "dir1/file1.bin"))
	if err != nil {
		t.Fatalf("failed to stat file1: %v", err)
	}
	if fi1.Size() != 30 {
		t.Errorf("expected file1 size 30, got %d", fi1.Size())
	}

	fi2, err := os.Stat(filepath.Join(tmpDir, "dir2/file2.bin"))
	if err != nil {
		t.Fatalf("failed to stat file2: %v", err)
	}
	if fi2.Size() != 40 {
		t.Errorf("expected file2 size 40, got %d", fi2.Size())
	}

	// Write block that spans across file1 and file2.
	// Torrent offsets:
	// file1: [0, 30)
	// file2: [30, 70)
	// Let's write from piece 1, offset 0 (global offset 25) with length 10.
	// This covers global offsets [25, 35).
	// File1 will contain global offsets [25, 30) -> 5 bytes.
	// File2 will contain global offsets [30, 35) -> 5 bytes.
	data := []byte("ABCDEFGHIJ") // 10 bytes
	err = s.WriteBlock(1, 0, data)
	if err != nil {
		t.Fatalf("failed to write boundary block: %v", err)
	}

	// Verify that the files actually contain the correct segments
	content1, err := os.ReadFile(filepath.Join(tmpDir, "dir1/file1.bin"))
	if err != nil {
		t.Fatalf("failed to read file1: %v", err)
	}
	// The last 5 bytes of file1 (offsets 25-29) should be "ABCDE"
	expected1 := make([]byte, 30)
	copy(expected1[25:], []byte("ABCDE"))
	if !bytes.Equal(content1, expected1) {
		t.Errorf("file1 content mismatch. Expected suffix %q, got %q", "ABCDE", content1[25:])
	}

	content2, err := os.ReadFile(filepath.Join(tmpDir, "dir2/file2.bin"))
	if err != nil {
		t.Fatalf("failed to read file2: %v", err)
	}
	// The first 5 bytes of file2 (offsets 0-4) should be "FGHIJ"
	expected2 := make([]byte, 40)
	copy(expected2[0:], []byte("FGHIJ"))
	if !bytes.Equal(content2, expected2) {
		t.Errorf("file2 content mismatch. Expected prefix %q, got %q", "FGHIJ", content2[:5])
	}

	// Read block back across the boundary
	readBuf := make([]byte, 10)
	n, err := s.ReadBlock(1, 0, readBuf)
	if err != nil {
		t.Fatalf("failed to read boundary block: %v", err)
	}
	if n != 10 {
		t.Errorf("expected to read 10 bytes, got %d", n)
	}
	if !bytes.Equal(readBuf, data) {
		t.Errorf("expected block content %q, got %q", data, readBuf)
	}
}

func TestStorage_VerifyPiece(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "torrent-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files := []FileInfo{
		{
			Path:   "test.bin",
			Length: 50,
		},
	}
	pieceLength := int64(20)

	s, err := NewStorage(tmpDir, files, pieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Fill piece 0 (length 20) with data
	piece0Data := []byte("0123456789abcdefghij") // 20 bytes
	err = s.WriteBlock(0, 0, piece0Data)
	if err != nil {
		t.Fatalf("failed to write piece 0 data: %v", err)
	}

	// Compute expected hash
	expectedHash0 := sha1.Sum(piece0Data)

	// Verify correct hash matches
	ok, err := s.VerifyPiece(0, expectedHash0)
	if err != nil {
		t.Fatalf("failed to verify piece 0: %v", err)
	}
	if !ok {
		t.Errorf("expected piece 0 verification to succeed")
	}

	// Verify incorrect hash fails
	wrongHash := [20]byte{1, 2, 3}
	ok, err = s.VerifyPiece(0, wrongHash)
	if err != nil {
		t.Fatalf("failed to verify piece 0 with wrong hash: %v", err)
	}
	if ok {
		t.Errorf("expected piece 0 verification to fail with incorrect hash")
	}

	// Verify last piece (piece 2, offset 0 to 10 -> length 10)
	piece2Data := []byte("lastegment") // 10 bytes
	err = s.WriteBlock(2, 0, piece2Data)
	if err != nil {
		t.Fatalf("failed to write piece 2 data: %v", err)
	}

	expectedHash2 := sha1.Sum(piece2Data)
	ok, err = s.VerifyPiece(2, expectedHash2)
	if err != nil {
		t.Fatalf("failed to verify piece 2: %v", err)
	}
	if !ok {
		t.Errorf("expected piece 2 verification to succeed")
	}
}

func TestStorage_FastResume(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "torrent-storage-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files := []FileInfo{
		{
			Path:   "test_file.bin",
			Length: 100,
		},
	}
	pieceLength := int64(50)

	s, err := NewStorage(tmpDir, files, pieceLength)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Write block
	piece0Data := make([]byte, 50)
	copy(piece0Data, []byte("some initial data"))
	_ = s.WriteBlock(0, 0, piece0Data)

	infoHashHex := "0123456789abcdef0123456789abcdef01234567"
	completedPieces := []int{0}

	err = s.SaveState(infoHashHex, completedPieces)
	if err != nil {
		t.Fatalf("failed to save fast resume state: %v", err)
	}

	// Load back
	loaded, err := s.LoadState(infoHashHex)
	if err != nil {
		t.Fatalf("failed to load state: %v", err)
	}

	if len(loaded) != 1 || loaded[0] != 0 {
		t.Errorf("expected loaded pieces [0], got %v", loaded)
	}

	// Try wrong info hash
	_, err = s.LoadState("0000000000000000000000000000000000000000")
	if err == nil {
		t.Error("expected error for non-existent or mismatching info hash")
	}
}

func TestStorageRejectsDuplicatePaths(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := NewStorage(tmpDir, []FileInfo{
		{Path: "same.bin", Length: 1},
		{Path: "./same.bin", Length: 1},
	}, 1)
	if err == nil {
		t.Fatal("expected duplicate storage paths to be rejected")
	}
}

func TestStorageRejectsReservedInternalNames(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{"dht routing table", ".dht_nodes"},
		{"fast-resume state", ".abc123.state"},
		{"reserved top-level dir", filepath.Join(".dht_nodes", "inner.bin")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if _, err := NewStorage(tmpDir, []FileInfo{{Path: tc.path, Length: 1}}, 1); err == nil {
				t.Fatalf("expected reserved name %q to be rejected", tc.path)
			}
		})
	}
}

func TestStorageAllowsNestedReservedNames(t *testing.T) {
	// The collision only exists at the download-dir root, so a reserved name nested
	// under a normal top-level directory must remain allowed.
	tmpDir := t.TempDir()
	if _, err := NewStorage(tmpDir, []FileInfo{{Path: filepath.Join("movie", ".dht_nodes"), Length: 1}}, 1); err != nil {
		t.Fatalf("nested reserved-like name should be allowed, got: %v", err)
	}
}

func TestStorageRejectsSymlinkPathComponents(t *testing.T) {
	tmpDir := t.TempDir()
	outside := t.TempDir()
	linkPath := filepath.Join(tmpDir, "linkdir")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Skipf("symlink creation not available: %v", err)
	}

	_, err := NewStorage(tmpDir, []FileInfo{{Path: "linkdir/file.bin", Length: 1}}, 1)
	if err == nil {
		t.Fatal("expected symlink path component to be rejected")
	}
}

func TestStorageRejectsFinalSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.bin")
	if err := os.WriteFile(outside, []byte("do-not-touch"), 0644); err != nil {
		t.Fatalf("failed to create outside file: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(tmpDir, "file.bin")); err != nil {
		t.Skipf("symlink creation not available: %v", err)
	}

	_, err := NewStorage(tmpDir, []FileInfo{{Path: "file.bin", Length: 1}}, 1)
	if err == nil {
		t.Fatal("expected final symlink to be rejected")
	}
	content, err := os.ReadFile(outside)
	if err != nil {
		t.Fatalf("failed to read outside file: %v", err)
	}
	if string(content) != "do-not-touch" {
		t.Fatalf("outside file was modified through symlink: %q", content)
	}
}

// TestStorageConcurrentBlockIO exercises the dropped global lock: many goroutines
// read distinct blocks concurrently (and one writes a separate region) while the
// data is verified for correctness. Under -race this also proves the cached-handle
// read path is safe for parallel use.
func TestStorageConcurrentBlockIO(t *testing.T) {
	tmpDir := t.TempDir()
	const pieceLen = 1 << 16 // 64 KB
	const numPieces = 32
	total := int64(pieceLen * numPieces)

	s, err := NewStorage(tmpDir, []FileInfo{{Path: "big.bin", Length: total}}, pieceLen)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer s.Close()

	// Seed every piece with deterministic, distinguishable bytes.
	for p := 0; p < numPieces; p++ {
		block := make([]byte, pieceLen)
		for i := range block {
			block[i] = byte((p*131 + i) & 0xff)
		}
		if err := s.WriteBlock(int64(p), 0, block); err != nil {
			t.Fatalf("seed write piece %d: %v", p, err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, numPieces*4)

	// Concurrent readers across all pieces.
	for g := 0; g < numPieces*4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p := g % numPieces
			buf := make([]byte, pieceLen)
			if _, err := s.ReadBlock(int64(p), 0, buf); err != nil {
				errCh <- err
				return
			}
			for i := range buf {
				if buf[i] != byte((p*131+i)&0xff) {
					errCh <- err
					return
				}
			}
		}(g)
	}

	// A concurrent writer to an independent region (last piece), exercising the
	// read-lock / write-lock interplay.
	wg.Add(1)
	go func() {
		defer wg.Done()
		block := make([]byte, pieceLen)
		for i := range block {
			block[i] = byte(i & 0xff)
		}
		for i := 0; i < 50; i++ {
			if err := s.WriteBlock(numPieces-1, 0, block); err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent block I/O failed: %v", err)
		}
	}
}

// TestStorageCloseReleasesHandlesAndBlocksIO verifies Close is idempotent and that
// block operations fail cleanly afterward instead of using a released handle.
func TestStorageCloseReleasesHandlesAndBlocksIO(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := NewStorage(tmpDir, []FileInfo{{Path: "f.bin", Length: 64}}, 64)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	data := bytes.Repeat([]byte{'z'}, 64)
	if err := s.WriteBlock(0, 0, data); err != nil {
		t.Fatalf("write: %v", err)
	}
	// Populate the cached read handle.
	if _, err := s.ReadBlock(0, 0, make([]byte, 64)); err != nil {
		t.Fatalf("read: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close should be a no-op, got: %v", err)
	}

	if _, err := s.ReadBlock(0, 0, make([]byte, 64)); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed reading after close, got %v", err)
	}
	if err := s.WriteBlock(0, 0, data); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("expected ErrStorageClosed writing after close, got %v", err)
	}
}

func TestStorageWriteBlockRepairsMissingFile(t *testing.T) {
	tmpDir := t.TempDir()
	files := []FileInfo{{Path: "missing.bin", Length: 32}}
	s, err := NewStorage(tmpDir, files, 32)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	filePath := filepath.Join(tmpDir, "missing.bin")
	if err := os.Remove(filePath); err != nil {
		t.Fatalf("failed to remove test file: %v", err)
	}

	data := bytes.Repeat([]byte{'x'}, 32)
	err = s.WriteBlock(0, 0, data)
	if !errors.Is(err, ErrFileRepaired) {
		t.Fatalf("expected ErrFileRepaired, got %v", err)
	}

	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read repaired file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("repaired file data mismatch")
	}
}

func TestOpenNoFollowSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "target.bin")
	if err := os.WriteFile(targetFile, []byte("target-content"), 0644); err != nil {
		t.Fatalf("failed to write target file: %v", err)
	}

	linkFile := filepath.Join(tmpDir, "link.bin")
	if err := os.Symlink(targetFile, linkFile); err != nil {
		t.Skipf("symlink creation not supported in this environment: %v", err)
	}

	f, err := openNoFollow(linkFile, os.O_RDONLY, 0)
	if err == nil {
		_ = f.Close()
		t.Fatal("expected openNoFollow to fail when opening a symlink")
	}

	content, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("failed to read target file: %v", err)
	}
	if string(content) != "target-content" {
		t.Fatalf("expected target file content to be unmodified, got %q", string(content))
	}
}
