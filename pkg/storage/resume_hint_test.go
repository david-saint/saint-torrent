package storage

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestSaveResumeHintMatchesJSONMarshalAndTrustRules(t *testing.T) {
	root := t.TempDir()
	files := []FileInfo{
		{Path: "ascii.bin", Length: 6},
		{Path: `sub/quote"and\slash<tag>&amp.bin`, Length: 4},
		{Path: "日本語/ファイル.bin", Length: 6},
	}
	st, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	for i := int64(0); i < 4; i++ {
		if err := st.WriteBlock(i, 0, []byte{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
	}

	const infoHash = "0123456789abcdef0123456789abcdef01234567"

	// Hint before any durable checkpoint: all identities must be empty and
	// VerifiedPieces must be empty, matching json.Marshal(FastResumeState).
	if err := st.SaveResumeState(infoHash, []int{0, 1}, []int{2, 3}, false); err != nil {
		t.Fatal(err)
	}
	assertHintMatchesMarshal(t, st, infoHash, nil, []int{0, 1, 2, 3}, []bool{true, true, true})

	// Durable checkpoint establishes identities for all three files.
	if err := st.SaveResumeState(infoHash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}

	// Clean hint preserves all checkpoint identities and filters VerifiedPieces
	// to only those the caller still holds as verified.
	if err := st.SaveResumeState(infoHash, []int{0, 2, 3}, []int{1}, false); err != nil {
		t.Fatal(err)
	}
	assertHintMatchesMarshal(t, st, infoHash, []int{0, 2, 3}, []int{0, 2, 3, 1}, []bool{false, false, false})

	// Write to file 1 (piece 1 spans file 0 and file 1, piece 2 spans file 1 and file 2;
	// writing offset 0 of piece 2 writes the tail of file 1? Let's write directly into file 1 via piece 1 offset 2).
	if err := st.WriteBlock(1, 2, []byte{9, 9}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveResumeState(infoHash, []int{0, 3}, []int{1, 2}, false); err != nil {
		t.Fatal(err)
	}
	assertHintMatchesMarshal(t, st, infoHash, []int{0, 3}, []int{0, 3, 1, 2}, []bool{false, true, false})

	// Next hint touches file 2 without touching file 1 again: file 1 must STILL
	// keep an empty identity because it has not been durably checkpointed since
	// the write.
	if err := st.WriteBlock(3, 0, []byte{7, 7, 7, 7}); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveResumeState(infoHash, []int{0}, []int{1, 2, 3}, false); err != nil {
		t.Fatal(err)
	}
	assertHintMatchesMarshal(t, st, infoHash, []int{0}, []int{0, 1, 2, 3}, []bool{false, true, true})

	// Reload on a fresh storage instance and confirm trust rules hold.
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = next.Close() }()
	got, err := next.LoadResumeState(infoHash)
	if err != nil {
		t.Fatal(err)
	}
	want := ResumeState{Verified: []int{0}, Recheck: []int{1, 2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadResumeState = %+v, want %+v", got, want)
	}
}

func assertHintMatchesMarshal(t *testing.T, st *FileStorage, infoHash string, verifiedOut, completed []int, expectEmptyID []bool) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(st.BaseDir(), "."+infoHash+".state"))
	if err != nil {
		t.Fatal(err)
	}
	var loaded FastResumeState
	if err := json.Unmarshal(raw, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.InfoHashHex != infoHash || loaded.Version != 2 || loaded.PieceLength != st.pieceLength {
		t.Fatalf("unexpected header fields: %+v", loaded)
	}
	if len(loaded.Identities) != len(st.files) || len(loaded.Files) != len(st.files) {
		t.Fatalf("unexpected table lengths: ids=%d files=%d", len(loaded.Identities), len(loaded.Files))
	}
	for i, empty := range expectEmptyID {
		if empty && loaded.Identities[i] != "" {
			t.Fatalf("file %d identity = %q, want empty after write since checkpoint", i, loaded.Identities[i])
		}
		if !empty && loaded.Identities[i] == "" {
			t.Fatalf("file %d identity unexpectedly empty", i)
		}
		if loaded.Files[i].Path != st.files[i].path || loaded.Files[i].Size != st.files[i].length {
			t.Fatalf("file %d entry = %+v, want path=%q size=%d", i, loaded.Files[i], st.files[i].path, st.files[i].length)
		}
	}
	if !reflect.DeepEqual(loaded.VerifiedPieces, verifiedOut) {
		t.Fatalf("VerifiedPieces = %v, want %v", loaded.VerifiedPieces, verifiedOut)
	}
	if !reflect.DeepEqual(loaded.CompletedPieces, completed) {
		t.Fatalf("CompletedPieces = %v, want %v", loaded.CompletedPieces, completed)
	}
	// Verify byte-for-byte identity with encoding/json.Marshal(FastResumeState).
	reMarshaled, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, reMarshaled) {
		t.Fatalf("hint JSON differs from encoding/json:\ngot:  %s\nwant: %s", raw, reMarshaled)
	}
}

func TestSaveResumeHintEmptyFilesAndPieces(t *testing.T) {
	st, err := NewFileStorage(t.TempDir(), []FileInfo{{Path: "pad", Length: 4, Pad: true}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	const infoHash = "fedcba9876543210fedcba9876543210fedcba98"
	if err := st.SaveResumeState(infoHash, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(st.BaseDir(), "."+infoHash+".state"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := json.Marshal(FastResumeState{
		Version:      2,
		PieceLength:  4,
		IdentityMode: resumeIdentityMode,
		InfoHashHex:  infoHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, expected) {
		t.Fatalf("empty-files hint JSON mismatch:\ngot:  %s\nwant: %s", raw, expected)
	}
	got, err := st.LoadResumeState(infoHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Verified) != 0 || len(got.Recheck) != 0 {
		t.Fatalf("LoadResumeState = %+v, want empty", got)
	}
}

func TestSaveResumeHintConcurrentWritesAndCheckpoints(t *testing.T) {
	const numFiles = 200
	files := make([]FileInfo, numFiles)
	for i := range files {
		files[i] = FileInfo{Path: filepath.Join("dir", string(rune('a'+i%26))+"_"+string(rune('0'+i/26))), Length: 64}
	}
	st, err := NewFileStorage(t.TempDir(), files, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	const infoHash = "1111222233334444555566667777888899990000"
	if err := st.SaveResumeState(infoHash, []int{0, 1}, nil, true); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		buf := make([]byte, 128)
		for i := 0; i < 100; i++ {
			piece := int64(i % st.pieceCount())
			if err := st.WriteBlock(piece, 0, buf); err != nil {
				t.Errorf("WriteBlock: %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			if err := st.SaveResumeState(infoHash, []int{0}, []int{1, 2}, false); err != nil {
				t.Errorf("SaveResumeState(false): %v", err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := st.SaveResumeState(infoHash, []int{0, 1}, nil, true); err != nil {
				t.Errorf("SaveResumeState(true): %v", err)
				return
			}
		}
	}()
	wg.Wait()

	// Final hint after concurrent activity must remain valid JSON and load cleanly.
	if err := st.SaveResumeState(infoHash, []int{0}, []int{1, 2}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadResumeState(infoHash); err != nil {
		t.Fatalf("LoadResumeState after concurrent writes: %v", err)
	}
}

func TestSaveResumeHintLayoutInvalidation(t *testing.T) {
	root := t.TempDir()
	st, err := NewFileStorage(root, []FileInfo{{Path: "a.bin", Length: 8}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()

	const infoHash = "abcdef0123456789abcdef0123456789abcdef01"
	if err := st.SaveResumeState(infoHash, []int{0}, nil, false); err != nil {
		t.Fatal(err)
	}
	if !st.hintCache.matchesLayout(4, 1) {
		t.Fatal("expected hint cache to match initial layout")
	}
	if st.hintCache.matchesLayout(8, 1) || st.hintCache.matchesLayout(4, 2) {
		t.Fatal("expected hint cache to invalidate on piece length or file count change")
	}

	// Simulate layout change and verify cache rebuilds cleanly for the new layout.
	bPath := filepath.Join(root, "b.bin")
	if err := os.WriteFile(bPath, make([]byte, 8), 0600); err != nil {
		t.Fatal(err)
	}
	st.pieceLength = 8
	st.totalSize = 16
	st.files = append(st.files, &fileLayout{
		index:        1,
		path:         "b.bin",
		downloadRoot: st.downloadRoot,
		length:       8,
		startOffset:  8,
		endOffset:    16,
	})
	st.fileMtime = append(st.fileMtime, 0)
	st.syncDirty = append(st.syncDirty, false)

	if err := st.SaveResumeState(infoHash, []int{0}, nil, false); err != nil {
		t.Fatal(err)
	}
	assertHintMatchesMarshal(t, st, infoHash, nil, []int{0}, []bool{true, true})
}
