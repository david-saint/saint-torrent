package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// referenceHint is the hint the pre-cache implementation wrote: the whole file
// table rebuilt under mtMu and marshaled by encoding/json. The cached writer must
// produce exactly these bytes, so a state file written by either side loads on the
// other with identical results.
func referenceHint(s *FileStorage, infoHash string, verified, completed []int) []byte {
	state := FastResumeState{IdentityMode: resumeIdentityMode, Version: 2, PieceLength: s.pieceLength, InfoHashHex: infoHash, CompletedPieces: completed}
	trusted := make([]bool, s.pieceCount())
	for _, idx := range verified {
		if idx >= 0 && idx < len(trusted) {
			trusted[idx] = true
		}
	}
	s.mtMu.Lock()
	s.refreshDirtyLocked()
	checkpoint := s.checkpoint
	for i, file := range s.files {
		identity := ""
		mtime := s.fileMtime[i]
		if checkpoint.valid && s.dirty[file]&dirtySync == 0 {
			identity = checkpoint.identities[i]
			mtime = checkpoint.mtimes[i]
		}
		state.Identities = append(state.Identities, identity)
		state.Files = append(state.Files, resumeFileEntry{file.path, file.length, mtime})
	}
	s.mtMu.Unlock()
	for _, idx := range checkpoint.verified {
		if idx >= 0 && idx < len(trusted) && trusted[idx] {
			state.VerifiedPieces = append(state.VerifiedPieces, idx)
		}
	}
	data, err := json.Marshal(state)
	if err != nil {
		panic(err)
	}
	return data
}

func readHint(t *testing.T, s *FileStorage, hash string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(s.BaseDir(), "."+hash+".state"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// requireHintMatchesReference writes a hint and checks it byte for byte against the
// reference serialization of the same storage state. Nothing may write between the
// two, so the dirty set the reference sees is the one the hint saw.
func requireHintMatchesReference(t *testing.T, s *FileStorage, hash string, verified, unverified []int) {
	t.Helper()
	if err := s.SaveResumeState(hash, verified, unverified, false); err != nil {
		t.Fatal(err)
	}
	completed := append(append([]int(nil), verified...), unverified...)
	got := readHint(t, s, hash)
	want := referenceHint(s, hash, verified, completed)
	if !bytes.Equal(got, want) {
		t.Fatalf("hint differs from the reference serialization\n got: %.400s\nwant: %.400s", got, want)
	}
}

func TestResumeHintMatchesReferenceThroughTheLifecycle(t *testing.T) {
	files := []FileInfo{{Path: "a", Length: 6}, {Path: "dir/b & 'q' é", Length: 4}, {Path: "empty", Length: 0}, {Path: "c", Length: 6}}
	st, err := NewFileStorage(t.TempDir(), files, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	const hash = "hintreference"

	// Nothing written, no checkpoint, no pieces at all.
	requireHintMatchesReference(t, st, hash, nil, nil)
	for i := int64(0); i < 4; i++ {
		if err := st.WriteBlock(i, 0, []byte{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
	}
	requireHintMatchesReference(t, st, hash, []int{0, 1}, []int{2, 3})
	// A durable checkpoint, then a hint that replays it untouched.
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, st, hash, []int{0, 1, 2, 3}, nil)
	// Only the pieces the caller still holds verified keep their durable status.
	requireHintMatchesReference(t, st, hash, []int{0, 3}, []int{1, 2})
	// A file written since the checkpoint loses its identity; the others keep it.
	time.Sleep(15 * time.Millisecond)
	if err := st.WriteBlock(2, 0, []byte{9, 9, 9, 9}); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, st, hash, []int{0, 1, 2, 3}, nil)
	requireHintMatchesReference(t, st, hash, []int{0, 1, 2, 3}, nil)
	// A new durable checkpoint re-anchors everything, including the mtimes the
	// hint mirrors.
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, st, hash, []int{0, 1, 2, 3}, nil)
	time.Sleep(15 * time.Millisecond)
	if err := st.WriteBlock(0, 0, []byte{7, 7, 7, 7}); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, st, hash, []int{1, 2, 3}, []int{0})
}

func TestResumeHintFollowsStateLoadedFromDisk(t *testing.T) {
	root := t.TempDir()
	files := []FileInfo{{Path: "a", Length: 6}, {Path: "b", Length: 4}}
	first, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 3; i++ {
		block := []byte{1, 2, 3, 4}
		if i == 2 {
			block = block[:2] // the last piece is short
		}
		if err := first.WriteBlock(i, 0, block); err != nil {
			t.Fatal(err)
		}
	}
	const hash = "hintreload"
	if err := first.SaveResumeState(hash, []int{0, 1, 2}, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if _, err := second.LoadResumeState(hash); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, second, hash, []int{0, 1, 2}, nil)
	// The legacy loader path rewrites the mtimes the hint reports.
	if _, err := second.LoadState(hash); err != nil {
		t.Fatal(err)
	}
	requireHintMatchesReference(t, second, hash, []int{0, 1}, []int{2})
}

func TestResumeHintWithoutFiles(t *testing.T) {
	st, err := NewFileStorage(t.TempDir(), nil, 4)
	if err != nil {
		t.Skipf("storage with no files is not constructible: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	requireHintMatchesReference(t, st, "hintempty", nil, nil)
	requireHintMatchesReference(t, st, "hintempty", []int{}, []int{})
}

// TestResumeHintNeverBlessesAWrittenFile pins the trust rules the cache must keep:
// a file written since the last durable checkpoint reports no identity, and a
// piece the caller no longer holds verified is not replayed as verified.
func TestResumeHintNeverBlessesAWrittenFile(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if err := st.WriteBlock(3, 0, []byte{5, 5, 5, 5}); err != nil { // lands in file c
		t.Fatal(err)
	}
	if err := st.SaveResumeState(hash, []int{0, 1, 3}, []int{2}, false); err != nil {
		t.Fatal(err)
	}
	var state FastResumeState
	if err := json.Unmarshal(readHint(t, st, hash), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Identities) != 3 || state.Identities[0] == "" || state.Identities[1] == "" || state.Identities[2] != "" {
		t.Fatalf("identities = %q, want the written file c to lose its identity only", state.Identities)
	}
	for _, idx := range state.VerifiedPieces {
		if idx == 2 {
			t.Fatalf("verified = %v still blesses piece 2 the caller holds unverified", state.VerifiedPieces)
		}
	}
}

func TestResumeHintConcurrentWithWrites(t *testing.T) {
	files := make([]FileInfo, 24)
	for i := range files {
		files[i] = FileInfo{Path: fmt.Sprintf("f%02d", i), Length: 4}
	}
	st, err := NewFileStorage(t.TempDir(), files, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	const hash = "hintconcurrent"
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := int64(0); ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := st.WriteBlock(i%24, 0, []byte{1, 2, 3, 4}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 50; i++ {
		if err := st.SaveResumeState(hash, nil, []int{0, 1}, false); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	<-done
	requireHintMatchesReference(t, st, hash, nil, []int{0, 1})
}

// TestAppendJSONStringMatchesEncodingJSON covers the characters a payload path
// cannot carry on every platform, so the escaping is checked without creating
// files named after them.
func TestAppendJSONStringMatchesEncodingJSON(t *testing.T) {
	for _, s := range []string{"", "plain", `quote"and\slash`, "<tag>&amp", "tab\tnewline\n", "caf\u00e9 \u65e5\u672c", "sep\u2028\u2029", "bad\xffutf8", "del\x7f", "16777232:20000000:1700000000000000000"} {
		want, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		if got := appendJSONString(nil, s); !bytes.Equal(got, want) {
			t.Errorf("appendJSONString(%q) = %s, want %s", s, got, want)
		}
	}
}
