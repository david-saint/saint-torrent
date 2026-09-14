package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func checkpointFixture(t *testing.T) (*FileStorage, string) {
	t.Helper()
	root := t.TempDir()
	// Piece 1 spans a and b; piece 2 spans b and c.
	st, err := NewFileStorage(root, []FileInfo{{Path: "a", Length: 6}, {Path: "b", Length: 4}, {Path: "c", Length: 6}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for i := int64(0); i < 4; i++ {
		if err := st.WriteBlock(i, 0, []byte{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
	}
	return st, "checkpoint"
}

func TestDurableResumeAndSelectiveFileChanges(t *testing.T) {
	for _, change := range []string{"unchanged", "modified", "replaced", "resized", "missing"} {
		t.Run(change, func(t *testing.T) {
			st, hash := checkpointFixture(t)
			if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
				t.Fatal(err)
			}
			root := st.BaseDir()
			// Close cached payload handles before simulating edits between launches.
			// Windows correctly refuses removal while those handles are still open.
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "b")
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "modified":
				if err = os.WriteFile(path, []byte{9, 9, 9, 9}, 0600); err != nil {
					t.Fatal(err)
				}
				future := info.ModTime().Add(time.Second)
				if err = os.Chtimes(path, future, future); err != nil {
					t.Fatal(err)
				}
			case "replaced":
				// Retain the old inode so the filesystem cannot reuse its identity.
				if err = os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, []byte{9, 9, 9, 9}, 0600); err != nil {
					t.Fatal(err)
				}
				if err = os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
					t.Fatal(err)
				}
			case "resized":
				if err = os.Truncate(path, 1); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			st, err = NewFileStorage(root, []FileInfo{{Path: "a", Length: 6}, {Path: "b", Length: 4}, {Path: "c", Length: 6}}, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			result, err := st.LoadResumeState(hash)
			if err != nil {
				t.Fatal(err)
			}
			want := ResumeState{Verified: []int{0, 1, 2, 3}}
			if change != "unchanged" {
				want = ResumeState{Verified: []int{0, 3}, Recheck: []int{1, 2}}
			}
			if !reflect.DeepEqual(result, want) {
				t.Fatalf("got %+v; want %+v", result, want)
			}
		})
	}
}

func TestResumeNeverPromotesUnverifiedOrLegacyHints(t *testing.T) {
	for _, mode := range []string{"legacy", "active", "durable"} {
		t.Run(mode, func(t *testing.T) {
			st, hash := checkpointFixture(t)
			var err error
			if mode == "legacy" {
				err = st.SaveState(hash, []int{0, 1, 2, 3})
			} else {
				err = st.SaveResumeState(hash, []int{0, 3}, []int{1, 2}, mode == "durable")
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := st.LoadResumeState(hash)
			if err != nil {
				t.Fatal(err)
			}
			want := ResumeState{Recheck: []int{0, 1, 2, 3}}
			if mode == "durable" {
				want = ResumeState{Verified: []int{0, 3}, Recheck: []int{1, 2}}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %+v; want %+v", got, want)
			}
		})
	}
}

func TestResumeRejectsMalformedCheckpoint(t *testing.T) {
	for _, corruption := range []string{"truncated", "piece length", "duplicate", "out of range", "unclaimed verified", "identity count"} {
		t.Run(corruption, func(t *testing.T) {
			st, hash := checkpointFixture(t)
			if err := st.SaveResumeState(hash, []int{0}, nil, true); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(st.BaseDir(), "."+hash+".state")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var state FastResumeState
			if err = json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "piece length":
				state.PieceLength++
			case "duplicate":
				state.CompletedPieces = []int{0, 0}
			case "out of range":
				state.CompletedPieces = []int{99}
			case "unclaimed verified":
				state.VerifiedPieces = []int{3}
			case "identity count":
				state.Identities = nil
			}
			data, err = json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			if corruption == "truncated" {
				data = data[:len(data)/2]
			}
			if err = os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = st.LoadResumeState(hash); err == nil {
				t.Fatal("accepted malformed checkpoint")
			}
		})
	}
}

func TestAtomicResumeReplacement(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0}, nil, true); err != nil {
		t.Fatal(err)
	}
	// A leftover incomplete temporary file from an interrupted save is ignored.
	if err := os.WriteFile(filepath.Join(st.BaseDir(), "."+hash+".state.interrupted.tmp"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := st.LoadResumeState(hash); err != nil || !reflect.DeepEqual(got.Verified, []int{0}) {
		t.Fatalf("old checkpoint lost: %+v %v", got, err)
	}
	// Replacing an existing file works on all supported OSes.
	if err := st.SaveResumeState(hash, []int{0, 1}, nil, true); err != nil {
		t.Fatal(err)
	}
	if got, err := st.LoadResumeState(hash); err != nil || !reflect.DeepEqual(got.Verified, []int{0, 1}) {
		t.Fatalf("new checkpoint invalid: %+v %v", got, err)
	}
}
