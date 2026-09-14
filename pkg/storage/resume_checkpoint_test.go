package storage

import (
	"encoding/json"
	"errors"
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

func TestCheckpointOmitsUnclaimedPiecesFromRecheck(t *testing.T) {
	st, hash := checkpointFixture(t)
	// Only the pieces inside "a" are claimed; "b" and "c" hold nothing yet.
	if err := st.SaveResumeState(hash, []int{0}, nil, true); err != nil {
		t.Fatal(err)
	}
	root := st.BaseDir()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "c")); err != nil {
		t.Fatal(err)
	}
	files := []FileInfo{{Path: "a", Length: 6}, {Path: "b", Length: 4}, {Path: "c", Length: 6}}
	next, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	got, err := next.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	// Recreating "c" must not make its never-downloaded pieces wait for a hash.
	want := ResumeState{Verified: []int{0}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestCheckpointSurvivesUnreadablePayloadFile(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := os.Remove(filepath.Join(st.BaseDir(), "c")); err != nil {
		t.Fatal(err)
	}
	// A payload that vanished at runtime must not fail the whole checkpoint, or the
	// caller retries a full flush sweep every second and reports a permanent error.
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatalf("checkpoint aborted on missing payload: %v", err)
	}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	// Pieces 2 and 3 overlap the missing file; piece 0 lies entirely inside "a".
	want := ResumeState{Verified: []int{0, 1}, Recheck: []int{2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestResumeHintPreservesDurableCheckpoint(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	// A hint written while a recheck is running (or at shutdown) must not replace
	// the checkpoint with a version-less guess that rehashes everything.
	if err := st.SaveResumeState(hash, []int{0, 1}, []int{2, 3}, false); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	want := ResumeState{Verified: []int{0, 1}, Recheck: []int{2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
	// A piece the recheck rejected drops out of the claim and stays out.
	if err = st.SaveResumeState(hash, []int{0}, []int{2, 3}, false); err != nil {
		t.Fatal(err)
	}
	if got, err = st.LoadResumeState(hash); err != nil {
		t.Fatal(err)
	}
	want = ResumeState{Verified: []int{0}, Recheck: []int{2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v; want %+v", got, want)
	}
}

func TestCheckpointSkipsRepeatedFlushOfCleanFiles(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(st.BaseDir(), "."+hash+".state")
	first, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// An unchanged checkpoint is not rewritten, so quitting a stack of idle
	// torrents costs no per-file sweep at all.
	if !os.SameFile(first, second) {
		t.Fatal("identical checkpoint was rewritten")
	}
	if err = st.WriteBlock(0, 0, []byte{5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	third, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(second, third) {
		t.Fatal("checkpoint not refreshed after a write")
	}
}

func TestCheckpointTrustsRepairedFileOnceRewritten(t *testing.T) {
	root := t.TempDir()
	files := []FileInfo{{Path: "a", Length: 4}, {Path: "b", Length: 4}}
	st, err := NewFileStorage(root, files, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	for i := int64(0); i < 2; i++ {
		if err = st.WriteBlock(i, 0, []byte{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
	}
	// Drop the cached handle so the next write reopens (and recreates) the file.
	st.files[1].invalidateWriter()
	if err = os.Remove(filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	if err = st.WriteBlock(1, 0, []byte{1, 2, 3, 4}); !errors.Is(err, ErrFileRepaired) {
		t.Fatalf("WriteBlock = %v, want ErrFileRepaired", err)
	}
	// The checkpoint taken right after a repair distrusts the recreated file.
	if err = st.SaveResumeState("repair", []int{0, 1}, nil, true); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadResumeState("repair")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ResumeState{Verified: []int{0}, Recheck: []int{1}}) {
		t.Fatalf("repair was blessed immediately: %+v", got)
	}
	// A session that keeps running re-anchors the file instead of rehashing it on
	// every launch for the rest of its life.
	if err = st.SaveResumeState("repair", []int{0, 1}, nil, true); err != nil {
		t.Fatal(err)
	}
	if got, err = st.LoadResumeState("repair"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, ResumeState{Verified: []int{0, 1}}) {
		t.Fatalf("repaired file never regained trust: %+v", got)
	}
}

// Every payload must be checkpointed with an identity. Reading it after the file
// was closed leaves it empty on the platforms that take the change timestamp from
// the handle, which silently turns fast resume into a full rehash there.
func TestCheckpointRecordsIdentityForEveryFile(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(st.BaseDir(), "."+hash+".state"))
	if err != nil {
		t.Fatal(err)
	}
	var state FastResumeState
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if state.Version != 2 || len(state.Identities) != 3 {
		t.Fatalf("checkpoint layout: %+v", state)
	}
	for i, identity := range state.Identities {
		if identity == "" {
			t.Fatalf("file %d was checkpointed without an identity: %+v", i, state)
		}
	}
}

// A file the checkpoint distrusted must not become the baseline the next one
// compares against, or the second checkpoint of an ordinary pause-then-quit
// declares the edited file unchanged and restores its pieces as verified.
func TestCheckpointNeverBlessesAnEditItAlreadyDistrusted(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(st.BaseDir(), "b")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte{9, 9, 9, 9}, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		// Writing to another file keeps the checkpoint from reusing the previous one,
		// so every attempt re-examines the edited file.
		if err = st.WriteBlock(0, 0, []byte{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
			t.Fatal(err)
		}
		got, err := st.LoadResumeState(hash)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, ResumeState{Verified: []int{0, 3}, Recheck: []int{1, 2}}) {
			t.Fatalf("edit was blessed on attempt %d: %+v", attempt, got)
		}
	}
}

// A checkpoint restored from disk with every file intact is as current as one this
// process wrote, so pausing or quitting an untouched torrent costs no sweep.
func TestRestoredCheckpointSkipsRedundantFlush(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	root := st.BaseDir()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := NewFileStorage(root, []FileInfo{{Path: "a", Length: 6}, {Path: "b", Length: 4}, {Path: "c", Length: 6}}, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	if _, err = next.LoadResumeState(hash); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "."+hash+".state")
	first, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	second, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(first, second) {
		t.Fatal("a restored checkpoint nothing had touched was rewritten")
	}
}
