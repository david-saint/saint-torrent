//go:build !windows

package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
)

// Opening a file on macOS records com.apple.lastuseddate#PS as an extended
// attribute, which moves the change timestamp but not the contents or the
// modification time. A metadata-only change like that, or a chmod, must not
// make the next launch rehash the file: watching a downloaded video used to.
func TestMetadataOnlyChangeKeepsCheckpoint(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(st.BaseDir(), "b")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	setLastUsedXattr(t, path)
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("metadata change moved the modification time: %v -> %v", before.ModTime(), after.ModTime())
	}
	want := ResumeState{Verified: []int{0, 1, 2, 3}}
	got, err := st.LoadResumeState(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata-only change forced a recheck: %+v", got)
	}
	// A checkpoint taken while the client keeps running must still trust it too.
	if err = st.WriteBlock(0, 0, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	if got, err = st.LoadResumeState(hash); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("checkpoint after a metadata-only change = %+v, %v; want %+v", got, err, want)
	}
}

// Checkpoints written while the identity carried the change timestamp are honoured
// by device and inode, so upgrading costs no recheck, and a hint written afterwards
// carries them forward under the current mode.
func TestLegacyChangeTimeCheckpointStillRestores(t *testing.T) {
	st, hash := checkpointFixture(t)
	if err := st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(st.BaseDir(), "."+hash+".state")
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	var state FastResumeState
	if err = json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	state.IdentityMode = "change_time"
	for i, name := range []string{"a", "b", "c"} {
		fi, err := os.Stat(filepath.Join(st.BaseDir(), name))
		if err != nil {
			t.Fatal(err)
		}
		sys := fi.Sys().(*syscall.Stat_t)
		ino := uint64(sys.Ino)
		if name == "c" {
			ino++ // a different file now sits at this path
		}
		state.Identities[i] = fmt.Sprintf("%d:%d:1700000000.000000001", sys.Dev, ino)
	}
	if data, err = json.Marshal(state); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(statePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	// Piece 2 spans b and c, piece 3 lies in c alone.
	want := ResumeState{Verified: []int{0, 1}, Recheck: []int{2, 3}}
	got, err := st.LoadResumeState(hash)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("legacy checkpoint = %+v, %v; want %+v", got, err, want)
	}
	if err = st.SaveResumeState(hash, []int{0, 1}, []int{2, 3}, false); err != nil {
		t.Fatal(err)
	}
	if got, err = st.LoadResumeState(hash); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("hint after a legacy checkpoint = %+v, %v; want %+v", got, err, want)
	}
	if data, err = os.ReadFile(statePath); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &state); err != nil || state.IdentityMode != resumeIdentityMode {
		t.Fatalf("hint identity mode = %q, %v; want %q", state.IdentityMode, err, resumeIdentityMode)
	}
}
