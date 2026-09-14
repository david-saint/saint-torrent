//go:build !windows

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointDoesNotBlessExternalEditWithRestoredMtime(t *testing.T) {
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
	for attempt := 0; attempt < 2; attempt++ {
		if err = st.SaveResumeState(hash, []int{0, 1, 2, 3}, nil, true); err != nil {
			t.Fatal(err)
		}
		got, err := st.LoadResumeState(hash)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Recheck) != 2 || len(got.Verified) != 2 {
			t.Fatalf("edit was blessed on attempt %d: %+v", attempt, got)
		}
	}
}
