package storage

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRemoveFilesRefusesJunctions: Go reports a directory junction as
// irregular, not as a symlink, so the old Lstat check let one through and
// DeleteFileW followed it out of the download directory. Creating a junction
// needs no privilege, so any local user with write access could plant one.
func TestRemoveFilesRefusesJunctions(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "dl")
	writeTree(t, parent, "outside/victim", "dl/keep")
	link := filepath.Join(base, "sub")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", link, filepath.Join(parent, "outside")).CombinedOutput(); err != nil {
		t.Skipf("mklink /J failed: %v: %s", err, out)
	}
	if _, err := ResolveAndValidatePath(base, filepath.Join("sub", "victim")); err == nil {
		t.Fatal("ResolveAndValidatePath accepted a path through a junction")
	}
	removed, err := RemoveFiles(base, []string{filepath.Join("sub", "victim")})
	if removed != 0 || err == nil {
		t.Fatalf("RemoveFiles through a junction = %d, %v; want a refusal", removed, err)
	}
	requireExists(t, parent, "outside/victim", "dl/sub", "dl/keep")
}
