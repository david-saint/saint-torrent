//go:build linux

package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

// TestRemoveFilesSurvivesDirectorySwapRace: another local user who can write to
// a shared download directory keeps swapping a torrent directory with a symlink
// to a directory outside it. Removing by path after a separate Lstat walk
// followed the swap and deleted the outside file in a fraction of attempts.
// Every lookup is now relative to a verified directory handle, so it never can.
func TestRemoveFilesSurvivesDirectorySwapRace(t *testing.T) {
	parent := t.TempDir()
	base := filepath.Join(parent, "dl")
	outside := filepath.Join(parent, "outside")
	victim := filepath.Join(outside, "victim")
	for _, dir := range []string{filepath.Join(base, "sub"), outside} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(base, "subalt")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Renameat2(unix.AT_FDCWD, filepath.Join(base, "sub"), unix.AT_FDCWD, filepath.Join(base, "subalt"), unix.RENAME_EXCHANGE); err != nil {
		t.Skipf("RENAME_EXCHANGE unsupported here: %v", err)
	}

	stop := make(chan struct{})
	var swapper sync.WaitGroup
	swapper.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = unix.Renameat2(unix.AT_FDCWD, filepath.Join(base, "sub"), unix.AT_FDCWD, filepath.Join(base, "subalt"), unix.RENAME_EXCHANGE)
			}
		}
	})
	defer func() {
		close(stop)
		swapper.Wait()
	}()

	for i := range 500 {
		if err := os.WriteFile(victim, []byte("outside"), 0644); err != nil {
			t.Fatal(err)
		}
		// Whichever name the real directory has right now, give it a victim too.
		_ = os.WriteFile(filepath.Join(base, "sub", "victim"), []byte("inside"), 0644)
		_ = os.WriteFile(filepath.Join(base, "subalt", "victim"), []byte("inside"), 0644)
		_, _ = RemoveFiles(base, []string{filepath.Join("sub", "victim")})
		if _, err := os.Stat(victim); err != nil {
			t.Fatalf("iteration %d: the file outside the download directory was deleted: %v", i, err)
		}
	}
}
