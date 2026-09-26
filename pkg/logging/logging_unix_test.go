//go:build !windows

package logging

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const victimContent = "export PATH=/usr/bin\n"

// newVictim creates the file an attacker wants appended to (a shell rc file
// in the real attack) and returns its path.
func newVictim(t *testing.T, dir string) string {
	t.Helper()
	victim := filepath.Join(dir, "victim_zshenv")
	if err := os.WriteFile(victim, []byte(victimContent), 0644); err != nil {
		t.Fatal(err)
	}
	return victim
}

func assertVictimUntouched(t *testing.T, victim string) {
	t.Helper()
	data, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != victimContent {
		t.Fatalf("victim file was written through the log path: %q", data)
	}
	info, err := os.Stat(victim)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0644 {
		t.Fatalf("victim file mode changed to %o", got)
	}
}

// Another local user pre-creates the predictable log path as a symlink to the
// victim's file; New must refuse it instead of appending and chmodding.
func TestNewRefusesSymlinkedLogPath(t *testing.T) {
	dir := t.TempDir()
	victim := newVictim(t, dir)
	link := filepath.Join(dir, "sainttorrent-debug.log")
	if err := os.Symlink(victim, link); err != nil {
		t.Fatal(err)
	}

	if logger, err := New(Config{Path: link}); err == nil {
		_ = logger.Log(LevelInfo, "session_added", String("name", "$(touch pwned)"))
		logger.Close()
		t.Fatal("New followed a symlinked log path")
	}
	assertVictimUntouched(t, victim)
}

// A refused log path says why and what to use instead: the bare "too many
// levels of symbolic links" of a no-follow open did not.
func TestRefusedLogPathErrorIsActionable(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "debug.log")
	if err := os.Symlink(filepath.Join(dir, "target.log"), link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo.log")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	for path, why := range map[string]string{link: "symbolic link", fifo: "not a regular file"} {
		logger, err := New(Config{Path: path})
		if err == nil {
			logger.Close()
			t.Fatalf("New accepted %s", path)
		}
		if msg := err.Error(); !strings.Contains(msg, why) || !strings.Contains(msg, "/dev/stderr") {
			t.Fatalf("New(%s) error %q, want it to say %q and suggest /dev/stderr", path, msg, why)
		}
	}
}

// A dangling link would otherwise make us create the attacker-chosen target.
func TestNewRefusesDanglingSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "not-yet-created-rc")
	link := filepath.Join(dir, "sainttorrent-debug.log")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if logger, err := New(Config{Path: link}); err == nil {
		logger.Close()
		t.Fatal("New followed a dangling symlink")
	}
	if _, err := os.Lstat(target); !os.IsNotExist(err) {
		t.Fatalf("symlink target was created: %v", err)
	}
}

// O_NOFOLLOW cannot see a hard link; the link count check must.
func TestNewRefusesHardLinkedLogPath(t *testing.T) {
	dir := t.TempDir()
	victim := newVictim(t, dir)
	link := filepath.Join(dir, "sainttorrent-debug.log")
	if err := os.Link(victim, link); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if logger, err := New(Config{Path: link}); err == nil {
		logger.Close()
		t.Fatal("New accepted a hard-linked log path")
	}
	assertVictimUntouched(t, victim)
}

// A planted FIFO must be refused promptly rather than blocking startup in open.
func TestNewRefusesFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sainttorrent-debug.log")
	if err := syscall.Mkfifo(path, 0666); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		logger, err := New(Config{Path: path})
		if err == nil {
			logger.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("New accepted a FIFO log path")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("New blocked opening a FIFO log path")
	}
}

// A regular file pre-created by another user must not be adopted either.
func TestNewRefusesFileOwnedByAnotherUser(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sainttorrent-debug.log")
	if err := os.WriteFile(path, nil, 0666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, os.Getuid()+1, -1); err != nil {
		t.Skipf("cannot chown to another uid (not root): %v", err)
	}
	logger, err := New(Config{Path: path})
	if err == nil {
		logger.Close()
		t.Fatal("New accepted a log file owned by another user")
	}
	if !strings.Contains(err.Error(), "not the current user") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewCreatesPrivateParentDirectories(t *testing.T) {
	base := t.TempDir()
	path := filepath.Join(base, "logs", "sainttorrent", "debug.log")
	logger, err := New(Config{Path: path})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	logger.Close()
	for _, dir := range []string{filepath.Join(base, "logs"), filepath.Dir(path)} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got&0077 != 0 {
			t.Fatalf("%s mode = %o; want no group/other access", dir, got)
		}
	}
}

// A link swapped in at the live log path is renamed aside by rotation, never
// followed: the victim stays untouched and logging continues in a fresh file.
func TestRotationDoesNotFollowPlantedSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := newVictim(t, dir)
	path := filepath.Join(dir, "debug.log")
	logger, err := New(Config{Path: path, MaxSizeBytes: 200, MaxBackups: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer logger.Close()
	if err := logger.Log(LevelInfo, "first", String("payload", strings.Repeat("x", 120))); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, path); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if err := logger.Log(LevelInfo, "rotate", String("payload", strings.Repeat("y", 120))); err != nil {
			t.Fatalf("Log %d: %v", i, err)
		}
	}
	assertVictimUntouched(t, victim)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("current log is %v; want a regular file", info.Mode())
	}
}

func TestOpenPrivateFileTightensExistingMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "timing.log")
	if err := os.WriteFile(path, []byte("old\n"), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := OpenPrivateFile(path)
	if err != nil {
		t.Fatalf("OpenPrivateFile: %v", err)
	}
	if _, err := f.WriteString("new\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "old\nnew\n" {
		t.Fatalf("content = %q; want appended", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != logFileMode {
		t.Fatalf("mode = %o; want %o", got, logFileMode)
	}
}
