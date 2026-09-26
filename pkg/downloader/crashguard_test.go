package downloader

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"sainttorrent/pkg/peer"
)

// crashGuardTestSession is a Session carrying just what crashGuard reads.
func crashGuardTestSession(infoHashHex string, rec *crashRecorder) *Session {
	s := &Session{infoHashHex: infoHashHex}
	s.crash.Store(rec)
	return s
}

// catchPanic runs fn and returns the value it panicked with, if any.
func catchPanic(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

// crashFilesIn lists the crash files in dir.
func crashFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if _, _, _, ok := parseCrashFileName(e.Name()); ok {
			names = append(names, e.Name())
		}
	}
	return names
}

// runCrashGuarded runs fn on its own goroutine with crashTerminate replaced
// by a stub that ends just that goroutine, and returns the panic value of each
// call the stub got. A real guard ends the process there, so no deferred call
// registered before it runs; runtime.Goexit keeps that for the rest of the
// goroutine's guards, which see no panic left to recover.
func runCrashGuarded(t *testing.T, fn func()) (terminated []any) {
	t.Helper()
	var mu sync.Mutex
	prev := crashTerminate
	crashTerminate = func(r any) {
		mu.Lock()
		terminated = append(terminated, r)
		mu.Unlock()
		runtime.Goexit()
	}
	defer func() { crashTerminate = prev }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return terminated
}

// crashGuardedPanic is a named frame the recorded stack must show.
func crashGuardedPanic(s *Session) {
	defer s.crashGuard("peer_loop")()
	var table []int
	_ = table[len(table)+2] // index out of range
}

// TestCrashGuardRecordsAndTerminates: the guard writes one private crash file
// named after the torrent and the component, holding the panic and the stack
// of the panicking goroutine, and then ends the process with the same value
// instead of letting the goroutine carry on.
func TestCrashGuardRecordsAndTerminates(t *testing.T) {
	dir := t.TempDir()
	hash := strings.Repeat("ab", 20)
	s := crashGuardTestSession(hash, &crashRecorder{dir: dir})

	got := runCrashGuarded(t, func() { crashGuardedPanic(s) })
	if len(got) != 1 {
		t.Fatalf("terminated %d times, want once", len(got))
	}
	rtErr, ok := got[0].(runtime.Error)
	if !ok || !strings.Contains(rtErr.Error(), "index out of range") {
		t.Fatalf("terminated with %v (%T), want the original runtime error", got[0], got[0])
	}

	files := crashFilesIn(t, dir)
	if len(files) != 1 {
		t.Fatalf("crash files = %v, want exactly one", files)
	}
	_, gotHash, component, _ := parseCrashFileName(files[0])
	if gotHash != hash || component != "peer_loop" {
		t.Fatalf("crash file %s: hash %q component %q, want %s and peer_loop", files[0], gotHash, component, hash)
	}
	path := filepath.Join(dir, files[0])
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Errorf("crash file mode %v, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"component: peer_loop", "info_hash: " + hash, "index out of range", "crashGuardedPanic", "time: "} {
		if !strings.Contains(string(data), want) {
			t.Errorf("crash file lacks %q:\n%s", want, data)
		}
	}
}

// TestCrashGuardWithoutRecorderTerminates: with persistence off there is
// nowhere to record, and the process still ends.
func TestCrashGuardWithoutRecorderTerminates(t *testing.T) {
	s := crashGuardTestSession(strings.Repeat("cd", 20), nil)
	got := runCrashGuarded(t, func() {
		defer s.crashGuard("choke")()
		panic("no recorder")
	})
	if len(got) != 1 || got[0] != "no recorder" {
		t.Fatalf("terminated with %v, want the original value once", got)
	}
	if got := s.crash.Load().record("choke", "", "x", nil); got != "" {
		t.Fatalf("nil recorder wrote %q", got)
	}
}

// TestNestedCrashGuardsRecordOnce: a peer loop runs inside a dial and an
// inbound handler, each guarded. The innermost guard sees the panic first,
// records it under its own component and ends the process, so the outer
// guards, and the deferred calls registered before it, never run for it.
func TestNestedCrashGuardsRecordOnce(t *testing.T) {
	dir := t.TempDir()
	s := crashGuardTestSession(strings.Repeat("ef", 20), &crashRecorder{dir: dir})
	cleanupRan := false
	got := runCrashGuarded(t, func() {
		defer s.crashGuard("peer_inbound")()
		func() {
			defer s.crashGuard("peer_dial")()
			func() {
				defer s.crashGuard("peer_loop")()
				defer func() { cleanupRan = recover() != nil }()
				func() {
					defer s.crashGuard("peer_reader")()
					panic("nested")
				}()
			}()
		}()
	})
	if len(got) != 1 || got[0] != "nested" {
		t.Fatalf("terminated with %v, want the original value once", got)
	}
	if cleanupRan {
		t.Fatal("a deferred call registered before the guard ran with the panic")
	}
	files := crashFilesIn(t, dir)
	if len(files) != 1 {
		t.Fatalf("crash files = %v, want exactly one", files)
	}
	if _, _, component, _ := parseCrashFileName(files[0]); component != "peer_reader" {
		t.Fatalf("recorded component %q, want the innermost guard's", component)
	}

	// A later, separate panic is recorded again.
	_ = runCrashGuarded(t, func() {
		defer s.crashGuard("choke")()
		panic("second")
	})
	if files := crashFilesIn(t, dir); len(files) != 2 {
		t.Fatalf("crash files after a second panic = %v, want two", files)
	}
}

// crashUnderLockChildEnv marks the child process of
// TestPeerLoopPanicUnderLockEndsProcess and names its crash directory.
const crashUnderLockChildEnv = "SAINTTORRENT_CRASH_UNDER_LOCK_CHILD"

// TestPeerLoopPanicUnderLockEndsProcess: a panic in the peer loop while it
// holds s.mu must end the process. Cleanup deferred earlier in the loop takes
// s.mu, and sync.RWMutex is not reentrant: were the panic to unwind into it,
// the goroutine would block there for good, with s.mu held, and the whole
// client would hang instead of crashing. The scenario runs in a child process,
// which must die promptly with the original panic and a crash file.
func TestPeerLoopPanicUnderLockEndsProcess(t *testing.T) {
	if dir := os.Getenv(crashUnderLockChildEnv); dir != "" {
		sess, _ := newSeedingWireTestSession(t, 4, 4*BlockSize)
		sess.mu.Lock()
		sess.started = true
		sess.mu.Unlock()
		sess.crash.Store(&crashRecorder{dir: dir})
		// Called with s.mu write-locked by the Interested unchoke scan.
		interestScanHook = func() { panic("crash-under-lock-probe") }
		w := startWirePeer(t, sess, 6881, fastReserved())
		w.send(&peer.Message{ID: peer.MsgInterested})
		time.Sleep(30 * time.Second)
		os.Exit(0) // still alive: the parent reports the hang
	}
	if testing.Short() {
		t.Skip("runs a child process")
	}

	crashDir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPeerLoopPanicUnderLockEndsProcess$")
	cmd.Env = append(os.Environ(), crashUnderLockChildEnv+"="+crashDir)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("child still running 20s after the panic: it hung instead of crashing\n%s", out.String())
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
		t.Fatalf("child exited with %v, want a crash\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "panic: crash-under-lock-probe") {
		t.Fatalf("child output lacks the original panic:\n%s", out.String())
	}
	files := crashFilesIn(t, crashDir)
	if len(files) != 1 {
		t.Fatalf("crash files = %v, want exactly one", files)
	}
	if _, _, component, _ := parseCrashFileName(files[0]); component != "peer_loop" {
		t.Fatalf("recorded component %q, want peer_loop", component)
	}
}

// panickyStringer panics when formatted.
type panickyStringer struct{}

func (panickyStringer) String() string { panic("String exploded") }

// TestCrashRecorderNeverPanics: recording runs while the process is dying, so
// an unwritable directory or a panic value that panics when printed must not
// raise a second panic from the recorder.
func TestCrashRecorderNeverPanics(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(notADir, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []*crashRecorder{
		{dir: filepath.Join(notADir, "crash")},
		{dir: filepath.Join(t.TempDir(), "missing", "crash")},
		{},
	} {
		if got := catchPanic(func() {
			if path := rec.record("verify", strings.Repeat("12", 20), panickyStringer{}, []byte("stack")); path != "" {
				t.Errorf("record into %q wrote %s", rec.dir, path)
			}
		}); got != nil {
			t.Fatalf("record into %q panicked: %v", rec.dir, got)
		}
	}

	// A writable directory still takes the crash, whatever the value does.
	dir := t.TempDir()
	path := (&crashRecorder{dir: dir}).record("verify", "", panickyStringer{}, nil)
	if path == "" {
		t.Fatal("record wrote nothing")
	}
	if _, hash, _, ok := parseCrashFileName(filepath.Base(path)); !ok || hash != "" {
		t.Fatalf("record without an info-hash wrote %s, want an unattributed crash", path)
	}
}

// TestCrashRecorderSanitizesNames: the component and info-hash become part of
// a file name, so anything but [a-z_]+ and 40 hex digits is replaced.
func TestCrashRecorderSanitizesNames(t *testing.T) {
	dir := t.TempDir()
	rec := &crashRecorder{dir: dir}
	path := rec.record("../../evil", "../../"+strings.Repeat("a", 34), "v", nil)
	if filepath.Dir(path) != dir {
		t.Fatalf("crash file %s escaped %s", path, dir)
	}
	_, hash, component, ok := parseCrashFileName(filepath.Base(path))
	if !ok || hash != "" || component != "unknown" {
		t.Fatalf("crash file %s: hash %q component %q, want unattributed and unknown", path, hash, component)
	}
	upper := strings.Repeat("AB", 20)
	path = rec.record("tui", upper, "v", nil)
	if _, hash, _, _ := parseCrashFileName(filepath.Base(path)); hash != strings.ToLower(upper) {
		t.Fatalf("crash file %s: hash %q, want the lower-case info-hash", path, hash)
	}
}

// TestOpenCrashLogPrunesAndRotates: startup keeps the newest maxCrashFiles
// crash files, leaves other files alone, rotates an oversized fatal.txt and
// opens a private one for appending.
func TestOpenCrashLogPrunesAndRotates(t *testing.T) {
	stateDir := t.TempDir()
	dir := CrashDir(stateDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UnixNano()
	for i := 0; i < maxCrashFiles+5; i++ {
		name := filepath.Join(dir, strings.Join([]string{strconv.FormatInt(base+int64(i), 10), unattributedCrash, "tui"}, "-")+".txt")
		if err := os.WriteFile(name, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	keepMe := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(keepMe, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	fatal := filepath.Join(dir, fatalLogName)
	if err := os.WriteFile(fatal, make([]byte, maxFatalLogSize+1), 0600); err != nil {
		t.Fatal(err)
	}

	f, err := OpenCrashLog(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	files := crashFilesIn(t, dir)
	if len(files) != maxCrashFiles {
		t.Fatalf("%d crash files kept, want %d", len(files), maxCrashFiles)
	}
	if nano, _, _, _ := parseCrashFileName(files[0]); nano != base+5 {
		t.Fatalf("oldest kept crash file %s, want the five oldest removed", files[0])
	}
	if _, err := os.Stat(keepMe); err != nil {
		t.Fatalf("a file not named like a crash file was pruned: %v", err)
	}
	if info, err := os.Stat(fatal + ".1"); err != nil || info.Size() != maxFatalLogSize+1 {
		t.Fatalf("rotated fatal log = %v, %v; want the oversized one moved to fatal.txt.1", info, err)
	}
	info, err := os.Stat(fatal)
	if err != nil || info.Size() != 0 {
		t.Fatalf("fatal log = %v, %v; want a fresh empty file", info, err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0600 {
		t.Errorf("fatal log mode %v, want 0600", info.Mode().Perm())
	}
}
