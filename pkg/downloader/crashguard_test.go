package downloader

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// crashGuardTestSession is a Session carrying just what crashGuard reads.
func crashGuardTestSession(infoHashHex string, rec *crashRecorder) *Session {
	s := &Session{infoHashHex: infoHashHex}
	s.crash.Store(rec)
	return s
}

// catchPanic runs fn and returns the value it panicked with.
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

// crashGuardedPanic is a named frame the recorded stack must show.
func crashGuardedPanic(s *Session) {
	defer s.crashGuard("peer_loop")()
	var table []int
	_ = table[len(table)+2] // index out of range
}

// TestCrashGuardRecordsAndRepanics: the guard writes one private crash file
// named after the torrent and the component, holding the panic and the stack
// of the panicking goroutine, and then panics again with the same value
// instead of letting the goroutine carry on.
func TestCrashGuardRecordsAndRepanics(t *testing.T) {
	dir := t.TempDir()
	hash := strings.Repeat("ab", 20)
	s := crashGuardTestSession(hash, &crashRecorder{dir: dir})

	got := catchPanic(func() { crashGuardedPanic(s) })
	rtErr, ok := got.(runtime.Error)
	if !ok || !strings.Contains(rtErr.Error(), "index out of range") {
		t.Fatalf("re-panicked with %v (%T), want the original runtime error", got, got)
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

// TestCrashGuardWithoutRecorderRepanics: with persistence off there is
// nowhere to record, and the panic still goes on.
func TestCrashGuardWithoutRecorderRepanics(t *testing.T) {
	s := crashGuardTestSession(strings.Repeat("cd", 20), nil)
	got := catchPanic(func() {
		defer s.crashGuard("choke")()
		panic("no recorder")
	})
	if got != "no recorder" {
		t.Fatalf("re-panicked with %v, want the original value", got)
	}
	if got := s.crash.Load().record("choke", "", "x", nil); got != "" {
		t.Fatalf("nil recorder wrote %q", got)
	}
}

// TestNestedCrashGuardsRecordOnce: a peer loop runs inside a dial and an
// inbound handler, each guarded, and the same panic reaches every guard. Only
// the innermost records it, under its own component.
func TestNestedCrashGuardsRecordOnce(t *testing.T) {
	dir := t.TempDir()
	s := crashGuardTestSession(strings.Repeat("ef", 20), &crashRecorder{dir: dir})
	got := catchPanic(func() {
		defer s.crashGuard("peer_inbound")()
		func() {
			defer s.crashGuard("peer_dial")()
			func() {
				defer s.crashGuard("peer_loop")()
				func() {
					defer s.crashGuard("peer_loop")()
					panic("nested")
				}()
			}()
		}()
	})
	if got != "nested" {
		t.Fatalf("re-panicked with %v, want the original value", got)
	}
	files := crashFilesIn(t, dir)
	if len(files) != 1 {
		t.Fatalf("crash files = %v, want exactly one", files)
	}
	if _, _, component, _ := parseCrashFileName(files[0]); component != "peer_loop" {
		t.Fatalf("recorded component %q, want the innermost guard's", component)
	}

	// A later, separate panic is recorded again.
	_ = catchPanic(func() {
		defer s.crashGuard("choke")()
		panic("second")
	})
	if files := crashFilesIn(t, dir); len(files) != 2 {
		t.Fatalf("crash files after a second panic = %v, want two", files)
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
