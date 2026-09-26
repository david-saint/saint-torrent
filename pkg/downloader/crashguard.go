package downloader

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Crash handling. A panic in a session or restore goroutine is recorded under
// <stateDir>/crash and then re-raised: the process never continues after a
// recover, because the goroutine may have died holding s.mu or m.mu, with some
// of its unlocks deferred and some not. Nothing here runs on a hot path: a
// guard is one deferred closure per goroutine, and disk I/O happens only on a
// crash and at startup.

const (
	crashDirName = "crash"
	fatalLogName = "fatal.txt"

	// maxCrashFiles is how many per-crash files OpenCrashLog keeps.
	maxCrashFiles = 32
	// maxFatalLogSize is the size past which OpenCrashLog rotates fatal.txt
	// to fatal.txt.1 before appending to it.
	maxFatalLogSize = 1 << 20
	// maxCrashValueLen bounds the panic value written to a crash file.
	maxCrashValueLen = 64 << 10

	// crashComponentRestore names a crash while a persisted torrent was being
	// restored.
	crashComponentRestore = "restore"
	// unattributedCrash stands in for the info-hash of a crash no torrent is
	// blamed for.
	unattributedCrash = "unattributed"
)

// CrashDir returns the directory under stateDir that holds one file per
// recorded crash and fatal.txt, the runtime's own crash output.
func CrashDir(stateDir string) string {
	return filepath.Join(stateDir, crashDirName)
}

// crashRecorder writes crash files into dir. It is immutable.
type crashRecorder struct {
	dir string
}

// record writes <dir>/<unixnano>-<info-hash or "unattributed">-<component>.txt
// holding the component, info-hash, time, panic value and stack, and returns
// its path, or "" if nothing could be written. It runs while the process is
// crashing, so it never panics and ignores its own errors.
func (c *crashRecorder) record(component, infoHashHex string, v any, stack []byte) (path string) {
	defer func() {
		if recover() != nil {
			path = ""
		}
	}()
	if c == nil || c.dir == "" {
		return ""
	}
	if !validCrashComponent(component) {
		component = "unknown"
	}
	attribution := crashAttributionName(infoHashHex)
	now := time.Now()
	value := fmt.Sprint(v)
	if len(value) > maxCrashValueLen {
		value = value[:maxCrashValueLen] + "..."
	}
	var body strings.Builder
	fmt.Fprintf(&body, "component: %s\n", component)
	fmt.Fprintf(&body, "info_hash: %s\n", attribution)
	fmt.Fprintf(&body, "time: %s\n", now.Format(time.RFC3339))
	// Quoted: the value can carry peer-supplied bytes, and this file is meant
	// to be read in a terminal.
	fmt.Fprintf(&body, "panic: %s\n\n", strconv.Quote(value))
	body.Write(stack)

	// O_EXCL never follows a planted symlink; a name taken in the same
	// nanosecond moves on to the next one.
	for i := int64(0); i < 4; i++ {
		name := fmt.Sprintf("%d-%s-%s.txt", now.UnixNano()+i, attribution, component)
		candidate := filepath.Join(c.dir, name)
		f, err := os.OpenFile(candidate, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			if os.IsExist(err) {
				continue
			}
			return ""
		}
		_, werr := f.WriteString(body.String())
		_ = f.Sync()
		_ = f.Close()
		if werr != nil {
			return ""
		}
		return candidate
	}
	return ""
}

// validCrashComponent reports whether component is a non-empty [a-z_]+ name
// that fits in a file name.
func validCrashComponent(component string) bool {
	if component == "" || len(component) > 32 {
		return false
	}
	for i := 0; i < len(component); i++ {
		if c := component[i]; (c < 'a' || c > 'z') && c != '_' {
			return false
		}
	}
	return true
}

// crashAttributionName is infoHashHex in lower case when it is a 40-digit hex
// info-hash, and "unattributed" otherwise.
func crashAttributionName(infoHashHex string) string {
	if !isInfoHashHex(infoHashHex) {
		return unattributedCrash
	}
	return strings.ToLower(infoHashHex)
}

func isInfoHashHex(s string) bool {
	if len(s) != 40 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// crashGuard returns the deferred half of a crash guard for a session
// goroutine: `defer s.crashGuard("peer_loop")()`, placed at the top of the
// function so every caller is covered. On a panic it records the crash
// against this torrent (when persistence is on) and panics again with the
// same value; the process then dies with the original trace. It never
// resumes the goroutine and never takes s.mu.
func (s *Session) crashGuard(component string) func() {
	return func() {
		if r := recover(); r != nil {
			if rec := s.crash.Load(); rec != nil {
				recordAndRepanic(rec, component, s.infoHashHex, r)
			}
			panic(r)
		}
	}
}

// crashGuard is Session.crashGuard for manager work on one persisted entry.
func (m *TorrentManager) crashGuard(component, infoHashHex string) func() {
	return func() {
		if r := recover(); r != nil {
			if rec := m.crash.Load(); rec != nil {
				recordAndRepanic(rec, component, infoHashHex, r)
			}
			panic(r)
		}
	}
}

// recordAndRepanic records the panic r and raises it again. Guards nest (a
// peer loop runs inside a dial or an inbound handler, each guarded), and the
// same panic reaches every one of them in turn; only the innermost records
// it. An outer guard sees the inner one's recordAndRepanic still on the
// stack, since a panic unwinds nothing until it is recovered for good.
//
//go:noinline
func recordAndRepanic(rec *crashRecorder, component, infoHashHex string, r any) {
	if !panicRecordedBelow() {
		rec.record(component, infoHashHex, r, debug.Stack())
	}
	panic(r)
}

// recordAndRepanicName is recordAndRepanic's symbol name as tracebacks show it.
// Set in init: a variable initializer referring to recordAndRepanic would be
// an initialization cycle.
var recordAndRepanicName string

func init() {
	recordAndRepanicName = runtime.FuncForPC(reflect.ValueOf(recordAndRepanic).Pointer()).Name()
}

// panicRecordedBelow reports whether the calling recordAndRepanic handles a
// panic an inner guard already recorded and raised again: a second
// recordAndRepanic frame on this goroutine's stack.
func panicRecordedBelow() bool {
	var pcs [64]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(1, pcs[:])])
	seen := 0
	for {
		frame, more := frames.Next()
		if frame.Function == recordAndRepanicName {
			if seen++; seen > 1 {
				return true
			}
		}
		if !more {
			return false
		}
	}
}

// RecordCrash records a crash the caller recovered itself (the TUI's), with
// the calling goroutine's stack, and returns the crash file's path, or "" when
// persistence is off or nothing could be written. infoHashHex may be empty.
func (m *TorrentManager) RecordCrash(component, infoHashHex string, v any) string {
	return m.crash.Load().record(component, infoHashHex, v, debug.Stack())
}

// OpenCrashLog prepares <stateDir>/crash for this run and opens its
// fatal.txt for appending, for debug.SetCrashOutput: fatal errors no guard
// can catch (concurrent map writes, stack exhaustion, panics in unguarded
// goroutines) then leave a trace too. It keeps the newest maxCrashFiles crash
// files and rotates fatal.txt to fatal.txt.1 past maxFatalLogSize. Call it
// only while holding the single-instance lock.
func OpenCrashLog(stateDir string) (*os.File, error) {
	dir := CrashDir(stateDir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	_ = os.Chmod(dir, 0700)
	pruneCrashFiles(dir, maxCrashFiles)

	path := filepath.Join(dir, fatalLogName)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is not a regular file", path)
		}
		if info.Size() > maxFatalLogSize {
			_ = os.Rename(path, path+".1")
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	// Refuse a symlink swapped in before the open: what was opened must be
	// the regular file at path itself.
	opened, statErr := f.Stat()
	named, lstatErr := os.Lstat(path)
	if statErr != nil || lstatErr != nil || !named.Mode().IsRegular() || !os.SameFile(opened, named) {
		_ = f.Close()
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	return f, nil
}

// parseCrashFileName splits <unixnano>-<info-hash or unattributed>-<component>.txt.
// infoHashHex is "" for an unattributed crash.
func parseCrashFileName(name string) (nano int64, infoHashHex, component string, ok bool) {
	base, found := strings.CutSuffix(name, ".txt")
	if !found {
		return 0, "", "", false
	}
	parts := strings.SplitN(base, "-", 3)
	if len(parts) != 3 || parts[0] == "" || !validCrashComponent(parts[2]) {
		return 0, "", "", false
	}
	nano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || nano < 0 {
		return 0, "", "", false
	}
	switch {
	case parts[1] == unattributedCrash:
	case isInfoHashHex(parts[1]):
		infoHashHex = strings.ToLower(parts[1])
	default:
		return 0, "", "", false
	}
	return nano, infoHashHex, parts[2], true
}

// pruneCrashFiles removes all but the newest keep crash files in dir.
// fatal.txt and anything else not named like a crash file is left alone.
func pruneCrashFiles(dir string, keep int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type crashEntry struct {
		nano int64
		name string
	}
	var files []crashEntry
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		if nano, _, _, ok := parseCrashFileName(e.Name()); ok {
			files = append(files, crashEntry{nano: nano, name: e.Name()})
		}
	}
	if len(files) <= keep {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].nano != files[j].nano {
			return files[i].nano < files[j].nano
		}
		return files[i].name < files[j].name
	})
	for _, f := range files[:len(files)-keep] {
		_ = os.Remove(filepath.Join(dir, f.name))
	}
}
