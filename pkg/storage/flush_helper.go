package storage

import (
	"errors"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"sainttorrent/internal/fsync"
)

// Out-of-process flushing.
//
// BitTorrent writes pieces out of order, and macOS does not throttle scattered
// writes the way it throttles a sequential writer: they land in the page cache
// at memory speed and reach the disk only when the kernel's delayed-write timer
// (about 30 s) or a flush pushes them. When the network outruns the disk the
// backlog grows to gigabytes, and flushing it on a slow volume takes minutes.
//
// A thread blocked in that flush cannot be torn down, so the process cannot
// exit until the flush returns: a forced os.Exit leaves it half-exited, holding
// the terminal. macOS offers no way to start write-back without waiting for it
// (msync(MS_ASYNC) and sync(2) both block until the data is written), so every
// long flush runs in a short-lived helper process instead: this executable
// re-run with FlushHelperArg, handed the already-open payload files as
// descriptors. The helper only pushes data out early. The checkpoint's own
// in-process flush stays the authoritative one, and once the helper has done
// the heavy lifting it only has the drive cache left to settle.
//
// Two paths use the helper. Write-behind (WriteBehind) pushes out whatever was
// written since the previous helper started, so the disk drains continuously
// instead of waiting for the kernel's timer. A durable checkpoint waits for the
// helper only up to durableFlushBudget; past that it saves a hint instead and
// reports ErrCheckpointDeferred, so quitting never waits on a slow disk. The
// helper keeps going after this process exits, and the next launch rechecks the
// pieces the deferred checkpoint could not prove.

// FlushHelperArg is the hidden subcommand that runs RunFlushHelper.
const FlushHelperArg = "__flush-payload"

// maxFlushHelperFiles caps the descriptors one helper inherits; a larger
// written set is pushed by consecutive helpers.
const maxFlushHelperFiles = 64

// maxWriteBehindHelpers caps the helpers running at once across every torrent,
// so many simultaneous downloads do not spawn a process per torrent per tick.
// A durable checkpoint is rare and does not count against it.
const maxWriteBehindHelpers = 4

// runningHelpers counts the helpers this process has running.
var runningHelpers atomic.Int32

// flushHelperFirstFD is the descriptor of the first handed-down file:
// os/exec's ExtraFiles start right after stdin, stdout and stderr.
const flushHelperFirstFD = 3

// durableFlushBudget bounds how long a durable checkpoint waits for the helper
// before it settles for a hint. The CLI force-exits two seconds into shutdown,
// so this leaves room for the rest of the teardown.
var durableFlushBudget = 750 * time.Millisecond

// ErrCheckpointDeferred reports that a durable checkpoint was not taken because
// written data was still on its way to the disk. A resume hint was saved in its
// place; the caller should try the checkpoint again later.
var ErrCheckpointDeferred = errors.New("durable checkpoint deferred: payload data still being flushed")

// WriteBehinder is implemented by storage that can push written data to the disk
// in the background. WriteBehind never waits on I/O beyond opening files.
type WriteBehinder interface {
	WriteBehind()
}

// flushSpawner starts a helper that pushes the data of every handle out to the
// device. The caller closes its own handles once it returns; wait reports how
// the helper ended.
type flushSpawner func(handles []*os.File) (wait func() error, err error)

var defaultFlushSpawner atomic.Pointer[flushSpawner]

// SetFlushHelper registers the executable that serves FlushHelperArg. Storage
// opened afterwards flushes payload data through it; without one, flushes run
// in-process as they always have. Platforms that cannot hand descriptors to a
// child keep the in-process flush.
func SetFlushHelper(executable string) {
	if executable == "" || !flushHelperSupported {
		defaultFlushSpawner.Store(nil)
		return
	}
	spawn := flushSpawner(func(handles []*os.File) (func() error, error) {
		return spawnFlushHelper(executable, handles)
	})
	defaultFlushSpawner.Store(&spawn)
}

func currentFlushSpawner() flushSpawner {
	if spawn := defaultFlushSpawner.Load(); spawn != nil {
		return *spawn
	}
	return nil
}

// RunFlushHelper is the helper's whole program: push out every descriptor it
// was handed and exit. It returns the process exit code.
func RunFlushHelper(args []string) int {
	if len(args) != 1 {
		return 2
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count <= 0 || count > maxFlushHelperFiles {
		return 2
	}
	code := 0
	for i := range count {
		f := os.NewFile(uintptr(flushHelperFirstFD+i), "payload")
		if f == nil {
			code = 1
			continue
		}
		if err := fsync.WriteBack(f); err != nil {
			code = 1
		}
		_ = f.Close()
	}
	return code
}

// flushJob is one running helper.
type flushJob struct {
	done chan struct{}
}

// WriteBehind starts a helper over the files written since the previous helper
// started, unless one is still running. It returns at once: the push runs in
// the helper, so neither this goroutine nor the process exit waits on it.
func (s *FileStorage) WriteBehind() {
	if s.closed.Load() {
		return
	}
	s.pushMu.Lock()
	defer s.pushMu.Unlock()
	if s.spawner == nil || (s.pushJob != nil && !s.pushJob.finished()) {
		return
	}
	if runningHelpers.Load() >= maxWriteBehindHelpers {
		return
	}
	s.startPushLocked()
}

// awaitPushed waits, at most budget, until a helper has pushed out everything
// written so far. It reports true when the checkpoint may flush in-process
// without a long wait: the data is out, or there is no helper to wait for (the
// in-process flush then does the work, as it did before helpers existed).
func (s *FileStorage) awaitPushed(budget time.Duration) bool {
	timer := time.NewTimer(budget)
	defer timer.Stop()
	for {
		s.pushMu.Lock()
		if s.spawner == nil {
			s.pushMu.Unlock()
			return true
		}
		job := s.pushJob
		if job == nil || job.finished() {
			started, ok := s.startPushLocked()
			s.pushMu.Unlock()
			if !ok || started == nil {
				// Nothing left to push, or no helper could start.
				return true
			}
			job = started
		} else {
			s.pushMu.Unlock()
		}
		select {
		case <-job.done:
		case <-timer.C:
			return false
		}
	}
}

// startPushLocked starts a helper over the files written since the previous
// helper started. It returns the job, or nil when nothing was written; ok is
// false when no helper could be started, after which this storage flushes
// in-process. The caller holds pushMu.
func (s *FileStorage) startPushLocked() (job *flushJob, ok bool) {
	s.mtMu.Lock()
	files := make([]*fileLayout, 0, min(len(s.pushDirty), maxFlushHelperFiles))
	for i, written := range s.pushDirty {
		if !written || i >= len(s.files) {
			continue
		}
		s.pushDirty[i] = false
		files = append(files, s.files[i])
		if len(files) == maxFlushHelperFiles {
			break
		}
	}
	s.mtMu.Unlock()
	if len(files) == 0 {
		return nil, true
	}

	handles := make([]*os.File, 0, len(files))
	for _, file := range files {
		if _, h, opened := s.openPayloadMetadata(file, resumeSyncOpenFlags); opened {
			handles = append(handles, h)
		}
	}
	// A file that could not be opened is left to the checkpoint, which treats it
	// the same way.
	if len(handles) == 0 {
		return nil, true
	}
	wait, err := s.spawner(handles)
	for _, h := range handles {
		_ = h.Close()
	}
	if err != nil {
		// Give the files back to the in-process flush and stop trying helpers.
		s.spawner = nil
		s.mtMu.Lock()
		for _, file := range files {
			s.pushDirty[file.index] = true
		}
		s.mtMu.Unlock()
		return nil, false
	}
	job = &flushJob{done: make(chan struct{})}
	s.pushJob = job
	runningHelpers.Add(1)
	go func() {
		// A helper that failed leaves its files' data to the checkpoint's own
		// flush, which still runs and decides whether each file can be trusted.
		_ = wait()
		runningHelpers.Add(-1)
		close(job.done)
	}()
	return job, true
}

func (j *flushJob) finished() bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}
