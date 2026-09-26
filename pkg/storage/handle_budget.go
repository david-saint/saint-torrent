package storage

import (
	"cmp"
	"slices"
	"sync"
	"sync/atomic"
)

// Bounds on how many payload handles every FileStorage in the process may keep
// cached at once (see handleBudget).
const (
	minHandleBudget = 256
	maxHandleBudget = 8192
)

// handleBudget bounds the payload file handles FileStorage keeps cached across
// every torrent in the process. Each touched file used to keep a read and a
// write handle until the storage closed, so a single torrent with a few
// thousand files, whether a large dataset or metadata crafted for it, used up
// RLIMIT_NOFILE (on macOS about 10k) and every socket accept, dial and resume
// write of the whole process then failed with EMFILE.
//
// A cache hit takes no lock and does no atomic read-modify-write: it loads the
// handle and stamps the file with the coarse use clock, which in steady state
// is a load and a compare. Opening a handle, a syscall anyway, registers it
// here. Once more handles are open than the limit allows, a background sweep
// closes the least recently used ones through the files' try-lock invalidation
// paths, so it never waits on an open or an I/O in progress. An operation whose
// handle it closes after the operation loaded it gets os.ErrClosed and retries
// with a reopened handle (see fileLayout). Should the sweep fall behind, the
// opener evicts on its own goroutine once the limit is exceeded by half.
type handleBudget struct {
	// open counts cached handles, read and write, across every storage.
	open atomic.Int64
	// clock advances on every open, and every cache hit stamps its file with
	// it, so a smaller stamp means a less recently used file.
	clock atomic.Int64
	// limit is the count above which cached handles are evicted. Zero until
	// first needed, when handleLimit derives it from the descriptor limit.
	limit atomic.Int64
	// sweeping is set while the background sweep runs, evicting while an opener
	// evicts on its own goroutine.
	sweeping, evicting atomic.Bool

	mu sync.Mutex
	// held lists every file holding at least one cached handle. A file's
	// heldSlot is its index here while its cached count is nonzero; both are
	// guarded by mu.
	held []*fileLayout
}

// fileHandles is the process-wide budget every FileStorage draws from.
var fileHandles handleBudget

// handleLimitFor derives the cached-handle limit from the soft descriptor
// limit: half of it minus a reserve for sockets, the listener and the resume
// and session files, kept within [minHandleBudget, maxHandleBudget] but never
// above half of a very small limit. The input is clamped first, so
// RLIM_INFINITY cannot overflow the arithmetic.
func handleLimitFor(soft uint64) int64 {
	const ceiling = 1 << 40
	n := int64(min(soft, ceiling))
	half := n / 2
	limit := min(max(half-256, minHandleBudget), maxHandleBudget)
	return max(min(limit, half), 16)
}

// limitValue returns the cached-handle limit, deriving it on first use rather
// than at package init so that it sees the descriptor limit the process runs
// with.
func (b *handleBudget) limitValue() int64 {
	if limit := b.limit.Load(); limit > 0 {
		return limit
	}
	b.limit.CompareAndSwap(0, handleLimit())
	return b.limit.Load()
}

// acquire registers a handle f has just cached. The caller holds f.rmu or
// f.wmu. Past the limit it starts the background sweep, and it reports whether
// the budget is overrun badly enough that the caller must also call
// evictOverrun once it has released that lock.
func (b *handleBudget) acquire(f *fileLayout) (evictNow bool) {
	f.lastUse.Store(b.clock.Add(1))
	b.mu.Lock()
	if f.cached == 0 {
		f.heldSlot = len(b.held)
		b.held = append(b.held, f)
	}
	f.cached++
	b.mu.Unlock()

	open, limit := b.open.Add(1), b.limitValue()
	if open <= limit {
		return false
	}
	if b.sweeping.CompareAndSwap(false, true) {
		go b.sweep()
	}
	return open > limit+limit/2
}

// evictOverrun runs an eviction pass on the caller's goroutine, for when the
// background sweep has fallen far behind (it may not even have been scheduled
// yet). One such pass at a time is enough; other openers carry on.
func (b *handleBudget) evictOverrun() {
	if b.evicting.CompareAndSwap(false, true) {
		b.evict()
		b.evicting.Store(false)
	}
}

// release unregisters a cached handle of f that has just been closed. The
// caller holds f.rmu or f.wmu.
func (b *handleBudget) release(f *fileLayout) {
	b.mu.Lock()
	f.cached--
	if f.cached == 0 {
		last := len(b.held) - 1
		moved := b.held[last]
		b.held[f.heldSlot] = moved
		moved.heldSlot = f.heldSlot
		b.held[last] = nil
		b.held = b.held[:last]
	}
	b.mu.Unlock()
	b.open.Add(-1)
}

// sweep evicts in the background until the budget is back under its limit.
func (b *handleBudget) sweep() {
	for {
		freed := b.evict()
		b.sweeping.Store(false)
		// An open that crossed the limit while this pass ran could not start
		// another sweep, so go again if still over it. A pass that freed nothing
		// found every file busy; the next open over the limit retries.
		if freed == 0 || b.open.Load() <= b.limitValue() || !b.sweeping.CompareAndSwap(false, true) {
			return
		}
	}
}

// heldFile is a file's use stamp as the sweep snapshotted it.
type heldFile struct {
	file  *fileLayout
	stamp int64
}

// evict closes the least recently used cached handles until a quarter of the
// limit is free again, so that each pass pays for many opens. It never waits:
// a handle whose file lock is held, because it is being opened or invalidated,
// is skipped. It returns how many handles it closed.
func (b *handleBudget) evict() int {
	limit := b.limitValue()
	excess := b.open.Load() - (limit - limit/4)
	if excess <= 0 {
		return 0
	}
	b.mu.Lock()
	candidates := make([]heldFile, len(b.held))
	for i, f := range b.held {
		candidates[i] = heldFile{file: f, stamp: f.lastUse.Load()}
	}
	b.mu.Unlock()
	slices.SortFunc(candidates, func(x, y heldFile) int { return cmp.Compare(x.stamp, y.stamp) })

	freed := 0
	for _, c := range candidates {
		if int64(freed) >= excess {
			break
		}
		freed += c.file.tryEvict()
	}
	return freed
}
