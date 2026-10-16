//go:build !unix

package storage

// evictCachedHandles is false because on Windows closing a handle another
// goroutine may be using can hang: os.File.Close waits for the handle's
// in-flight operations, and a Close that lands while a positioned read or
// write is queued behind another leaks that operation's reference in
// internal/poll (FD.readWriteLock), so the wait never ends; the eviction
// sweep, holding the file's lock, then blocks every later open of the file.
// Windows has no small descriptor limit to protect, so there handles stay
// cached until the storage closes, as they did before the budget existed.
const evictCachedHandles = false

// boundedHandleClose is true because the same leak can strike a Close that
// is not an eviction: FileStorage.Close, or a repair dropping a stale read
// handle, can race a read on that handle. closeCachedHandle then waits a
// bounded time instead of forever, so session teardown cannot hang on a
// handle. Go's master branch fixes readWriteLock, but go1.26.9 does not;
// drop this once the toolchain floor carries that fix.
const boundedHandleClose = true

// handleLimit returns the cached-handle limit where there is no descriptor
// limit to derive it from. Windows allows millions of handles per process, so
// the budget only keeps a many-file torrent from holding one per file.
func handleLimit() int64 {
	return maxHandleBudget
}
