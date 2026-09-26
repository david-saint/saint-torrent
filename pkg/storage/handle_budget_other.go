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

// handleLimit returns the cached-handle limit where there is no descriptor
// limit to derive it from. Windows allows millions of handles per process, so
// the budget only keeps a many-file torrent from holding one per file.
func handleLimit() int64 {
	return maxHandleBudget
}
