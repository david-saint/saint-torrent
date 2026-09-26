//go:build !unix

package storage

// handleLimit returns the cached-handle limit where there is no descriptor
// limit to derive it from. Windows allows millions of handles per process, so
// the budget only keeps a many-file torrent from holding one per file.
func handleLimit() int64 {
	return maxHandleBudget
}
