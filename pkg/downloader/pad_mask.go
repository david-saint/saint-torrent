package downloader

import (
	"sort"

	"sainttorrent/pkg/torrent"
)

// padMask is the immutable map of a torrent's BEP 47 pad ranges. Pad bytes are
// zeros by definition, so they are never requested from peers or webseeds: a
// block lying entirely in pad files is zero-filled when its piece is assembled,
// and a piece lying entirely in them is complete from the start. It is built
// once, when the torrent's metadata is known, and never changes afterwards, so
// the peer loops read it without any lock. A torrent without pad files has a
// nil mask and pays nothing for it.
type padMask struct {
	// spans are the pad ranges in torrent offsets, sorted, with adjacent pad
	// files merged.
	spans       []torrent.PadSpan
	pieceLength int64
	// fullPieces lists, in ascending order, the pieces lying entirely in pad
	// files. torrent.ParseInfo refuses a torrent unless each such piece hashes
	// as zeros, so marking them complete never skips a piece that could fail.
	fullPieces []int
}

// padRun is a run of blocks [first, end) of one piece that lie entirely in pad
// files.
type padRun struct {
	first, end int64
}

// newPadMask builds the mask of a torrent's files, or returns nil when it has
// no pad bytes (or no known piece length yet, as for a magnet).
func newPadMask(files []torrent.File, pieceLength int64, numPieces int) *padMask {
	spans := torrent.PadSpans(files)
	if len(spans) == 0 || pieceLength <= 0 {
		return nil
	}
	var total int64
	for _, f := range files {
		total += f.Length
	}
	m := &padMask{spans: spans, pieceLength: pieceLength}
	for _, span := range spans {
		for piece := (span.Start + pieceLength - 1) / pieceLength; piece < int64(numPieces); piece++ {
			start := piece * pieceLength
			if min(start+pieceLength, total) > span.End {
				break
			}
			m.fullPieces = append(m.fullPieces, int(piece))
		}
	}
	return m
}

// firstSpanEndingAfter returns the index of the first span that ends after
// offset.
func (m *padMask) firstSpanEndingAfter(offset int64) int {
	return sort.Search(len(m.spans), func(i int) bool { return m.spans[i].End > offset })
}

// pieceBlocks describes the pad blocks of a piece of length pieceLen split into
// numBlocks blocks: the runs of blocks lying entirely in pad files (nil when
// there are none), how many blocks those runs hold, and requestEnd, one past
// the last block that holds payload. Blocks that straddle payload and pad are
// requested normally. hasPad reports whether the piece holds any pad byte at
// all, so its assembly must zero them. It runs once per piece a peer opens.
func (m *padMask) pieceBlocks(piece, pieceLen, numBlocks int64) (runs []padRun, padBlocks, requestEnd int64, hasPad bool) {
	requestEnd = numBlocks
	pieceStart := piece * m.pieceLength
	pieceEnd := pieceStart + pieceLen
	for i := m.firstSpanEndingAfter(pieceStart); i < len(m.spans) && m.spans[i].Start < pieceEnd; i++ {
		hasPad = true
		start := max(m.spans[i].Start, pieceStart) - pieceStart
		end := min(m.spans[i].End, pieceEnd) - pieceStart
		first := (start + BlockSize - 1) / BlockSize
		last := end / BlockSize // first block not entirely before end
		if end == pieceLen {
			last = numBlocks // the short final block ends at pieceLen too
		}
		if last > first {
			runs = append(runs, padRun{first: first, end: last})
			padBlocks += last - first
		}
	}
	if n := len(runs); n > 0 && runs[n-1].end == numBlocks {
		requestEnd = runs[n-1].first
	}
	return runs, padBlocks, requestEnd, hasPad
}

// padRunEnd returns the end of the run in runs holding block b, or -1 when b
// holds payload.
func padRunEnd(runs []padRun, b int64) int64 {
	for _, r := range runs {
		if b < r.first {
			break
		}
		if b < r.end {
			return r.end
		}
	}
	return -1
}

// zeroPad clears the pad bytes of buf, which holds the torrent bytes starting
// at offset start. Assembly calls it for a piece holding pad bytes: its pad
// blocks were never received, and the pooled piece buffer is not zeroed, while
// the pad part of a block straddling payload holds whatever the peer sent. The
// piece is then hashed exactly as storage will serve and verify it.
func (m *padMask) zeroPad(start int64, buf []byte) {
	end := start + int64(len(buf))
	for i := m.firstSpanEndingAfter(start); i < len(m.spans) && m.spans[i].Start < end; i++ {
		clear(buf[max(m.spans[i].Start, start)-start : min(m.spans[i].End, end)-start])
	}
}

// markPadPiecesCompleteLocked marks every piece lying entirely in pad files
// complete. It writes PieceStates directly, like the bulk state rebuilds that
// call it; they recompute the needed set and the stats afterwards. Caller
// holds s.mu.
func (s *Session) markPadPiecesCompleteLocked() {
	if s.padMask == nil {
		return
	}
	for _, idx := range s.padMask.fullPieces {
		if idx < len(s.PieceStates) {
			s.PieceStates[idx] = PieceCompleted
		}
	}
}

// isPadFileLocked reports whether file index i of the torrent is a pad file.
// Pad files keep their place in the file list and the priorities, so indices
// (SetFilePriority, persisted priorities) still match the torrent's, but they
// are always skipped. Caller holds s.mu.
func (s *Session) isPadFileLocked(i int) bool {
	return s.Torrent != nil && i >= 0 && i < len(s.Torrent.Files) && s.Torrent.Files[i].Pad
}

// PayloadFiles returns files without its BEP 47 pad files, which hold no
// payload and are hidden from file lists, plus index, where index[i] is the
// position of payload[i] in files (the index SetFilePriority takes). When no
// file is a pad file it returns files itself and a nil index, allocating
// nothing.
func PayloadFiles(files []torrent.File) (payload []torrent.File, index []int) {
	pads := 0
	for _, f := range files {
		if f.Pad {
			pads++
		}
	}
	if pads == 0 {
		return files, nil
	}
	payload = make([]torrent.File, 0, len(files)-pads)
	index = make([]int, 0, len(files)-pads)
	for i, f := range files {
		if !f.Pad {
			payload = append(payload, f)
			index = append(index, i)
		}
	}
	return payload, index
}
