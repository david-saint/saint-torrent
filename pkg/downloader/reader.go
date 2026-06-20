package downloader

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"sainttorrent/pkg/storage"
)

// ErrReaderClosed is returned by TorrentReader operations after Close.
var ErrReaderClosed = errors.New("torrent reader is closed")

// ReaderOptions configures a verified torrent reader. Offset and Length describe
// the byte range exposed by the reader; Length <= 0 means through the end of the
// torrent. ReadaheadPieces controls the sequential picker window used while reads
// are waiting for future pieces.
type ReaderOptions struct {
	Offset          int64
	Length          int64
	ReadaheadPieces int
}

// TorrentReader exposes hash-verified torrent bytes as io.Reader, io.ReaderAt,
// io.Seeker, and io.Closer. Reads block until the requested pieces are verified.
type TorrentReader struct {
	sess            *Session
	base            int64
	length          int64
	readaheadPieces int

	mu     sync.Mutex
	offset int64
	closed atomic.Bool
}

var (
	_ io.Reader   = (*TorrentReader)(nil)
	_ io.ReaderAt = (*TorrentReader)(nil)
	_ io.Seeker   = (*TorrentReader)(nil)
	_ io.Closer   = (*TorrentReader)(nil)
)

// NewReader returns a verified reader over the configured torrent byte range.
func (s *Session) NewReader(opts ReaderOptions) (*TorrentReader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Storage == nil {
		return nil, fmt.Errorf("torrent storage is not available")
	}
	total := s.Storage.TotalSize()
	if opts.Offset < 0 {
		return nil, fmt.Errorf("negative reader offset: %d", opts.Offset)
	}
	if opts.Offset > total {
		return nil, fmt.Errorf("reader offset %d exceeds torrent size %d", opts.Offset, total)
	}
	length := opts.Length
	if length <= 0 || length > total-opts.Offset {
		length = total - opts.Offset
	}
	return &TorrentReader{
		sess:            s,
		base:            opts.Offset,
		length:          length,
		readaheadPieces: normalizeSequentialReadaheadPieces(opts.ReadaheadPieces),
	}, nil
}

// NewReaderAt is an alias for NewReader for callers that only need io.ReaderAt.
func (s *Session) NewReaderAt(opts ReaderOptions) (*TorrentReader, error) {
	return s.NewReader(opts)
}

// NewFileReader returns a verified reader scoped to one torrent file.
func (s *Session) NewFileReader(fileIndex int, opts ReaderOptions) (*TorrentReader, error) {
	s.mu.RLock()
	if s.Torrent == nil {
		s.mu.RUnlock()
		return nil, fmt.Errorf("torrent metadata is not available")
	}
	if fileIndex < 0 || fileIndex >= len(s.Torrent.Files) {
		s.mu.RUnlock()
		return nil, fmt.Errorf("file index %d out of range", fileIndex)
	}
	var fileStart int64
	for i := 0; i < fileIndex; i++ {
		fileStart += s.Torrent.Files[i].Length
	}
	fileLen := s.Torrent.Files[fileIndex].Length
	s.mu.RUnlock()

	if opts.Offset < 0 {
		return nil, fmt.Errorf("negative file reader offset: %d", opts.Offset)
	}
	if opts.Offset > fileLen {
		return nil, fmt.Errorf("file reader offset %d exceeds file size %d", opts.Offset, fileLen)
	}
	length := opts.Length
	if length <= 0 || length > fileLen-opts.Offset {
		length = fileLen - opts.Offset
	}
	return s.NewReader(ReaderOptions{
		Offset:          fileStart + opts.Offset,
		Length:          length,
		ReadaheadPieces: opts.ReadaheadPieces,
	})
}

func (r *TorrentReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	r.mu.Lock()
	offset := r.offset
	r.mu.Unlock()

	n, err := r.ReadAt(p, offset)

	r.mu.Lock()
	r.offset += int64(n)
	r.mu.Unlock()
	if n > 0 && errors.Is(err, io.EOF) {
		return n, nil
	}
	return n, err
}

func (r *TorrentReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	requested := len(p)
	if r.closed.Load() {
		return 0, ErrReaderClosed
	}
	if off < 0 {
		return 0, fmt.Errorf("negative read offset: %d", off)
	}
	if off >= r.length {
		return 0, io.EOF
	}

	readLen := int64(len(p))
	if readLen > r.length-off {
		readLen = r.length - off
	}
	p = p[:int(readLen)]

	globalStart := r.base + off
	r.prioritize(globalStart, int64(len(p)))

	total := 0
	for total < len(p) {
		if r.closed.Load() {
			if total > 0 {
				return total, ErrReaderClosed
			}
			return 0, ErrReaderClosed
		}

		globalOffset := globalStart + int64(total)
		st, pieceIndex, pieceOffset, chunkLen, err := r.chunkFor(globalOffset, len(p)-total)
		if err != nil {
			if total > 0 {
				return total, err
			}
			return 0, err
		}
		if err := r.waitForPiece(pieceIndex); err != nil {
			if total > 0 {
				return total, err
			}
			return 0, err
		}

		n, err := st.ReadBlock(pieceIndex, pieceOffset, p[total:total+chunkLen])
		total += n
		if err != nil {
			if total > 0 {
				return total, err
			}
			return 0, err
		}
		if n != chunkLen {
			if total > 0 {
				return total, io.ErrUnexpectedEOF
			}
			return 0, io.ErrUnexpectedEOF
		}
	}
	if readLen < int64(requested) {
		return total, io.EOF
	}
	return total, nil
}

func (r *TorrentReader) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		next = r.length + offset
	default:
		return r.offset, fmt.Errorf("invalid whence: %d", whence)
	}
	if next < 0 {
		return r.offset, fmt.Errorf("negative seek offset: %d", next)
	}
	r.offset = next
	return next, nil
}

func (r *TorrentReader) Close() error {
	if r.closed.CompareAndSwap(false, true) {
		r.sess.mu.Lock()
		r.sess.broadcastPieceWaitersLocked()
		r.sess.mu.Unlock()
	}
	return nil
}

func (r *TorrentReader) prioritize(globalOffset, length int64) {
	r.sess.mu.Lock()
	r.sess.prioritizeSequentialReadLocked(globalOffset, length, r.readaheadPieces)
	r.sess.mu.Unlock()
}

func (r *TorrentReader) chunkFor(globalOffset int64, maxLen int) (storage.Storage, int64, int64, int, error) {
	r.sess.mu.RLock()
	defer r.sess.mu.RUnlock()
	st := r.sess.Storage
	if st == nil {
		return nil, 0, 0, 0, fmt.Errorf("torrent storage is not available")
	}
	pieceLength := st.PieceLengthValue()
	if pieceLength <= 0 {
		return nil, 0, 0, 0, fmt.Errorf("invalid piece length: %d", pieceLength)
	}
	pieceIndex := globalOffset / pieceLength
	if pieceIndex < 0 || pieceIndex >= int64(len(r.sess.PieceStates)) {
		return nil, 0, 0, 0, io.EOF
	}
	pieceOffset := globalOffset % pieceLength
	pieceLen := st.PieceLength(pieceIndex)
	if pieceOffset >= pieceLen {
		return nil, 0, 0, 0, io.EOF
	}
	chunkLen := int(pieceLen - pieceOffset)
	if chunkLen > maxLen {
		chunkLen = maxLen
	}
	return st, pieceIndex, pieceOffset, chunkLen, nil
}

func (r *TorrentReader) waitForPiece(pieceIndex int64) error {
	r.sess.mu.Lock()
	defer r.sess.mu.Unlock()
	cond := r.sess.ensurePieceCondLocked()
	for {
		if r.closed.Load() {
			return ErrReaderClosed
		}
		if r.sess.closed {
			return storage.ErrStorageClosed
		}
		if r.sess.statusErr != nil {
			return r.sess.statusErr
		}
		if pieceIndex < 0 || pieceIndex >= int64(len(r.sess.PieceStates)) {
			return io.EOF
		}
		if r.sess.PieceStates[pieceIndex] == PieceCompleted {
			return nil
		}
		cond.Wait()
	}
}

func (s *Session) ensurePieceCondLocked() *sync.Cond {
	if s.pieceCond == nil {
		s.pieceCond = sync.NewCond(&s.mu)
	}
	return s.pieceCond
}

func (s *Session) broadcastPieceWaitersLocked() {
	if s.pieceCond != nil {
		s.pieceCond.Broadcast()
	}
}
