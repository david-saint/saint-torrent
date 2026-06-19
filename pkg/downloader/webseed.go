package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sainttorrent/pkg/torrent"
)

var (
	webseedHTTPClient     = http.DefaultClient
	webseedRequestTimeout = 30 * time.Second
	webseedIdleDelay      = 500 * time.Millisecond
	errWebseedPaused      = errors.New("webseed paused")
)

type webseedSpec struct {
	raw     string
	display string
	files   []webseedFile
	host    string
	port    uint16
}

type webseedFile struct {
	start int64
	end   int64
	url   string
}

type webseedPiece struct {
	index         int64
	hash          [20]byte
	length        int64
	absoluteStart int64
}

func (s *Session) webseedSpecsForStart() []webseedSpec {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.Torrent == nil || s.Storage == nil || s.metadataMode || len(s.Torrent.WebSeeds) == 0 || len(s.Torrent.Files) == 0 {
		return nil
	}

	files := makeWebseedTorrentFiles(s.Torrent.Files)
	webseedURLs := append([]string(nil), s.Torrent.WebSeeds...)
	multiFile := len(files) != 1 || len(files[0].path) != 1

	specs := make([]webseedSpec, 0, len(webseedURLs))
	seen := make(map[string]struct{}, len(webseedURLs))
	for _, raw := range webseedURLs {
		spec, ok := buildWebseedSpec(raw, files, multiFile)
		if !ok {
			continue
		}
		if _, exists := seen[spec.raw]; exists {
			continue
		}
		seen[spec.raw] = struct{}{}
		specs = append(specs, spec)
	}
	return specs
}

type webseedTorrentFile struct {
	length int64
	path   []string
}

func makeWebseedTorrentFiles(files []torrent.File) []webseedTorrentFile {
	out := make([]webseedTorrentFile, 0, len(files))
	for _, f := range files {
		path := append([]string(nil), f.Path...)
		out = append(out, webseedTorrentFile{length: f.Length, path: path})
	}
	return out
}

func buildWebseedSpec(raw string, files []webseedTorrentFile, multiFile bool) (webseedSpec, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return webseedSpec{}, false
	}
	base, err := url.Parse(raw)
	if err != nil || base.Host == "" {
		return webseedSpec{}, false
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return webseedSpec{}, false
	}

	spec := webseedSpec{
		raw:     base.String(),
		display: base.String(),
		host:    base.Hostname(),
		port:    defaultURLPort(base),
	}
	var offset int64
	for _, f := range files {
		fileURL := base.String()
		if multiFile {
			fileURL, err = url.JoinPath(base.String(), f.path...)
			if err != nil {
				return webseedSpec{}, false
			}
		}
		spec.files = append(spec.files, webseedFile{
			start: offset,
			end:   offset + f.length,
			url:   fileURL,
		})
		offset += f.length
	}
	return spec, true
}

func defaultURLPort(u *url.URL) uint16 {
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err == nil && n > 0 && n <= 65535 {
			return uint16(n)
		}
		return 0
	}
	switch u.Scheme {
	case "http":
		return 80
	case "https":
		return 443
	default:
		return 0
	}
}

func (s *Session) webseedLoop(spec webseedSpec) {
	defer s.wg.Done()

	peerAddr, pState := s.registerWebseedPeer(spec)
	defer s.unregisterWebseedPeer(peerAddr)

	client := webseedHTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	for {
		piece, ok, done := s.claimWebseedPiece()
		if done {
			return
		}
		if !ok {
			if !s.waitWebseedIdle() {
				return
			}
			continue
		}

		data, err := s.fetchWebseedPiece(s.ctx, client, spec, piece, pState)
		if errors.Is(err, errWebseedPaused) {
			s.releaseWebseedPiece(piece.index, nil)
			if !s.waitWebseedIdle() {
				return
			}
			continue
		}
		if err != nil {
			s.releaseWebseedPiece(piece.index, fmt.Errorf("webseed %s: %w", spec.display, err))
			return
		}

		result := make(chan pieceWriteResult, 1)
		s.ensurePieceWritePool()
		select {
		case s.pieceWriteCh <- pieceWriteJob{index: piece.index, hash: piece.hash, data: data, result: result}:
		case <-s.ctx.Done():
			s.releaseWebseedPiece(piece.index, nil)
			return
		}

		select {
		case res := <-result:
			switch res.status {
			case pieceWriteHashFailed:
				s.markWebseedFailed(peerAddr, fmt.Errorf("webseed %s served corrupt bytes: %w", spec.display, res.err))
				return
			case pieceWriteStorageFailed:
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) registerWebseedPeer(spec webseedSpec) (string, *PeerState) {
	addr := "webseed:" + spec.raw
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	pState := s.Peers[addr]
	if pState == nil {
		pState = &PeerState{}
		s.Peers[addr] = pState
	}
	pState.IP = spec.host
	pState.Port = spec.port
	pState.Choked = false
	pState.Interested = true
	pState.Active = true
	pState.AmChoking = true
	pState.LastAttempt = now
	pState.Dialable = false
	pState.Dialing = false
	pState.WindowBlocks = 1
	pState.TargetWindowBlocks = 1
	return addr, pState
}

func (s *Session) unregisterWebseedPeer(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if pState := s.Peers[addr]; pState != nil {
		pState.Active = false
		pState.Choked = true
		pState.Interested = false
		pState.LastAttempt = time.Now()
		pState.DownloadSpeed = 0
		pState.OutstandingBlocks = 0
		pState.OutstandingBytes = 0
	}
}

func (s *Session) markWebseedFailed(addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr = err
	if pState := s.Peers[addr]; pState != nil {
		pState.Active = false
		pState.Choked = true
		pState.Interested = false
		pState.LastAttempt = time.Now()
	}
}

func (s *Session) claimWebseedPiece() (webseedPiece, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.paused || s.Storage == nil || s.metadataMode {
		return webseedPiece{}, false, false
	}
	if s.isCompletedLocked() {
		return webseedPiece{}, false, true
	}
	bestIdx := s.selectNeededPieceLocked(func(int64) bool { return true })
	if bestIdx == -1 {
		return webseedPiece{}, false, false
	}
	s.PieceStates[bestIdx] = PieceDownloading
	s.removeNeededLocked(bestIdx)
	return webseedPiece{
		index:         int64(bestIdx),
		hash:          s.Torrent.PieceHashes[bestIdx],
		length:        s.Storage.PieceLength(int64(bestIdx)),
		absoluteStart: int64(bestIdx) * s.Storage.PieceLengthValue(),
	}, true, false
}

func (s *Session) releaseWebseedPiece(index int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastErr = err
	}
	if index >= 0 && index < int64(len(s.PieceStates)) && s.PieceStates[index] == PieceDownloading {
		s.PieceStates[index] = PieceEmpty
		s.addNeededLocked(int(index))
	}
}

func (s *Session) fetchWebseedPiece(ctx context.Context, client *http.Client, spec webseedSpec, piece webseedPiece, pState *PeerState) ([]byte, error) {
	if piece.length < 0 || piece.length > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("piece %d length is not addressable: %d", piece.index, piece.length)
	}
	data := make([]byte, int(piece.length))
	var offset int64
	for offset < piece.length {
		if s.webseedPaused() {
			return nil, errWebseedPaused
		}
		blockLen := int64(BlockSize)
		if remaining := piece.length - offset; remaining < blockLen {
			blockLen = remaining
		}
		block, err := s.fetchWebseedLinearRange(ctx, client, spec, piece.absoluteStart+offset, blockLen)
		if err != nil {
			return nil, err
		}
		copy(data[int(offset):], block)
		offset += blockLen

		n := int64(len(block))
		s.Downloaded.Add(n)
		atomic.AddInt64(&pState.Downloaded, n)
	}
	return data, nil
}

func (s *Session) webseedPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused || s.closed
}

func (s *Session) fetchWebseedLinearRange(ctx context.Context, client *http.Client, spec webseedSpec, start, length int64) ([]byte, error) {
	if length <= 0 {
		return nil, fmt.Errorf("invalid webseed range length %d", length)
	}
	out := make([]byte, 0, int(length))
	end := start + length
	for start < end {
		f, ok := spec.fileForOffset(start)
		if !ok {
			return nil, fmt.Errorf("offset %d is outside webseed file layout", start)
		}
		partLen := minInt64(end, f.end) - start
		part, err := s.fetchWebseedHTTPRange(ctx, client, f.url, start-f.start, partLen)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
		start += partLen
	}
	if int64(len(out)) != length {
		return nil, fmt.Errorf("assembled webseed range length %d, want %d", len(out), length)
	}
	return out, nil
}

func (spec webseedSpec) fileForOffset(offset int64) (webseedFile, bool) {
	for _, f := range spec.files {
		if offset >= f.start && offset < f.end {
			return f, true
		}
	}
	return webseedFile{}, false
}

func (s *Session) fetchWebseedHTTPRange(ctx context.Context, client *http.Client, rawURL string, start, length int64) ([]byte, error) {
	if length <= 0 || length > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("invalid HTTP range length %d", length)
	}
	refund, err := s.reserveWebseedDownload(ctx, int(length))
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, webseedRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		refund()
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))

	resp, err := client.Do(req)
	if err != nil {
		refund()
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		refund()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("range GET %s returned %s", rawURL, resp.Status)
	}
	if !contentRangeMatches(resp.Header.Get("Content-Range"), start, length) {
		refund()
		return nil, fmt.Errorf("range GET %s returned mismatched Content-Range %q", rawURL, resp.Header.Get("Content-Range"))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		refund()
		return nil, err
	}
	if int64(len(body)) != length {
		refund()
		return nil, fmt.Errorf("range GET %s returned %d bytes, want %d", rawURL, len(body), length)
	}
	return body, nil
}

func (s *Session) reserveWebseedDownload(ctx context.Context, n int) (func(), error) {
	for {
		reserved, retryAfter, refund := s.reserveDownloadWithRefund(n)
		if reserved {
			return refund, nil
		}
		if retryAfter <= 0 {
			retryAfter = 100 * time.Millisecond
		}
		timer := time.NewTimer(retryAfter)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, ctx.Err()
		}
	}
}

func contentRangeMatches(header string, start, length int64) bool {
	if header == "" {
		return true
	}
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(header, "bytes ") {
		return false
	}
	rangePart := strings.TrimPrefix(header, "bytes ")
	if slash := strings.IndexByte(rangePart, '/'); slash >= 0 {
		rangePart = rangePart[:slash]
	}
	parts := strings.SplitN(rangePart, "-", 2)
	if len(parts) != 2 {
		return false
	}
	gotStart, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return false
	}
	gotEnd, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return false
	}
	return gotStart == start && gotEnd == start+length-1
}

func (s *Session) waitWebseedIdle() bool {
	timer := time.NewTimer(webseedIdleDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}
