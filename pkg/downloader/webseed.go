package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"sainttorrent/pkg/torrent"
)

var (
	webseedHTTPClient        = http.DefaultClient
	webseedRequestTimeout    = 30 * time.Second
	webseedIdleDelay         = 500 * time.Millisecond
	webseedRetryBaseDelay    = time.Second
	webseedRetryMaxDelay     = 30 * time.Second
	webseedPausePollInterval = 50 * time.Millisecond
	errWebseedPaused         = errors.New("webseed paused")
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
	endgame       bool
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
	appendSingleFileName := !multiFile && strings.HasSuffix(base.EscapedPath(), "/")
	var offset int64
	for _, f := range files {
		var segments []string
		if multiFile || appendSingleFileName {
			segments = f.path
		}
		fileURL, ok := webseedURLForPath(base, segments)
		if !ok {
			return webseedSpec{}, false
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

func webseedURLForPath(base *url.URL, segments []string) (string, bool) {
	u := *base
	if len(segments) == 0 {
		return u.String(), true
	}

	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		parts = append(parts, url.PathEscape(segment))
	}
	if len(parts) == 0 {
		return u.String(), true
	}

	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	if escapedPath == "" {
		escapedPath = "/" + strings.Join(parts, "/")
	} else {
		escapedPath += "/" + strings.Join(parts, "/")
	}
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return "", false
	}
	u.Path = decodedPath
	u.RawPath = escapedPath
	return u.String(), true
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

	backoff := webseedRetryBaseDelay
	for {
		piece, ok, done := s.claimWebseedPiece()
		if done {
			return
		}
		if !ok {
			backoff = webseedRetryBaseDelay
			if !s.waitWebseedIdle() {
				return
			}
			continue
		}

		data, err := s.fetchWebseedPiece(s.ctx, client, spec, piece, pState)
		if errors.Is(err, errWebseedPaused) {
			s.releaseWebseedPiece(piece, nil)
			if !s.waitWebseedIdle() {
				return
			}
			continue
		}
		if err != nil {
			wrapped := fmt.Errorf("webseed %s: %w", spec.display, err)
			s.releaseWebseedPiece(piece, wrapped)
			s.recordWebseedError(peerAddr, wrapped)
			if !s.waitWebseedBackoff(backoff) {
				return
			}
			backoff = nextWebseedBackoff(backoff)
			continue
		}

		result := make(chan pieceWriteResult, 1)
		s.ensurePieceWritePool()
		select {
		case s.pieceWriteCh <- pieceWriteJob{index: piece.index, hash: piece.hash, data: data, result: result, recoverableStorageError: true}:
		case <-s.ctx.Done():
			s.releaseWebseedPiece(piece, nil)
			return
		}

		select {
		case res := <-result:
			switch res.status {
			case pieceWriteCompleted:
				backoff = webseedRetryBaseDelay
			case pieceWriteSkipped:
				s.releaseWebseedPiece(piece, nil)
				if res.err != nil {
					return
				}
				backoff = webseedRetryBaseDelay
			case pieceWriteHashFailed:
				wrapped := fmt.Errorf("webseed %s served corrupt bytes: %w", spec.display, res.err)
				s.recordWebseedError(peerAddr, wrapped)
				if !s.waitWebseedBackoff(backoff) {
					return
				}
				backoff = nextWebseedBackoff(backoff)
			case pieceWriteStorageFailed:
				wrapped := fmt.Errorf("webseed %s storage write failed: %w", spec.display, res.err)
				s.recordWebseedError(peerAddr, wrapped)
				if !s.waitWebseedBackoff(backoff) {
					return
				}
				backoff = nextWebseedBackoff(backoff)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func nextWebseedBackoff(cur time.Duration) time.Duration {
	if cur <= 0 {
		return webseedRetryBaseDelay
	}
	next := cur * 2
	if next > webseedRetryMaxDelay {
		return webseedRetryMaxDelay
	}
	return next
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
	pState.Interested = false
	pState.Active = true
	pState.AmChoking = false
	pState.LastAttempt = now
	pState.Dialable = false
	pState.Dialing = false
	pState.WebSeed = true
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
		pState.AppLimited = false
		pState.BudgetLimited = false
		pState.PieceCapLimited = false
		pState.WriterLimited = false
	}
}

func (s *Session) recordWebseedError(addr string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastErr = err
	if pState := s.Peers[addr]; pState != nil {
		pState.LastAttempt = time.Now()
		pState.DownloadSpeed = 0
	}
}

func (s *Session) claimWebseedPiece() (webseedPiece, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.paused || s.Storage == nil || s.metadataMode {
		return webseedPiece{}, false, false
	}
	if s.verifying && s.verifyFullScan {
		return webseedPiece{}, false, false
	}
	if s.isCompletedLocked() {
		return webseedPiece{}, false, true
	}

	endgame := false
	bestIdx := s.selectNeededPieceLocked(func(int64) bool { return true })
	if bestIdx == -1 && s.endgameActiveLocked() {
		bestIdx = s.selectEndgamePieceLocked(func(int64) bool { return true }, nil)
		endgame = bestIdx != -1
	}
	if bestIdx == -1 {
		return webseedPiece{}, false, false
	}
	if !endgame {
		s.PieceStates[bestIdx] = PieceDownloading
		s.removeNeededLocked(bestIdx)
	}
	return webseedPiece{
		index:         int64(bestIdx),
		hash:          s.Torrent.PieceHashes[bestIdx],
		length:        s.Storage.PieceLength(int64(bestIdx)),
		absoluteStart: int64(bestIdx) * s.Storage.PieceLengthValue(),
		endgame:       endgame,
	}, true, false
}

func (s *Session) releaseWebseedPiece(piece webseedPiece, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastErr = err
	}
	if piece.endgame {
		return
	}
	if piece.index >= 0 && piece.index < int64(len(s.PieceStates)) && s.PieceStates[piece.index] == PieceDownloading {
		s.PieceStates[piece.index] = PieceEmpty
		s.addNeededLocked(int(piece.index))
	}
}

func (s *Session) fetchWebseedPiece(ctx context.Context, client *http.Client, spec webseedSpec, piece webseedPiece, pState *PeerState) ([]byte, error) {
	if piece.length < 0 || piece.length > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("piece %d length is not addressable: %d", piece.index, piece.length)
	}
	pieceEnd := piece.absoluteStart + piece.length
	if pieceEnd < piece.absoluteStart {
		return nil, fmt.Errorf("piece %d range overflows int64", piece.index)
	}

	data := make([]byte, int(piece.length))
	var written int64
	absolute := piece.absoluteStart
	for absolute < pieceEnd {
		if s.webseedPaused() {
			return nil, errWebseedPaused
		}
		f, ok := spec.fileForOffset(absolute)
		if !ok {
			return nil, fmt.Errorf("offset %d is outside webseed file layout", absolute)
		}
		partLen := min(pieceEnd, f.end) - absolute
		if partLen <= 0 || written+partLen > int64(len(data)) {
			return nil, fmt.Errorf("invalid webseed range for piece %d", piece.index)
		}

		dst := data[int(written):int(written+partLen)]
		if err := s.fetchWebseedHTTPRange(ctx, client, f.url, absolute-f.start, dst); err != nil {
			return nil, err
		}

		absolute += partLen
		written += partLen
		s.Downloaded.Add(partLen)
		atomic.AddInt64(&pState.Downloaded, partLen)
	}
	return data, nil
}

func (s *Session) webseedPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused || s.closed
}

func (spec webseedSpec) fileForOffset(offset int64) (webseedFile, bool) {
	i := sort.Search(len(spec.files), func(i int) bool {
		return spec.files[i].end > offset
	})
	if i < len(spec.files) && offset >= spec.files[i].start && offset < spec.files[i].end {
		return spec.files[i], true
	}
	return webseedFile{}, false
}

func (s *Session) fetchWebseedHTTPRange(ctx context.Context, client *http.Client, rawURL string, start int64, dst []byte) error {
	length := int64(len(dst))
	if length <= 0 {
		return fmt.Errorf("invalid HTTP range length %d", length)
	}
	refund, err := s.reserveWebseedDownload(ctx, len(dst))
	if err != nil {
		return err
	}

	reqCtx, cancel := s.webseedRequestContext(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		refund()
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))

	resp, err := client.Do(req)
	if err != nil {
		refund()
		if s.webseedPaused() {
			return errWebseedPaused
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		refund()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("range GET %s returned %s", rawURL, resp.Status)
	}
	if !contentRangeMatches(resp.Header.Get("Content-Range"), start, length) {
		refund()
		return fmt.Errorf("range GET %s returned mismatched Content-Range %q", rawURL, resp.Header.Get("Content-Range"))
	}

	n, err := io.ReadFull(resp.Body, dst)
	if err != nil {
		refund()
		if s.webseedPaused() {
			return errWebseedPaused
		}
		return fmt.Errorf("range GET %s returned %d bytes, want %d: %w", rawURL, n, length, err)
	}
	var extra [1]byte
	extraN, extraErr := resp.Body.Read(extra[:])
	if extraN > 0 {
		refund()
		return fmt.Errorf("range GET %s returned more than %d bytes", rawURL, length)
	}
	if extraErr != nil && !errors.Is(extraErr, io.EOF) {
		refund()
		if s.webseedPaused() {
			return errWebseedPaused
		}
		return extraErr
	}
	return nil
}

func (s *Session) webseedRequestContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeoutCtx, timeoutCancel := context.WithTimeout(parent, webseedRequestTimeout)
	ctx, cancel := context.WithCancel(timeoutCtx)
	pollInterval := webseedPausePollInterval
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.webseedPaused() {
					cancel()
					return
				}
			}
		}
	}()
	return ctx, func() {
		cancel()
		timeoutCancel()
	}
}

func (s *Session) reserveWebseedDownload(ctx context.Context, n int) (func(), error) {
	if n <= 0 {
		return func() {}, nil
	}
	var refunds []func()
	refundAll := func() {
		for i := len(refunds) - 1; i >= 0; i-- {
			refunds[i]()
		}
	}

	remaining := n
	for remaining > 0 {
		if s.webseedPaused() {
			refundAll()
			return nil, errWebseedPaused
		}
		chunk := min(remaining, BlockSize)
		reserved, retryAfter, refund := s.reserveDownloadWithRefund(chunk)
		if reserved {
			if refund != nil {
				refunds = append(refunds, refund)
			}
			remaining -= chunk
			continue
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
			refundAll()
			return nil, ctx.Err()
		}
	}
	return refundAll, nil
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

func (s *Session) waitWebseedBackoff(delay time.Duration) bool {
	if delay <= 0 {
		delay = webseedRetryBaseDelay
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-s.ctx.Done():
		return false
	}
}
