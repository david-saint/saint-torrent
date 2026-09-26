package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

var (
	// webseedHTTPClient is the SSRF-hardened client shared with trackers; tests
	// may swap it, but it never falls back to http.DefaultClient.
	webseedHTTPClient     = tracker.HTTPClient
	webseedRequestTimeout = 30 * time.Second
	webseedIdleDelay      = 500 * time.Millisecond
	webseedRetryBaseDelay = time.Second
	webseedRetryMaxDelay  = 30 * time.Second
	// webseedRetireDelay parks a source that cannot serve this torrent (missing
	// file, auth, no range support) or keeps serving corrupt data. It is a long
	// sleep rather than a ban so a mirror that recovers is used again.
	webseedRetireDelay = 30 * time.Minute
	// webseedMaxRetryAfter caps how long a server's Retry-After may park a source.
	webseedMaxRetryAfter = 5 * time.Minute
	errWebseedPaused     = errors.New("webseed paused")
)

const (
	// maxWebseedWorkers bounds concurrent webseed downloads per torrent
	// (libtorrent's max_web_seed_connections is 3). Workers rotate through the
	// whole url-list, so large mirror lists are still used, while connections,
	// goroutines and piece buffers stay bounded however long the list is.
	maxWebseedWorkers = 4
	// maxWebseedSources bounds how many distinct url-list entries are kept.
	maxWebseedSources = 1024
	// webseedMaxHashFailures retires a source that served this many corrupt
	// pieces since it was last retired.
	webseedMaxHashFailures = 3
)

// webseedWorker is one webseed download slot. Session.Start runs a
// webseedLoop per worker; a torrent's workers share one pool of sources.
type webseedWorker struct {
	pool *webseedPool
}

// webseedPool is a torrent's deduplicated url-list plus the file layout
// shared by every source.
type webseedPool struct {
	files     []webseedFile
	multiFile bool

	mu      sync.Mutex
	sources []*webseedSource
	next    int // rotation cursor
}

// webseedSource is one url-list entry.
type webseedSource struct {
	base    *url.URL // validated: http(s), host, no userinfo, no fragment
	display string   // scheme://host[:port]/path, safe for errors and the UI
	host    string
	port    uint16
	// appendNames is set for a single-file torrent whose URL ends in '/': the
	// file name is appended, as for multi-file torrents.
	appendNames bool

	// Guarded by webseedPool.mu.
	busy      bool
	notBefore time.Time
	backoff   time.Duration
	hashFails int
}

type webseedFile struct {
	start int64
	end   int64
	path  []string
}

type webseedPiece struct {
	index         int64
	hash          [20]byte
	length        int64
	absoluteStart int64
	endgame       bool
}

// webseedSpecsForStart returns the webseed workers Session.Start should run:
// at most maxWebseedWorkers, all sharing one pool of the torrent's sources.
func (s *Session) webseedSpecsForStart() []webseedWorker {
	s.mu.RLock()
	if s.Torrent == nil || s.Storage == nil || s.metadataMode || len(s.Torrent.WebSeeds) == 0 || len(s.Torrent.Files) == 0 {
		s.mu.RUnlock()
		return nil
	}
	files := makeWebseedTorrentFiles(s.Torrent.Files)
	webseedURLs := append([]string(nil), s.Torrent.WebSeeds...)
	s.mu.RUnlock()

	pool := newWebseedPool(webseedURLs, files)
	if len(pool.sources) == 0 {
		return nil
	}
	workers := make([]webseedWorker, min(maxWebseedWorkers, len(pool.sources)))
	for i := range workers {
		workers[i] = webseedWorker{pool: pool}
	}
	return workers
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

// newWebseedPool builds the pool, keeping each source once by
// scheme+host+port+path (query and fragment ignored, so "?v=1", "?v=2", ...
// cannot multiply one server) and at most maxWebseedSources of them.
func newWebseedPool(rawURLs []string, files []webseedTorrentFile) *webseedPool {
	pool := &webseedPool{multiFile: len(files) != 1 || len(files[0].path) != 1}
	var offset int64
	for _, f := range files {
		pool.files = append(pool.files, webseedFile{start: offset, end: offset + f.length, path: f.path})
		offset += f.length
	}
	seen := make(map[string]struct{}, min(len(rawURLs), maxWebseedSources))
	for _, raw := range rawURLs {
		if len(pool.sources) >= maxWebseedSources {
			break
		}
		src, key, ok := buildWebseedSource(raw, pool.multiFile)
		if !ok {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		pool.sources = append(pool.sources, src)
	}
	return pool
}

// buildWebseedSource validates one url-list entry and returns it with its
// dedup key. URLs carrying credentials are rejected: Go would send them as
// Basic auth, and they would leak into errors and the UI.
func buildWebseedSource(raw string, multiFile bool) (*webseedSource, string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, "", false
	}
	base, err := url.Parse(raw)
	if err != nil || base.Hostname() == "" || base.User != nil {
		return nil, "", false
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, "", false
	}
	port := defaultURLPort(base)
	if port == 0 {
		return nil, "", false
	}
	base.Fragment, base.RawFragment = "", ""
	key := base.Scheme + "://" + net.JoinHostPort(strings.ToLower(base.Hostname()), strconv.Itoa(int(port))) + base.EscapedPath()
	return &webseedSource{
		base:        base,
		display:     redactedURL(base),
		host:        base.Hostname(),
		port:        port,
		appendNames: !multiFile && strings.HasSuffix(base.EscapedPath(), "/"),
	}, key, true
}

// redactedURL renders u as scheme://host[:port]/path, without userinfo, query
// or fragment, which can carry passwords and signed tokens.
func redactedURL(u *url.URL) string {
	c := *u
	c.User = nil
	c.RawQuery, c.ForceQuery = "", false
	c.Fragment, c.RawFragment = "", ""
	return c.String()
}

// fileURL returns the URL this source serves file f at.
func (src *webseedSource) fileURL(f webseedFile, multiFile bool) (*url.URL, bool) {
	var segments []string
	if multiFile || src.appendNames {
		segments = f.path
	}
	return webseedURLForPath(src.base, segments)
}

func webseedURLForPath(base *url.URL, segments []string) (*url.URL, bool) {
	u := *base
	if len(segments) == 0 {
		return &u, true
	}

	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		parts = append(parts, url.PathEscape(segment))
	}
	if len(parts) == 0 {
		return &u, true
	}

	escapedPath := strings.TrimRight(u.EscapedPath(), "/")
	if escapedPath == "" {
		escapedPath = "/" + strings.Join(parts, "/")
	} else {
		escapedPath += "/" + strings.Join(parts, "/")
	}
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return nil, false
	}
	u.Path = decodedPath
	u.RawPath = escapedPath
	return &u, true
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

func (p *webseedPool) fileForOffset(offset int64) (webseedFile, bool) {
	i := sort.Search(len(p.files), func(i int) bool {
		return p.files[i].end > offset
	})
	if i < len(p.files) && offset >= p.files[i].start && offset < p.files[i].end {
		return p.files[i], true
	}
	return webseedFile{}, false
}

// acquire hands out the next idle source whose rest is over, rotating through
// the list. When none is ready it returns the time the earliest resting
// source becomes ready (zero when every other source is busy).
func (p *webseedPool) acquire(now time.Time) (*webseedSource, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var wake time.Time
	n := len(p.sources)
	for k := 0; k < n; k++ {
		i := (p.next + k) % n
		src := p.sources[i]
		if src.busy {
			continue
		}
		if now.Before(src.notBefore) {
			if wake.IsZero() || src.notBefore.Before(wake) {
				wake = src.notBefore
			}
			continue
		}
		src.busy = true
		p.next = (i + 1) % n
		return src, time.Time{}
	}
	return nil, wake
}

// release returns src to the pool; no worker takes it again for rest.
func (p *webseedPool) release(src *webseedSource, rest time.Duration, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	src.busy = false
	src.notBefore = now.Add(rest)
}

// succeeded resets src's backoff after a verified piece. Only this resets it:
// an idle spell says nothing about whether a source works.
func (p *webseedPool) succeeded(src *webseedSource) {
	p.mu.Lock()
	defer p.mu.Unlock()
	src.backoff = 0
}

// restAfterError returns how long src rests after a failed range request:
// retired for a permanent failure, otherwise its growing backoff or the
// server's Retry-After, whichever is longer.
func (p *webseedPool) restAfterError(src *webseedSource, err error) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	var se *webseedStatusError
	isStatus := errors.As(err, &se)
	if isStatus && se.permanent {
		return retireWebseedLocked(src)
	}
	src.backoff = nextWebseedBackoff(src.backoff)
	if isStatus && se.retryAfter > src.backoff {
		return se.retryAfter
	}
	return src.backoff
}

// restAfterHashFailure counts a corrupt piece against src and retires it
// after webseedMaxHashFailures.
func (p *webseedPool) restAfterHashFailure(src *webseedSource) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	src.hashFails++
	if src.hashFails >= webseedMaxHashFailures {
		return retireWebseedLocked(src)
	}
	src.backoff = nextWebseedBackoff(src.backoff)
	return src.backoff
}

// restAfterLocalFailure backs src off after a failure that was not its fault
// (a storage write), without counting it towards retirement.
func (p *webseedPool) restAfterLocalFailure(src *webseedSource) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	src.backoff = nextWebseedBackoff(src.backoff)
	return src.backoff
}

func retireWebseedLocked(src *webseedSource) time.Duration {
	src.backoff = 0
	src.hashFails = 0
	return webseedRetireDelay
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

// webseedStatusError is a range request answered with an unusable status.
type webseedStatusError struct {
	url  string // redacted
	code int
	// permanent means the source cannot serve this torrent (missing file,
	// auth, no range support), so it is retired rather than retried.
	permanent bool
	// retryAfter is the server's Retry-After, capped at webseedMaxRetryAfter.
	retryAfter time.Duration
}

func (e *webseedStatusError) Error() string {
	return fmt.Sprintf("range GET %s returned %d %s", e.url, e.code, http.StatusText(e.code))
}

func newWebseedStatusError(display string, resp *http.Response, now time.Time) *webseedStatusError {
	e := &webseedStatusError{url: display, code: resp.StatusCode}
	switch resp.StatusCode {
	case http.StatusOK, // Range ignored: the server cannot serve pieces
		http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusGone, http.StatusRequestedRangeNotSatisfiable:
		e.permanent = true
	default:
		e.retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), now)
	}
	return e
}

// parseRetryAfter reads a Retry-After header (delay-seconds or HTTP-date),
// capped at webseedMaxRetryAfter; zero when absent or invalid.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	secs, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err == nil || errors.Is(err, strconv.ErrRange):
		// Out-of-range values saturate at the int64 bound of their sign.
		if secs >= int64(webseedMaxRetryAfter/time.Second) {
			return webseedMaxRetryAfter
		}
		d = time.Duration(max(secs, 0)) * time.Second
	default:
		if t, err := http.ParseTime(v); err == nil {
			d = t.Sub(now)
		}
	}
	return min(max(d, 0), webseedMaxRetryAfter)
}

func (s *Session) webseedLoop(w webseedWorker) {
	defer s.wg.Done()

	client := webseedHTTPClient
	if client == nil {
		client = tracker.HTTPClient
	}

	for {
		src, wake := w.pool.acquire(time.Now())
		if src == nil {
			if !s.waitWebseedUntil(wake) {
				return
			}
			continue
		}
		rest, done := s.runWebseedSource(client, w.pool, src)
		w.pool.release(src, rest, time.Now())
		if done {
			return
		}
	}
}

// runWebseedSource downloads pieces from src until it fails, returning how
// long src should rest before a worker uses it again. done reports that the
// worker should exit (the torrent is complete or the session is closing).
func (s *Session) runWebseedSource(client *http.Client, pool *webseedPool, src *webseedSource) (rest time.Duration, done bool) {
	peerAddr, pState := s.registerWebseedPeer(src)
	defer s.unregisterWebseedPeer(peerAddr)

	for {
		piece, ok, complete := s.claimWebseedPiece()
		if complete {
			return 0, true
		}
		if !ok {
			if !s.waitWebseedIdle() {
				return 0, true
			}
			continue
		}

		data, buf, err := s.fetchWebseedPiece(s.ctx, client, pool, src, piece, pState)
		if errors.Is(err, errWebseedPaused) {
			s.releaseWebseedPiece(piece, nil)
			if !s.waitWebseedIdle() {
				return 0, true
			}
			continue
		}
		if err != nil {
			if s.ctx.Err() != nil {
				s.releaseWebseedPiece(piece, nil)
				return 0, true
			}
			wrapped := fmt.Errorf("webseed %s: %w", src.display, err)
			s.releaseWebseedPiece(piece, wrapped)
			s.recordWebseedError(peerAddr, wrapped)
			return pool.restAfterError(src, err), false
		}

		result := make(chan pieceWriteResult, 1)
		s.ensurePieceWritePool()
		select {
		case s.pieceWriteCh <- pieceWriteJob{index: piece.index, hash: piece.hash, data: data, pieceBuf: buf, result: result, recoverableStorageError: true}:
		case <-s.ctx.Done():
			s.putPieceBuf(buf)
			s.releaseWebseedPiece(piece, nil)
			return 0, true
		}

		select {
		case res := <-result:
			switch res.status {
			case pieceWriteCompleted:
				pool.succeeded(src)
			case pieceWriteSkipped:
				s.releaseWebseedPiece(piece, nil)
				if res.err != nil {
					return 0, true
				}
			case pieceWriteHashFailed:
				wrapped := fmt.Errorf("webseed %s served corrupt bytes: %w", src.display, res.err)
				s.recordWebseedError(peerAddr, wrapped)
				return pool.restAfterHashFailure(src), false
			case pieceWriteStorageFailed:
				wrapped := fmt.Errorf("webseed %s storage write failed: %w", src.display, res.err)
				s.recordWebseedError(peerAddr, wrapped)
				return pool.restAfterLocalFailure(src), false
			}
		case <-s.ctx.Done():
			return 0, true
		}
	}
}

func (s *Session) registerWebseedPeer(src *webseedSource) (string, *PeerState) {
	addr := "webseed:" + src.display
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	pState := s.Peers[addr]
	if pState == nil {
		pState = &PeerState{}
		s.Peers[addr] = pState
	}
	pState.IP = src.host
	pState.Port = src.port
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
		s.setPieceStateLocked(bestIdx, PieceDownloading)
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
		s.setPieceStateLocked(int(piece.index), PieceEmpty)
	}
}

// fetchWebseedPiece downloads one piece from src into a buffer borrowed from
// the session's piece-buffer pool. On success the caller owns the buffer and
// hands it to the write worker, which returns it to the pool.
func (s *Session) fetchWebseedPiece(ctx context.Context, client *http.Client, pool *webseedPool, src *webseedSource, piece webseedPiece, pState *PeerState) ([]byte, *[]byte, error) {
	if piece.length < 0 || piece.length > int64(^uint(0)>>1) {
		return nil, nil, fmt.Errorf("piece %d length is not addressable: %d", piece.index, piece.length)
	}
	pieceEnd := piece.absoluteStart + piece.length
	if pieceEnd < piece.absoluteStart {
		return nil, nil, fmt.Errorf("piece %d range overflows int64", piece.index)
	}

	buf := s.getPieceBuf(piece.length)
	data := *buf
	fail := func(err error) ([]byte, *[]byte, error) {
		s.putPieceBuf(buf)
		return nil, nil, err
	}
	var written int64
	absolute := piece.absoluteStart
	for absolute < pieceEnd {
		if s.webseedPaused() {
			return fail(errWebseedPaused)
		}
		f, ok := pool.fileForOffset(absolute)
		if !ok {
			return fail(fmt.Errorf("offset %d is outside webseed file layout", absolute))
		}
		partLen := min(pieceEnd, f.end) - absolute
		if partLen <= 0 || written+partLen > int64(len(data)) {
			return fail(fmt.Errorf("invalid webseed range for piece %d", piece.index))
		}
		fileURL, ok := src.fileURL(f, pool.multiFile)
		if !ok {
			return fail(fmt.Errorf("cannot build webseed URL for piece %d", piece.index))
		}

		dst := data[int(written):int(written+partLen)]
		if err := s.fetchWebseedHTTPRange(ctx, client, fileURL, absolute-f.start, dst); err != nil {
			return fail(err)
		}

		absolute += partLen
		written += partLen
		s.Downloaded.Add(partLen)
		atomic.AddInt64(&pState.Downloaded, partLen)
	}
	return data, buf, nil
}

func (s *Session) webseedPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused || s.closed
}

// pauseStateSignal reports whether the session is currently paused or closed,
// along with the channel that will be closed on the next pause/resume
// transition. Callers block on the returned channel instead of polling
// webseedPaused on a timer.
func (s *Session) pauseStateSignal() (bool, <-chan struct{}) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused || s.closed, s.pauseStateCh
}

// webseedCause strips net/http's *url.Error wrapper, whose message repeats
// the full request URL (query tokens included).
func webseedCause(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

func (s *Session) fetchWebseedHTTPRange(ctx context.Context, client *http.Client, fileURL *url.URL, start int64, dst []byte) error {
	length := int64(len(dst))
	if length <= 0 {
		return fmt.Errorf("invalid HTTP range length %d", length)
	}
	display := redactedURL(fileURL)
	refund, err := s.reserveWebseedDownload(ctx, len(dst))
	if err != nil {
		return err
	}

	reqCtx, cancel := s.webseedRequestContext(ctx)
	defer cancel()
	req, err := tracker.NewRequest(reqCtx, tracker.PurposeWebseed, fileURL.String())
	if err != nil {
		refund()
		return fmt.Errorf("range GET %s: %w", display, webseedCause(err))
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))

	resp, err := client.Do(req)
	if err != nil {
		refund()
		if s.webseedPaused() {
			return errWebseedPaused
		}
		return fmt.Errorf("range GET %s: %w", display, webseedCause(err))
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusPartialContent:
		if cr := resp.Header.Get("Content-Range"); !contentRangeMatches(cr, start, length) {
			refund()
			return fmt.Errorf("range GET %s returned mismatched Content-Range %.64q", display, cr)
		}
	case resp.StatusCode == http.StatusOK && start == 0 && resp.ContentLength == length:
		// The server ignored Range, but the whole file is exactly the span we
		// asked for (a small file inside one piece), so the body is usable.
	default:
		refund()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1024))
		return newWebseedStatusError(display, resp, time.Now())
	}

	n, err := io.ReadFull(resp.Body, dst)
	if err != nil {
		refund()
		if s.webseedPaused() {
			return errWebseedPaused
		}
		return fmt.Errorf("range GET %s returned %d bytes, want %d: %w", display, n, length, err)
	}
	var extra [1]byte
	extraN, extraErr := resp.Body.Read(extra[:])
	if extraN > 0 {
		refund()
		return fmt.Errorf("range GET %s returned more than %d bytes", display, length)
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
	go func() {
		for {
			paused, changed := s.pauseStateSignal()
			if paused {
				cancel()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
				// Pause state changed; loop to re-check rather than polling.
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

// waitWebseedUntil sleeps until wake (webseedIdleDelay when wake is zero,
// i.e. every source is busy), returning false if the session closes first.
func (s *Session) waitWebseedUntil(wake time.Time) bool {
	delay := webseedIdleDelay
	if !wake.IsZero() {
		delay = max(time.Until(wake), time.Millisecond)
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
