package downloader

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/tracker"
)

// PieceState represents the download state of a piece.
type PieceState int

const (
	PieceEmpty       PieceState = iota
	PieceDownloading PieceState = iota
	PieceCompleted   PieceState = iota
	// PieceUnverified means resume data claims this piece is complete but it has not
	// yet been hash-checked. The picker treats it as "present" (does not re-download
	// it), but it is never advertised, served, or counted toward seeding until the
	// background verification pass promotes it to PieceCompleted.
	PieceUnverified PieceState = iota
)

// verifyGate bounds how many piece hash checks run concurrently across all sessions,
// so background verification saturates available cores without thrashing the disk.
var verifyGate = make(chan struct{}, max(1, runtime.GOMAXPROCS(0)))

// BlockSize is the standard block length for BitTorrent transfers (16 KB).
const BlockSize = 16384

// Keep enough block requests in flight to saturate high-latency peers.
const maxPendingBlockRequests = 128

const blockRequestTimeout = 20 * time.Second
const maxBlockRequestRetries = 2

// Large enough to avoid kernel socket buffers becoming the bottleneck on fast peers.
const peerSocketBufferSize = 4 * 1024 * 1024

const trackerDefaultNumWant = 200

var trackerAnnounceTimeout = 15 * time.Second

// PeerState holds per-peer state visible to the TUI.
type PeerState struct {
	IP            string
	Port          uint16
	Choked        bool
	Interested    bool
	DownloadSpeed float64 // Bytes per second
	UploadSpeed   float64 // Bytes per second
	Downloaded    int64
	Uploaded      int64
	Active        bool
	AmChoking     bool
	LastAttempt   time.Time
}

// FilePriority controls download ordering for files.
type FilePriority int

const (
	PrioritySkip   FilePriority = 0
	PriorityLow    FilePriority = 1
	PriorityNormal FilePriority = 2
	PriorityHigh   FilePriority = 3
)

// Session orchestrates the download/upload of a single torrent.
type Session struct {
	Torrent   *torrent.Torrent
	Storage   *storage.Storage
	PeerID    [20]byte
	Port      uint16
	StartTime time.Time
	AddedAt   time.Time

	mu          sync.RWMutex
	Downloaded  int64
	Uploaded    int64
	PieceStates []PieceState
	Peers       map[string]*PeerState
	activePeers map[string]*peer.Client // for sending Have messages

	lifecycleMu        sync.Mutex
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	closeOnce          sync.Once
	listener           net.Listener
	currentSpeed       float64
	lastErr            error
	statusErr          error // current blocking failure; drives Error status
	lastTrackerErr     error
	paused             bool
	pauseEpoch         uint64 // increments when active connections are closed for pause
	closed             bool
	started            bool
	resumeCh           chan struct{} // signal to wake tracker loop on resume
	trackerEvents      []string      // Queue of pending tracker events
	completedAnnounced bool
	stoppedAnnounced   bool

	// Background fast-resume verification. Pieces claimed by resume data start as
	// PieceUnverified and are hash-checked off the startup path (kicked off by Start).
	verifying         bool
	verifyStarted     bool
	verifyFullScan    bool
	verifyDone        chan struct{}
	verifyGateRelease func() // releases this session's global verification slot (once)

	OnStateChange         func()
	MagnetURI             string
	PendingFilePriorities []FilePriority

	// File selection and priorities
	FilePriorities []FilePriority

	// Bandwidth limiting
	DownloadLimiter       *RateLimiter
	UploadLimiter         *RateLimiter
	GlobalDownloadLimiter *RateLimiter
	GlobalUploadLimiter   *RateLimiter

	// Choking strategy state
	chokeTimer      *time.Ticker
	optimisticTimer *time.Ticker

	// Metadata exchange state
	metadataSize        int
	metadataBuf         []byte
	metadataPieces      []bool
	metadataCompleted   bool
	metadataMode        bool
	metadataCompletedCh chan struct{}
	DHT                 *dht.DHT
	downloadDir         string
}

// blockRequest tracks an outstanding block request sent to a peer.
type blockRequest struct {
	pieceIndex  int64
	begin       int64
	length      int64
	requested   bool
	received    bool
	requestedAt time.Time
	retries     int
}

// NewSession creates a new download session for a torrent.
func NewSession(tor *torrent.Torrent, st *storage.Storage, peerID [20]byte, port uint16, downloadDir string) (*Session, error) {
	numPieces := len(tor.PieceHashes)
	states := make([]PieceState, numPieces)

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize file priorities to Normal for all files
	priorities := make([]FilePriority, len(tor.Files))
	for i := range priorities {
		priorities[i] = PriorityNormal
	}

	metadataMode := tor.PieceLength == 0

	resolvedDir := downloadDir
	if st != nil && resolvedDir == "" {
		resolvedDir = st.BaseDir()
	}

	sess := &Session{
		Torrent:             tor,
		Storage:             st,
		PeerID:              peerID,
		Port:                port,
		StartTime:           time.Now(),
		AddedAt:             time.Now(),
		PieceStates:         states,
		Peers:               make(map[string]*PeerState),
		activePeers:         make(map[string]*peer.Client),
		ctx:                 ctx,
		cancel:              cancel,
		resumeCh:            make(chan struct{}, 1),
		FilePriorities:      priorities,
		DownloadLimiter:     NewRateLimiter(0), // unlimited by default
		UploadLimiter:       NewRateLimiter(0), // unlimited by default
		metadataMode:        metadataMode,
		metadataCompletedCh: make(chan struct{}),
		downloadDir:         resolvedDir,
		trackerEvents:       []string{"started"},
	}

	if !metadataMode {
		// Cheaply load fast-resume hints (no hashing). Actual hash verification runs in
		// the background once Start() is called, keeping startup off the hashing path.
		sess.loadResumeState()
	}

	return sess, nil
}

// loadResumeState reads the fast-resume hint and marks the pieces it claims complete
// as PieceUnverified (or schedules a full scan when there is no hint). It does no disk
// hashing, so it is cheap enough for the startup path; the actual hash verification is
// deferred to verifyResume, which Start() launches in the background.
//
// Invariant preserved from the original synchronous verifier ("never trust resume data
// blindly"): a PieceUnverified piece is never advertised in a bitfield, served to a
// peer, or counted toward seeding/completion until it has been hash-verified here.
func (s *Session) loadResumeState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Storage == nil || len(s.Torrent.PieceHashes) == 0 {
		return
	}
	s.PieceStates = make([]PieceState, len(s.Torrent.PieceHashes))

	infoHashHex := fmt.Sprintf("%x", s.Torrent.InfoHash)
	completedIndices, err := s.Storage.LoadState(infoHashHex)
	if err == nil {
		for _, idx := range completedIndices {
			if idx >= 0 && idx < len(s.PieceStates) {
				s.PieceStates[idx] = PieceUnverified
			}
		}
	} else {
		// No valid hint: leave pieces empty (a fresh torrent downloads immediately) and
		// let the background pass scan for any already-present data on disk.
		s.verifyFullScan = true
	}
	s.verifying = true
	s.verifyDone = make(chan struct{})
}

// maybeStartVerification launches the background verification goroutine exactly once.
// Idempotent; safe to call from Start() for both restored and freshly added torrents.
func (s *Session) maybeStartVerification() {
	s.mu.Lock()
	if !s.verifying || s.verifyStarted || s.closed {
		s.mu.Unlock()
		return
	}
	s.verifyStarted = true
	ctx := s.ctx
	s.mu.Unlock()

	// Deliberately NOT tracked by s.wg: a VerifyPiece read can block on slow I/O, and
	// Close()/RemoveSession() must never wait on it. The goroutine stops promptly on ctx
	// cancel (checked between pieces) and never mutates a closed session's state.
	go s.verifyResume(ctx)
}

// verifyResume hash-checks the pieces flagged by loadResumeState. On success a piece
// becomes PieceCompleted (now advertisable and seedable); on failure it returns to
// PieceEmpty so the downloader re-fetches it.
func (s *Session) verifyResume(ctx context.Context) {
	if s.runVerification(ctx) {
		s.finishVerify()
	}
	// If cancelled mid-way the session is shutting down; WaitVerified wakes on ctx.
}

func (s *Session) runVerification(ctx context.Context) bool {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return false
	}
	if s.Storage == nil || !s.verifying {
		s.mu.Unlock()
		return true
	}
	st := s.Storage
	pieceHashes := append([][20]byte(nil), s.Torrent.PieceHashes...)
	fullScan := s.verifyFullScan
	toCheck := make([]int, 0, len(s.PieceStates))
	for i, state := range s.PieceStates {
		if fullScan {
			if state == PieceEmpty {
				toCheck = append(toCheck, i)
			}
		} else if state == PieceUnverified {
			toCheck = append(toCheck, i)
		}
	}
	s.mu.Unlock()
	if len(toCheck) == 0 {
		return true
	}

	// Take one verification slot for this session, bounding concurrent hashing across all
	// sessions. Acquisition is cancellable, and the slot is reclaimed by Close() even if a
	// VerifyPiece read is wedged on slow I/O — so a closed session can never permanently
	// consume verification capacity.
	select {
	case verifyGate <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	var relOnce sync.Once
	release := func() { relOnce.Do(func() { <-verifyGate }) }
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		release()
		return false
	}
	s.verifyGateRelease = release
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.verifyGateRelease = nil
		s.mu.Unlock()
		release()
	}()

	for _, idx := range toCheck {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		ok, verifyErr := st.VerifyPiece(int64(idx), pieceHashes[idx])

		s.mu.Lock()
		if s.closed || s.Storage != st || idx >= len(s.PieceStates) {
			s.mu.Unlock()
			return false
		}
		var nowCompleted bool
		if fullScan {
			// Adopt only positively-verified pieces, and only if the downloader has not
			// already claimed the slot in the meantime.
			if verifyErr == nil && ok && s.PieceStates[idx] == PieceEmpty {
				s.PieceStates[idx] = PieceCompleted
				nowCompleted = true
			}
		} else if s.PieceStates[idx] == PieceUnverified {
			if verifyErr == nil && ok {
				s.PieceStates[idx] = PieceCompleted
				nowCompleted = true
			} else {
				s.PieceStates[idx] = PieceEmpty
			}
		}
		s.mu.Unlock()

		if nowCompleted {
			s.broadcastHave(uint32(idx))
		}
	}
	return true
}

// finishVerify clears the verifying flag, persists the verified piece set, fires the
// completion announce if the torrent is now done, and wakes WaitVerified callers.
func (s *Session) finishVerify() {
	s.mu.Lock()
	if !s.verifying {
		s.mu.Unlock()
		return
	}
	s.verifying = false
	s.verifyFullScan = false
	// Skip the state write if the session is closing so a late finish can't resurrect a
	// .state file that RemoveSession is deleting.
	if !s.closed {
		s.saveStateLocked()
	}

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	done := s.verifyDone
	s.verifyDone = nil
	s.mu.Unlock()

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
	if done != nil {
		close(done)
	}
}

// IsVerifying reports whether background fast-resume verification is still pending.
func (s *Session) IsVerifying() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.verifying
}

// WaitVerified blocks until background verification finishes or the session is closed.
// Intended for tests and the headless benchmark path.
func (s *Session) WaitVerified() {
	s.mu.RLock()
	done := s.verifyDone
	verifying := s.verifying
	ctx := s.ctx
	s.mu.RUnlock()
	if !verifying || done == nil {
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *Session) saveStateLocked() {
	if s.Storage == nil {
		return
	}
	infoHashHex := fmt.Sprintf("%x", s.Torrent.InfoHash)
	var completed []int
	for i, state := range s.PieceStates {
		// Include PieceUnverified so a quit mid-verification keeps the resume hint;
		// those pieces are re-checked (not re-downloaded) on the next start.
		if state == PieceCompleted || state == PieceUnverified {
			completed = append(completed, i)
		}
	}
	_ = s.Storage.SaveState(infoHashHex, completed)
}

// TotalPieces returns the number of pieces in the torrent.
func (s *Session) TotalPieces() int {
	return len(s.Torrent.PieceHashes)
}

// PercentComplete returns the percentage of the torrent that has been downloaded.
func (s *Session) PercentComplete() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.Storage == nil {
		return 0.0
	}

	stats := s.completionStatsLocked()
	if stats.wantedBytes == 0 {
		return 100.0
	}
	return (float64(stats.completedWantedBytes) / float64(stats.wantedBytes)) * 100.0
}

// TotalSize returns the total size of the torrent.
func (s *Session) TotalSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Storage == nil {
		return 0
	}
	return s.Storage.TotalSize()
}

// Files returns a list of files in the torrent.
func (s *Session) Files() []torrent.File {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Torrent == nil {
		return nil
	}
	return s.Torrent.Files
}

// DownloadSpeed returns the average download speed since session start.
func (s *Session) DownloadSpeed() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	elapsed := time.Since(s.StartTime).Seconds()
	if elapsed <= 0 {
		return 0.0
	}
	return float64(s.Downloaded) / elapsed
}

type byteRange struct {
	start int64
	end   int64
}

type completionStats struct {
	totalBytes            int64
	completedTotalBytes   int64
	wantedBytes           int64
	completedWantedBytes  int64
	wantedPieces          int
	completedWantedPieces int
}

func (s *Session) completionStatsLocked() completionStats {
	var stats completionStats
	if s.Storage == nil {
		return stats
	}

	stats.totalBytes = s.Storage.TotalSize()
	ranges := s.wantedFileRangesLocked()
	if len(ranges) == 0 && len(s.Torrent.Files) == 0 && stats.totalBytes > 0 {
		ranges = []byteRange{{start: 0, end: stats.totalBytes}}
	}

	for _, r := range ranges {
		if r.end > r.start {
			stats.wantedBytes += r.end - r.start
		}
	}

	for i, state := range s.PieceStates {
		pieceStart := int64(i) * s.Storage.PieceLengthValue()
		pieceLen := s.Storage.PieceLength(int64(i))
		pieceEnd := pieceStart + pieceLen
		if state == PieceCompleted {
			stats.completedTotalBytes += pieceLen
		}

		wantedOverlap := int64(0)
		for _, r := range ranges {
			overlapStart := maxInt64(pieceStart, r.start)
			overlapEnd := minInt64(pieceEnd, r.end)
			if overlapEnd > overlapStart {
				wantedOverlap += overlapEnd - overlapStart
			}
		}
		if wantedOverlap > 0 {
			stats.wantedPieces++
			if state == PieceCompleted {
				stats.completedWantedBytes += wantedOverlap
				stats.completedWantedPieces++
			}
		}
	}

	return stats
}

func (s *Session) wantedFileRangesLocked() []byteRange {
	if s.Torrent == nil {
		return nil
	}

	ranges := make([]byteRange, 0, len(s.Torrent.Files))
	var fileStart int64
	for i, f := range s.Torrent.Files {
		fileEnd := fileStart + f.Length
		priority := PriorityNormal
		if len(s.FilePriorities) > 0 && i < len(s.FilePriorities) {
			priority = s.FilePriorities[i]
		}
		if priority != PrioritySkip && fileEnd > fileStart {
			ranges = append(ranges, byteRange{start: fileStart, end: fileEnd})
		}
		fileStart = fileEnd
	}
	return ranges
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func tunePeerConn(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(peerSocketBufferSize)
	_ = tcpConn.SetWriteBuffer(peerSocketBufferSize)
}

func (s *Session) blocksInPiece(pieceIndex int64) int64 {
	if s.Storage == nil {
		return 0
	}
	length := s.Storage.PieceLength(pieceIndex)
	return (length + BlockSize - 1) / BlockSize
}

func (s *Session) markPieceCompleted(index int64) {
	s.mu.Lock()
	s.PieceStates[index] = PieceCompleted
	s.lastErr = nil
	s.statusErr = nil
	s.saveStateLocked()

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	s.mu.Unlock()

	// Notify active peers
	s.broadcastHave(uint32(index))

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
}

func (s *Session) resetProgressAfterStorageRepair(index int64) {
	s.mu.Lock()
	for i := range s.PieceStates {
		s.PieceStates[i] = PieceEmpty
	}
	s.PieceStates[index] = PieceCompleted
	s.lastErr = nil
	s.statusErr = nil
	s.saveStateLocked()

	var completedNow bool
	if !s.completedAnnounced && !s.metadataMode && s.Storage != nil {
		stats := s.completionStatsLocked()
		if stats.completedTotalBytes == stats.totalBytes && stats.totalBytes > 0 {
			s.completedAnnounced = true
			s.queueTrackerEventLocked("completed")
			completedNow = true
		}
	}
	s.mu.Unlock()

	s.broadcastHave(uint32(index))

	if completedNow {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
}

func (s *Session) broadcastHave(index uint32) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.RLock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.RUnlock()
		return
	}
	var clients []*peer.Client
	for _, client := range s.activePeers {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	for _, client := range clients {
		s.wg.Add(1)
		go func(c *peer.Client) {
			defer s.wg.Done()
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			_ = c.SendHave(index)
		}(client)
	}
}

// Start begins the download session.
// P1 FIX: Listener binds synchronously before launching goroutines,
// ensuring Port is correct before the first tracker announce.
func (s *Session) Start() {
	s.lifecycleMu.Lock()
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		s.lifecycleMu.Unlock()
		return
	}
	if s.started {
		wasPaused := s.paused
		if wasPaused {
			s.paused = false
			s.queueTrackerEventLocked("started")
			for _, pState := range s.Peers {
				if !pState.Active {
					pState.LastAttempt = time.Time{}
				}
			}
		}
		s.mu.Unlock()
		s.lifecycleMu.Unlock()
		if wasPaused {
			select {
			case s.resumeCh <- struct{}{}:
			default:
			}
		}
		return
	}
	s.started = true
	s.mu.Unlock()
	started := false
	defer func() {
		if !started {
			s.mu.Lock()
			s.started = false
			s.mu.Unlock()
			s.lifecycleMu.Unlock()
		}
	}()

	// Bind listener SYNCHRONOUSLY so Port is set before first announce
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		s.mu.Lock()
		s.lastErr = fmt.Errorf("inbound listener failed: %w", err)
		s.mu.Unlock()
		// Continue without listener — outbound connections still work
	} else {
		s.mu.Lock()
		s.lastErr = nil
		s.listener = listener
		_, portStr, parseErr := net.SplitHostPort(listener.Addr().String())
		if parseErr == nil {
			var p int
			_, _ = fmt.Sscanf(portStr, "%d", &p)
			if p > 0 && p <= 65535 {
				s.Port = uint16(p)
			}
		}
		s.mu.Unlock()
	}

	goroutineCount := 3 // tracker + speed monitor + choke loop
	if listener != nil {
		goroutineCount = 4 // + inbound listener
	}
	s.mu.RLock()
	hasDHT := s.DHT != nil
	s.mu.RUnlock()
	if hasDHT {
		goroutineCount++
	}
	s.wg.Add(goroutineCount)
	s.lifecycleMu.Unlock()
	started = true
	go s.trackerLoop()
	go s.speedMonitorLoop()
	go s.chokeLoop()
	if listener != nil {
		go s.inboundListenerLoop()
	}
	if hasDHT {
		go s.dhtLoop()
	}

	// Kick off background fast-resume verification (no-op if nothing to verify).
	s.maybeStartVerification()
}

// Close shuts down the session and waits for its lifecycle goroutines (tracker, peer,
// choke, listener, and DHT loops) to exit. Background piece verification is intentionally
// NOT awaited — a VerifyPiece read can be wedged on slow I/O — but its global verification
// slot is reclaimed here so capacity is never permanently lost, and it stops mutating the
// session once s.closed is set.
func (s *Session) Close() {
	s.lifecycleMu.Lock()
	var gateRelease func()
	var verifyDone chan struct{}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		wasStarted := s.started
		s.mu.Unlock()
		if wasStarted {
			s.announceStopped()
		}
		if s.cancel != nil {
			s.cancel()
		}

		s.mu.Lock()
		s.closed = true
		s.verifying = false
		s.verifyFullScan = false
		verifyDone = s.verifyDone
		s.verifyDone = nil
		gateRelease = s.verifyGateRelease
		s.verifyGateRelease = nil
		if s.listener != nil {
			s.listener.Close()
			s.listener = nil
		}
		for _, client := range s.activePeers {
			_ = client.Conn.Close()
		}
		if s.chokeTimer != nil {
			s.chokeTimer.Stop()
		}
		if s.optimisticTimer != nil {
			s.optimisticTimer.Stop()
		}
		s.mu.Unlock()
	})
	s.lifecycleMu.Unlock()

	// Reclaim a verification slot held by a wedged VerifyPiece (outside any lock).
	if gateRelease != nil {
		gateRelease()
	}
	if verifyDone != nil {
		close(verifyDone)
	}

	s.wg.Wait()
}

// trackerLoop handles periodic tracker announces.
// P1 FIX: Resume uses resumeCh signal instead of spawning untracked goroutines.
func (s *Session) trackerLoop() {
	defer s.wg.Done()
	var nextInterval time.Duration = 0 // announce immediately on start

	for {
		s.mu.RLock()
		paused := s.paused
		hasEvents := len(s.trackerEvents) > 0
		s.mu.RUnlock()

		var interval int
		if !paused || hasEvents {
			interval = s.announceAndConnect()
		}
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		s.mu.RLock()
		hasMoreEvents := len(s.trackerEvents) > 0
		s.mu.RUnlock()

		if interval == 0 && hasMoreEvents {
			// Announce failed, back off to prevent spamming trackers
			nextInterval = 15 * time.Second
		} else if hasMoreEvents {
			// Flush transition events quickly
			nextInterval = 100 * time.Millisecond
		} else if interval > 0 {
			nextInterval = time.Duration(interval) * time.Second
		} else {
			nextInterval = 30 * time.Second
		}

		if !hasMoreEvents {
			if nextInterval < 5*time.Second {
				nextInterval = 5 * time.Second
			}
			if nextInterval > 1*time.Hour {
				nextInterval = 1 * time.Hour
			}
		}

		timer := time.NewTimer(nextInterval)
		select {
		case <-timer.C:
		case <-s.resumeCh:
			// Event triggered — announce immediately
			timer.Stop()
		case <-s.ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (s *Session) speedMonitorLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastGlobalDownloaded int64
	lastPeerDownloaded := make(map[string]int64)
	lastPeerUploaded := make(map[string]int64)

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.paused {
				s.currentSpeed = 0
				for _, pState := range s.Peers {
					pState.DownloadSpeed = 0
					pState.UploadSpeed = 0
				}
				s.mu.Unlock()
				continue
			}

			globalDiff := s.Downloaded - lastGlobalDownloaded
			s.currentSpeed = float64(globalDiff)
			lastGlobalDownloaded = s.Downloaded

			for addr, pState := range s.Peers {
				if pState.Active {
					lastVal := lastPeerDownloaded[addr]
					peerDiff := pState.Downloaded - lastVal
					pState.DownloadSpeed = float64(peerDiff)
					lastPeerDownloaded[addr] = pState.Downloaded

					lastUploaded := lastPeerUploaded[addr]
					uploadDiff := pState.Uploaded - lastUploaded
					pState.UploadSpeed = float64(uploadDiff)
					lastPeerUploaded[addr] = pState.Uploaded
				} else {
					delete(lastPeerDownloaded, addr)
					delete(lastPeerUploaded, addr)
				}
			}
			s.mu.Unlock()

		case <-s.ctx.Done():
			s.mu.Lock()
			s.currentSpeed = 0
			for _, pState := range s.Peers {
				pState.DownloadSpeed = 0
				pState.UploadSpeed = 0
			}
			s.mu.Unlock()
			return
		}
	}
}

type trackerAnnounceResult struct {
	peers    []tracker.Peer
	interval int
	err      error
}

func announceTracker(ctx context.Context, tr string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string, timeout time.Duration) trackerAnnounceResult {
	announceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if bytes.HasPrefix([]byte(tr), []byte("udp")) {
		resp, err := tracker.UDPAnnounce(announceCtx, tr, infoHash, peerID, port, uploaded, downloaded, left, event, trackerDefaultNumWant)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}
		return trackerAnnounceResult{peers: resp.Peers, interval: resp.Interval}
	}

	if bytes.HasPrefix([]byte(tr), []byte("http")) {
		u, err := tracker.BuildTrackerURL(tr, infoHash, peerID, port, uploaded, downloaded, left, true, event, trackerDefaultNumWant)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		req, err := http.NewRequestWithContext(announceCtx, "GET", u, nil)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		client := &http.Client{Timeout: timeout}
		resp, err := client.Do(req)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, resp.Body)
		resp.Body.Close()
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		trackerResp, err := tracker.ParseTrackerResponse(buf.Bytes())
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		return trackerAnnounceResult{peers: trackerResp.Peers, interval: trackerResp.Interval}
	}

	return trackerAnnounceResult{err: fmt.Errorf("unsupported tracker scheme: %s", tr)}
}

func (s *Session) announceAndConnect() int {
	s.mu.Lock()
	trackers := append([]string(nil), s.Torrent.Trackers...)
	if len(trackers) == 0 {
		// No trackers configured: consume all queued events as completed/success
		if len(s.trackerEvents) > 0 {
			for _, ev := range s.trackerEvents {
				if ev == "stopped" {
					s.stoppedAnnounced = true
				} else if ev == "started" {
					s.stoppedAnnounced = false
				}
			}
			s.trackerEvents = nil
		}
		s.mu.Unlock()
		return 0
	}

	var downloaded, left int64
	if s.metadataMode || s.Storage == nil || len(s.PieceStates) == 0 {
		left = 1
	} else {
		stats := s.completionStatsLocked()
		downloaded = stats.completedTotalBytes
		left = stats.totalBytes - stats.completedTotalBytes
		if left < 0 {
			left = 0
		}
	}
	port := s.Port
	uploaded := s.Uploaded
	infoHash := s.Torrent.InfoHash
	peerID := s.PeerID
	var event string
	if len(s.trackerEvents) > 0 {
		event = s.trackerEvents[0]
		s.trackerEvents = s.trackerEvents[1:]
	}
	paused := s.paused
	s.mu.Unlock()

	var peers []tracker.Peer
	var interval int
	var trackerErr error

	results := make(chan trackerAnnounceResult, len(trackers))
	for _, tr := range trackers {
		trackerURL := tr
		go func() {
			results <- announceTracker(s.ctx, trackerURL, infoHash, peerID, port, uploaded, downloaded, left, event, trackerAnnounceTimeout)
		}()
	}

	seenPeers := make(map[string]bool)
	trackerSuccess := false
	for range trackers {
		result := <-results
		if result.err != nil {
			if !trackerSuccess {
				trackerErr = result.err
			}
			continue
		}

		trackerSuccess = true
		trackerErr = nil
		if result.interval > 0 && (interval == 0 || result.interval < interval) {
			interval = result.interval
		}
		for _, p := range result.peers {
			if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
				continue
			}
			peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
			if seenPeers[peerAddr] {
				continue
			}
			seenPeers[peerAddr] = true
			peers = append(peers, p)
		}
	}

	s.mu.Lock()
	s.lastTrackerErr = trackerErr
	if trackerErr != nil {
		if event != "" {
			s.trackerEvents = append([]string{event}, s.trackerEvents...)
			if event == "stopped" {
				s.stoppedAnnounced = false
			}
		}
	} else {
		if event == "stopped" {
			s.stoppedAnnounced = true
		} else if event == "started" {
			s.stoppedAnnounced = false
		}
	}
	s.mu.Unlock()

	if trackerErr != nil {
		return 0
	}

	if paused {
		return interval
	}

	// Connect to new peers
	for _, p := range peers {
		if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
			continue
		}
		peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		s.mu.RLock()
		pState, exists := s.Peers[peerAddr]
		var shouldDial bool
		if !exists {
			shouldDial = true
		} else if !pState.Active && time.Since(pState.LastAttempt) > 60*time.Second {
			shouldDial = true
		}
		s.mu.RUnlock()

		if shouldDial {
			s.mu.Lock()
			if !s.closed && !s.paused {
				if !exists {
					s.Peers[peerAddr] = &PeerState{
						IP:          p.IP.String(),
						Port:        p.Port,
						Choked:      true,
						Active:      false,
						AmChoking:   true,
						LastAttempt: time.Now(),
					}
				} else {
					s.Peers[peerAddr].LastAttempt = time.Now()
				}
				s.wg.Add(1)
				go func(tp tracker.Peer) {
					defer s.wg.Done()
					s.connectToPeer(tp)
				}(p)
			}
			s.mu.Unlock()
		}
	}

	return interval
}

// connectToPeer dials a peer and runs the message loop.
// P2 FIX: Uses DialContext for context-aware cancellation.
func (s *Session) connectToPeer(p tracker.Peer) {
	peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)

	var dialErr error
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(s.ctx, "tcp", peerAddr)
	if err != nil {
		dialErr = err
	}

	if dialErr != nil {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	defer conn.Close()
	tunePeerConn(conn)

	// Spawn context monitor to interrupt immediately on shutdown
	doneCh := make(chan struct{})
	monitorDone := make(chan struct{})
	defer func() {
		close(doneCh)
		<-monitorDone
	}()
	go func() {
		defer close(monitorDone)
		select {
		case <-s.ctx.Done():
			_ = conn.Close()
		case <-doneCh:
		}
	}()

	// Handshake with deadline
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	handshake, err := client.Handshake()
	if err != nil {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
		return
	}
	_ = conn.SetDeadline(time.Time{}) // clear deadline

	if handshake.InfoHash != s.Torrent.InfoHash {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok {
			ps.Active = false
			ps.LastAttempt = time.Now()
		}
		s.mu.Unlock()
		return
	}

	s.mu.Lock()
	if ps, ok := s.Peers[peerAddr]; ok {
		ps.LastAttempt = time.Now()
	}
	s.mu.Unlock()

	s.runPeerMessageLoop(client, conn, peerAddr, p.IP.String(), p.Port, handshake.Reserved)
}

// inboundListenerLoop accepts incoming peer connections on the already-bound listener.
func (s *Session) inboundListenerLoop() {
	defer s.wg.Done()

	s.mu.RLock()
	listener := s.listener
	s.mu.RUnlock()

	if listener == nil {
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.mu.Lock()
		if s.closed {
			conn.Close()
			s.mu.Unlock()
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleIncomingConnection(c)
		}(conn)
		s.mu.Unlock()
	}
}

func (s *Session) handleIncomingConnection(conn net.Conn) {
	defer conn.Close()
	tunePeerConn(conn)

	s.mu.RLock()
	paused := s.paused
	s.mu.RUnlock()
	if paused {
		return
	}

	// Spawn context monitor to interrupt immediately on shutdown
	doneCh := make(chan struct{})
	monitorDone := make(chan struct{})
	defer func() {
		close(doneCh)
		<-monitorDone
	}()
	go func() {
		defer close(monitorDone)
		select {
		case <-s.ctx.Done():
			_ = conn.Close()
		case <-doneCh:
		}
	}()

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	handshake, err := peer.ParseHandshake(conn)
	if err != nil {
		return
	}

	if handshake.InfoHash != s.Torrent.InfoHash {
		return
	}

	respHs := &peer.Handshake{
		Pstr:     "BitTorrent protocol",
		InfoHash: s.Torrent.InfoHash,
		PeerID:   s.PeerID,
	}
	respHs.Reserved[5] = 0x10 // Support extension protocol (BEP 10)
	_, err = conn.Write(respHs.Serialize())
	if err != nil {
		return
	}

	_ = conn.SetDeadline(time.Time{})

	client := peer.NewClient(conn, s.Torrent.InfoHash, s.PeerID)
	peerAddr := conn.RemoteAddr().String()
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return
	}
	var portVal int
	_, _ = fmt.Sscanf(portStr, "%d", &portVal)
	if portVal <= 0 || portVal > 65535 {
		return
	}

	s.runPeerMessageLoop(client, conn, peerAddr, host, uint16(portVal), handshake.Reserved)
}

// isPieceWanted checks if a piece should be downloaded based on file selection.
func (s *Session) isPieceWanted(pieceIndex int64) bool {
	// If no file priorities set, all pieces are wanted
	if len(s.FilePriorities) == 0 {
		return true
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	// Check if any file overlapping this piece has non-skip priority
	for i, f := range s.Torrent.Files {
		if i >= len(s.FilePriorities) {
			return true // default to wanted
		}
		if s.FilePriorities[i] == PrioritySkip {
			continue
		}

		// Calculate file boundaries in the torrent
		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		// Check overlap
		if pieceStart < fileEnd && pieceEnd > fileStart {
			return true
		}
	}
	return false
}

// piecePriority returns the highest priority among files that overlap this piece.
func (s *Session) piecePriority(pieceIndex int64) FilePriority {
	if len(s.FilePriorities) == 0 {
		return PriorityNormal
	}

	pieceStart := pieceIndex * s.Storage.PieceLengthValue()
	pieceEnd := pieceStart + s.Storage.PieceLength(pieceIndex)

	maxPri := PrioritySkip
	for i, f := range s.Torrent.Files {
		if i >= len(s.FilePriorities) {
			break
		}
		var fileStart int64
		for j := 0; j < i; j++ {
			fileStart += s.Torrent.Files[j].Length
		}
		fileEnd := fileStart + f.Length

		if pieceStart < fileEnd && pieceEnd > fileStart {
			if s.FilePriorities[i] > maxPri {
				maxPri = s.FilePriorities[i]
			}
		}
	}
	return maxPri
}

func (s *Session) runPeerMessageLoop(client *peer.Client, conn net.Conn, peerAddr string, ip string, port uint16, peerReserved [8]byte) {
	s.mu.Lock()
	if s.paused || s.closed {
		s.mu.Unlock()
		return
	}
	connectionPauseEpoch := s.pauseEpoch
	pState, ok := s.Peers[peerAddr]
	if !ok {
		pState = &PeerState{
			IP:          ip,
			Port:        port,
			Choked:      true,
			Active:      false,
			AmChoking:   true,
			LastAttempt: time.Now(),
		}
		s.Peers[peerAddr] = pState
	}
	pState.Active = true
	s.activePeers[peerAddr] = client
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		reconnectAfterResume := false
		if activeClient, active := s.activePeers[peerAddr]; active && activeClient == client {
			if ps, ok := s.Peers[peerAddr]; ok {
				ps.Active = false
				ps.Choked = true
				if s.pauseEpoch != connectionPauseEpoch && !s.paused && !s.closed {
					ps.LastAttempt = time.Time{}
					reconnectAfterResume = true
				} else {
					ps.LastAttempt = time.Now()
				}
			}
			delete(s.activePeers, peerAddr)
		}
		s.mu.Unlock()

		if reconnectAfterResume {
			s.AddPeerFromDiscovery(peerAddr)
		}
	}()

	s.mu.RLock()
	inMeta := s.metadataMode
	numPieces := len(s.PieceStates)
	s.mu.RUnlock()

	var initializedPeersAndBitfield bool = false

	if !inMeta {
		s.mu.RLock()
		bf := make([]byte, (numPieces+7)/8)
		hasAny := false
		for i, state := range s.PieceStates {
			if state == PieceCompleted {
				bf[i/8] |= 1 << (7 - (i % 8))
				hasAny = true
			}
		}
		s.mu.RUnlock()

		if hasAny {
			_ = client.SendBitfield(bf)
		}
		_ = client.SendInterested()
		initializedPeersAndBitfield = true
	}

	// Send extension handshake if peer supports extensions (BEP 10)
	if peerReserved[5]&0x10 != 0 {
		s.mu.RLock()
		infoLen := len(s.Torrent.InfoBytes)
		s.mu.RUnlock()
		// Local ut_metadata ID is 1
		_ = client.SendExtHandshake(1, infoLen)
	}

	// Read peer wire loop
	var peerBitfield []byte
	if !inMeta {
		peerBitfield = make([]byte, (numPieces+7)/8)
	}

	type activeDownload struct {
		pieceIndex     int64
		hash           [20]byte
		length         int64
		blocks         [][]byte                // actual block data, nil if not received
		pending        map[int64]*blockRequest // begin offset -> request
		blocksReceived int64
	}
	var currentDownload *activeDownload

	var peerUtMetadataID int = -1

	// Helper: check if peer has piece
	hasPiece := func(index int64) bool {
		byteIndex := index / 8
		bitIndex := index % 8
		if byteIndex >= int64(len(peerBitfield)) {
			return false
		}
		return (peerBitfield[byteIndex] & (1 << (7 - bitIndex))) != 0
	}

	sendRequests := func() {
		s.mu.RLock()
		paused := s.paused
		choked := pState.Choked
		s.mu.RUnlock()
		if paused || currentDownload == nil || choked {
			return
		}
		dl := currentDownload
		now := time.Now()
		numBlocks := s.blocksInPiece(dl.pieceIndex)
		pendingCount := 0
		for _, req := range dl.pending {
			if req.requested && !req.received {
				if now.Sub(req.requestedAt) >= blockRequestTimeout {
					if req.retries >= maxBlockRequestRetries {
						s.mu.Lock()
						s.lastErr = fmt.Errorf("timed out downloading piece %d", dl.pieceIndex)
						s.PieceStates[dl.pieceIndex] = PieceEmpty
						s.mu.Unlock()
						currentDownload = nil
						_ = conn.Close()
						return
					}
					req.requested = false
					req.retries++
					continue
				}
				pendingCount++
			}
		}

		for pendingCount < maxPendingBlockRequests {
			// Find next block to request
			var nextBegin int64 = -1
			for b := int64(0); b < numBlocks; b++ {
				begin := b * BlockSize
				req, exists := dl.pending[begin]
				if !exists {
					// New block — create request
					blockLen := int64(BlockSize)
					pieceLen := s.Storage.PieceLength(dl.pieceIndex)
					if begin+blockLen > pieceLen {
						blockLen = pieceLen - begin
					}
					dl.pending[begin] = &blockRequest{
						pieceIndex:  dl.pieceIndex,
						begin:       begin,
						length:      blockLen,
						requested:   true,
						received:    false,
						requestedAt: now,
					}
					nextBegin = begin
					break
				} else if !req.requested && !req.received {
					req.requested = true
					req.requestedAt = now
					nextBegin = begin
					break
				}
			}

			if nextBegin == -1 {
				break // All blocks requested or received
			}

			req := dl.pending[nextBegin]
			// Apply download rate limiting
			if err := s.DownloadLimiter.Wait(s.ctx, int(req.length)); err != nil {
				break
			}
			if s.GlobalDownloadLimiter != nil {
				if err := s.GlobalDownloadLimiter.Wait(s.ctx, int(req.length)); err != nil {
					break
				}
			}

			err := client.SendRequest(uint32(dl.pieceIndex), uint32(nextBegin), uint32(req.length))
			if err != nil {
				req.requested = false
				break
			}
			req.requestedAt = now
			pendingCount++
		}
	}

	// Reduced read deadline for faster shutdown (P2 fix)
	for {
		s.mu.RLock()
		paused := s.paused
		s.mu.RUnlock()
		if paused {
			break
		}

		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msg, err := client.ReadMessage()
		if err != nil {
			break
		}

		if msg == nil {
			// Keep alive
			continue
		}

		s.mu.RLock()
		inMetaNow := s.metadataMode
		numPiecesNow := len(s.PieceStates)
		s.mu.RUnlock()

		if !inMetaNow && !initializedPeersAndBitfield {
			// Initialize now that metadata is downloaded!
			s.mu.RLock()
			bf := make([]byte, (numPiecesNow+7)/8)
			hasAny := false
			for i, state := range s.PieceStates {
				if state == PieceCompleted {
					bf[i/8] |= 1 << (7 - (i % 8))
					hasAny = true
				}
			}
			s.mu.RUnlock()

			if hasAny {
				_ = client.SendBitfield(bf)
			}
			_ = client.SendInterested()

			peerBitfield = make([]byte, (numPiecesNow+7)/8)
			initializedPeersAndBitfield = true
		}

		switch msg.ID {
		case peer.MsgExtended:
			if len(msg.Payload) < 2 {
				continue
			}
			extMsgID := msg.Payload[0]
			payloadBytes := msg.Payload[1:]

			if extMsgID == peer.ExtHandshake {
				hs, err := peer.ParseExtensionHandshake(payloadBytes)
				if err == nil {
					if utID, ok := hs.Extensions["ut_metadata"]; ok {
						peerUtMetadataID = utID

						// If we are in metadata mode, request the metadata blocks
						s.mu.Lock()
						inMetaMode := s.metadataMode
						metadataComp := s.metadataCompleted
						sz := s.metadataSize
						s.mu.Unlock()

						if inMetaMode && !metadataComp {
							if hs.MetadataSize <= 0 || hs.MetadataSize > peer.MaxMetadataSize {
								s.mu.Lock()
								s.lastErr = fmt.Errorf("invalid metadata size from peer: %d", hs.MetadataSize)
								s.mu.Unlock()
								continue
							}
							s.mu.Lock()
							if s.metadataSize == 0 {
								s.metadataSize = hs.MetadataSize
								s.metadataBuf = make([]byte, hs.MetadataSize)
								numBlocks := (hs.MetadataSize + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
								s.metadataPieces = make([]bool, numBlocks)
								sz = hs.MetadataSize
							} else if s.metadataSize != hs.MetadataSize {
								s.mu.Unlock()
								continue
							}
							s.mu.Unlock()

							if sz > 0 {
								numBlocks := (sz + peer.MetadataBlockSize - 1) / peer.MetadataBlockSize
								for i := 0; i < numBlocks; i++ {
									s.mu.Lock()
									alreadyGot := s.metadataPieces[i]
									s.mu.Unlock()
									if !alreadyGot {
										_ = client.SendMetadataRequest(byte(peerUtMetadataID), i)
									}
								}
							}
						}
					}
				}
			} else if extMsgID == 1 { // our local ID for ut_metadata is 1
				metaMsg, err := peer.ParseMetadataMessage(payloadBytes)
				if err == nil {
					switch metaMsg.MsgType {
					case peer.MetadataRequest:
						s.mu.Lock()
						inMetaMode := s.metadataMode
						infoBytes := s.Torrent.InfoBytes
						s.mu.Unlock()

						if !inMetaMode && len(infoBytes) > 0 {
							offset := int64(metaMsg.Piece) * peer.MetadataBlockSize
							if offset >= 0 && offset < int64(len(infoBytes)) {
								blockLen := int64(peer.MetadataBlockSize)
								if offset+blockLen > int64(len(infoBytes)) {
									blockLen = int64(len(infoBytes)) - offset
								}
								blockData := infoBytes[offset : offset+blockLen]
								if peerUtMetadataID != -1 {
									_ = client.SendMetadataData(byte(peerUtMetadataID), metaMsg.Piece, len(infoBytes), blockData)
								}
							} else {
								if peerUtMetadataID != -1 {
									_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
								}
							}
						} else {
							if peerUtMetadataID != -1 {
								_ = client.SendMetadataReject(byte(peerUtMetadataID), metaMsg.Piece)
							}
						}

					case peer.MetadataData:
						s.mu.Lock()
						if s.metadataMode && !s.metadataCompleted && s.metadataSize > 0 && metaMsg.Piece >= 0 && metaMsg.Piece < len(s.metadataPieces) && !s.metadataPieces[metaMsg.Piece] {
							if metaMsg.TotalSize > 0 && metaMsg.TotalSize != s.metadataSize {
								s.mu.Unlock()
								continue
							}
							offset := metaMsg.Piece * peer.MetadataBlockSize
							expectedLen := peer.MetadataBlockSize
							if offset+expectedLen > s.metadataSize {
								expectedLen = s.metadataSize - offset
							}
							if expectedLen > 0 && len(metaMsg.Data) == expectedLen && offset+len(metaMsg.Data) <= len(s.metadataBuf) {
								copy(s.metadataBuf[offset:], metaMsg.Data)
								s.metadataPieces[metaMsg.Piece] = true

								allCompleted := true
								for _, done := range s.metadataPieces {
									if !done {
										allCompleted = false
										break
									}
								}

								if allCompleted {
									bufCopy := make([]byte, len(s.metadataBuf))
									copy(bufCopy, s.metadataBuf)
									s.mu.Unlock()

									err := s.onMetadataDownloaded(bufCopy)
									if err != nil {
										s.mu.Lock()
										s.lastErr = err
										s.mu.Unlock()
									}
								} else {
									s.mu.Unlock()
								}
							} else {
								s.mu.Unlock()
							}
						} else {
							s.mu.Unlock()
						}

					case peer.MetadataReject:
						// Peer rejected metadata piece request, nothing to do.
					}
				}
			}

		case peer.MsgChoke:
			s.mu.Lock()
			pState.Choked = true
			s.mu.Unlock()
			if currentDownload != nil {
				s.mu.Lock()
				s.PieceStates[currentDownload.pieceIndex] = PieceEmpty
				s.mu.Unlock()
				currentDownload = nil
			}

		case peer.MsgUnchoke:
			s.mu.Lock()
			pState.Choked = false
			s.mu.Unlock()

		case peer.MsgInterested:
			s.mu.Lock()
			pState.Interested = true
			s.mu.Unlock()

		case peer.MsgNotInterested:
			s.mu.Lock()
			pState.Interested = false
			s.mu.Unlock()

		case peer.MsgHave:
			if len(msg.Payload) == 4 {
				index := binary.BigEndian.Uint32(msg.Payload)
				if numPiecesNow < 0 || uint64(numPiecesNow) > uint64(^uint32(0)) {
					continue
				}
				if index >= uint32(numPiecesNow) {
					continue
				}
				byteIndex := index / 8
				bitIndex := index % 8
				if byteIndex >= uint32(len(peerBitfield)) {
					continue
				}
				peerBitfield[byteIndex] |= 1 << (7 - bitIndex)
			}

		case peer.MsgBitfield:
			expectedLen := (numPiecesNow + 7) / 8
			if expectedLen == 0 || len(msg.Payload) != expectedLen {
				continue
			}
			if len(peerBitfield) != expectedLen {
				peerBitfield = make([]byte, expectedLen)
			}
			copy(peerBitfield, msg.Payload)

		case peer.MsgPiece:
			if len(msg.Payload) < 8 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
			blockData := msg.Payload[8:]

			// P1 FIX: Validate against outstanding requests
			if currentDownload == nil || currentDownload.pieceIndex != index {
				continue // Not our current piece, discard
			}
			dl := currentDownload

			// Validate begin is block-aligned
			if begin%BlockSize != 0 {
				continue
			}

			// Validate this block was requested and not already received
			req, exists := dl.pending[begin]
			if !exists || !req.requested || req.received {
				continue // Unsolicited or duplicate block
			}

			// Validate block length matches expected
			if int64(len(blockData)) != req.length {
				continue
			}

			// Accept the block
			blockIndex := begin / BlockSize
			if blockIndex < int64(len(dl.blocks)) {
				dl.blocks[blockIndex] = blockData
				req.received = true
				dl.blocksReceived++

				s.mu.Lock()
				s.Downloaded += int64(len(blockData))
				pState.Downloaded += int64(len(blockData))
				s.mu.Unlock()

				// Check if piece is complete
				numBlocks := s.blocksInPiece(dl.pieceIndex)
				if dl.blocksReceived == numBlocks {
					pieceData := make([]byte, dl.length)
					var offset int64
					validPiece := true
					for b := int64(0); b < numBlocks; b++ {
						block := dl.blocks[b]
						if block == nil || offset+int64(len(block)) > int64(len(pieceData)) {
							validPiece = false
							break
						}
						copy(pieceData[offset:], block)
						offset += int64(len(block))
					}

					disconnectPeer := false
					if validPiece && offset == int64(len(pieceData)) && sha1.Sum(pieceData) == dl.hash {
						if err := s.Storage.WriteBlock(dl.pieceIndex, 0, pieceData); err == nil || err == storage.ErrFileRepaired {
							if err == storage.ErrFileRepaired {
								s.mu.Lock()
								s.lastErr = fmt.Errorf("download file was missing or resized; recreated target file")
								s.mu.Unlock()
								s.resetProgressAfterStorageRepair(dl.pieceIndex)
							} else {
								s.markPieceCompleted(dl.pieceIndex)
							}
						} else {
							s.mu.Lock()
							s.lastErr = err
							s.statusErr = err
							s.PieceStates[dl.pieceIndex] = PieceEmpty
							s.mu.Unlock()
						}
					} else {
						// Do not immediately retry corrupt data from the same peer.
						s.mu.Lock()
						s.lastErr = fmt.Errorf("piece %d failed hash verification", dl.pieceIndex)
						s.PieceStates[dl.pieceIndex] = PieceEmpty
						s.mu.Unlock()
						disconnectPeer = true
					}

					currentDownload = nil
					if disconnectPeer {
						return
					}
				}
			}

		case peer.MsgRequest:
			if len(msg.Payload) == 12 {
				index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
				begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
				length := int64(binary.BigEndian.Uint32(msg.Payload[8:12]))

				s.mu.RLock()
				paused := s.paused
				numPieces := len(s.PieceStates)
				amChoking := pState.AmChoking
				var isCompleted bool
				var pieceLen int64
				if index >= 0 && index < int64(numPieces) {
					isCompleted = s.PieceStates[index] == PieceCompleted
					pieceLen = s.Storage.PieceLength(index)
				}
				s.mu.RUnlock()

				if paused || amChoking {
					continue
				}

				if isCompleted && length > 0 && length <= BlockSize && begin >= 0 && begin+length <= pieceLen {
					// Apply upload rate limiting
					if err := s.UploadLimiter.Wait(s.ctx, int(length)); err != nil {
						continue
					}
					if s.GlobalUploadLimiter != nil {
						if err := s.GlobalUploadLimiter.Wait(s.ctx, int(length)); err != nil {
							continue
						}
					}
					buf := make([]byte, length)
					_, err := s.Storage.ReadBlock(index, begin, buf)
					if err == nil {
						_ = client.SendPiece(uint32(index), uint32(begin), buf)
						s.mu.Lock()
						s.Uploaded += length
						pState.Uploaded += length
						s.mu.Unlock()
					}
				}
			}
		}

		// If unchoked and need work, pick next available piece that the peer has
		s.mu.Lock()
		paused = s.paused
		choked := pState.Choked
		s.mu.Unlock()

		if !paused && !choked && currentDownload == nil {
			// Find an empty piece that the peer has, respecting priority ordering
			type candidate struct {
				index    int
				priority FilePriority
			}
			var candidates []candidate

			s.mu.Lock()
			for i, state := range s.PieceStates {
				if state == PieceEmpty && hasPiece(int64(i)) && s.isPieceWanted(int64(i)) {
					candidates = append(candidates, candidate{
						index:    i,
						priority: s.piecePriority(int64(i)),
					})
				}
			}

			// Sort by priority (highest first)
			sort.Slice(candidates, func(a, b int) bool {
				return candidates[a].priority > candidates[b].priority
			})

			if len(candidates) > 0 {
				chosen := candidates[0].index
				s.PieceStates[chosen] = PieceDownloading
				s.mu.Unlock()

				numBlocks := s.blocksInPiece(int64(chosen))
				currentDownload = &activeDownload{
					pieceIndex:     int64(chosen),
					hash:           s.Torrent.PieceHashes[chosen],
					length:         s.Storage.PieceLength(int64(chosen)),
					blocks:         make([][]byte, numBlocks),
					pending:        make(map[int64]*blockRequest),
					blocksReceived: 0,
				}
			} else {
				s.mu.Unlock()
			}
		}

		// Send requests if we have an active piece download
		if currentDownload != nil {
			sendRequests()
		}
	}

	// If we disconnected while holding a piece, return it to empty
	if currentDownload != nil {
		s.mu.Lock()
		s.PieceStates[currentDownload.pieceIndex] = PieceEmpty
		s.mu.Unlock()
	}
}

// GetActivePeers returns a slice of active peer states for TUI updates.
func (s *Session) GetActivePeers() []PeerState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []PeerState
	for _, p := range s.Peers {
		if p.Active {
			list = append(list, *p)
		}
	}
	return list
}

// GetPieceStates returns a copy of the current piece states.
func (s *Session) GetPieceStates() []PieceState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	statesCopy := make([]PieceState, len(s.PieceStates))
	copy(statesCopy, s.PieceStates)
	return statesCopy
}

// DownloadedBytes returns the number of downloaded bytes.
func (s *Session) DownloadedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Downloaded
}

// UploadedBytes returns the number of uploaded bytes.
func (s *Session) UploadedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Uploaded
}

// IsPaused returns whether the session is currently paused.
func (s *Session) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused
}

// LastError returns the last encountered error.
func (s *Session) LastError() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.statusErr != nil {
		return s.statusErr
	}
	if s.lastErr == nil {
		return s.lastTrackerErr
	}
	return s.lastErr
}

// CurrentSpeed returns the rolling 1-second download speed in bytes/sec.
func (s *Session) CurrentSpeed() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSpeed
}

func (s *Session) hasInboundListenerLocked() bool {
	return s.listener != nil && s.Port != 0
}

// sendPeerControlLocked queues a peer control message while s.mu is held.
func (s *Session) sendPeerControlLocked(c *peer.Client, fn func(*peer.Client) error) {
	if s.closed {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		select {
		case <-s.ctx.Done():
			return
		default:
		}
		_ = fn(c)
	}()
}

func (s *Session) isCompletedLocked() bool {
	if s.Storage == nil || s.metadataMode || s.Torrent == nil || len(s.PieceStates) == 0 {
		return false
	}
	stats := s.completionStatsLocked()
	return stats.wantedBytes == 0 || (stats.wantedPieces > 0 && stats.completedWantedPieces == stats.wantedPieces)
}

func (s *Session) queueTrackerEventLocked(event string) {
	if len(s.trackerEvents) > 0 && s.trackerEvents[len(s.trackerEvents)-1] == event {
		return
	}
	s.trackerEvents = append(s.trackerEvents, event)
}

// IsCompleted returns whether the download is completed.
func (s *Session) IsCompleted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isCompletedLocked()
}

func (s *Session) statusLocked() string {
	isCompleted := s.isCompletedLocked()
	if s.paused {
		if isCompleted {
			return "Stopped"
		}
		return "Paused"
	}
	if s.statusErr != nil {
		return "Error"
	}
	if s.metadataMode {
		return "Metadata"
	}
	// Only resume-hint verification shows "Checking"; a no-hint background scan runs
	// opportunistically and the torrent shows its normal (downloading/seeding) status.
	if s.verifying && !s.verifyFullScan {
		return "Checking"
	}

	if isCompleted {
		return "Seeding"
	}
	return "Downloading"
}

// Status returns the current status text (Downloading, Seeding, Paused, Stopped, or Error).
func (s *Session) Status() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.statusLocked()
}

// SessionSortSnapshot holds a snapshot of sorting keys.
type SessionSortSnapshot struct {
	StatusScore int
	AddedAt     time.Time
	Name        string
	InfoHashHex string
}

// GetSortSnapshot gathers sorting keys under a single read lock.
func (s *Session) GetSortSnapshot() SessionSortSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := s.statusLocked()
	var statusScore int
	switch status {
	case "Downloading", "Metadata", "Checking":
		statusScore = 0
	case "Seeding":
		statusScore = 1
	case "Paused", "Stopped":
		statusScore = 2
	case "Error":
		statusScore = 3
	default:
		statusScore = 4
	}

	name := ""
	infoHashHex := ""
	if s.Torrent != nil {
		name = s.Torrent.Name
		infoHashHex = fmt.Sprintf("%x", s.Torrent.InfoHash)
	}

	return SessionSortSnapshot{
		StatusScore: statusScore,
		AddedAt:     s.AddedAt,
		Name:        name,
		InfoHashHex: infoHashHex,
	}
}

// Name returns the torrent name, protected by a read lock.
func (s *Session) Name() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Torrent != nil {
		return s.Torrent.Name
	}
	return ""
}

// DownloadDir returns the download directory of the session, protected by a read lock.
func (s *Session) DownloadDir() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.downloadDir
}

// ContentPath returns the absolute on-disk path to this torrent's root item:
// the file itself for single-file torrents, or the top-level folder for
// multi-file torrents. ok is false when the path is not yet known, e.g. a
// magnet still fetching metadata with no file list.
//
// For both layouts the torrent name is the first component of every file path
// (see torrent.Parse), so joining it onto the download directory yields the
// item a user expects to be revealed in their file manager.
func (s *Session) ContentPath() (path string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.Torrent == nil || len(s.Torrent.Files) == 0 {
		return "", false
	}
	root := s.Torrent.Files[0].Path
	if len(root) == 0 || root[0] == "" {
		return "", false
	}
	return filepath.Join(s.downloadDir, root[0]), true
}

// IsMetadataMode returns whether the session is currently in metadata download mode.
func (s *Session) IsMetadataMode() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metadataMode
}

// Pause pauses the session, closes active connections, and queues a stopped tracker event.
func (s *Session) Pause() {
	s.mu.Lock()
	if s.paused {
		s.mu.Unlock()
		return
	}
	s.paused = true
	s.pauseEpoch++
	s.queueTrackerEventLocked("stopped")
	for _, client := range s.activePeers {
		if client.Conn != nil {
			_ = client.Conn.Close()
		}
	}
	s.mu.Unlock()

	// Signal tracker loop to announce immediately
	select {
	case s.resumeCh <- struct{}{}:
	default:
		// Already signaled
	}

	if s.OnStateChange != nil {
		s.OnStateChange()
	}
}

// Resume resumes the session and queues a started tracker event.
// P1 FIX: No longer spawns untracked goroutines — signals tracker loop via resumeCh.
func (s *Session) Resume() {
	s.mu.Lock()
	if !s.paused {
		s.mu.Unlock()
		return
	}
	s.paused = false
	s.queueTrackerEventLocked("started")
	for _, pState := range s.Peers {
		if !pState.Active {
			pState.LastAttempt = time.Time{}
		}
	}
	s.mu.Unlock()

	// Signal tracker loop to announce immediately
	select {
	case s.resumeCh <- struct{}{}:
	default:
		// Already signaled
	}

	if s.OnStateChange != nil {
		s.OnStateChange()
	}
}

// SetFilePriority sets the download priority for a specific file.
func (s *Session) SetFilePriority(fileIndex int, priority FilePriority) {
	s.mu.Lock()
	changed := false
	if fileIndex >= 0 && fileIndex < len(s.FilePriorities) {
		if s.FilePriorities[fileIndex] != priority {
			s.FilePriorities[fileIndex] = priority
			changed = true
		}
	}
	s.mu.Unlock()

	if changed && s.OnStateChange != nil {
		s.OnStateChange()
	}
}

// GetFilePriorities returns a copy of the current file priorities.
func (s *Session) GetFilePriorities() []FilePriority {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]FilePriority, len(s.FilePriorities))
	copy(cp, s.FilePriorities)
	return cp
}

// SetDownloadLimit sets the download bandwidth limit in bytes/sec. 0 = unlimited.
func (s *Session) SetDownloadLimit(bytesPerSec int64) {
	s.DownloadLimiter.SetLimit(bytesPerSec)
}

// SetUploadLimit sets the upload bandwidth limit in bytes/sec. 0 = unlimited.
func (s *Session) SetUploadLimit(bytesPerSec int64) {
	s.UploadLimiter.SetLimit(bytesPerSec)
}

func (s *Session) chokeLoop() {
	defer s.wg.Done()
	s.mu.Lock()
	s.chokeTimer = time.NewTicker(10 * time.Second)
	s.optimisticTimer = time.NewTicker(30 * time.Second)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.chokeTimer != nil {
			s.chokeTimer.Stop()
		}
		if s.optimisticTimer != nil {
			s.optimisticTimer.Stop()
		}
		s.mu.Unlock()
	}()

	var optimisticPeer string

	for {
		select {
		case <-s.chokeTimer.C:
			s.recalculateChoking(&optimisticPeer)
		case <-s.optimisticTimer.C:
			s.recalculateOptimistic(&optimisticPeer)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Session) recalculateChoking(optimisticPeer *string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused {
		return
	}

	// 1. Determine if we are seeding wanted content or still downloading it.
	stats := s.completionStatsLocked()
	isSeeding := stats.wantedBytes == 0 || (stats.wantedPieces > 0 && stats.completedWantedPieces == stats.wantedPieces)

	// 2. Collect all active peers that are interested in us
	type peerCandidate struct {
		addr   string
		speed  float64
		client *peer.Client
	}
	var candidates []peerCandidate

	for addr, pState := range s.Peers {
		if !pState.Active || !pState.Interested {
			continue
		}
		client, activeExists := s.activePeers[addr]
		if !activeExists {
			continue
		}

		var speed float64
		if isSeeding {
			speed = pState.UploadSpeed
		} else {
			speed = pState.DownloadSpeed
		}

		candidates = append(candidates, peerCandidate{
			addr:   addr,
			speed:  speed,
			client: client,
		})
	}

	// 3. Sort candidates (descending speed)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].speed > candidates[j].speed
	})

	// 4. Select top 4 peers to unchoke
	unchokedCount := 0
	topPeers := make(map[string]bool)

	for _, cand := range candidates {
		if unchokedCount < 4 {
			topPeers[cand.addr] = true
			unchokedCount++
			// Unchoke if choking
			pState := s.Peers[cand.addr]
			if pState.AmChoking {
				pState.AmChoking = false
				s.sendPeerControlLocked(cand.client, (*peer.Client).SendUnchoke)
			}
		} else {
			break
		}
	}

	// 5. Choke everyone else (except the optimistic peer, if set and interested)
	for addr, pState := range s.Peers {
		if !pState.Active {
			continue
		}
		client, activeExists := s.activePeers[addr]
		if !activeExists {
			continue
		}

		// Skip top unchoked peers
		if topPeers[addr] {
			continue
		}

		// Skip the optimistic peer
		if *optimisticPeer != "" && addr == *optimisticPeer && pState.Interested {
			// Keep optimistic peer unchoked and account it as one upload slot.
			if pState.AmChoking && unchokedCount < 4 {
				pState.AmChoking = false
				topPeers[addr] = true
				unchokedCount++
				s.sendPeerControlLocked(client, (*peer.Client).SendUnchoke)
			} else if !pState.AmChoking {
				topPeers[addr] = true
			}
			continue
		}

		// Otherwise, choke
		if !pState.AmChoking {
			pState.AmChoking = true
			s.sendPeerControlLocked(client, (*peer.Client).SendChoke)
		}
	}
}

func (s *Session) recalculateOptimistic(optimisticPeer *string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused {
		return
	}

	// Gather candidates for optimistic unchoke:
	// They must be active, interested in us, and currently choked by us
	var candidates []string
	for addr, pState := range s.Peers {
		if pState.Active && pState.Interested && pState.AmChoking {
			candidates = append(candidates, addr)
		}
	}

	if len(candidates) == 0 {
		return
	}

	// Pick a random candidate using time
	idx := time.Now().UnixNano() % int64(len(candidates))
	chosenAddr := candidates[idx]

	*optimisticPeer = chosenAddr

	pState := s.Peers[chosenAddr]
	pState.AmChoking = false
	if client, ok := s.activePeers[chosenAddr]; ok {
		s.sendPeerControlLocked(client, (*peer.Client).SendUnchoke)
	}
}

// onMetadataDownloaded handles processing of the downloaded metadata info dictionary.
func (s *Session) onMetadataDownloaded(infoBytes []byte) (err error) {
	defer func() {
		if err != nil {
			s.mu.Lock()
			s.metadataCompleted = false
			for i := range s.metadataPieces {
				s.metadataPieces[i] = false
			}
			s.mu.Unlock()
		}
	}()

	hash := sha1.Sum(infoBytes)
	if hash != s.Torrent.InfoHash {
		return fmt.Errorf("metadata hash mismatch: expected %x, got %x", s.Torrent.InfoHash, hash)
	}

	// Wrap the info dict in a dummy bencode dictionary
	wrapped := append([]byte("d4:info"), infoBytes...)
	wrapped = append(wrapped, 'e')

	parsed, err := torrent.Parse(wrapped)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %w", err)
	}

	s.mu.Lock()
	s.Torrent.PieceLength = parsed.PieceLength
	s.Torrent.PieceHashes = parsed.PieceHashes
	s.Torrent.Name = parsed.Name
	s.Torrent.Files = parsed.Files
	s.Torrent.InfoBytes = parsed.InfoBytes

	// Reinitialize priorities
	s.FilePriorities = make([]FilePriority, len(s.Torrent.Files))
	for i := range s.FilePriorities {
		s.FilePriorities[i] = PriorityNormal
	}
	if len(s.PendingFilePriorities) > 0 {
		for i := 0; i < len(s.FilePriorities) && i < len(s.PendingFilePriorities); i++ {
			prio := s.PendingFilePriorities[i]
			if prio >= PrioritySkip && prio <= PriorityHigh {
				s.FilePriorities[i] = prio
			}
		}
		s.PendingFilePriorities = nil
	}

	// Reinitialize piece states
	numPieces := len(s.Torrent.PieceHashes)
	s.PieceStates = make([]PieceState, numPieces)
	for i := range s.PieceStates {
		s.PieceStates[i] = PieceEmpty
	}

	// Initialize storage now that we know the files
	var fileInfos []storage.FileInfo
	for _, f := range s.Torrent.Files {
		fileInfos = append(fileInfos, storage.FileInfo{
			Path:   filepath.Join(f.Path...),
			Length: f.Length,
		})
	}
	st, err := storage.NewStorage(s.downloadDir, fileInfos, s.Torrent.PieceLength)
	if err != nil {
		statusErr := fmt.Errorf("failed to initialize storage: %w", err)
		s.lastErr = statusErr
		s.statusErr = statusErr
		s.mu.Unlock()
		return statusErr
	}
	s.Storage = st
	s.statusErr = nil
	storageToVerify := st
	s.mu.Unlock()

	// Load any fast-resume hint and verify in the background (metadata just arrived, so
	// most pieces are not on disk yet; verification stays off the hot path).
	s.loadResumeState()
	s.maybeStartVerification()

	s.mu.Lock()
	if s.Storage != storageToVerify {
		s.mu.Unlock()
		return fmt.Errorf("storage changed while completing metadata")
	}
	s.metadataCompleted = true
	s.metadataMode = false
	close(s.metadataCompletedCh)
	s.mu.Unlock()

	if s.OnStateChange != nil {
		s.OnStateChange()
	}

	return nil
}

func (s *Session) dhtLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// Initial lookup
	select {
	case <-time.After(1 * time.Second):
	case <-s.ctx.Done():
		return
	}
	s.mu.RLock()
	paused := s.paused
	d := s.DHT
	peerPort := s.Port
	hasInbound := s.hasInboundListenerLocked()
	var infoHash [20]byte
	if s.Torrent != nil {
		infoHash = s.Torrent.InfoHash
	}
	s.mu.RUnlock()

	if !paused && d != nil && s.Torrent != nil && hasInbound {
		d.Lookup(infoHash, peerPort)
	}

	for {
		select {
		case <-ticker.C:
			s.mu.RLock()
			paused = s.paused
			d = s.DHT
			peerPort = s.Port
			hasInbound = s.hasInboundListenerLocked()
			if s.Torrent != nil {
				infoHash = s.Torrent.InfoHash
			}
			s.mu.RUnlock()

			if !paused && d != nil && s.Torrent != nil && hasInbound {
				d.Lookup(infoHash, peerPort)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// AddPeerFromDiscovery adds a new peer found via DHT or other discovery mechanism
// and attempts to initiate a connection.
func (s *Session) AddPeerFromDiscovery(peerAddr string) {
	host, portStr, err := net.SplitHostPort(peerAddr)
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return
	}
	if port <= 0 || port > 65535 {
		return
	}

	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused || s.closed || !s.started {
		return
	}

	pState, exists := s.Peers[peerAddr]
	var shouldDial bool
	if !exists {
		shouldDial = true
	} else if !pState.Active && time.Since(pState.LastAttempt) > 60*time.Second {
		shouldDial = true
	}

	if shouldDial {
		if !exists {
			s.Peers[peerAddr] = &PeerState{
				IP:          host,
				Port:        uint16(port),
				AmChoking:   true,
				Choked:      true,
				LastAttempt: time.Now(),
			}
		} else {
			s.Peers[peerAddr].LastAttempt = time.Now()
		}
		s.wg.Add(1)
		go func(tp tracker.Peer) {
			defer s.wg.Done()
			s.connectToPeer(tp)
		}(tracker.Peer{IP: ip, Port: uint16(port)})
	}
}

// AttachDHT dynamically associates a DHT client and starts the dhtLoop if the session is running.
func (s *Session) AttachDHT(d *dht.DHT) {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	if s.closed || s.DHT != nil || d == nil {
		s.mu.Unlock()
		return
	}
	s.DHT = d
	shouldStart := s.started && !s.closed
	s.mu.Unlock()

	if shouldStart {
		s.wg.Add(1)
		go s.dhtLoop()
	}
}

func (s *Session) announceStopped() {
	s.mu.Lock()
	if s.stoppedAnnounced {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	// Short timeout (2s) on a background context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if s.announceWithEvent(ctx, "stopped") {
		s.mu.Lock()
		s.stoppedAnnounced = true
		s.mu.Unlock()
	}
}

func (s *Session) announceWithEvent(ctx context.Context, event string) bool {
	trackers := s.Torrent.Trackers
	if len(trackers) == 0 {
		return true // No trackers configured, counts as success
	}

	s.mu.RLock()
	var downloaded, left int64
	if s.metadataMode || s.Storage == nil || len(s.PieceStates) == 0 {
		left = 1
	} else {
		stats := s.completionStatsLocked()
		downloaded = stats.completedTotalBytes
		left = stats.totalBytes - stats.completedTotalBytes
		if left < 0 {
			left = 0
		}
	}
	port := s.Port
	uploaded := s.Uploaded
	s.mu.RUnlock()

	success := false
	for _, tr := range trackers {
		if bytes.HasPrefix([]byte(tr), []byte("udp")) {
			_, err := tracker.UDPAnnounce(ctx, tr, s.Torrent.InfoHash, s.PeerID, port, uploaded, downloaded, left, event, trackerDefaultNumWant)
			if err == nil {
				success = true
			}
		} else if bytes.HasPrefix([]byte(tr), []byte("http")) {
			u, err := tracker.BuildTrackerURL(tr, s.Torrent.InfoHash, s.PeerID, port, uploaded, downloaded, left, true, event, trackerDefaultNumWant)
			if err != nil {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
			if err != nil {
				continue
			}
			client := &http.Client{Timeout: 2 * time.Second}
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				success = true
			}
		}
	}
	return success
}
