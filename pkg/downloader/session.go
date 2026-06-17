package downloader

import (
	"context"
	"crypto/sha1"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/peer"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
	"sainttorrent/pkg/utp"
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

// BlockSize is the standard block length for BitTorrent transfers (16 KB).
const BlockSize = 16384

// blockRequestTimeout is how long an outstanding block request may go unanswered
// before pump re-arms it (and, after maxBlockRequestRetries, drops the peer). A var
// (not const) so tests can shorten it; treat it as a constant in production.
var blockRequestTimeout = 20 * time.Second

const maxBlockRequestRetries = 2

// PeerState holds per-peer state visible to the TUI.
type PeerState struct {
	IP            string
	Port          uint16
	Choked        bool
	Interested    bool
	DownloadSpeed float64 // Bytes per second
	UploadSpeed   float64 // Bytes per second
	// Downloaded and Uploaded are cumulative byte counters bumped on the peer's
	// hot path WITHOUT holding s.mu. They must be accessed only via sync/atomic
	// (AddInt64/LoadInt64); never read or copy them with a plain struct copy.
	// (Kept as plain int64 rather than atomic.Int64 so PeerState stays copyable
	// for the snapshots GetActivePeers hands to the TUI.)
	Downloaded  int64
	Uploaded    int64
	Active      bool
	AmChoking   bool
	LastAttempt time.Time
	// Dialable means IP:Port came from tracker/DHT discovery or an outbound dial,
	// rather than only from an inbound connection's usually-ephemeral source port.
	Dialable bool
	// Dialing prevents tracker, DHT, maintenance, and resume paths from launching
	// duplicate concurrent attempts to the same endpoint.
	Dialing bool

	WindowBlocks         int
	TargetWindowBlocks   int
	OutstandingBlocks    int
	OutstandingBytes     int64
	PipelineQueueSeconds float64
	PipelineRTT          time.Duration
	PipelineRate         float64
	TimeoutRate          float64
	AppLimited           bool
	BudgetLimited        bool
	PieceCapLimited      bool
	WriterLimited        bool
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
	// EncryptionPolicy controls whether peer connections use MSE/PE. Sessions
	// created directly default to plaintext compatibility; the CLI-managed
	// TorrentManager sets its configured policy when adding sessions.
	EncryptionPolicy mse.Policy

	mu sync.RWMutex
	// Downloaded and Uploaded are cumulative session byte counters. They are
	// updated on the peer hot path via atomics (no s.mu), so other accesses use
	// the atomic methods too rather than taking the lock.
	Downloaded  atomic.Int64
	Uploaded    atomic.Int64
	PieceStates []PieceState
	// neededPieces is the incrementally-maintained set of pieces that are still
	// PieceEmpty and wanted (file priority != skip). The picker scans only this set
	// instead of all pieces on every pick, so selection cost is O(remaining-needed)
	// — which shrinks toward zero as the download completes — rather than O(total).
	// It is a hint: the picker re-verifies state==PieceEmpty before claiming, so a
	// stale entry (e.g. a test mutating PieceStates directly) is harmless. Guarded
	// by s.mu like PieceStates.
	neededPieces map[int]struct{}
	// pieceAvailability[i] counts how many currently-connected peers advertise piece
	// i (via bitfield/Have, decremented on disconnect). The picker prefers rarer
	// pieces (#7, rarest-first) so the swarm keeps more pieces fetchable. Same length
	// as PieceStates; guarded by s.mu.
	pieceAvailability []int
	Peers             map[string]*PeerState
	activePeers       map[string]*peer.Client // for sending Have messages
	pipelineBudget    *pipelineByteBudget

	// Async hash/write pool (item #2). Completed-piece buffers are handed to a small
	// background worker pool that verifies the SHA-1, writes to storage, and persists
	// fast-resume state — keeping that disk/CPU work off the peer read loop so the
	// socket keeps draining and new requests keep flowing. Created lazily on the
	// first completed piece; workers stop on ctx cancellation and are intentionally
	// not tracked by s.wg (like background verification) so Close never blocks on a
	// disk write wedged on slow I/O.
	pieceWriteCh   chan pieceWriteJob
	pieceWriteOnce sync.Once

	lifecycleMu         sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	closeOnce           sync.Once
	listener            net.Listener
	utpSocket           *utp.Socket
	sharedInbound       bool
	outboundSlots       chan struct{} // semaphore bounding concurrent outbound dials (lock-free)
	inboundSlots        chan struct{} // semaphore bounding concurrent inbound connections (lock-free)
	globalOutboundSlots chan struct{} // manager-wide outbound cap shared across sessions (nil if standalone)
	globalInboundSlots  chan struct{} // manager-wide inbound cap shared across sessions (nil if standalone)
	currentSpeed        float64
	currentUploadSpeed  float64
	trackerSeeders      int
	trackerLeechers     int
	trackerCompleted    int
	lastErr             error
	statusErr           error // current blocking failure; drives Error status
	lastTrackerErr      error
	paused              bool
	pauseEpoch          uint64 // increments when active connections are closed for pause
	closed              bool
	started             bool
	resumeCh            chan struct{} // signal to wake tracker loop on resume
	trackerEvents       []string      // Queue of pending tracker events
	completedAnnounced  bool
	stoppedAnnounced    bool

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
		EncryptionPolicy:    mse.PolicyDisable,
		PieceStates:         states,
		pieceAvailability:   make([]int, numPieces),
		Peers:               make(map[string]*PeerState),
		activePeers:         make(map[string]*peer.Client),
		pipelineBudget:      newPipelineByteBudget(dynamicPipelineSessionBudgetBytes),
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
		outboundSlots:       make(chan struct{}, maxOutboundPeers),
		inboundSlots:        make(chan struct{}, maxInboundPeers),
	}

	if !metadataMode {
		// Cheaply load fast-resume hints (no hashing). Actual hash verification runs in
		// the background once Start() is called, keeping startup off the hashing path.
		sess.loadResumeState()
	}

	return sess, nil
}

func (s *Session) allowsDecentralizedPeerDiscoveryLocked() bool {
	return s.Torrent == nil || !s.Torrent.Private
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
	return float64(s.Downloaded.Load()) / elapsed
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

	s.mu.RLock()
	sharedInbound := s.sharedInbound
	s.mu.RUnlock()

	var listener net.Listener
	if !sharedInbound {
		// Standalone sessions own their listener. Managed sessions use the
		// manager's shared listener and already have their advertised port set.
		var err error
		listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
		if err != nil {
			s.mu.Lock()
			s.lastErr = fmt.Errorf("inbound listener failed: %w", err)
			s.mu.Unlock()
			// Continue without listener — outbound connections still work.
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
	}

	goroutineCount := 4 // tracker + speed monitor + choke loop + peer maintenance
	if listener != nil {
		goroutineCount = 5 // + inbound listener
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
	go s.peerMaintenanceLoop()
	if listener != nil {
		go s.inboundListenerLoop()
	}
	if hasDHT {
		go s.dhtLoop()
	}

	// Kick off background fast-resume verification (no-op if nothing to verify).
	s.maybeStartVerification()
}

// Close shuts down the session, releases its storage ownership, and waits for its
// lifecycle goroutines (tracker, peer, choke, listener, and DHT loops) to exit.
// Background piece verification is intentionally NOT awaited — a VerifyPiece read
// can be wedged on slow I/O — but its global verification slot is reclaimed here
// so capacity is never permanently lost, and it stops mutating the session once
// s.closed is set.
func (s *Session) Close() {
	s.lifecycleMu.Lock()
	var gateRelease func()
	var verifyDone chan struct{}
	var storageToClose *storage.Storage
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
		storageToClose = s.Storage
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

	if storageToClose != nil {
		_ = storageToClose.Close()
	}

	// Reclaim a verification slot held by a wedged VerifyPiece (outside any lock).
	if gateRelease != nil {
		gateRelease()
	}
	if verifyDone != nil {
		close(verifyDone)
	}

	s.wg.Wait()
}

func (s *Session) speedMonitorLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastGlobalDownloaded int64
	var lastGlobalUploaded int64
	lastPeerDownloaded := make(map[string]int64)
	lastPeerUploaded := make(map[string]int64)

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			if s.paused {
				s.currentSpeed = 0
				s.currentUploadSpeed = 0
				lastGlobalDownloaded = s.Downloaded.Load()
				lastGlobalUploaded = s.Uploaded.Load()
				for _, pState := range s.Peers {
					pState.DownloadSpeed = 0
					pState.UploadSpeed = 0
				}
				s.mu.Unlock()
				continue
			}

			curDownloaded := s.Downloaded.Load()
			globalDiff := curDownloaded - lastGlobalDownloaded
			s.currentSpeed = float64(globalDiff)
			lastGlobalDownloaded = curDownloaded
			curUploaded := s.Uploaded.Load()
			globalUploadDiff := curUploaded - lastGlobalUploaded
			s.currentUploadSpeed = float64(globalUploadDiff)
			lastGlobalUploaded = curUploaded

			for addr, pState := range s.Peers {
				if pState.Active {
					peerDownloaded := atomic.LoadInt64(&pState.Downloaded)
					lastVal := lastPeerDownloaded[addr]
					peerDiff := peerDownloaded - lastVal
					pState.DownloadSpeed = float64(peerDiff)
					lastPeerDownloaded[addr] = peerDownloaded

					peerUploaded := atomic.LoadInt64(&pState.Uploaded)
					lastUploaded := lastPeerUploaded[addr]
					uploadDiff := peerUploaded - lastUploaded
					pState.UploadSpeed = float64(uploadDiff)
					lastPeerUploaded[addr] = peerUploaded
				} else {
					delete(lastPeerDownloaded, addr)
					delete(lastPeerUploaded, addr)
				}
			}
			s.mu.Unlock()

		case <-s.ctx.Done():
			s.mu.Lock()
			s.currentSpeed = 0
			s.currentUploadSpeed = 0
			for _, pState := range s.Peers {
				pState.DownloadSpeed = 0
				pState.UploadSpeed = 0
			}
			s.mu.Unlock()
			return
		}
	}
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
	return s.Downloaded.Load()
}

// UploadedBytes returns the number of uploaded bytes.
func (s *Session) UploadedBytes() int64 {
	return s.Uploaded.Load()
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

// CurrentUploadSpeed returns the rolling 1-second upload speed in bytes/sec.
func (s *Session) CurrentUploadSpeed() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentUploadSpeed
}

func (s *Session) hasInboundListenerLocked() bool {
	return (s.listener != nil || s.sharedInbound) && s.Port != 0
}

func (s *Session) setAdvertisedPort(port uint16) {
	if port == 0 {
		return
	}
	s.mu.Lock()
	if s.Port == port {
		s.mu.Unlock()
		return
	}
	s.Port = port
	started := s.started && !s.closed
	if started {
		s.queueTrackerEventLocked("started")
	}
	s.mu.Unlock()

	if started {
		select {
		case s.resumeCh <- struct{}{}:
		default:
		}
	}
}

func (s *Session) isCompletedLocked() bool {
	if s.Storage == nil || s.metadataMode || s.Torrent == nil || len(s.PieceStates) == 0 {
		return false
	}
	stats := s.completionStatsLocked()
	return stats.wantedBytes == 0 || (stats.wantedPieces > 0 && stats.completedWantedPieces == stats.wantedPieces)
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
			// Wanted-ness of the pieces overlapping this file may have flipped, so
			// rebuild the needed set (rare, user-initiated — O(pieces) is fine here).
			s.recomputeNeededLocked()
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

func (s *Session) reserveDownloadWithRefund(n int) (reserved bool, retryAfter time.Duration, refund func()) {
	localOK, localCharged, localRetry := s.DownloadLimiter.tryReserve(n)
	if !localOK {
		return false, localRetry, nil
	}
	globalCharged := false
	if s.GlobalDownloadLimiter != nil {
		globalOK, charged, globalRetry := s.GlobalDownloadLimiter.tryReserve(n)
		if !globalOK {
			if localCharged {
				s.DownloadLimiter.refund(n)
			}
			return false, globalRetry, nil
		}
		globalCharged = charged
	}
	return true, 0, func() {
		if localCharged {
			s.DownloadLimiter.refund(n)
		}
		if globalCharged && s.GlobalDownloadLimiter != nil {
			s.GlobalDownloadLimiter.refund(n)
		}
	}
}

// SessionPipelineStats summarizes the dynamic request windows across active peers.
type SessionPipelineStats struct {
	ActiveDownloadPeers          int
	TotalOutstandingBlocks       int
	TotalOutstandingBytes        int64
	PipelineBudgetBytes          int64
	PipelineBudgetUsedBytes      int64
	PipelineBudgetHighWaterBytes int64
	AppLimitedPeers              int
	BudgetLimitedPeers           int
	PieceCapLimitedPeers         int
	WriterLimitedPeers           int
}

func (s *Session) PipelineStats() SessionPipelineStats {
	if s.pipelineBudget == nil {
		return SessionPipelineStats{}
	}
	limit, used, highWater := s.pipelineBudget.snapshot()
	stats := SessionPipelineStats{
		PipelineBudgetBytes:          limit,
		PipelineBudgetUsedBytes:      used,
		PipelineBudgetHighWaterBytes: highWater,
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.Peers {
		if !p.Active {
			continue
		}
		stats.ActiveDownloadPeers++
		stats.TotalOutstandingBlocks += p.OutstandingBlocks
		stats.TotalOutstandingBytes += p.OutstandingBytes
		if p.AppLimited {
			stats.AppLimitedPeers++
		}
		if p.BudgetLimited {
			stats.BudgetLimitedPeers++
		}
		if p.PieceCapLimited {
			stats.PieceCapLimitedPeers++
		}
		if p.WriterLimited {
			stats.WriterLimitedPeers++
		}
	}
	return stats
}

// onMetadataDownloaded handles processing of the downloaded metadata info dictionary.
func (s *Session) onMetadataDownloaded(infoBytes []byte) (err error) {
	defer func() {
		if err != nil {
			// A full assembly that fails the infohash check means the size/blocks we
			// locked onto were bad — most likely a peer that won the race to advertise
			// metadata_size handed us a poisoned (but in-range) value. Discard the whole
			// accumulator, not just the completion flag, so metadataSize resets to 0 and
			// the next peer's advertised size can take over instead of every honest peer
			// being rejected forever by the size-mismatch guard.
			s.mu.Lock()
			s.metadataCompleted = false
			s.metadataSize = 0
			s.metadataBuf = nil
			s.metadataPieces = nil
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
	s.Torrent.Private = parsed.Private
	if parsed.Private {
		s.DHT = nil
	}

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
	s.pieceAvailability = make([]int, numPieces)

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
