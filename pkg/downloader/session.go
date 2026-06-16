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
	"sync/atomic"
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

// maxPendingBlockRequests is the per-peer request window: the number of 16 KB
// block requests we keep in flight to a single peer. Throughput per peer is
// roughly window÷RTT, so this must cover the bandwidth-delay product of a fast,
// high-latency peer (256 × 16 KB = 4 MB, e.g. ~50 MB/s at 80 ms RTT). Crucially,
// the window now spans MULTIPLE pieces (see maxConcurrentPiecesPerPeer): it is no
// longer capped by a single piece's block count, which previously throttled
// small-piece torrents to one piece's worth of in-flight data.
const maxPendingBlockRequests = 256

// maxConcurrentPiecesPerPeer bounds how many pieces a single peer downloads at
// once. It exists so small-piece torrents can still fill maxPendingBlockRequests
// (a 256 KB piece is only 16 blocks, so one piece could never fill a 256-block
// window). Because we open just enough pieces to fill the window, the in-flight
// buffer memory stays ≈ window×16 KB per peer regardless of piece size.
const maxConcurrentPiecesPerPeer = 16

// blockRequestTimeout is how long an outstanding block request may go unanswered
// before pump re-arms it (and, after maxBlockRequestRetries, drops the peer). A var
// (not const) so tests can shorten it; treat it as a constant in production.
var blockRequestTimeout = 20 * time.Second

const maxBlockRequestRetries = 2

// peerStallTimeout bounds how long an outbound peer may hold its connection slot
// without delivering a single block of data we want. A connection's slot is held
// for the whole life of its read loop, and a peer that chokes us forever — or
// trickles only keep-alives/Have messages — resets the socket read deadline
// without ever giving us data, so without this it would occupy a slot indefinitely.
// In a slow swarm those dead-weight connections accumulate until the (shared,
// manager-wide) outbound pool is full and NO session can dial a fresh peer, which
// flatlines every torrent at once until a restart clears the pools. Any single
// received block resets the timer, so a genuinely-slow-but-working peer survives;
// only one delivering < one block per peerStallTimeout (≈273 B/s at 60 s) is
// reaped, freeing the slot for a productive peer. Reaping never applies while we
// are seeding (we want no data) — see the reaper in runPeerMessageLoop.
// A var (not const) so tests can shorten it; treat it as a constant in production.
var peerStallTimeout = 60 * time.Second

// peerMaintenanceInterval is how often peerMaintenanceLoop redials toward a full
// outbound connection set. New dials previously happened ONLY on a tracker
// announce (interval up to an hour) or a 30 s DHT lookup, so a slot freed by a
// dropped/reaped peer could sit idle for a long time even with known peers on
// hand. The maintenance tick refills from the known-peer set as soon as slots
// open, decoupling connection churn from the announce cadence.
var peerMaintenanceInterval = 5 * time.Second

// peerRedialBackoff is the minimum gap before peerMaintenanceLoop re-dials a known
// peer that is not currently connected, so a peer that just dropped (or that we
// just reaped) is not hammered in a tight loop.
var peerRedialBackoff = 60 * time.Second

// Large enough to avoid kernel socket buffers becoming the bottleneck on fast peers.
const peerSocketBufferSize = 4 * 1024 * 1024

const trackerDefaultNumWant = 200

// maxTrackerResponse caps how many bytes of an HTTP tracker's announce response we
// buffer. Legitimate replies (a compact peer list plus a little metadata) are a few KB
// even at numwant=200; this ceiling stops a malicious or MITM'd tracker from streaming
// unbounded data into memory.
const maxTrackerResponse = 2 * 1024 * 1024

// maxOutboundPeers bounds how many peers a single session dials concurrently. This is
// the download engine: it governs throughput on swarms made of many slow peers, so it
// is set generously (mainline/libtorrent use ~200 per torrent). An attacker cannot
// occupy these slots — they are only ever filled by peers we chose to connect to.
const maxOutboundPeers = 200

// maxInboundPeers bounds how many incoming peer connections a session accepts at once.
// The listen port is public (announced to trackers/DHT), so this is the real abuse
// surface: the cap stops a flood of inbound connections from exhausting file descriptors.
// It is a SEPARATE budget from outbound, so an inbound flood can never starve downloads.
const maxInboundPeers = 100

// maxKnownPeers bounds the size of the Peers map so a tracker/DHT feeding an endless
// stream of unique addresses cannot grow it without limit. Active peers are retained;
// inactive entries are evicted oldest-first.
const maxKnownPeers = 2048

var trackerAnnounceTimeout = 15 * time.Second

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
	sharedInbound       bool
	outboundSlots       chan struct{} // semaphore bounding concurrent outbound dials (lock-free)
	inboundSlots        chan struct{} // semaphore bounding concurrent inbound connections (lock-free)
	globalOutboundSlots chan struct{} // manager-wide outbound cap shared across sessions (nil if standalone)
	globalInboundSlots  chan struct{} // manager-wide inbound cap shared across sessions (nil if standalone)
	currentSpeed        float64
	currentUploadSpeed  float64
	trackerSeeders      int
	trackerLeechers     int
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

// pieceWriteJob is a completed piece handed off to the async hash/write pool.
type pieceWriteJob struct {
	index int64
	hash  [20]byte
	data  []byte
	// conn is the connection of the peer that supplied the piece. If the assembled
	// data fails the SHA-1 check the worker closes it, dropping the misbehaving peer
	// (its read loop unblocks and exits) — the decoupled equivalent of the old inline
	// disconnect-on-corruption.
	conn net.Conn
}

// pieceWriteQueueDepth bounds how many completed-piece buffers can be queued for the
// write pool. Each entry holds a full piece, so this caps the pool's memory; once
// full, submitting applies backpressure to the peer goroutine, which is the intended
// bound (a peer can't outrun the disk without limit).
const pieceWriteQueueDepth = 8

// ensurePieceWritePool lazily starts the background hash/write workers. Idempotent.
func (s *Session) ensurePieceWritePool() {
	s.pieceWriteOnce.Do(func() {
		s.pieceWriteCh = make(chan pieceWriteJob, pieceWriteQueueDepth)
		workers := runtime.GOMAXPROCS(0)
		if workers < 1 {
			workers = 1
		}
		for i := 0; i < workers; i++ {
			go s.pieceWriteWorker()
		}
	})
}

func (s *Session) pieceWriteWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.pieceWriteCh:
			s.processCompletedPiece(job)
		}
	}
}

// processCompletedPiece verifies, writes, and records a completed piece off the peer
// read loop. On a hash mismatch it returns the piece to the pool and disconnects the
// feeding peer; on a storage repair it resets progress; otherwise it marks the piece
// complete (which persists fast-resume state and advertises Have).
func (s *Session) processCompletedPiece(job pieceWriteJob) {
	// Drop the write if the session is shutting down: the piece will be re-fetched on
	// the next run, and a late state persist must not resurrect a .state file a remove
	// is deleting. Also drop a piece already completed by another peer (#8 endgame
	// produces redundant copies) so we don't re-write storage or re-announce Have.
	s.mu.RLock()
	closed := s.closed
	alreadyDone := job.index >= 0 && job.index < int64(len(s.PieceStates)) &&
		s.PieceStates[job.index] == PieceCompleted
	s.mu.RUnlock()
	if closed || alreadyDone {
		return
	}

	if sha1.Sum(job.data) != job.hash {
		s.mu.Lock()
		s.lastErr = fmt.Errorf("piece %d failed hash verification", job.index)
		if job.index >= 0 && job.index < int64(len(s.PieceStates)) && s.PieceStates[job.index] == PieceDownloading {
			s.PieceStates[job.index] = PieceEmpty
			s.addNeededLocked(int(job.index))
		}
		s.mu.Unlock()
		if job.conn != nil {
			_ = job.conn.Close()
		}
		return
	}

	err := s.Storage.WriteBlock(job.index, 0, job.data)
	switch err {
	case nil:
		s.markPieceCompleted(job.index)
	case storage.ErrFileRepaired:
		s.mu.Lock()
		s.lastErr = fmt.Errorf("download file was missing or resized; recreated target file")
		s.mu.Unlock()
		s.resetProgressAfterStorageRepair(job.index)
	case storage.ErrStorageClosed:
		// Session is tearing down; nothing to record.
		return
	default:
		s.mu.Lock()
		s.lastErr = err
		s.statusErr = err
		if job.index >= 0 && job.index < int64(len(s.PieceStates)) && s.PieceStates[job.index] == PieceDownloading {
			s.PieceStates[job.index] = PieceEmpty
			s.addNeededLocked(int(job.index))
		}
		s.mu.Unlock()
	}
}

// recomputeNeededLocked rebuilds the needed-piece set from scratch. Used after bulk
// state changes (resume load, metadata arrival, storage repair, priority changes).
// Caller holds s.mu.
func (s *Session) recomputeNeededLocked() {
	if s.neededPieces == nil {
		s.neededPieces = make(map[int]struct{}, len(s.PieceStates))
	} else {
		clear(s.neededPieces)
	}
	for i, state := range s.PieceStates {
		if state == PieceEmpty && s.isPieceWanted(int64(i)) {
			s.neededPieces[i] = struct{}{}
		}
	}
}

// addNeededLocked records a piece as needed when it (re)enters PieceEmpty, if wanted.
// Caller holds s.mu.
func (s *Session) addNeededLocked(idx int) {
	if idx < 0 || idx >= len(s.PieceStates) {
		return
	}
	if !s.isPieceWanted(int64(idx)) {
		return
	}
	if s.neededPieces == nil {
		s.neededPieces = make(map[int]struct{}, len(s.PieceStates))
	}
	s.neededPieces[idx] = struct{}{}
}

// removeNeededLocked drops a piece from the needed set when it leaves PieceEmpty.
// Caller holds s.mu.
func (s *Session) removeNeededLocked(idx int) {
	delete(s.neededPieces, idx)
}

// selectNeededPieceLocked returns the index of the piece to fetch next: the
// highest-priority, then rarest (lowest swarm availability), then lowest-index
// PieceEmpty wanted piece for which hasPiece reports the peer has it, or -1 if none.
// It scans only the incrementally-maintained needed set (empty & wanted) rather than
// every piece, so cost is O(remaining-needed) — shrinking toward zero as the download
// finishes — instead of O(total) per pick. The set is a hint: entries that are no
// longer empty (e.g. completed out-of-band) are pruned in-line, and membership is
// re-verified against PieceStates, so a stale entry is never mis-selected. Caller
// holds s.mu.
func (s *Session) selectNeededPieceLocked(hasPiece func(pieceIndex int64) bool) int {
	bestIdx := -1
	bestPriority := PrioritySkip
	bestAvail := 0
	for i := range s.neededPieces {
		if i < 0 || i >= len(s.PieceStates) || s.PieceStates[i] != PieceEmpty {
			delete(s.neededPieces, i)
			continue
		}
		idx := int64(i)
		if !hasPiece(idx) || !s.isPieceWanted(idx) {
			continue
		}
		pri := s.piecePriority(idx)
		avail := s.pieceAvailabilityAt(i)
		if betterPick(bestIdx, i, bestPriority, pri, bestAvail, avail) {
			bestIdx, bestPriority, bestAvail = i, pri, avail
		}
	}
	return bestIdx
}

// betterPick reports whether candidate piece cand (priority cp, availability ca)
// should beat the current best (priority bp, availability ba). Ordering: highest
// priority, then rarest (lowest availability), then lowest index. best == -1 means
// no candidate chosen yet.
func betterPick(best, cand int, bp, cp FilePriority, ba, ca int) bool {
	if best == -1 {
		return true
	}
	if cp != bp {
		return cp > bp
	}
	if ca != ba {
		return ca < ba
	}
	return cand < best
}

// pieceAvailabilityAt returns how many connected peers advertise piece i (0 if out
// of range). Caller holds s.mu.
func (s *Session) pieceAvailabilityAt(i int) int {
	if i >= 0 && i < len(s.pieceAvailability) {
		return s.pieceAvailability[i]
	}
	return 0
}

// bitfieldHas reports whether bit i is set in a BitTorrent bitfield (MSB-first
// within each byte).
func bitfieldHas(bf []byte, i int) bool {
	byteIdx := i / 8
	if byteIdx < 0 || byteIdx >= len(bf) {
		return false
	}
	return bf[byteIdx]&(1<<(7-uint(i%8))) != 0
}

// addPieceAvailability records that a peer now advertises piece idx (a Have).
func (s *Session) addPieceAvailability(idx int) {
	s.mu.Lock()
	if idx >= 0 && idx < len(s.pieceAvailability) {
		s.pieceAvailability[idx]++
	}
	s.mu.Unlock()
}

// applyBitfieldAvailability folds the delta between a peer's previous and new
// advertised bitfield into the swarm availability counts. This handles a peer that
// re-sends or extends its bitfield without double-counting.
func (s *Session) applyBitfieldAvailability(oldBF, newBF []byte) {
	s.mu.Lock()
	for i := range s.pieceAvailability {
		old := bitfieldHas(oldBF, i)
		now := bitfieldHas(newBF, i)
		switch {
		case now && !old:
			s.pieceAvailability[i]++
		case old && !now && s.pieceAvailability[i] > 0:
			s.pieceAvailability[i]--
		}
	}
	s.mu.Unlock()
}

// removePeerAvailability drops a disconnecting peer's contribution to the counts,
// using the bitfield it had accumulated (Haves set bits incrementally, so this is
// the exact set it added). Caller does not hold s.mu.
func (s *Session) removePeerAvailability(bf []byte) {
	if bf == nil {
		return
	}
	s.mu.Lock()
	for i := range s.pieceAvailability {
		if bitfieldHas(bf, i) && s.pieceAvailability[i] > 0 {
			s.pieceAvailability[i]--
		}
	}
	s.mu.Unlock()
}

// --- #8: endgame mode ---
//
// Endgame begins once every empty wanted piece has been claimed (the needed set is
// empty) while pieces are still in flight. Past that point a peer with nothing fresh
// to fetch would idle while the final pieces trickle in from the slowest peers, so we
// let it redundantly fetch a piece another peer already holds open. The first copy to
// complete wins; the losers cancel their outstanding block requests (see
// dropCompletedElsewhere in the peer loop).

// endgameActiveLocked reports whether the download has reached the completion tail:
// no empty wanted pieces are left to hand out, so remaining wanted pieces (if any) are
// all in flight. O(1). Caller holds s.mu.
func (s *Session) endgameActiveLocked() bool {
	return len(s.neededPieces) == 0
}

// selectEndgamePieceLocked picks the highest-priority, rarest, lowest-index in-progress
// (PieceDownloading) wanted piece the peer has that it is not already downloading, or
// -1. Used only in endgame to fetch a piece redundantly. Caller holds s.mu.
func (s *Session) selectEndgamePieceLocked(hasPiece func(pieceIndex int64) bool, owned map[int64]bool) int {
	bestIdx := -1
	bestPriority := PrioritySkip
	bestAvail := 0
	for i, state := range s.PieceStates {
		if state != PieceDownloading {
			continue
		}
		idx := int64(i)
		if owned[idx] || !hasPiece(idx) || !s.isPieceWanted(idx) {
			continue
		}
		pri := s.piecePriority(idx)
		avail := s.pieceAvailabilityAt(i)
		if betterPick(bestIdx, i, bestPriority, pri, bestAvail, avail) {
			bestIdx, bestPriority, bestAvail = i, pri, avail
		}
	}
	return bestIdx
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
		pieceAvailability:   make([]int, numPieces),
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
	s.recomputeNeededLocked()
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
				s.removeNeededLocked(idx)
				nowCompleted = true
			}
		} else if s.PieceStates[idx] == PieceUnverified {
			if verifyErr == nil && ok {
				s.PieceStates[idx] = PieceCompleted
				nowCompleted = true
			} else {
				// Resume data was wrong: return the piece to the pool for re-download.
				s.PieceStates[idx] = PieceEmpty
				s.addNeededLocked(idx)
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
	return float64(s.Downloaded.Load()) / elapsed
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

// prunePeersLocked evicts inactive known-peer entries when the Peers map grows past
// maxKnownPeers, oldest-attempt-first; active peers are never evicted. Caller holds s.mu.
func (s *Session) prunePeersLocked() {
	if len(s.Peers) <= maxKnownPeers {
		return
	}
	type agedPeer struct {
		addr string
		at   time.Time
	}
	inactive := make([]agedPeer, 0, len(s.Peers))
	for addr, ps := range s.Peers {
		if ps.Active {
			continue
		}
		inactive = append(inactive, agedPeer{addr: addr, at: ps.LastAttempt})
	}
	sort.Slice(inactive, func(i, j int) bool {
		return inactive[i].at.Before(inactive[j].at)
	})
	// Evict down to ~75% of the cap so pruning isn't triggered on every insert.
	evict := len(s.Peers) - (maxKnownPeers * 3 / 4)
	for i := 0; i < evict && i < len(inactive); i++ {
		delete(s.Peers, inactive[i].addr)
	}
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
	s.removeNeededLocked(int(index))
	s.lastErr = nil
	s.statusErr = nil
	// Skip the resume persist if the session is closing so a late piece write (the
	// async pool is not awaited by Close) cannot recreate a .state file a remove is
	// deleting — mirroring finishVerify.
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
	s.recomputeNeededLocked()
	s.lastErr = nil
	s.statusErr = nil
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

type trackerAnnounceResult struct {
	peers      []tracker.Peer
	interval   int
	complete   int
	incomplete int
	err        error
}

func announceTracker(ctx context.Context, tr string, infoHash [20]byte, peerID [20]byte, port uint16, uploaded, downloaded, left int64, event string, timeout time.Duration) trackerAnnounceResult {
	announceCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if bytes.HasPrefix([]byte(tr), []byte("udp")) {
		resp, err := tracker.UDPAnnounce(announceCtx, tr, infoHash, peerID, port, uploaded, downloaded, left, event, trackerDefaultNumWant)
		if err != nil {
			return trackerAnnounceResult{err: err}
		}
		return trackerAnnounceResult{
			peers:      resp.Peers,
			interval:   resp.Interval,
			complete:   resp.Complete,
			incomplete: resp.Incomplete,
		}
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
		// Bound how much we buffer: a tracker announce reply is a few KB even at
		// numwant=200. The cap stops a malicious or MITM'd tracker from streaming
		// unbounded data into memory. Reading one byte past the cap lets us detect
		// and reject an over-limit response rather than silently truncating it.
		_, err = io.Copy(buf, io.LimitReader(resp.Body, maxTrackerResponse+1))
		resp.Body.Close()
		if err != nil {
			return trackerAnnounceResult{err: err}
		}
		if buf.Len() > maxTrackerResponse {
			return trackerAnnounceResult{err: fmt.Errorf("tracker response exceeds %d bytes", maxTrackerResponse)}
		}

		trackerResp, err := tracker.ParseTrackerResponse(buf.Bytes())
		if err != nil {
			return trackerAnnounceResult{err: err}
		}

		return trackerAnnounceResult{
			peers:      trackerResp.Peers,
			interval:   trackerResp.Interval,
			complete:   trackerResp.Complete,
			incomplete: trackerResp.Incomplete,
		}
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
	uploaded := s.Uploaded.Load()
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
	var trackerSeeders int
	var trackerLeechers int

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
		trackerSeeders = max(trackerSeeders, result.complete)
		trackerLeechers = max(trackerLeechers, result.incomplete)
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
	if trackerSuccess {
		s.trackerSeeders = trackerSeeders
		s.trackerLeechers = trackerLeechers
	}
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

	// Connect to new peers. The outbound semaphore in connectToPeer is the hard cap on
	// concurrent connections; this loop additionally bounds how many new dials we start
	// in one announce cycle so a tracker returning a huge peer list can't spawn a
	// goroutine storm. slotsHeld is snapshotted once before the loop (len() is a safe,
	// lock-free read, 0 for nil test sessions) so goroutines that acquire a slot mid-loop
	// are not double-counted against launched — double-counting previously throttled
	// connection ramp-up under load.
	slotsHeld := len(s.outboundSlots)
	launched := 0
	for _, p := range peers {
		if p.Port == 0 || p.IP == nil || p.IP.IsUnspecified() {
			continue
		}
		peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		s.mu.Lock()
		if s.closed || s.paused {
			s.mu.Unlock()
			break
		}
		if slotsHeld+launched >= maxOutboundPeers {
			s.mu.Unlock()
			break
		}
		pState, exists := s.Peers[peerAddr]
		shouldDial := false
		if !exists {
			shouldDial = true
		} else {
			// A tracker response is authoritative evidence that this endpoint is
			// dialable, even if the same address was first seen as an inbound peer.
			pState.Dialable = true
			if !pState.Active && !pState.Dialing && time.Since(pState.LastAttempt) > peerRedialBackoff {
				shouldDial = true
			}
		}
		if shouldDial {
			if !exists {
				s.prunePeersLocked()
				s.Peers[peerAddr] = &PeerState{
					IP:          p.IP.String(),
					Port:        p.Port,
					Choked:      true,
					Active:      false,
					AmChoking:   true,
					LastAttempt: time.Now(),
					Dialable:    true,
					Dialing:     true,
				}
			} else {
				s.Peers[peerAddr].LastAttempt = time.Now()
				s.Peers[peerAddr].Dialing = true
			}
			s.wg.Add(1)
			launched++
			go func(tp tracker.Peer) {
				defer s.wg.Done()
				s.connectToPeer(tp)
			}(p)
		}
		s.mu.Unlock()
	}

	return interval
}

// peerMaintenanceLoop periodically refills the outbound connection set from the
// known-peer map, so a slot freed by a dropped or reaped peer is reused promptly
// instead of waiting for the next tracker announce (up to an hour out) or DHT
// lookup. This is what keeps a slow swarm churning toward productive peers rather
// than wedging at zero once the initial connections go stale.
func (s *Session) peerMaintenanceLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(peerMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.maintainPeerConnections()
		case <-s.ctx.Done():
			return
		}
	}
}

// maintainPeerConnections dials known-but-disconnected peers up to the outbound cap
// while we still need data (pieces or metadata). It mirrors the dial gating in
// announceAndConnect: it respects the per-session slot count and the per-peer redial
// backoff, and launches connectToPeer (which acquires the real per-session and
// manager-wide slots) in its own goroutine so the dial never blocks under s.mu.
func (s *Session) maintainPeerConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.paused || s.closed || !s.started {
		return
	}
	// Only maintain download connections while there is still something to fetch.
	// When seeding, inbound connections and the normal announce flow cover uploads;
	// isCompletedLocked is false in metadata mode, so metadata fetches still churn.
	if s.isCompletedLocked() {
		return
	}

	slotsHeld := len(s.outboundSlots)
	// Also bound launches by the manager-wide pool's free room (a lock-free hint): if
	// the global pool is full, connectToPeer would acquire nothing and return without
	// dialing, yet the pre-set LastAttempt below would still suppress the peer for a
	// full backoff. Gating here avoids burning the backoff on dials that can't happen.
	globalRoom := maxOutboundPeers
	if s.globalOutboundSlots != nil {
		globalRoom = cap(s.globalOutboundSlots) - len(s.globalOutboundSlots)
	}

	launched := 0
	now := time.Now()
	for _, ps := range s.Peers {
		if slotsHeld+launched >= maxOutboundPeers || launched >= globalRoom {
			break
		}
		// Skip connected peers, attempts already in flight, and inbound-only source
		// endpoints whose ports were never advertised as listening ports.
		if ps.Active || ps.Dialing || !ps.Dialable {
			continue
		}
		// Eligible to (re)dial once the backoff has elapsed. A zero LastAttempt means
		// "dial now" (e.g. Resume clears it on every inactive peer); the dedup against a
		// concurrent dial is the LastAttempt = now set below, under the lock.
		if !ps.LastAttempt.IsZero() && now.Sub(ps.LastAttempt) <= peerRedialBackoff {
			continue
		}
		ip := net.ParseIP(ps.IP)
		if ip == nil || ip.IsUnspecified() || ps.Port == 0 {
			continue
		}
		ps.LastAttempt = now
		ps.Dialing = true
		launched++
		s.wg.Add(1)
		go func(tp tracker.Peer) {
			defer s.wg.Done()
			s.connectToPeer(tp)
		}(tracker.Peer{IP: ip, Port: ps.Port})
	}
}

// connectToPeer dials a peer and runs the message loop.
// P2 FIX: Uses DialContext for context-aware cancellation.
func (s *Session) connectToPeer(p tracker.Peer) {
	peerAddr := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
	s.mu.RLock()
	dialPauseEpoch := s.pauseEpoch
	s.mu.RUnlock()
	acquiredSlots := false
	defer func() {
		s.mu.Lock()
		if ps, ok := s.Peers[peerAddr]; ok && ps.Dialing {
			ps.Dialing = false
			// A full per-session or manager-wide pool means no network attempt was
			// made. Keep the peer immediately eligible instead of burning a full
			// redial backoff because a lock-free capacity hint raced another session.
			resumedDuringDial := s.pauseEpoch != dialPauseEpoch && !s.paused && !s.closed
			if (!acquiredSlots || resumedDuringDial) && !ps.Active {
				ps.LastAttempt = time.Time{}
			}
		}
		s.mu.Unlock()
	}()

	// Acquire an outbound slot so concurrent dials stay bounded (see maxOutboundPeers).
	// outboundSlots is nil only for sessions built outside NewSession (tests), which
	// stay unbounded. Bail without opening a socket when at capacity; the peer is
	// retried after its normal backoff.
	if s.outboundSlots != nil {
		select {
		case s.outboundSlots <- struct{}{}:
			defer func() { <-s.outboundSlots }()
		default:
			return
		}
	}
	// Also hold a manager-wide outbound slot so many torrents can't collectively
	// exhaust file descriptors. If the global budget is full, the per-session slot
	// above is released by its deferred receive when we return.
	if s.globalOutboundSlots != nil {
		select {
		case s.globalOutboundSlots <- struct{}{}:
			defer func() { <-s.globalOutboundSlots }()
		default:
			return
		}
	}
	acquiredSlots = true

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
		ps.Dialable = true
		ps.Dialing = false
	}
	s.mu.Unlock()

	s.runPeerMessageLoop(client, conn, peerAddr, p.IP.String(), p.Port, handshake.Reserved, true)
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

	// Bound concurrent inbound connections (see maxInboundPeers); drop new ones once
	// we're at capacity. This is a separate budget from outbound dials, so an inbound
	// flood can never starve our own downloads.
	if s.inboundSlots != nil {
		select {
		case s.inboundSlots <- struct{}{}:
			defer func() { <-s.inboundSlots }()
		default:
			return
		}
	}
	// Also hold a manager-wide inbound slot (released by the per-session deferred
	// receive if the global budget is full).
	if s.globalInboundSlots != nil {
		select {
		case s.globalInboundSlots <- struct{}{}:
			defer func() { <-s.globalInboundSlots }()
		default:
			return
		}
	}

	s.serveIncomingConnection(conn, nil)
}

// handleRoutedIncomingConnection serves a connection whose handshake was parsed
// by the manager's shared listener. The manager already holds the global inbound
// slot, so only the per-session budget is acquired here.
func (s *Session) handleRoutedIncomingConnection(conn net.Conn, handshake *peer.Handshake) {
	if s.inboundSlots != nil {
		select {
		case s.inboundSlots <- struct{}{}:
			defer func() { <-s.inboundSlots }()
		default:
			return
		}
	}
	s.serveIncomingConnection(conn, handshake)
}

func (s *Session) serveIncomingConnection(conn net.Conn, handshake *peer.Handshake) {
	tunePeerConn(conn)

	s.mu.RLock()
	paused := s.paused
	closed := s.closed
	s.mu.RUnlock()
	if paused || closed {
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

	if handshake == nil {
		var err error
		handshake, err = peer.ParseHandshake(conn)
		if err != nil {
			return
		}
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
	if _, err := conn.Write(respHs.Serialize()); err != nil {
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

	s.runPeerMessageLoop(client, conn, peerAddr, host, uint16(portVal), handshake.Reserved, false)
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

func (s *Session) runPeerMessageLoop(client *peer.Client, conn net.Conn, peerAddr string, ip string, port uint16, peerReserved [8]byte, outbound bool) {
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
	// An outbound connection confirms this is a listening endpoint. An inbound
	// connection does not erase prior tracker/DHT evidence for the same endpoint.
	if outbound {
		pState.Dialable = true
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
		isComplete := s.isCompletedLocked()
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
		if isComplete {
			_ = client.SendNotInterested()
		} else {
			_ = client.SendInterested()
		}
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
	// Drop this peer's contribution to swarm piece availability on exit. peerBitfield
	// accumulates exactly the pieces we counted (bitfield delta + Haves), so the
	// closure reads its final value here. (#7, rarest-first.)
	defer func() { s.removePeerAvailability(peerBitfield) }()

	// A peer downloads several pieces at once (activeDownloads, filled in slice
	// order so earlier pieces complete first). The request window spans all of
	// them, so in-flight depth is bounded by maxPendingBlockRequests rather than
	// by a single piece's block count — the key throughput fix for small pieces.
	type activeDownload struct {
		pieceIndex     int64
		hash           [20]byte
		length         int64
		numBlocks      int64
		blocks         [][]byte                // received block data, nil until received
		pending        map[int64]*blockRequest // begin offset -> request
		blocksReceived int64
		nextBlock      int64   // index of the next never-requested block (cursor)
		retry          []int64 // begin offsets of timed-out requests awaiting re-send
		// endgame is set when this is a redundant copy of a piece another peer already
		// holds open (#8). The piece's PieceDownloading state is owned by that other
		// peer, so this copy never returns the piece to the pool on release.
		endgame bool
	}
	var activeDownloads []*activeDownload

	findDownload := func(index int64) *activeDownload {
		for _, dl := range activeDownloads {
			if dl.pieceIndex == index {
				return dl
			}
		}
		return nil
	}
	removeDownload := func(index int64) {
		for i, dl := range activeDownloads {
			if dl.pieceIndex == index {
				activeDownloads = append(activeDownloads[:i], activeDownloads[i+1:]...)
				return
			}
		}
	}
	// releaseDownloads returns still-in-progress pieces to PieceEmpty so other
	// peers can re-pick them (used on choke and on disconnect).
	releaseDownloads := func(dls []*activeDownload) {
		if len(dls) == 0 {
			return
		}
		s.mu.Lock()
		for _, dl := range dls {
			if dl.endgame {
				continue // redundant endgame copy; the owning peer holds the piece state
			}
			if dl.pieceIndex >= 0 && dl.pieceIndex < int64(len(s.PieceStates)) &&
				s.PieceStates[dl.pieceIndex] == PieceDownloading {
				s.PieceStates[dl.pieceIndex] = PieceEmpty
				s.addNeededLocked(int(dl.pieceIndex))
			}
		}
		s.mu.Unlock()
	}

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

	// openNewPiece claims the highest-priority, rarest empty wanted piece this peer has
	// and marks it PieceDownloading. In endgame (no fresh pieces left to claim) it
	// instead returns a redundant copy of an in-progress piece this peer has, leaving
	// that piece's state owned by the original downloader. Returns nil when the peer
	// has nothing left for us.
	openNewPiece := func() *activeDownload {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.paused || s.closed {
			return nil
		}
		endgame := false
		bestIdx := s.selectNeededPieceLocked(hasPiece)
		if bestIdx == -1 {
			if s.endgameActiveLocked() {
				owned := make(map[int64]bool, len(activeDownloads))
				for _, dl := range activeDownloads {
					owned[dl.pieceIndex] = true
				}
				bestIdx = s.selectEndgamePieceLocked(hasPiece, owned)
				endgame = true
			}
			if bestIdx == -1 {
				return nil
			}
		}
		if !endgame {
			s.PieceStates[bestIdx] = PieceDownloading
			s.removeNeededLocked(bestIdx)
		}
		numBlocks := s.blocksInPiece(int64(bestIdx))
		return &activeDownload{
			pieceIndex: int64(bestIdx),
			hash:       s.Torrent.PieceHashes[bestIdx],
			length:     s.Storage.PieceLength(int64(bestIdx)),
			numBlocks:  numBlocks,
			blocks:     make([][]byte, numBlocks),
			pending:    make(map[int64]*blockRequest),
			endgame:    endgame,
		}
	}

	// nextBlockInPiece returns the begin offset of the next block to request from
	// dl (re-sending timed-out blocks first, then advancing the fresh cursor),
	// creating/arming its blockRequest, or -1 when the piece is fully requested.
	nextBlockInPiece := func(dl *activeDownload, now time.Time) int64 {
		for len(dl.retry) > 0 {
			begin := dl.retry[len(dl.retry)-1]
			dl.retry = dl.retry[:len(dl.retry)-1]
			if req, ok := dl.pending[begin]; ok && !req.requested && !req.received {
				req.requested = true
				req.requestedAt = now
				return begin
			}
		}
		for dl.nextBlock < dl.numBlocks {
			b := dl.nextBlock
			dl.nextBlock++
			begin := b * BlockSize
			if _, exists := dl.pending[begin]; exists {
				continue
			}
			blockLen := int64(BlockSize)
			if begin+blockLen > dl.length {
				blockLen = dl.length - begin
			}
			dl.pending[begin] = &blockRequest{
				pieceIndex:  dl.pieceIndex,
				begin:       begin,
				length:      blockLen,
				requested:   true,
				requestedAt: now,
			}
			return begin
		}
		return -1
	}
	requestable := func(dl *activeDownload) bool {
		return len(dl.retry) > 0 || dl.nextBlock < dl.numBlocks
	}

	// These timestamps are owned by this peer goroutine. A request gives the peer a
	// fresh stall-timeout window even if rate limiting delayed issuing it; a received
	// block records actual forward progress.
	lastProgressAt := time.Now()
	lastRequestAt := time.Time{}
	waitingForBandwidth := false

	// pump re-arms timed-out requests, then fills the request window across all
	// active pieces, opening new pieces as needed. Called after each inbound
	// message — INCLUDING keep-alives — so the pipeline stays full across piece
	// boundaries and, crucially, so the timeout sweep below still runs when a peer
	// that already took our requests goes quiet but keeps the socket warm with
	// keep-alives (which otherwise reset the read deadline and skipped pump).
	pump := func() time.Duration {
		// Block requests below are queued into the client's write buffer; flush the
		// whole burst in one syscall on the way out, regardless of which branch
		// returns. Flushing an empty buffer is a no-op, so this is cheap on the
		// paused/choked/keep-alive paths that write nothing.
		defer func() { _ = client.Flush() }()

		s.mu.RLock()
		paused := s.paused
		choked := pState.Choked
		s.mu.RUnlock()
		if paused {
			waitingForBandwidth = false
			return 0
		}

		now := time.Now()

		// Re-arm timed-out requests, or drop a peer that has stalled past its retry
		// budget, and count what is still outstanding. This sweep runs even when the
		// peer is choking us: a peer that unchoked us, took a window of requests, then
		// re-choked (or simply stopped responding) still has outstanding requests that
		// must be timed out so the connection is dropped instead of held forever.
		outstanding := 0
		for _, dl := range activeDownloads {
			for begin, req := range dl.pending {
				if !req.requested || req.received {
					continue
				}
				if now.Sub(req.requestedAt) >= blockRequestTimeout {
					if req.retries >= maxBlockRequestRetries {
						s.mu.Lock()
						s.lastErr = fmt.Errorf("timed out downloading piece %d", dl.pieceIndex)
						s.mu.Unlock()
						_ = conn.Close() // pieces are released by the disconnect cleanup
						return 0
					}
					req.requested = false
					req.retries++
					dl.retry = append(dl.retry, begin)
					continue
				}
				outstanding++
			}
		}

		// A choked peer won't fulfill new requests, so don't open pieces or send;
		// the timeout sweep above has already run, which is the part that matters
		// for not leaking a stalled connection.
		if choked {
			waitingForBandwidth = false
			return 0
		}

		// Fill the window, opening pieces on demand.
		for outstanding < maxPendingBlockRequests {
			var chosen *activeDownload
			var begin int64 = -1
			for _, dl := range activeDownloads {
				if !requestable(dl) {
					continue
				}
				if b := nextBlockInPiece(dl, now); b != -1 {
					chosen, begin = dl, b
					break
				}
			}
			if chosen == nil {
				if len(activeDownloads) >= maxConcurrentPiecesPerPeer {
					break
				}
				newDL := openNewPiece()
				if newDL == nil {
					break
				}
				activeDownloads = append(activeDownloads, newDL)
				continue
			}

			req := chosen.pending[begin]
			// Never wait for bandwidth in the peer event loop: even with an empty
			// request window, blocking here lets many rate-limited peers occupy every
			// manager-wide connection slot while none of their sockets are drained.
			// The event loop schedules another pump when the limiter says tokens should
			// be available, while the dedicated reader below remains responsive.
			if reserved, retryAfter := s.reserveDownload(int(req.length)); !reserved {
				req.requested = false
				chosen.retry = append(chosen.retry, begin)
				// With no request in flight, this idle period is intentional: the
				// limiter is accumulating enough tokens for one full block. If other
				// requests are outstanding, the peer still owes us data and remains
				// subject to the normal stall and request timeouts.
				waitingForBandwidth = outstanding == 0
				return retryAfter
			}
			if err := client.WriteRequest(uint32(chosen.pieceIndex), uint32(begin), uint32(req.length)); err != nil {
				req.requested = false
				return 0 // dead connection; cleanup releases the pieces
			}
			sentAt := time.Now()
			req.requestedAt = sentAt
			lastRequestAt = sentAt
			waitingForBandwidth = false
			outstanding++
		}
		waitingForBandwidth = false
		return 0
	}

	// dropCompletedElsewhere is the endgame "cancel on receipt" path: it drops any
	// in-progress piece that another peer has finished (so its state is no longer
	// PieceDownloading) and sends a Cancel for each of our still-outstanding blocks so
	// the peer stops feeding us data the swarm no longer needs. Bounded by
	// maxConcurrentPiecesPerPeer, so it is cheap to run every message.
	dropCompletedElsewhere := func() {
		if len(activeDownloads) == 0 {
			return
		}
		var finished []int64
		s.mu.RLock()
		for _, dl := range activeDownloads {
			if dl.pieceIndex < 0 || dl.pieceIndex >= int64(len(s.PieceStates)) ||
				s.PieceStates[dl.pieceIndex] != PieceDownloading {
				finished = append(finished, dl.pieceIndex)
			}
		}
		s.mu.RUnlock()
		for _, idx := range finished {
			dl := findDownload(idx)
			if dl == nil {
				continue
			}
			for begin, req := range dl.pending {
				if req.requested && !req.received {
					_ = client.SendCancel(uint32(idx), uint32(begin), uint32(req.length))
				}
			}
			removeDownload(idx)
		}
	}

	type peerReadResult struct {
		msg *peer.Message
		err error
	}
	readCh := make(chan peerReadResult, 1)
	readDone := make(chan struct{})
	readStop := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			msg, err := client.ReadMessage()
			select {
			case readCh <- peerReadResult{msg: msg, err: err}:
			case <-s.ctx.Done():
				return
			case <-readStop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		close(readStop)
		_ = conn.Close()
		<-readDone
	}()

	var rateTimer *time.Timer
	var rateRetry <-chan time.Time
	scheduleRateRetry := func(delay time.Duration) {
		if rateTimer != nil {
			if !rateTimer.Stop() {
				select {
				case <-rateTimer.C:
				default:
				}
			}
		}
		rateRetry = nil
		if delay <= 0 {
			return
		}
		if rateTimer == nil {
			rateTimer = time.NewTimer(delay)
		} else {
			rateTimer.Reset(delay)
		}
		rateRetry = rateTimer.C
	}
	defer func() {
		if rateTimer != nil {
			rateTimer.Stop()
		}
	}()

	// Read and scheduling event loop. Socket parsing stays in one dedicated goroutine,
	// while limiter retry timers can wake the request pump without interrupting a
	// partially-read peer-wire message.
peerLoop:
	for {
		s.mu.RLock()
		paused := s.paused
		s.mu.RUnlock()
		if paused {
			break
		}

		// Reap an unproductive peer: drop a connection that hasn't delivered a block
		// within peerStallTimeout so its outbound slot can be reused for a peer that
		// will. Gated by cheap checks that keep the O(pieces) completion check off the
		// per-message hot path: only OUTBOUND connections (an inbound peer holds an
		// inbound slot, not an outbound one, and is keyed by an ephemeral port we can't
		// redial — reaping it just drops a productive uploader). A recently issued
		// request also grants a fresh timeout window, which prevents an intentionally
		// slow limiter wait from making the request look stale before it is sent.
		lastUsefulAt := lastProgressAt
		if lastRequestAt.After(lastUsefulAt) {
			lastUsefulAt = lastRequestAt
		}
		if outbound && !waitingForBandwidth && time.Since(lastUsefulAt) > peerStallTimeout {
			s.mu.RLock()
			seeding := s.isCompletedLocked()
			// Background resume verification can hold pieces PieceUnverified, so there
			// may be nothing to request yet through no fault of the peer; don't reap
			// while verifying.
			verifying := s.verifying
			s.mu.RUnlock()
			if !seeding && !verifying {
				break
			}
		}

		var msg *peer.Message
		select {
		case result := <-readCh:
			if result.err != nil {
				break peerLoop
			}
			msg = result.msg
		case <-rateRetry:
			rateRetry = nil
			scheduleRateRetry(pump())
			continue
		case <-s.ctx.Done():
			break peerLoop
		}

		if msg == nil {
			// Keep alive: still run pump so outstanding requests to a now-silent peer
			// time out (and the peer is dropped after its retry budget) instead of the
			// keep-alive merely resetting the read deadline and stalling forever.
			scheduleRateRetry(pump())
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
			isComplete := s.isCompletedLocked()
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
			if isComplete {
				_ = client.SendNotInterested()
			} else {
				_ = client.SendInterested()
			}

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
								lastProgressAt = time.Now() // metadata progress; keeps the stall reaper off

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
			// A choked peer won't fulfill our requests; return the in-progress
			// pieces so other peers can grab them. We re-pick on unchoke.
			releaseDownloads(activeDownloads)
			activeDownloads = nil

		case peer.MsgUnchoke:
			s.mu.Lock()
			pState.Choked = false
			s.mu.Unlock()

		case peer.MsgInterested:
			s.mu.Lock()
			pState.Interested = true
			unchokedInterested := 0
			for _, candidate := range s.Peers {
				if candidate.Active && candidate.Interested && !candidate.AmChoking {
					unchokedInterested++
				}
			}
			shouldUnchoke := pState.AmChoking && unchokedInterested < 4
			if shouldUnchoke {
				pState.AmChoking = false
			}
			s.mu.Unlock()
			if shouldUnchoke {
				_ = client.SendUnchoke()
			}

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
				mask := byte(1 << (7 - bitIndex))
				if peerBitfield[byteIndex]&mask == 0 {
					peerBitfield[byteIndex] |= mask
					s.addPieceAvailability(int(index))
				}
			}

		case peer.MsgBitfield:
			expectedLen := (numPiecesNow + 7) / 8
			if expectedLen == 0 || len(msg.Payload) != expectedLen {
				continue
			}
			if len(peerBitfield) != expectedLen {
				peerBitfield = make([]byte, expectedLen)
			}
			oldBF := append([]byte(nil), peerBitfield...)
			copy(peerBitfield, msg.Payload)
			s.applyBitfieldAvailability(oldBF, peerBitfield)

		case peer.MsgPiece:
			if len(msg.Payload) < 8 {
				continue
			}
			index := int64(binary.BigEndian.Uint32(msg.Payload[0:4]))
			begin := int64(binary.BigEndian.Uint32(msg.Payload[4:8]))
			blockData := msg.Payload[8:]

			// Validate against our outstanding requests for this piece.
			dl := findDownload(index)
			if dl == nil {
				continue // not a piece we're currently downloading; discard
			}

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
			if blockIndex >= int64(len(dl.blocks)) {
				continue
			}
			dl.blocks[blockIndex] = blockData
			req.received = true
			dl.blocksReceived++
			lastProgressAt = time.Now() // forward progress; keeps the stall reaper off

			// Counters are bumped lock-free on this hot path; s.mu would
			// otherwise be taken per 16 KB block by every peer goroutine.
			s.Downloaded.Add(int64(len(blockData)))
			atomic.AddInt64(&pState.Downloaded, int64(len(blockData)))

			if dl.blocksReceived != dl.numBlocks {
				break // piece not complete yet; pump tops up at the loop bottom
			}

			// Piece complete: assemble the buffer and hand it to the async hash/write
			// pool. The peer goroutine keeps draining the socket and requesting instead
			// of stalling on sha1 + WriteBlock + the fast-resume persist. The pool
			// verifies the hash, writes, persists state, and — on a hash failure —
			// disconnects this peer (via its conn) and returns the piece to the pool.
			pieceData := make([]byte, dl.length)
			var offset int64
			validPiece := true
			for b := int64(0); b < dl.numBlocks; b++ {
				block := dl.blocks[b]
				if block == nil || offset+int64(len(block)) > int64(len(pieceData)) {
					validPiece = false
					break
				}
				copy(pieceData[offset:], block)
				offset += int64(len(block))
			}

			pieceIdx := dl.pieceIndex
			pieceHash := dl.hash
			removeDownload(dl.pieceIndex)

			if !validPiece || offset != int64(len(pieceData)) {
				// Assembly invariant violated (shouldn't happen): return to the pool.
				s.mu.Lock()
				if pieceIdx >= 0 && pieceIdx < int64(len(s.PieceStates)) && s.PieceStates[pieceIdx] == PieceDownloading {
					s.PieceStates[pieceIdx] = PieceEmpty
					s.addNeededLocked(int(pieceIdx))
				}
				s.mu.Unlock()
				break
			}

			s.ensurePieceWritePool()
			select {
			case s.pieceWriteCh <- pieceWriteJob{index: pieceIdx, hash: pieceHash, data: pieceData, conn: conn}:
			case <-s.ctx.Done():
				return
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
						if err := client.SendPiece(uint32(index), uint32(begin), buf); err == nil {
							// Lock-free counter update (see the download hot path above).
							s.Uploaded.Add(length)
							atomic.AddInt64(&pState.Uploaded, length)
						}
					}
				}
			}
		}

		// Cancel and drop pieces another peer finished (endgame), then keep the
		// request pipeline full across all active pieces, opening new pieces as
		// needed (pump no-ops when paused, choked, or seeding).
		dropCompletedElsewhere()
		scheduleRateRetry(pump())
	}

	// If we disconnected while holding pieces, return them to empty so other
	// peers can fetch them.
	releaseDownloads(activeDownloads)
}

// GetActivePeers returns a slice of active peer states for TUI updates.
func (s *Session) GetActivePeers() []PeerState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []PeerState
	for _, p := range s.Peers {
		if p.Active {
			// Build the snapshot field-by-field rather than copying *p: the
			// Downloaded/Uploaded counters are written lock-free on the peer hot
			// path, so a whole-struct copy would race them (the copy reads those
			// words non-atomically). Every other field is guarded by s.mu, held
			// here; the two counters are loaded atomically.
			list = append(list, PeerState{
				IP:            p.IP,
				Port:          p.Port,
				Choked:        p.Choked,
				Interested:    p.Interested,
				DownloadSpeed: p.DownloadSpeed,
				UploadSpeed:   p.UploadSpeed,
				Downloaded:    atomic.LoadInt64(&p.Downloaded),
				Uploaded:      atomic.LoadInt64(&p.Uploaded),
				Active:        p.Active,
				AmChoking:     p.AmChoking,
				LastAttempt:   p.LastAttempt,
			})
		}
	}
	return list
}

// UploadPeerStats summarizes whether connected peers currently want data from us.
type UploadPeerStats struct {
	Connected  int
	Interested int
	Unchoked   int
}

func (s *Session) GetUploadPeerStats() UploadPeerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stats UploadPeerStats
	for _, p := range s.Peers {
		if !p.Active {
			continue
		}
		stats.Connected++
		if p.Interested {
			stats.Interested++
			if !p.AmChoking {
				stats.Unchoked++
			}
		}
	}
	return stats
}

// TrackerSwarmStats returns the largest seed/leecher counts from the latest
// successful tracker announce cycle.
func (s *Session) TrackerSwarmStats() (seeders, leechers int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.trackerSeeders, s.trackerLeechers
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

// reserveDownload non-blockingly reserves n bytes from the per-session and (if set)
// manager-wide limiter. retryAfter estimates when a failed reservation should be
// retried; a charged per-session reservation is refunded if the global limiter fails.
func (s *Session) reserveDownload(n int) (reserved bool, retryAfter time.Duration) {
	localOK, localCharged, localRetry := s.DownloadLimiter.tryReserve(n)
	if !localOK {
		return false, localRetry
	}
	if s.GlobalDownloadLimiter != nil {
		globalOK, _, globalRetry := s.GlobalDownloadLimiter.tryReserve(n)
		if !globalOK {
			if localCharged {
				s.DownloadLimiter.refund(n)
			}
			return false, globalRetry
		}
	}
	return true, 0
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
	} else {
		// Discovery supplies a listening endpoint, so an inbound-only entry with the
		// same address becomes eligible for maintenance retries.
		pState.Dialable = true
		if !pState.Active && !pState.Dialing && time.Since(pState.LastAttempt) > peerRedialBackoff {
			shouldDial = true
		}
	}

	// Don't exceed the outbound connection cap.
	if shouldDial && len(s.outboundSlots) >= maxOutboundPeers {
		shouldDial = false
	}

	if shouldDial {
		if !exists {
			s.prunePeersLocked()
			s.Peers[peerAddr] = &PeerState{
				IP:          host,
				Port:        uint16(port),
				AmChoking:   true,
				Choked:      true,
				LastAttempt: time.Now(),
				Dialable:    true,
				Dialing:     true,
			}
		} else {
			s.Peers[peerAddr].LastAttempt = time.Now()
			s.Peers[peerAddr].Dialing = true
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
	uploaded := s.Uploaded.Load()
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
