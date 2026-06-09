// Package downloader implements torrent session management, state persistence,
// and speed rate limiting.
package downloader

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"sainttorrent/pkg/bencode"
	"sainttorrent/pkg/dht"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// TorrentManager manages multiple Torrent Session instances, allowing
// control over their lifecycles and enforcing global upload/download rate limits.
type TorrentManager struct {
	mu                    sync.RWMutex
	sessions              map[string]*Session
	globalDownloadLimiter *RateLimiter
	globalUploadLimiter   *RateLimiter
	dht                   *dht.DHT
	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
	closed                bool

	stateDir       string
	restoring      bool
	writeMu        sync.Mutex
	failedTorrents []PersistedTorrent
}

// NewTorrentManager creates and initializes a TorrentManager.
func NewTorrentManager() *TorrentManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &TorrentManager{
		sessions:              make(map[string]*Session),
		globalDownloadLimiter: NewRateLimiter(0), // unlimited by default
		globalUploadLimiter:   NewRateLimiter(0), // unlimited by default
		ctx:                   ctx,
		cancel:                cancel,
	}
}

// StartDHT starts the global Kademlia DHT client.
func (m *TorrentManager) StartDHT(downloadDir string, listenPort int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dht != nil {
		return nil
	}

	d, err := dht.NewDHT(downloadDir, listenPort)
	if err != nil {
		return err
	}
	m.dht = d

	// Route discovered peers to sessions
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			select {
			case peer, ok := <-d.PeerChan():
				if !ok {
					return
				}
				m.mu.RLock()
				sess, exists := m.sessions[fmt.Sprintf("%x", peer.InfoHash)]
				m.mu.RUnlock()
				if exists {
					addr := net.JoinHostPort(peer.IP.String(), fmt.Sprintf("%d", peer.Port))
					sess.AddPeerFromDiscovery(addr)
				}
			case <-m.ctx.Done():
				return
			}
		}
	}()

	for _, sess := range m.sessions {
		sess.AttachDHT(d)
	}

	return nil
}

// DHT returns the global DHT instance.
func (m *TorrentManager) DHT() *dht.DHT {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.dht
}

// AddSession adds a session to the manager. If global rate limiters or DHT are set on the manager,
// they are automatically linked to the session.
func (m *TorrentManager) AddSession(infoHashHex string, sess *Session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.sessions[infoHashHex] = sess
	sess.mu.Lock()
	if sess.AddedAt.IsZero() {
		sess.AddedAt = time.Now()
	}
	sess.GlobalDownloadLimiter = m.globalDownloadLimiter
	sess.GlobalUploadLimiter = m.globalUploadLimiter
	sess.mu.Unlock()

	if m.dht != nil {
		sess.AttachDHT(m.dht)
	}
}

// RemoveSession stops the session associated with the given info hash, removes it from the manager,
// and deletes state files. If deleteFiles is true, it also deletes the downloaded files.
// It returns any aggregated errors encountered during the removal process.
func (m *TorrentManager) RemoveSession(infoHashHex string, deleteFiles bool) error {
	m.mu.Lock()
	sess, ok := m.sessions[infoHashHex]
	if ok {
		delete(m.sessions, infoHashHex)
	}
	var newFailed []PersistedTorrent
	failedRemoved := false
	var failedEntry PersistedTorrent
	for _, failed := range m.failedTorrents {
		if !strings.EqualFold(failed.InfoHashHex, infoHashHex) {
			newFailed = append(newFailed, failed)
		} else {
			failedRemoved = true
			failedEntry = failed
		}
	}
	m.failedTorrents = newFailed
	stateDir := m.stateDir
	m.mu.Unlock()

	if !ok && !failedRemoved {
		return fmt.Errorf("torrent with info hash %s not found", infoHashHex)
	}

	var downloadDir string
	var torrentFiles []torrent.File
	var closeSession func()

	if ok {
		sess.mu.RLock()
		if sess.Storage != nil {
			downloadDir = sess.Storage.BaseDir()
		} else {
			downloadDir = sess.downloadDir
		}
		if sess.Torrent != nil {
			torrentFiles = sess.Torrent.Files
		}
		sess.mu.RUnlock()
		closeSession = sess.Close
	} else if failedRemoved {
		downloadDir = failedEntry.DownloadDir
	}

	var errs []error

	// 1. Close session if active (this will block until all goroutines exit)
	if closeSession != nil {
		closeSession()
	}

	// 2. Delete the fast-resume state file
	if downloadDir != "" {
		resumePath := filepath.Join(downloadDir, "."+infoHashHex+".state")
		if err := os.Remove(resumePath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("failed to delete fast-resume state file: %w", err))
		}
	}

	// 3. Delete downloaded files if requested
	if deleteFiles {
		// For failed restoration entries, try to parse the cached torrent file if files list is empty
		if failedRemoved && len(torrentFiles) == 0 && stateDir != "" {
			cachedPath := filepath.Join(stateDir, "torrents", infoHashHex+".torrent")
			if torrentData, err := os.ReadFile(cachedPath); err == nil {
				if tor, err := torrent.Parse(torrentData); err == nil {
					torrentFiles = tor.Files
				} else {
					errs = append(errs, fmt.Errorf("failed to parse cached torrent file for file list: %w", err))
				}
			} else if !os.IsNotExist(err) {
				errs = append(errs, fmt.Errorf("failed to read cached torrent file for file list: %w", err))
			} else {
				errs = append(errs, fmt.Errorf("cannot delete files: cached torrent file is missing"))
			}
		}

		if downloadDir == "" {
			errs = append(errs, fmt.Errorf("cannot delete files: download directory is empty"))
		} else if len(torrentFiles) > 0 {
			for _, f := range torrentFiles {
				relPath := filepath.Join(f.Path...)
				absPath, err := storage.ResolveAndValidatePath(downloadDir, relPath)
				if err != nil {
					errs = append(errs, fmt.Errorf("safe path validation failed for %s: %w", relPath, err))
					continue
				}

				if err := os.Remove(absPath); err != nil && !os.IsNotExist(err) {
					errs = append(errs, fmt.Errorf("failed to delete file %s: %w", relPath, err))
				}

				// Clean up empty parent folders up to downloadDir
				parent := filepath.Dir(absPath)
				canonicalDownloadDir, err := filepath.EvalSymlinks(downloadDir)
				if err != nil {
					canonicalDownloadDir, err = filepath.Abs(downloadDir)
					if err != nil {
						canonicalDownloadDir = downloadDir
					}
				}
				cleanDownloadDir := filepath.Clean(canonicalDownloadDir)

				for parent != cleanDownloadDir && parent != "." && parent != "/" {
					if err := os.Remove(parent); err != nil {
						if pe, ok := err.(*os.PathError); ok {
							if errno, ok := pe.Err.(syscall.Errno); ok {
								// ENOTEMPTY (Unix) or 145 (Windows ERROR_DIR_NOT_EMPTY) is expected behavior for non-empty dirs.
								if errno == syscall.ENOTEMPTY || errno == 145 {
									break
								}
							}
						}
						if os.IsNotExist(err) {
							parent = filepath.Dir(parent)
							continue
						}
						// Aggregate other unexpected errors (like permission issues) and stop propagating
						errs = append(errs, fmt.Errorf("failed to clean up empty directory %s: %w", parent, err))
						break
					}
					parent = filepath.Dir(parent)
				}
			}
		}
	}

	// 4. Delete the cached .torrent file
	if stateDir != "" {
		cachedPath := filepath.Join(stateDir, "torrents", infoHashHex+".torrent")
		if err := os.Remove(cachedPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("failed to delete cached torrent file: %w", err))
		}
	}

	m.saveState()

	if len(errs) > 0 {
		var errStrs []string
		for _, e := range errs {
			errStrs = append(errStrs, e.Error())
		}
		return fmt.Errorf("removal completed with errors: %s", strings.Join(errStrs, "; "))
	}

	return nil
}

// GetSession retrieves a session by its info hash hex string.
func (m *TorrentManager) GetSession(infoHashHex string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[infoHashHex]
}

// ListSessions returns a slice of all managed sessions, stably sorted.
func (m *TorrentManager) ListSessions() []*Session {
	m.mu.RLock()
	list := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		list = append(list, sess)
	}
	m.mu.RUnlock()

	type sortKey struct {
		sess        *Session
		statusScore int
		addedAt     time.Time
		name        string
		infoHashHex string
	}

	keys := make([]sortKey, len(list))
	for i, sess := range list {
		snap := sess.GetSortSnapshot()
		keys[i] = sortKey{
			sess:        sess,
			statusScore: snap.StatusScore,
			addedAt:     snap.AddedAt,
			name:        snap.Name,
			infoHashHex: snap.InfoHashHex,
		}
	}

	sort.Slice(keys, func(i, j int) bool {
		ki, kj := keys[i], keys[j]
		if ki.statusScore != kj.statusScore {
			return ki.statusScore < kj.statusScore
		}
		if !ki.addedAt.Equal(kj.addedAt) {
			return ki.addedAt.Before(kj.addedAt)
		}
		nameI := strings.ToLower(ki.name)
		nameJ := strings.ToLower(kj.name)
		if nameI != nameJ {
			return nameI < nameJ
		}
		return ki.infoHashHex < kj.infoHashHex
	})

	for i, k := range keys {
		list[i] = k.sess
	}

	return list
}

// PauseAll pauses all managed sessions.
func (m *TorrentManager) PauseAll() {
	m.mu.RLock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.mu.RUnlock()

	for _, sess := range sessions {
		sess.Pause()
	}
}

// ResumeAll resumes all managed sessions.
func (m *TorrentManager) ResumeAll() {
	m.mu.RLock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.mu.RUnlock()

	for _, sess := range sessions {
		sess.Resume()
	}
}

// SetGlobalDownloadLimit updates the global download bandwidth limit in bytes/sec. 0 = unlimited.
func (m *TorrentManager) SetGlobalDownloadLimit(bytesPerSec int64) {
	m.globalDownloadLimiter.SetLimit(bytesPerSec)
	m.saveState()
}

// SetGlobalUploadLimit updates the global upload bandwidth limit in bytes/sec. 0 = unlimited.
func (m *TorrentManager) SetGlobalUploadLimit(bytesPerSec int64) {
	m.globalUploadLimiter.SetLimit(bytesPerSec)
	m.saveState()
}

// GlobalDownloadLimit returns the current global download limit in bytes/sec.
func (m *TorrentManager) GlobalDownloadLimit() int64 {
	return m.globalDownloadLimiter.Limit()
}

// GlobalUploadLimit returns the current global upload limit in bytes/sec.
func (m *TorrentManager) GlobalUploadLimit() int64 {
	return m.globalUploadLimiter.Limit()
}

// Close closes all managed sessions.
func (m *TorrentManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.cancel()
	m.mu.Unlock()

	m.saveStateInternal(true)

	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.sessions = make(map[string]*Session)
	d := m.dht
	m.dht = nil
	m.mu.Unlock()

	// H1: best-effort, parallel, bounded "stopped" announces. Pre-marks every session as
	// announced so the per-session Close() below never re-attempts a blocking announce.
	m.announceStoppedAll(sessions)

	// H2: close sessions concurrently — teardown becomes ~max(session) instead of the sum.
	var closeWG sync.WaitGroup
	for _, sess := range sessions {
		closeWG.Add(1)
		go func(s *Session) {
			defer closeWG.Done()
			s.Close()
		}(sess)
	}
	closeWG.Wait()

	if d != nil {
		d.Close()
	}
	m.wg.Wait()
}

// stopAnnounceBudget bounds how long shutdown waits for "stopped" tracker announces.
// Responsive trackers answer well within this; a dead one can't stall the exit.
const stopAnnounceBudget = 750 * time.Millisecond

// announceStoppedAll fires "stopped" to every started session's trackers concurrently and
// waits at most stopAnnounceBudget. It is best-effort: a slow or dead tracker can never
// delay shutdown. Every started session is then marked announced so the subsequent
// per-session Close() does not re-attempt a (potentially blocking) announce.
func (m *TorrentManager) announceStoppedAll(sessions []*Session) {
	ctx, cancel := context.WithTimeout(context.Background(), stopAnnounceBudget)
	defer cancel()

	var wg sync.WaitGroup
	for _, sess := range sessions {
		sess.mu.RLock()
		started := sess.started
		already := sess.stoppedAnnounced
		sess.mu.RUnlock()
		if !started || already {
			continue
		}
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			_ = s.announceWithEvent(ctx, "stopped")
		}(sess)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}

	for _, sess := range sessions {
		sess.mu.Lock()
		if sess.started {
			sess.stoppedAnnounced = true
		}
		sess.mu.Unlock()
	}
}

// AddMagnet parses a magnet URI and adds it to the manager as a metadata session.
func (m *TorrentManager) AddMagnet(uri string, downloadDir string) (*Session, error) {
	mag, err := torrent.ParseMagnet(uri)
	if err != nil {
		return nil, err
	}

	absDir, err := filepath.Abs(downloadDir)
	if err == nil {
		downloadDir = absDir
	}

	m.mu.Lock()
	infoHashHex := fmt.Sprintf("%x", mag.InfoHash)
	if s, exists := m.sessions[infoHashHex]; exists {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	tor := &torrent.Torrent{
		InfoHash: mag.InfoHash,
		Name:     mag.Name,
		Trackers: mag.Trackers,
	}
	if tor.Name == "" {
		tor.Name = fmt.Sprintf("magnet-%x", mag.InfoHash[:6])
	}

	var peerID [20]byte
	copy(peerID[:8], "-ST0001-")
	_, _ = rand.Read(peerID[8:])

	sess, err := NewSession(tor, nil, peerID, 0, downloadDir) // port 0 for dynamic
	if err != nil {
		return nil, err
	}

	sess.MagnetURI = uri
	sess.OnStateChange = func() {
		m.saveState()
	}

	m.AddSession(infoHashHex, sess)
	m.saveState()

	return sess, nil
}

// AddTorrentFile parses a bencoded torrent file and adds it to the manager.
func (m *TorrentManager) AddTorrentFile(torrentPath string, downloadDir string) (*Session, error) {
	torrentData, err := os.ReadFile(torrentPath)
	if err != nil {
		return nil, err
	}

	tor, err := torrent.Parse(torrentData)
	if err != nil {
		return nil, err
	}

	absDir, err := filepath.Abs(downloadDir)
	if err == nil {
		downloadDir = absDir
	}

	m.mu.Lock()
	infoHashHex := fmt.Sprintf("%x", tor.InfoHash)
	if s, exists := m.sessions[infoHashHex]; exists {
		m.mu.Unlock()
		return s, nil
	}
	m.mu.Unlock()

	// Prepare storage files
	files := make([]storage.FileInfo, len(tor.Files))
	for i, f := range tor.Files {
		files[i] = storage.FileInfo{
			Path:   filepath.Join(f.Path...),
			Length: f.Length,
		}
	}

	st, err := storage.NewStorage(downloadDir, files, tor.PieceLength)
	if err != nil {
		return nil, err
	}

	var peerID [20]byte
	copy(peerID[:8], "-ST0001-")
	_, _ = rand.Read(peerID[8:])

	sess, err := NewSession(tor, st, peerID, 0, downloadDir) // port 0 for dynamic
	if err != nil {
		st.Close()
		return nil, err
	}

	sess.OnStateChange = func() {
		m.saveState()
	}

	m.mu.Lock()
	stateDir := m.stateDir
	restoring := m.restoring
	m.mu.Unlock()

	if stateDir != "" && !restoring {
		destPath := filepath.Join(stateDir, "torrents", infoHashHex+".torrent")
		if torrentPath != destPath {
			m.writeMu.Lock()
			_ = atomicWriteFile(destPath, torrentData)
			m.writeMu.Unlock()
		}
	}

	m.AddSession(infoHashHex, sess)
	m.saveState()

	return sess, nil
}

type PersistedTorrent struct {
	InfoHashHex    string         `json:"info_hash_hex"`
	MagnetURI      string         `json:"magnet_uri,omitempty"`
	DownloadDir    string         `json:"download_dir"`
	Paused         bool           `json:"paused"`
	FilePriorities []FilePriority `json:"file_priorities,omitempty"`
	AddedAt        *time.Time     `json:"added_at,omitempty"`
}

type PersistedState struct {
	Version             int                `json:"version"`
	GlobalDownloadLimit int64              `json:"global_download_limit"`
	GlobalUploadLimit   int64              `json:"global_upload_limit"`
	Torrents            []PersistedTorrent `json:"torrents"`
}

func (m *TorrentManager) getSnapshotLocked() PersistedState {
	state := PersistedState{
		Version:             1,
		GlobalDownloadLimit: m.globalDownloadLimiter.Limit(),
		GlobalUploadLimit:   m.globalUploadLimiter.Limit(),
		Torrents:            nil,
	}

	for _, sess := range m.sessions {
		sess.mu.RLock()
		infoHashHex := fmt.Sprintf("%x", sess.Torrent.InfoHash)
		var priorities []FilePriority
		if len(sess.FilePriorities) > 0 {
			priorities = make([]FilePriority, len(sess.FilePriorities))
			copy(priorities, sess.FilePriorities)
		} else if len(sess.PendingFilePriorities) > 0 {
			priorities = make([]FilePriority, len(sess.PendingFilePriorities))
			copy(priorities, sess.PendingFilePriorities)
		}
		magnetURI := sess.MagnetURI
		downloadDir := sess.downloadDir
		paused := sess.paused
		addedAt := sess.AddedAt
		sess.mu.RUnlock()

		var addedAtPtr *time.Time
		if !addedAt.IsZero() {
			addedAtCopy := addedAt
			addedAtPtr = &addedAtCopy
		}

		state.Torrents = append(state.Torrents, PersistedTorrent{
			InfoHashHex:    infoHashHex,
			MagnetURI:      magnetURI,
			DownloadDir:    downloadDir,
			Paused:         paused,
			FilePriorities: priorities,
			AddedAt:        addedAtPtr,
		})
	}

	state.Torrents = append(state.Torrents, m.failedTorrents...)

	return state
}

func (m *TorrentManager) saveState() {
	m.saveStateInternal(false)
}

func (m *TorrentManager) saveStateInternal(allowClosed bool) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	m.mu.Lock()
	if m.stateDir == "" || m.restoring || (m.closed && !allowClosed) {
		m.mu.Unlock()
		return
	}
	state := m.getSnapshotLocked()
	stateDir := m.stateDir

	type checkItem struct {
		infoHashHex string
		torrent     *torrent.Torrent
		metadata    bool
	}
	var items []checkItem
	for _, sess := range m.sessions {
		sess.mu.RLock()
		tor := sess.Torrent
		inMetadata := sess.metadataMode
		var infoBytesCopy []byte
		if tor != nil && len(tor.InfoBytes) > 0 {
			infoBytesCopy = make([]byte, len(tor.InfoBytes))
			copy(infoBytesCopy, tor.InfoBytes)
		}
		var torCopy *torrent.Torrent
		if tor != nil {
			torCopy = &torrent.Torrent{
				Announce:  tor.Announce,
				Trackers:  append([]string(nil), tor.Trackers...),
				InfoHash:  tor.InfoHash,
				InfoBytes: infoBytesCopy,
			}
		}
		sess.mu.RUnlock()

		if torCopy != nil {
			items = append(items, checkItem{
				infoHashHex: fmt.Sprintf("%x", torCopy.InfoHash),
				torrent:     torCopy,
				metadata:    inMetadata,
			})
		}
	}
	m.mu.Unlock()

	for _, item := range items {
		if !item.metadata {
			cachedPath := filepath.Join(stateDir, "torrents", item.infoHashHex+".torrent")
			if _, err := os.Stat(cachedPath); os.IsNotExist(err) {
				_ = reconstructAndWriteTorrent(stateDir, item.infoHashHex, item.torrent)
			}
		}
	}

	_ = atomicWriteState(stateDir, state)
}

func reconstructAndWriteTorrent(stateDir string, infoHashHex string, tor *torrent.Torrent) error {
	var buf bytes.Buffer
	buf.WriteByte('d')

	announce := tor.Announce
	if announce == "" && len(tor.Trackers) > 0 {
		announce = tor.Trackers[0]
	}

	// 1. announce
	if announce != "" {
		buf.WriteString("8:announce")
		announceBencoded, err := bencode.Marshal(announce)
		if err != nil {
			return err
		}
		buf.Write(announceBencoded)
	}

	// 2. announce-list
	if len(tor.Trackers) > 0 {
		buf.WriteString("13:announce-list")
		var trackersList [][]string
		for _, tr := range tor.Trackers {
			trackersList = append(trackersList, []string{tr})
		}
		trackersListBencoded, err := bencode.Marshal(trackersList)
		if err != nil {
			return err
		}
		buf.Write(trackersListBencoded)
	}

	// 3. info
	buf.WriteString("4:info")
	buf.Write(tor.InfoBytes)

	buf.WriteByte('e')

	destPath := filepath.Join(stateDir, "torrents", infoHashHex+".torrent")
	return atomicWriteFile(destPath, buf.Bytes())
}

func atomicWriteState(stateDir string, state PersistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	statePath := filepath.Join(stateDir, "session.json")
	return atomicWriteFile(statePath, data)
}

func atomicWriteFile(destPath string, data []byte) error {
	tmpPath := destPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	f.Close()

	if err := os.Rename(tmpPath, destPath); err != nil {
		return err
	}

	parentDir := filepath.Dir(destPath)
	if dir, err := os.Open(parentDir); err == nil {
		_ = dir.Sync()
		dir.Close()
	}

	return nil
}

// EnablePersistence initializes the manager state directory and restores previous torrents.
// It returns a non-fatal warning message (if any recovery was needed) and a fatal error.
func (m *TorrentManager) EnablePersistence(stateDir string) (string, error) {
	// 1. Directory creation outside lock
	if err := os.MkdirAll(filepath.Join(stateDir, "torrents"), 0755); err != nil {
		return "", fmt.Errorf("failed to create persistence directories: %w", err)
	}

	statePath := filepath.Join(stateDir, "session.json")
	var savedState PersistedState
	var warning string

	// 2. Read session.json outside lock
	data, err := os.ReadFile(statePath)
	if err == nil {
		corrupted := false
		if err := json.Unmarshal(data, &savedState); err != nil {
			corrupted = true
		} else if savedState.Version != 1 {
			corrupted = true
		}

		if corrupted {
			timestamp := time.Now().Format("20060102-150405")
			corruptPath := filepath.Join(stateDir, "session.json.corrupt_"+timestamp)
			if err := os.Rename(statePath, corruptPath); err != nil {
				warning = fmt.Sprintf("corrupted session.json detected but failed to rename: %v", err)
			} else {
				warning = fmt.Sprintf("corrupted session.json renamed to session.json.corrupt_%s", timestamp)
			}
			// Proceed with empty state
			savedState = PersistedState{}
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("failed to read session.json: %w", err)
	}

	// 3. Lock only to set restoring flag and stateDir
	m.mu.Lock()
	m.stateDir = stateDir
	m.restoring = true
	m.mu.Unlock()

	// 4. Load sessions outside manager lock (since AddMagnet/AddTorrentFile lock the manager)
	m.SetGlobalDownloadLimit(savedState.GlobalDownloadLimit)
	m.SetGlobalUploadLimit(savedState.GlobalUploadLimit)

	restoreOne := func(entry PersistedTorrent) {
		absoluteDownloadDir, err := filepath.Abs(entry.DownloadDir)
		if err != nil {
			absoluteDownloadDir = entry.DownloadDir
		}

		var sess *Session
		var loadErr error
		cachedPath := filepath.Join(stateDir, "torrents", entry.InfoHashHex+".torrent")
		if _, statErr := os.Stat(cachedPath); statErr == nil {
			sess, loadErr = m.AddTorrentFile(cachedPath, absoluteDownloadDir)
			if loadErr != nil && entry.MagnetURI != "" {
				// Fallback to MagnetURI if cached torrent failed to parse
				sess, _ = m.AddMagnet(entry.MagnetURI, absoluteDownloadDir)
			}
		} else if entry.MagnetURI != "" {
			sess, _ = m.AddMagnet(entry.MagnetURI, absoluteDownloadDir)
		}

		if sess != nil {
			// Restore AddedAt
			if entry.AddedAt != nil && !entry.AddedAt.IsZero() {
				sess.mu.Lock()
				sess.AddedAt = *entry.AddedAt
				sess.mu.Unlock()
			} else {
				// Fallback to cached torrent file modification time
				if stat, statErr := os.Stat(cachedPath); statErr == nil {
					sess.mu.Lock()
					sess.AddedAt = stat.ModTime()
					sess.mu.Unlock()
				}
			}

			// Re-apply properties to session
			sess.OnStateChange = func() {
				m.saveState()
			}

			// Restore pause state
			if entry.Paused {
				sess.mu.Lock()
				sess.paused = true
				var newEvents []string
				for _, ev := range sess.trackerEvents {
					if ev != "started" {
						newEvents = append(newEvents, ev)
					}
				}
				sess.trackerEvents = newEvents
				sess.mu.Unlock()
			}

			// Restore file priorities
			if len(entry.FilePriorities) > 0 {
				sess.mu.Lock()
				if sess.metadataMode {
					var pending []FilePriority
					for _, prio := range entry.FilePriorities {
						if prio >= PrioritySkip && prio <= PriorityHigh {
							pending = append(pending, prio)
						} else {
							pending = append(pending, PriorityNormal)
						}
					}
					sess.PendingFilePriorities = pending
				} else {
					for i := 0; i < len(sess.FilePriorities) && i < len(entry.FilePriorities); i++ {
						prio := entry.FilePriorities[i]
						if prio >= PrioritySkip && prio <= PriorityHigh {
							sess.FilePriorities[i] = prio
						}
					}
				}
				sess.mu.Unlock()
			}
		} else {
			// Restore failed, save the entry to failedTorrents so it is not lost
			m.mu.Lock()
			m.failedTorrents = append(m.failedTorrents, entry)
			m.mu.Unlock()
		}
	}

	// De-duplicate by info hash before restoring concurrently: the check-and-insert in
	// AddTorrentFile is not atomic, so two entries with the same hash could otherwise
	// build and insert two sessions and apply persisted properties to the detached one.
	uniqueEntries := make([]PersistedTorrent, 0, len(savedState.Torrents))
	seenHashes := make(map[string]bool, len(savedState.Torrents))
	for _, entry := range savedState.Torrents {
		key := strings.ToLower(entry.InfoHashHex)
		if key != "" {
			if seenHashes[key] {
				continue
			}
			seenHashes[key] = true
		}
		uniqueEntries = append(uniqueEntries, entry)
	}

	// Restore torrents concurrently (bounded by GOMAXPROCS). The heavy per-torrent work
	// — parsing the cached .torrent and opening storage — runs off the critical path;
	// manager mutations are mutex-protected and display order comes from a later sort,
	// so insertion order does not matter.
	workers := runtime.GOMAXPROCS(0)
	if workers > len(uniqueEntries) {
		workers = len(uniqueEntries)
	}
	if workers < 1 {
		workers = 1
	}
	entryCh := make(chan PersistedTorrent)
	var restoreWG sync.WaitGroup
	for w := 0; w < workers; w++ {
		restoreWG.Add(1)
		go func() {
			defer restoreWG.Done()
			for entry := range entryCh {
				restoreOne(entry)
			}
		}()
	}
	for _, entry := range uniqueEntries {
		entryCh <- entry
	}
	close(entryCh)
	restoreWG.Wait()

	// 5. Turn off restoring and save initial state
	m.mu.Lock()
	m.restoring = false
	m.mu.Unlock()

	m.saveState()

	return warning, nil
}
