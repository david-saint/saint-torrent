package downloader

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"

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
	sess.GlobalDownloadLimiter = m.globalDownloadLimiter
	sess.GlobalUploadLimiter = m.globalUploadLimiter
	sess.mu.Unlock()

	if m.dht != nil {
		sess.AttachDHT(m.dht)
	}
}

// RemoveSession stops the session associated with the given info hash and removes it from the manager.
func (m *TorrentManager) RemoveSession(infoHashHex string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sess, ok := m.sessions[infoHashHex]; ok {
		sess.Close()
		delete(m.sessions, infoHashHex)
	}
}

// GetSession retrieves a session by its info hash hex string.
func (m *TorrentManager) GetSession(infoHashHex string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[infoHashHex]
}

// ListSessions returns a slice of all managed sessions.
func (m *TorrentManager) ListSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		list = append(list, sess)
	}
	return list
}

// PauseAll pauses all managed sessions.
func (m *TorrentManager) PauseAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions {
		sess.Pause()
	}
}

// ResumeAll resumes all managed sessions.
func (m *TorrentManager) ResumeAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, sess := range m.sessions {
		sess.Resume()
	}
}

// SetGlobalDownloadLimit updates the global download bandwidth limit in bytes/sec. 0 = unlimited.
func (m *TorrentManager) SetGlobalDownloadLimit(bytesPerSec int64) {
	m.globalDownloadLimiter.SetLimit(bytesPerSec)
}

// SetGlobalUploadLimit updates the global upload bandwidth limit in bytes/sec. 0 = unlimited.
func (m *TorrentManager) SetGlobalUploadLimit(bytesPerSec int64) {
	m.globalUploadLimiter.SetLimit(bytesPerSec)
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

	sessions := make([]*Session, 0, len(m.sessions))
	for _, sess := range m.sessions {
		sessions = append(sessions, sess)
	}
	m.sessions = make(map[string]*Session)
	d := m.dht
	m.dht = nil
	m.mu.Unlock()

	for _, sess := range sessions {
		sess.Close()
	}
	if d != nil {
		d.Close()
	}
	m.wg.Wait()
}

// AddMagnet parses a magnet URI and adds it to the manager as a metadata session.
func (m *TorrentManager) AddMagnet(uri string, downloadDir string) (*Session, error) {
	mag, err := torrent.ParseMagnet(uri)
	if err != nil {
		return nil, err
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

	m.AddSession(infoHashHex, sess)
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

	m.AddSession(infoHashHex, sess)
	return sess, nil
}
