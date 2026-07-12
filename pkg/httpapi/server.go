// Package httpapi exposes optional read-only HTTP endpoints for monitoring.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"sainttorrent/pkg/downloader"
)

const statsVersion = 1

// Server owns the optional HTTP stats listener.
type Server struct {
	server   *http.Server
	listener net.Listener
}

// Start starts the optional HTTP stats server on addr.
func Start(addr string, manager *downloader.TorrentManager) (*Server, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, errors.New("HTTP stats address is required")
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:           NewHandler(manager),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	api := &Server{
		server:   srv,
		listener: listener,
	}

	go func() {
		_ = srv.Serve(listener)
	}()

	return api, nil
}

// Addr returns the actual address the server is listening on.
func (s *Server) Addr() string {
	if s == nil || s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Shutdown gracefully stops the HTTP stats server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// NewHandler returns an HTTP handler exposing read-only monitoring endpoints.
func NewHandler(manager *downloader.TorrentManager) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		writeJSON(w, Snapshot(manager))
	})
	return mux
}

func allowGet(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", http.MethodGet)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	return false
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// Stats is the JSON snapshot returned by GET /stats.
type Stats struct {
	Version     int            `json:"version"`
	GeneratedAt time.Time      `json:"generated_at"`
	Manager     ManagerStats   `json:"manager"`
	Torrents    []TorrentStats `json:"torrents"`
}

// ManagerStats summarizes process-wide torrent manager state.
type ManagerStats struct {
	TorrentCount                int      `json:"torrent_count"`
	DownloadLimitBytesPerSecond int64    `json:"download_limit_bytes_per_second"`
	UploadLimitBytesPerSecond   int64    `json:"upload_limit_bytes_per_second"`
	DownloadSpeedBytesPerSecond float64  `json:"download_speed_bytes_per_second"`
	UploadSpeedBytesPerSecond   float64  `json:"upload_speed_bytes_per_second"`
	DownloadedBytes             int64    `json:"downloaded_bytes"`
	UploadedBytes               int64    `json:"uploaded_bytes"`
	PeerListenPort              uint16   `json:"peer_listen_port"`
	AdvertisedPeerPort          uint16   `json:"advertised_peer_port"`
	DHTListenPort               uint16   `json:"dht_listen_port"`
	NAT                         NATStats `json:"nat"`
}

// NATStats describes the current automatic port-mapping state.
type NATStats struct {
	Enabled        bool   `json:"enabled"`
	Protocol       string `json:"protocol"`
	ExternalIP     string `json:"external_ip"`
	ListenPort     uint16 `json:"listen_port"`
	AdvertisedPort uint16 `json:"advertised_port"`
	TCPMapped      bool   `json:"tcp_mapped"`
	UDPMapped      bool   `json:"udp_mapped"`
	LastError      string `json:"last_error,omitempty"`
}

// TorrentStats summarizes a single torrent session.
type TorrentStats struct {
	Name                        string      `json:"name"`
	InfoHash                    string      `json:"info_hash"`
	Status                      string      `json:"status"`
	Paused                      bool        `json:"paused"`
	MetadataMode                bool        `json:"metadata_mode"`
	DownloadDir                 string      `json:"download_dir"`
	TotalSizeBytes              int64       `json:"total_size_bytes"`
	DownloadedBytes             int64       `json:"downloaded_bytes"`
	UploadedBytes               int64       `json:"uploaded_bytes"`
	PercentComplete             float64     `json:"percent_complete"`
	DownloadSpeedBytesPerSecond float64     `json:"download_speed_bytes_per_second"`
	UploadSpeedBytesPerSecond   float64     `json:"upload_speed_bytes_per_second"`
	LastError                   string      `json:"last_error,omitempty"`
	Pieces                      PieceStats  `json:"pieces"`
	Peers                       []PeerStats `json:"peers"`
	Files                       []FileStats `json:"files"`
}

// PieceStats summarizes the torrent piece state vector.
type PieceStats struct {
	Total       int `json:"total"`
	Empty       int `json:"empty"`
	Downloading int `json:"downloading"`
	Completed   int `json:"completed"`
	Unverified  int `json:"unverified"`
	Unknown     int `json:"unknown,omitempty"`
}

// PeerStats is the read-only peer state exposed in the stats JSON.
type PeerStats struct {
	Address                     string  `json:"address"`
	IP                          string  `json:"ip"`
	Port                        uint16  `json:"port"`
	Choked                      bool    `json:"choked"`
	Interested                  bool    `json:"interested"`
	AmChoking                   bool    `json:"am_choking"`
	DownloadSpeedBytesPerSecond float64 `json:"download_speed_bytes_per_second"`
	UploadSpeedBytesPerSecond   float64 `json:"upload_speed_bytes_per_second"`
	DownloadedBytes             int64   `json:"downloaded_bytes"`
	UploadedBytes               int64   `json:"uploaded_bytes"`
	OutstandingBlocks           int     `json:"outstanding_blocks"`
	OutstandingBytes            int64   `json:"outstanding_bytes"`
}

// FileStats describes one file inside a torrent.
type FileStats struct {
	Path          string `json:"path"`
	LengthBytes   int64  `json:"length_bytes"`
	Priority      string `json:"priority"`
	PriorityValue int    `json:"priority_value"`
}

// Snapshot returns a point-in-time JSON-safe snapshot of the torrent manager.
func Snapshot(manager *downloader.TorrentManager) Stats {
	return SnapshotAt(manager, time.Now())
}

// SnapshotAt returns a point-in-time JSON-safe snapshot using generatedAt.
func SnapshotAt(manager *downloader.TorrentManager, generatedAt time.Time) Stats {
	stats := Stats{
		Version:     statsVersion,
		GeneratedAt: generatedAt,
		Torrents:    []TorrentStats{},
	}
	if manager == nil {
		return stats
	}

	sessions := manager.ListSessions()
	stats.Manager = ManagerStats{
		TorrentCount:                len(sessions),
		DownloadLimitBytesPerSecond: manager.GlobalDownloadLimit(),
		UploadLimitBytesPerSecond:   manager.GlobalUploadLimit(),
		PeerListenPort:              manager.PeerListenPort(),
		AdvertisedPeerPort:          manager.AdvertisedPeerPort(),
		DHTListenPort:               manager.DHTListenPort(),
		NAT:                         snapshotNAT(manager.NATStatus()),
	}

	for _, sess := range sessions {
		torrentStats := snapshotSession(sess)
		stats.Manager.DownloadSpeedBytesPerSecond += torrentStats.DownloadSpeedBytesPerSecond
		stats.Manager.UploadSpeedBytesPerSecond += torrentStats.UploadSpeedBytesPerSecond
		stats.Manager.DownloadedBytes += torrentStats.DownloadedBytes
		stats.Manager.UploadedBytes += torrentStats.UploadedBytes
		stats.Torrents = append(stats.Torrents, torrentStats)
	}

	return stats
}

func snapshotNAT(status downloader.NATStatus) NATStats {
	return NATStats{
		Enabled:        status.Enabled,
		Protocol:       status.Protocol,
		ExternalIP:     status.ExternalIP,
		ListenPort:     status.ListenPort,
		AdvertisedPort: status.AdvertisedPort,
		TCPMapped:      status.TCPMapped,
		UDPMapped:      status.UDPMapped,
		LastError:      status.LastError,
	}
}

func snapshotSession(sess *downloader.Session) TorrentStats {
	sortSnapshot := sess.GetSortSnapshot()
	lastErr := ""
	if err := sess.LastError(); err != nil {
		lastErr = err.Error()
	}

	return TorrentStats{
		Name:                        sess.Name(),
		InfoHash:                    sortSnapshot.InfoHashHex,
		Status:                      sess.Status(),
		Paused:                      sess.IsPaused(),
		MetadataMode:                sess.IsMetadataMode(),
		DownloadDir:                 sess.DownloadDir(),
		TotalSizeBytes:              sess.TotalSize(),
		DownloadedBytes:             sess.DownloadedBytes(),
		UploadedBytes:               sess.UploadedBytes(),
		PercentComplete:             sess.PercentComplete(),
		DownloadSpeedBytesPerSecond: sess.CurrentSpeed(),
		UploadSpeedBytesPerSecond:   sess.CurrentUploadSpeed(),
		LastError:                   lastErr,
		Pieces:                      summarizePieces(sess.GetPieceStates()),
		Peers:                       snapshotPeers(sess.GetActivePeers()),
		Files:                       snapshotFiles(sess),
	}
}

func summarizePieces(states []downloader.PieceState) PieceStats {
	stats := PieceStats{Total: len(states)}
	for _, state := range states {
		switch state {
		case downloader.PieceEmpty:
			stats.Empty++
		case downloader.PieceDownloading:
			stats.Downloading++
		case downloader.PieceCompleted:
			stats.Completed++
		case downloader.PieceUnverified:
			stats.Unverified++
		default:
			stats.Unknown++
		}
	}
	return stats
}

func snapshotPeers(peers []downloader.PeerState) []PeerStats {
	stats := make([]PeerStats, 0, len(peers))
	for _, peer := range peers {
		stats = append(stats, PeerStats{
			Address:                     net.JoinHostPort(peer.IP, strconv.Itoa(int(peer.Port))),
			IP:                          peer.IP,
			Port:                        peer.Port,
			Choked:                      peer.Choked,
			Interested:                  peer.Interested,
			AmChoking:                   peer.AmChoking,
			DownloadSpeedBytesPerSecond: peer.DownloadSpeed,
			UploadSpeedBytesPerSecond:   peer.UploadSpeed,
			DownloadedBytes:             peer.Downloaded,
			UploadedBytes:               peer.Uploaded,
			OutstandingBlocks:           peer.OutstandingBlocks,
			OutstandingBytes:            peer.OutstandingBytes,
		})
	}
	return stats
}

func snapshotFiles(sess *downloader.Session) []FileStats {
	files := sess.Files()
	priorities := sess.GetFilePriorities()
	stats := make([]FileStats, 0, len(files))
	for i, file := range files {
		priority := downloader.PriorityNormal
		if i < len(priorities) {
			priority = priorities[i]
		}
		stats = append(stats, FileStats{
			Path:          strings.Join(file.Path, "/"),
			LengthBytes:   file.Length,
			Priority:      priorityName(priority),
			PriorityValue: int(priority),
		})
	}
	return stats
}

func priorityName(priority downloader.FilePriority) string {
	switch priority {
	case downloader.PrioritySkip:
		return "skip"
	case downloader.PriorityLow:
		return "low"
	case downloader.PriorityNormal:
		return "normal"
	case downloader.PriorityHigh:
		return "high"
	default:
		return "unknown"
	}
}
