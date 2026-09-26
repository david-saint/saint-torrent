// Package httpapi exposes optional read-only HTTP endpoints for monitoring.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"sainttorrent/pkg/downloader"
)

const statsVersion = 1

// maxConns caps concurrent stats connections. The API serves a few local
// dashboards or scripts; the cap stops a flood of idle connections from eating
// the file descriptors that peer sockets and storage need.
const maxConns = 64

// maxHeaderBytes caps a request's header block (net/http reads 4 KiB past it
// before answering 431). The API takes only bare GETs, so this is plenty,
// where net/http's 1 MiB default would let each of the maxConns slots pin a
// megabyte.
const maxHeaderBytes = 8 << 10

// statsCacheTTL is how long an encoded /stats body is served again. A snapshot
// takes every session's lock, so a client polling in a loop (on up to maxConns
// connections) would otherwise turn its request rate into lock traffic that
// competes with the peer loops. Dashboards poll at 1 s or slower.
const statsCacheTTL = 500 * time.Millisecond

// statsSnapshot builds the /stats payload; tests swap it to count builds.
var statsSnapshot = Snapshot

// ErrNotLoopback is returned by Start when addr is not a loopback address and
// Options.AllowRemote is unset.
var ErrNotLoopback = errors.New("address is not loopback")

// Options configures Start.
type Options struct {
	// AllowRemote permits binding a wildcard or non-loopback address. The API
	// has no authentication, so this exposes torrent names, local paths and
	// peer addresses to anyone who can reach the port.
	AllowRemote bool
	// AllowHosts are host names accepted in a request's Host header besides
	// IP literals, localhost and the listen host: the name a LAN dashboard,
	// container scraper or reverse proxy reaches the API by. Each is a bare
	// host name (see CheckAllowHost). Only names the user trusts belong here:
	// a page served under one can read the API through DNS rebinding.
	AllowHosts []string
}

// CheckAllowHost reports whether name can be an Options.AllowHosts entry: a
// bare DNS host name, without a scheme, port, path or wildcard. IP literals
// and localhost are always accepted and need no entry.
func CheckAllowHost(name string) error {
	if name == "" || len(name) > 253 {
		return fmt.Errorf("invalid host name %q: want a name such as nas.lan", name)
	}
	if _, err := netip.ParseAddr(name); err == nil {
		return fmt.Errorf("%s is an IP address; IP addresses are always allowed", name)
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || len(label) > 63 {
			return fmt.Errorf("invalid host name %q: want a name such as nas.lan", name)
		}
		for _, r := range label {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			default:
				return fmt.Errorf("invalid host name %q: want a bare name such as nas.lan, without a scheme, port or path", name)
			}
		}
	}
	return nil
}

// Server owns the optional HTTP stats listener.
type Server struct {
	server   *http.Server
	listener net.Listener
	loopback bool
}

// Start starts the optional HTTP stats server on addr. Unless
// opts.AllowRemote is set it refuses any address that is not loopback.
func Start(addr string, manager *downloader.TorrentManager, opts Options) (*Server, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, errors.New("HTTP stats address is required")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	for _, name := range opts.AllowHosts {
		if err := CheckAllowHost(name); err != nil {
			return nil, err
		}
	}
	// Refuse wildcard and non-loopback IP literals before binding, so the port
	// is never open on other interfaces even briefly.
	if !opts.AllowRemote && nonLoopbackLiteral(host) {
		return nil, fmt.Errorf("%w: %s (the stats API has no authentication)", ErrNotLoopback, addr)
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	// A host name is only known after resolution: check what was bound.
	loopback := loopbackAddr(listener.Addr())
	if !loopback && !opts.AllowRemote {
		_ = listener.Close()
		return nil, fmt.Errorf("%w: %s resolved to %s (the stats API has no authentication)", ErrNotLoopback, addr, listener.Addr())
	}
	listener = newLimitListener(listener, maxConns)

	srv := &http.Server{
		Handler:           NewHandler(manager, append([]string{host}, opts.AllowHosts...)...),
		MaxHeaderBytes:    maxHeaderBytes,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	api := &Server{
		server:   srv,
		listener: listener,
		loopback: loopback,
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

// Loopback reports whether the server only accepts loopback connections.
func (s *Server) Loopback() bool {
	return s != nil && s.loopback
}

// Shutdown gracefully stops the HTTP stats server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.Shutdown(ctx)
}

// NewHandler returns an HTTP handler exposing read-only monitoring endpoints.
// Besides IP literals and localhost, it accepts requests whose Host header
// names one of extraHosts (Start passes the host of its listen address).
func NewHandler(manager *downloader.TorrentManager, extraHosts ...string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if !allowGet(w, r) {
			return
		}
		writeJSON(w, map[string]string{"status": "ok"})
	})
	mux.Handle("/stats", newStatsHandler(manager))
	return guard(mux, extraHosts)
}

// statsHandler serves GET /stats from an encoding cached for statsCacheTTL.
// Requests that arrive while one of them builds a snapshot queue on mu and
// then share its bytes, so however many clients poll, sessions are locked for
// at most one snapshot per statsCacheTTL.
type statsHandler struct {
	manager *downloader.TorrentManager
	now     func() time.Time

	mu sync.Mutex
	// body is never modified once stored; a rebuild replaces it, and expiry
	// drops it once it is stale, so an idle API does not hold the last response
	// (every file of every torrent) until the next request.
	body   []byte
	at     time.Time
	expiry *time.Timer
}

func newStatsHandler(manager *downloader.TorrentManager) *statsHandler {
	return &statsHandler{manager: manager, now: time.Now}
}

func (h *statsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !allowGet(w, r) {
		return
	}
	body, err := h.encoded()
	if err != nil {
		http.Error(w, "cannot encode stats", http.StatusInternalServerError)
		return
	}
	setJSONHeaders(w)
	_, _ = w.Write(body)
}

// encoded returns the cached /stats body, rebuilding it once it is
// statsCacheTTL old.
func (h *statsHandler) encoded() ([]byte, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	if age := now.Sub(h.at); h.body != nil && age >= 0 && age < statsCacheTTL {
		return h.body, nil
	}
	body, err := json.Marshal(statsSnapshot(h.manager))
	if err != nil {
		return nil, err
	}
	// Byte for byte what writeJSON's json.Encoder writes.
	h.body = append(body, '\n')
	h.at = now
	if h.expiry == nil {
		h.expiry = time.AfterFunc(statsCacheTTL, h.dropStale)
	} else {
		h.expiry.Reset(statsCacheTTL)
	}
	return h.body, nil
}

// dropStale releases the cached body once encoded would no longer serve it.
// It judges age by h.now, so it never drops a body a request could still use.
func (h *statsHandler) dropStale() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if age := h.now().Sub(h.at); age < 0 || age >= statsCacheTTL {
		h.body = nil
	}
}

// guard applies the browser-facing checks the API needs even on loopback. A
// DNS-rebinding page reaches 127.0.0.1 under its own host name and is then
// same-origin with us, so only Host values such a page cannot send are
// accepted: IP literals, localhost, and the configured listen host. Browsers
// label cross-site subresource requests with Sec-Fetch-Site (and CORS requests
// with Origin), which stops other sites from polling the snapshot. curl and
// scripts send neither header and are unaffected.
func guard(next http.Handler, extraHosts []string) http.Handler {
	allowed := map[string]struct{}{"localhost": {}}
	for _, h := range extraHosts {
		if h = strings.ToLower(strings.TrimSpace(h)); h != "" {
			allowed[h] = struct{}{}
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hostAllowed(r.Host, allowed) {
			reject(w, "misdirected request: use an IP address, localhost or the --http-addr host, or allow this name with --http-allow-host", http.StatusMisdirectedRequest)
			return
		}
		switch r.Header.Get("Sec-Fetch-Site") {
		case "", "none", "same-origin":
		default:
			reject(w, "cross-site requests are not allowed", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) {
			reject(w, "cross-origin requests are not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// reject answers a refused request and closes its connection. Kept alive, a
// page's rejected connections would each hold one of the maxConns slots for
// the idle timeout, enough to lock local monitoring out.
func reject(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Connection", "close")
	http.Error(w, msg, code)
}

// hostAllowed reports whether a Host header value is an IP literal or one of
// the allowed names. An empty Host only comes from HTTP/1.0 clients, never
// from a browser.
func hostAllowed(hostport string, allowed map[string]struct{}) bool {
	if hostport == "" {
		return true
	}
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	_, ok := allowed[strings.ToLower(host)]
	return ok
}

// sameOrigin reports whether an Origin header names the host the request was
// sent to. We serve no pages, so no legitimate request comes from elsewhere.
func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	return err == nil && u.Host != "" && strings.EqualFold(u.Host, host)
}

// nonLoopbackLiteral reports whether a listen host is a wildcard or a
// non-loopback IP literal. Host names are checked by loopbackAddr once bound.
func nonLoopbackLiteral(host string) bool {
	if host == "" {
		return true // ":port" listens on every interface
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && !ip.IsLoopback()
}

func loopbackAddr(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

// limitListener caps the number of simultaneously open connections. Accept
// waits for a free slot before taking the next connection off the kernel
// queue, so excess clients wait in the backlog without holding descriptors.
type limitListener struct {
	net.Listener
	sem       chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newLimitListener(l net.Listener, n int) net.Listener {
	return &limitListener{
		Listener: l,
		sem:      make(chan struct{}, n),
		done:     make(chan struct{}),
	}
}

func (l *limitListener) Accept() (net.Conn, error) {
	select {
	case l.sem <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.sem
		return nil, err
	}
	return &limitConn{Conn: conn, release: func() { <-l.sem }}, nil
}

func (l *limitListener) Close() error {
	err := l.Listener.Close()
	l.closeOnce.Do(func() { close(l.done) })
	return err
}

type limitConn struct {
	net.Conn
	release     func()
	releaseOnce sync.Once
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.releaseOnce.Do(c.release)
	return err
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
	setJSONHeaders(w)
	_ = json.NewEncoder(w).Encode(v)
}

func setJSONHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
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
		Pieces:                      snapshotPieces(sess.PieceCounts()),
		Peers:                       snapshotPeers(sess.GetActivePeers()),
		Files:                       snapshotFiles(sess),
	}
}

func snapshotPieces(counts downloader.PieceCounts) PieceStats {
	return PieceStats{
		Total:       counts.Total,
		Empty:       counts.Empty,
		Downloading: counts.Downloading,
		Completed:   counts.Completed,
		Unverified:  counts.Unverified,
		Unknown:     counts.Unknown,
	}
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
