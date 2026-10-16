// Package main implements the CLI and TUI entry point for the saintTorrent client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/httpapi"
	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

var (
	errLockContention = errors.New("lock contention")

	programMu  sync.RWMutex
	teaProgram *tea.Program
)

// version is the build version of saintTorrent. It defaults to "dev" and can be
// overridden at build time via:
//
//	go build -ldflags "-X main.version=v1.2.3" ./cmd/sainttorrent
var version = "dev"

// --- startup/shutdown timing ---
// Enabled via SAINTTORRENT_TIMING=1 (and implicitly under SAINTTORRENT_BENCH=1).
// Marks are buffered and printed after the TUI releases the terminal, so they never
// corrupt the display. This is the "where do the milliseconds go" view used to drive
// and verify the startup/close optimizations.
type perfMark struct {
	label string
	at    time.Duration
}

var (
	perfEnabled bool
	perfStart   time.Time
	perfMu      sync.Mutex
	perfMarks   []perfMark
)

// perfInit records the process start time and reads the timing env switches.
// Call it as the very first statement in main().
func perfInit() {
	perfStart = time.Now()
	perfEnabled = os.Getenv("SAINTTORRENT_TIMING") == "1" || os.Getenv("SAINTTORRENT_BENCH") == "1"
}

// perfMarkf records elapsed time since process start under a label. Cheap no-op when disabled.
func perfMarkf(label string) {
	if !perfEnabled {
		return
	}
	perfMu.Lock()
	perfMarks = append(perfMarks, perfMark{label: label, at: time.Since(perfStart)})
	perfMu.Unlock()
}

func msOf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

// perfReport prints the recorded phase breakdown (cumulative + per-phase delta) to w,
// and appends it to SAINTTORRENT_TIMING_LOG if set.
func perfReport(w io.Writer) {
	if !perfEnabled {
		return
	}
	perfMu.Lock()
	defer perfMu.Unlock()
	writeRows := func(out io.Writer) {
		var prev time.Duration
		for _, m := range perfMarks {
			fmt.Fprintf(out, "  %-22s %8.1fms (Δ %.1fms)\n", m.label, msOf(m.at), msOf(m.at-prev))
			prev = m.at
		}
	}
	fmt.Fprintln(w, "── saintTorrent timing ──")
	writeRows(w)
	if logPath := os.Getenv("SAINTTORRENT_TIMING_LOG"); logPath != "" {
		if f, err := logging.OpenPrivateFile(logPath); err == nil {
			fmt.Fprintf(f, "── %s ──\n", time.Now().Format(time.RFC3339))
			writeRows(f)
			f.Close()
		}
	}
}

const terminalWindowTitle = "saintTorrent"
const defaultPeerPort = 51413

type viewMode int

const (
	viewList viewMode = iota
	viewDetail
	viewFiles
	viewInput
	viewDeleteConfirm
	viewAddConfirm
)

type pendingItem struct {
	rawURL        string
	displayName   string
	infoHashHex   string
	downloadDir   string
	downloadPaths downloadPathOptions
	isDuplicate   bool
	respChan      chan addTorrentResponse
}

func (p pendingItem) pathOptions() downloadPathOptions {
	if p.downloadPaths.primary != "" {
		return p.downloadPaths
	}
	return downloadPathOptions{primary: p.downloadDir}
}

type addTorrentMsg struct {
	msg      socketMessage
	respChan chan addTorrentResponse
}

type addTorrentResponse struct {
	err error
}

type deleteFinishedMsg struct {
	infoHashHex string
	err         error
}

type socketMessage struct {
	Action               string   `json:"action,omitempty"`
	Items                []string `json:"items"`
	Confirm              bool     `json:"confirm"`
	DownloadDir          string   `json:"download_dir"`
	FallbackDownloadDirs []string `json:"fallback_download_dirs"`
}

type socketResponse struct {
	Status          string `json:"status"`
	Message         string `json:"message"`
	TerminalTTY     string `json:"terminal_tty,omitempty"`
	TerminalProgram string `json:"terminal_program,omitempty"`
	TerminalTitle   string `json:"terminal_title,omitempty"`
}

type terminalIdentity struct {
	TTY     string
	Program string
	Title   string
}

type cliOptions struct {
	downloadDir          string
	downloadDirSet       bool
	fallbackDownloadDirs []string
	fallbackDirsSet      bool
	configDir            string
	verifyOnStartup      bool
	persist              bool
	startPaused          bool
	confirm              bool
	headless             bool
	theme                string
	listenPort           int
	httpAddr             string
	httpAllowRemote      bool
	httpAllowHosts       []string
	natEnabled           bool
	encryption           mse.Policy
	storage              storage.Backend
	logPath              string
	logLevel             logging.Level
	logLevelSet          bool
	help                 bool
	showVersion          bool
	kill                 bool
	err                  error
	items                []string
}

type inputMode int

const (
	inputNone inputMode = iota
	inputAddTorrent
	inputLimitDownload
	inputLimitUpload
)

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

type model struct {
	manager          *downloader.TorrentManager
	downloadDir      string
	downloadPaths    downloadPathOptions
	progress         progress.Model
	textInput        textinput.Model
	viewMode         viewMode
	inputMode        inputMode
	selectedIdx      int
	selectedFileIdx  int
	detailScroll     int
	quitting         bool
	inputErr         string
	flash            string
	startupWarn      string
	sessions         []*downloader.Session
	deleteWithFiles  bool
	deleteInProgress bool
	deleteErr        error
	addConfirmErr    error
	deleteTargetName string
	deleteTargetHash string
	deleteOriginView viewMode
	pendingItems     []pendingItem
	pendingIdx       int

	// UI/responsiveness + theming
	width          int
	height         int
	theme          *theme
	configDir      string
	persistEnabled bool
	// speedHistory is a UI-only per-torrent ring of recent download speeds
	// (keyed by info-hash hex), used to draw the Mono throughput sparkline.
	speedHistory map[string][]float64

	// Per-tick display snapshots (issue #57). The views render from these so the
	// ~10 Hz animation frame loop never re-locks a session on its download hot
	// path. rows parallels sessions; detail/files cover the selected session.
	rows           []sessionRow
	detail         detailSnapshot
	files          filesSnapshot
	hasDownloading bool   // any session is Downloading (drives the speed pulse)
	animRunning    bool   // an animCmd loop is currently scheduled
	detailVersion  uint64 // bumped when the detail snapshot changes; keys detailBody

	// Detail body cache: renderDetails is memoized so a scroll keypress or resize
	// clamps against the cached line count instead of re-rendering the whole
	// detail view (which View would then render again).
	detailBody  string
	detailLines int
	detailSig   detailSig
	detailBuilt bool
}

// speedHistoryLen bounds the per-torrent speed ring (samples at the tick rate).
const speedHistoryLen = 60

// recordSpeeds appends the current download speed for each session to its ring
// (bounded) and prunes rings for sessions that are gone.
func (m *model) recordSpeeds() {
	if m.speedHistory == nil {
		m.speedHistory = make(map[string][]float64)
	}
	// Sample from the per-tick snapshot rows so this refresh (like the render
	// path) stays off the session lock.
	live := make(map[string]struct{}, len(m.rows))
	for _, row := range m.rows {
		key := row.infoHashHex
		live[key] = struct{}{}
		ring := append(m.speedHistory[key], row.transferSpeed)
		if len(ring) > speedHistoryLen {
			ring = ring[len(ring)-speedHistoryLen:]
		}
		m.speedHistory[key] = ring
	}
	for key := range m.speedHistory {
		if _, ok := live[key]; !ok {
			delete(m.speedHistory, key)
		}
	}
}

// cycleTheme switches to the next theme, flashes the new name, and persists the
// choice when persistence is enabled.
func (m *model) cycleTheme() {
	m.theme = nextTheme(m.theme)
	m.flash = "Theme: " + m.theme.label
	m.clampDetailScroll()
	if m.persistEnabled {
		if err := saveUIPrefs(m.configDir, m.theme.name); err != nil {
			m.flash = "Theme: " + m.theme.label + " (not saved: " + err.Error() + ")"
		}
	}
}

// detailSignature captures the inputs that determine the rendered detail body.
func (m *model) detailSignature() detailSig {
	return detailSig{
		width:   m.width,
		theme:   m.theme,
		version: m.detailVersion,
		flash:   m.flash,
	}
}

// ensureDetailBody renders the detail view body (pre vertical slice) into the
// cache when its inputs have changed. Scrolling changes only detailScroll, not
// the body, so a scroll keypress reuses the cache and does not re-render — this
// is what removes the double render (clamp render + View render) on every scroll
// notch or resize.
func (m *model) ensureDetailBody() {
	sig := m.detailSignature()
	if m.detailBuilt && m.detailSig == sig {
		return
	}
	// clampLines never changes the line count (it only truncates within a line),
	// so the cached count matches what View ultimately slices.
	m.detailBody = m.theme.renderDetails(m)
	m.detailLines = renderedLineCount(m.detailBody)
	m.detailSig = sig
	m.detailBuilt = true
}

// detailViewBody returns the rendered detail body, reusing the memoized copy when
// it is current. View has a value receiver and cannot populate the cache, so it
// falls back to a fresh render when the cache is stale (the next Update refills
// it); the common per-frame path hits the cache.
func (m model) detailViewBody() string {
	if m.detailBuilt && m.detailSig == m.detailSignature() {
		return m.detailBody
	}
	return m.theme.renderDetails(&m)
}

func (m *model) detailMaxScroll() int {
	if m.viewMode != viewDetail || m.height <= 0 {
		return 0
	}
	m.ensureDetailBody()
	return max(0, m.detailLines-m.height)
}

// wantAnim reports whether the 10 Hz pulse tick should run: only the list view
// animates (its download-speed cells pulse), and only while something is actually
// downloading. Everything else (seeding, paused, detail/files/input views) is
// static between data ticks, so the frame loop pauses.
func (m *model) wantAnim() bool {
	return m.viewMode == viewList && m.hasDownloading
}

func (m *model) clampDetailScroll() {
	m.detailScroll = clamp(m.detailScroll, 0, m.detailMaxScroll())
}

func (m *model) scrollDetails(delta int) {
	m.detailScroll += delta
	m.clampDetailScroll()
}

func (m *model) moveListSelection(delta int) {
	if len(m.sessions) == 0 {
		return
	}
	m.selectedIdx = clamp(m.selectedIdx+delta, 0, len(m.sessions)-1)
}

// selectedSession returns the session under the list cursor, or ok=false when
// the list is empty or the cursor is out of range.
func (m *model) selectedSession() (*downloader.Session, bool) {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return nil, false
	}
	return m.sessions[m.selectedIdx], true
}

func (m *model) moveFileSelection(delta int) {
	s, ok := m.selectedSession()
	if !ok {
		return
	}
	files := s.Files()
	if len(files) == 0 {
		return
	}
	m.selectedFileIdx = clamp(m.selectedFileIdx+delta, 0, len(files)-1)
}

// resumePendingOr switches to fallback, unless queued magnet adds are waiting
// (they can arrive at any moment, e.g. from a second instance), in which case
// the add-confirm flow resumes instead.
func (m *model) resumePendingOr(fallback viewMode) {
	if m.pendingIdx < len(m.pendingItems) {
		m.viewMode = viewAddConfirm
	} else {
		m.viewMode = fallback
	}
}

func (m *model) startDelete(withFiles bool, origin viewMode) {
	s, ok := m.selectedSession()
	if !ok {
		return
	}
	m.viewMode = viewDeleteConfirm
	m.deleteWithFiles = withFiles
	m.deleteErr = nil
	m.deleteOriginView = origin
	m.deleteTargetName = displayText(s.Name())
	m.deleteTargetHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
}

func initialModel(mgr *downloader.TorrentManager, downloadDir string, startupWarn string, pending []pendingItem) model {
	p := progress.New(progress.WithDefaultGradient())
	ti := textinput.New()

	mode := viewList
	if len(pending) > 0 {
		mode = viewAddConfirm
	}

	// Default to a full-width layout until the first WindowSizeMsg arrives.
	width := maxOuterWidth
	p.Width = bodyWidth(width)
	ti.Width = bodyWidth(width) - dispWidth(ti.Prompt)

	m := model{
		manager:        mgr,
		downloadDir:    downloadDir,
		downloadPaths:  downloadPathOptions{primary: downloadDir},
		progress:       p,
		textInput:      ti,
		viewMode:       mode,
		startupWarn:    startupWarn,
		sessions:       mgr.ListSessions(),
		pendingItems:   pending,
		pendingIdx:     0,
		width:          width,
		theme:          themeByName[defaultThemeName],
		persistEnabled: false,
		speedHistory:   make(map[string][]float64),
	}
	// Prime the display snapshots so the first View (which can arrive before any
	// data tick) renders from cached data rather than live session getters.
	m.buildSnapshots()
	return m
}

func (m *model) refreshSessions() {
	var selectedHash string
	if s, ok := m.selectedSession(); ok && s.Torrent != nil {
		selectedHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
	}

	m.sessions = m.manager.ListSessions()

	if selectedHash != "" {
		newIdx := -1
		for idx, s := range m.sessions {
			if s.Torrent != nil && fmt.Sprintf("%x", s.Torrent.InfoHash) == selectedHash {
				newIdx = idx
				break
			}
		}
		if newIdx != -1 {
			m.selectedIdx = newIdx
		} else if m.selectedIdx >= len(m.sessions) {
			if len(m.sessions) > 0 {
				m.selectedIdx = len(m.sessions) - 1
			} else {
				m.selectedIdx = 0
			}
		}
	} else if m.selectedIdx >= len(m.sessions) {
		if len(m.sessions) > 0 {
			m.selectedIdx = len(m.sessions) - 1
		} else {
			m.selectedIdx = 0
		}
	}

	// Refresh the per-tick display snapshots now that sessions and the selection
	// index are settled.
	m.buildSnapshots()
}

// openSelectedLocation reveals the currently selected torrent's content in the
// OS file manager (Finder on macOS). It records a flash message when the
// location is not yet known (e.g. a magnet still fetching metadata) or the file
// manager could not be launched.
func (m *model) openSelectedLocation() {
	s, ok := m.selectedSession()
	if !ok {
		return
	}
	path, ok := s.ContentPath()
	if !ok {
		m.flash = "Location not available yet (still fetching metadata)"
		return
	}
	if err := revealInFileManager(path); err != nil {
		m.flash = fmt.Sprintf("Couldn't open location: %v", err)
	}
}

func (m model) Init() tea.Cmd {
	perfMarkf("ui-ready")
	// Start all managed sessions
	for _, s := range m.sessions {
		s.Start()
	}
	// The animation (pulse) tick is started on demand by the data tick once a
	// download is in progress, so an idle client does no per-frame rendering.
	return tea.Batch(tickCmd(), textinput.Blink, tea.SetWindowTitle(terminalWindowTitle))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// A flash message lives until the next keypress, so any key clears the
		// previous one before this key's handler optionally sets a new one.
		m.flash = ""
		switch m.viewMode {
		case viewInput:
			switch msg.String() {
			case "esc":
				m.resumePendingOr(viewList)
				m.inputMode = inputNone
				m.inputErr = ""
				m.textInput.Blur()
				return m, nil
			case "enter":
				val := strings.TrimSpace(m.textInput.Value())
				if val == "" && m.inputMode != inputLimitDownload && m.inputMode != inputLimitUpload {
					m.inputErr = "Input cannot be empty"
					return m, nil
				}

				switch m.inputMode {
				case inputAddTorrent:
					sess, err := addTorrentWithDownloadPaths(m.manager, val, m.downloadPaths)

					if err != nil {
						m.inputErr = fmt.Sprintf("Failed to load torrent: %v", err)
						return m, nil
					}
					sess.Start()
					m.refreshSessions()
					m.resumePendingOr(viewList)
					m.inputMode = inputNone
					m.inputErr = ""
					m.textInput.Blur()

				case inputLimitDownload:
					limitKb, err := strconv.ParseInt(val, 10, 64)
					if err != nil || limitKb < 0 {
						if val == "" || val == "0" {
							limitKb = 0
						} else {
							m.inputErr = "Please enter a non-negative number"
							return m, nil
						}
					}
					m.manager.SetGlobalDownloadLimit(limitKb * 1024)
					m.resumePendingOr(viewList)
					m.inputMode = inputNone
					m.inputErr = ""
					m.textInput.Blur()

				case inputLimitUpload:
					limitKb, err := strconv.ParseInt(val, 10, 64)
					if err != nil || limitKb < 0 {
						if val == "" || val == "0" {
							limitKb = 0
						} else {
							m.inputErr = "Please enter a non-negative number"
							return m, nil
						}
					}
					m.manager.SetGlobalUploadLimit(limitKb * 1024)
					m.resumePendingOr(viewList)
					m.inputMode = inputNone
					m.inputErr = ""
					m.textInput.Blur()
				}
				return m, nil
			}

			m.textInput, cmd = m.textInput.Update(msg)
			return m, cmd

		case viewList:
			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "up", "k":
				m.moveListSelection(-1)
			case "down", "j":
				m.moveListSelection(1)
			case "pgup":
				m.moveListSelection(-max(1, m.height/2))
			case "pgdown":
				m.moveListSelection(max(1, m.height/2))
			case "home":
				m.selectedIdx = 0
			case "end":
				if len(m.sessions) > 0 {
					m.selectedIdx = len(m.sessions) - 1
				}
			case " ":
				if s, ok := m.selectedSession(); ok {
					if s.IsPaused() {
						s.Resume()
					} else {
						s.Pause()
					}
				}
			case "enter":
				if _, ok := m.selectedSession(); ok {
					m.viewMode = viewDetail
					m.detailScroll = 0
					// Refresh the detail snapshot for the session just selected
					// (the cursor may have moved since the last data tick).
					m.buildDetailSnapshot()
				}
			case "a":
				m.viewMode = viewInput
				m.inputMode = inputAddTorrent
				m.inputErr = ""
				m.textInput.Reset()
				m.textInput.Focus()
				m.textInput.Placeholder = "Torrent filepath or Magnet URI"
			case "d":
				m.viewMode = viewInput
				m.inputMode = inputLimitDownload
				m.inputErr = ""
				m.textInput.Reset()
				m.textInput.Focus()
				m.textInput.Placeholder = "Download limit in KB/s (0 for unlimited)"
			case "u":
				m.viewMode = viewInput
				m.inputMode = inputLimitUpload
				m.inputErr = ""
				m.textInput.Reset()
				m.textInput.Focus()
				m.textInput.Placeholder = "Upload limit in KB/s (0 for unlimited)"
			case "o":
				m.openSelectedLocation()
			case "x":
				m.startDelete(false, viewList)
			case "X":
				m.startDelete(true, viewList)
			case "t":
				m.cycleTheme()
			}

		case viewDetail:
			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "esc":
				m.detailScroll = 0
				m.resumePendingOr(viewList)
			case "up", "k":
				m.scrollDetails(-1)
			case "down", "j":
				m.scrollDetails(1)
			case "pgup":
				m.scrollDetails(-max(1, m.height-1))
			case "pgdown":
				m.scrollDetails(max(1, m.height-1))
			case "home":
				m.detailScroll = 0
			case "end":
				m.detailScroll = m.detailMaxScroll()
			case " ":
				if s, ok := m.selectedSession(); ok {
					if s.IsPaused() {
						s.Resume()
					} else {
						s.Pause()
					}
				}
			case "f":
				if s, ok := m.selectedSession(); ok && !s.IsMetadataMode() {
					m.viewMode = viewFiles
					m.selectedFileIdx = 0
					m.buildFilesSnapshot()
				}
			case "o":
				m.openSelectedLocation()
			case "x":
				m.startDelete(false, viewDetail)
			case "X":
				m.startDelete(true, viewDetail)
			case "t":
				m.cycleTheme()
			}

		case viewFiles:
			s, ok := m.selectedSession()
			if !ok {
				m.resumePendingOr(viewList)
				return m, nil
			}
			files := s.Files()

			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "esc":
				m.viewMode = viewDetail
			case "up", "k":
				m.moveFileSelection(-1)
			case "down", "j":
				m.moveFileSelection(1)
			case "pgup":
				m.moveFilePage(-1)
			case "pgdown":
				m.moveFilePage(1)
			case "home":
				m.selectedFileIdx = 0
			case "end":
				m.selectedFileIdx = max(0, len(files)-1)
			case " ", "p":
				if len(files) > 0 && m.selectedFileIdx < len(files) {
					priorities := s.GetFilePriorities()
					current := downloader.PriorityNormal
					if m.selectedFileIdx < len(priorities) {
						current = priorities[m.selectedFileIdx]
					}
					next := downloader.PriorityNormal
					switch current {
					case downloader.PriorityNormal:
						next = downloader.PriorityHigh
					case downloader.PriorityHigh:
						next = downloader.PrioritySkip
					case downloader.PrioritySkip:
						next = downloader.PriorityNormal
					}
					s.SetFilePriority(m.selectedFileIdx, next)
					m.buildFilesSnapshot()
				}
			}

		case viewDeleteConfirm:
			if m.deleteInProgress {
				return m, nil
			}
			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "esc", "n", "N":
				if m.deleteErr != nil {
					m.resumePendingOr(viewList)
					m.deleteErr = nil
				} else {
					m.resumePendingOr(m.deleteOriginView)
				}
			case "y", "Y":
				if m.deleteErr != nil {
					m.resumePendingOr(viewList)
					m.deleteErr = nil
					return m, nil
				}
				if m.deleteTargetHash != "" {
					infoHashHex := m.deleteTargetHash
					deleteFiles := m.deleteWithFiles
					m.deleteInProgress = true
					return m, func() tea.Msg {
						return deleteFinishedMsg{
							infoHashHex: infoHashHex,
							err:         m.manager.RemoveSession(infoHashHex, deleteFiles),
						}
					}
				}
				m.refreshSessions()
				m.resumePendingOr(viewList)
			}

		case viewAddConfirm:
			if m.addConfirmErr != nil {
				switch msg.String() {
				case "esc", "n", "N", "y", "Y":
					m.addConfirmErr = nil
					m.pendingIdx++
					if m.pendingIdx >= len(m.pendingItems) {
						m.pendingItems = nil
						m.pendingIdx = 0
						m.viewMode = viewList
					}
				}
				return m, nil
			}

			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "y", "Y":
				if m.pendingIdx < len(m.pendingItems) {
					item := m.pendingItems[m.pendingIdx]
					var addErr error
					if !item.isDuplicate {
						sess, err := addTorrentWithDownloadPaths(m.manager, item.rawURL, item.pathOptions())
						addErr = err
						if addErr == nil {
							sess.Start()
						}
					} else {
						sess := m.manager.GetSession(item.infoHashHex)
						if sess != nil {
							sess.Resume()
						}
					}
					m.refreshSessions()
					if addErr != nil {
						m.addConfirmErr = addErr
					} else {
						m.addConfirmErr = nil
						m.pendingIdx++
						if m.pendingIdx >= len(m.pendingItems) {
							m.pendingItems = nil
							m.pendingIdx = 0
							m.viewMode = viewList
						}
					}
				}
				return m, nil
			case "n", "N":
				if m.pendingIdx < len(m.pendingItems) {
					m.addConfirmErr = nil
					m.pendingIdx++
					if m.pendingIdx >= len(m.pendingItems) {
						m.pendingItems = nil
						m.pendingIdx = 0
						m.viewMode = viewList
					}
				}
				return m, nil
			}
		}

	case tea.MouseMsg:
		const wheelStep = 3
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			switch m.viewMode {
			case viewList:
				m.moveListSelection(-wheelStep)
			case viewDetail:
				m.scrollDetails(-wheelStep)
			case viewFiles:
				m.moveFileSelection(-wheelStep)
			}
		case tea.MouseButtonWheelDown:
			switch m.viewMode {
			case viewList:
				m.moveListSelection(wheelStep)
			case viewDetail:
				m.scrollDetails(wheelStep)
			case viewFiles:
				m.moveFileSelection(wheelStep)
			}
		}
		return m, nil

	case addTorrentMsg:
		if !msg.msg.Confirm {
			var addErr error
			paths := socketDownloadPaths(msg.msg, m.downloadPaths)
			for _, item := range msg.msg.Items {
				sess, err := addTorrentWithDownloadPaths(m.manager, item, paths)
				if err == nil {
					sess.Start()
				} else {
					addErr = err
				}
			}
			m.refreshSessions()
			if msg.respChan != nil {
				select {
				case msg.respChan <- addTorrentResponse{err: addErr}:
				default:
				}
			}
			return m, nil
		}

		// Convert msg.msg.Items to pendingItems
		var newPending []pendingItem
		paths := socketDownloadPaths(msg.msg, m.downloadPaths)
		for _, item := range msg.msg.Items {
			name, hashHex, err := parseItem(item)
			isDuplicate := false
			if err == nil && hashHex != "" {
				if m.manager.GetSession(hashHex) != nil {
					isDuplicate = true
				}
			}
			displayName := item
			if err == nil && name != "" {
				displayName = name
			}
			displayName = displayText(displayName)

			pItem := pendingItem{
				rawURL:        item,
				displayName:   displayName,
				infoHashHex:   hashHex,
				downloadDir:   paths.primary,
				downloadPaths: paths,
				isDuplicate:   isDuplicate,
			}
			newPending = append(newPending, pItem)
		}

		if len(newPending) > 0 {
			if m.pendingIdx >= len(m.pendingItems) {
				m.pendingItems = nil
				m.pendingIdx = 0
			}
			m.pendingItems = append(m.pendingItems, newPending...)
			if m.viewMode == viewList || m.viewMode == viewDetail {
				m.viewMode = viewAddConfirm
			}
		}

		if msg.respChan != nil {
			select {
			case msg.respChan <- addTorrentResponse{}:
			default:
			}
		}
		return m, nil

	case deleteFinishedMsg:
		if msg.infoHashHex != m.deleteTargetHash {
			return m, nil
		}
		m.deleteInProgress = false
		m.refreshSessions()
		if errors.Is(msg.err, downloader.ErrFilesKept) {
			// Removed; only files a cross-seed still uses were kept.
			m.flash = "Removed; " + msg.err.Error()
			m.resumePendingOr(viewList)
			return m, nil
		}
		if msg.err != nil {
			m.deleteErr = msg.err
			m.viewMode = viewDeleteConfirm
			return m, nil
		}
		m.resumePendingOr(viewList)
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		inner := bodyWidth(m.width)
		m.progress.Width = inner
		m.textInput.Width = inner - dispWidth(m.textInput.Prompt)
		if m.textInput.Width < 1 {
			m.textInput.Width = 1
		}
		m.clampDetailScroll()
		return m, nil

	case tickMsg:
		if m.quitting {
			return m, nil
		}
		m.refreshSessions()
		m.recordSpeeds()
		if m.viewMode == viewFiles {
			m.refreshFilesSnapshot()
		}
		if m.viewMode == viewDetail {
			// Refresh the cached body now so the following View reuses it and a
			// scroll before the next tick clamps without re-rendering.
			m.ensureDetailBody()
		}
		// Restart the pulse loop if a download began while it was paused. The
		// animRunning guard keeps exactly one animCmd loop alive at a time.
		if m.wantAnim() && !m.animRunning {
			m.animRunning = true
			return m, tea.Batch(tickCmd(), animCmd())
		}
		return m, tickCmd()

	case animMsg:
		// Pure re-render to advance time-based animations; no data refresh. The
		// loop stops itself whenever nothing on screen is animating, and the data
		// tick restarts it when a download resumes.
		if m.quitting {
			return m, nil
		}
		if !m.wantAnim() {
			m.animRunning = false
			return m, nil
		}
		return m, animCmd()

	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		m.progress = progressModel.(progress.Model)
		return m, cmd
	}

	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return "\nShutting down saintTorrent client...\n"
	}

	var out string
	switch m.viewMode {
	case viewList:
		// list/details own their full screen (incl. theme-specific banner).
		out = m.theme.renderList(&m)
	case viewDetail:
		out = m.detailViewBody()
	default:
		// secondary screens share a layout under a themed banner.
		var sb strings.Builder
		sb.WriteString(m.secondaryBanner())
		switch m.viewMode {
		case viewFiles:
			sb.WriteString(m.viewFileExplorer())
		case viewInput:
			sb.WriteString(m.viewInputBox())
		case viewDeleteConfirm:
			sb.WriteString(m.viewDeleteConfirm())
		case viewAddConfirm:
			sb.WriteString(m.viewAddConfirm())
		}
		out = sb.String()
	}

	// Final safety net: never let any line exceed the width cap.
	out = clampLines(out, outerWidth(m.width))
	switch m.viewMode {
	case viewDetail:
		out = verticalSlice(out, m.detailScroll, m.height)
	case viewList, viewFiles:
		// Keep the header + list (incl. the selected row) and let the help
		// block clip from the bottom when the terminal is too short for all of it.
		out = verticalSlice(out, 0, m.height)
	}
	return out
}

// secondaryBanner is the themed banner above the secondary screens.
func (m model) secondaryBanner() string {
	return m.theme.styles.Title.Render(" saintTorrent CLI v0.2 ") + "\n"
}

func newTUIProgram(m tea.Model, opts ...tea.ProgramOption) *tea.Program {
	opts = append([]tea.ProgramOption{tea.WithAltScreen(), tea.WithMouseCellMotion()}, opts...)
	return tea.NewProgram(m, opts...)
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func formatSpeed(speed float64) string {
	if speed < 1024 {
		return fmt.Sprintf("%.0f B/s", speed)
	} else if speed < 1024*1024 {
		return fmt.Sprintf("%.1f KB/s", speed/1024)
	}
	return fmt.Sprintf("%.1f MB/s", speed/(1024*1024))
}

type appConfig struct {
	BinaryPath           string   `json:"binaryPath"`
	SocketPath           string   `json:"socketPath"`
	DefaultDownloadDir   string   `json:"defaultDownloadDir"`
	FallbackDownloadDirs []string `json:"fallbackDownloadDirs"`
	TerminalApp          string   `json:"terminalApp"`
}

func loadUserConfig(configDir string) (appConfig, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return appConfig{}, "", fmt.Errorf("get user home directory: %w", err)
	}
	cfg := appConfig{
		DefaultDownloadDir:   filepath.Join(home, "Downloads"),
		FallbackDownloadDirs: []string{},
	}
	defaultDownloadDir := cfg.DefaultDownloadDir
	if configDir == "" {
		configDir = filepath.Join(home, ".config", "sainttorrent")
	}
	configPath := filepath.Join(configDir, "config.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, configPath, nil
		}
		return appConfig{}, configPath, err
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return appConfig{}, configPath, err
	}
	if cfg.DefaultDownloadDir == "" {
		cfg.DefaultDownloadDir = defaultDownloadDir
	}
	if cfg.FallbackDownloadDirs == nil {
		cfg.FallbackDownloadDirs = []string{}
	}
	for index, dir := range cfg.FallbackDownloadDirs {
		if dir == "" {
			return appConfig{}, configPath, fmt.Errorf("fallbackDownloadDirs[%d] must be a non-empty string", index)
		}
	}
	return cfg, configPath, nil
}

func applyUserDownloadConfig(opts *cliOptions, cfg appConfig) {
	if !opts.downloadDirSet && cfg.DefaultDownloadDir != "" {
		opts.downloadDir = cfg.DefaultDownloadDir
	}
	if !opts.fallbackDirsSet && cfg.FallbackDownloadDirs != nil {
		opts.fallbackDownloadDirs = append([]string{}, cfg.FallbackDownloadDirs...)
	}
}

// magnetPrefix is the only magnet form accepted; torrent.ParseMagnet requires
// this exact lowercase spelling.
const magnetPrefix = "magnet:?"

// urlScheme returns the lowercased RFC 3986 scheme of item, or "" for a plain
// path. A scheme needs at least two characters so Windows drive letters
// (C:\x.torrent) stay paths.
func urlScheme(item string) string {
	for i := 0; i < len(item); i++ {
		c := item[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case i > 0 && ('0' <= c && c <= '9' || c == '+' || c == '-' || c == '.'):
		case c == ':' && i >= 2:
			return strings.ToLower(item[:i])
		default:
			return ""
		}
	}
	return ""
}

// canonicalItem classifies a torrent source from the command line, the IPC
// socket or the add prompt. A magnet link comes back with its scheme
// lowercased; a plain path is returned as is. Any other URL is rejected,
// including an opaque "magnet:x/../../dev/zero": the macOS launcher forwards
// whatever a web page links to, and reading that as a relative path (which
// filepath.Abs would clean to /dev/zero) would let a page pick a local file.
func canonicalItem(item string) (canonical string, isMagnet bool, err error) {
	switch urlScheme(item) {
	case "":
		return item, false, nil
	case "magnet":
		if len(item) >= len(magnetPrefix) && item[len(magnetPrefix)-1] == '?' {
			return magnetPrefix + item[len(magnetPrefix):], true, nil
		}
		return "", false, fmt.Errorf("invalid magnet link %q: must start with %q", boundText(item, 128), magnetPrefix)
	default:
		return "", false, fmt.Errorf("unsupported URL %q: pass a .torrent file path (./name for a name with ':') or a %s link", boundText(item, 128), magnetPrefix)
	}
}

func parseItem(item string) (name string, hashHex string, err error) {
	item, isMagnet, err := canonicalItem(item)
	if err != nil {
		return "", "", err
	}
	if isMagnet {
		mag, err := torrent.ParseMagnet(item)
		if err != nil {
			return "", "", err
		}
		return mag.Name, fmt.Sprintf("%x", mag.InfoHash), nil
	}
	data, err := readTorrentFile(item)
	if err != nil {
		return "", "", err
	}
	tor, err := torrent.Parse(data)
	if err != nil {
		return "", "", err
	}
	return tor.Name, fmt.Sprintf("%x", tor.InfoHash), nil
}

// readTorrentFile reads a .torrent from disk the way the manager does
// (torrent.ReadFile: at most torrent.MaxFileSize, regular files only).
// parseItem can run on the TUI's event loop, where opening a FIFO blocks and
// a device such as /dev/zero never ends. torrent.ReadFile checks what it
// opened; the Stat here also refuses those before any open, which keeps the
// event loop safe with a torrent.ReadFile that opens before it checks.
func readTorrentFile(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s: not a regular file", path)
	}
	return torrent.ReadFile(path)
}

// normalizeForwardedItems prepares items for a running instance, whose working
// directory differs: file paths are made absolute and magnet links are
// canonicalized. Anything with another URL scheme is rejected here rather
// than cleaned into a path.
func normalizeForwardedItems(items []string) ([]string, error) {
	normalized := make([]string, 0, len(items))
	for _, item := range items {
		canonical, isMagnet, err := canonicalItem(item)
		if err != nil {
			return nil, err
		}
		if isMagnet {
			normalized = append(normalized, canonical)
			continue
		}
		absPath, err := filepath.Abs(canonical)
		if err != nil {
			normalized = append(normalized, canonical)
			continue
		}
		normalized = append(normalized, absPath)
	}
	return normalized, nil
}

// usageText returns the help message printed for -h/--help.
func usageText() string {
	return `saintTorrent - a beautiful, high-performance BitTorrent client for the terminal.

Usage:
  sainttorrent [options] [torrent-file-or-magnet-uri ...]
  sainttorrent kill

Commands:
  kill, stop                Stop the running saintTorrent instance and exit

Options:
  -d, --dir <path>          Preferred download directory (default ~/Downloads)
      --fallback-dir <path> Fallback download directory (repeatable)
  -c, --config <path>       Configuration/IPC directory
  -k, --kill                Stop the running saintTorrent instance and exit
  -p, --port <port>         Peer listen port (0 for ephemeral, default 51413)
      --no-nat              Disable automatic UPnP/NAT-PMP port mapping
      --encryption <mode>   Peer encryption: prefer, require, or disable (default prefer)
      --storage <backend>   Storage backend: file, mmap, or mem (default file)
      --theme <name>        Color theme
      --headless            Run without the TUI
      --confirm             Require confirmation before adding forwarded torrents
      --no-confirm          Skip confirmation when adding forwarded torrents
      --no-persist          Keep no state: nothing is restored on the next
                            launch, and crash handling is off
      --start-paused        Restore every torrent paused for this run, including
                            any a crash left unloaded
      --recheck             Fully hash-check restored torrents on this launch
      --http-addr <addr>    Enable the read-only JSON stats API on this address
                            (loopback only, e.g. 127.0.0.1:16666)
      --http-allow-remote   Allow --http-addr on a LAN or wildcard address; the
                            API has no authentication
      --http-allow-host <name>
                            Also answer requests for this host name, e.g. the
                            LAN or reverse-proxy name (repeatable)
      --log <path>          Write JSON-lines debug logs to a rotating file, or
                            to /dev/stderr or /dev/stdout
      --log-level <level>   Log level: debug, info, warn, or error
      --write-config <path> Write a default config file and exit
  -h, --help                Show this help message and exit
  -v, --version             Show version information and exit

Examples:
  sainttorrent -d ~/Downloads
  sainttorrent -d "/Volumes/External SSD/Downloads" --fallback-dir ~/Downloads
  sainttorrent -d ~/Downloads "magnet:?xt=urn:btih:..."
  sainttorrent --headless --http-addr 127.0.0.1:16666
  sainttorrent kill
`
}

func parseCLIArgs(args []string) cliOptions {
	opts := cliOptions{
		downloadDir: ".",
		persist:     true,
		confirm:     true,
		listenPort:  defaultPeerPort,
		natEnabled:  true,
		encryption:  mse.PolicyPrefer,
		storage:     storage.BackendFile,
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-d", "--dir":
			if i+1 < len(args) {
				opts.downloadDir = args[i+1]
				opts.downloadDirSet = true
				i++
			} else {
				opts.err = fmt.Errorf("%s requires a directory path", args[i])
			}
		case "--fallback-dir":
			if i+1 < len(args) {
				opts.fallbackDownloadDirs = append(opts.fallbackDownloadDirs, args[i+1])
				opts.fallbackDirsSet = true
				i++
			} else {
				opts.err = fmt.Errorf("%s requires a directory path", args[i])
			}
		case "-c", "--config":
			if i+1 < len(args) {
				opts.configDir = args[i+1]
				i++
			} else {
				opts.err = fmt.Errorf("%s requires a directory path", args[i])
			}
		case "--recheck":
			opts.verifyOnStartup = true
		case "--no-persist":
			opts.persist = false
		case "--start-paused":
			opts.startPaused = true
		case "--confirm":
			opts.confirm = true
		case "--no-confirm":
			opts.confirm = false
		case "--headless":
			opts.headless = true
		case "--theme":
			if i+1 < len(args) {
				opts.theme = args[i+1]
				i++
			}
		case "-p", "--port":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires a port", args[i])
				continue
			}
			port, err := strconv.Atoi(args[i+1])
			i++
			if err != nil || port < 0 || port > 65535 {
				opts.err = fmt.Errorf("invalid peer port %q", args[i])
				continue
			}
			opts.listenPort = port
		case "--http-addr", "--http":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires a listen address", args[i])
				continue
			}
			opts.httpAddr = args[i+1]
			i++
		case "--http-allow-remote":
			opts.httpAllowRemote = true
		case "--http-allow-host":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires a host name", args[i])
				continue
			}
			name := strings.TrimSpace(args[i+1])
			i++
			if err := httpapi.CheckAllowHost(name); err != nil {
				opts.err = fmt.Errorf("--http-allow-host: %w", err)
				continue
			}
			opts.httpAllowHosts = append(opts.httpAllowHosts, name)
		case "--no-nat":
			opts.natEnabled = false
		case "--encryption":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires prefer, require, or disable", args[i])
				continue
			}
			policy, err := mse.ParsePolicy(args[i+1])
			i++
			if err != nil {
				opts.err = err
				continue
			}
			opts.encryption = policy
		case "--storage":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires file, mmap, or mem", args[i])
				continue
			}
			backend, err := storage.ParseBackend(args[i+1])
			i++
			if err != nil {
				opts.err = err
				continue
			}
			opts.storage = backend
		case "--log":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires a file path", args[i])
				continue
			}
			opts.logPath = args[i+1]
			i++
		case "--log-level":
			if i+1 >= len(args) {
				opts.err = fmt.Errorf("%s requires debug, info, warn, or error", args[i])
				continue
			}
			level, err := logging.ParseLevel(args[i+1])
			i++
			if err != nil {
				opts.err = err
				continue
			}
			opts.logLevel = level
			opts.logLevelSet = true
		case "-k", "--kill", "--stop":
			opts.kill = true
		case "kill", "stop":
			opts.kill = true
		case "-h", "--help":
			opts.help = true
		case "-v", "--version":
			opts.showVersion = true
		default:
			opts.items = append(opts.items, args[i])
		}
	}
	return opts
}

func resolveIPCDir() (string, error) {
	if envDir := os.Getenv("SAINTTORRENT_IPC_DIR"); envDir != "" {
		absDir, err := filepath.Abs(envDir)
		if err != nil {
			return "", fmt.Errorf("failed to get absolute path for SAINTTORRENT_IPC_DIR: %w", err)
		}
		if err := os.MkdirAll(absDir, 0700); err != nil {
			return "", fmt.Errorf("failed to create IPC directory: %w", err)
		}
		if err := os.Chmod(absDir, 0700); err != nil {
			return "", fmt.Errorf("failed to chmod IPC directory: %w", err)
		}
		sockPath := filepath.Join(absDir, "sainttorrent.sock")
		if len(sockPath) >= 104 {
			return "", fmt.Errorf("resolved socket path %q too long (%d bytes, max 103)", sockPath, len(sockPath))
		}
		return absDir, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get user home directory: %w", err)
	}
	dir := filepath.Join(home, ".config", "sainttorrent")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", fmt.Errorf("failed to create IPC directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", fmt.Errorf("failed to chmod IPC directory: %w", err)
	}
	sockPath := filepath.Join(dir, "sainttorrent.sock")
	if len(sockPath) >= 104 {
		return "", fmt.Errorf("resolved socket path %q too long (%d bytes, max 103)", sockPath, len(sockPath))
	}
	return dir, nil
}

// The PID file names the process holding sainttorrent.lock, so `sainttorrent
// kill` can signal it when the IPC socket does not answer. It sits beside the
// lock rather than inside it: Windows locks the lock file's first byte, so no
// other process can read a PID stored there.
func pidFilePath(ipcDir string) string {
	return filepath.Join(ipcDir, "sainttorrent.pid")
}

func writePID(ipcDir string) {
	_ = os.WriteFile(pidFilePath(ipcDir), []byte(strconv.Itoa(os.Getpid())+"\n"), 0600)
}

func removePID(ipcDir string) {
	_ = os.Remove(pidFilePath(ipcDir))
}

func readPID(ipcDir string) int {
	data, err := os.ReadFile(pidFilePath(ipcDir))
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// lockHolderPID picks the process to signal when the instance holding lockPath
// did not stop over IPC. The PID file is trusted only while it names a running
// saintTorrent process: a crash leaves it behind, and its PID may since have
// been reused. Without a usable PID file (an instance started before PID files
// existed), a process search is accepted only when it finds exactly one other
// saintTorrent process, since it cannot tell which IPC directory each serves.
func lockHolderPID(ipcDir, lockPath string, isSaintTorrent func(int) bool, findPIDs func() []int) (int, error) {
	self := os.Getpid()
	if pid := readPID(ipcDir); pid > 0 && pid != self && isSaintTorrent(pid) {
		return pid, nil
	}
	var candidates []int
	for _, pid := range findPIDs() {
		if pid != self {
			candidates = append(candidates, pid)
		}
	}
	switch len(candidates) {
	case 0:
		return 0, fmt.Errorf("running instance detected, but process ID could not be determined")
	case 1:
		return candidates[0], nil
	default:
		return 0, fmt.Errorf("running instance detected, but %d saintTorrent processes are running (PIDs %v) and the one holding %s cannot be identified; stop it manually", len(candidates), candidates, lockPath)
	}
}

func killRunningInstance() error {
	ipcDir, err := resolveIPCDir()
	if err != nil {
		return fmt.Errorf("resolving IPC directory: %w", err)
	}

	lockPath := filepath.Join(ipcDir, "sainttorrent.lock")
	socketPath := filepath.Join(ipcDir, "sainttorrent.sock")

	lockFile, lockErr := acquireLock(lockPath)
	if lockErr == nil {
		// Clear stale files while still holding the lock, so an instance that
		// starts right now cannot have its fresh socket or PID file deleted.
		_ = os.Remove(socketPath)
		removePID(ipcDir)
		_ = lockFile.Close()
		fmt.Println("No running saintTorrent instance found.")
		return nil
	}

	if !errors.Is(lockErr, errLockContention) {
		return fmt.Errorf("checking lock: %w", lockErr)
	}

	// Instance is holding the lock. First, try graceful stop via IPC socket.
	conn, dialErr := net.DialTimeout("unix", socketPath, 1*time.Second)
	if dialErr == nil {
		msg := socketMessage{Action: "kill"}
		if data, err := json.Marshal(msg); err == nil {
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if err := writeFrame(conn, data); err == nil {
				_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 256)
				_, _ = conn.Read(buf)
			}
		}
		_ = conn.Close()

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(100 * time.Millisecond)
			if testLock, testErr := acquireLock(lockPath); testErr == nil {
				_ = testLock.Close()
				fmt.Println("saintTorrent instance stopped.")
				return nil
			}
		}
	}

	// If socket failed or instance did not stop within deadline, terminate by PID.
	pid, err := lockHolderPID(ipcDir, lockPath, isSaintTorrentProcess, findProcessPIDs)
	if err != nil {
		return err
	}
	_ = terminateProcess(pid)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		if testLock, testErr := acquireLock(lockPath); testErr == nil {
			_ = testLock.Close()
			fmt.Println("saintTorrent instance stopped.")
			return nil
		}
	}

	_ = killProcess(pid)

	time.Sleep(200 * time.Millisecond)
	if testLock, testErr := acquireLock(lockPath); testErr == nil {
		_ = testLock.Close()
		fmt.Println("saintTorrent instance killed.")
		return nil
	}

	return fmt.Errorf("failed to kill running saintTorrent instance")
}

var activeConns struct {
	sync.Mutex
	conns map[net.Conn]struct{}
}

func registerConn(conn net.Conn) {
	activeConns.Lock()
	if activeConns.conns == nil {
		activeConns.conns = make(map[net.Conn]struct{})
	}
	activeConns.conns[conn] = struct{}{}
	activeConns.Unlock()
}

func unregisterConn(conn net.Conn) {
	activeConns.Lock()
	if activeConns.conns != nil {
		delete(activeConns.conns, conn)
	}
	activeConns.Unlock()
}

func closeActiveConns() {
	activeConns.Lock()
	var conns []net.Conn
	for conn := range activeConns.conns {
		conns = append(conns, conn)
	}
	activeConns.Unlock()

	for _, conn := range conns {
		conn.Close()
	}
}

func writeFrame(conn net.Conn, payload []byte) error {
	data := append(payload, '\n')
	written := 0
	for written < len(data) {
		n, err := conn.Write(data[written:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		written += n
	}
	return nil
}

func handleSocketConnection(conn net.Conn, shutdownChan chan struct{}, mgr *downloader.TorrentManager, handlersWG *sync.WaitGroup, terminal terminalIdentity, headless bool, defaults downloadPathOptions) {
	defer handlersWG.Done()
	defer unregisterConn(conn)
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		sendResponse(conn, "error", fmt.Sprintf("set read deadline error: %v", err), terminal)
		return
	}

	var requestData []byte
	buf := make([]byte, 1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			sendResponse(conn, "error", fmt.Sprintf("read error: %v", err), terminal)
			return
		}
		if n > 0 {
			if buf[0] == '\n' {
				break
			}
			requestData = append(requestData, buf[0])
			if len(requestData) > 65536 {
				sendResponse(conn, "error", "request frame too large (max 65536 bytes)", terminal)
				return
			}
		}
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		sendResponse(conn, "error", fmt.Sprintf("clear read deadline error: %v", err), terminal)
		return
	}

	var msg socketMessage
	if err := json.Unmarshal(requestData, &msg); err != nil {
		sendResponse(conn, "error", fmt.Sprintf("invalid JSON payload: %v", err), terminal)
		return
	}

	if msg.Action == "kill" || msg.Action == "quit" || msg.Action == "stop" {
		sendResponse(conn, "ok", "shutting down", terminal)
		triggerShutdown()
		return
	}

	programMu.RLock()
	p := teaProgram
	programMu.RUnlock()

	if p == nil {
		if !headless {
			sendResponse(conn, "starting", "saintTorrent is starting up", terminal)
			return
		}
		if err := handleHeadlessSocketMessage(msg, mgr, defaults); err != nil {
			sendResponse(conn, "error", err.Error(), terminal)
		} else {
			sendResponse(conn, "ok", "torrent request handled", terminal)
		}
		return
	}

	respChan := make(chan addTorrentResponse, 1)
	select {
	case <-shutdownChan:
		sendResponse(conn, "error", "application is shutting down", terminal)
		return
	default:
		p.Send(addTorrentMsg{msg: msg, respChan: respChan})
	}

	select {
	case resp := <-respChan:
		if resp.err != nil {
			sendResponse(conn, "error", resp.err.Error(), terminal)
		} else {
			sendResponse(conn, "ok", "torrent request handled", terminal)
		}
	case <-shutdownChan:
		sendResponse(conn, "error", "application is shutting down", terminal)
	case <-time.After(3 * time.Second):
		sendResponse(conn, "error", "TUI processing timeout", terminal)
	}
}

func handleHeadlessSocketMessage(msg socketMessage, mgr *downloader.TorrentManager, defaults downloadPathOptions) error {
	if msg.Confirm {
		return fmt.Errorf("confirmation is unavailable in headless mode; retry with --no-confirm")
	}

	paths := socketDownloadPaths(msg, defaults)

	var errs []error
	for _, item := range msg.Items {
		sess, err := addTorrentWithDownloadPaths(mgr, item, paths)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		sess.Start()
	}
	return errors.Join(errs...)
}

func sendResponse(conn net.Conn, status string, message string, terminal terminalIdentity) {
	resp := socketResponse{
		Status:          status,
		Message:         message,
		TerminalTTY:     terminal.TTY,
		TerminalProgram: terminal.Program,
		TerminalTitle:   terminal.Title,
	}
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}
	_ = writeFrame(conn, data)
}

func (m *model) resolveRemainingPending(err error) {
	for i := m.pendingIdx; i < len(m.pendingItems); i++ {
		item := m.pendingItems[i]
		if item.respChan != nil {
			select {
			case item.respChan <- addTorrentResponse{err: err}:
			default:
			}
		}
	}
}

func main() {
	perfInit()
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--write-config" {
			if i+1 >= len(os.Args) {
				fmt.Fprintln(os.Stderr, "Error: --write-config requires an output file path")
				os.Exit(1)
			}
			outputPath := os.Args[i+1]
			ipcDir, err := resolveIPCDir()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error resolving IPC directory: %v\n", err)
				os.Exit(1)
			}
			socketPath := filepath.Join(ipcDir, "sainttorrent.sock")
			execPath, err := os.Executable()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error getting executable path: %v\n", err)
				os.Exit(1)
			}
			home, err := os.UserHomeDir()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error getting user home directory: %v\n", err)
				os.Exit(1)
			}
			cfg := appConfig{
				BinaryPath:           execPath,
				SocketPath:           socketPath,
				DefaultDownloadDir:   filepath.Join(home, "Downloads"),
				FallbackDownloadDirs: []string{},
				TerminalApp:          "Terminal",
			}
			data, err := json.MarshalIndent(cfg, "", "  ")
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error marshaling config: %v\n", err)
				os.Exit(1)
			}
			if err := os.WriteFile(outputPath, data, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Error writing config file: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	opts := parseCLIArgs(os.Args[1:])
	if opts.help {
		fmt.Fprint(os.Stdout, usageText())
		os.Exit(0)
	}
	if opts.showVersion {
		fmt.Fprintf(os.Stdout, "saintTorrent %s\n", version)
		os.Exit(0)
	}
	if opts.err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", opts.err)
		os.Exit(2)
	}
	if opts.kill {
		if err := killRunningInstance(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	userConfig, userConfigPath, configErr := loadUserConfig(opts.configDir)
	if configErr != nil {
		fmt.Fprintf(os.Stderr, "Error loading config %s: %v\n", userConfigPath, configErr)
		os.Exit(2)
	}
	applyUserDownloadConfig(&opts, userConfig)
	logConfig, err := logging.ConfigFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring debug log: %v\n", err)
		os.Exit(2)
	}
	if opts.logPath != "" {
		logConfig.Path = opts.logPath
	}
	if opts.logLevelSet {
		logConfig.Level = opts.logLevel
	}
	if err := logging.Configure(logConfig); err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring debug log: %v\n", err)
		os.Exit(1)
	}
	defer logging.Close()
	redirectStdLog()

	downloadPaths := downloadPathOptions{
		primary:   opts.downloadDir,
		fallbacks: opts.fallbackDownloadDirs,
	}.normalized()
	downloadDir := downloadPaths.primary
	configDir := opts.configDir
	persist := opts.persist
	confirmFlag := opts.confirm
	filesToAdd := opts.items

	if logging.Enabled() {
		logging.Info("app_start",
			logging.String("download_dir", downloadDir),
			logging.Int("fallback_download_dirs", len(downloadPaths.fallbacks)),
			logging.Int("listen_port", opts.listenPort),
			logging.Bool("nat_enabled", opts.natEnabled),
			logging.String("encryption", opts.encryption.String()),
			logging.String("storage", string(opts.storage)),
		)
	}

	ipcDir, err := resolveIPCDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error resolving IPC directory: %v\n", err)
		os.Exit(1)
	}

	lockPath := filepath.Join(ipcDir, "sainttorrent.lock")
	socketPath := filepath.Join(ipcDir, "sainttorrent.sock")
	lockFile, lockErr := acquireLock(lockPath)
	if lockErr != nil {
		if !errors.Is(lockErr, errLockContention) {
			fmt.Fprintf(os.Stderr, "Fatal lock error: %v\n", lockErr)
			os.Exit(1)
		}

		if len(filesToAdd) == 0 {
			fmt.Println("saintTorrent is already running.")
			os.Exit(0)
		}

		normalizedItems, err := normalizeForwardedItems(filesToAdd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %s\n", escapeForTerminal(err.Error()))
			os.Exit(2)
		}

		var conn net.Conn
		var connErr error
		var resp socketResponse
		success := false

		for retry := 0; retry < 120; retry++ {
			conn, connErr = net.Dial("unix", socketPath)
			if connErr != nil {
				time.Sleep(250 * time.Millisecond)
				continue
			}

			msg := socketMessage{
				Items:                normalizedItems,
				Confirm:              confirmFlag,
				DownloadDir:          downloadPaths.primary,
				FallbackDownloadDirs: downloadPaths.fallbacks,
			}
			data, err := json.Marshal(msg)
			if err != nil {
				conn.Close()
				fmt.Fprintf(os.Stderr, "Error encoding request: %v\n", err)
				os.Exit(1)
			}

			if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				conn.Close()
				fmt.Fprintf(os.Stderr, "Error setting write deadline: %v\n", err)
				os.Exit(1)
			}
			if err := writeFrame(conn, data); err != nil {
				conn.Close()
				time.Sleep(250 * time.Millisecond)
				continue
			}

			if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				conn.Close()
				fmt.Fprintf(os.Stderr, "Error setting read deadline: %v\n", err)
				os.Exit(1)
			}

			var respData []byte
			buf := make([]byte, 1)
			readErr := error(nil)
			for {
				n, err := conn.Read(buf)
				if err != nil {
					if err == io.EOF {
						break
					}
					readErr = err
					break
				}
				if n > 0 {
					if buf[0] == '\n' {
						break
					}
					respData = append(respData, buf[0])
				}
			}

			if readErr != nil {
				conn.Close()
				time.Sleep(250 * time.Millisecond)
				continue
			}

			if err := json.Unmarshal(respData, &resp); err != nil {
				conn.Close()
				fmt.Fprintf(os.Stderr, "Error parsing response: %v\n", err)
				os.Exit(1)
			}

			if resp.Status == "starting" {
				conn.Close()
				time.Sleep(250 * time.Millisecond)
				continue
			}

			if resp.Status != "ok" {
				conn.Close()
				fmt.Fprintf(os.Stderr, "Error from running instance: %s\n", escapeForTerminal(resp.Message))
				os.Exit(1)
			}

			conn.Close()
			success = true
			break
		}

		if !success {
			if connErr != nil {
				fmt.Fprintf(os.Stderr, "Error connecting to running instance: %v\n", connErr)
			} else {
				fmt.Fprintf(os.Stderr, "Error from running instance: client timed out waiting for server startup\n")
			}
			os.Exit(1)
		}

		fmt.Println("Torrents forwarded successfully.")
		os.Exit(0)
	}

	defer func() {
		if lockFile != nil {
			lockFile.Close()
		}
	}()
	writePID(ipcDir)
	defer removePID(ipcDir)

	var startupInfos []string
	var startupWarns []string
	// Shown ahead of startupWarns: see leadStartupWarnings.
	var exposureWarn, persistWarn string

	// Resolve the state directory before anything starts goroutines, so a
	// fatal error from here on also lands in <configDir>/crash/fatal.txt.
	if persist {
		if configDir == "" {
			userConfig, err := os.UserConfigDir()
			if err == nil {
				configDir = filepath.Join(userConfig, "sainttorrent")
			} else {
				configDir = ".sainttorrent"
			}
		}
		if crashLog, err := setUpCrashOutput(configDir); err != nil {
			startupWarns = append(startupWarns, fmt.Sprintf("Crash log unavailable: %v", err))
		} else {
			// Open for the life of the process.
			defer crashLog.Close()
		}
	}

	selectedDownloadDir, pathErr := selectDownloadPath(downloadPaths)
	if pathErr != nil {
		startupWarns = append(startupWarns, pathErr.Error())
	} else {
		downloadDir = selectedDownloadDir
		if downloadDir != downloadPaths.primary {
			startupWarns = append(startupWarns, fmt.Sprintf("Using fallback download directory %s; preferred directory is unavailable", downloadDir))
		}
	}

	mgr := downloader.NewTorrentManager()
	mgr.SetEncryptionPolicy(opts.encryption)
	mgr.SetVerifyOnStartup(opts.verifyOnStartup)
	mgr.SetStartPaused(opts.startPaused)
	if err := mgr.SetStorageBackend(opts.storage); err != nil {
		fmt.Fprintf(os.Stderr, "Error configuring storage backend: %v\n", err)
		mgr.Close()
		os.Exit(1)
	}
	perfMarkf("manager")
	if err := mgr.StartPeerListener(uint16(opts.listenPort)); err != nil {
		fmt.Fprintf(os.Stderr, "Error starting peer listener on port %d: %v\n", opts.listenPort, err)
		fmt.Fprintln(os.Stderr, "Choose another stable port with --port.")
		mgr.Close()
		os.Exit(1)
	}
	perfMarkf("peer-listener")

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error removing stale socket file: %v\n", err)
		os.Exit(1)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting socket listener: %v\n", err)
		os.Exit(1)
	}
	if err := setSocketPermissions(socketPath); err != nil {
		listener.Close()
		fmt.Fprintf(os.Stderr, "Error setting socket file permissions: %v\n", err)
		os.Exit(1)
	}

	shutdownChan := make(chan struct{})
	var handlersWG sync.WaitGroup
	var acceptLoopWG sync.WaitGroup
	terminal := terminalIdentity{
		TTY:     detectTerminalTTY(os.Stdin),
		Program: os.Getenv("TERM_PROGRAM"),
		Title:   terminalWindowTitle,
	}

	acceptLoopWG.Add(1)
	go func() {
		defer acceptLoopWG.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			registerConn(conn)
			handlersWG.Add(1)
			go handleSocketConnection(conn, shutdownChan, mgr, &handlersWG, terminal, opts.headless, downloadPaths)
		}
	}()

	if err := mgr.StartDHT(downloadDir, int(mgr.PeerListenPort())); err != nil {
		startupWarns = append(startupWarns, fmt.Sprintf("DHT unavailable: %v", err))
	}
	if opts.natEnabled {
		if err := mgr.StartNATTraversal(mgr.PeerListenPort(), mgr.DHTListenPort()); err != nil {
			startupWarns = append(startupWarns, fmt.Sprintf("NAT traversal unavailable: %v", err))
		}
	}
	perfMarkf("dht")

	var statsServer *httpapi.Server
	if opts.httpAddr != "" {
		statsServer, err = httpapi.Start(opts.httpAddr, mgr, httpapi.Options{
			AllowRemote: opts.httpAllowRemote,
			AllowHosts:  opts.httpAllowHosts,
		})
		if err != nil {
			listener.Close()
			acceptLoopWG.Wait()
			close(shutdownChan)
			closeActiveConns()
			handlersWG.Wait()
			mgr.Close()
			fmt.Fprintf(os.Stderr, "Error starting HTTP stats endpoint on %s: %v\n", opts.httpAddr, err)
			if errors.Is(err, httpapi.ErrNotLoopback) {
				fmt.Fprintln(os.Stderr, "Use a loopback address such as 127.0.0.1:16666, or pass --http-allow-remote to expose it without authentication.")
			}
			os.Exit(1)
		}
		startupInfos = append(startupInfos, fmt.Sprintf("HTTP stats endpoint: http://%s/stats", statsServer.Addr()))
		if !statsServer.Loopback() {
			exposureWarn = fmt.Sprintf("HTTP stats API on %s is reachable from the network without authentication", statsServer.Addr())
		}
	}
	perfMarkf("http-stats")

	if persist {
		warning, err := mgr.EnablePersistence(configDir)
		if err != nil {
			persistWarn = fmt.Sprintf("Failed to initialize persistence: %v", err)
		} else {
			persistWarn = warning
		}
	}
	perfMarkf("persistence")

	// Theme: --theme flag overrides persisted preference overrides default.
	selectedTheme := resolveInitialTheme(opts.theme, persist, configDir, &startupWarns)

	var initialPending []pendingItem
	for _, item := range filesToAdd {
		if opts.headless {
			sess, err := addTorrentWithDownloadPaths(mgr, item, downloadPaths)
			if err != nil {
				startupWarns = append(startupWarns, fmt.Sprintf("Failed to load torrent %s: %v", item, err))
				continue
			}
			sess.Start()
			continue
		}

		name, hashHex, err := parseItem(item)
		isDuplicate := false
		if err == nil && hashHex != "" {
			if mgr.GetSession(hashHex) != nil {
				isDuplicate = true
			}
		}
		displayName := item
		if err == nil && name != "" {
			displayName = name
		}
		displayName = displayText(displayName)
		initialPending = append(initialPending, pendingItem{
			rawURL:        item,
			displayName:   displayName,
			infoHashHex:   hashHex,
			downloadDir:   downloadDir,
			downloadPaths: downloadPaths,
			isDuplicate:   isDuplicate,
		})
	}

	startupWarns = leadStartupWarnings(exposureWarn, persistWarn, startupWarns)
	startupWarn := tuiStartupLine(startupInfos, startupWarns)

	exitCode := 0
	var p *tea.Program
	if !opts.headless {
		startModel := initialModel(mgr, downloadDir, startupWarn, initialPending)
		startModel.downloadPaths = downloadPaths
		startModel.theme = selectedTheme
		startModel.configDir = configDir
		startModel.persistEnabled = persist
		// This is a full-screen TUI. The alternate screen prevents terminal
		// scrollback reflow during resize from leaving stale copies of prior frames.
		p = newTUIProgram(startModel)
		perfMarkf("tui-build")

		setTeaProgram(p)
	} else {
		perfMarkf("headless-ready")
	}

	benchMode := os.Getenv("SAINTTORRENT_BENCH") == "1"
	if benchMode {
		// Headless measurement: run the real startup work and emulate UI bring-up
		// (start sessions exactly like model.Init), then fall through to the real
		// teardown below — no interactive TUI. Makes start+close scriptable with `time`.
		for _, s := range mgr.ListSessions() {
			s.Start()
		}
		perfMarkf("ui-ready")
		fmt.Printf("startup_ms=%.1f\n", msOf(time.Since(perfStart)))
	} else if opts.headless {
		for _, s := range mgr.ListSessions() {
			s.Start()
		}
		perfMarkf("ui-ready")
		writeHeadlessStartupMessages(os.Stderr, startupInfos, startupWarns)
		waitForShutdownSignal()
	} else {
		// Closing the terminal window quits like q does (see notifyHangup).
		hangup := make(chan os.Signal, 1)
		if notifyHangup(hangup) {
			go func() {
				<-hangup
				p.Quit()
			}()
		}
		if _, err := p.Run(); err != nil {
			if errors.Is(err, tea.ErrProgramPanic) {
				// Bubble Tea recovered the panic, printed it and restored the
				// terminal. Record it and keep the running sentinel, so the next
				// start counts this run as a crash; still shut down normally, as
				// the torrents' state is intact, then exit non-zero.
				exitCode = 1
				mgr.MarkUncleanExit()
				if mgr.RecordCrash("tui", "", err) != "" {
					fmt.Fprintf(os.Stderr, "saintTorrent's UI crashed; details in %s\n", downloader.CrashDir(configDir))
				} else {
					fmt.Fprintf(os.Stderr, "saintTorrent's UI crashed: %v\n", err)
				}
			} else {
				fmt.Printf("Error running UI: %v\n", err)
			}
		}
		perfMarkf("quit")
	}

	shutdownStart := time.Now()
	if statsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := statsServer.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error stopping HTTP stats endpoint: %v\n", err)
		}
		cancel()
	}
	listener.Close()
	acceptLoopWG.Wait()
	close(shutdownChan)
	closeActiveConns()
	handlersWG.Wait()
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error removing socket file: %v\n", err)
	}
	// The PID file stays until mgr.Close finishes, so `sainttorrent kill` can
	// still find this process if a slow close outlasts its graceful wait.
	// H3: hard ceiling on close. mgr.Close persists state before any network/teardown,
	// so if a hung tracker or stuck join blows past the deadline we force-exit safely.
	const shutdownForceDeadline = 2 * time.Second
	perfMarkf("close-begin")
	closeDone := make(chan struct{})
	go func() {
		mgr.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
		perfMarkf("exit")
		if benchMode {
			fmt.Printf("shutdown_ms=%.1f\n", msOf(time.Since(shutdownStart)))
		}
		perfReport(os.Stderr)
	case <-time.After(shutdownForceDeadline):
		perfMarkf("exit-forced")
		if benchMode {
			fmt.Printf("shutdown_ms=%.1f (forced)\n", msOf(time.Since(shutdownStart)))
		}
		perfReport(os.Stderr)
		removePID(ipcDir)
		os.Exit(exitCode)
	}
	if exitCode != 0 {
		removePID(ipcDir)
		logging.Close()
		os.Exit(exitCode)
	}
}

// setUpCrashOutput makes the runtime write fatal errors (panics no crash
// guard caught, in DHT, uTP, NAT and HTTP goroutines; concurrent map writes;
// running out of memory or stack) to <configDir>/crash/fatal.txt as well as
// to stderr, which the TUI's alternate screen hides, and returns that file.
// Call it only while holding the single-instance lock: it prunes and rotates
// that directory.
func setUpCrashOutput(configDir string) (*os.File, error) {
	f, err := downloader.OpenCrashLog(configDir)
	if err != nil {
		return nil, err
	}
	if err := debug.SetCrashOutput(f, debug.CrashOptions{}); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// redirectStdLog routes the standard library logger into the debug log. It
// must run before NAT and DHT start: dependencies such as goupnp log raw
// bytes from LAN replies through it, which would otherwise be written to the
// terminal under the TUI with any escape sequences intact.
func redirectStdLog() {
	log.SetFlags(0) // debug log lines carry their own timestamp
	log.SetOutput(logging.StdLogWriter())
}

var (
	headlessShutdownChan = make(chan struct{})
	shutdownOnce         sync.Once
	shutdownRequested    bool
	shutdownReqMu        sync.Mutex
)

func triggerShutdown() {
	shutdownReqMu.Lock()
	shutdownRequested = true
	shutdownReqMu.Unlock()

	shutdownOnce.Do(func() {
		close(headlessShutdownChan)
	})

	programMu.RLock()
	p := teaProgram
	programMu.RUnlock()
	if p != nil {
		go p.Quit()
	}
}

// setTeaProgram publishes p to the socket handlers and replays a shutdown that
// was requested over IPC before p existed.
func setTeaProgram(p *tea.Program) {
	programMu.Lock()
	teaProgram = p
	programMu.Unlock()

	shutdownReqMu.Lock()
	requested := shutdownRequested
	shutdownReqMu.Unlock()
	if requested {
		go p.Quit()
	}
}

func waitForShutdownSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	hangup := make(chan os.Signal, 1)
	notifyHangup(hangup)
	select {
	case <-sigCh:
	case <-hangup:
	case <-headlessShutdownChan:
	}
	signal.Stop(sigCh)
}

// notifyHangup relays SIGHUP to ch for the rest of the run and reports whether
// it does. Closing the terminal window hangs saintTorrent up, and Go's default
// for SIGHUP is to exit at once: the session state goes unsaved and the running
// sentinel stays behind, so the next start takes the close for a crash, and two
// in a row restore every torrent paused. The caller shuts down cleanly on the
// first SIGHUP; later ones land in the full channel and are dropped instead of
// killing the shutdown. A SIGHUP ignored at startup (nohup) stays ignored,
// which Notify would undo.
func notifyHangup(ch chan<- os.Signal) bool {
	if signal.Ignored(syscall.SIGHUP) {
		return false
	}
	signal.Notify(ch, syscall.SIGHUP)
	return true
}

// leadStartupWarnings puts the warning that the stats API is exposed, then the
// persistence warning, ahead of the other startup warnings: the TUI shows them
// on one line cut to its width. The persistence warning starts with crash
// containment, such as a torrent that was not loaded and how to load it, which
// nothing else on screen shows. Empty warnings are dropped.
func leadStartupWarnings(exposure, persistence string, rest []string) []string {
	var warns []string
	for _, w := range []string{exposure, persistence} {
		if w != "" {
			warns = append(warns, w)
		}
	}
	return append(warns, rest...)
}

// tuiStartupLine joins startup warnings and infos into the TUI's single
// startup line, so a TUI user also sees where the stats API is listening.
// Warnings come first because the line is cut to the terminal width. Headless
// mode prints them separately via writeHeadlessStartupMessages.
func tuiStartupLine(infos, warns []string) string {
	return strings.Join(append(append([]string(nil), warns...), infos...), "; ")
}

// writeHeadlessStartupMessages prints startup infos and warnings. Warnings can
// embed torrent-controlled text (failed-add errors quote file paths), so each
// line is escaped before it reaches the terminal.
func writeHeadlessStartupMessages(w io.Writer, infos []string, warns []string) {
	for _, info := range infos {
		fmt.Fprintln(w, escapeForTerminal(info))
	}
	for _, warn := range warns {
		fmt.Fprintf(w, "Warning: %s\n", escapeForTerminal(warn))
	}
}
