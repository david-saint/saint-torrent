// Package main implements the CLI and TUI entry point for the saintTorrent client.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
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
	"sainttorrent/pkg/mse"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

var (
	errLockContention = errors.New("lock contention")

	programMu  sync.RWMutex
	teaProgram *tea.Program
)

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
		if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
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
	rawURL      string
	displayName string
	infoHashHex string
	downloadDir string
	isDuplicate bool
	respChan    chan addTorrentResponse
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
	Items       []string `json:"items"`
	Confirm     bool     `json:"confirm"`
	DownloadDir string   `json:"download_dir"`
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
	downloadDir string
	configDir   string
	persist     bool
	confirm     bool
	headless    bool
	theme       string
	listenPort  int
	httpAddr    string
	natEnabled  bool
	encryption  mse.Policy
	storage     storage.Backend
	err         error
	items       []string
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
}

// speedHistoryLen bounds the per-torrent speed ring (samples at the tick rate).
const speedHistoryLen = 60

// recordSpeeds appends the current download speed for each session to its ring
// (bounded) and prunes rings for sessions that are gone.
func (m *model) recordSpeeds() {
	if m.speedHistory == nil {
		m.speedHistory = make(map[string][]float64)
	}
	live := make(map[string]struct{}, len(m.sessions))
	for _, s := range m.sessions {
		if s.Torrent == nil {
			continue
		}
		key := fmt.Sprintf("%x", s.Torrent.InfoHash)
		live[key] = struct{}{}
		ring := append(m.speedHistory[key], currentTransferSpeed(s))
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

func (m *model) detailMaxScroll() int {
	if m.viewMode != viewDetail || m.height <= 0 {
		return 0
	}
	rendered := clampLines(m.theme.renderDetails(m), outerWidth(m.width))
	return maxVerticalOffset(rendered, m.height)
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

func (m *model) moveFileSelection(delta int) {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return
	}
	files := m.sessions[m.selectedIdx].Files()
	if len(files) == 0 {
		return
	}
	m.selectedFileIdx = clamp(m.selectedFileIdx+delta, 0, len(files)-1)
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

	return model{
		manager:        mgr,
		downloadDir:    downloadDir,
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
}

func (m *model) refreshSessions() {
	var selectedHash string
	if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
		s := m.sessions[m.selectedIdx]
		if s.Torrent != nil {
			selectedHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
		}
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
}

// openSelectedLocation reveals the currently selected torrent's content in the
// OS file manager (Finder on macOS). It records a flash message when the
// location is not yet known (e.g. a magnet still fetching metadata) or the file
// manager could not be launched.
func (m *model) openSelectedLocation() {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return
	}
	s := m.sessions[m.selectedIdx]
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
	return tea.Batch(tickCmd(), animCmd(), textinput.Blink, tea.SetWindowTitle(terminalWindowTitle))
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
				m.viewMode = viewList
				if m.pendingIdx < len(m.pendingItems) {
					m.viewMode = viewAddConfirm
				}
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
					var sess *downloader.Session
					var err error
					if strings.HasPrefix(val, "magnet:?") {
						sess, err = m.manager.AddMagnet(val, m.downloadDir)
					} else {
						sess, err = m.manager.AddTorrentFile(val, m.downloadDir)
					}

					if err != nil {
						m.inputErr = fmt.Sprintf("Failed to load torrent: %v", err)
						return m, nil
					}
					sess.Start()
					m.refreshSessions()
					m.viewMode = viewList
					if m.pendingIdx < len(m.pendingItems) {
						m.viewMode = viewAddConfirm
					}
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
					m.viewMode = viewList
					if m.pendingIdx < len(m.pendingItems) {
						m.viewMode = viewAddConfirm
					}
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
					m.viewMode = viewList
					if m.pendingIdx < len(m.pendingItems) {
						m.viewMode = viewAddConfirm
					}
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
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					if s.IsPaused() {
						s.Resume()
					} else {
						s.Pause()
					}
				}
			case "enter":
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					m.viewMode = viewDetail
					m.detailScroll = 0
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
				m.viewMode = viewList
				m.detailScroll = 0
				if m.pendingIdx < len(m.pendingItems) {
					m.viewMode = viewAddConfirm
				}
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
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					if s.IsPaused() {
						s.Resume()
					} else {
						s.Pause()
					}
				}
			case "f":
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					if !s.IsMetadataMode() {
						m.viewMode = viewFiles
						m.selectedFileIdx = 0
					}
				}
			case "o":
				m.openSelectedLocation()
			case "x":
				m.viewMode = viewDeleteConfirm
				m.deleteWithFiles = false
				m.deleteErr = nil
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					m.deleteTargetName = sanitizeText(s.Name())
					m.deleteTargetHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
				} else {
					m.deleteTargetName = ""
					m.deleteTargetHash = ""
				}
			case "X":
				m.viewMode = viewDeleteConfirm
				m.deleteWithFiles = true
				m.deleteErr = nil
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					m.deleteTargetName = sanitizeText(s.Name())
					m.deleteTargetHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
				} else {
					m.deleteTargetName = ""
					m.deleteTargetHash = ""
				}
			case "t":
				m.cycleTheme()
			}

		case viewFiles:
			if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
				m.viewMode = viewList
				if m.pendingIdx < len(m.pendingItems) {
					m.viewMode = viewAddConfirm
				}
				return m, nil
			}
			s := m.sessions[m.selectedIdx]
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
					m.viewMode = viewList
					if m.pendingIdx < len(m.pendingItems) {
						m.viewMode = viewAddConfirm
					}
					m.deleteErr = nil
				} else {
					m.viewMode = viewDetail
				}
			case "y", "Y":
				if m.deleteErr != nil {
					m.viewMode = viewList
					if m.pendingIdx < len(m.pendingItems) {
						m.viewMode = viewAddConfirm
					}
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
				m.viewMode = viewList
				if m.pendingIdx < len(m.pendingItems) {
					m.viewMode = viewAddConfirm
				}
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
						var sess *downloader.Session
						if strings.HasPrefix(item.rawURL, "magnet:?") {
							sess, addErr = m.manager.AddMagnet(item.rawURL, item.downloadDir)
						} else {
							sess, addErr = m.manager.AddTorrentFile(item.rawURL, item.downloadDir)
						}
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
			pDir := m.downloadDir
			if msg.msg.DownloadDir != "" {
				pDir = msg.msg.DownloadDir
			}
			for _, item := range msg.msg.Items {
				var sess *downloader.Session
				var err error
				if strings.HasPrefix(item, "magnet:?") {
					sess, err = m.manager.AddMagnet(item, pDir)
				} else {
					sess, err = m.manager.AddTorrentFile(item, pDir)
				}
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
		pDir := m.downloadDir
		if msg.msg.DownloadDir != "" {
			pDir = msg.msg.DownloadDir
		}
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
			displayName = sanitizeText(displayName)

			pItem := pendingItem{
				rawURL:      item,
				displayName: displayName,
				infoHashHex: hashHex,
				downloadDir: pDir,
				isDuplicate: isDuplicate,
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
		if msg.err != nil {
			m.deleteErr = msg.err
			m.viewMode = viewDeleteConfirm
			return m, nil
		}
		m.viewMode = viewList
		if m.pendingIdx < len(m.pendingItems) {
			m.viewMode = viewAddConfirm
		}
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
		return m, tickCmd()

	case animMsg:
		// Pure re-render to advance time-based animations; no data refresh.
		if m.quitting {
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

	st := m.theme.styles
	var out string
	switch m.viewMode {
	case viewList:
		// list/details own their full screen (incl. theme-specific banner).
		out = m.theme.renderList(&m)
	case viewDetail:
		out = m.theme.renderDetails(&m)
	default:
		// secondary screens share a layout under a themed banner.
		var sb strings.Builder
		sb.WriteString(st.Title.Render(" saintTorrent CLI v0.2 ") + "\n")
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
	case viewList:
		// Keep the header + list (incl. the selected torrent) and let the help
		// block clip from the bottom when the terminal is too short for all of it.
		out = verticalSlice(out, 0, m.height)
	}
	return out
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
	BinaryPath         string `json:"binaryPath"`
	SocketPath         string `json:"socketPath"`
	DefaultDownloadDir string `json:"defaultDownloadDir"`
	TerminalApp        string `json:"terminalApp"`
}

func sanitizeText(s string) string {
	var sb strings.Builder
	for _, r := range s {
		if r < 32 || r == 127 || (r >= 0x80 && r <= 0x9F) {
			sb.WriteRune(' ')
		} else {
			sb.WriteRune(r)
		}
	}
	res := sb.String()
	for strings.Contains(res, "  ") {
		res = strings.ReplaceAll(res, "  ", " ")
	}
	return strings.TrimSpace(res)
}

func parseItem(item string) (name string, hashHex string, err error) {
	if strings.HasPrefix(item, "magnet:?") {
		mag, err := torrent.ParseMagnet(item)
		if err != nil {
			return "", "", err
		}
		return mag.Name, fmt.Sprintf("%x", mag.InfoHash), nil
	}
	data, err := os.ReadFile(item)
	if err != nil {
		return "", "", err
	}
	tor, err := torrent.Parse(data)
	if err != nil {
		return "", "", err
	}
	return tor.Name, fmt.Sprintf("%x", tor.InfoHash), nil
}

func normalizeForwardedItems(items []string) []string {
	normalized := make([]string, 0, len(items))
	for _, item := range items {
		if strings.HasPrefix(item, "magnet:?") {
			normalized = append(normalized, item)
			continue
		}
		absPath, err := filepath.Abs(item)
		if err != nil {
			normalized = append(normalized, item)
			continue
		}
		normalized = append(normalized, absPath)
	}
	return normalized
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
				i++
			}
		case "-c", "--config":
			if i+1 < len(args) {
				opts.configDir = args[i+1]
				i++
			}
		case "--no-persist":
			opts.persist = false
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

func handleSocketConnection(conn net.Conn, shutdownChan chan struct{}, mgr *downloader.TorrentManager, handlersWG *sync.WaitGroup, terminal terminalIdentity, headless bool) {
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

	programMu.RLock()
	p := teaProgram
	programMu.RUnlock()

	if p == nil {
		if !headless {
			sendResponse(conn, "starting", "saintTorrent is starting up", terminal)
			return
		}
		if err := handleHeadlessSocketMessage(msg, mgr); err != nil {
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

func handleHeadlessSocketMessage(msg socketMessage, mgr *downloader.TorrentManager) error {
	if msg.Confirm {
		return fmt.Errorf("confirmation is unavailable in headless mode; retry with --no-confirm")
	}

	downloadDir := msg.DownloadDir
	if downloadDir == "" {
		downloadDir = "."
	}

	var errs []error
	for _, item := range msg.Items {
		var sess *downloader.Session
		var err error
		if strings.HasPrefix(item, "magnet:?") {
			sess, err = mgr.AddMagnet(item, downloadDir)
		} else {
			sess, err = mgr.AddTorrentFile(item, downloadDir)
		}
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
				BinaryPath:         execPath,
				SocketPath:         socketPath,
				DefaultDownloadDir: filepath.Join(home, "Downloads"),
				TerminalApp:        "Terminal",
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
	if opts.err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", opts.err)
		os.Exit(2)
	}
	downloadDir := opts.downloadDir
	configDir := opts.configDir
	persist := opts.persist
	confirmFlag := opts.confirm
	filesToAdd := opts.items

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

		normalizedItems := normalizeForwardedItems(filesToAdd)

		var absDownloadDir string
		if downloadDir != "" {
			var err error
			absDownloadDir, err = filepath.Abs(downloadDir)
			if err != nil {
				absDownloadDir = downloadDir
			}
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
				Items:       normalizedItems,
				Confirm:     confirmFlag,
				DownloadDir: absDownloadDir,
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
				fmt.Fprintf(os.Stderr, "Error from running instance: %s\n", resp.Message)
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

	var startupWarns []string

	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		startupWarns = append(startupWarns, fmt.Sprintf("Failed to create download dir %s: %v", downloadDir, err))
	}

	mgr := downloader.NewTorrentManager()
	mgr.SetEncryptionPolicy(opts.encryption)
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
			go handleSocketConnection(conn, shutdownChan, mgr, &handlersWG, terminal, opts.headless)
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
		statsServer, err = httpapi.Start(opts.httpAddr, mgr)
		if err != nil {
			listener.Close()
			acceptLoopWG.Wait()
			close(shutdownChan)
			closeActiveConns()
			handlersWG.Wait()
			mgr.Close()
			fmt.Fprintf(os.Stderr, "Error starting HTTP stats endpoint on %s: %v\n", opts.httpAddr, err)
			os.Exit(1)
		}
		startupWarns = append(startupWarns, fmt.Sprintf("HTTP stats endpoint: http://%s/stats", statsServer.Addr()))
	}
	perfMarkf("http-stats")

	if persist {
		if configDir == "" {
			userConfig, err := os.UserConfigDir()
			if err == nil {
				configDir = filepath.Join(userConfig, "sainttorrent")
			} else {
				configDir = ".sainttorrent"
			}
		}
		warning, err := mgr.EnablePersistence(configDir)
		if err != nil {
			startupWarns = append(startupWarns, fmt.Sprintf("Failed to initialize persistence: %v", err))
		} else if warning != "" {
			startupWarns = append(startupWarns, warning)
		}
	}
	perfMarkf("persistence")

	// Theme: --theme flag overrides persisted preference overrides default.
	selectedTheme := resolveInitialTheme(opts.theme, persist, configDir, &startupWarns)

	var initialPending []pendingItem
	for _, item := range filesToAdd {
		if opts.headless {
			var sess *downloader.Session
			var err error
			if strings.HasPrefix(item, "magnet:?") {
				sess, err = mgr.AddMagnet(item, downloadDir)
			} else {
				sess, err = mgr.AddTorrentFile(item, downloadDir)
			}
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
		displayName = sanitizeText(displayName)
		initialPending = append(initialPending, pendingItem{
			rawURL:      item,
			displayName: displayName,
			infoHashHex: hashHex,
			downloadDir: downloadDir,
			isDuplicate: isDuplicate,
		})
	}

	startupWarn := ""
	if len(startupWarns) > 0 {
		startupWarn = strings.Join(startupWarns, "; ")
	}

	var p *tea.Program
	if !opts.headless {
		startModel := initialModel(mgr, downloadDir, startupWarn, initialPending)
		startModel.theme = selectedTheme
		startModel.configDir = configDir
		startModel.persistEnabled = persist
		// This is a full-screen TUI. The alternate screen prevents terminal
		// scrollback reflow during resize from leaving stale copies of prior frames.
		p = newTUIProgram(startModel)
		perfMarkf("tui-build")

		programMu.Lock()
		teaProgram = p
		programMu.Unlock()
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
		if statsServer != nil {
			fmt.Fprintf(os.Stderr, "HTTP stats endpoint: http://%s/stats\n", statsServer.Addr())
		}
		for _, warn := range startupWarns {
			fmt.Fprintf(os.Stderr, "Warning: %s\n", warn)
		}
		waitForShutdownSignal()
	} else {
		if _, err := p.Run(); err != nil {
			fmt.Printf("Error running UI: %v\n", err)
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
		os.Exit(0)
	}
}

func waitForShutdownSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	signal.Stop(sigCh)
}
