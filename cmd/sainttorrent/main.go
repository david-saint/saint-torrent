package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"sainttorrent/pkg/downloader"
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

// Styles using Lipgloss
var (
	subtleStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#6272a4"))
	infoStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#50fa7b"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("#ffb86c"))
	errorStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#ff5555"))

	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#f8f8f2")).
			Background(lipgloss.Color("#bd93f9")).
			Padding(0, 1).
			MarginBottom(1)

	headerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00f0ff")).
			Bold(true)

	cardStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#6272a4")).
			Padding(1).
			Width(68).
			MarginBottom(1)

	peersCardStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#bd93f9")).
			Padding(1).
			Width(68).
			MarginBottom(1)

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#44475a")).
			Italic(true)

	statusPausedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#ffb86c")).
				Bold(true)

	statusDownloadingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#50fa7b")).
				Bold(true)

	statusSeedingStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#bd93f9")).
				Bold(true)

	statusMetadataStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#ff79c6")).
				Bold(true)

	statusErrorStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#ff5555")).
				Bold(true)

	selectedRowStyle = lipgloss.NewStyle().
				Background(lipgloss.Color("#44475a")).
				Foreground(lipgloss.Color("#50fa7b")).
				Bold(true)

	normalRowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#f8f8f2"))

	badgeStyle = lipgloss.NewStyle().
			Bold(true).
			Padding(0, 1)

	priorityNormalStyle = badgeStyle.Copy().
				Background(lipgloss.Color("#6272a4")).
				Foreground(lipgloss.Color("#f8f8f2"))

	priorityHighStyle = badgeStyle.Copy().
				Background(lipgloss.Color("#ff5555")).
				Foreground(lipgloss.Color("#f8f8f2"))

	prioritySkipStyle = badgeStyle.Copy().
				Background(lipgloss.Color("#44475a")).
				Foreground(lipgloss.Color("#6272a4"))
)

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
	quitting         bool
	inputErr         string
	flash            string
	startupWarn      string
	sessions         []*downloader.Session
	deleteWithFiles  bool
	deleteErr        error
	addConfirmErr    error
	deleteTargetName string
	deleteTargetHash string
	pendingItems     []pendingItem
	pendingIdx       int
}

func initialModel(mgr *downloader.TorrentManager, downloadDir string, startupWarn string, pending []pendingItem) model {
	p := progress.New(progress.WithDefaultGradient())
	ti := textinput.New()
	ti.Width = 50

	mode := viewList
	if len(pending) > 0 {
		mode = viewAddConfirm
	}

	return model{
		manager:      mgr,
		downloadDir:  downloadDir,
		progress:     p,
		textInput:    ti,
		viewMode:     mode,
		startupWarn:  startupWarn,
		sessions:     mgr.ListSessions(),
		pendingItems: pending,
		pendingIdx:   0,
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
				if m.selectedIdx > 0 {
					m.selectedIdx--
				}
			case "down", "j":
				if m.selectedIdx < len(m.sessions)-1 {
					m.selectedIdx++
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
			}

		case viewDetail:
			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.resolveRemainingPending(fmt.Errorf("client shutting down"))
				return m, tea.Quit
			case "esc":
				m.viewMode = viewList
				if m.pendingIdx < len(m.pendingItems) {
					m.viewMode = viewAddConfirm
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
				if m.selectedFileIdx > 0 {
					m.selectedFileIdx--
				}
			case "down", "j":
				if m.selectedFileIdx < len(files)-1 {
					m.selectedFileIdx++
				}
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
					err := m.manager.RemoveSession(m.deleteTargetHash, m.deleteWithFiles)
					if err != nil {
						m.deleteErr = err
						m.refreshSessions()
						return m, nil
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

	case tickMsg:
		if m.quitting {
			return m, nil
		}
		m.refreshSessions()
		return m, tickCmd()

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

	var sb strings.Builder
	sb.WriteString(titleStyle.Render(" saintTorrent CLI v0.2 "))
	sb.WriteString("\n")

	switch m.viewMode {
	case viewList:
		sb.WriteString(m.viewTorrentList())
	case viewDetail:
		sb.WriteString(m.viewTorrentDetails())
	case viewFiles:
		sb.WriteString(m.viewFileExplorer())
	case viewInput:
		sb.WriteString(m.viewInputBox())
	case viewDeleteConfirm:
		sb.WriteString(m.viewDeleteConfirm())
	case viewAddConfirm:
		sb.WriteString(m.viewAddConfirm())
	}

	return sb.String()
}

func (m model) viewTorrentList() string {
	var sb strings.Builder

	if len(m.sessions) == 0 {
		sb.WriteString("\n  No active torrents. Press 'a' to add a torrent or magnet link.\n\n")
	} else {
		sb.WriteString(fmt.Sprintf("  %-3s %-25s %-9s %-12s %-11s %-15s\n", "ACT", "NAME", "SIZE", "PROGRESS", "STATUS", "SPEED"))
		sb.WriteString(subtleStyle.Render("  "+strings.Repeat("─", 78)) + "\n")

		for i, s := range m.sessions {
			indicator := getIndicator(s.IsPaused(), s.IsCompleted())

			name := sanitizeText(s.Name())
			if len(name) > 23 {
				name = name[:20] + "..."
			}

			sizeStr := formatBytes(s.TotalSize())
			if s.IsMetadataMode() {
				sizeStr = "unknown"
			}

			pct := s.PercentComplete()
			pctStr := fmt.Sprintf("%.1f%%", pct)
			if s.IsMetadataMode() {
				pctStr = "0.0%"
			}

			status := s.Status()
			var statusBadge string
			switch status {
			case "Paused":
				statusBadge = statusPausedStyle.Render("PAUSED")
			case "Stopped":
				statusBadge = statusPausedStyle.Render("STOPPED")
			case "Seeding":
				statusBadge = statusSeedingStyle.Render("SEEDING")
			case "Metadata":
				statusBadge = statusMetadataStyle.Render("METADATA")
			case "Error":
				statusBadge = statusErrorStyle.Render("ERROR")
			case "Checking":
				statusBadge = statusMetadataStyle.Render("CHECKING")
			default:
				statusBadge = statusDownloadingStyle.Render("DOWNLOADING")
			}

			speedStr := getSpeedStr(s.IsPaused(), s.IsCompleted(), s.CurrentSpeed())

			rowContent := fmt.Sprintf("  %-3s %-25s %-9s %-12s %-11s %-15s",
				indicator, name, sizeStr, pctStr, statusBadge, speedStr)

			if i == m.selectedIdx {
				sb.WriteString(selectedRowStyle.Render(rowContent) + "\n")
			} else {
				sb.WriteString(normalRowStyle.Render(rowContent) + "\n")
			}
		}
		sb.WriteString("\n")
	}
	if m.startupWarn != "" {
		sb.WriteString("  " + warnStyle.Render(sanitizeText(m.startupWarn)) + "\n\n")
	}

	dhtNodes := 0
	if d := m.manager.DHT(); d != nil {
		dhtNodes = d.NodesCount()
	}

	var totalSpeed float64
	for _, s := range m.sessions {
		totalSpeed += s.CurrentSpeed()
	}

	downLimit := m.manager.GlobalDownloadLimit()
	upLimit := m.manager.GlobalUploadLimit()

	downLimitStr := "Unlimited"
	if downLimit > 0 {
		downLimitStr = formatSpeed(float64(downLimit))
	}
	upLimitStr := "Unlimited"
	if upLimit > 0 {
		upLimitStr = formatSpeed(float64(upLimit))
	}

	sb.WriteString(subtleStyle.Render("  "+strings.Repeat("─", 78)) + "\n")
	sb.WriteString(fmt.Sprintf(
		"  DHT: %s nodes | Speed: %s/s | Limits: ↓ %s / ↑ %s\n\n",
		infoStyle.Render(strconv.Itoa(dhtNodes)),
		infoStyle.Render(formatSpeed(totalSpeed)),
		warnStyle.Render(downLimitStr),
		warnStyle.Render(upLimitStr),
	))

	spaceActionHelp := "Pause/Resume"
	if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
		s := m.sessions[m.selectedIdx]
		spaceActionHelp = getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	}
	if m.flash != "" {
		sb.WriteString("  " + warnStyle.Render(sanitizeText(m.flash)) + "\n")
	}
	sb.WriteString(helpStyle.Render(fmt.Sprintf("  [enter] Details | [space] %s | [o] Open Folder | [a] Add | [d] Down Limit | [u] Up Limit | [q] Quit", spaceActionHelp)) + "\n")
	return sb.String()
}

func (m model) viewTorrentDetails() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return ""
	}
	s := m.sessions[m.selectedIdx]

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Torrent Details:"), sanitizeText(s.Name())))

	pct := s.PercentComplete() / 100.0
	status := s.Status()

	var statusBadge string
	switch status {
	case "Paused":
		statusBadge = statusPausedStyle.Render("PAUSED")
	case "Stopped":
		statusBadge = statusPausedStyle.Render("STOPPED")
	case "Seeding":
		statusBadge = statusSeedingStyle.Render("SEEDING")
	case "Metadata":
		statusBadge = statusMetadataStyle.Render("METADATA")
	case "Error":
		statusBadge = statusErrorStyle.Render("ERROR")
	case "Checking":
		statusBadge = statusMetadataStyle.Render("CHECKING")
	default:
		statusBadge = statusDownloadingStyle.Render("DOWNLOADING")
	}

	cardContent := fmt.Sprintf(
		"%s: %s\n%s: %s\n%s: %.2f%%\n%s: %s\n\n%s",
		headerStyle.Render("Hash"), fmt.Sprintf("%x", s.Torrent.InfoHash),
		headerStyle.Render("Total Size"), formatBytes(s.TotalSize()),
		headerStyle.Render("Complete"), pct*100,
		headerStyle.Render("Status"), statusBadge,
		m.progress.ViewAs(pct),
	)
	if err := s.LastError(); err != nil {
		cardContent += "\n" + headerStyle.Render("Last Issue") + ": " + sanitizeText(err.Error())
	}
	sb.WriteString(cardStyle.Render(cardContent))
	sb.WriteString("\n")

	var peersContent strings.Builder
	peersContent.WriteString(headerStyle.Render("Connected Peers:") + "\n")
	peers := s.GetActivePeers()
	if len(peers) == 0 {
		if s.IsPaused() {
			if s.IsCompleted() {
				peersContent.WriteString(subtleStyle.Render("  Session is stopped.") + "\n")
			} else {
				peersContent.WriteString(subtleStyle.Render("  Session is paused.") + "\n")
			}
		} else {
			peersContent.WriteString(subtleStyle.Render("  No connected peers. Searching via DHT/Tracker...") + "\n")
		}
	} else {
		for _, p := range peers {
			chokeStr := "Unchoked"
			if p.Choked {
				chokeStr = "Choked"
			}
			peersContent.WriteString(fmt.Sprintf(
				"  - %s:%-5d | %-8s | Speed: %s/s\n",
				p.IP, p.Port, chokeStr, formatSpeed(p.DownloadSpeed),
			))
		}
	}
	sb.WriteString(peersCardStyle.Render(peersContent.String()))
	sb.WriteString("\n")

	if !s.IsMetadataMode() {
		sb.WriteString("  " + headerStyle.Render("Pieces Visual Map:") + "\n")
		var pieceMapStr strings.Builder
		pieceMapStr.WriteString("  ")
		pieceMap := s.GetPieceStates()
		for i, state := range pieceMap {
			if i > 0 && i%30 == 0 {
				pieceMapStr.WriteString("\n  ")
			}
			switch state {
			case downloader.PieceCompleted:
				pieceMapStr.WriteString(infoStyle.Render("█"))
			case downloader.PieceDownloading:
				pieceMapStr.WriteString(warnStyle.Render("░"))
			default:
				pieceMapStr.WriteString(subtleStyle.Render("."))
			}
		}
		sb.WriteString(pieceMapStr.String())
		sb.WriteString("\n\n")
	}

	spaceActionHelp := getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	if m.flash != "" {
		sb.WriteString("  " + warnStyle.Render(sanitizeText(m.flash)) + "\n")
	}
	sb.WriteString(helpStyle.Render(fmt.Sprintf("  [esc] Back | [space] %s | [f] Files | [o] Open Folder | [x] Delete Task | [X] Delete Task & Files | [q] Quit", spaceActionHelp)) + "\n")
	return sb.String()
}

func (m model) viewFileExplorer() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return ""
	}
	s := m.sessions[m.selectedIdx]
	files := s.Files()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("File Explorer:"), sanitizeText(s.Name())))

	if len(files) == 0 {
		sb.WriteString("  No files in metadata.\n\n")
	} else {
		priorities := s.GetFilePriorities()
		for i, f := range files {
			prio := downloader.PriorityNormal
			if i < len(priorities) {
				prio = priorities[i]
			}

			var prioBadge string
			switch prio {
			case downloader.PriorityHigh:
				prioBadge = priorityHighStyle.Render(" HIGH ")
			case downloader.PrioritySkip:
				prioBadge = prioritySkipStyle.Render(" SKIP ")
			default:
				prioBadge = priorityNormalStyle.Render("NORMAL")
			}

			path := sanitizeText(filepath.Join(f.Path...))
			if len(path) > 40 {
				path = "..." + path[len(path)-37:]
			}

			rowContent := fmt.Sprintf("  %-40s %-10s %s", path, formatBytes(f.Length), prioBadge)
			if i == m.selectedFileIdx {
				sb.WriteString(selectedRowStyle.Render(rowContent) + "\n")
			} else {
				sb.WriteString(normalRowStyle.Render(rowContent) + "\n")
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString(helpStyle.Render("  [esc] Back to Details | [space]/[p] Toggle Priority (Normal -> High -> Skip) | [q] Quit") + "\n")
	return sb.String()
}

func (m model) viewInputBox() string {
	var sb strings.Builder
	var title string

	switch m.inputMode {
	case inputAddTorrent:
		title = "Add Torrent / Magnet Link"
	case inputLimitDownload:
		title = "Set Global Download Limit (KB/s)"
	case inputLimitUpload:
		title = "Set Global Upload Limit (KB/s)"
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s\n", headerStyle.Render(title)))
	sb.WriteString(subtleStyle.Render("  "+strings.Repeat("─", 60)) + "\n\n")
	sb.WriteString("  " + m.textInput.View() + "\n\n")

	if m.inputErr != "" {
		sb.WriteString(fmt.Sprintf("  %s\n\n", errorStyle.Render(sanitizeText(m.inputErr))))
	}

	sb.WriteString(helpStyle.Render("  [enter] Confirm | [esc] Cancel") + "\n")
	return sb.String()
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

func generatePeerID() [20]byte {
	var id [20]byte
	copy(id[:8], "-ST0001-")
	_, _ = io.ReadFull(rand.Reader, id[8:])
	return id
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

func acquireLock(lockPath string) (*os.File, error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		file.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, errLockContention
		}
		return nil, fmt.Errorf("failed to flock file: %w", err)
	}
	return file, nil
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

func handleSocketConnection(conn net.Conn, shutdownChan chan struct{}, mgr *downloader.TorrentManager, handlersWG *sync.WaitGroup, terminal terminalIdentity) {
	defer handlersWG.Done()
	defer unregisterConn(conn)
	defer conn.Close()

	programMu.RLock()
	p := teaProgram
	programMu.RUnlock()

	if p == nil {
		sendResponse(conn, "starting", "saintTorrent is starting up", terminal)
		return
	}

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

func detectTerminalTTY(input *os.File) string {
	return findTerminalTTY(input, []string{"/dev/ttys*", "/dev/pts/*"})
}

func findTerminalTTY(input *os.File, patterns []string) string {
	info, err := input.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return ""
	}
	inputStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}

	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, path := range paths {
			ttyInfo, err := os.Stat(path)
			if err != nil {
				continue
			}
			ttyStat, ok := ttyInfo.Sys().(*syscall.Stat_t)
			if ok && ttyStat.Rdev == inputStat.Rdev {
				return path
			}
		}
	}

	return ""
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

func (m model) viewAddConfirm() string {
	if m.pendingIdx >= len(m.pendingItems) {
		return ""
	}
	item := m.pendingItems[m.pendingIdx]

	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("  %s\n", headerStyle.Render("Confirm Add Torrent")))
	sb.WriteString(subtleStyle.Render("  "+strings.Repeat("─", 60)) + "\n\n")

	if m.addConfirmErr != nil {
		sb.WriteString(fmt.Sprintf("  %s: %s\n\n", errorStyle.Render("Error adding torrent"), sanitizeText(m.addConfirmErr.Error())))
		sb.WriteString(helpStyle.Render("  [esc]/[n]/[y] Dismiss and continue") + "\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("  %s: %s\n", headerStyle.Render("Torrent Name"), item.displayName))
	sb.WriteString(fmt.Sprintf("  %s: %s\n\n", headerStyle.Render("Download Dir"), sanitizeText(item.downloadDir)))

	if item.isDuplicate {
		sb.WriteString(fmt.Sprintf("  %s\n\n", errorStyle.Render("Warning: This torrent is already in the download list. Confirming will resume it.")))
	}

	sb.WriteString(helpStyle.Render("  [y] Yes, Confirm Download | [n] No, Skip | [q] Quit") + "\n")
	return sb.String()
}

func (m model) viewDeleteConfirm() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		if m.deleteErr != nil {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Deletion Failure:"), sanitizeText(m.deleteTargetName)))
			sb.WriteString(cardStyle.Render(errorStyle.Render(sanitizeText(m.deleteErr.Error()))))
			sb.WriteString("\n\n")
			sb.WriteString(helpStyle.Render("  [esc]/[n]/[y] Back to Dashboard") + "\n")
			return sb.String()
		}
		return ""
	}

	var sb strings.Builder
	if m.deleteErr != nil {
		sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Deletion Failure:"), sanitizeText(m.deleteTargetName)))
		sb.WriteString(cardStyle.Render(errorStyle.Render(sanitizeText(m.deleteErr.Error()))))
		sb.WriteString("\n\n")
		sb.WriteString(helpStyle.Render("  [esc]/[n]/[y] Back to Dashboard") + "\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Confirm Delete:"), sanitizeText(m.deleteTargetName)))

	var warnMsg string
	if m.deleteWithFiles {
		warnMsg = errorStyle.Render("WARNING: This will permanently delete the task, state, and ALL downloaded files from disk!")
	} else {
		warnMsg = warnStyle.Render("This will delete the task state and fast-resume file, but keep downloaded files on disk.")
	}

	cardContent := fmt.Sprintf(
		"Are you sure you want to delete this torrent?\n\n%s",
		warnMsg,
	)
	sb.WriteString(cardStyle.Render(cardContent))
	sb.WriteString("\n\n")
	sb.WriteString(helpStyle.Render("  [y] Yes, Confirm Delete | [n]/[esc] Cancel") + "\n")
	return sb.String()
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
	perfMarkf("manager")

	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error removing stale socket file: %v\n", err)
		os.Exit(1)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error starting socket listener: %v\n", err)
		os.Exit(1)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
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
			go handleSocketConnection(conn, shutdownChan, mgr, &handlersWG, terminal)
		}
	}()

	if err := mgr.StartDHT(downloadDir, 6881); err != nil {
		startupWarns = append(startupWarns, fmt.Sprintf("DHT unavailable: %v", err))
	}
	perfMarkf("dht")

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

	var initialPending []pendingItem
	for _, item := range filesToAdd {
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

	p := tea.NewProgram(initialModel(mgr, downloadDir, startupWarn, initialPending))
	perfMarkf("tui-build")

	programMu.Lock()
	teaProgram = p
	programMu.Unlock()

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
	} else {
		if _, err := p.Run(); err != nil {
			fmt.Printf("Error running UI: %v\n", err)
		}
		perfMarkf("quit")
	}

	shutdownStart := time.Now()
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
