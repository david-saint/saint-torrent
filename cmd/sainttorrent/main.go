package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"sainttorrent/pkg/downloader"
)

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
)

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
	startupWarn      string
	sessions         []*downloader.Session
	deleteWithFiles  bool
	deleteErr        error
	deleteTargetName string
	deleteTargetHash string
}

func initialModel(mgr *downloader.TorrentManager, downloadDir string, startupWarn string) model {
	p := progress.New(progress.WithDefaultGradient())
	ti := textinput.New()
	ti.Width = 50

	return model{
		manager:     mgr,
		downloadDir: downloadDir,
		progress:    p,
		textInput:   ti,
		viewMode:    viewList,
		startupWarn: startupWarn,
		sessions:    mgr.ListSessions(),
	}
}

func (m model) Init() tea.Cmd {
	// Start all managed sessions
	for _, s := range m.sessions {
		s.Start()
	}
	return tea.Batch(tickCmd(), textinput.Blink)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch m.viewMode {
		case viewInput:
			switch msg.String() {
			case "esc":
				m.viewMode = viewList
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
					m.sessions = m.manager.ListSessions()
					m.viewMode = viewList
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
				m.manager.Close()
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
			}

		case viewDetail:
			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.manager.Close()
				return m, tea.Quit
			case "esc":
				m.viewMode = viewList
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
			case "x":
				m.viewMode = viewDeleteConfirm
				m.deleteWithFiles = false
				m.deleteErr = nil
				if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
					s := m.sessions[m.selectedIdx]
					m.deleteTargetName = s.Torrent.Name
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
					m.deleteTargetName = s.Torrent.Name
					m.deleteTargetHash = fmt.Sprintf("%x", s.Torrent.InfoHash)
				} else {
					m.deleteTargetName = ""
					m.deleteTargetHash = ""
				}
			}

		case viewFiles:
			if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
				m.viewMode = viewList
				return m, nil
			}
			s := m.sessions[m.selectedIdx]
			files := s.Files()

			switch msg.String() {
			case "q", "ctrl+c":
				m.quitting = true
				m.manager.Close()
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
				m.manager.Close()
				return m, tea.Quit
			case "esc", "n", "N":
				if m.deleteErr != nil {
					m.viewMode = viewList
					m.deleteErr = nil
				} else {
					m.viewMode = viewDetail
				}
			case "y", "Y":
				if m.deleteErr != nil {
					m.viewMode = viewList
					m.deleteErr = nil
					return m, nil
				}
				if m.deleteTargetHash != "" {
					err := m.manager.RemoveSession(m.deleteTargetHash, m.deleteWithFiles)
					if err != nil {
						m.deleteErr = err
						m.sessions = m.manager.ListSessions()
						if m.selectedIdx >= len(m.sessions) && len(m.sessions) > 0 {
							m.selectedIdx = len(m.sessions) - 1
						} else if len(m.sessions) == 0 {
							m.selectedIdx = 0
						}
						return m, nil
					}
				}
				m.sessions = m.manager.ListSessions()
				if m.selectedIdx >= len(m.sessions) && len(m.sessions) > 0 {
					m.selectedIdx = len(m.sessions) - 1
				} else if len(m.sessions) == 0 {
					m.selectedIdx = 0
				}
				m.viewMode = viewList
			}
		}

	case tickMsg:
		if m.quitting {
			return m, nil
		}
		m.sessions = m.manager.ListSessions()
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

			name := s.Torrent.Name
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
		sb.WriteString("  " + warnStyle.Render(m.startupWarn) + "\n\n")
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
	sb.WriteString(helpStyle.Render(fmt.Sprintf("  [enter] Details | [space] %s | [a] Add | [d] Down Limit | [u] Up Limit | [q] Quit", spaceActionHelp)) + "\n")
	return sb.String()
}

func (m model) viewTorrentDetails() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return ""
	}
	s := m.sessions[m.selectedIdx]

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Torrent Details:"), s.Torrent.Name))

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
	sb.WriteString(helpStyle.Render(fmt.Sprintf("  [esc] Back | [space] %s | [f] Files | [x] Delete Task | [X] Delete Task & Files | [q] Quit", spaceActionHelp)) + "\n")
	return sb.String()
}

func (m model) viewFileExplorer() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return ""
	}
	s := m.sessions[m.selectedIdx]
	files := s.Files()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("File Explorer:"), s.Torrent.Name))

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

			path := filepath.Join(f.Path...)
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
		sb.WriteString(fmt.Sprintf("  %s\n\n", errorStyle.Render(m.inputErr)))
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

func main() {
	downloadDir := "."
	configDir := ""
	persist := true
	var filesToAdd []string

	for i := 1; i < len(os.Args); i++ {
		arg := os.Args[i]
		if arg == "-d" || arg == "--dir" {
			if i+1 < len(os.Args) {
				downloadDir = os.Args[i+1]
				i++
			}
		} else if arg == "-c" || arg == "--config" {
			if i+1 < len(os.Args) {
				configDir = os.Args[i+1]
				i++
			}
		} else if arg == "--no-persist" {
			persist = false
		} else {
			filesToAdd = append(filesToAdd, arg)
		}
	}

	var startupWarns []string

	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		startupWarns = append(startupWarns, fmt.Sprintf("Failed to create download dir %s: %v", downloadDir, err))
	}

	mgr := downloader.NewTorrentManager()
	defer mgr.Close()

	if err := mgr.StartDHT(downloadDir, 6881); err != nil {
		startupWarns = append(startupWarns, fmt.Sprintf("DHT unavailable: %v", err))
	}

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

	for _, item := range filesToAdd {
		var sess *downloader.Session
		var err error
		if strings.HasPrefix(item, "magnet:?") {
			sess, err = mgr.AddMagnet(item, downloadDir)
		} else {
			sess, err = mgr.AddTorrentFile(item, downloadDir)
		}
		if err == nil {
			sess.Start()
		} else {
			startupWarns = append(startupWarns, fmt.Sprintf("Failed to add %s: %v", filepath.Base(item), err))
		}
	}

	startupWarn := ""
	if len(startupWarns) > 0 {
		startupWarn = strings.Join(startupWarns, "; ")
	}

	p := tea.NewProgram(initialModel(mgr, downloadDir, startupWarn))
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error running UI: %v\n", err)
	}
}

func (m model) viewDeleteConfirm() string {
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		if m.deleteErr != nil {
			var sb strings.Builder
			sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Deletion Failure:"), m.deleteTargetName))
			sb.WriteString(cardStyle.Render(errorStyle.Render(m.deleteErr.Error())))
			sb.WriteString("\n\n")
			sb.WriteString(helpStyle.Render("  [esc]/[n]/[y] Back to Dashboard") + "\n")
			return sb.String()
		}
		return ""
	}

	var sb strings.Builder
	if m.deleteErr != nil {
		sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Deletion Failure:"), m.deleteTargetName))
		sb.WriteString(cardStyle.Render(errorStyle.Render(m.deleteErr.Error())))
		sb.WriteString("\n\n")
		sb.WriteString(helpStyle.Render("  [esc]/[n]/[y] Back to Dashboard") + "\n")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("  %s %s\n\n", headerStyle.Render("Confirm Delete:"), m.deleteTargetName))

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
