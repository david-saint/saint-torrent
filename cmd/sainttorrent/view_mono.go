package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"sainttorrent/pkg/downloader"
)

// view_mono.go renders the "Mono Hairline" look (ported from
// spikes/03a-mono-hairline): a brightness-driven monochrome layout with thin
// line meters, a wordmark header, and a braille throughput sparkline. It owns
// its top banner and is width-flexible.

var brailleRamp = []rune{' ', '⡀', '⣀', '⣄', '⣆', '⣇', '⣧', '⣷', '⣿'}

func monoStatusIcon(st styles, s *downloader.Session) (string, lipgloss.Style) {
	switch s.Status() {
	case "Seeding":
		return "▲", st.Dim
	case "Metadata":
		return "◌", st.Faint
	case "Paused", "Stopped":
		return "‖", st.Faint
	case "Error":
		return "✕", st.Emphasis
	default:
		return "▼", st.Dim
	}
}

// sessionTotals sums instantaneous transfer rates across all sessions.
func sessionTotals(sessions []*downloader.Session) (down, up float64) {
	for _, s := range sessions {
		down += s.CurrentSpeed()
		up += s.CurrentUploadSpeed()
	}
	return down, up
}

func renderListMono(m *model) string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)
	col := listColumns(bw)

	count := fmt.Sprintf("%d torrents", len(m.sessions))
	if len(m.sessions) == 1 {
		count = "1 torrent"
	}
	totalDown, totalUp := sessionTotals(m.sessions)
	left := st.Bold.Render("saintTorrent") + "   " + st.Dim.Render(count)
	right := st.Dim.Render("▲ "+formatSpeed(totalUp)) + "  " + st.AccentBold.Render("↓ "+formatSpeed(totalDown))
	prefix := g + spread(left, right, bw) + "\n"
	prefix += dividerLine(st, m.width) + "\n"

	if len(m.sessions) == 0 {
		prefix += "\n" + g + st.Muted.Render("No active torrents. Press [a] to add a torrent or magnet link.") + "\n\n"
	} else {
		prefix += g + st.Dim.Render(monoHeaderRow(col)) + "\n"
		prefix += dividerLine(st, m.width) + "\n"
	}

	dhtNodes := 0
	if d := m.manager.DHT(); d != nil {
		dhtNodes = d.NodesCount()
	}
	downLimitStr := "Unlimited"
	if v := m.manager.GlobalDownloadLimit(); v > 0 {
		downLimitStr = formatSpeed(float64(v))
	}
	upLimitStr := "Unlimited"
	if v := m.manager.GlobalUploadLimit(); v > 0 {
		upLimitStr = formatSpeed(float64(v))
	}
	// head holds the always-shown footer rows; help is the clippable key list.
	var head strings.Builder
	head.WriteString(dividerLine(st, m.width) + "\n")
	dot := st.Hairline.Render("·")
	footer := st.Dim.Render("DHT") + " " + st.Muted.Render(strconv.Itoa(dhtNodes)+" nodes") + "   " + dot + "   " +
		st.Dim.Render("Total") + " " + st.AccentBold.Render("↓ "+formatSpeed(totalDown)) + " " + st.Muted.Render("↑ "+formatSpeed(totalUp)) + "   " + dot + "   " +
		st.Dim.Render("Limits") + " " + st.Muted.Render("↓ "+downLimitStr+" / ↑ "+upLimitStr)
	head.WriteString(g + footer + "\n\n")

	if m.startupWarn != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.startupWarn), bw)) + "\n\n")
	}

	spaceActionHelp := "Pause/Resume"
	if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
		s := m.sessions[m.selectedIdx]
		spaceActionHelp = getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	}
	if m.flash != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	help := renderHelpColumns([][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", spaceActionHelp}, {"o", "Open"}, {"a", "Add"},
		{"d", "Down"}, {"u", "Up"}, {"x", "Delete"}, {"X", "Delete+Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, helpRowsPerColumn, st, m.width)

	var sb strings.Builder
	sb.WriteString(prefix)
	if len(m.sessions) > 0 {
		capacity := dashboardCapacity(m.height, prefix, head.String(), help, 3, len(m.sessions))
		start, end := visibleSessionRange(len(m.sessions), m.selectedIdx, capacity)
		for i := start; i < end; i++ {
			sb.WriteString(monoRow(m, st, col, i, m.sessions[i]))
		}
	}
	sb.WriteString(head.String())
	sb.WriteString(help + "\n")
	return sb.String()
}

func monoHeaderRow(col listLayout) string {
	var b strings.Builder
	if col.showAct {
		b.WriteString(padTo("", col.actW) + " ")
	}
	b.WriteString(padTo("NAME", col.nameW))
	if col.showSize {
		b.WriteString(" " + padTo("SIZE", col.sizeW))
	}
	if col.showDone {
		b.WriteString(" " + padTo("DONE", col.doneW))
	}
	if col.showEta {
		b.WriteString(" " + padTo("ETA", col.etaW))
	}
	if col.showStatus {
		b.WriteString(" " + padTo("STATUS", col.statusW))
	}
	if col.showSpeed {
		b.WriteString(" " + padTo("SPEED", col.speedW))
	}
	return b.String()
}

func monoRow(m *model, st styles, col listLayout, i int, s *downloader.Session) string {
	cell := func(v string, w int) string { return padTo(truncateRight(v, w), w) }

	icon, iconSt := monoStatusIcon(st, s)
	name := sanitizeText(s.Name())
	sizeStr := formatBytes(s.TotalSize())
	if s.IsMetadataMode() {
		sizeStr = "unknown"
	}
	pctStr := fmt.Sprintf("%.1f%%", s.PercentComplete())
	if s.IsMetadataMode() {
		pctStr = "0.0%"
	}
	statusLabel, statusSt := statusLabelStyle(st, s.Status())
	speedStr := getSpeedStr(s.IsPaused(), s.IsCompleted(), currentTransferSpeed(s))
	spdSt := speedStyle(st, s.Status() == "Downloading")

	selected := i == m.selectedIdx
	g := gutterStr(m.width)
	if selected && effGutter(m.width) >= 2 {
		g = st.Accent.Render("▏") + " "
	}
	nameSt := st.Primary
	if selected {
		nameSt = st.Emphasis
	}

	var b strings.Builder
	b.WriteString(g)
	if col.showAct {
		b.WriteString(iconSt.Render(cell(icon, col.actW)) + " ")
	}
	b.WriteString(nameSt.Render(cell(name, col.nameW)))
	if col.showSize {
		b.WriteString(" " + st.Muted.Render(cell(sizeStr, col.sizeW)))
	}
	if col.showDone {
		b.WriteString(" " + st.Primary.Render(cell(pctStr, col.doneW)))
	}
	if col.showEta {
		b.WriteString(" " + st.Muted.Render(cell(sessionETA(s), col.etaW)))
	}
	if col.showStatus {
		b.WriteString(" " + statusSt.Render(cell(statusLabel, col.statusW)))
	}
	if col.showSpeed {
		b.WriteString(" " + spdSt.Render(cell(speedStr, col.speedW)))
	}
	b.WriteString("\n")

	// Thin line meter on its own line. At the middle breakpoint the row drops
	// SPEED and shows it beside the meter instead.
	meterW := bodyWidth(m.width)
	meterSuffix := ""
	if col.foldSpeed {
		meterSuffix = truncateRight(speedStr, meterW)
		meterW -= dispWidth(meterSuffix) + 1
		if meterW < 1 {
			meterW = 1
			meterSuffix = ""
		}
	}
	var bar string
	switch {
	case s.IsMetadataMode():
		bar = st.Track.Render(strings.Repeat("─", meterW))
	case s.Status() == "Downloading" || s.Status() == "Checking":
		bar = hairBar(s.PercentComplete(), meterW, st.Accent, st.Track)
	default:
		bar = hairBar(s.PercentComplete(), meterW, st.Muted, st.Track)
	}
	if meterSuffix != "" {
		bar += " " + spdSt.Render(meterSuffix)
	}
	b.WriteString(gutterStr(m.width) + bar + "\n\n")
	return b.String()
}

func renderDetailsMono(m *model) string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	var sb strings.Builder
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		sb.WriteString(g + st.Bold.Render("saintTorrent") + "\n")
		return sb.String()
	}
	s := m.sessions[m.selectedIdx]
	hashHex := fmt.Sprintf("%x", s.Torrent.InfoHash)
	active := s.GetActivePeers()

	// header
	sb.WriteString(g + st.Dim.Render("TORRENT") + "   " +
		st.Bold.Render(truncateRight(sanitizeText(s.Name()), bw-dispWidth("TORRENT   "))) + "\n")
	sb.WriteString(g + st.Dim.Render("hash") + "  " +
		st.Faint.Render(truncateRight(hashHex, bw-dispWidth("hash  "))) + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n")

	statusLabel, statusSt := statusLabelStyle(st, s.Status())
	upSpeed := s.CurrentUploadSpeed()
	uploadPeers := s.GetUploadPeerStats()
	seeders, leechers, completed := s.TrackerSwarmStats()
	stats := st.Dim.Render("SIZE") + " " + st.Primary.Render(formatBytes(s.TotalSize())) + "   " +
		st.Dim.Render("STATUS") + " " + statusSt.Render(statusLabel) + "   " +
		st.Dim.Render("ETA") + " " + st.Emphasis.Render(sessionETA(s)) + "   " +
		st.Dim.Render("RATIO") + " " + st.Muted.Render(monoRatio(s))
	sb.WriteString(g + stats + "\n")
	line2 := st.Dim.Render("PEERS") + " " + st.Primary.Render(strconv.Itoa(len(active))+" connected") + "   " +
		st.Dim.Render("DOWN") + " " + st.AccentBold.Render("↓ "+formatSpeed(s.CurrentSpeed())) + "   " +
		st.Dim.Render("UP") + " " + st.Muted.Render("↑ "+formatSpeed(upSpeed))
	sb.WriteString(g + line2 + "\n")
	sb.WriteString(g + st.Dim.Render("UPLOAD") + " " +
		st.Muted.Render(fmt.Sprintf("%d interested / %d slots", uploadPeers.Interested, uploadPeers.Unchoked)) + "   " +
		st.Dim.Render("SWARM") + " " +
		st.Muted.Render(fmt.Sprintf("%d seeds / %d leechers / %d completed", seeders, leechers, completed)) + "\n")
	sb.WriteString(g + st.Dim.Render("PORT") + " " + st.Muted.Render(peerPortStatus(m.manager)) + "\n\n")

	pct := s.PercentComplete()
	sb.WriteString(g + st.Dim.Render("COMPLETE") + "  " + st.Bold.Render(fmt.Sprintf("%.1f%%", pct)) + "\n")
	sb.WriteString(g + hairBar(pct, bw, st.Accent, st.Track) + "\n\n")

	transferSpeed := currentTransferSpeed(s)
	direction := "↓ "
	if s.IsCompleted() {
		direction = "↑ "
	}
	spark := monoSparkline(m.speedHistory[hashHex], st)
	sb.WriteString(g + st.Dim.Render("THROUGHPUT") + "  " + spark + "   " +
		st.AccentBold.Render(direction+formatSpeed(transferSpeed)) + "\n\n")

	sb.WriteString(g + st.Dim.Render("CONNECTED PEERS") + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n")
	if len(active) == 0 {
		if s.IsPaused() {
			sb.WriteString(g + st.Muted.Render("Session is paused.") + "\n")
		} else {
			sb.WriteString(g + st.Muted.Render("No connected peers. Searching via DHT/Tracker…") + "\n")
		}
	} else {
		for _, p := range active {
			choke := "They allow"
			pst := st.Muted
			if p.Choked {
				choke = "They choke"
				pst = st.Faint
			}
			uploadState := "not interested"
			if p.Interested {
				if p.AmChoking {
					uploadState = "wants / we choke"
				} else {
					uploadState = "wants / upload open"
				}
			}
			addr := fmt.Sprintf("%s:%d", p.IP, p.Port)
			row := padTo(truncateRight(addr, 24), 24) + " " + padTo(choke, 10) + " " +
				padTo(uploadState, 20) + " ↓ " + formatSpeed(p.DownloadSpeed) + " ↑ " + formatSpeed(p.UploadSpeed)
			sb.WriteString(g + pst.Render(truncateRight(row, bw)) + "\n")
		}
	}
	sb.WriteString("\n")

	if !s.IsMetadataMode() {
		pieces := s.GetPieceStates()
		sb.WriteString(g + st.Dim.Render("PIECE MAP") + "   " + st.Muted.Render(strconv.Itoa(len(pieces))+" pieces") + "\n")
		perRow := bw
		if perRow < 1 {
			perRow = 1
		}
		var pm strings.Builder
		for i, state := range pieces {
			if i%perRow == 0 {
				if i > 0 {
					pm.WriteString("\n")
				}
				pm.WriteString(g)
			}
			switch state {
			case downloader.PieceCompleted:
				pm.WriteString(st.PieceHave.Render("█"))
			case downloader.PieceDownloading:
				pm.WriteString(st.PieceDownloading.Render("▒"))
			default:
				pm.WriteString(st.PiecePending.Render("·"))
			}
		}
		sb.WriteString(pm.String() + "\n\n")
	}

	spaceActionHelp := getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	if m.flash != "" {
		sb.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	sb.WriteString(dividerLine(st, m.width) + "\n")
	sb.WriteString(renderHelp([][2]string{
		{"↑/↓", "Scroll"}, {"pgup/pgdn", "Page"},
		{"esc", "Back"}, {"space", spaceActionHelp}, {"f", "Files"}, {"o", "Open"},
		{"x", "Delete"}, {"X", "Delete+Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}

// monoSparkline renders a braille sparkline from a per-torrent speed history,
// scaled to its own peak. Empty history (just-opened torrent) shows a hint.
func monoSparkline(vals []float64, st styles) string {
	if len(vals) == 0 {
		return st.Muted.Render("collecting…")
	}
	var max float64
	for _, v := range vals {
		if v > max {
			max = v
		}
	}
	var b strings.Builder
	for _, v := range vals {
		idx := 0
		if max > 0 {
			idx = int(v/max*float64(len(brailleRamp)-1) + 0.5)
		}
		idx = clamp(idx, 0, len(brailleRamp)-1)
		b.WriteRune(brailleRamp[idx])
	}
	return st.Accent.Render(b.String())
}

func monoRatio(s *downloader.Session) string {
	size := s.TotalSize()
	downloaded := int64(s.PercentComplete() / 100.0 * float64(size))
	if downloaded <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.2f", float64(s.UploadedBytes())/float64(downloaded))
}
