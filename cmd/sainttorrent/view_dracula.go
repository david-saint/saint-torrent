package main

import (
	"fmt"
	"strconv"
	"strings"

	"sainttorrent/pkg/downloader"
)

// view_dracula.go renders the original tabular list + details look. It owns its
// top banner (the purple title block) and is width-flexible: columns flex via
// listColumns and cards/bars are frame-aware.

const dracBanner = " saintTorrent CLI v0.2 "

func renderListDracula(m *model) string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)
	col := listColumns(bw)

	prefix := st.Title.Render(dracBanner) + "\n"

	if len(m.sessions) == 0 {
		prefix += "\n" + g + "No active torrents. Press 'a' to add a torrent or magnet link.\n\n"
	} else {
		prefix += g + st.Subtle.Render(dracHeaderRow(col)) + "\n"
		prefix += dividerLine(st, m.width) + "\n"
	}

	// head holds the always-shown footer rows; help is the clippable key list.
	var head strings.Builder
	head.WriteString("\n")
	if m.startupWarn != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.startupWarn), bw)) + "\n\n")
	}

	head.WriteString(dividerLine(st, m.width) + "\n")
	head.WriteString(g + dracFooter(m, st) + "\n\n")

	spaceActionHelp := "Pause/Resume"
	if len(m.sessions) > 0 && m.selectedIdx < len(m.sessions) {
		s := m.sessions[m.selectedIdx]
		spaceActionHelp = getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	}
	if m.flash != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	help := renderHelp([][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", spaceActionHelp}, {"o", "Open Folder"},
		{"a", "Add"}, {"d", "Down Limit"}, {"u", "Up Limit"}, {"t", "Theme"}, {"q", "Quit"},
	}, st, m.width)

	var sb strings.Builder
	sb.WriteString(prefix)
	if len(m.sessions) > 0 {
		capacity := dashboardCapacity(m.height, prefix, head.String(), help, 1, len(m.sessions))
		start, end := visibleSessionRange(len(m.sessions), m.selectedIdx, capacity)
		for i := start; i < end; i++ {
			sb.WriteString(g + dracRow(m, st, col, i, m.sessions[i]) + "\n")
		}
	}
	sb.WriteString(head.String())
	sb.WriteString(help + "\n")
	return sb.String()
}

func dracHeaderRow(col listLayout) string {
	var b strings.Builder
	if col.showAct {
		b.WriteString(padTo("ACT", col.actW) + " ")
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

func dracRow(m *model, st styles, col listLayout, i int, s *downloader.Session) string {
	cell := func(v string, w int) string { return padTo(truncateRight(v, w), w) }

	indicator := getIndicator(s.IsPaused(), s.IsCompleted())
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

	if i == m.selectedIdx {
		// Solid highlight across the whole row (single style).
		var b strings.Builder
		if col.showAct {
			b.WriteString(cell(indicator, col.actW) + " ")
		}
		b.WriteString(cell(name, col.nameW))
		if col.showSize {
			b.WriteString(" " + cell(sizeStr, col.sizeW))
		}
		if col.showDone {
			b.WriteString(" " + cell(pctStr, col.doneW))
		}
		if col.showEta {
			b.WriteString(" " + cell(sessionETA(s), col.etaW))
		}
		if col.showStatus {
			b.WriteString(" " + cell(statusLabel, col.statusW))
		}
		if col.showSpeed {
			b.WriteString(" " + cell(speedStr, col.speedW))
		}
		return st.SelectedRow.Render(b.String())
	}

	var b strings.Builder
	if col.showAct {
		b.WriteString(st.NormalRow.Render(cell(indicator, col.actW)) + " ")
	}
	b.WriteString(st.NormalRow.Render(cell(name, col.nameW)))
	if col.showSize {
		b.WriteString(" " + st.NormalRow.Render(cell(sizeStr, col.sizeW)))
	}
	if col.showDone {
		b.WriteString(" " + st.NormalRow.Render(cell(pctStr, col.doneW)))
	}
	if col.showEta {
		b.WriteString(" " + st.NormalRow.Render(cell(sessionETA(s), col.etaW)))
	}
	if col.showStatus {
		b.WriteString(" " + statusSt.Render(cell(statusLabel, col.statusW)))
	}
	if col.showSpeed {
		b.WriteString(" " + spdSt.Render(cell(speedStr, col.speedW)))
	}
	return b.String()
}

func dracFooter(m *model, st styles) string {
	dhtNodes := 0
	if d := m.manager.DHT(); d != nil {
		dhtNodes = d.NodesCount()
	}
	var totalDown float64
	var totalUp float64
	for _, s := range m.sessions {
		totalDown += s.CurrentSpeed()
		totalUp += s.CurrentUploadSpeed()
	}
	downLimitStr := "Unlimited"
	if v := m.manager.GlobalDownloadLimit(); v > 0 {
		downLimitStr = formatSpeed(float64(v))
	}
	upLimitStr := "Unlimited"
	if v := m.manager.GlobalUploadLimit(); v > 0 {
		upLimitStr = formatSpeed(float64(v))
	}
	return "DHT: " + st.Info.Render(strconv.Itoa(dhtNodes)) + " nodes | Speed: ↓ " +
		st.Info.Render(formatSpeed(totalDown)) + " / ↑ " + st.Info.Render(formatSpeed(totalUp)) + " | Limits: ↓ " +
		st.Warn.Render(downLimitStr) + " / ↑ " + st.Warn.Render(upLimitStr)
}

func renderDetailsDracula(m *model) string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	var sb strings.Builder
	sb.WriteString(st.Title.Render(dracBanner) + "\n")

	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return sb.String()
	}
	s := m.sessions[m.selectedIdx]

	label := "Torrent Details: "
	sb.WriteString(g + st.Header.Render("Torrent Details:") + " " +
		truncateRight(sanitizeText(s.Name()), bw-dispWidth(label)) + "\n\n")

	pct := s.PercentComplete() / 100.0
	statusLabel, statusSt := statusLabelStyle(st, s.Status())

	inner := outerWidth(m.width) - st.Card.GetHorizontalFrameSize()
	if inner < 1 {
		inner = 1
	}
	m.progress.Width = inner
	card := cardWidth(st.Card, m.width)
	uploadPeers := s.GetUploadPeerStats()
	seeders, leechers := s.TrackerSwarmStats()

	cardContent := st.Header.Render("Hash") + ": " + fmt.Sprintf("%x", s.Torrent.InfoHash) + "\n" +
		st.Header.Render("Total Size") + ": " + formatBytes(s.TotalSize()) + "\n" +
		st.Header.Render("Complete") + ": " + fmt.Sprintf("%.2f%%", pct*100) + "\n" +
		st.Header.Render("Status") + ": " + statusSt.Render(statusLabel) + "\n" +
		st.Header.Render("Speed") + ": ↓ " + formatSpeed(s.CurrentSpeed()) +
		" / ↑ " + formatSpeed(s.CurrentUploadSpeed()) + "\n" +
		st.Header.Render("Upload Demand") + ": " +
		fmt.Sprintf("%d interested / %d slots", uploadPeers.Interested, uploadPeers.Unchoked) + "\n" +
		st.Header.Render("Tracker Swarm") + ": " +
		fmt.Sprintf("%d seeds / %d leechers", seeders, leechers) + "\n" +
		st.Header.Render("Peer Port") + ": " + peerPortStatus(m.manager) + "\n\n" +
		m.progress.ViewAs(pct)
	if err := s.LastError(); err != nil {
		cardContent += "\n" + st.Header.Render("Last Issue") + ": " + sanitizeText(err.Error())
	}
	sb.WriteString(card.Render(cardContent))
	sb.WriteString("\n")

	var peers strings.Builder
	peers.WriteString(st.Header.Render("Connected Peers:") + "\n")
	active := s.GetActivePeers()
	if len(active) == 0 {
		switch {
		case s.IsPaused() && s.IsCompleted():
			peers.WriteString(st.Subtle.Render("  Session is stopped.") + "\n")
		case s.IsPaused():
			peers.WriteString(st.Subtle.Render("  Session is paused.") + "\n")
		default:
			peers.WriteString(st.Subtle.Render("  No connected peers. Searching via DHT/Tracker...") + "\n")
		}
	} else {
		for _, p := range active {
			chokeStr := "they allow"
			if p.Choked {
				chokeStr = "they choke"
			}
			uploadState := "not interested"
			if p.Interested {
				if p.AmChoking {
					uploadState = "wants, we choke"
				} else {
					uploadState = "wants, upload open"
				}
			}
			line := fmt.Sprintf("  - %s:%-5d | %s | %s | ↓ %s | ↑ %s",
				p.IP, p.Port, chokeStr, uploadState, formatSpeed(p.DownloadSpeed), formatSpeed(p.UploadSpeed))
			peers.WriteString(truncateRight(line, inner) + "\n")
		}
	}
	sb.WriteString(cardWidth(st.PeersCard, m.width).Render(peers.String()))
	sb.WriteString("\n")

	if !s.IsMetadataMode() {
		sb.WriteString(g + st.Header.Render("Pieces Visual Map:") + "\n")
		pieces := s.GetPieceStates()
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
				pm.WriteString(st.PieceDownloading.Render("░"))
			default:
				pm.WriteString(st.PiecePending.Render("."))
			}
		}
		sb.WriteString(pm.String() + "\n\n")
	}

	spaceActionHelp := getSpaceActionHelp(s.IsPaused(), s.IsCompleted())
	if m.flash != "" {
		sb.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	sb.WriteString(renderHelp([][2]string{
		{"↑/↓", "Scroll"}, {"pgup/pgdn", "Page"},
		{"esc", "Back"}, {"space", spaceActionHelp}, {"f", "Files"}, {"o", "Open Folder"},
		{"x", "Delete Task"}, {"X", "Delete Task & Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}
