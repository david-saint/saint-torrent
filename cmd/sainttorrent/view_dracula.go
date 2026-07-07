package main

import (
	"fmt"
	"strconv"
	"strings"
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

	if len(m.rows) == 0 {
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
	if row, ok := m.selectedRow(); ok {
		spaceActionHelp = getSpaceActionHelp(row.paused, row.completed)
	}
	if m.flash != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	help := renderHelp([][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", spaceActionHelp}, {"o", "Open Folder"},
		{"a", "Add"}, {"d", "Down Limit"}, {"u", "Up Limit"},
		{"x", "Delete Task"}, {"X", "Delete Task & Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, listHelpColumns, st, m.width)

	var sb strings.Builder
	sb.WriteString(prefix)
	if len(m.rows) > 0 {
		capacity := dashboardCapacity(m.height, prefix, head.String(), help, 1, len(m.rows))
		start, end := visibleSessionRange(len(m.rows), m.selectedIdx, capacity)
		for i := start; i < end; i++ {
			sb.WriteString(g + dracRow(st, col, i == m.selectedIdx, m.rows[i]) + "\n")
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

func dracRow(st styles, col listLayout, selected bool, row sessionRow) string {
	cell := func(v string, w int) string { return padTo(truncateRight(v, w), w) }

	indicator := getIndicator(row.paused, row.completed)
	name := row.name
	sizeStr := formatBytes(row.totalSize)
	if row.metadataMode {
		sizeStr = "unknown"
	}
	pctStr := fmt.Sprintf("%.1f%%", row.percent)
	if row.metadataMode {
		pctStr = "0.0%"
	}
	statusLabel, statusSt := statusLabelStyle(st, row.status)
	speedStr := getSpeedStr(row.paused, row.completed, row.transferSpeed)
	spdSt := speedStyle(st, row.status == "Downloading")

	if selected {
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
			b.WriteString(" " + cell(row.eta, col.etaW))
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
		b.WriteString(" " + st.NormalRow.Render(cell(row.eta, col.etaW)))
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
	for _, row := range m.rows {
		totalDown += row.downloadSpeed
		totalUp += row.uploadSpeed
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

	d := m.detailData()
	if !d.valid {
		return sb.String()
	}
	row := d.row

	label := "Torrent Details: "
	sb.WriteString(g + st.Header.Render("Torrent Details:") + " " +
		truncateRight(row.name, bw-dispWidth(label)) + "\n\n")

	pct := row.percent / 100.0
	statusLabel, statusSt := statusLabelStyle(st, row.status)

	inner := outerWidth(m.width) - st.Card.GetHorizontalFrameSize()
	if inner < 1 {
		inner = 1
	}
	m.progress.Width = inner
	card := cardWidth(st.Card, m.width)
	uploadPeers := d.uploadPeers

	cardContent := st.Header.Render("Hash") + ": " + row.infoHashHex + "\n" +
		st.Header.Render("Total Size") + ": " + formatBytes(row.totalSize) + "\n" +
		st.Header.Render("Complete") + ": " + fmt.Sprintf("%.2f%%", row.percent) + "\n" +
		st.Header.Render("Status") + ": " + statusSt.Render(statusLabel) + "\n" +
		st.Header.Render("Speed") + ": ↓ " + formatSpeed(row.downloadSpeed) +
		" / ↑ " + formatSpeed(row.uploadSpeed) + "\n" +
		st.Header.Render("Upload Demand") + ": " +
		fmt.Sprintf("%d interested / %d slots", uploadPeers.Interested, uploadPeers.Unchoked) + "\n" +
		st.Header.Render("Tracker Swarm") + ": " +
		fmt.Sprintf("%d seeds / %d leechers / %d completed", d.seeders, d.leechers, d.completed) + "\n" +
		st.Header.Render("Peer Port") + ": " + peerPortStatus(m.manager) + "\n\n" +
		m.progress.ViewAs(pct)
	if row.lastErrText != "" {
		cardContent += "\n" + st.Header.Render("Last Issue") + ": " + sanitizeText(row.lastErrText)
	}
	sb.WriteString(card.Render(cardContent))
	sb.WriteString("\n")

	var peers strings.Builder
	peers.WriteString(st.Header.Render("Connected Peers:") + "\n")
	active := d.peers
	if len(active) == 0 {
		switch {
		case row.paused && row.completed:
			peers.WriteString(st.Subtle.Render("  Session is stopped.") + "\n")
		case row.paused:
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

	if !row.metadataMode {
		sb.WriteString(g + st.Header.Render("Pieces Visual Map:") + "\n")
		sb.WriteString(renderPieceMap(st, d.pieceStates, bw, g, "█", "░", ".") + "\n\n")
	}

	spaceActionHelp := getSpaceActionHelp(row.paused, row.completed)
	if m.flash != "" {
		sb.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	sb.WriteString(renderHelp([][2]string{
		{"↑/↓", "Scroll"}, {"pgup/pgdn", "Page"},
		{"esc", "Back"}, {"space", spaceActionHelp}, {"f", "Files"}, {"o", "Open Folder"},
		{"x", "Delete Task"}, {"X", "Delete Task & Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, helpColumns, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}
