package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// view_mono.go renders the "Mono Hairline" look (ported from
// spikes/03a-mono-hairline): a brightness-driven monochrome layout with thin
// line meters, a wordmark header, and a braille throughput sparkline. It owns
// its top banner and is width-flexible.

var brailleRamp = []rune{' ', '⡀', '⣀', '⣄', '⣆', '⣇', '⣧', '⣷', '⣿'}

func monoStatusIcon(st styles, status string) (string, lipgloss.Style) {
	switch status {
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

// sessionTotals sums instantaneous transfer rates across the snapshot rows.
func sessionTotals(rows []sessionRow) (down, up float64) {
	for _, row := range rows {
		down += row.downloadSpeed
		up += row.uploadSpeed
	}
	return down, up
}

func renderListMono(m *model) string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)
	col := listColumns(bw)

	count := fmt.Sprintf("%d torrents", len(m.rows))
	if len(m.rows) == 1 {
		count = "1 torrent"
	}
	totalDown, totalUp := sessionTotals(m.rows)
	left := st.Bold.Render("saintTorrent") + "   " + st.Dim.Render(count)
	right := st.Dim.Render("▲ "+formatSpeed(totalUp)) + "  " + st.AccentBold.Render("↓ "+formatSpeed(totalDown))
	prefix := g + spread(left, right, bw) + "\n"
	prefix += dividerLine(st, m.width) + "\n"

	if len(m.rows) == 0 {
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
	if row, ok := m.selectedRow(); ok {
		spaceActionHelp = getSpaceActionHelp(row.paused, row.completed)
	}
	if m.flash != "" {
		head.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	help := renderHelp([][2]string{
		{"↑/↓", "Select"}, {"pgup/pgdn", "Page"},
		{"enter", "Details"}, {"space", spaceActionHelp}, {"o", "Open"}, {"a", "Add"},
		{"d", "Down"}, {"u", "Up"}, {"x", "Delete"}, {"X", "Delete+Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, listHelpColumns, st, m.width)

	var sb strings.Builder
	sb.WriteString(prefix)
	if len(m.rows) > 0 {
		capacity := dashboardCapacity(m.height, prefix, head.String(), help, 3, len(m.rows))
		start, end := visibleSessionRange(len(m.rows), m.selectedIdx, capacity)
		for i := start; i < end; i++ {
			sb.WriteString(monoRow(m, st, col, i == m.selectedIdx, m.rows[i]))
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

func monoRow(m *model, st styles, col listLayout, selected bool, row sessionRow) string {
	cell := func(v string, w int) string { return padTo(truncateRight(v, w), w) }

	icon, iconSt := monoStatusIcon(st, row.status)
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
		b.WriteString(" " + st.Muted.Render(cell(row.eta, col.etaW)))
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
	case row.metadataMode:
		bar = st.Track.Render(strings.Repeat("─", meterW))
	case row.status == "Downloading" || row.status == "Checking":
		bar = hairBar(row.percent, meterW, st.Accent, st.Track)
	default:
		bar = hairBar(row.percent, meterW, st.Muted, st.Track)
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
	d := m.detailData()
	if !d.valid {
		sb.WriteString(g + st.Bold.Render("saintTorrent") + "\n")
		return sb.String()
	}
	row := d.row
	hashHex := row.infoHashHex
	active := d.peers

	// header
	sb.WriteString(g + st.Dim.Render("TORRENT") + "   " +
		st.Bold.Render(truncateRight(row.name, bw-dispWidth("TORRENT   "))) + "\n")
	sb.WriteString(g + st.Dim.Render("hash") + "  " +
		st.Faint.Render(truncateRight(hashHex, bw-dispWidth("hash  "))) + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n")

	statusLabel, statusSt := statusLabelStyle(st, row.status)
	upSpeed := row.uploadSpeed
	uploadPeers := d.uploadPeers
	stats := st.Dim.Render("SIZE") + " " + st.Primary.Render(formatBytes(row.totalSize)) + "   " +
		st.Dim.Render("STATUS") + " " + statusSt.Render(statusLabel) + "   " +
		st.Dim.Render("ETA") + " " + st.Emphasis.Render(row.eta) + "   " +
		st.Dim.Render("RATIO") + " " + st.Muted.Render(monoRatio(row))
	sb.WriteString(g + stats + "\n")
	line2 := st.Dim.Render("PEERS") + " " + st.Primary.Render(strconv.Itoa(len(active))+" connected") + "   " +
		st.Dim.Render("DOWN") + " " + st.AccentBold.Render("↓ "+formatSpeed(row.downloadSpeed)) + "   " +
		st.Dim.Render("UP") + " " + st.Muted.Render("↑ "+formatSpeed(upSpeed))
	sb.WriteString(g + line2 + "\n")
	sb.WriteString(g + st.Dim.Render("UPLOAD") + " " +
		st.Muted.Render(fmt.Sprintf("%d interested / %d slots", uploadPeers.Interested, uploadPeers.Unchoked)) + "   " +
		st.Dim.Render("SWARM") + " " +
		st.Muted.Render(fmt.Sprintf("%d seeds / %d leechers / %d completed", d.seeders, d.leechers, d.completed)) + "\n")
	sb.WriteString(g + st.Dim.Render("PORT") + " " + st.Muted.Render(peerPortStatus(m.manager)) + "\n\n")

	pct := row.percent
	sb.WriteString(g + st.Dim.Render("COMPLETE") + "  " + st.Bold.Render(fmt.Sprintf("%.1f%%", pct)) + "\n")
	sb.WriteString(g + hairBar(pct, bw, st.Accent, st.Track) + "\n\n")

	transferSpeed := row.transferSpeed
	direction := "↓ "
	if row.completed {
		direction = "↑ "
	}
	spark := monoSparkline(m.speedHistory[hashHex], st)
	sb.WriteString(g + st.Dim.Render("THROUGHPUT") + "  " + spark + "   " +
		st.AccentBold.Render(direction+formatSpeed(transferSpeed)) + "\n\n")

	sb.WriteString(g + st.Dim.Render("CONNECTED PEERS") + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n")
	if len(active) == 0 {
		if row.paused {
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

	if !row.metadataMode {
		pieces := d.pieceStates
		sb.WriteString(g + st.Dim.Render("PIECE MAP") + "   " + st.Muted.Render(strconv.Itoa(len(pieces))+" pieces") + "\n")
		sb.WriteString(renderPieceMap(st, pieces, bw, g, "█", "▒", "·") + "\n\n")
	}

	spaceActionHelp := getSpaceActionHelp(row.paused, row.completed)
	if m.flash != "" {
		sb.WriteString(g + st.Warn.Render(truncateRight(sanitizeText(m.flash), bw)) + "\n")
	}
	sb.WriteString(dividerLine(st, m.width) + "\n")
	sb.WriteString(renderHelp([][2]string{
		{"↑/↓", "Scroll"}, {"pgup/pgdn", "Page"},
		{"esc", "Back"}, {"space", spaceActionHelp}, {"f", "Files"}, {"o", "Open"},
		{"x", "Delete"}, {"X", "Delete+Files"}, {"t", "Theme"}, {"q", "Quit"},
	}, helpColumns, st, m.width))
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

func monoRatio(row sessionRow) string {
	size := row.totalSize
	downloaded := int64(row.percent / 100.0 * float64(size))
	if downloaded <= 0 {
		return "—"
	}
	return fmt.Sprintf("%.2f", float64(row.uploadedBytes)/float64(downloaded))
}
