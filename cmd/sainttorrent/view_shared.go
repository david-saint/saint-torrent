package main

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
	"sainttorrent/pkg/downloader"
)

// --- width budget ---------------------------------------------------------
//
// outerWidth is the cap on the entire rendered line, including the leading
// gutter; it is what the max-line-width tests assert against. bodyWidth is the
// content area inside the gutter that columns/bars/cards lay out within. There
// is no hard floor: at tiny sizes the gutter collapses and content truncates so
// the invariant lineWidth <= outerWidth always holds.

const (
	gutter        = 2
	maxOuterWidth = 115 // +15% over the original 100-cell cap
)

func peerPortStatus(manager *downloader.TorrentManager) string {
	listenPort := manager.PeerListenPort()
	if listenPort == 0 {
		return "unavailable"
	}
	status := manager.NATStatus()
	if status.TCPMapped {
		if status.AdvertisedPort != 0 && status.AdvertisedPort != listenPort {
			return fmt.Sprintf("%d→%d %s", listenPort, status.AdvertisedPort, status.Protocol)
		}
		return fmt.Sprintf("%d %s", listenPort, status.Protocol)
	}
	if status.Enabled && status.LastError == "" {
		return fmt.Sprintf("%d mapping", listenPort)
	}
	return fmt.Sprintf("%d manual", listenPort)
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func outerWidth(termWidth int) int {
	if termWidth <= 0 {
		return maxOuterWidth // sensible default before the first WindowSizeMsg
	}
	if termWidth > maxOuterWidth {
		return maxOuterWidth
	}
	return termWidth
}

func effGutter(termWidth int) int {
	if outerWidth(termWidth) <= gutter {
		return 0
	}
	return gutter
}

func bodyWidth(termWidth int) int {
	w := outerWidth(termWidth) - effGutter(termWidth)
	if w < 1 {
		w = 1
	}
	return w
}

func gutterStr(termWidth int) string { return strings.Repeat(" ", effGutter(termWidth)) }

// dispWidth is the terminal display-cell width of s (ANSI-aware, Unicode-safe).
func dispWidth(s string) int { return ansi.StringWidth(s) }

// padTo right-pads s to n display cells (no truncation).
func padTo(s string, n int) string {
	if w := dispWidth(s); w < n {
		return s + strings.Repeat(" ", n-w)
	}
	return s
}

// truncateRight shortens s to at most n display cells, appending an ellipsis
// when it cuts.
func truncateRight(s string, n int) string {
	if n <= 0 {
		return ""
	}
	if dispWidth(s) <= n {
		return s
	}
	return ansi.Truncate(s, n, "…")
}

// truncateMiddle keeps the tail of s (paths read better by their tail) behind a
// leading ellipsis when s exceeds n cells.
func truncateMiddle(s string, n int) string {
	if n <= 0 {
		return ""
	}
	w := dispWidth(s)
	if w <= n {
		return s
	}
	if n == 1 {
		return "…"
	}
	return ansi.TruncateLeft(s, w-(n-1), "…")
}

// spread justifies left and right to fill total display cells.
func spread(left, right string, total int) string {
	used := dispWidth(left) + dispWidth(right)
	if used >= total {
		return left + " " + right
	}
	return left + strings.Repeat(" ", total-used) + right
}

// hairBar renders a thin meter: heavy ━ fill over light ─ track, width w cells.
func hairBar(pct float64, w int, fill, track lipgloss.Style) string {
	if w < 1 {
		return ""
	}
	n := clamp(int(math.Round(pct/100*float64(w))), 0, w)
	return fill.Render(strings.Repeat("━", n)) + track.Render(strings.Repeat("─", w-n))
}

// dividerLine is a full-width hairline rule (gutter-prefixed).
func dividerLine(st styles, termWidth int) string {
	return gutterStr(termWidth) + st.Hairline.Render(strings.Repeat("─", bodyWidth(termWidth)))
}

// Preferred number of footer columns. Secondary views use helpColumns; the
// list view passes listHelpColumns so its longer shortcut list stays short
// instead of growing taller. The layout drops to fewer columns when the body
// is too narrow to fit them.
const (
	helpColumns     = 2
	listHelpColumns = 3
)

// renderHelp lays out [key] Label pairs in an aligned grid of up to maxCols
// columns (row-major), each row gutter-prefixed. Lines never overrun the body
// width: the column count is chosen to fit, and clampLines is the final net.
func renderHelp(items [][2]string, maxCols int, st styles, termWidth int) string {
	g := gutterStr(termWidth)
	bw := bodyWidth(termWidth)
	if len(items) == 0 {
		return ""
	}

	pieces := make([]string, len(items))
	for i, it := range items {
		pieces[i] = "[" + it[0] + "] " + it[1]
	}

	const gap = 2
	// Pick the widest column count (<= maxCols) whose grid fits the body.
	cols := 1
	for c := min(maxCols, len(pieces)); c > 1; c-- {
		if helpGridWidth(pieces, c, gap) <= bw {
			cols = c
			break
		}
	}

	// Per-column maxima so cells align across rows (row-major fill).
	colW := make([]int, cols)
	for i, p := range pieces {
		if c := i % cols; dispWidth(p) > colW[c] {
			colW[c] = dispWidth(p)
		}
	}

	var lines []string
	for r := 0; r*cols < len(pieces); r++ {
		var cells []string
		for c := 0; c < cols && r*cols+c < len(pieces); c++ {
			p := pieces[r*cols+c]
			if c < cols-1 {
				p = padTo(p, colW[c]) // align inner columns; trailing cell stays ragged
			}
			cells = append(cells, st.Help.Render(p))
		}
		lines = append(lines, g+strings.Join(cells, strings.Repeat(" ", gap)))
	}
	return strings.Join(lines, "\n")
}

// helpGridWidth returns the worst-case display width of a row-major grid with the
// given column count (sum of per-column maxima plus inter-column gaps).
func helpGridWidth(pieces []string, cols, gap int) int {
	colW := make([]int, cols)
	for i, p := range pieces {
		if c := i % cols; dispWidth(p) > colW[c] {
			colW[c] = dispWidth(p)
		}
	}
	total := gap * (cols - 1)
	for _, w := range colW {
		total += w
	}
	return total
}

// clampLines is the final safety net: it truncates every line of a rendered
// view to max display cells so nothing overruns the width cap, regardless of any
// per-renderer arithmetic drift.
func clampLines(s string, max int) string {
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		if dispWidth(ln) > max {
			lines[i] = ansi.Truncate(ln, max, "")
		}
	}
	return strings.Join(lines, "\n")
}

func renderedLineCount(s string) int {
	if s == "" {
		return 0
	}
	return len(strings.Split(s, "\n"))
}

// lineCount reports how many visible lines s occupies, treating a trailing
// newline as terminating the last line rather than starting a new empty one.
func lineCount(s string) int {
	if s == "" {
		return 0
	}
	n := strings.Count(s, "\n")
	if !strings.HasSuffix(s, "\n") {
		n++
	}
	return n
}

func verticalSlice(s string, offset, height int) string {
	if s == "" || height <= 0 {
		return s
	}
	lines := strings.Split(s, "\n")
	offset = clamp(offset, 0, max(0, len(lines)-height))
	end := min(len(lines), offset+height)
	return strings.Join(lines[offset:end], "\n")
}

func visibleSessionRange(total, selected, maxItems int) (start, end int) {
	if total <= 0 {
		return 0, 0
	}
	if maxItems <= 0 || maxItems >= total {
		return 0, total
	}
	selected = clamp(selected, 0, total-1)
	start = selected - maxItems/2
	if start < 0 {
		start = 0
	}
	end = start + maxItems
	if end > total {
		end = total
		start = max(0, end-maxItems)
	}
	return start, end
}

// dashboardCapacity reports how many session rows the list should render below
// the prefix. When the terminal is tall enough it reserves the entire footer
// (head + help) so the help block stays fully visible; when space is tight it
// keeps the head and lets the help block clip from the bottom (via the height
// clamp in View), giving the torrent list priority. It always returns at least 1
// so the selected torrent stays on screen.
func dashboardCapacity(height int, prefix, head, help string, linesPerItem, total int) int {
	if total <= 0 || height <= 0 {
		return total
	}
	budget := height - lineCount(prefix)
	available := budget - lineCount(head) - lineCount(help)
	if available < linesPerItem {
		// Not enough room for the full footer plus a row: drop the help block
		// from the reservation and let it clip instead of squeezing the list.
		available = budget - lineCount(head)
	}
	return clamp(available/max(1, linesPerItem), 1, total)
}

// statusLabelStyle maps a session status to its uppercase badge label and the
// active theme's style for it.
func statusLabelStyle(st styles, status string) (string, lipgloss.Style) {
	switch status {
	case "Paused":
		return "PAUSED", st.StatusPaused
	case "Stopped":
		return "STOPPED", st.StatusPaused
	case "Seeding":
		return "SEEDING", st.StatusSeeding
	case "Metadata":
		return "METADATA", st.StatusMetadata
	case "Checking":
		return "CHECKING", st.StatusMetadata
	case "Error":
		return "ERROR", st.StatusError
	default:
		return "DOWNLOADING", st.StatusDownloading
	}
}

// priorityBadge renders a file-priority badge in the active theme.
func priorityBadge(st styles, prio downloader.FilePriority) string {
	switch prio {
	case downloader.PriorityHigh:
		return st.PriorityHigh.Render(" HIGH ")
	case downloader.PrioritySkip:
		return st.PrioritySkip.Render(" SKIP ")
	default:
		return st.PriorityNormal.Render("NORMAL")
	}
}

// cardWidth returns a frame-aware content width so a bordered+padded card's
// total rendered width fits within the line cap.
func cardWidth(st lipgloss.Style, termWidth int) lipgloss.Style {
	inner := outerWidth(termWidth) - st.GetHorizontalFrameSize()
	if inner < 1 {
		inner = 1
	}
	return st.Width(inner)
}

// --- responsive list columns ---------------------------------------------
//
// The wide list shows ACT · NAME(flex) · SIZE · DONE · ETA · STATUS · SPEED. NAME
// absorbs slack; explicit breakpoints progressively remove ETA, SPEED, SIZE, STATUS,
// and ACT. At ultra-narrow widths only NAME remains.

type listLayout struct {
	bodyW                           int
	actW, nameW, sizeW, doneW       int
	etaW, statusW, speedW           int
	showAct, showDone, showEta      bool
	showSize, showStatus, showSpeed bool
	foldSpeed                       bool
}

func listColumns(bw int) listLayout {
	const (
		actW    = 3
		sizeW   = 9
		doneW   = 6
		etaW    = 8
		statusW = 11
		speedW  = 11
	)
	l := listLayout{
		bodyW: bw, actW: actW, sizeW: sizeW, doneW: doneW, etaW: etaW, statusW: statusW, speedW: speedW,
	}

	// Breakpoints use body width (terminal width minus the leading gutter). ETA only
	// appears once everything else fits with NAME still comfortable; the lower tiers
	// match the original 80/68/52-column terminal breakpoints.
	switch {
	case bw >= 90:
		l.showAct, l.showDone, l.showEta = true, true, true
		l.showSize, l.showStatus, l.showSpeed = true, true, true
	case bw >= 78:
		l.showAct, l.showDone = true, true
		l.showSize, l.showStatus, l.showSpeed = true, true, true
	case bw >= 66:
		l.showAct, l.showDone = true, true
		l.showSize, l.showStatus = true, true
		l.foldSpeed = true
	case bw >= 50:
		l.showAct, l.showDone = true, true
		l.showStatus = true
	case bw >= doneW+2:
		l.showDone = true
	}

	fixedWidth := func() (width, count int) {
		if l.showAct {
			width += actW
			count++
		}
		if l.showSize {
			width += sizeW
			count++
		}
		if l.showDone {
			width += doneW
			count++
		}
		if l.showEta {
			width += etaW
			count++
		}
		if l.showStatus {
			width += statusW
			count++
		}
		if l.showSpeed {
			width += speedW
			count++
		}
		return width, count
	}

	fixed, count := fixedWidth()
	l.nameW = bw - fixed - count // one gap between NAME and each fixed column

	// The explicit breakpoints keep NAME comfortably above its normal floor.
	// This loop is a defensive fallback for future width changes.
	for l.nameW < 1 {
		switch {
		case l.showEta:
			l.showEta = false
		case l.showSpeed:
			l.showSpeed = false
		case l.showSize:
			l.showSize = false
		case l.showStatus:
			l.showStatus = false
		case l.showAct:
			l.showAct = false
		case l.showDone:
			l.showDone = false
		default:
			l.nameW = max(1, bw)
			return l
		}
		fixed, count = fixedWidth()
		l.nameW = bw - fixed - count
	}

	return l
}

// --- shared secondary views ----------------------------------------------

func (m model) viewFileExplorer() string {
	st := m.theme.styles
	data := m.filesData()
	if !data.valid {
		return ""
	}
	files := data.files

	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	var sb strings.Builder
	label := "File Explorer: "
	sb.WriteString(g + st.Header.Render("File Explorer:") + " " +
		truncateRight(data.name, bw-dispWidth(label)) + "\n\n")

	if len(files) == 0 {
		sb.WriteString(g + "No files in metadata.\n\n")
	} else {
		priorities := data.priorities
		const badgeW = 6 // " HIGH " / "NORMAL" / " SKIP "
		const sizeW = 10
		pathW := bw - (sizeW + 1 + badgeW + 1)
		if pathW < 4 {
			pathW = 4
		}
		for i, f := range files {
			prio := downloader.PriorityNormal
			if i < len(priorities) {
				prio = priorities[i]
			}
			path := truncateMiddle(sanitizeText(filepath.Join(f.Path...)), pathW)
			row := padTo(path, pathW) + " " + padTo(formatBytes(f.Length), sizeW) + " " + priorityBadge(st, prio)
			if i == m.selectedFileIdx {
				sb.WriteString(g + st.SelectedRow.Render(row) + "\n")
			} else {
				sb.WriteString(g + st.NormalRow.Render(row) + "\n")
			}
		}
		sb.WriteString("\n")
	}

	sb.WriteString(renderHelp([][2]string{
		{"esc", "Back to Details"}, {"space/p", "Toggle Priority"}, {"q", "Quit"},
	}, helpColumns, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}

func (m model) viewInputBox() string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	var title string
	switch m.inputMode {
	case inputAddTorrent:
		title = "Add Torrent / Magnet Link"
	case inputLimitDownload:
		title = "Set Global Download Limit (KB/s)"
	case inputLimitUpload:
		title = "Set Global Upload Limit (KB/s)"
	}

	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(g + st.Header.Render(truncateRight(title, bw)) + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n\n")
	sb.WriteString(g + m.textInput.View() + "\n\n")
	if m.inputErr != "" {
		sb.WriteString(g + st.Error.Render(truncateRight(sanitizeText(m.inputErr), bw)) + "\n\n")
	}
	sb.WriteString(renderHelp([][2]string{{"enter", "Confirm"}, {"esc", "Cancel"}}, helpColumns, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}

func (m model) viewAddConfirm() string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	if m.pendingIdx >= len(m.pendingItems) {
		return ""
	}
	item := m.pendingItems[m.pendingIdx]

	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString(g + st.Header.Render("Confirm Add Torrent") + "\n")
	sb.WriteString(dividerLine(st, m.width) + "\n\n")

	if m.addConfirmErr != nil {
		label := "Error adding torrent: "
		sb.WriteString(g + st.Error.Render("Error adding torrent") + ": " +
			truncateRight(sanitizeText(m.addConfirmErr.Error()), bw-dispWidth(label)) + "\n\n")
		sb.WriteString(renderHelp([][2]string{{"esc/n/y", "Dismiss and continue"}}, helpColumns, st, m.width))
		sb.WriteString("\n")
		return sb.String()
	}

	nameLabel := "Torrent Name: "
	sb.WriteString(g + st.Header.Render("Torrent Name") + ": " +
		truncateRight(item.displayName, bw-dispWidth(nameLabel)) + "\n")
	dirLabel := "Download Dir: "
	sb.WriteString(g + st.Header.Render("Download Dir") + ": " +
		truncateMiddle(sanitizeText(item.downloadDir), bw-dispWidth(dirLabel)) + "\n\n")

	if item.isDuplicate {
		sb.WriteString(g + st.Error.Render(truncateRight(
			"Warning: This torrent is already in the download list. Confirming will resume it.", bw)) + "\n\n")
	}

	sb.WriteString(renderHelp([][2]string{
		{"y", "Yes, Confirm Download"}, {"n", "No, Skip"}, {"q", "Quit"},
	}, helpColumns, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}

func (m model) viewDeleteConfirm() string {
	st := m.theme.styles
	bw := bodyWidth(m.width)
	g := gutterStr(m.width)
	card := cardWidth(st.Card, m.width)

	header := func(label, name string) string {
		return g + st.Header.Render(label) + " " +
			truncateRight(sanitizeText(name), bw-dispWidth(label+" ")) + "\n\n"
	}

	if m.deleteInProgress {
		var sb strings.Builder
		sb.WriteString(header("Deleting:", m.deleteTargetName))
		status := "Stopping torrent and removing task state..."
		if m.deleteWithFiles {
			status = "Stopping torrent and deleting downloaded files..."
		}
		sb.WriteString(card.Render(status))
		sb.WriteString("\n\n")
		sb.WriteString(g + st.Help.Render("Deletion is running in the background") + "\n")
		return sb.String()
	}

	if m.deleteErr != nil {
		var sb strings.Builder
		sb.WriteString(header("Deletion Failure:", m.deleteTargetName))
		sb.WriteString(card.Render(st.Error.Render(sanitizeText(m.deleteErr.Error()))))
		sb.WriteString("\n\n")
		sb.WriteString(renderHelp([][2]string{{"esc/n/y", "Back to Dashboard"}}, helpColumns, st, m.width))
		sb.WriteString("\n")
		return sb.String()
	}

	if _, ok := m.selectedSession(); !ok {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(header("Confirm Delete:", m.deleteTargetName))

	var warnMsg string
	if m.deleteWithFiles {
		warnMsg = st.Error.Render("WARNING: This will permanently delete the task, state, and ALL downloaded files from disk!")
	} else {
		warnMsg = st.Warn.Render("This will delete the task state and fast-resume file, but keep downloaded files on disk.")
	}
	sb.WriteString(card.Render("Are you sure you want to delete this torrent?\n\n" + warnMsg))
	sb.WriteString("\n\n")
	sb.WriteString(renderHelp([][2]string{{"y", "Yes, Confirm Delete"}, {"n/esc", "Cancel"}}, helpColumns, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}
