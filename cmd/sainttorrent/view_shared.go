package main

import (
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
	maxOuterWidth = 100
)

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

// renderHelp lays out [key] Label pairs, greedily packing them into rows no
// wider than the body and wrapping to more rows as needed (gutter-prefixed).
func renderHelp(items [][2]string, st styles, termWidth int) string {
	g := gutterStr(termWidth)
	bw := bodyWidth(termWidth)

	var lines []string
	var cur []string
	curW := 0
	for _, it := range items {
		piece := "[" + it[0] + "] " + it[1]
		w := dispWidth(piece)
		if len(cur) > 0 && curW+2+w > bw {
			lines = append(lines, g+strings.Join(cur, "  "))
			cur, curW = nil, 0
		}
		if len(cur) > 0 {
			curW += 2
		}
		cur = append(cur, st.Help.Render(piece))
		curW += w
	}
	if len(cur) > 0 {
		lines = append(lines, g+strings.Join(cur, "  "))
	}
	return strings.Join(lines, "\n")
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

func maxVerticalOffset(s string, height int) int {
	if height <= 0 {
		return 0
	}
	return max(0, renderedLineCount(s)-height)
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

func visibleSessionCapacity(height int, fixedView string, linesPerItem, total int) int {
	if total <= 0 || height <= 0 {
		return total
	}
	available := height - renderedLineCount(fixedView)
	capacity := available / max(1, linesPerItem)
	return clamp(capacity, 1, total)
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
// The wide list shows ACT · NAME(flex) · SIZE · DONE · STATUS · SPEED. NAME
// absorbs slack; explicit breakpoints progressively remove SPEED, SIZE, STATUS,
// and ACT. At ultra-narrow widths only NAME remains.

type listLayout struct {
	bodyW                           int
	actW, nameW, sizeW, doneW       int
	statusW, speedW                 int
	showAct, showDone               bool
	showSize, showStatus, showSpeed bool
	foldSpeed                       bool
}

func listColumns(bw int) listLayout {
	const (
		actW    = 3
		sizeW   = 9
		doneW   = 6
		statusW = 11
		speedW  = 11
	)
	l := listLayout{
		bodyW: bw, actW: actW, sizeW: sizeW, doneW: doneW, statusW: statusW, speedW: speedW,
	}

	// Breakpoints use body width (terminal width minus the leading gutter).
	// They correspond to terminal widths 80, 68, and 52 respectively.
	switch {
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
	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
		return ""
	}
	s := m.sessions[m.selectedIdx]
	files := s.Files()

	bw := bodyWidth(m.width)
	g := gutterStr(m.width)

	var sb strings.Builder
	label := "File Explorer: "
	sb.WriteString(g + st.Header.Render("File Explorer:") + " " +
		truncateRight(sanitizeText(s.Name()), bw-dispWidth(label)) + "\n\n")

	if len(files) == 0 {
		sb.WriteString(g + "No files in metadata.\n\n")
	} else {
		priorities := s.GetFilePriorities()
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
	}, st, m.width))
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
	sb.WriteString(renderHelp([][2]string{{"enter", "Confirm"}, {"esc", "Cancel"}}, st, m.width))
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
		sb.WriteString(renderHelp([][2]string{{"esc/n/y", "Dismiss and continue"}}, st, m.width))
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
	}, st, m.width))
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
		sb.WriteString(renderHelp([][2]string{{"esc/n/y", "Back to Dashboard"}}, st, m.width))
		sb.WriteString("\n")
		return sb.String()
	}

	if len(m.sessions) == 0 || m.selectedIdx >= len(m.sessions) {
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
	sb.WriteString(renderHelp([][2]string{{"y", "Yes, Confirm Delete"}, {"n/esc", "Cancel"}}, st, m.width))
	sb.WriteString("\n")
	return sb.String()
}
