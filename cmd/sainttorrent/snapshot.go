package main

import (
	"fmt"
	"strings"
	"time"

	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/torrent"
)

// --- per-tick display snapshots (issue #57) -------------------------------
//
// The TUI re-renders at the 10 Hz animation rate but only refreshes data at the
// 2 Hz tick rate. Rendering every frame from live session getters means each
// visible row re-locks the session (s.mu) ~12×/sec, competing with the download
// hot path that needs the same lock to record completed pieces. To avoid that,
// every getter the views need is gathered once per data tick into these plain
// structs; View renders purely from them and never touches a session lock. This
// is what makes the "renders from the cached session snapshot" claim in anim.go
// actually true.

// sessionRow is the snapshot of one list row. Derived string fields (sanitized
// name, ETA, transfer speed) are computed once here rather than per row per
// frame.
type sessionRow struct {
	// session is retained only for keypress-driven actions (pause, delete,
	// open-folder). It is never queried on the per-frame render path.
	session *downloader.Session

	infoHashHex   string
	name          string // sanitized once per tick, not per row per frame
	totalSize     int64
	percent       float64
	status        string
	paused        bool
	completed     bool
	metadataMode  bool
	downloadSpeed float64
	uploadSpeed   float64
	transferSpeed float64
	eta           string
	uploadedBytes int64
	lastErrText   string
}

// detailSnapshot is the snapshot backing the detail view for the selected
// session. It carries the heavier per-frame data (active peers, the full piece
// state slice) that GetActivePeers/GetPieceStates would otherwise copy under the
// session lock on every frame.
type detailSnapshot struct {
	valid       bool
	row         sessionRow
	peers       []downloader.PeerState
	pieceStates []downloader.PieceState
	uploadPeers downloader.UploadPeerStats
	seeders     int
	leechers    int
	completed   int
}

// detailSig keys the memoized detail body. It captures every input that changes
// the rendered detail view — width, active theme, the detail snapshot version,
// and the flash line — but deliberately NOT detailScroll, so scrolling reuses the
// cached body instead of re-rendering it.
type detailSig struct {
	width   int
	theme   *theme
	version uint64
	flash   string
}

// filesSnapshot is the snapshot backing the file explorer. Rebuilt when the
// files view is entered and after a priority toggle, so it stays responsive
// without the view re-locking the session every frame.
type filesSnapshot struct {
	valid      bool
	name       string
	files      []torrent.File
	priorities []downloader.FilePriority
}

// rowFromSnapshot converts a downloader.SessionSnapshot into the UI row,
// computing the derived display fields once.
func rowFromSnapshot(s *downloader.Session, snap downloader.SessionSnapshot) sessionRow {
	row := sessionRow{
		session:       s,
		infoHashHex:   fmt.Sprintf("%x", snap.InfoHash),
		name:          sanitizeText(snap.Name),
		totalSize:     snap.TotalSize,
		percent:       snap.Percent,
		status:        snap.Status,
		paused:        snap.Paused,
		completed:     snap.Completed,
		metadataMode:  snap.MetadataMode,
		downloadSpeed: snap.DownloadSpeed,
		uploadSpeed:   snap.UploadSpeed,
		uploadedBytes: snap.UploadedBytes,
	}
	row.transferSpeed = row.downloadSpeed
	if row.completed {
		row.transferSpeed = row.uploadSpeed
	}
	row.eta = rowETA(row)
	if snap.LastError != nil {
		row.lastErrText = snap.LastError.Error()
	}
	return row
}

// rowETA mirrors the former sessionETA helper but reads the pre-snapshotted
// fields instead of re-locking the session.
func rowETA(row sessionRow) string {
	if row.completed {
		return "—"
	}
	speed := row.downloadSpeed
	if speed <= 0 {
		return "—"
	}
	downloaded := int64(row.percent / 100.0 * float64(row.totalSize))
	remaining := row.totalSize - downloaded
	if remaining <= 0 {
		return "—"
	}
	d := time.Duration(float64(remaining)/speed) * time.Second
	switch {
	case d >= time.Hour:
		return fmt.Sprintf("~%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	case d >= time.Minute:
		return fmt.Sprintf("~%dm %ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("~%ds", int(d.Seconds()))
	}
}

// buildSnapshots refreshes the list rows and the selected session's detail
// snapshot from the live sessions, and records whether anything on screen still
// animates (a Downloading row drives the speed pulse). Called once per data tick
// and at startup — never on the per-frame render path.
func (m *model) buildSnapshots() {
	rows := make([]sessionRow, len(m.sessions))
	animate := false
	for i, s := range m.sessions {
		rows[i] = rowFromSnapshot(s, s.Snapshot())
		if rows[i].status == "Downloading" {
			animate = true
		}
	}
	m.rows = rows
	m.hasDownloading = animate
	m.buildDetailSnapshot()
}

// selectedRow returns the snapshot row under the list cursor.
func (m *model) selectedRow() (sessionRow, bool) {
	if m.selectedIdx < 0 || m.selectedIdx >= len(m.rows) {
		return sessionRow{}, false
	}
	return m.rows[m.selectedIdx], true
}

// buildDetailSnapshot rebuilds the detail snapshot for the currently selected
// session. Called on tick and whenever the detail view is (re)entered, so it
// reflects the latest selection immediately rather than after the next tick.
func (m *model) buildDetailSnapshot() {
	row, ok := m.selectedRow()
	if !ok {
		m.detail = detailSnapshot{}
		m.detailVersion++
		return
	}
	s := row.session
	seeders, leechers, completed := s.TrackerSwarmStats()
	d := detailSnapshot{
		valid:       true,
		row:         row,
		peers:       s.GetActivePeers(),
		uploadPeers: s.GetUploadPeerStats(),
		seeders:     seeders,
		leechers:    leechers,
		completed:   completed,
	}
	if !row.metadataMode {
		d.pieceStates = s.GetPieceStates()
	}
	m.detail = d
	m.detailVersion++
}

// buildFilesSnapshot rebuilds the file-explorer snapshot for the selected
// session.
func (m *model) buildFilesSnapshot() {
	s, ok := m.selectedSession()
	if !ok {
		m.files = filesSnapshot{}
		return
	}
	m.files = filesSnapshot{
		valid:      true,
		name:       sanitizeText(s.Name()),
		files:      s.Files(),
		priorities: s.GetFilePriorities(),
	}
}

// filesData returns the current files snapshot, falling back to a fresh live
// build when one has not been prepared yet (e.g. a test that sets viewMode
// directly). The fallback is off the per-frame path in normal use.
func (m *model) filesData() filesSnapshot {
	if m.files.valid {
		return m.files
	}
	s, ok := m.selectedSession()
	if !ok {
		return filesSnapshot{}
	}
	return filesSnapshot{
		valid:      true,
		name:       sanitizeText(s.Name()),
		files:      s.Files(),
		priorities: s.GetFilePriorities(),
	}
}

// detailData returns the detail snapshot, falling back to a fresh live build
// when one has not been prepared yet (e.g. a test that sets viewMode directly).
func (m *model) detailData() detailSnapshot {
	if m.detail.valid {
		return m.detail
	}
	row, ok := m.selectedRow()
	if !ok {
		if s, sok := m.selectedSession(); sok {
			row = rowFromSnapshot(s, s.Snapshot())
			ok = true
		}
	}
	if !ok {
		return detailSnapshot{}
	}
	s := row.session
	seeders, leechers, completed := s.TrackerSwarmStats()
	d := detailSnapshot{
		valid:       true,
		row:         row,
		peers:       s.GetActivePeers(),
		uploadPeers: s.GetUploadPeerStats(),
		seeders:     seeders,
		leechers:    leechers,
		completed:   completed,
	}
	if !row.metadataMode {
		d.pieceStates = s.GetPieceStates()
	}
	return d
}

// renderPieceMap draws the piece grid, batching maximal runs of identical piece
// states into a single styled Render call instead of one Render per piece. A
// 50k-piece torrent otherwise issues 50k Render calls (and ~1 MB/s of string
// garbage) per frame; run-batching collapses that to a handful per row. glyphs
// maps have/downloading/pending states to their rune; g is the gutter prefix.
func renderPieceMap(st styles, states []downloader.PieceState, perRow int, g, have, downloading, pending string) string {
	if perRow < 1 {
		perRow = 1
	}
	styleFor := func(state downloader.PieceState) (glyph string, style func(...string) string) {
		switch state {
		case downloader.PieceCompleted:
			return have, st.PieceHave.Render
		case downloader.PieceDownloading:
			return downloading, st.PieceDownloading.Render
		default:
			return pending, st.PiecePending.Render
		}
	}

	var pm strings.Builder
	for start := 0; start < len(states); start += perRow {
		end := start + perRow
		if end > len(states) {
			end = len(states)
		}
		if start > 0 {
			pm.WriteString("\n")
		}
		pm.WriteString(g)
		// Batch consecutive identical states within this row into one Render.
		i := start
		for i < end {
			glyph, style := styleFor(states[i])
			j := i + 1
			for j < end && states[j] == states[i] {
				j++
			}
			pm.WriteString(style(strings.Repeat(glyph, j-i)))
			i = j
		}
	}
	return pm.String()
}
