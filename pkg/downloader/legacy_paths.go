package downloader

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/storage"
	"sainttorrent/pkg/torrent"
)

// migrateLegacyPaths moves the payload files an older version laid out under
// their pre-sanitizer names (torrent.File.LegacyPath) to the names they have
// now, inside baseDir, and returns how many it moved. Without it an upgrade
// orphaned those files and downloaded the torrent again from nothing. The
// caller has claimed the current paths and not yet built storage on them, so
// no torrent of this process can be creating them meanwhile. A legacy path
// any torrent holds, this one included, is left alone. Only a torrent with
// legacy paths, which is rare, touches the disk here.
func (m *TorrentManager) migrateLegacyPaths(infoHash [20]byte, baseDir string, files []torrent.File) int {
	var moves []storage.LegacyMove
	for _, f := range files {
		if len(f.LegacyPath) > 0 {
			moves = append(moves, storage.LegacyMove{
				From:   filepath.Join(f.LegacyPath...),
				To:     filepath.Join(f.Path...),
				Length: f.Length,
			})
		}
	}
	if len(moves) == 0 {
		return 0
	}
	from := make([]string, len(moves))
	for i, move := range moves {
		from[i] = move.From
	}
	free, release := m.claimUnheldPaths(baseDir, from)
	defer release()
	unheld := make([]storage.LegacyMove, len(free))
	for i, index := range free {
		unheld[i] = moves[index]
	}
	moved, err := storage.MoveLegacyFiles(baseDir, unheld)
	if logging.Enabled() {
		if len(moved) > 0 {
			logging.Info("legacy_paths_migrated",
				logging.String("info_hash", fmt.Sprintf("%x", infoHash)),
				logging.String("download_dir", baseDir),
				logging.Int("files", len(moved)),
			)
		}
		if err != nil {
			logging.Warn("legacy_path_migration_failed",
				logging.String("info_hash", fmt.Sprintf("%x", infoHash)),
				logging.String("download_dir", baseDir),
				logging.Err(err),
			)
		}
	}
	return len(moved)
}

// legacyMigrations tallies, while a launch restores its torrents, the files
// migrateLegacyPaths moved, for the startup notice.
type legacyMigrations struct {
	files int
	names []string
}

// noteRestoreMigration records that restoring the torrent name moved files of
// it (see migrateLegacyPaths).
func (m *TorrentManager) noteRestoreMigration(name string, files int) {
	m.mu.Lock()
	m.restoreMigrations.files += files
	m.restoreMigrations.names = append(m.restoreMigrations.names, name)
	m.mu.Unlock()
}

// maxMigrationNoticeNames bounds the torrent names the notice lists: the TUI
// shows one line.
const maxMigrationNoticeNames = 3

// takeRestoreMigrationNotice returns the one-line notice for the files this
// launch's restore moved to their current names, or "" when it moved none.
func (m *TorrentManager) takeRestoreMigrationNotice() string {
	m.mu.Lock()
	migrations := m.restoreMigrations
	m.restoreMigrations = legacyMigrations{}
	m.mu.Unlock()
	if migrations.files == 0 {
		return ""
	}
	names := migrations.names
	sort.Strings(names)
	listed := names
	if len(listed) > maxMigrationNoticeNames {
		listed = listed[:maxMigrationNoticeNames]
	}
	list := strings.Join(listed, ", ")
	if more := len(names) - len(listed); more > 0 {
		list += fmt.Sprintf(" and %d more", more)
	}
	return fmt.Sprintf("renamed %d file(s) of %d torrent(s) that an older version saved under unsafe names: %s",
		migrations.files, len(names), list)
}
