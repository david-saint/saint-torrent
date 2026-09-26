package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// LegacyMove is a payload file to move from the path an older version laid it
// out under to the path it has now. Both are relative to the download
// directory; Length is the file's length in the torrent.
type LegacyMove struct {
	From, To string
	Length   int64
}

// MoveLegacyFiles moves, inside baseDir, each file an older version wrote at
// From to To, and returns the moves it made. A move is made only when From is
// a regular file of the torrent's length and nothing exists at To. Neither
// path may pass through a symlink, a Windows junction or anything else that
// is not a real directory, nor start with a reserved storage name, and every
// operation goes through a handle on baseDir, so nothing outside it can be
// reached. Missing directories of To are created, and the directories of From
// that the move leaves empty are removed. A move that cannot be made is
// skipped, which leaves the storage to create To as it would have anyway; the
// errors of those that found a file at From are returned joined. A baseDir
// that does not exist holds nothing to move, and nothing is created then.
func MoveLegacyFiles(baseDir string, moves []LegacyMove) ([]LegacyMove, error) {
	if len(moves) == 0 {
		return nil, nil
	}
	root, err := openExistingPlatformDownloadRoot(baseDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open download directory %s: %w", baseDir, err)
	}
	defer root.Close()

	// dirs caches the directories already found to be real ones, so a torrent
	// of many files checks each of its directories once.
	dirs := make(map[string]bool)
	var moved []LegacyMove
	var errs []error
	for _, move := range moves {
		ok, err := moveLegacyFile(root, move, dirs)
		if err != nil {
			errs = append(errs, fmt.Errorf("move %q to %q: %w", move.From, move.To, err))
		}
		if ok {
			moved = append(moved, move)
		}
	}
	return moved, errors.Join(errs...)
}

func moveLegacyFile(root *os.Root, move LegacyMove, dirs map[string]bool) (bool, error) {
	from, to := filepath.Clean(move.From), filepath.Clean(move.To)
	if from == to || !filepath.IsLocal(from) || !filepath.IsLocal(to) ||
		isReservedStorageName(topComponent(from)) || isReservedStorageName(topComponent(to)) {
		return false, nil
	}
	// No file an older version could have written is there: the name is
	// missing, one this filesystem cannot hold (too long, invalid UTF-8 on
	// APFS, a reserved Windows name), under a symlink, or not a regular file
	// of the length that version sized it to.
	info, err := lstatUnderDirs(root, from, dirs)
	if err != nil || !info.Mode().IsRegular() || info.Size() != move.Length {
		return false, nil
	}
	// A file already at To is what the storage will use: never replace it.
	if _, err := lstatUnderDirs(root, to, dirs); !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if err := makeDirs(root, filepath.Dir(to), dirs); err != nil {
		return false, err
	}
	if err := root.Rename(from, to); err != nil {
		return false, err
	}
	// Remove the old directories the move emptied; Remove refuses any other.
	for dir := filepath.Dir(from); dir != "."; dir = filepath.Dir(dir) {
		if info, err := root.Lstat(dir); err != nil || !info.IsDir() || root.Remove(dir) != nil {
			break
		}
		delete(dirs, dir)
	}
	return true, nil
}

// lstatUnderDirs is root.Lstat(rel) that also checks, without following any
// symlink, that every parent of rel is a real directory. A missing parent
// reports fs.ErrNotExist.
func lstatUnderDirs(root *os.Root, rel string, dirs map[string]bool) (os.FileInfo, error) {
	components := strings.Split(rel, string(filepath.Separator))
	for i := 1; i < len(components); i++ {
		dir := filepath.Join(components[:i]...)
		if dirs[dir] {
			continue
		}
		info, err := root.Lstat(dir)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s is not a directory (%s)", dir, info.Mode().Type())
		}
		dirs[dir] = true
	}
	return root.Lstat(rel)
}

// makeDirs creates the missing directories of dir inside root, checking that
// the existing ones are real directories.
func makeDirs(root *os.Root, dir string, dirs map[string]bool) error {
	if dir == "." {
		return nil
	}
	components := strings.Split(dir, string(filepath.Separator))
	for i := 1; i <= len(components); i++ {
		prefix := filepath.Join(components[:i]...)
		if dirs[prefix] {
			continue
		}
		info, err := root.Lstat(prefix)
		if errors.Is(err, fs.ErrNotExist) {
			if err = root.Mkdir(prefix, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
				return err
			}
			info, err = root.Lstat(prefix)
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("%s is not a directory (%s)", prefix, info.Mode().Type())
		}
		dirs[prefix] = true
	}
	return nil
}

func topComponent(rel string) string {
	if i := strings.IndexRune(rel, filepath.Separator); i >= 0 {
		return rel[:i]
	}
	return rel
}
