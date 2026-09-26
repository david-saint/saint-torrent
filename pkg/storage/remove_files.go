package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// RemoveFiles deletes a torrent's payload files, given as paths relative to
// baseDir, and then the directories under baseDir they leave empty. baseDir
// itself is kept, and a baseDir that does not exist has nothing to delete.
//
// Every operation is anchored to a handle on baseDir that is opened without
// creating anything. Each directory is checked with a no-follow Lstat and
// opened relative to its already-verified parent, so a symlink or Windows
// junction planted in the tree, or a directory swapped for one mid-removal,
// can neither lead outside baseDir nor redirect a delete within it. Only
// regular files are removed: symlinks, junctions (which Windows reports as
// irregular) and other special files are refused and reported. Paths are
// processed in sorted order with only the current chain of directories open,
// so each directory is validated once however many files it holds.
//
// removed counts the files actually deleted; files already missing are not an
// error. err joins everything that was refused or failed.
func RemoveFiles(baseDir string, relPaths []string) (removed int, err error) {
	root, err := openExistingPlatformDownloadRoot(baseDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open download directory %s: %w", baseDir, err)
	}

	var errs []error
	paths := make([][]string, 0, len(relPaths))
	for _, relPath := range relPaths {
		clean := filepath.Clean(relPath)
		if checkNoTrailingSeparator(relPath) != nil || clean == "." || !filepath.IsLocal(clean) {
			errs = append(errs, fmt.Errorf("refusing to delete %q: not a path inside the download directory", relPath))
			continue
		}
		paths = append(paths, strings.Split(clean, string(filepath.Separator)))
	}
	// Component-wise order keeps every directory's files contiguous, so the walk
	// below opens each directory once and leaves it for good.
	slices.SortFunc(paths, slices.Compare)

	w := removalWalk{dirs: []*os.Root{root}}
	for _, components := range paths {
		dir, name := components[:len(components)-1], components[len(components)-1]
		parent, err := w.enter(dir)
		if err != nil {
			errs = append(errs, err)
		}
		if parent == nil {
			continue // under a directory that is missing or was refused
		}
		info, err := parent.Lstat(name)
		switch {
		case errors.Is(err, fs.ErrNotExist):
			continue
		case err != nil:
			errs = append(errs, fmt.Errorf("inspect %s: %w", filepath.Join(components...), err))
			continue
		case !info.Mode().IsRegular():
			errs = append(errs, fmt.Errorf("refusing to delete %s: not a regular file (%s)", filepath.Join(components...), info.Mode().Type()))
			continue
		}
		if err := parent.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, fmt.Errorf("delete %s: %w", filepath.Join(components...), err))
			continue
		}
		removed++
	}
	errs = append(errs, w.leave(0)...)
	if err := root.Close(); err != nil {
		errs = append(errs, err)
	}
	return removed, errors.Join(errs...)
}

// removalWalk keeps the chain of verified directory handles from the download
// root to the directory of the file being removed. dirs[0] is the root and
// dirs[i] is names[:i]. blocked is a directory that is missing or was refused;
// every file under it is skipped without another lookup.
type removalWalk struct {
	dirs    []*os.Root
	names   []string
	blocked []string
}

// enter moves the walk to dir and returns its handle, or nil when dir lies
// under a missing or refused directory.
func (w *removalWalk) enter(dir []string) (*os.Root, error) {
	if w.blocked != nil && len(dir) >= len(w.blocked) && slices.Equal(dir[:len(w.blocked)], w.blocked) {
		return nil, nil
	}
	w.blocked = nil
	common := 0
	for common < len(w.names) && common < len(dir) && w.names[common] == dir[common] {
		common++
	}
	var errs []error
	if leaveErrs := w.leave(common); len(leaveErrs) > 0 {
		errs = append(errs, leaveErrs...)
	}
	for _, name := range dir[common:] {
		parent := w.dirs[len(w.dirs)-1]
		next, err := openDirNoFollow(parent, name)
		if err != nil {
			w.blocked = slices.Clone(dir[:len(w.names)+1])
			if !errors.Is(err, fs.ErrNotExist) {
				errs = append(errs, fmt.Errorf("refusing to delete under %s: %w", filepath.Join(w.blocked...), err))
			}
			return nil, errors.Join(errs...)
		}
		w.dirs = append(w.dirs, next)
		w.names = append(w.names, name)
	}
	return w.dirs[len(w.dirs)-1], errors.Join(errs...)
}

// leave closes the directories deeper than depth, deepest first, and removes
// each one it can: a directory holding anything else is left in place.
func (w *removalWalk) leave(depth int) []error {
	var errs []error
	for len(w.names) > depth {
		last := len(w.names) - 1
		name := w.names[last]
		if err := w.dirs[last+1].Close(); err != nil {
			errs = append(errs, err)
		}
		w.dirs = w.dirs[:last+1]
		w.names = w.names[:last]
		parent := w.dirs[last]
		// Re-check the entry: only an empty real directory is removed, never
		// whatever may have been swapped in under its name.
		if info, err := parent.Lstat(name); err != nil || !info.IsDir() {
			continue
		}
		if err := parent.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) && !isDirNotEmpty(err) {
			errs = append(errs, fmt.Errorf("remove empty directory %s: %w", filepath.Join(append(slices.Clone(w.names), name)...), err))
		}
	}
	return errs
}

// openDirNoFollow opens the directory name beneath parent, refusing anything
// that is not a real directory at the time it is opened: a symlink, a Windows
// junction (reported as irregular, never as a directory), or an entry swapped
// between the check and the open.
func openDirNoFollow(parent *os.Root, name string) (*os.Root, error) {
	info, err := parent.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory (%s)", name, info.Mode().Type())
	}
	dir, err := parent.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	opened, err := dir.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		_ = dir.Close()
		if err == nil {
			err = errUnsafeRootPath
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return dir, nil
}
