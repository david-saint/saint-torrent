package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// RemoveFiles deletes a torrent's payload files, given as paths relative to
// baseDir, and then the directories under baseDir they leave empty (including
// directories whose only remaining entries are OS-generated metadata files such
// as macOS .DS_Store / AppleDouble "._*" or Windows Thumbs.db / desktop.ini).
// baseDir itself is kept, and a baseDir that does not exist has nothing to
// delete.
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
// removed counts the payload files actually deleted; files already missing are
// not an error. err joins everything that was refused or failed.
func RemoveFiles(baseDir string, relPaths []string) (removed int, err error) {
	return RemoveFilesKeeping(baseDir, relPaths, nil)
}

// RemoveFilesKeeping behaves like RemoveFiles, except that paths listed in
// keepPaths (relative to baseDir) are never removed during empty-directory
// metadata cleanup — protecting files another active torrent (such as a
// cross-seed) still uses even when their name matches an OS metadata file.
func RemoveFilesKeeping(baseDir string, relPaths, keepPaths []string) (removed int, err error) {
	root, err := openExistingPlatformDownloadRoot(baseDir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open download directory %s: %w", baseDir, err)
	}

	var keep map[string]struct{}
	if len(keepPaths) > 0 {
		keep = make(map[string]struct{}, len(keepPaths))
		for _, kp := range keepPaths {
			clean := filepath.Clean(kp)
			if clean != "." && filepath.IsLocal(clean) {
				keep[pathKey(clean)] = struct{}{}
			}
		}
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

	w := removalWalk{dirs: []*os.Root{root}, keep: keep}
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
// every file under it is skipped without another lookup. keep holds pathKeys of
// files another torrent still uses, which must never be cleaned as OS metadata.
type removalWalk struct {
	dirs    []*os.Root
	names   []string
	blocked []string
	keep    map[string]struct{}
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
		err := parent.Remove(name)
		if err != nil && isDirNotEmpty(err) && w.removeOSMetadataOnlyDir(parent, name) {
			if info, statErr := parent.Lstat(name); statErr != nil || !info.IsDir() {
				continue
			}
			err = parent.Remove(name)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) && !isDirNotEmpty(err) {
			errs = append(errs, fmt.Errorf("remove empty directory %s: %w", filepath.Join(append(slices.Clone(w.names), name)...), err))
		}
	}
	return errs
}

// appleDoubleMagic is the 4-byte RFC 1740 / macOS AppleDouble header at offset 0
// of every "._*" sidecar file macOS writes on non-APFS/HFS+ volumes (exFAT,
// FAT32, NTFS, SMB, NFS).
var appleDoubleMagic = [4]byte{0x00, 0x05, 0x16, 0x07}

// isOSMetadataName reports whether name is a well-known OS-generated folder
// metadata file (macOS Finder or Windows Explorer) that should not keep an
// otherwise empty torrent directory on disk.
func isOSMetadataName(name string) bool {
	return strings.EqualFold(name, ".DS_Store") ||
		strings.EqualFold(name, "Thumbs.db") ||
		strings.EqualFold(name, "ehthumbs.db") ||
		strings.EqualFold(name, "ehthumbs_vista.db") ||
		strings.EqualFold(name, "desktop.ini")
}

// isAppleDoubleFile reports whether name inside dir is a regular macOS
// AppleDouble sidecar file ("._<name>" starting with the 4-byte AppleDouble
// magic header). Checking the magic avoids touching unrelated dotfiles that
// happen to start with "._".
func isAppleDoubleFile(dir *os.Root, name string, info os.FileInfo) bool {
	if !strings.HasPrefix(name, "._") || len(name) <= 2 || info.Size() < int64(len(appleDoubleMagic)) {
		return false
	}
	f, err := openRegularReadOnly(dir, name)
	if err != nil {
		return false
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(info, opened) {
		return false
	}
	var hdr [4]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return false
	}
	return hdr == appleDoubleMagic
}

func (w *removalWalk) isKept(dirName, entryName string) bool {
	if len(w.keep) == 0 {
		return false
	}
	parts := make([]string, 0, len(w.names)+2)
	parts = append(parts, w.names...)
	parts = append(parts, dirName, entryName)
	_, ok := w.keep[pathKey(filepath.Join(parts...))]
	return ok
}

// removeOSMetadataOnlyDir inspects the real directory name under parent and,
// if every entry in it is an unkept regular OS metadata file, removes those
// files and returns true so the caller can retry removing the now-empty
// directory. If any other file, directory, symlink, or kept cross-seed path is
// present, nothing is deleted and it returns false.
func (w *removalWalk) removeOSMetadataOnlyDir(parent *os.Root, name string) bool {
	dir, err := openDirNoFollow(parent, name)
	if err != nil {
		return false
	}
	defer dir.Close()

	f, err := dir.Open(".")
	if err != nil {
		return false
	}

	var junk []string
	for {
		entries, readErr := f.ReadDir(64)
		for _, entry := range entries {
			entryName := entry.Name()
			if w.isKept(name, entryName) {
				_ = f.Close()
				return false
			}
			info, statErr := dir.Lstat(entryName)
			if errors.Is(statErr, fs.ErrNotExist) {
				continue
			}
			if statErr != nil || !info.Mode().IsRegular() {
				_ = f.Close()
				return false
			}
			if !isOSMetadataName(entryName) && !isAppleDoubleFile(dir, entryName, info) {
				_ = f.Close()
				return false
			}
			junk = append(junk, entryName)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = f.Close()
			return false
		}
	}
	if err := f.Close(); err != nil {
		return false
	}

	for _, entryName := range junk {
		info, err := dir.Lstat(entryName)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			return false
		}
		if err := dir.Remove(entryName); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false
		}
	}
	return true
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
