package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sainttorrent/pkg/downloader"
	"sainttorrent/pkg/storage"
)

// downloadPathOptions is an ordered set of download directories. The primary
// directory is always tried first, followed by each configured fallback.
type downloadPathOptions struct {
	primary   string
	fallbacks []string
}

func (p downloadPathOptions) candidates() []string {
	paths := make([]string, 0, 1+len(p.fallbacks))
	seen := make(map[string]struct{}, 1+len(p.fallbacks))
	for _, candidate := range append([]string{p.primary}, p.fallbacks...) {
		if candidate == "" {
			continue
		}
		absCandidate, err := filepath.Abs(candidate)
		if err == nil {
			candidate = absCandidate
		}
		candidate = filepath.Clean(candidate)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		paths = append(paths, candidate)
	}
	return paths
}

func (p downloadPathOptions) normalized() downloadPathOptions {
	candidates := p.candidates()
	if len(candidates) == 0 {
		return downloadPathOptions{}
	}
	var fallbacks []string
	if p.fallbacks != nil {
		fallbacks = append([]string{}, candidates[1:]...)
	}
	return downloadPathOptions{
		primary:   candidates[0],
		fallbacks: fallbacks,
	}
}

// selectDownloadPath returns the first directory that can be created and
// durably written. It runs only when adding a torrent, never on the piece I/O
// hot path.
func selectDownloadPath(paths downloadPathOptions) (string, error) {
	var pathErrors []error
	for _, candidate := range paths.candidates() {
		if err := probeDownloadPath(candidate); err != nil {
			pathErrors = append(pathErrors, fmt.Errorf("%s: %w", candidate, err))
			continue
		}
		return candidate, nil
	}

	return "", downloadPathError(pathErrors)
}

func downloadPathError(pathErrors []error) error {
	if len(pathErrors) == 0 {
		return fmt.Errorf("no download directories configured")
	}
	return fmt.Errorf("no usable download directory: %w", errors.Join(pathErrors...))
}

func probeDownloadPath(path string) error {
	root, err := storage.OpenDownloadRoot(path)
	if err != nil {
		return fmt.Errorf("open download directory: %w", err)
	}
	defer root.Close()

	var randomSuffix [8]byte
	if _, err := rand.Read(randomSuffix[:]); err != nil {
		return fmt.Errorf("create write test name: %w", err)
	}
	probeName := fmt.Sprintf(".sainttorrent-write-test-%x", randomSuffix)
	probe, err := root.OpenFile(probeName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("create write test: %w", err)
	}
	defer root.Remove(probeName)

	if _, err := probe.Write([]byte{0}); err != nil {
		_ = probe.Close()
		return fmt.Errorf("write test: %w", err)
	}
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		return fmt.Errorf("sync write test: %w", err)
	}
	if err := probe.Close(); err != nil {
		return fmt.Errorf("close write test: %w", err)
	}
	return nil
}

func socketDownloadPaths(msg socketMessage, defaults downloadPathOptions) downloadPathOptions {
	if msg.DownloadDir == "" {
		return defaults
	}
	fallbacks := msg.FallbackDownloadDirs
	if fallbacks == nil {
		fallbacks = defaults.fallbacks
	}
	return downloadPathOptions{
		primary:   msg.DownloadDir,
		fallbacks: fallbacks,
	}
}

type torrentAdder interface {
	AddMagnet(string, string) (*downloader.Session, error)
	AddTorrentFile(string, string) (*downloader.Session, error)
	GetSession(string) *downloader.Session
}

func addTorrentWithDownloadPaths(mgr torrentAdder, item string, paths downloadPathOptions) (*downloader.Session, error) {
	// Validate the source before touching any candidate directories. Storage
	// initialization can still fail for a specific disk (for example when it
	// fills between the write probe and file preallocation), in which case the
	// next usable fallback is tried.
	_, infoHash, err := parseItem(item)
	if err != nil {
		return nil, err
	}

	var pathErrors []error
	candidates := paths.candidates()
	isMagnet := strings.HasPrefix(item, "magnet:?")
	if isMagnet {
		if existing := mgr.GetSession(infoHash); existing != nil {
			existing.MergeFallbackDownloadDirs(candidates)
			return existing, nil
		}
	}
	for index, downloadDir := range candidates {
		if err := probeDownloadPath(downloadDir); err != nil {
			pathErrors = append(pathErrors, fmt.Errorf("%s: %w", downloadDir, err))
			continue
		}

		var (
			sess *downloader.Session
			err  error
		)
		if isMagnet {
			sess, err = mgr.AddMagnet(item, downloadDir)
		} else {
			sess, err = mgr.AddTorrentFile(item, downloadDir)
		}
		if err == nil {
			if isMagnet {
				if filepath.Clean(sess.DownloadDir()) == filepath.Clean(downloadDir) {
					sess.MergeFallbackDownloadDirs(candidates[index+1:])
				} else {
					// Another add may have won the race. Preserve its existing path
					// chain and merge every configured candidate into it.
					sess.MergeFallbackDownloadDirs(candidates)
				}
			}
			return sess, nil
		}
		pathErrors = append(pathErrors, fmt.Errorf("%s: initialize torrent storage: %w", downloadDir, err))
	}
	return nil, downloadPathError(pathErrors)
}
