package downloader

import (
	"errors"
	"fmt"
	"hash/maphash"
	"path/filepath"
	"sync"

	"sainttorrent/pkg/storage"
)

// ErrPathInUse reports a torrent whose payload would share an on-disk file with
// another active torrent. Both would write it and corrupt each other's
// verified data, and deleting either with its files would delete the other's.
var ErrPathInUse = errors.New("file is already used by another torrent")

// pathClaimKey identifies one resolved payload path. It is a 128-bit hash of
// storage.PathKey, so a many-file torrent does not keep a second copy of every
// path in memory.
type pathClaimKey [2]uint64

// pathClaimSeeds are fixed for the process, so keys are comparable across calls.
var pathClaimSeeds = [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}

// pathClaim is the torrent holding a path and how many of its sessions do: a
// replaced duplicate add briefly holds the same paths as its successor.
type pathClaim struct {
	infoHash [20]byte
	refs     int
}

// pathClaimKeys returns the claim keys of relPaths under baseDir. The base is
// resolved through symlinks so two spellings of one directory collide. The
// whole path is folded, which can over-report only on a case-sensitive
// filesystem holding two download directories that differ only in case.
func pathClaimKeys(baseDir string, relPaths []string) []pathClaimKey {
	base := resolveClaimBase(baseDir)
	keys := make([]pathClaimKey, len(relPaths))
	for i, rel := range relPaths {
		key := storage.PathKey(filepath.Join(base, rel))
		keys[i] = pathClaimKey{maphash.String(pathClaimSeeds[0], key), maphash.String(pathClaimSeeds[1], key)}
	}
	return keys
}

// resolveClaimBase makes baseDir absolute and resolves the symlinks of its
// longest existing prefix. A download directory the first add is about to
// create does not exist yet when its paths are claimed; resolving only a base
// that exists keyed that add under the unresolved spelling and a later add
// through the resolved one (macOS /tmp is /private/tmp) under another, so the
// two never collided.
func resolveClaimBase(baseDir string) string {
	base := baseDir
	if abs, err := filepath.Abs(base); err == nil {
		base = abs
	}
	var missing []string
	for dir := base; ; {
		if resolved, err := filepath.EvalSymlinks(dir); err == nil {
			return filepath.Join(append([]string{resolved}, missing...)...)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return base
		}
		missing = append([]string{filepath.Base(dir)}, missing...)
		dir = parent
	}
}

// claimPaths reserves files under baseDir for infoHash before any storage is
// built on them. It fails with ErrPathInUse, reserving nothing, when another
// torrent holds one of the paths. The returned release is idempotent.
func (m *TorrentManager) claimPaths(infoHash [20]byte, baseDir string, files []storage.FileInfo) (func(), error) {
	relPaths := make([]string, len(files))
	for i, f := range files {
		relPaths[i] = f.Path
	}
	keys := pathClaimKeys(baseDir, relPaths)

	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	for i, key := range keys {
		if c, ok := m.pathClaims[key]; ok && c.infoHash != infoHash {
			return nil, fmt.Errorf("%w: %q in %s (torrent %x)", ErrPathInUse, relPaths[i], baseDir, c.infoHash)
		}
	}
	m.addClaimsLocked(infoHash, keys)
	return m.releaseFunc(keys), nil
}

// claimFreePaths reserves, for a deletion, the relPaths under baseDir that no
// other torrent holds. Those are returned in free and stay reserved until
// release, so no torrent can be added on them while they are deleted; the
// others are returned in kept.
func (m *TorrentManager) claimFreePaths(infoHash [20]byte, baseDir string, relPaths []string) (free, kept []string, release func()) {
	keys := pathClaimKeys(baseDir, relPaths)
	freeKeys := make([]pathClaimKey, 0, len(keys))

	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	for i, key := range keys {
		if c, ok := m.pathClaims[key]; ok && c.infoHash != infoHash {
			kept = append(kept, relPaths[i])
			continue
		}
		free = append(free, relPaths[i])
		freeKeys = append(freeKeys, key)
	}
	m.addClaimsLocked(infoHash, freeKeys)
	return free, kept, m.releaseFunc(freeKeys)
}

func (m *TorrentManager) addClaimsLocked(infoHash [20]byte, keys []pathClaimKey) {
	if m.pathClaims == nil {
		m.pathClaims = make(map[pathClaimKey]pathClaim, len(keys))
	}
	for _, key := range keys {
		c := m.pathClaims[key]
		c.infoHash = infoHash
		c.refs++
		m.pathClaims[key] = c
	}
}

func (m *TorrentManager) releaseFunc(keys []pathClaimKey) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.claimMu.Lock()
			defer m.claimMu.Unlock()
			for _, key := range keys {
				c, ok := m.pathClaims[key]
				if !ok {
					continue
				}
				if c.refs--; c.refs <= 0 {
					delete(m.pathClaims, key)
				} else {
					m.pathClaims[key] = c
				}
			}
		})
	}
}

// releasePathClaims gives back the payload paths this session reserved with
// its manager. The manager calls it once the session is closed and, for a
// removal, once its files are deleted.
func (s *Session) releasePathClaims() {
	s.mu.Lock()
	release := s.releaseClaims
	s.releaseClaims = nil
	s.mu.Unlock()
	if release != nil {
		release()
	}
}
