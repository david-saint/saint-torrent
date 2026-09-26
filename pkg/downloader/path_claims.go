package downloader

import (
	"errors"
	"fmt"
	"hash/maphash"
	"path/filepath"
	"sync"

	"sainttorrent/pkg/logging"
	"sainttorrent/pkg/storage"
)

// ErrPathInUse reports a torrent whose payload would share an on-disk file with
// another active torrent that declares it with a different length, or with one
// whose files are being deleted. The second would resize the file and both
// would overwrite each other's verified data. Torrents declaring the same file
// with the same length may share it: that is cross-seeding, one payload
// published under several info-hashes.
var ErrPathInUse = errors.New("file is already used by another torrent")

// pathClaimKey identifies one resolved payload path. It is a 128-bit hash of
// storage.PathKey, so a many-file torrent does not keep a second copy of every
// path in memory.
type pathClaimKey [2]uint64

// pathClaimSeeds are fixed for the process, so keys are comparable across calls.
var pathClaimSeeds = [2]maphash.Seed{maphash.MakeSeed(), maphash.MakeSeed()}

// pathClaim is who holds one payload path. The first torrent to hold it is
// recorded inline, so the common unshared path costs one pointer-free map
// entry. Torrents cross-seeding it are counted in shared and recorded in
// TorrentManager.sharedClaims. A torrent holds a path more than once while a
// replaced duplicate add and its successor both do.
type pathClaim struct {
	length   int64    // the declared length every holder agreed on
	infoHash [20]byte // the first holder, holding it while refs > 0
	refs     int32    // claims by infoHash
	shared   int32    // claims by other torrents, in sharedClaims
	deleting int32    // removals deleting the file; nobody else joins meanwhile
}

// sharedClaimKey is a cross-seeding torrent's hold on a path.
type sharedClaimKey struct {
	key      pathClaimKey
	infoHash [20]byte
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

// ownRefsLocked returns how many claims infoHash holds on c, the claim at key.
// Caller holds m.claimMu.
func (m *TorrentManager) ownRefsLocked(key pathClaimKey, c pathClaim, infoHash [20]byte) int32 {
	if c.refs > 0 && c.infoHash == infoHash {
		return c.refs
	}
	if c.shared == 0 {
		return 0
	}
	return m.sharedClaims[sharedClaimKey{key, infoHash}]
}

// claimPaths reserves files under baseDir for infoHash before any storage is
// built on them. It fails with ErrPathInUse, reserving nothing, when another
// torrent holds one of the paths with a different length or is deleting it. A
// path another torrent holds with the same length is shared (cross-seeding)
// and logged. The returned release is idempotent.
func (m *TorrentManager) claimPaths(infoHash [20]byte, baseDir string, files []storage.FileInfo) (func(), error) {
	relPaths := make([]string, len(files))
	for i, f := range files {
		relPaths[i] = f.Path
	}
	keys := pathClaimKeys(baseDir, relPaths)

	sharedWith, err := m.addHolds(infoHash, keys, files, relPaths, baseDir)
	if err != nil {
		return nil, err
	}
	if sharedWith > 0 && logging.Enabled() {
		logging.Warn("payload_files_shared",
			logging.String("info_hash", fmt.Sprintf("%x", infoHash)),
			logging.String("download_dir", baseDir),
			logging.Int("files", sharedWith),
		)
	}
	return m.releaseHoldFunc(infoHash, keys), nil
}

// addHolds records infoHash's claims on keys, all or none, and returns how many
// of the paths it now shares with another torrent.
func (m *TorrentManager) addHolds(infoHash [20]byte, keys []pathClaimKey, files []storage.FileInfo, relPaths []string, baseDir string) (sharedWith int, err error) {
	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	for i, key := range keys {
		c, ok := m.pathClaims[key]
		if !ok || m.ownRefsLocked(key, c, infoHash) > 0 {
			continue
		}
		if c.deleting > 0 || (c.refs+c.shared > 0 && c.length != files[i].Length) {
			owner := ""
			if c.refs > 0 {
				owner = fmt.Sprintf(" (torrent %x)", c.infoHash)
			}
			return 0, fmt.Errorf("%w: %q in %s%s", ErrPathInUse, relPaths[i], baseDir, owner)
		}
		if c.refs+c.shared > 0 {
			sharedWith++
		}
	}
	for i, key := range keys {
		m.addHoldLocked(key, infoHash, files[i].Length)
	}
	return sharedWith, nil
}

// addHoldLocked records one claim by infoHash on key. Caller holds m.claimMu
// and has checked that the claim is allowed.
func (m *TorrentManager) addHoldLocked(key pathClaimKey, infoHash [20]byte, length int64) {
	if m.pathClaims == nil {
		m.pathClaims = make(map[pathClaimKey]pathClaim)
	}
	c := m.pathClaims[key]
	switch {
	case c.refs > 0 && c.infoHash == infoHash:
		c.refs++
	case c.shared > 0 && m.sharedClaims[sharedClaimKey{key, infoHash}] > 0:
		m.sharedClaims[sharedClaimKey{key, infoHash}]++
		c.shared++
	case c.refs == 0:
		// The inline slot is free: take it. Any other holder is a cross-seed
		// of the same length, so the length stands either way.
		c.infoHash = infoHash
		c.refs = 1
		c.length = length
	default:
		if m.sharedClaims == nil {
			m.sharedClaims = make(map[sharedClaimKey]int32)
		}
		m.sharedClaims[sharedClaimKey{key, infoHash}]++
		c.shared++
	}
	m.pathClaims[key] = c
}

// storeClaimLocked writes c back, dropping it once nobody holds or deletes
// the path. Caller holds m.claimMu.
func (m *TorrentManager) storeClaimLocked(key pathClaimKey, c pathClaim) {
	if c.refs == 0 && c.shared == 0 && c.deleting == 0 {
		delete(m.pathClaims, key)
		return
	}
	m.pathClaims[key] = c
}

func (m *TorrentManager) releaseHoldFunc(infoHash [20]byte, keys []pathClaimKey) func() {
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
				if c.refs > 0 && c.infoHash == infoHash {
					c.refs--
				} else if hold := (sharedClaimKey{key, infoHash}); c.shared > 0 && m.sharedClaims[hold] > 0 {
					if m.sharedClaims[hold]--; m.sharedClaims[hold] == 0 {
						delete(m.sharedClaims, hold)
					}
					c.shared--
				}
				m.storeClaimLocked(key, c)
			}
		})
	}
}

// claimFreePaths reserves, for a deletion, the relPaths under baseDir that no
// torrent but infoHash holds. Those are returned in free and stay reserved
// until release: no other torrent can be added on them, not even a cross-seed,
// while they are deleted. The paths another torrent still uses are returned in
// kept and must not be deleted.
func (m *TorrentManager) claimFreePaths(infoHash [20]byte, baseDir string, relPaths []string) (free, kept []string, release func()) {
	keys := pathClaimKeys(baseDir, relPaths)
	freeKeys := make([]pathClaimKey, 0, len(keys))

	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	for i, key := range keys {
		c := m.pathClaims[key]
		if c.refs+c.shared > m.ownRefsLocked(key, c, infoHash) {
			kept = append(kept, relPaths[i])
			continue
		}
		free = append(free, relPaths[i])
		freeKeys = append(freeKeys, key)
		m.markDeletingLocked(key, c)
	}
	return free, kept, m.releaseDeletingFunc(freeKeys)
}

// claimUnheldPaths reserves, for moving files away, the relPaths under baseDir
// that no torrent holds or is deleting, the caller included, and returns their
// indexes. As for a deletion, no torrent can be added on them until release.
func (m *TorrentManager) claimUnheldPaths(baseDir string, relPaths []string) (free []int, release func()) {
	keys := pathClaimKeys(baseDir, relPaths)
	freeKeys := make([]pathClaimKey, 0, len(keys))

	m.claimMu.Lock()
	defer m.claimMu.Unlock()
	for i, key := range keys {
		// A path listed twice is reserved by its first entry, then held.
		if c := m.pathClaims[key]; c.refs+c.shared+c.deleting == 0 {
			free = append(free, i)
			freeKeys = append(freeKeys, key)
			m.markDeletingLocked(key, c)
		}
	}
	return free, m.releaseDeletingFunc(freeKeys)
}

// markDeletingLocked records c, the claim at key, as reserved by one more
// removal or move. Caller holds m.claimMu.
func (m *TorrentManager) markDeletingLocked(key pathClaimKey, c pathClaim) {
	c.deleting++
	if m.pathClaims == nil {
		m.pathClaims = make(map[pathClaimKey]pathClaim)
	}
	m.pathClaims[key] = c
}

// releaseDeletingFunc returns the idempotent release of the reservations
// markDeletingLocked made on keys.
func (m *TorrentManager) releaseDeletingFunc(keys []pathClaimKey) func() {
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
				c.deleting--
				m.storeClaimLocked(key, c)
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
