// Package torrent parses and processes bencoded torrent metadata and magnet URIs.
package torrent

import (
	"crypto/sha1"
	"fmt"
	"path/filepath"
	"strings"

	"sainttorrent/pkg/bencode"
)

// Metainfo limits. Each sits well above what real torrent creators emit, so no
// legitimate swarm is refused, yet low enough that the buffers and per-piece
// state sized from metainfo stay allocatable on every platform, 32-bit
// included. All comparisons are done in int64 so GOARCH=386 behaves like amd64.
const (
	// MaxPieceLength bounds "piece length". Piece buffers (assembly, web seeds)
	// are allocated at this size; creators top out at 16-256 MiB.
	MaxPieceLength = 256 << 20
	// MaxTotalLength bounds each file and the torrent's total size, matching
	// libtorrent's max_file_offset.
	MaxTotalLength int64 = 1 << 48
	// MaxPieceCount bounds the number of pieces, which sizes the per-torrent
	// piece state and every peer's bitfield.
	MaxPieceCount = 1 << 22
	// MaxFileCount bounds the number of files. Storage creates, stats and
	// tracks every file up front, so an info dictionary of millions of empty
	// entries would pin that much work and memory; the largest real datasets
	// hold a few hundred thousand files.
	MaxFileCount = 1 << 20
)

// maxPathDepth bounds the number of path components one file may declare.
const maxPathDepth = 128

// File represents an individual file and its size in a multi-file torrent.
type File struct {
	Length int64
	Path   []string
	// LegacyPath is where versions before the current name sanitizer laid the
	// file out, set only when that differs from Path (see
	// legacyPathComponent). The downloader moves a file it finds there to
	// Path, so upgrading does not orphan it and download it again.
	LegacyPath []string
	// BEP 47 file attributes ("attr"). Pad marks a pad file: Length zero bytes
	// that align the next file to a piece boundary, which are never fetched or
	// created on disk and are exempt from the duplicate-path checks (libtorrent
	// names them ".pad/<size>", so equal sizes collide). Only a multi-file
	// torrent can have one. Exec, Hidden and Symlink are recorded but inert:
	// nothing is ever applied to disk from them.
	Pad     bool
	Exec    bool
	Hidden  bool
	Symlink bool
}

// Torrent represents the metadata extracted from a torrent file.
type Torrent struct {
	Announce    string
	Trackers    []string
	WebSeeds    []string
	InfoHash    [20]byte
	PieceLength int64
	PieceHashes [][20]byte
	Name        string // Display name, sanitized like a file name and at most 255 bytes
	Files       []File
	InfoBytes   []byte // Raw bencoded info dictionary
	Private     bool   // BEP 27 private torrents must use trackers only
}

// Parse decodes a bencoded torrent file, calculates the info hash, and returns a Torrent struct.
func Parse(data []byte) (*Torrent, error) {
	// The outer dictionary is decoded like libtorrent's: a repeated key keeps
	// its first value, as FindRawValue does, so a .torrent whose editor
	// appended a second "comment" or "announce" still loads. Those keys are not
	// hashed. The info dictionary, which is, is decoded strictly by ParseInfo
	// from the exact bytes the info-hash commits to.
	val, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal torrent bencode: %w", err)
	}

	dict, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("torrent file is not a bencoded dictionary")
	}

	// 1. Announce / Trackers
	var announce string
	if raw, ok := getString(dict, "announce"); ok {
		announce, _ = normalizeURL(raw, true, MaxTrackerURLLength)
	}

	trackers := newURLSet(maxTrackers, true, MaxTrackerURLLength)
	if announceList, ok := dict["announce-list"].([]interface{}); ok {
	tiers:
		for _, tierVal := range announceList {
			if tier, ok := tierVal.([]interface{}); ok {
				for _, trackerVal := range tier {
					if trackerStr, ok := trackerVal.(string); ok {
						if !trackers.add(trackerStr) {
							break tiers
						}
					}
				}
			}
		}
	}

	// Fallback to announce if no trackers were extracted from announce-list
	if len(trackers.list) == 0 && announce != "" {
		trackers.list = []string{announce}
	}

	webSeeds := newURLSet(maxWebSeeds, false, MaxWebSeedURLLength)
	switch v := dict["url-list"].(type) {
	case string:
		webSeeds.add(v)
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok && !webSeeds.add(s) {
				break
			}
		}
	}

	// 2. Info Dictionary
	infoVal, ok := dict["info"]
	if !ok {
		return nil, fmt.Errorf("missing info dictionary")
	}
	if _, ok := infoVal.(map[string]interface{}); !ok {
		return nil, fmt.Errorf("invalid info field type")
	}

	// 3. Decode the info fields from the exact bytes that are hashed, so the
	// info-hash always commits to the content that gets downloaded. A second
	// "info" is refused: a reader keeping the last copy would hash and
	// download another torrent from the same file.
	bencodedInfo, err := bencode.FindUniqueRawValue(data, "info")
	if err != nil {
		return nil, fmt.Errorf("failed to extract raw info dictionary: %w", err)
	}
	tor, err := ParseInfo(bencodedInfo)
	if err != nil {
		return nil, err
	}
	tor.Announce = announce
	tor.Trackers = trackers.list
	tor.WebSeeds = webSeeds.list
	return tor, nil
}

// ParseInfo parses a bare info dictionary, such as metadata fetched with
// ut_metadata (BEP 9). infoBytes must be exactly one bencoded dictionary with
// nothing after it. The info-hash is the SHA-1 of exactly those bytes, and the
// returned Torrent keeps infoBytes as its InfoBytes, so the caller must not
// modify the slice afterwards. Trackers and web seeds live outside the info
// dictionary and are left empty.
func ParseInfo(infoBytes []byte) (*Torrent, error) {
	val, err := bencode.UnmarshalStrict(infoBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal info dictionary: %w", err)
	}
	info, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("info is not a bencoded dictionary")
	}

	name, ok := getString(info, "name")
	if !ok {
		return nil, fmt.Errorf("missing or invalid name in info dict")
	}

	pieceLength, ok := getInt64(info, "piece length")
	if !ok {
		return nil, fmt.Errorf("missing or invalid piece length in info dict")
	}
	if pieceLength <= 0 {
		return nil, fmt.Errorf("piece length must be positive, got %d", pieceLength)
	}
	if pieceLength > MaxPieceLength {
		return nil, fmt.Errorf("piece length %d exceeds the maximum of %d", pieceLength, MaxPieceLength)
	}

	piecesStr, ok := getString(info, "pieces")
	if !ok {
		return nil, fmt.Errorf("missing or invalid pieces in info dict")
	}
	if len(piecesStr)%20 != 0 {
		return nil, fmt.Errorf("pieces length must be a multiple of 20, got %d", len(piecesStr))
	}
	numPieces := len(piecesStr) / 20
	if numPieces == 0 {
		return nil, fmt.Errorf("torrent must contain at least one piece hash")
	}

	// Any nonzero value marks the torrent private, as in libtorrent: treating
	// a non-standard value as public would leak it to DHT and PEX.
	private := false
	if privateFlag, ok := getInt64(info, "private"); ok {
		private = privateFlag != 0
	}

	// Files list (handling single-file vs multi-file)
	var files []File
	var totalLength int64
	if filesVal, ok := info["files"]; ok {
		// Multi-file mode
		filesSlice, ok := filesVal.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid files field format in info dict")
		}
		if len(filesSlice) == 0 {
			return nil, fmt.Errorf("files list cannot be empty")
		}
		if err := checkFileCount(len(filesSlice)); err != nil {
			return nil, err
		}
		cleanName := sanitizePathComponent(name)
		legacyName, legacyRoot := legacyComponent(name, cleanName)
		for _, fVal := range filesSlice {
			fMap, ok := fVal.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid file entry in files list")
			}
			length, ok := getInt64(fMap, "length")
			if !ok {
				return nil, fmt.Errorf("missing or invalid length in files list entry")
			}
			if err := checkFileLength(length); err != nil {
				return nil, err
			}
			// Both terms are at most MaxTotalLength, so the sum cannot overflow.
			if totalLength > MaxTotalLength-length {
				return nil, fmt.Errorf("torrent total length exceeds the maximum of %d", MaxTotalLength)
			}
			totalLength += length
			pathSlice, ok := fMap["path"].([]interface{})
			if !ok {
				return nil, fmt.Errorf("missing or invalid path in files list entry")
			}
			if len(pathSlice) > maxPathDepth {
				return nil, fmt.Errorf("file path has %d components, more than the maximum of %d", len(pathSlice), maxPathDepth)
			}
			path := make([]string, 0, len(pathSlice)+1)
			path = append(path, cleanName) // root under sanitized torrent name
			legacyDiffers := legacyRoot
			for _, pVal := range pathSlice {
				pStr, ok := pVal.(string)
				if !ok {
					return nil, fmt.Errorf("invalid path component in file path")
				}
				cleanedComp := sanitizePathComponent(pStr)
				if cleanedComp != "" {
					path = append(path, cleanedComp)
				}
				if _, differs := legacyComponent(pStr, cleanedComp); differs {
					legacyDiffers = true
				}
			}
			if len(path) == 1 { // Only contains the torrent name, no actual files
				path = append(path, "unknown_file")
			}
			file := File{
				Length: length,
				Path:   path,
			}
			parseFileAttr(fMap, &file)
			if !file.Pad && legacyDiffers {
				file.LegacyPath = legacyFilePath(legacyName, pathSlice)
			}
			files = append(files, file)
		}
	} else {
		// Single-file mode
		length, ok := getInt64(info, "length")
		if !ok {
			return nil, fmt.Errorf("missing or invalid length in single-file mode")
		}
		if err := checkFileLength(length); err != nil {
			return nil, err
		}
		totalLength = length
		cleanName := sanitizePathComponent(name)
		file := File{
			Length: length,
			Path:   []string{cleanName},
		}
		parseFileAttr(info, &file)
		// A single-file torrent's one file is its whole payload: honouring "p"
		// there would make a torrent with nothing to download.
		file.Pad = false
		if legacyName, differs := legacyComponent(name, cleanName); differs && len(legacyName) <= maxLegacyComponentBytes {
			file.LegacyPath = []string{legacyName}
		}
		files = []File{file}
	}
	if totalLength <= 0 {
		return nil, fmt.Errorf("torrent total length must be positive")
	}
	if err := checkPieceCount(numPieces, pieceLength, totalLength); err != nil {
		return nil, err
	}
	pieceHashes := make([][20]byte, numPieces)
	for i := range pieceHashes {
		copy(pieceHashes[i][:], piecesStr[i*20:])
	}
	if err := checkPadFiles(files, pieceLength, totalLength, pieceHashes); err != nil {
		return nil, err
	}
	// Reject paths that differ only in case or Unicode normalization: case- or
	// normalization-insensitive filesystems would store them as one file. Pad
	// files (BEP 47) are never created on disk, and libtorrent 2.x deliberately
	// lets equal-sized ".pad/<size>" files collide, so they are exempt.
	seenPaths := make(map[string]struct{}, len(files))
	keys := make([]string, len(files))
	for i, f := range files {
		if f.Pad {
			continue
		}
		relPath := filepath.Join(f.Path...)
		key := pathKey(relPath)
		if _, dup := seenPaths[key]; dup {
			return nil, fmt.Errorf("duplicate file path detected in torrent metadata: %q", relPath)
		}
		seenPaths[key] = struct{}{}
		keys[i] = key
	}
	if err := checkFileDirCollisions(files, keys); err != nil {
		return nil, err
	}

	return &Torrent{
		InfoHash:    sha1.Sum(infoBytes),
		PieceLength: pieceLength,
		PieceHashes: pieceHashes,
		Name:        sanitizeName(name),
		Files:       files,
		InfoBytes:   infoBytes,
		Private:     private,
	}, nil
}

// pathEntry names a file or directory in a torrent's folded path tree: a
// folded path component inside the directory numbered parent (0 is the root).
type pathEntry struct {
	parent int
	name   string
}

// checkFileDirCollisions rejects a file whose path is also the directory of
// another file, as in [a] and [a b]: no filesystem can lay that out.
// libtorrent renames such a file (within max_duplicate_filenames attempts);
// here it is refused like a duplicate path. keys are the files' pathKey
// values, so case and normalization fold exactly as in the duplicate check;
// folding neither adds nor removes a separator, and sanitized components hold
// none. Each directory component is hashed once, under its parent's number,
// so the cost is linear in the path bytes: keying every directory by its
// whole path would hash and keep O(depth^2) bytes per file, 64 times the
// metadata for 128-deep paths.
func checkFileDirCollisions(files []File, keys []string) error {
	if len(files) < 2 {
		return nil
	}
	sep := string(filepath.Separator)
	dirs := make(map[pathEntry]int)
	leaves := make([]pathEntry, len(files))
	for i, key := range keys {
		if files[i].Pad {
			continue
		}
		parent, rest := 0, key
		for {
			name, tail, ok := strings.Cut(rest, sep)
			if !ok {
				break
			}
			dir := pathEntry{parent: parent, name: name}
			id, seen := dirs[dir]
			if !seen {
				id = len(dirs) + 1
				dirs[dir] = id
			}
			parent, rest = id, tail
		}
		leaves[i] = pathEntry{parent: parent, name: rest}
	}
	for i, leaf := range leaves {
		if files[i].Pad {
			continue
		}
		if _, isDir := dirs[leaf]; isDir {
			return fmt.Errorf("file path %q is also a directory in torrent metadata", filepath.Join(files[i].Path...))
		}
	}
	return nil
}

// checkPieceCount rejects a piece count above MaxPieceCount or one that does
// not match the declared lengths. The comparison stays in int64: converting to
// int first truncates on 32-bit builds and let one hash "cover" terabytes of
// data that would never be verified.
func checkPieceCount(numPieces int, pieceLength, totalLength int64) error {
	if numPieces > MaxPieceCount {
		return fmt.Errorf("torrent has %d pieces, more than the maximum of %d", numPieces, MaxPieceCount)
	}
	expectedPieces := (totalLength-1)/pieceLength + 1
	if int64(numPieces) != expectedPieces {
		return fmt.Errorf("piece hash count mismatch: got %d, expected %d", numPieces, expectedPieces)
	}
	return nil
}

// checkFileCount rejects a files list longer than MaxFileCount.
func checkFileCount(n int) error {
	if n > MaxFileCount {
		return fmt.Errorf("torrent has %d files, more than the maximum of %d", n, MaxFileCount)
	}
	return nil
}

// checkFileLength rejects a negative or implausibly large file length.
func checkFileLength(length int64) error {
	if length < 0 {
		return fmt.Errorf("file length cannot be negative: %d", length)
	}
	if length > MaxTotalLength {
		return fmt.Errorf("file length %d exceeds the maximum of %d", length, MaxTotalLength)
	}
	return nil
}

// parseFileAttr decodes BEP 47 file attribute flags from m["attr"]. Only "p"
// affects client behavior (virtual zero padding); "x", "h" and "l" are
// recorded as inert metadata flags and never applied to disk, and unknown
// flags are ignored as BEP 47 specifies.
func parseFileAttr(m map[string]interface{}, f *File) {
	attr, ok := getString(m, "attr")
	if !ok {
		return
	}
	for i := 0; i < len(attr); i++ {
		switch attr[i] {
		case 'p':
			f.Pad = true
		case 'x':
			f.Exec = true
		case 'h':
			f.Hidden = true
		case 'l':
			f.Symlink = true
		}
	}
}

// PadSpan is a run of BEP 47 pad bytes, [Start, End) in torrent byte offsets.
type PadSpan struct {
	Start, End int64
}

// PadSpans returns the byte ranges files' pad files cover, in offset order,
// with adjacent pad files merged into one span. It returns nil when no pad
// file has any length, which is every torrent but aligned and hybrid ones.
func PadSpans(files []File) []PadSpan {
	var spans []PadSpan
	var offset int64
	for _, f := range files {
		if f.Pad && f.Length > 0 {
			if n := len(spans); n > 0 && spans[n-1].End == offset {
				spans[n-1].End += f.Length
			} else {
				spans = append(spans, PadSpan{Start: offset, End: offset + f.Length})
			}
		}
		offset += f.Length
	}
	return spans
}

// checkPadFiles refuses torrents whose pad files cannot be honoured. Pad bytes
// are zeros that are never fetched, so a piece lying entirely in pad files is
// complete from the start; if its hash is not that of zeros it could never
// verify, and the torrent would sit below 100% forever with nothing to fetch.
// libtorrent's pad files are shorter than a piece, so only a crafted torrent
// has such a piece. A torrent that is nothing but pad files has no payload.
func checkPadFiles(files []File, pieceLength, totalLength int64, pieceHashes [][20]byte) error {
	spans := PadSpans(files)
	if spans == nil {
		return nil
	}
	var padBytes int64
	for _, span := range spans {
		padBytes += span.End - span.Start
	}
	if padBytes >= totalLength {
		return fmt.Errorf("torrent has no payload: every file is a BEP 47 pad file")
	}
	var zeroHashes map[int64][20]byte // by piece length; at most two
	for _, span := range spans {
		for piece := (span.Start + pieceLength - 1) / pieceLength; piece < int64(len(pieceHashes)); piece++ {
			start := piece * pieceLength
			end := min(start+pieceLength, totalLength)
			if end > span.End {
				break
			}
			length := end - start
			zero, ok := zeroHashes[length]
			if !ok {
				zero = zeroPieceHash(length)
				if zeroHashes == nil {
					zeroHashes = make(map[int64][20]byte, 2)
				}
				zeroHashes[length] = zero
			}
			if pieceHashes[piece] != zero {
				return fmt.Errorf("piece %d lies entirely in BEP 47 pad files, but its hash is not that of zeros", piece)
			}
		}
	}
	return nil
}

// zeroPieceHash returns the SHA-1 of length zero bytes.
func zeroPieceHash(length int64) [20]byte {
	var zeros [32 << 10]byte
	h := sha1.New()
	for length > 0 {
		n := min(length, int64(len(zeros)))
		_, _ = h.Write(zeros[:n])
		length -= n
	}
	var sum [20]byte
	copy(sum[:], h.Sum(nil))
	return sum
}

func getString(m map[string]interface{}, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func getInt64(m map[string]interface{}, key string) (int64, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	i, ok := v.(int64)
	return i, ok
}
