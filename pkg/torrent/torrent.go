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
)

// maxPathDepth bounds the number of path components one file may declare.
const maxPathDepth = 128

// File represents an individual file and its size in a multi-file torrent.
type File struct {
	Length int64
	Path   []string
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
	// Metainfo is decoded strictly: a repeated key would let the info-hash and
	// the decoded fields (or another client) disagree about the same bytes.
	val, err := bencode.UnmarshalStrict(data)
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
		announce, _ = normalizeURL(raw, true)
	}

	trackers := newURLSet(maxTrackers, true)
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

	webSeeds := newURLSet(maxWebSeeds, false)
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
	// info-hash always commits to the content that gets downloaded.
	bencodedInfo, err := bencode.FindRawValue(data, "info")
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
		cleanName := sanitizePathComponent(name)
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
			for _, pVal := range pathSlice {
				pStr, ok := pVal.(string)
				if !ok {
					return nil, fmt.Errorf("invalid path component in file path")
				}
				cleanedComp := sanitizePathComponent(pStr)
				if cleanedComp != "" {
					path = append(path, cleanedComp)
				}
			}
			if len(path) == 1 { // Only contains the torrent name, no actual files
				path = append(path, "unknown_file")
			}
			files = append(files, File{
				Length: length,
				Path:   path,
			})
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
		files = []File{
			{
				Length: length,
				Path:   []string{sanitizePathComponent(name)},
			},
		}
	}
	if totalLength <= 0 {
		return nil, fmt.Errorf("torrent total length must be positive")
	}
	if err := checkPieceCount(numPieces, pieceLength, totalLength); err != nil {
		return nil, err
	}
	// Reject paths that differ only in case or Unicode normalization: case- or
	// normalization-insensitive filesystems would store them as one file.
	seenPaths := make(map[string]struct{}, len(files))
	keys := make([]string, len(files))
	for i, f := range files {
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
	pieceHashes := make([][20]byte, numPieces)
	for i := range pieceHashes {
		copy(pieceHashes[i][:], piecesStr[i*20:])
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
// another file, as in [a] and [a b]: no filesystem can lay that out, and
// libtorrent refuses it too. keys are the files' pathKey values, so case and
// normalization fold exactly as in the duplicate check; folding neither adds
// nor removes a separator, and sanitized components hold none. Each directory
// component is hashed once, under its parent's number, so the cost is linear
// in the path bytes: keying every directory by its whole path would hash and
// keep O(depth^2) bytes per file, 64 times the metadata for 128-deep paths.
func checkFileDirCollisions(files []File, keys []string) error {
	if len(files) < 2 {
		return nil
	}
	sep := string(filepath.Separator)
	dirs := make(map[pathEntry]int)
	leaves := make([]pathEntry, len(files))
	for i, key := range keys {
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
