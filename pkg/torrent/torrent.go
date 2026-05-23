package torrent

import (
	"crypto/sha1"
	"fmt"
	"math"
	"path/filepath"
	"strings"

	"sainttorrent/pkg/bencode"
)

// File represents an individual file and its size in a multi-file torrent.
type File struct {
	Length int64
	Path   []string
}

// Torrent represents the metadata extracted from a torrent file.
type Torrent struct {
	Announce    string
	Trackers    []string
	InfoHash    [20]byte
	PieceLength int64
	PieceHashes [][20]byte
	Name        string
	Files       []File
	InfoBytes   []byte // Raw bencoded info dictionary
}

// Parse decodes a bencoded torrent file, calculates the info hash, and returns a Torrent struct.
func Parse(data []byte) (*Torrent, error) {
	val, err := bencode.Unmarshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal torrent bencode: %w", err)
	}

	dict, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("torrent file is not a bencoded dictionary")
	}

	// 1. Announce / Trackers
	announce, _ := getString(dict, "announce")

	var trackers []string
	if announceList, ok := dict["announce-list"].([]interface{}); ok {
		for _, tierVal := range announceList {
			if tier, ok := tierVal.([]interface{}); ok {
				for _, trackerVal := range tier {
					if trackerStr, ok := trackerVal.(string); ok && trackerStr != "" {
						trackers = append(trackers, trackerStr)
					}
				}
			}
		}
	}

	// Fallback to announce if no trackers were extracted from announce-list
	if len(trackers) == 0 && announce != "" {
		trackers = []string{announce}
	}

	// 2. Info Dictionary
	infoVal, ok := dict["info"]
	if !ok {
		return nil, fmt.Errorf("missing info dictionary")
	}
	info, ok := infoVal.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid info field type")
	}

	// 3. Info Hash Calculation (using exact raw bencoded bytes)
	bencodedInfo, err := bencode.FindRawValue(data, "info")
	if err != nil {
		return nil, fmt.Errorf("failed to extract raw info dictionary: %w", err)
	}
	infoHash := sha1.Sum(bencodedInfo)

	// 4. Extract standard Info fields
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

	piecesStr, ok := getString(info, "pieces")
	if !ok {
		return nil, fmt.Errorf("missing or invalid pieces in info dict")
	}
	piecesBytes := []byte(piecesStr)
	if len(piecesBytes)%20 != 0 {
		return nil, fmt.Errorf("pieces length must be a multiple of 20, got %d", len(piecesBytes))
	}
	numPieces := len(piecesBytes) / 20
	if numPieces == 0 {
		return nil, fmt.Errorf("torrent must contain at least one piece hash")
	}
	pieceHashes := make([][20]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		copy(pieceHashes[i][:], piecesBytes[i*20:(i+1)*20])
	}

	// 5. Files list (handling single-file vs multi-file)
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
			if length < 0 {
				return nil, fmt.Errorf("file length cannot be negative: %d", length)
			}
			if totalLength > math.MaxInt64-length {
				return nil, fmt.Errorf("total file length overflows int64")
			}
			totalLength += length
			pathSlice, ok := fMap["path"].([]interface{})
			if !ok {
				return nil, fmt.Errorf("missing or invalid path in files list entry")
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
		if length < 0 {
			return nil, fmt.Errorf("file length cannot be negative: %d", length)
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
	expectedPieces := int((totalLength-1)/pieceLength + 1)
	if numPieces != expectedPieces {
		return nil, fmt.Errorf("piece hash count mismatch: got %d, expected %d", numPieces, expectedPieces)
	}
	// Check for case-insensitive duplicate paths
	seenPaths := make(map[string]bool)
	for _, f := range files {
		relPath := filepath.Join(f.Path...)
		lowerPath := strings.ToLower(filepath.Clean(relPath))
		if seenPaths[lowerPath] {
			return nil, fmt.Errorf("duplicate file path detected in torrent metadata: %s", relPath)
		}
		seenPaths[lowerPath] = true
	}

	return &Torrent{
		Announce:    announce,
		Trackers:    trackers,
		InfoHash:    infoHash,
		PieceLength: pieceLength,
		PieceHashes: pieceHashes,
		Name:        name,
		Files:       files,
		InfoBytes:   bencodedInfo,
	}, nil
}

func sanitizePathComponent(p string) string {
	// Clean up any path separators or relative directory navigation
	p = filepath.Clean(p)
	// Remove any leading/trailing slash or backslash
	p = strings.Trim(p, "/\\")
	// If it contains ".." or is empty, sanitize to prevent traversal
	if p == ".." || p == "." || p == "" {
		return "safe_name"
	}
	// Replace path separators to prevent breaking out
	p = strings.ReplaceAll(p, "/", "_")
	p = strings.ReplaceAll(p, "\\", "_")
	p = strings.ReplaceAll(p, "..", "_")
	return p
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
