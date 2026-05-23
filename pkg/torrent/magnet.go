package torrent

import (
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
)

// MagnetLink represents a parsed magnet URI.
type MagnetLink struct {
	InfoHash [20]byte // The info hash from xt=urn:btih:<hash>
	Name     string   // Display name from dn= (optional)
	Trackers []string // Tracker URLs from tr= (can be multiple)
}

// ParseMagnet parses a magnet URI string.
// Supports: magnet:?xt=urn:btih:<40-char-hex or 32-char-base32>&dn=<name>&tr=<tracker>
//
// The xt (exact topic) parameter is required and must contain a valid
// BitTorrent info hash in either 40-character hexadecimal or 32-character
// base32 encoding. The dn (display name) parameter is optional. The tr
// (tracker) parameter may appear multiple times to specify multiple trackers.
func ParseMagnet(uri string) (*MagnetLink, error) {
	const prefix = "magnet:?"
	if !strings.HasPrefix(uri, prefix) {
		return nil, fmt.Errorf("invalid magnet URI: missing %q prefix", prefix)
	}

	query := uri[len(prefix):]
	params, err := url.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet URI: failed to parse query parameters: %w", err)
	}

	// xt (exact topic) is required.
	xt := params.Get("xt")
	if xt == "" {
		return nil, fmt.Errorf("invalid magnet URI: missing required xt parameter")
	}

	const btihPrefix = "urn:btih:"
	if !strings.HasPrefix(xt, btihPrefix) {
		return nil, fmt.Errorf("invalid magnet URI: xt must begin with %q, got %q", btihPrefix, xt)
	}

	hashStr := xt[len(btihPrefix):]
	infoHash, err := decodeInfoHash(hashStr)
	if err != nil {
		return nil, fmt.Errorf("invalid magnet URI: %w", err)
	}

	return &MagnetLink{
		InfoHash: infoHash,
		Name:     params.Get("dn"),
		Trackers: params["tr"],
	}, nil
}

// decodeInfoHash decodes a hex (40 chars) or base32 (32 chars) encoded info hash.
func decodeInfoHash(s string) ([20]byte, error) {
	var hash [20]byte

	switch len(s) {
	case 40:
		// 40-character hex encoding
		decoded, err := hex.DecodeString(s)
		if err != nil {
			return hash, fmt.Errorf("invalid hex info hash: %w", err)
		}
		copy(hash[:], decoded)

	case 32:
		// 32-character base32 encoding (RFC 4648, no padding)
		decoded, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(s))
		if err != nil {
			return hash, fmt.Errorf("invalid base32 info hash: %w", err)
		}
		if len(decoded) != 20 {
			return hash, fmt.Errorf("base32 info hash decoded to %d bytes, expected 20", len(decoded))
		}
		copy(hash[:], decoded)

	default:
		return hash, fmt.Errorf("info hash must be 40 hex chars or 32 base32 chars, got %d chars", len(s))
	}

	return hash, nil
}
