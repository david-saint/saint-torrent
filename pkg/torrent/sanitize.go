package torrent

import (
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	// maxComponentBytes is the longest file name common filesystems accept
	// (NAME_MAX on Linux and macOS). Longer names failed at create time, and
	// the name is also what the UI renders.
	maxComponentBytes = 255
	// maxExtensionBytes is the longest extension, dot included, that
	// truncation keeps, so a shortened "long name.mkv" still opens as video.
	maxExtensionBytes = 16
)

// pathFolder is stateless and safe for concurrent use (see cases.Fold).
var pathFolder = cases.Fold()

// sanitizeName makes a torrent-supplied name safe to show and store: control
// characters and invalid UTF-8 become '_', invisible bidi and zero-width
// characters are removed, and the result is cut to maxComponentBytes.
func sanitizeName(s string) string {
	return truncateName(cleanRunes(s), maxComponentBytes)
}

// sanitizePathComponent turns one torrent-supplied name or path element into a
// single safe file name for this platform.
func sanitizePathComponent(p string) string {
	return sanitizeComponent(p, runtime.GOOS == "windows")
}

// sanitizeComponent is sanitizePathComponent with the platform as a parameter
// so the Windows rules can be tested everywhere. Only Windows gets them:
// renaming files on other platforms would orphan existing partial downloads.
func sanitizeComponent(p string, windows bool) string {
	// Characters first, so an invisible character cannot hide a ".." below.
	p = cleanRunes(p)
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
	limit := maxComponentBytes
	if windows {
		p = strings.Map(replaceWindowsReserved, p)
		limit-- // leave room for the '_' a device name gets below
	}
	p = truncateName(p, limit)
	// Only now: truncation can leave a stem ending in '.' before the extension.
	p = strings.ReplaceAll(p, "..", "_")
	if windows {
		p = sanitizeWindowsName(p)
	}
	return p
}

// cleanRunes replaces C0 and C1 control characters, DEL and invalid UTF-8
// with '_', and removes the invisible bidi and zero-width format characters
// that can disguise a name: "photo<U+202E>gpj.exe" displays as "photoexe.jpg".
func cleanRunes(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c >= 0x7f {
			return cleanRunesFrom(s, i)
		}
	}
	return s // printable ASCII, the common case
}

func cleanRunesFrom(s string, i int) string {
	var b strings.Builder
	b.Grow(len(s))
	b.WriteString(s[:i])
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1, r < 0x20, r >= 0x7f && r <= 0x9f:
			b.WriteByte('_')
		case isInvisibleFormat(r):
			// dropped
		default:
			b.WriteString(s[i : i+size])
		}
		i += size
	}
	return b.String()
}

// isInvisibleFormat reports the bidi controls and zero-width characters
// cleanRunes removes: U+061C (Arabic letter mark), U+200B-U+200F (zero-width
// space and joiners, LRM, RLM), U+202A-U+202E (bidi embeddings and
// overrides), U+2060-U+2064 (word joiner, invisible operators), U+2066-U+2069
// (bidi isolates) and U+FEFF (zero-width no-break space).
func isInvisibleFormat(r rune) bool {
	return r == 0x061c ||
		(r >= 0x200b && r <= 0x200f) ||
		(r >= 0x202a && r <= 0x202e) ||
		(r >= 0x2060 && r <= 0x2064) ||
		(r >= 0x2066 && r <= 0x2069) ||
		r == 0xfeff
}

// truncateName cuts s to at most limit bytes on a rune boundary, keeping an
// extension of up to maxExtensionBytes. s must be valid UTF-8.
func truncateName(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	ext := ""
	if i := strings.LastIndexByte(s, '.'); i > 0 && len(s)-i <= maxExtensionBytes {
		ext = s[i:]
	}
	cut := limit - len(ext)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ext
}

func replaceWindowsReserved(r rune) rune {
	switch r {
	case '<', '>', ':', '"', '|', '?', '*':
		return '_'
	}
	return r
}

// sanitizeWindowsName applies the Win32 naming rules: the OS strips trailing
// dots and spaces (so "a." and "a" alias one file) and opens a device for
// names like CON or COM1, with or without an extension.
func sanitizeWindowsName(p string) string {
	p = strings.TrimRight(p, ". ")
	if p == "" {
		return "_"
	}
	if isWindowsDeviceName(p) {
		return "_" + p
	}
	return p
}

func isWindowsDeviceName(p string) bool {
	base := p
	if i := strings.IndexByte(base, '.'); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimRight(base, " ")
	for _, name := range []string{"CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"} {
		if strings.EqualFold(base, name) {
			return true
		}
	}
	if len(base) < 4 {
		return false
	}
	if port := base[:3]; !strings.EqualFold(port, "COM") && !strings.EqualFold(port, "LPT") {
		return false
	}
	switch base[3:] {
	// Windows also reserves the superscript digits 1-3.
	case "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "\u00b9", "\u00b2", "\u00b3":
		return true
	}
	return false
}

// pathKey is the duplicate-detection key for a relative path: Unicode NFC plus
// full case folding, so two names a case- or normalization-insensitive
// filesystem (APFS, NTFS) stores as one file are caught as duplicates.
func pathKey(p string) string {
	return norm.NFC.String(pathFolder.String(norm.NFD.String(filepath.Clean(p))))
}
