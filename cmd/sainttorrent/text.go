package main

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// --- untrusted text -------------------------------------------------------
//
// Torrent names, file paths, tracker messages and the errors that embed them
// are chosen by whoever made the torrent or runs the tracker. The TUI renders
// them through displayText/sanitizeText; plain stdout/stderr output goes
// through escapeForTerminal.

// maxDisplayBytes bounds untrusted text before it is sanitized and measured
// for display. A name can be up to 16 MiB (ut_metadata) and a tracker failure
// reason megabytes long, yet no row or card shows more than a few hundred
// cells; without the cut the per-tick sanitize and the per-frame width scan
// cost O(size) and freeze the TUI.
const maxDisplayBytes = 512

// maxTerminalBytes bounds one untrusted fragment in plain terminal output.
const maxTerminalBytes = 4096

// hiddenRunePlaceholder replaces invisible and reordering characters so their
// presence shows: "x\u202efdp.exe" renders as "x\ufffdfdp.exe" instead of being
// reordered into "xexe.pdf" by a bidi-aware terminal.
const hiddenRunePlaceholder = '\ufffd'

// isControlRune reports C0, DEL and C1 controls, the bytes that start escape
// sequences.
func isControlRune(r rune) bool {
	return r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f)
}

// isHiddenRune reports characters that are invisible or reorder the text
// around them: format characters (bidi overrides and isolates, zero-width
// characters, the BOM, tag characters), line and paragraph separators, and
// variation selectors.
func isHiddenRune(r rune) bool {
	switch {
	case r < 0xAD:
		return false // fast path: nothing below the soft hyphen qualifies
	case r >= 0xFE00 && r <= 0xFE0F, r >= 0xE0100 && r <= 0xE01EF:
		return true // variation selectors
	case r >= 0xE0000 && r <= 0xE007F:
		return true // tag characters
	}
	return unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp)
}

// sanitizeText makes untrusted text safe to render in the TUI. Control
// characters become spaces (so no escape sequence reaches the terminal), runs
// of spaces collapse, and hidden characters become a visible placeholder.
func sanitizeText(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	pendingSpace := false
	for _, r := range s {
		switch {
		case r == ' ' || isControlRune(r):
			pendingSpace = sb.Len() > 0
			continue
		case isHiddenRune(r):
			r = hiddenRunePlaceholder
		}
		if pendingSpace {
			sb.WriteByte(' ')
			pendingSpace = false
		}
		sb.WriteRune(r)
	}
	return strings.TrimSpace(sb.String())
}

// displayText cuts untrusted text to maxDisplayBytes and sanitizes it. Use it
// wherever a name, path or error from a torrent or tracker reaches the TUI.
func displayText(s string) string {
	return sanitizeText(boundText(s, maxDisplayBytes))
}

// boundText cuts s to about n bytes on rune boundaries. It keeps the head and
// the tail around an ellipsis, so a long path still shows its extension.
func boundText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	head := n / 2
	for head > 0 && !utf8.RuneStart(s[head]) {
		head--
	}
	tail := len(s) - n/2
	for tail < len(s) && !utf8.RuneStart(s[tail]) {
		tail++
	}
	return s[:head] + "…" + s[tail:]
}

// escapeForTerminal renders untrusted text for plain stdout/stderr output,
// which, unlike the TUI, writes bytes straight to the terminal. Controls
// (ESC-based sequences such as OSC 52 clipboard writes or cursor movement),
// hidden characters and invalid UTF-8 are written as Go-style escapes.
func escapeForTerminal(s string) string {
	s = boundText(s, maxTerminalBytes)
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			fmt.Fprintf(&sb, `\x%02x`, s[i])
		case r == '\n':
			sb.WriteString(`\n`)
		case r == '\r':
			sb.WriteString(`\r`)
		case r == '\t':
			sb.WriteString(`\t`)
		case r < 0x80 && isControlRune(r):
			fmt.Fprintf(&sb, `\x%02x`, r)
		case isControlRune(r) || isHiddenRune(r):
			if r > 0xFFFF {
				fmt.Fprintf(&sb, `\U%08x`, r)
			} else {
				fmt.Fprintf(&sb, `\u%04x`, r)
			}
		default:
			sb.WriteString(s[i : i+size])
		}
		i += size
	}
	return sb.String()
}
