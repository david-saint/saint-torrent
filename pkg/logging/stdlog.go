package logging

import (
	"io"
	"strings"
	"unicode"
)

// StdLogWriter returns a writer for the standard library logger
// (log.SetOutput). Dependencies such as goupnp log through it, sometimes with
// raw bytes from the network; left on stderr, those bytes would reach the
// TUI's terminal unsanitized. Each line is forwarded to the debug log with
// control and format characters replaced, or dropped when logging is off.
func StdLogWriter() io.Writer {
	return stdLogWriter{}
}

type stdLogWriter struct{}

func (stdLogWriter) Write(p []byte) (int, error) {
	if EnabledFor(LevelWarn) {
		Warn("stdlog", String("message", stripControl(string(p))))
	}
	return len(p), nil
}

// stripControl replaces control and format characters with spaces and drops
// the trailing newline the log package appends.
func stripControl(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, strings.TrimRight(s, "\n"))
}
