package main

import (
	"fmt"
	"path/filepath"
	"strings"
)

// revealInFileManager reveals path in the platform's file manager (Finder on
// macOS). It returns once the helper process has been launched; the reveal
// itself happens asynchronously, so the TUI is never blocked. The child's
// stdout/stderr are left unset (discarded) so it cannot corrupt the display.
func revealInFileManager(path string) error {
	// The path ends in a torrent-chosen name; requiring it to be absolute
	// also means it can never be taken for a command-line option.
	if !filepath.IsAbs(path) {
		return fmt.Errorf("location %q is not an absolute path", path)
	}
	cmd, err := revealCommand(path)
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap the short-lived helper so it does not linger as a zombie.
	go func() { _ = cmd.Wait() }()
	return nil
}

// explorerCmdLine builds the explorer.exe command line that reveals path,
// selected in its folder, or opens parent when path cannot be quoted. Explorer
// splits its arguments on commas rather than following the usual argv rules,
// and Go only adds quotes for spaces, so a torrent name such as
// "Movie,calc.exe" would otherwise be parsed as a second switch or object.
// It is platform-neutral so the quoting can be tested everywhere.
func explorerCmdLine(exe, path, parent string) (string, bool) {
	if q, ok := quoteExplorerArg(path); ok {
		return `"` + exe + `" /select,` + q, true
	}
	if q, ok := quoteExplorerArg(parent); ok {
		return `"` + exe + `" ` + q, true
	}
	return "", false
}

// quoteExplorerArg double-quotes a path for explorer.exe. It refuses a path
// with a quote or a control character, which would end or corrupt the
// quoted argument.
func quoteExplorerArg(path string) (string, bool) {
	if path == "" || strings.ContainsFunc(path, func(r rune) bool { return r == '"' || r < 0x20 || r == 0x7f }) {
		return "", false
	}
	// A trailing backslash would escape the closing quote; "C:\." names the
	// same folder as "C:\".
	if strings.HasSuffix(path, `\`) {
		path += "."
	}
	return `"` + path + `"`, true
}
