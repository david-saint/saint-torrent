//go:build !windows

package main

import (
	"os/exec"
	"path/filepath"
	"runtime"
)

// revealCommand builds the OS command that opens a file manager with path
// revealed (selected in its parent folder). It is split out from execution so
// the argument construction stays unit-testable. On Linux there is no portable
// "select this item" flag, so we fall back to opening the containing folder.
func revealCommand(path string) (*exec.Cmd, error) {
	if runtime.GOOS == "darwin" {
		// By absolute path, so a directory earlier in $PATH cannot supply
		// its own "open".
		return exec.Command("/usr/bin/open", "-R", path), nil
	}
	return exec.Command("xdg-open", filepath.Dir(path)), nil
}
