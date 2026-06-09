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
func revealCommand(path string) *exec.Cmd {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", "-R", path)
	case "windows":
		// explorer wants the select verb glued to the path in one argument.
		return exec.Command("explorer", "/select,"+path)
	default:
		return exec.Command("xdg-open", filepath.Dir(path))
	}
}

// revealInFileManager reveals path in the platform's file manager (Finder on
// macOS). It returns once the helper process has been launched; the reveal
// itself happens asynchronously, so the TUI is never blocked. The child's
// stdout/stderr are left unset (discarded) so it cannot corrupt the display.
func revealInFileManager(path string) error {
	cmd := revealCommand(path)
	if err := cmd.Start(); err != nil {
		return err
	}
	// Reap the short-lived helper so it does not linger as a zombie.
	go func() { _ = cmd.Wait() }()
	return nil
}
