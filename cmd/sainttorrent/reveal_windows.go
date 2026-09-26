//go:build windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// revealCommand opens Explorer with path selected. The command line is set
// explicitly (see explorerCmdLine) because Go's argv quoting does not match
// explorer.exe's own comma-splitting parser.
func revealCommand(path string) (*exec.Cmd, error) {
	exe := explorerPath()
	line, ok := explorerCmdLine(exe, path, filepath.Dir(path))
	if !ok {
		return nil, fmt.Errorf("cannot pass %q to Explorer", path)
	}
	cmd := exec.Command(exe)
	cmd.SysProcAttr = &syscall.SysProcAttr{CmdLine: line}
	return cmd, nil
}

// explorerPath resolves explorer.exe from the Windows directory rather than
// through PATH.
func explorerPath() string {
	if root := os.Getenv("SystemRoot"); root != "" {
		return filepath.Join(root, "explorer.exe")
	}
	return "explorer.exe"
}
