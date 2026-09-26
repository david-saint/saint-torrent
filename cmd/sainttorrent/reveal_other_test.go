//go:build !windows

package main

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestRevealCommand(t *testing.T) {
	const path = "/tmp/downloads/MyShow"
	cmd, err := revealCommand(path)
	if err != nil || cmd == nil {
		t.Fatalf("revealCommand: %v", err)
	}

	wantArgs := []string{"xdg-open", filepath.Dir(path)}
	if runtime.GOOS == "darwin" {
		// Resolved by absolute path, never through $PATH.
		wantArgs = []string{"/usr/bin/open", "-R", path}
		if cmd.Path != "/usr/bin/open" {
			t.Fatalf("cmd.Path = %q, want /usr/bin/open", cmd.Path)
		}
	}

	if len(cmd.Args) != len(wantArgs) {
		t.Fatalf("cmd.Args = %v, want %v", cmd.Args, wantArgs)
	}
	for i := range wantArgs {
		if cmd.Args[i] != wantArgs[i] {
			t.Errorf("cmd.Args[%d] = %q, want %q", i, cmd.Args[i], wantArgs[i])
		}
	}
}
