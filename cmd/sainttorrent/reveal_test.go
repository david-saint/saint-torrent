package main

import (
	"path/filepath"
	"runtime"
	"testing"
)

func TestRevealCommand(t *testing.T) {
	const path = "/tmp/downloads/MyShow"
	cmd := revealCommand(path)
	if cmd == nil {
		t.Fatal("revealCommand returned nil")
	}

	var wantArgs []string
	switch runtime.GOOS {
	case "darwin":
		wantArgs = []string{"open", "-R", path}
	case "windows":
		wantArgs = []string{"explorer", "/select," + path}
	default:
		wantArgs = []string{"xdg-open", filepath.Dir(path)}
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
