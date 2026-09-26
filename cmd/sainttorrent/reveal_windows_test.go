//go:build windows

package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRevealCommand(t *testing.T) {
	const path = `C:\Users\me\Downloads\Movie,calc.exe`
	cmd, err := revealCommand(path)
	if err != nil {
		t.Fatalf("revealCommand: %v", err)
	}
	if !strings.EqualFold(filepath.Base(cmd.Path), "explorer.exe") {
		t.Fatalf("cmd.Path = %q, want explorer.exe", cmd.Path)
	}
	if cmd.SysProcAttr == nil {
		t.Fatal("command line not set explicitly")
	}
	if want := ` /select,"` + path + `"`; !strings.HasSuffix(cmd.SysProcAttr.CmdLine, want) {
		t.Fatalf("CmdLine = %q, want suffix %q", cmd.SysProcAttr.CmdLine, want)
	}
}
