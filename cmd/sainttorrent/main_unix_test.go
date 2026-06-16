//go:build !windows

package main

import (
	"os"
	"testing"
)

func TestFindTerminalTTY(t *testing.T) {
	input, err := os.Open("/dev/null")
	if err != nil {
		t.Fatalf("failed to open character device: %v", err)
	}
	defer input.Close()

	if got := findTerminalTTY(input, []string{"/dev/null"}); got != "/dev/null" {
		t.Fatalf("expected matching device path, got %q", got)
	}

	tempFile, err := os.CreateTemp("", "sainttorrent-not-a-tty-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	if got := findTerminalTTY(tempFile, []string{tempFile.Name()}); got != "" {
		t.Fatalf("expected regular file to be rejected, got %q", got)
	}
}
