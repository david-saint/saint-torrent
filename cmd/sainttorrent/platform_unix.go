//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func acquireLock(lockPath string) (*os.File, error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		_ = file.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, errLockContention
		}
		return nil, fmt.Errorf("failed to flock file: %w", err)
	}
	return file, nil
}

func detectTerminalTTY(input *os.File) string {
	return findTerminalTTY(input, []string{"/dev/ttys*", "/dev/pts/*"})
}

func findTerminalTTY(input *os.File, patterns []string) string {
	info, err := input.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return ""
	}
	inputStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}

	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			continue
		}
		for _, path := range paths {
			ttyInfo, err := os.Stat(path)
			if err != nil {
				continue
			}
			ttyStat, ok := ttyInfo.Sys().(*syscall.Stat_t)
			if ok && ttyStat.Rdev == inputStat.Rdev {
				return path
			}
		}
	}

	return ""
}

func setSocketPermissions(socketPath string) error {
	return os.Chmod(socketPath, 0600)
}
