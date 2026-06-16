//go:build windows

package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os"
)

func acquireLock(lockPath string) (*os.File, error) {
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	h := windows.Handle(file.Fd())
	var ol windows.Overlapped
	err = windows.LockFileEx(h, windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &ol)
	if err != nil {
		_ = file.Close()
		if errno, ok := err.(windows.Errno); ok {
			if errno == windows.ERROR_LOCK_VIOLATION || errno == windows.ERROR_SHARING_VIOLATION {
				return nil, errLockContention
			}
		}
		return nil, fmt.Errorf("failed to lock file: %w", err)
	}
	return file, nil
}

func detectTerminalTTY(input *os.File) string {
	return ""
}

func setSocketPermissions(socketPath string) error {
	return nil
}
