//go:build windows

package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os"
	"path/filepath"
	"strings"
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

func terminateProcess(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

func killProcess(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return proc.Kill()
}

// stillActive is the exit code GetExitCodeProcess reports for a running process.
const stillActive = 259

// isSaintTorrentProcess reports whether pid is a running process whose image is
// sainttorrent.exe.
func isSaintTorrentProcess(pid int) bool {
	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(h)
	var code uint32
	if err := windows.GetExitCodeProcess(h, &code); err != nil || code != stillActive {
		return false
	}
	buf := make([]uint16, windows.MAX_LONG_PATH)
	size := uint32(len(buf))
	if err := windows.QueryFullProcessImageName(h, 0, &buf[0], &size); err != nil {
		return false
	}
	return strings.EqualFold(filepath.Base(windows.UTF16ToString(buf[:size])), "sainttorrent.exe")
}

func findProcessPIDs() []int {
	return nil
}
