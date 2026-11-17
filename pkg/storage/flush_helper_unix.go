//go:build !windows

package storage

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

const flushHelperSupported = true

// spawnFlushHelper runs executable as a flush helper over handles. The helper
// gets its own process group, so the Ctrl-C that quits the TUI does not kill a
// push still in flight, and it has no stdio of its own.
func spawnFlushHelper(executable string, handles []*os.File) (func() error, error) {
	cmd := exec.Command(executable, FlushHelperArg, strconv.Itoa(len(handles)))
	cmd.ExtraFiles = handles
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd.Wait, nil
}
