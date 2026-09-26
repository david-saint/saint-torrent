//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package storage

import (
	"runtime"

	"golang.org/x/sys/unix"
)

// physicalMemory reports the machine's total RAM in bytes.
func physicalMemory() (uint64, bool) {
	name := "hw.physmem"
	switch runtime.GOOS {
	case "darwin", "ios":
		name = "hw.memsize"
	case "netbsd", "openbsd":
		name = "hw.physmem64"
	}
	total, err := unix.SysctlUint64(name)
	if err != nil || total == 0 {
		return 0, false
	}
	return total, true
}
