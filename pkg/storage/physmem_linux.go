//go:build linux

package storage

import "golang.org/x/sys/unix"

// physicalMemory reports the machine's total RAM in bytes.
func physicalMemory() (uint64, bool) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil || info.Totalram == 0 {
		return 0, false
	}
	return uint64(info.Totalram) * uint64(max(info.Unit, 1)), true
}
