//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package storage

// physicalMemory reports that the machine's RAM is unknown on this platform,
// which leaves the mem backend bounded only by the address space.
func physicalMemory() (uint64, bool) {
	return 0, false
}
