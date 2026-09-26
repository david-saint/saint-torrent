//go:build linux || openbsd

package storage

import "syscall"

// No _linux file suffix: it would limit this file to Linux despite the tag.
func statChangeTime(st *syscall.Stat_t) (int64, int64, bool) {
	return int64(st.Ctim.Sec), int64(st.Ctim.Nsec), true
}
