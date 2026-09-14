//go:build darwin || ios || freebsd || netbsd

package storage

import "syscall"

func statChangeTime(st *syscall.Stat_t) (int64, int64, bool) {
	return int64(st.Ctimespec.Sec), int64(st.Ctimespec.Nsec), true
}
