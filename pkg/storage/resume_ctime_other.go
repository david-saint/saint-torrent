//go:build !windows && !darwin && !ios && !freebsd && !netbsd && !linux && !openbsd

package storage

import (
	"reflect"
	"syscall"
)

// Stat_t names the change timestamp Ctim or Ctimespec depending on the platform.
// The supported targets use the build-tagged accessors above; anything else falls
// back to a reflective lookup so an unusual Unix still gets a durable checkpoint.
func statChangeTime(st *syscall.Stat_t) (int64, int64, bool) {
	value := reflect.ValueOf(st).Elem()
	change := value.FieldByName("Ctim")
	if !change.IsValid() {
		change = value.FieldByName("Ctimespec")
	}
	if !change.IsValid() {
		return 0, 0, false
	}
	seconds := change.FieldByName("Sec")
	nanoseconds := change.FieldByName("Nsec")
	if !seconds.IsValid() || !nanoseconds.IsValid() {
		return 0, 0, false
	}
	return seconds.Int(), nanoseconds.Int(), true
}
