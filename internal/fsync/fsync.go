// Package fsync flushes files to stable storage without stalling the Go
// runtime.
//
// On macOS, (*os.File).Sync issues F_FULLFSYNC through runtime.fcntl, a raw
// libc call that never tells the scheduler it is in a system call. While the
// flush blocks, which on a slow or USB-attached disk can take many seconds, the
// goroutine still counts as running. The garbage collector cannot suspend it to
// scan its stack, so a GC cycle cannot finish and every goroutine that
// allocates (the TUI among them) parks behind it: the whole process freezes
// until the disk answers. File issues the same F_FULLFSYNC through
// golang.org/x/sys/unix, which enters the system call properly, so the GC and
// the rest of the program keep running during the flush.
//
// Every durable flush in this module goes through File; see the test that
// rejects a bare Sync call.
package fsync

import "os"

// File flushes f's contents and metadata to stable storage, like f.Sync(). f
// may be a regular file or a directory.
func File(f *os.File) error {
	if f == nil {
		return os.ErrInvalid
	}
	return file(f)
}

// WriteBack pushes f's written data out to the device without the drive-cache
// barrier File adds on macOS. It is the cheap half of a durable flush: once it
// has run, a File on the same file only has the drive cache left to settle.
func WriteBack(f *os.File) error {
	if f == nil {
		return os.ErrInvalid
	}
	return writeBack(f)
}
