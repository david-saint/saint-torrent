package fsync

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func file(f *os.File) error {
	return control(f, fullSync)
}

func writeBack(f *os.File) error {
	return control(f, plainSync)
}

func control(f *os.File, sync func(fd uintptr) error) error {
	conn, err := f.SyscallConn()
	if err != nil {
		return err
	}
	var syncErr error
	if err := conn.Control(func(fd uintptr) { syncErr = sync(fd) }); err != nil {
		return err
	}
	if syncErr != nil {
		return &os.PathError{Op: "sync", Path: f.Name(), Err: syncErr}
	}
	return nil
}

// fullSync mirrors internal/poll's darwin Fsync: F_FULLFSYNC, falling back to
// fsync where the filesystem refuses it (SMB mounts answer ENOTSUP), retrying
// on EINTR. Unlike it, both calls go through x/sys/unix and so enter the system
// call the way the scheduler and the GC expect.
func fullSync(fd uintptr) error {
	for {
		_, err := unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)
		if errors.Is(err, unix.ENOTSUP) {
			err = unix.Fsync(int(fd))
		}
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// plainSync is fsync(2), which on macOS writes the file's dirty pages to the
// device but leaves the drive's own cache unflushed.
func plainSync(fd uintptr) error {
	for {
		err := unix.Fsync(int(fd))
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}
