//go:build windows

package storage

import (
	"golang.org/x/sys/windows"
	"os"
)

func openNoFollow(path string, flag int, perm os.FileMode) (*os.File, error) {
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}

	var access uint32
	switch flag & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		access = windows.GENERIC_READ
	case os.O_WRONLY:
		access = windows.GENERIC_WRITE
	case os.O_RDWR:
		access = windows.GENERIC_READ | windows.GENERIC_WRITE
	}

	var sharemode uint32 = windows.FILE_SHARE_READ | windows.FILE_SHARE_WRITE | windows.FILE_SHARE_DELETE

	var createmode uint32
	switch {
	case flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		createmode = windows.CREATE_NEW
	case flag&os.O_CREATE != 0 && flag&os.O_TRUNC != 0:
		createmode = windows.CREATE_ALWAYS
	case flag&os.O_CREATE != 0:
		createmode = windows.OPEN_ALWAYS
	case flag&os.O_TRUNC != 0:
		createmode = windows.TRUNCATE_EXISTING
	default:
		createmode = windows.OPEN_EXISTING
	}

	var attrs uint32 = windows.FILE_ATTRIBUTE_NORMAL | windows.FILE_FLAG_OPEN_REPARSE_POINT

	h, err := windows.CreateFile(pathp, access, sharemode, nil, createmode, attrs, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}

	var d windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(h, &d)
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	if d.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		_ = windows.CloseHandle(h)
		return nil, &os.PathError{Op: "open", Path: path, Err: windows.ERROR_REPARSE_TAG_MISMATCH}
	}

	return os.NewFile(uintptr(h), path), nil
}
