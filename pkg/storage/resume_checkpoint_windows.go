package storage

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	resumeMetadataReadFlags = os.O_RDONLY
	// FlushFileBuffers needs write access, so a checkpoint opens read/write for the
	// payload files it has to flush.
	resumeSyncOpenFlags = os.O_RDWR
)

// fileBasicInfo mirrors FILE_BASIC_INFO. Only ChangeTime is read: it is the
// Windows equivalent of the Unix change timestamp and is what catches an in-place
// edit whose author restored the modification time.
type fileBasicInfo struct {
	CreationTime   int64
	LastAccessTime int64
	LastWriteTime  int64
	ChangeTime     int64
	FileAttributes uint32
	_              uint32
}

func fileIdentity(f *os.File, _ os.FileInfo) string {
	if f == nil {
		return ""
	}
	var info windows.ByHandleFileInformation
	if windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &info) != nil {
		return ""
	}
	var basic fileBasicInfo
	if windows.GetFileInformationByHandleEx(windows.Handle(f.Fd()), windows.FileBasicInfo, (*byte)(unsafe.Pointer(&basic)), uint32(unsafe.Sizeof(basic))) != nil {
		// Without a change timestamp the identity cannot prove the content is
		// unchanged, so report none and let the pieces be rechecked.
		return ""
	}
	return fmt.Sprintf("%d:%d:%d:%d", info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow, basic.ChangeTime)
}

func replaceResumeFile(root *DownloadRoot, oldName, newName string, _ bool) error {
	directory, err := root.OpenFile(".", os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	old, err := windows.NewNTUnicodeString(oldName)
	if err != nil {
		return err
	}
	attributes := windows.OBJECT_ATTRIBUTES{Length: uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})), RootDirectory: windows.Handle(directory.Fd()), ObjectName: old, Attributes: windows.OBJ_CASE_INSENSITIVE}
	var handle windows.Handle
	var status windows.IO_STATUS_BLOCK
	err = windows.NtCreateFile(&handle, windows.DELETE|windows.SYNCHRONIZE, &attributes, &status, nil, 0, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, windows.FILE_OPEN, windows.FILE_SYNCHRONOUS_IO_NONALERT|windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT, 0, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	name, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	type renameInfo struct {
		Replace uint32
		Root    windows.Handle
		Length  uint32
		Name    [1]uint16
	}
	var layout renameInfo
	size := int(unsafe.Offsetof(layout.Name)) + (len(name)-1)*2
	buffer := make([]byte, size)
	info := (*renameInfo)(unsafe.Pointer(&buffer[0]))
	info.Replace = windows.FILE_RENAME_REPLACE_IF_EXISTS
	info.Root = windows.Handle(directory.Fd())
	info.Length = uint32((len(name) - 1) * 2)
	copy(unsafe.Slice(&info.Name[0], len(name)-1), name)
	return windows.NtSetInformationFile(handle, &status, &buffer[0], uint32(size), windows.FileRenameInformation)
}

func sameFileVersion(a, b os.FileInfo) bool {
	return b != nil && os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}
