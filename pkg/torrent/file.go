package torrent

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// MaxFileSize bounds a .torrent file read into memory, as qBittorrent's
// default does. Real torrents are at most a few MiB, and parsing decodes the
// whole file into a tree many times its size before unknown keys are dropped.
const MaxFileSize = 100 << 20

// ReadFile reads a .torrent file of at most MaxFileSize bytes. Only regular
// files are read: a FIFO, device or directory at path is refused without
// blocking, since opening a FIFO or reading a device could hang the caller
// forever. The size is checked on the open file before anything is read, and
// the read itself is bounded too, for a file that grows meanwhile.
func ReadFile(path string) ([]byte, error) {
	f, info, err := openRegular(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if info.Size() > MaxFileSize {
		return nil, fileTooLarge(path)
	}
	buf := bytes.NewBuffer(make([]byte, 0, info.Size()+bytes.MinRead))
	if _, err := buf.ReadFrom(io.LimitReader(f, MaxFileSize+1)); err != nil {
		return nil, err
	}
	if buf.Len() > MaxFileSize {
		return nil, fileTooLarge(path)
	}
	return buf.Bytes(), nil
}

// openRegular opens path for reading and returns it with its FileInfo, or an
// error when it is not a regular file. The check runs on the open file, so
// nothing can be swapped in between it and the read.
func openRegular(path string) (*os.File, os.FileInfo, error) {
	f, err := openForRead(path)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, nil, fmt.Errorf("%s is not a regular file", path)
	}
	return f, info, nil
}

func fileTooLarge(path string) error {
	return fmt.Errorf("torrent file %s is larger than the maximum of %d bytes", path, MaxFileSize)
}
