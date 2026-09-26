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

// ReadFile reads a .torrent file of at most MaxFileSize bytes. The size is
// checked on the open file before anything is read, and the read itself is
// bounded too, for a file that grows meanwhile or reports no size (a pipe).
func ReadFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
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

func fileTooLarge(path string) error {
	return fmt.Errorf("torrent file %s is larger than the maximum of %d bytes", path, MaxFileSize)
}
