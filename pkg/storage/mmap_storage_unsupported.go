//go:build windows

package storage

import "errors"

var errMMapUnsupported = errors.New("mmap storage is not supported on this platform")

// MMapStorage is unavailable on this platform.
type MMapStorage struct{}

var _ Storage = (*MMapStorage)(nil)

// NewMMapStorage reports that mmap storage is unavailable on this platform.
func NewMMapStorage(baseDir string, files []FileInfo, pieceLength int64) (*MMapStorage, error) {
	return nil, errMMapUnsupported
}

// BaseDir returns an empty base directory for the unavailable backend.
func (s *MMapStorage) BaseDir() string { return "" }

// TotalSize returns zero for the unavailable backend.
func (s *MMapStorage) TotalSize() int64 { return 0 }

// PieceLengthValue returns zero for the unavailable backend.
func (s *MMapStorage) PieceLengthValue() int64 { return 0 }

// PieceLength returns zero for the unavailable backend.
func (s *MMapStorage) PieceLength(pieceIndex int64) int64 { return 0 }

// ReadBlock returns the platform unsupported error.
func (s *MMapStorage) ReadBlock(pieceIndex int64, offset int64, buf []byte) (int, error) {
	return 0, errMMapUnsupported
}

// WriteBlock returns the platform unsupported error.
func (s *MMapStorage) WriteBlock(pieceIndex int64, offset int64, data []byte) error {
	return errMMapUnsupported
}

// VerifyPiece returns the platform unsupported error.
func (s *MMapStorage) VerifyPiece(pieceIndex int64, expectedHash [20]byte) (bool, error) {
	return false, errMMapUnsupported
}

// SaveState returns the platform unsupported error.
func (s *MMapStorage) SaveState(infoHashHex string, completedPieces []int) error {
	return errMMapUnsupported
}

// LoadState returns the platform unsupported error.
func (s *MMapStorage) LoadState(infoHashHex string) ([]int, error) {
	return nil, errMMapUnsupported
}

// Close is a no-op for the unavailable backend.
func (s *MMapStorage) Close() error { return nil }
