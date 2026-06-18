//go:build !windows

package storage

func mmapFactory() (Factory, error) {
	return func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
		return NewMMapStorage(baseDir, files, pieceLength)
	}, nil
}
