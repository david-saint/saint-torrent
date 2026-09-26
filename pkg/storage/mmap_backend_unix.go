//go:build !windows

package storage

func mmapFactory() (Factory, error) {
	return func(baseDir string, files []FileInfo, pieceLength int64) (Storage, error) {
		st, err := NewMMapStorage(baseDir, files, pieceLength)
		if err != nil {
			// An untyped nil, never a nil *MMapStorage boxed in the interface.
			return nil, err
		}
		return st, nil
	}, nil
}
