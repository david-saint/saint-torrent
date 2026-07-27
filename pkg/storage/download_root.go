package storage

import "os"

// OpenDownloadRoot creates a download directory when it is safe to do so and
// returns an anchored root handle for subsequent control-path operations.
// Platform implementations protect removable-volume mount points from path
// races while the directory is created/opened.
func OpenDownloadRoot(path string) (*os.Root, error) {
	return openPlatformDownloadRoot(path)
}

func mkdirAllInRoot(root *os.Root, path string, perm os.FileMode) error {
	if path == "" || path == "." {
		return nil
	}
	current := root
	for _, component := range splitPathComponents(path) {
		if err := current.Mkdir(component, perm); err != nil && !os.IsExist(err) {
			if current != root {
				_ = current.Close()
			}
			return err
		}
		next, err := current.OpenRoot(component)
		if current != root {
			_ = current.Close()
		}
		if err != nil {
			return err
		}
		current = next
	}
	if current != root {
		return current.Close()
	}
	return nil
}
