//go:build !linux && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package files

import (
	"os"
	"time"
)

func updateMode(path string, mode os.FileMode) error {
	if mode == 0 {
		return nil
	}
	return os.Chmod(path, mode)
}

func updateMtime(path string, mtime time.Time) error {
	if mtime.IsZero() {
		return nil
	}
	return os.Chtimes(path, mtime, mtime)
}
