package files

import (
	"os"
	"time"
)

// os.Chmod - On Windows, only the 0200 bit (owner writable) of mode is used; It
// controls whether the file's read-only attribute is set or cleared. The other
// bits are currently unused.
//
// Use mode 0400 for a read-only file and 0600 for a readable+writable file.
func updateMode(path string, mode os.FileMode) error {
	if mode == 0 {
		return nil
	}
	// read+write if owner, group or world writeable
	if mode&0222 != 0 {
		return os.Chmod(path, 0600)
	}
	// otherwise read-only
	return os.Chmod(path, 0400)
}

func updateMtime(path string, mtime time.Time) error {
	if mtime.IsZero() {
		return nil
	}
	return os.Chtimes(path, mtime, mtime)
}
