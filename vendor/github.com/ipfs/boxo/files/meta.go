package files

import (
	"fmt"
	"os"
	"time"
)

// UpdateMeta sets the unix mode and modification time of the filesystem object
// referenced by path.
func UpdateMeta(path string, mode os.FileMode, mtime time.Time) error {
	if err := UpdateModTime(path, mtime); err != nil {
		return err
	}
	return UpdateFileMode(path, mode)
}

// UpdateUnix sets the unix mode and modification time of the filesystem object
// referenced by path. The mode is in the form of a unix mode.
func UpdateMetaUnix(path string, mode uint32, mtime time.Time) error {
	return UpdateMeta(path, UnixPermsToModePerms(mode), mtime)
}

// UpdateFileMode sets the unix mode of the filesystem object referenced by path.
func UpdateFileMode(path string, mode os.FileMode) error {
	if err := updateMode(path, mode); err != nil {
		return fmt.Errorf("[%v] failed to update file mode on '%s'", err, path)
	}
	return nil
}

// UpdateFileModeUnix sets the unix mode of the filesystem object referenced by
// path. It takes the mode in the form of a unix mode.
func UpdateFileModeUnix(path string, mode uint32) error {
	return UpdateFileMode(path, UnixPermsToModePerms(mode))
}

// UpdateModTime sets the last access and modification time of the target
// filesystem object to the given time. When the given path references a
// symlink, if supported, the symlink is updated.
func UpdateModTime(path string, mtime time.Time) error {
	if err := updateMtime(path, mtime); err != nil {
		return fmt.Errorf("[%v] failed to update last modification time on '%s'", err, path)
	}
	return nil
}
