package files

import "os"

// ToFile is an alias for n.(File). If the file isn't a regular file, nil value
// will be returned
func ToFile(n Node) File {
	f, _ := n.(File)
	return f
}

// ToDir is an alias for n.(Directory). If the file isn't directory, a nil value
// will be returned
func ToDir(n Node) Directory {
	d, _ := n.(Directory)
	return d
}

// FileFromEntry calls ToFile on Node in the given entry
func FileFromEntry(e DirEntry) File {
	return ToFile(e.Node())
}

// DirFromEntry calls ToDir on Node in the given entry
func DirFromEntry(e DirEntry) Directory {
	return ToDir(e.Node())
}

// UnixPermsOrDefault returns the unix style permissions stored for the given
// Node, or default unix permissions for the Node type.
func UnixPermsOrDefault(n Node) uint32 {
	perms := ModePermsToUnixPerms(n.Mode())
	if perms != 0 {
		return perms
	}

	switch n.(type) {
	case *Symlink:
		return 0777
	case Directory:
		return 0755
	default:
		return 0644
	}
}

// ModePermsToUnixPerms converts the permission bits of an os.FileMode to unix
// style mode permissions.
func ModePermsToUnixPerms(fileMode os.FileMode) uint32 {
	return uint32((fileMode & 0xC00000 >> 12) | (fileMode & os.ModeSticky >> 11) | (fileMode & 0x1FF))
}

// UnixPermsToModePerms converts unix style mode permissions to os.FileMode
// permissions, as it only operates on permission bits it does not set the
// underlying type (fs.ModeDir, fs.ModeSymlink, etc.) in the returned
// os.FileMode.
func UnixPermsToModePerms(unixPerms uint32) os.FileMode {
	if unixPerms == 0 {
		return 0
	}
	return os.FileMode((unixPerms & 0x1FF) | (unixPerms & 0xC00 << 12) | (unixPerms & 0x200 << 11))
}
