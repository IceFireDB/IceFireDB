//go:build linux || freebsd || netbsd || openbsd || dragonfly

package files

import (
	"os"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
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
	var AtFdCwd = -100
	pathname, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}

	tm := syscall.NsecToTimespec(mtime.UnixNano())
	ts := [2]syscall.Timespec{tm, tm}
	_, _, e := syscall.Syscall6(syscall.SYS_UTIMENSAT, uintptr(AtFdCwd),
		uintptr(unsafe.Pointer(pathname)), uintptr(unsafe.Pointer(&ts)),
		uintptr(unix.AT_SYMLINK_NOFOLLOW), 0, 0)
	if e != 0 {
		return error(e)
	}
	return nil
}
