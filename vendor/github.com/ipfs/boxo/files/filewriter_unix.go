//go:build darwin || linux || netbsd || openbsd || freebsd || dragonfly || wasip1

package files

import "syscall"

// noFollowFlag adds O_NOFOLLOW to createNewFile in filewriter_posix.go,
// preventing symlink-traversal races where syscall supports the flag.
// GOOS=js falls back to 0 (no protection).
const noFollowFlag = syscall.O_NOFOLLOW
