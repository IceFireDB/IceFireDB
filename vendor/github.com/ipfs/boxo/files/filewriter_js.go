//go:build js

package files

// GOOS=js does not define syscall.O_NOFOLLOW; createNewFile in
// filewriter_posix.go runs without symlink-traversal protection.
const noFollowFlag = 0
