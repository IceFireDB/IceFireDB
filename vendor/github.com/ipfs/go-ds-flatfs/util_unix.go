//go:build !windows

package flatfs

import (
	"os"
)

func tempFileOnce(dir, pattern string) (*os.File, error) {
	return os.CreateTemp(dir, pattern)
}

func readFileOnce(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}
