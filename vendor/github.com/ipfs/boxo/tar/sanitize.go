//go:build !windows

package tar

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func isNullDevice(path string) bool {
	return path == os.DevNull
}

func validatePlatformPath(platformPath string) error {
	if strings.Contains(platformPath, "\x00") {
		return fmt.Errorf("invalid platform path: path components cannot contain null: %q", platformPath)
	}
	return nil
}

func validatePathComponent(c string) error {
	if c == ".." {
		return errors.New("invalid platform path: path component cannot be '..'")
	}
	if strings.Contains(c, "\x00") {
		return fmt.Errorf("invalid platform path: path components cannot contain null: %q", c)
	}
	return nil
}
