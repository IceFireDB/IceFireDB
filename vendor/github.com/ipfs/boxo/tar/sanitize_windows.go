package tar

import (
	"fmt"
	"path/filepath"
	"strings"
)

// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
var reservedNames = [...]string{"CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"}

const reservedCharsStr = `[<>:"\|?*]` + "\x00" // NOTE: `/` is not included as it is our standard path separator

func isNullDevice(path string) bool {
	// This is a case insensitive comparison to NUL
	if len(path) != 3 {
		return false
	}
	if path[0]|0x20 != 'n' {
		return false
	}
	if path[1]|0x20 != 'u' {
		return false
	}
	if path[2]|0x20 != 'l' {
		return false
	}
	return true
}

// validatePathComponent returns an error if the given path component is not allowed on the platform
func validatePathComponent(c string) error {
	// MSDN: Do not end a file or directory name with a space or a period
	if strings.HasSuffix(c, ".") {
		return fmt.Errorf("invalid platform path: path components cannot end with '.' : %q", c)
	}
	if strings.HasSuffix(c, " ") {
		return fmt.Errorf("invalid platform path: path components cannot end with ' ' : %q", c)
	}

	if c == ".." {
		return fmt.Errorf("invalid platform path: path component cannot be '..'")
	}
	// error on reserved characters
	if strings.ContainsAny(c, reservedCharsStr) {
		return fmt.Errorf("invalid platform path: path components cannot contain any of %s : %q", reservedCharsStr, c)
	}

	// error on reserved names
	for _, rn := range reservedNames {
		if c == rn {
			return fmt.Errorf("invalid platform path: path component is a reserved name: %s", c)
		}
	}

	return nil
}

func validatePlatformPath(platformPath string) error {
	// remove the volume name
	p := platformPath[len(filepath.VolumeName(platformPath)):]

	// convert to cleaned slash-path
	p = filepath.ToSlash(p)
	p = strings.Trim(p, "/")

	// make sure all components of the path are valid
	for _, e := range strings.Split(p, "/") {
		if err := validatePathComponent(e); err != nil {
			return err
		}
	}
	return nil
}
