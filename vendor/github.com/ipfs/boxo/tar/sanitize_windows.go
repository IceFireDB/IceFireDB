package tar

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// NOTE: `/` is not included, it is already reserved as the standard path separator.
const reservedRunes = `<>:"\|?*` + "\x00"

// https://msdn.microsoft.com/en-us/library/windows/desktop/aa365247(v=vs.85).aspx
var reservedNames = [...]string{
	"CON", "PRN", "AUX", "NUL", "COM1", "COM2",
	"COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
	"COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
	"LPT6", "LPT7", "LPT8", "LPT9",
}

func isNullDevice(path string) bool {
	// "NUL" is standard but the device is case insensitive.
	// Normalize to upper for the compare.
	const nameLen = len(os.DevNull)
	return len(path) == nameLen && strings.ToUpper(path) == os.DevNull
}

// validatePathComponent returns an error if the given path component is not allowed on the platform.
func validatePathComponent(component string) error {
	const invalidPathErr = "invalid platform path"
	for _, suffix := range [...]string{
		".", // MSDN: Do not end a file or directory
		" ", // name with a space or a period.
	} {
		if strings.HasSuffix(component, suffix) {
			return fmt.Errorf(
				`%s: path components cannot end with '%s': "%s"`,
				invalidPathErr, suffix, component,
			)
		}
	}
	if strings.ContainsAny(component, reservedRunes) {
		return fmt.Errorf(
			`%s: path components cannot contain any of "%s": "%s"`,
			invalidPathErr, reservedRunes, component,
		)
	}
	if slices.Contains(reservedNames[:], strings.ToUpper(component)) {
		return fmt.Errorf(
			`%s: path component is a reserved name: "%s"`,
			invalidPathErr, component,
		)
	}
	return nil
}

func validatePlatformPath(nativePath string) error {
	normalized := normalizeToGoPath(nativePath)
	for component := range strings.SplitSeq(normalized, "/") {
		if err := validatePathComponent(component); err != nil {
			return err
		}
	}
	return nil
}

func normalizeToGoPath(nativePath string) string {
	var (
		volumeName     = filepath.VolumeName(nativePath)
		relativeNative = nativePath[len(volumeName):]
		goPath         = filepath.ToSlash(relativeNative)
		relativeGo     = strings.Trim(goPath, "/")
	)
	return relativeGo
}
