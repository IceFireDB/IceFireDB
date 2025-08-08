//go:build !plan9

package flatfs

import "os"

func rename(a, b string) error {
	return os.Rename(a, b)
}
