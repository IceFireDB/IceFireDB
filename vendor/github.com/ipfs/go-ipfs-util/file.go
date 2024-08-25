package util

import "os"

// FileExists check if the file with the given path exits.
//
// Deprecated: use github.com/ipfs/boxo/util.FileExists
func FileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !os.IsNotExist(err)) {
		return true
	}
	return false
}
