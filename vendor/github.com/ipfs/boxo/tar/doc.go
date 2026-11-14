// Package tar extracts tar archives to the local filesystem.
//
// [Extractor] handles tar files containing regular files, directories, and
// symlinks. Archives must have either a single root file or symlink, or all
// entries nested under a single root directory. File modes and modification
// times from the archive are preserved.
package tar
