package files

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// SerialFileOptions configures file traversal behavior for NewSerialFileWithOptions.
type SerialFileOptions struct {
	// Filter determines which files to include or exclude during traversal.
	// If nil, all files are included.
	Filter *Filter

	// DereferenceSymlinks controls symlink handling during file traversal.
	// When false (default), symlinks are stored as UnixFS nodes with
	// Data.Type=symlink (4) containing the target path as specified in
	// https://specs.ipfs.tech/unixfs/
	// When true, symlinks are dereferenced and replaced with their target:
	// symlinks to files become regular file nodes, symlinks to directories
	// are traversed recursively.
	DereferenceSymlinks bool
}

// serialFile implements Node, and reads from a path on the OS filesystem.
// No more than one file will be opened at a time.
type serialFile struct {
	path                string
	files               []os.FileInfo
	stat                os.FileInfo
	filter              *Filter
	dereferenceSymlinks bool
}

type serialIterator struct {
	files               []os.FileInfo
	path                string
	filter              *Filter
	dereferenceSymlinks bool

	curName string
	curFile Node

	err error
}

var (
	_ Directory   = (*serialFile)(nil)
	_ DirIterator = (*serialIterator)(nil)
)

// NewSerialFile takes a filepath, a bool specifying if hidden files should be included,
// and a fileInfo and returns a Node representing file, directory or special file.
func NewSerialFile(path string, includeHidden bool, stat os.FileInfo) (Node, error) {
	filter, err := NewFilter("", nil, includeHidden)
	if err != nil {
		return nil, err
	}
	return NewSerialFileWithFilter(path, filter, stat)
}

// NewSerialFileWithFilter takes a filepath, a filter for determining which files should be
// operated upon if the filepath is a directory, and a fileInfo and returns a
// Node representing file, directory or special file.
func NewSerialFileWithFilter(path string, filter *Filter, stat os.FileInfo) (Node, error) {
	return NewSerialFileWithOptions(path, stat, SerialFileOptions{Filter: filter})
}

// NewSerialFileWithOptions creates a Node from a filesystem path with configurable options.
// The stat parameter should be obtained via os.Lstat (not os.Stat) to correctly detect symlinks.
func NewSerialFileWithOptions(path string, stat os.FileInfo, opts SerialFileOptions) (Node, error) {
	// If dereferencing symlinks and this is a symlink, stat the target instead
	if opts.DereferenceSymlinks && stat.Mode()&os.ModeSymlink != 0 {
		targetStat, err := os.Stat(path) // follows symlink
		if err != nil {
			return nil, err
		}
		stat = targetStat
	}

	switch mode := stat.Mode(); {
	case mode.IsRegular():
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return NewReaderPathFile(path, file, stat)
	case mode.IsDir():
		// for directories, stat all of the contents first, so we know what files to
		// open when Entries() is called
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		contents := make([]fs.FileInfo, 0, len(entries))
		for _, entry := range entries {
			content, err := entry.Info()
			if err != nil {
				return nil, err
			}
			contents = append(contents, content)
		}
		return &serialFile{
			path:                path,
			files:               contents,
			stat:                stat,
			filter:              opts.Filter,
			dereferenceSymlinks: opts.DereferenceSymlinks,
		}, nil
	case mode&os.ModeSymlink != 0:
		// Only reached if DereferenceSymlinks is false
		target, err := os.Readlink(path)
		if err != nil {
			return nil, err
		}
		return NewLinkFile(target, stat), nil
	default:
		return nil, fmt.Errorf("unrecognized file type for %s: %s", path, mode.String())
	}
}

func (it *serialIterator) Name() string {
	return it.curName
}

func (it *serialIterator) Node() Node {
	return it.curFile
}

func (it *serialIterator) Next() bool {
	// if there aren't any files left in the root directory, we're done
	if len(it.files) == 0 {
		return false
	}

	stat := it.files[0]
	it.files = it.files[1:]
	for it.filter != nil && it.filter.ShouldExclude(stat) {
		if len(it.files) == 0 {
			return false
		}

		stat = it.files[0]
		it.files = it.files[1:]
	}

	// open the next file
	filePath := filepath.ToSlash(filepath.Join(it.path, stat.Name()))

	// recursively call the constructor on the next file
	// if it's a regular file, we will open it as a ReaderFile
	// if it's a directory, files in it will be opened serially
	sf, err := NewSerialFileWithOptions(filePath, stat, SerialFileOptions{
		Filter:              it.filter,
		DereferenceSymlinks: it.dereferenceSymlinks,
	})
	if err != nil {
		it.err = err
		return false
	}

	it.curName = stat.Name()
	it.curFile = sf
	return true
}

func (it *serialIterator) Err() error {
	return it.err
}

func (f *serialFile) Entries() DirIterator {
	return &serialIterator{
		path:                f.path,
		files:               f.files,
		filter:              f.filter,
		dereferenceSymlinks: f.dereferenceSymlinks,
	}
}

func (f *serialFile) Close() error {
	return nil
}

func (f *serialFile) Stat() os.FileInfo {
	return f.stat
}

func (f *serialFile) Size() (int64, error) {
	if !f.stat.IsDir() {
		// something went terribly, terribly wrong
		return 0, errors.New("serialFile is not a directory")
	}

	var du int64
	err := filepath.Walk(f.path, func(p string, fi os.FileInfo, err error) error {
		if err != nil || fi == nil {
			return err
		}

		if f.filter != nil && f.filter.ShouldExclude(fi) {
			if fi.Mode().IsDir() {
				return filepath.SkipDir
			}
		} else if fi.Mode().IsRegular() {
			du += fi.Size()
		}

		return nil
	})

	return du, err
}

func (f *serialFile) Mode() os.FileMode {
	return f.stat.Mode()
}

func (f *serialFile) ModTime() time.Time {
	return f.stat.ModTime()
}
