package tar

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ipfs/boxo/files"
)

var (
	errTraverseSymlink          = errors.New("cannot traverse symlinks")
	errInvalidRoot              = errors.New("tar has invalid root")
	errInvalidRootMultipleRoots = fmt.Errorf("contains more than one root or the root directory is not the first entry : %w", errInvalidRoot)
)

// Extractor is used for extracting tar files to a filesystem.
//
// The Extractor can only extract tar files containing files, directories and
// symlinks. Additionally, the tar files must either have a single file, or
// symlink in them, or must have all of its objects inside of a single root
// directory object.
//
// If the tar file contains a single file/symlink then it will try and extract
// it with semantics similar to Linux's `cp`. In particular, the name of the
// extracted file/symlink will match the extraction path. If the extraction
// path is a directory then it will extract into the directory using its
// original name.
//
// If an associated mode and last modification time was stored in the archive
// it is restored.
//
// Overwriting: Extraction of files and symlinks will result in overwriting the
// existing objects with the same name when possible (i.e. other files,
// symlinks, and empty directories).
type Extractor struct {
	Path            string
	Progress        func(int64) int64
	deferredUpdates []deferredUpdate
}

// Extract extracts a tar file to the file system. See the Extractor for more
// information on the limitations on the tar files that can be extracted.
func (te *Extractor) Extract(reader io.Reader) error {
	if isNullDevice(te.Path) {
		return nil
	}

	tarReader := tar.NewReader(reader)

	header, err := tarReader.Next()
	if err != nil && err != io.EOF {
		return err
	}
	if header == nil || err == io.EOF {
		return errors.New("empty tar file")
	}

	te.deferredUpdates = make([]deferredUpdate, 0, 80)
	doUpdates := func() error {
		for i := len(te.deferredUpdates) - 1; i >= 0; i-- {
			m := te.deferredUpdates[i]
			err := files.UpdateMetaUnix(m.path, uint32(m.mode), m.mtime)
			if err != nil {
				return err
			}
		}
		te.deferredUpdates = nil
		return nil
	}
	defer func() { err = doUpdates() }()

	// Specially handle the first entry assuming it is a single root object
	// (e.g. root directory, single file, or single symlink).

	// track what the root tar path is so we can ensure that all other entries
	// are below the root.
	if strings.Contains(header.Name, "/") {
		return fmt.Errorf("root name contains multiple components : %q : %w", header.Name, errInvalidRoot)
	}
	switch header.Name {
	case "", ".", "..":
		return fmt.Errorf("invalid root path: %q : %w", header.Name, errInvalidRoot)
	}
	rootName := header.Name

	// Get the platform-specific output path.
	rootOutputPath := filepath.Clean(te.Path)
	if err := validatePlatformPath(rootOutputPath); err != nil {
		return err
	}

	var firstObjectWasDir bool

	// If the last element in the rootOutputPath (which is passed by the user)
	// is a symlink do not follow it this makes it easier for users to reason
	// about where files are getting extracted to even when the tar is not from
	// a trusted source
	//
	// For example, if the user extracts a mutable link to a tar file
	// (http://sometimesbad.tld/t.tar) and situationally it contains a folder,
	// file, or symlink the outputs could hop around the user's file system.
	// This is especially annoying since we allow symlinks to point anywhere a
	// user might want them to.
	switch header.Typeflag {
	case tar.TypeDir:
		// if this is the root directory, use it as the output path for
		// remaining files.
		firstObjectWasDir = true
		if err := te.extractDir(rootOutputPath); err != nil {
			return err
		}
		if err := te.deferUpdate(rootOutputPath, header); err != nil {
			return err
		}
	case tar.TypeReg, tar.TypeSymlink:
		// Check if the output path already exists, so we know whether we
		// should create our output with that name, or if we should put the
		// output inside a preexisting directory.

		rootIsExistingDirectory := false
		// We do not follow links here
		if stat, err := os.Lstat(rootOutputPath); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
		} else if stat.IsDir() {
			rootIsExistingDirectory = true
		}

		outputPath := rootOutputPath
		// If the root is a directory which already exists then put the
		// file/symlink in the directory.
		if rootIsExistingDirectory {
			// make sure the root has a valid name.
			if err := validatePathComponent(rootName); err != nil {
				return err
			}

			// If the output path directory exists then put the file/symlink
			// into the directory.
			outputPath = filepath.Join(rootOutputPath, rootName)
		}

		// If an object with the target name already exists overwrite it.
		if header.Typeflag == tar.TypeReg {
			if err := te.extractFile(outputPath, tarReader); err != nil {
				return err
			}
			if err := files.UpdateMetaUnix(outputPath, uint32(header.Mode), header.ModTime); err != nil {
				return err
			}
		} else if err := te.extractSymlink(outputPath, rootOutputPath, header); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized tar header type: %d", header.Typeflag)
	}

	// files come recursively in order.
	for {
		header, err := tarReader.Next()
		if err != nil && err != io.EOF {
			return err
		}
		if header == nil || err == io.EOF {
			break
		}

		// Make sure that we only have a single root element.
		if !firstObjectWasDir {
			return fmt.Errorf("the root was not a directory and the tar has multiple entries: %w", errInvalidRoot)
		}

		// validate the path in order to remove paths we refuse to work with
		// and make it easier to reason about.
		if err := validateTarPath(header.Name); err != nil {
			return err
		}
		cleanedPath := header.Name

		relPath, err := getRelativePath(rootName, cleanedPath)
		if err != nil {
			return err
		}

		outputPath, err := te.outputPath(rootOutputPath, relPath)
		if err != nil {
			return err
		}

		// This check should already be covered by previous validation, but may
		// catch bugs that slip through. Checks if the relative path matches or
		// exceeds the root We check for matching because the outputPath
		// function strips the original root
		rel, err := filepath.Rel(rootOutputPath, outputPath)
		if err != nil || rel == "." {
			return errInvalidRootMultipleRoots
		}
		for _, e := range strings.Split(filepath.ToSlash(rel), "/") {
			if e == ".." {
				return errors.New("relative path contains '..'")
			}
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := te.extractDir(outputPath); err != nil {
				return err
			}
			if err := te.deferUpdate(outputPath, header); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := te.extractFile(outputPath, tarReader); err != nil {
				return err
			}
			if err := files.UpdateMetaUnix(outputPath, uint32(header.Mode), header.ModTime); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := te.extractSymlink(outputPath, rootOutputPath, header); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized tar header type: %d", header.Typeflag)
		}
	}
	return nil
}

// validateTarPath returns an error if the path has problematic characters.
func validateTarPath(tarPath string) error {
	if len(tarPath) == 0 {
		return errors.New("path is empty")
	}

	if tarPath[0] == '/' {
		return fmt.Errorf("%q : path starts with '/'", tarPath)
	}

	elems := strings.Split(tarPath, "/") // break into elems
	for _, e := range elems {
		switch e {
		case "", ".", "..":
			return fmt.Errorf("%q : path contains %q", tarPath, e)
		}
	}
	return nil
}

// getRelativePath returns the relative path between rootTarPath and tarPath.
// Assumes both paths have been cleaned. Will error if the tarPath is not below
// the rootTarPath.
func getRelativePath(rootName, tarPath string) (string, error) {
	if !strings.HasPrefix(tarPath, rootName+"/") {
		return "", errInvalidRootMultipleRoots
	}
	return tarPath[len(rootName)+1:], nil
}

// outputPath returns the directory path at which to place the file
// relativeTarPath. Assumes relativeTarPath is cleaned.
func (te *Extractor) outputPath(basePlatformPath, relativeTarPath string) (string, error) {
	elems := strings.Split(relativeTarPath, "/")

	platformPath := basePlatformPath
	for i, e := range elems {
		if err := validatePathComponent(e); err != nil {
			return "", err
		}
		platformPath = filepath.Join(platformPath, e)

		// Last element is not checked since it will be removed (if it exists)
		// by any of the extraction functions. For more details see:
		// https://github.com/libarchive/libarchive/blob/0fd2ed25d78e9f4505de5dcb6208c6c0ff8d2edb/libarchive/archive_write_disk_posix.c#L2810
		if i == len(elems)-1 {
			break
		}

		fi, err := os.Lstat(platformPath)
		if err != nil {
			return "", err
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			return "", errTraverseSymlink
		}
		if !fi.Mode().IsDir() {
			return "", errors.New("cannot traverse non-directory objects")
		}
	}

	return platformPath, nil
}

var errExtractedDirToSymlink = errors.New("cannot extract to symlink")

func (te *Extractor) extractDir(path string) error {
	err := os.MkdirAll(path, 0o755)
	if err != nil {
		return err
	}

	stat, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return errExtractedDirToSymlink
	}
	return nil
}

func (te *Extractor) extractSymlink(path, rootPath string, h *tar.Header) error {
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Before extracting a file or other symlink, the old path is removed to
	// prevent a simlink being created that causes a subsequent extraction to
	// escape the root.
	//
	// Each element of the path of the symlink being extracted is evaluated to
	// ensure that there is not a symlink at any point in the path. This is
	// done in outputPath.
	err = os.Symlink(h.Linkname, path)
	if err != nil {
		return err
	}

	switch runtime.GOOS {
	case "linux", "freebsd", "netbsd", "openbsd", "dragonfly":
		return files.UpdateModTime(path, h.ModTime)
	default:
		return nil
	}
}

func (te *Extractor) extractFile(path string, r *tar.Reader) error {
	// Attempt removing the target so we can overwrite files, symlinks and
	// empty directories.
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Create a temporary file in the target directory and then rename the
	// temporary file to the target to better deal with races on the file
	// system.
	base := filepath.Dir(path)
	tmpfile, err := os.CreateTemp(base, "")
	if err != nil {
		return err
	}
	if err = copyWithProgress(tmpfile, r, te.Progress); err != nil {
		_ = tmpfile.Close()
		_ = os.Remove(tmpfile.Name())
		return err
	}
	if err = tmpfile.Close(); err != nil {
		_ = os.Remove(tmpfile.Name())
		return err
	}

	if err = os.Rename(tmpfile.Name(), path); err != nil {
		_ = os.Remove(tmpfile.Name())
		return err
	}

	return nil
}

func copyWithProgress(to io.Writer, from io.Reader, cb func(int64) int64) error {
	buf := make([]byte, 4096)
	for {
		n, err := from.Read(buf)
		if n != 0 {
			if cb != nil {
				cb(int64(n))
			}
			_, err2 := to.Write(buf[:n])
			if err2 != nil {
				return err2
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

type deferredUpdate struct {
	path  string
	mode  int64
	mtime time.Time
}

func (te *Extractor) deferUpdate(path string, header *tar.Header) error {
	if header.Mode == 0 && header.ModTime.IsZero() {
		return nil
	}

	prefix := func() string {
		for i := len(path) - 1; i >= 0; i-- {
			if path[i] == '/' {
				return path[:i]
			}
		}
		return path
	}

	n := len(te.deferredUpdates)
	if n > 0 && len(path) < len(te.deferredUpdates[n-1].path) {
		// if possible, apply the previous deferral.
		m := te.deferredUpdates[n-1]
		if strings.HasPrefix(m.path, prefix()) {
			err := files.UpdateMetaUnix(m.path, uint32(m.mode), m.mtime)
			if err != nil {
				return err
			}
			te.deferredUpdates = te.deferredUpdates[:n-1]
		}
	}

	te.deferredUpdates = append(te.deferredUpdates, deferredUpdate{
		path:  path,
		mode:  header.Mode,
		mtime: header.ModTime,
	})

	return nil
}
