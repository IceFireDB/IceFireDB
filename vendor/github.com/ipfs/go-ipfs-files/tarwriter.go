package files

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

var (
	ErrUnixFSPathOutsideRoot = errors.New("relative UnixFS paths outside the root are now allowed, use CAR instead")
)

type TarWriter struct {
	TarW       *tar.Writer
	baseDirSet bool
	baseDir    string
}

// NewTarWriter wraps given io.Writer into a new tar writer
func NewTarWriter(w io.Writer) (*TarWriter, error) {
	return &TarWriter{
		TarW: tar.NewWriter(w),
	}, nil
}

func (w *TarWriter) writeDir(f Directory, fpath string) error {
	if err := writeDirHeader(w.TarW, fpath); err != nil {
		return err
	}

	it := f.Entries()
	for it.Next() {
		if err := w.WriteFile(it.Node(), path.Join(fpath, it.Name())); err != nil {
			return err
		}
	}
	return it.Err()
}

func (w *TarWriter) writeFile(f File, fpath string) error {
	size, err := f.Size()
	if err != nil {
		return err
	}

	if err := writeFileHeader(w.TarW, fpath, uint64(size)); err != nil {
		return err
	}

	if _, err := io.Copy(w.TarW, f); err != nil {
		return err
	}
	w.TarW.Flush()
	return nil
}

func validateTarFilePath(baseDir, fpath string) bool {
	// Ensure the filepath has no ".", "..", etc within the known root directory.
	fpath = path.Clean(fpath)

	// If we have a non-empty baseDir, check if the filepath starts with baseDir.
	// If not, we can exclude it immediately. For 'ipfs get' and for the gateway,
	// the baseDir would be '{cid}.tar'.
	if baseDir != "" && !strings.HasPrefix(path.Clean(fpath), baseDir) {
		return false
	}

	// Otherwise, check if the path starts with '..' which would make it fall
	// outside the root path. This works since the path has already been cleaned.
	if strings.HasPrefix(fpath, "..") {
		return false
	}

	return true
}

// WriteNode adds a node to the archive.
func (w *TarWriter) WriteFile(nd Node, fpath string) error {
	if !w.baseDirSet {
		w.baseDirSet = true // Use a variable for this as baseDir may be an empty string.
		w.baseDir = fpath
	}

	if !validateTarFilePath(w.baseDir, fpath) {
		return ErrUnixFSPathOutsideRoot
	}

	switch nd := nd.(type) {
	case *Symlink:
		return writeSymlinkHeader(w.TarW, nd.Target, fpath)
	case File:
		return w.writeFile(nd, fpath)
	case Directory:
		return w.writeDir(nd, fpath)
	default:
		return fmt.Errorf("file type %T is not supported", nd)
	}
}

// Close closes the tar writer.
func (w *TarWriter) Close() error {
	return w.TarW.Close()
}

func writeDirHeader(w *tar.Writer, fpath string) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Typeflag: tar.TypeDir,
		Mode:     0777,
		ModTime:  time.Now().Truncate(time.Second),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

func writeFileHeader(w *tar.Writer, fpath string, size uint64) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Size:     int64(size),
		Typeflag: tar.TypeReg,
		Mode:     0644,
		ModTime:  time.Now().Truncate(time.Second),
		// TODO: set mode, dates, etc. when added to unixFS
	})
}

func writeSymlinkHeader(w *tar.Writer, target, fpath string) error {
	return w.WriteHeader(&tar.Header{
		Name:     fpath,
		Linkname: target,
		Mode:     0777,
		Typeflag: tar.TypeSymlink,
	})
}
