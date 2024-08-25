package files

import (
	"os"
	"strings"
	"time"
)

type Symlink struct {
	Target string

	mtime  time.Time
	reader strings.Reader
}

func NewLinkFile(target string, stat os.FileInfo) File {
	mtime := time.Time{}
	if stat != nil {
		mtime = stat.ModTime()
	}
	return NewSymlinkFile(target, mtime)
}

func NewSymlinkFile(target string, mtime time.Time) File {
	lf := &Symlink{
		Target: target,
		mtime:  mtime,
	}
	lf.reader.Reset(lf.Target)
	return lf
}

func (lf *Symlink) Mode() os.FileMode {
	return os.ModeSymlink | os.ModePerm
}

func (lf *Symlink) ModTime() time.Time {
	return lf.mtime
}

func (lf *Symlink) Close() error {
	return nil
}

func (lf *Symlink) Read(b []byte) (int, error) {
	return lf.reader.Read(b)
}

func (lf *Symlink) Seek(offset int64, whence int) (int64, error) {
	return lf.reader.Seek(offset, whence)
}

func (lf *Symlink) Size() (int64, error) {
	return lf.reader.Size(), nil
}

func ToSymlink(n Node) *Symlink {
	l, _ := n.(*Symlink)
	return l
}

var _ File = &Symlink{}
