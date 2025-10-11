package fslock

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	logging "github.com/ipfs/go-log/v2"
	lock "go4.org/lock"
)

// log is the fsrepo logger
var log = logging.Logger("lock")

// LockedError is returned as the inner error type when the lock is already
// taken.
type LockedError string

func (e LockedError) Error() string {
	return string(e)
}

// Lock creates the lock.
func Lock(confdir, lockFileName string) (io.Closer, error) {
	lockFilePath := filepath.Join(confdir, lockFileName)
	lk, err := lock.Lock(lockFilePath)
	if err != nil {
		switch {
		case lockedByOthers(err):
			return lk, &os.PathError{
				Op:   "lock",
				Path: lockFilePath,
				Err:  LockedError("someone else has the lock"),
			}
		case strings.Contains(err.Error(), "already locked"):
			// we hold the lock ourselves
			return lk, &os.PathError{
				Op:   "lock",
				Path: lockFilePath,
				Err:  LockedError("lock is already held by us"),
			}
		case errors.Is(err, fs.ErrPermission) || isLockCreatePermFail(err):
			// lock fails on permissions error

			// Using a path error like this ensures that
			// os.IsPermission works on the returned error.
			return lk, &os.PathError{
				Op:   "lock",
				Path: lockFilePath,
				Err:  os.ErrPermission,
			}
		}
	}
	return lk, err
}

// Locked checks if there is a lock already set.
func Locked(confdir, lockFile string) (bool, error) {
	log.Debugf("Checking lock")
	if !fileExists(filepath.Join(confdir, lockFile)) {
		log.Debugf("File doesn't exist: %s", filepath.Join(confdir, lockFile))
		return false, nil
	}

	lk, err := Lock(confdir, lockFile)
	if err == nil {
		log.Debugf("No one has a lock")
		lk.Close()
		return false, nil
	}

	log.Debug(err)

	if errors.As(err, new(LockedError)) {
		return true, nil
	}
	return false, err
}

// WaitLock keeps trying to acquire the lock that is held by someone else,
// until the lock is acquired or until the context is canceled. Retires once
// per second. Logs warning on each retry.
func WaitLock(ctx context.Context, confdir, lockFileName string) (io.Closer, error) {
	var ticker *time.Ticker

retry:
	lk, err := Lock(confdir, lockFileName)
	if err != nil {
		var lkErr LockedError
		if errors.As(err, &lkErr) && lkErr.Error() == "someone else has the lock" {
			pe, ok := err.(*os.PathError)
			if !ok {
				return nil, err
			}
			log.Warnf("%s: %s. Retrying...", pe.Path, lkErr.Error())
			if ticker == nil {
				ticker = time.NewTicker(time.Second)
				defer ticker.Stop()
			}
			select {
			case <-ctx.Done():
				log.Warnf("did not acquire lock: %s", ctx.Err())
				return nil, err
			case <-ticker.C:
				goto retry
			}
		}
	}
	return lk, err
}

func isLockCreatePermFail(err error) bool {
	s := err.Error()
	return strings.Contains(s, "Lock Create of") && strings.Contains(s, "permission denied")
}

func fileExists(filename string) bool {
	fi, err := os.Lstat(filename)
	if fi != nil || (err != nil && !errors.Is(err, fs.ErrNotExist)) {
		return true
	}
	return false
}
