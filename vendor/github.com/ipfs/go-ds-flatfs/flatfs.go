// Package flatfs is a Datastore implementation that stores all
// objects in a two-level directory structure in the local file
// system, regardless of the hierarchy of the keys.
//
// This is a special-purpose datastore designed exclusively for content-addressed
// storage (CID:block pairs). It assumes keys are derived from cryptographic hashes
// of values, meaning the same key always maps to the same value.
//
// Write semantics: flatfs uses "first-successful-writer-wins" for concurrent or
// duplicate writes to the same key. This is safe for content-addressed data where
// identical keys guarantee identical values. For mutable data requiring
// last-writer-wins semantics, use go-ds-leveldb or go-ds-pebble instead.
package flatfs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("flatfs")

const (
	extension                  = ".data"
	diskUsageMessageTimeout    = 5 * time.Second
	diskUsageCheckpointPercent = 1.0
	diskUsageCheckpointTimeout = 2 * time.Second

	// maxConcurrentPuts limits the number of parallel async writes during
	// batch operations. Each Put spawns a goroutine that writes to a temp
	// file; this constant bounds how many can run simultaneously.
	//
	// The value 16 balances several constraints:
	//   - macOS default file descriptor limit is only 256, so we stay conservative
	//   - provides good parallelism for SSDs (common case) without overwhelming HDDs
	//   - keeps memory overhead bounded (~16-32 MiB with typical 1-2 MiB block sizes)
	//   - matches typical "conservative concurrent I/O" defaults in other tools
	maxConcurrentPuts = 16
)

var (
	// DiskUsageFile is the name of the file to cache the size of the
	// datastore in disk
	DiskUsageFile = "diskUsage.cache"
	// DiskUsageFilesAverage is the maximum number of files per folder
	// to stat in order to calculate the size of the datastore.
	// The size of the rest of the files in a folder will be assumed
	// to be the average of the values obtained. This includes
	// regular files and directories.
	DiskUsageFilesAverage = 2000
	// DiskUsageCalcTimeout is the maximum time to spend
	// calculating the DiskUsage upon a start when no
	// DiskUsageFile is present.
	// If this period did not suffice to read the size of the datastore,
	// the remaining sizes will be estimated.
	DiskUsageCalcTimeout = 5 * time.Minute
	// RetryDelay is a timeout for a backoff on retrying operations
	// that fail due to transient errors like too many file descriptors open.
	RetryDelay = time.Millisecond * 200

	// RetryAttempts is the maximum number of retries that will be attempted
	// before giving up.
	RetryAttempts = 6
)

const (
	opPut = iota
	opDelete
	opRename
)

type initAccuracy string

const (
	unknownA  initAccuracy = "unknown"
	exactA    initAccuracy = "initial-exact"
	approxA   initAccuracy = "initial-approximate"
	timedoutA initAccuracy = "initial-timed-out"
)

func combineAccuracy(a, b initAccuracy) initAccuracy {
	if a == unknownA || b == unknownA {
		return unknownA
	}
	if a == timedoutA || b == timedoutA {
		return timedoutA
	}
	if a == approxA || b == approxA {
		return approxA
	}
	if a == exactA && b == exactA {
		return exactA
	}
	if a == "" {
		return b
	}
	if b == "" {
		return a
	}
	return unknownA
}

var _ datastore.Datastore = (*Datastore)(nil)
var _ datastore.PersistentDatastore = (*Datastore)(nil)
var _ datastore.Batching = (*Datastore)(nil)
var _ datastore.Batch = (*flatfsBatch)(nil)
var _ DiscardableBatch = (*flatfsBatch)(nil)
var _ BatchReader = (*flatfsBatch)(nil)

var (
	ErrDatastoreExists       = errors.New("datastore already exists")
	ErrDatastoreDoesNotExist = errors.New("datastore directory does not exist")
	ErrShardingFileMissing   = fmt.Errorf("%s file not found in datastore", SHARDING_FN)
	ErrClosed                = errors.New("datastore closed")
	ErrInvalidKey            = errors.New("key not supported by flatfs")
)

// Datastore implements the go-datastore Interface.
// Note this datastore cannot guarantee order of concurrent
// write operations to the same key. See the explanation in
// Put().
type Datastore struct {
	// atomic operations should always be used with diskUsage.
	// Must be first in struct to ensure correct alignment
	// (see https://golang.org/pkg/sync/atomic/#pkg-note-BUG)
	diskUsage int64

	path     string
	tempPath string

	shardStr string
	getDir   ShardFunc

	// synchronize all writes and directory changes for added safety
	sync bool

	// these values should only be used during internalization or
	// inside the checkpoint loop
	dirty       bool
	storedValue diskUsageValue

	// Used to trigger a checkpoint.
	checkpointCh chan struct{}
	done         chan struct{}

	shutdownLock sync.RWMutex
	shutdown     bool

	// opMap handles concurrent write operations (put/delete)
	// to the same key
	opMap *opMap
}

type diskUsageValue struct {
	DiskUsage int64        `json:"diskUsage"`
	Accuracy  initAccuracy `json:"accuracy"`
}

type ShardFunc func(string) string

type opT int

// op wraps useful arguments of write operations
type op struct {
	typ  opT           // operation type
	key  datastore.Key // datastore key. Mandatory.
	tmp  string        // temp file path
	path string        // file path
	v    []byte        // value
}

// opMap is a synchronisation structure where a single op can be stored
// for each key.
type opMap struct {
	ops sync.Map
}

type opResult struct {
	mu      sync.RWMutex
	success bool

	opMap *opMap
	name  string
}

// Begins starts the processing of an op:
// - if no other op for the same key exist, register it and return immediately
// - if another op exist for the same key, wait until it's done:
//   - if that previous op succeeded, consider that ours shouldn't execute and return nil
//   - if that previous op failed, start ours
func (m *opMap) Begin(name string) *opResult {
	for {
		myOp := &opResult{opMap: m, name: name}
		myOp.mu.Lock()
		opIface, loaded := m.ops.LoadOrStore(name, myOp)
		if !loaded { // no one else doing ops with this key
			return myOp
		}

		op := opIface.(*opResult)
		// someone else doing ops with this key, wait for the
		// result Note: we are using `op.mu` as a syncing
		// primitive to make several threads WAIT. The first
		// operation will grab the write-lock. Everyone else
		// tries to grab a read-lock as a way of waiting for
		// the operation to be finished by the thead that
		// grabbed the write-lock. The Read-lock does not need
		// to be unlocked, as the operation is never used or
		// re-used for anything else from that point.
		op.mu.RLock()
		if op.success {
			return nil
		}

		// if we are here, we will retry the operation
	}
}

func (o *opResult) Finish(ok bool) {
	o.success = ok
	o.opMap.ops.Delete(o.name)
	o.mu.Unlock()
}

func Create(path string, fun *ShardIdV1) error {
	err := os.Mkdir(path, 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	dsFun, err := ReadShardFunc(path)
	switch err {
	case ErrShardingFileMissing:
		isEmpty, err := DirIsEmpty(path)
		if err != nil {
			return err
		}
		if !isEmpty {
			return fmt.Errorf("directory missing %s file: %s", SHARDING_FN, path)
		}

		err = WriteShardFunc(path, fun)
		if err != nil {
			return err
		}
		err = WriteReadme(path, fun)
		return err
	case nil:
		if fun.String() != dsFun.String() {
			return fmt.Errorf("specified shard func '%s' does not match repo shard func '%s'",
				fun.String(), dsFun.String())
		}
		return ErrDatastoreExists
	default:
		return err
	}
}

func Open(path string, syncFiles bool) (*Datastore, error) {
	_, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrDatastoreDoesNotExist
	} else if err != nil {
		return nil, err
	}

	tempPath := filepath.Join(path, ".temp")
	err = os.RemoveAll(tempPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("failed to remove temporary directory: %v", err)
	}

	err = os.Mkdir(tempPath, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary directory: %v", err)
	}

	shardId, err := ReadShardFunc(path)
	if err != nil {
		return nil, err
	}

	fs := &Datastore{
		path:         path,
		tempPath:     tempPath,
		shardStr:     shardId.String(),
		getDir:       shardId.Func(),
		sync:         syncFiles,
		checkpointCh: make(chan struct{}, 1),
		done:         make(chan struct{}),
		diskUsage:    0,
		opMap:        new(opMap),
	}

	// This sets diskUsage to the correct value
	// It might be slow, but allowing it to happen
	// while the datastore is usable might
	// cause diskUsage to not be accurate.
	err = fs.calculateDiskUsage()
	if err != nil {
		// Cannot stat() all
		// elements in the datastore.
		return nil, err
	}

	go fs.checkpointLoop()
	return fs, nil
}

// convenience method
func CreateOrOpen(path string, fun *ShardIdV1, sync bool) (*Datastore, error) {
	err := Create(path, fun)
	if err != nil && err != ErrDatastoreExists {
		return nil, err
	}
	return Open(path, sync)
}

func (fs *Datastore) ShardStr() string {
	return fs.shardStr
}

// encode returns the directory and file names for a given key according to
// the sharding function.
func (fs *Datastore) encode(key datastore.Key) (dir, file string) {
	noslash := key.String()[1:]
	dir = filepath.Join(fs.path, fs.getDir(noslash))
	file = filepath.Join(dir, noslash+extension)
	return dir, file
}

// decode returns the datastore.Key corresponding to a file name, according
// to the sharding function.
func (fs *Datastore) decode(file string) (key datastore.Key, ok bool) {
	if !strings.HasSuffix(file, extension) {
		// We expect random files like "put-". Log when we encounter
		// others.
		if !strings.HasPrefix(file, "put-") {
			log.Warnw("failed to decode flatfs filename", "file", file)
		}
		return datastore.Key{}, false
	}
	name := file[:len(file)-len(extension)]
	return datastore.NewKey(name), true
}

// makeDir is identical to makeDirNoSync but also enforce the sync
// if required by the config.
func (fs *Datastore) makeDir(dir string) error {
	created, err := fs.makeDirNoSync(dir)
	if err != nil {
		return err
	}

	// In theory, if we create a new prefix dir and add a file to
	// it, the creation of the prefix dir itself might not be
	// durable yet. Sync the root dir after a successful mkdir of
	// a prefix dir, just to be paranoid.
	if fs.sync && created {
		if err := syncDir(fs.path); err != nil {
			return err
		}
	}
	return nil
}

// makeDirNoSync create a directory on disk and report if it was created or
// already existed.
func (fs *Datastore) makeDirNoSync(dir string) (created bool, err error) {
	if err := os.Mkdir(dir, 0755); err != nil {
		if os.IsExist(err) {
			return false, nil
		}
		return false, err
	}

	// Track DiskUsage of this NEW folder
	fs.updateDiskUsage(dir, true)
	return true, nil
}

// This function always runs under an opLock. Therefore, only one thread is
// touching the affected files.
func (fs *Datastore) renameAndUpdateDiskUsage(tmpPath, path string) error {
	fi, err := os.Stat(path)

	// Destination exists, we need to discount it from diskUsage
	if fi != nil && err == nil {
		atomic.AddInt64(&fs.diskUsage, -fi.Size())
	} else if !os.IsNotExist(err) {
		return err
	}

	// Rename and add new file's diskUsage. If the rename fails,
	// it will either a) Re-add the size of an existing file, which
	// was subtracted before b) Add 0 if there is no existing file.
	for i := 0; i < RetryAttempts; i++ {
		err = rename(tmpPath, path)
		// if there's no error, or the source file doesn't exist, abort.
		if err == nil || os.IsNotExist(err) {
			break
		}
		// Otherwise, this could be a transient error due to some other
		// process holding open one of the files. Wait a bit and then
		// retry.
		time.Sleep(time.Duration(i+1) * RetryDelay)
	}
	fs.updateDiskUsage(path, true)
	return err
}

// Put stores a key/value in the datastore.
//
// Note, that we do not guarantee order of write operations (Put or Delete)
// to the same key in this datastore.
//
// For example. i.e. in the case of two concurrent Put, we only guarantee
// that one of them will come through, but cannot assure which one even if
// one arrived slightly later than the other. In the case of a
// concurrent Put and a Delete operation, we cannot guarantee which one
// will win.
func (fs *Datastore) Put(ctx context.Context, key datastore.Key, value []byte) error {
	if !keyIsValid(key) {
		return fmt.Errorf("when putting '%q': %v", key, ErrInvalidKey)
	}

	fs.shutdownLock.RLock()
	defer fs.shutdownLock.RUnlock()
	if fs.shutdown {
		return ErrClosed
	}

	_, err := fs.doWriteOp(&op{
		typ: opPut,
		key: key,
		v:   value,
	})
	return err
}

func (fs *Datastore) Sync(ctx context.Context, prefix datastore.Key) error {
	fs.shutdownLock.RLock()
	defer fs.shutdownLock.RUnlock()
	if fs.shutdown {
		return ErrClosed
	}

	return nil
}

func (fs *Datastore) doOp(oper *op) error {
	switch oper.typ {
	case opPut:
		return fs.doPut(oper.key, oper.v)
	case opDelete:
		return fs.doDelete(oper.key)
	case opRename:
		return fs.renameAndUpdateDiskUsage(oper.tmp, oper.path)
	default:
		panic("bad operation, this is a bug")
	}
}

func isTooManyFDError(err error) bool {
	perr, ok := err.(*os.PathError)
	if ok && perr.Err == syscall.EMFILE {
		return true
	}

	return false
}

// doWrite optimizes out write operations (put/delete) to the same
// key by queueing them and succeeding all queued
// operations if one of them does. In such case,
// we assume that the first succeeding operation
// on that key was the last one to happen after
// all successful others.
//
// done is true if we actually performed the operation, false if we skipped or
// failed.
func (fs *Datastore) doWriteOp(oper *op) (done bool, err error) {
	keyStr := oper.key.String()

	opRes := fs.opMap.Begin(keyStr)
	if opRes == nil { // nothing to do, a concurrent op succeeded
		return false, nil
	}

	err = fs.doOp(oper)

	// Finish it. If no error, it will signal other operations
	// waiting on this result to succeed. Otherwise, they will
	// retry.
	opRes.Finish(err == nil)
	return err == nil, err
}

func (fs *Datastore) doPut(key datastore.Key, val []byte) error {

	dir, path := fs.encode(key)
	if err := fs.makeDir(dir); err != nil {
		return err
	}

	tmp, err := fs.tempFile()
	if err != nil {
		return err
	}
	closed := false
	removed := false
	defer func() {
		if !closed {
			// silence errcheck
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}
	}()

	if _, err := tmp.Write(val); err != nil {
		return err
	}
	if fs.sync {
		if err := syncFile(tmp); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true

	err = fs.renameAndUpdateDiskUsage(tmp.Name(), path)
	if err != nil {
		return err
	}
	removed = true

	if fs.sync {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func (fs *Datastore) Get(ctx context.Context, key datastore.Key) (value []byte, err error) {
	// Can't exist in datastore.
	if !keyIsValid(key) {
		return nil, datastore.ErrNotFound
	}

	_, path := fs.encode(key)
	data, err := readFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, datastore.ErrNotFound
		}
		// no specific error to return, so just pass it through
		return nil, err
	}
	return data, nil
}

func (fs *Datastore) Has(ctx context.Context, key datastore.Key) (exists bool, err error) {
	// Can't exist in datastore.
	if !keyIsValid(key) {
		return false, nil
	}

	_, path := fs.encode(key)
	switch _, err := os.Stat(path); {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func (fs *Datastore) GetSize(ctx context.Context, key datastore.Key) (size int, err error) {
	// Can't exist in datastore.
	if !keyIsValid(key) {
		return -1, datastore.ErrNotFound
	}

	_, path := fs.encode(key)
	switch s, err := os.Stat(path); {
	case err == nil:
		return int(s.Size()), nil
	case os.IsNotExist(err):
		return -1, datastore.ErrNotFound
	default:
		return -1, err
	}
}

// Delete removes a key/value from the Datastore. Please read
// the Put() explanation about the handling of concurrent write
// operations to the same key.
func (fs *Datastore) Delete(ctx context.Context, key datastore.Key) error {
	// Can't exist in datastore.
	if !keyIsValid(key) {
		return nil
	}

	fs.shutdownLock.RLock()
	defer fs.shutdownLock.RUnlock()
	if fs.shutdown {
		return ErrClosed
	}

	_, err := fs.doWriteOp(&op{
		typ: opDelete,
		key: key,
		v:   nil,
	})
	return err
}

// This function always runs within an opLock for the given
// key, and not concurrently.
func (fs *Datastore) doDelete(key datastore.Key) error {
	_, path := fs.encode(key)

	fSize := fileSize(path)

	var err error
	for i := 0; i < RetryAttempts; i++ {
		err = os.Remove(path)
		if err == nil {
			break
		} else if os.IsNotExist(err) {
			return nil
		}
	}

	if err == nil {
		atomic.AddInt64(&fs.diskUsage, -fSize)
		fs.checkpointDiskUsage()
	}

	return err
}

func (fs *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	prefix := datastore.NewKey(q.Prefix).String()
	if prefix != "/" {
		// This datastore can't include keys with multiple components.
		// Therefore, it's always correct to return an empty result when
		// the user requests a filter by prefix.
		log.Warnw(
			"flatfs was queried with a key prefix but flatfs only supports keys at the root",
			"prefix", q.Prefix,
			"query", q,
		)
		return query.ResultsWithEntries(q, nil), nil
	}

	// Replicates the logic in ResultsWithChan but actually respects calls
	// to `Close`.
	results := query.ResultsWithContext(q, func(qctx context.Context, output chan<- query.Result) {
		err := fs.walkTopLevel(qctx, q, fs.path, output)
		if err != nil {
			select {
			case output <- query.Result{Error: errors.New("walk failed: " + err.Error())}:
			case <-qctx.Done():
			}
		}
	})

	// We don't apply _any_ of the query logic ourselves so we'll leave it
	// all up to the naive query engine.
	return query.NaiveQueryApply(q, results), nil
}

func (fs *Datastore) walkTopLevel(ctx context.Context, q query.Query, path string, output chan<- query.Result) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	entries, err := dir.Readdir(-1)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := entry.Name()
		if len(dir) == 0 || dir[0] == '.' {
			continue
		}

		err = fs.walk(ctx, q, filepath.Join(path, dir), output)
		if err != nil {
			return err
		}

		// Are we closing?
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	return nil
}

// folderSize estimates the diskUsage of a folder by reading
// up to DiskUsageFilesAverage entries in it and assuming any
// other files will have an average size.
func folderSize(path string, deadline time.Time) (int64, initAccuracy, error) {
	var du int64

	folder, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer folder.Close()

	stat, err := folder.Stat()
	if err != nil {
		return 0, "", err
	}

	files, err := folder.Readdirnames(-1)
	if err != nil {
		return 0, "", err
	}

	totalFiles := len(files)
	i := 0
	filesProcessed := 0
	maxFiles := DiskUsageFilesAverage
	if maxFiles <= 0 {
		maxFiles = totalFiles
	}

	// randomize file order
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	r.Shuffle(len(files), func(i, j int) {
		files[i], files[j] = files[j], files[i]
	})

	accuracy := exactA
	for {
		// Do not process any files after deadline is over
		if time.Now().After(deadline) {
			accuracy = timedoutA
			break
		}

		if i >= totalFiles || filesProcessed >= maxFiles {
			if filesProcessed >= maxFiles {
				accuracy = approxA
			}
			break
		}

		// Stat the file
		fname := files[i]
		subpath := filepath.Join(path, fname)
		st, err := os.Stat(subpath)
		if err != nil {
			return 0, "", err
		}

		// Find folder size recursively
		if st.IsDir() {
			du2, acc, err := folderSize(filepath.Join(subpath), deadline)
			if err != nil {
				return 0, "", err
			}
			accuracy = combineAccuracy(acc, accuracy)
			du += du2
			filesProcessed++
		} else { // in any other case, add the file size
			du += st.Size()
			filesProcessed++
		}

		i++
	}

	nonProcessed := totalFiles - filesProcessed

	// Avg is total size in this folder up to now / total files processed
	// it includes folders ant not folders
	avg := 0.0
	if filesProcessed > 0 {
		avg = float64(du) / float64(filesProcessed)
	}
	duEstimation := int64(avg * float64(nonProcessed))
	du += duEstimation
	du += stat.Size()
	//fmt.Println(path, "total:", totalFiles, "totalStat:", i, "totalFile:", filesProcessed, "left:", nonProcessed, "avg:", int(avg), "est:", int(duEstimation), "du:", du)
	return du, accuracy, nil
}

// calculateDiskUsage tries to read the DiskUsageFile for a cached
// diskUsage value, otherwise walks the datastore files.
// it is only safe to call in Open()
func (fs *Datastore) calculateDiskUsage() error {
	// Try to obtain a previously stored value from disk
	if persDu := fs.readDiskUsageFile(); persDu > 0 {
		fs.diskUsage = persDu
		return nil
	}

	msgDone := make(chan struct{}, 1) // prevent race condition
	msgTimer := time.AfterFunc(diskUsageMessageTimeout, func() {
		fmt.Printf("Calculating datastore size. This might take %s at most and will happen only once\n",
			DiskUsageCalcTimeout.String())
		msgDone <- struct{}{}
	})
	defer msgTimer.Stop()
	deadline := time.Now().Add(DiskUsageCalcTimeout)
	du, accuracy, err := folderSize(fs.path, deadline)
	if err != nil {
		return err
	}
	if !msgTimer.Stop() {
		<-msgDone
	}
	if accuracy == timedoutA {
		fmt.Println("WARN: It took to long to calculate the datastore size")
		fmt.Printf("WARN: The total size (%d) is an estimation. You can fix errors by\n", du)
		fmt.Printf("WARN: replacing the %s file with the right disk usage in bytes and\n",
			filepath.Join(fs.path, DiskUsageFile))
		fmt.Println("WARN: re-opening the datastore")
	}

	fs.storedValue.Accuracy = accuracy
	fs.diskUsage = du
	fs.writeDiskUsageFile(du, true)

	return nil
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

// updateDiskUsage reads the size of path and atomically
// increases or decreases the diskUsage variable.
// setting add to false will subtract from disk usage.
func (fs *Datastore) updateDiskUsage(path string, add bool) {
	fsize := fileSize(path)
	if !add {
		fsize = -fsize
	}

	if fsize != 0 {
		atomic.AddInt64(&fs.diskUsage, fsize)
		fs.checkpointDiskUsage()
	}
}

// checkpointDiskUsage triggers a disk usage checkpoint write.
func (fs *Datastore) checkpointDiskUsage() {
	select {
	case fs.checkpointCh <- struct{}{}:
		// msg sent
	default:
		// checkpoint request already pending
	}
}

// checkpointLoop periodically or following checkpoint event, write the current
// disk usage on disk.
func (fs *Datastore) checkpointLoop() {
	defer close(fs.done)

	timerActive := true
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case _, more := <-fs.checkpointCh:
			du := atomic.LoadInt64(&fs.diskUsage)
			fs.dirty = true
			if !more { // shutting down
				fs.writeDiskUsageFile(du, true)
				if fs.dirty {
					log.Error("could not store final value of disk usage to file, future estimates may be inaccurate")
				}
				return
			}
			// If the difference between the checkpointed disk usage and
			// current one is larger than than `diskUsageCheckpointPercent`
			// of the checkpointed: store it.
			newDu := float64(du)
			lastCheckpointDu := float64(fs.storedValue.DiskUsage)
			diff := math.Abs(newDu - lastCheckpointDu)
			if lastCheckpointDu*diskUsageCheckpointPercent < diff*100.0 {
				fs.writeDiskUsageFile(du, false)
			}
			// Otherwise insure the value will be written to disk after
			// `diskUsageCheckpointTimeout`
			if fs.dirty && !timerActive {
				timer.Reset(diskUsageCheckpointTimeout)
				timerActive = true
			}
		case <-timer.C:
			timerActive = false
			if fs.dirty {
				du := atomic.LoadInt64(&fs.diskUsage)
				fs.writeDiskUsageFile(du, false)
			}
		}
	}
}

// writeDiskUsageFile write the given checkpoint disk usage in a file.
func (fs *Datastore) writeDiskUsageFile(du int64, doSync bool) {
	tmp, err := fs.tempFile()
	if err != nil {
		log.Warnw("could not write disk usage", "error", err)
		return
	}

	removed := false
	closed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !removed {
			// silence errcheck
			_ = os.Remove(tmp.Name())
		}

	}()

	toWrite := fs.storedValue
	toWrite.DiskUsage = du
	encoder := json.NewEncoder(tmp)
	if err := encoder.Encode(&toWrite); err != nil {
		log.Warnw("cound not write disk usage", "error", err)
		return
	}

	if doSync {
		if err := tmp.Sync(); err != nil {
			log.Warnw("cound not sync", "error", err, "file", DiskUsageFile)
			return
		}
	}

	if err := tmp.Close(); err != nil {
		log.Warnw("cound not write disk usage", "error", err)
		return
	}
	closed = true

	if err := rename(tmp.Name(), filepath.Join(fs.path, DiskUsageFile)); err != nil {
		log.Warnw("cound not write disk usage", "error", err)
		return
	}
	removed = true

	fs.storedValue = toWrite
	fs.dirty = false
}

// readDiskUsageFile is only safe to call in Open()
func (fs *Datastore) readDiskUsageFile() int64 {
	fpath := filepath.Join(fs.path, DiskUsageFile)
	duB, err := readFile(fpath)
	if err != nil {
		return 0
	}
	err = json.Unmarshal(duB, &fs.storedValue)
	if err != nil {
		return 0
	}
	return fs.storedValue.DiskUsage
}

// DiskUsage implements the PersistentDatastore interface
// and returns the current disk usage in bytes used by
// this datastore.
//
// The size is approximative and may slightly differ from
// the real disk values.
func (fs *Datastore) DiskUsage(ctx context.Context) (uint64, error) {
	// it may differ from real disk values if
	// the filesystem has allocated for blocks
	// for a directory because it has many files in it
	// we don't account for "resized" directories.
	// In a large datastore, the differences should be
	// are negligible though.

	du := atomic.LoadInt64(&fs.diskUsage)
	return uint64(du), nil
}

// Accuracy returns a string representing the accuracy of the
// DiskUsage() result, the value returned is implementation defined
// and for informational purposes only
func (fs *Datastore) Accuracy() string {
	return string(fs.storedValue.Accuracy)
}

func (fs *Datastore) tempFile() (*os.File, error) {
	file, err := tempFile(fs.tempPath, "temp-")
	return file, err
}

// only call this on directories.
func (fs *Datastore) walk(ctx context.Context, q query.Query, path string, output chan<- query.Result) error {
	dir, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// not an error if the file disappeared
			return nil
		}
		return err
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, fn := range names {

		if len(fn) == 0 || fn[0] == '.' {
			continue
		}

		key, ok := fs.decode(fn)
		if !ok {
			// not a block.
			continue
		}

		var result query.Result
		result.Key = key.String()
		if !q.KeysOnly {
			value, err := readFile(filepath.Join(path, fn))
			if err != nil {
				result.Error = err
			} else {
				// NOTE: Don't set the value/size on error. We
				// don't want to return partial values.
				result.Value = value
				result.Size = len(value)
			}
		} else if q.ReturnsSizes {
			var stat os.FileInfo
			stat, err := os.Stat(filepath.Join(path, fn))
			if err != nil {
				result.Error = err
			} else {
				result.Size = int(stat.Size())
			}
		}

		select {
		case output <- result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Deactivate closes background maintenance threads, most write
// operations will fail but readonly operations will continue to
// function
func (fs *Datastore) deactivate() {
	fs.shutdownLock.Lock()
	defer fs.shutdownLock.Unlock()
	if fs.shutdown {
		return
	}
	fs.shutdown = true
	close(fs.checkpointCh)
	<-fs.done
}

func (fs *Datastore) Close() error {
	fs.deactivate()
	return nil
}

// DiscardableBatch is an optional interface for batches that support discarding changes
type DiscardableBatch interface {
	datastore.Batch
	Discard(ctx context.Context) error
}

// BatchReader is an optional interface for batches that support read operations
type BatchReader interface {
	datastore.Batch
	datastore.Read
}

// flatfsBatch implements atomic batch operations using a temporary directory.
//
// Design principles:
//   - All Put operations write to a temp directory (no sharding)
//   - Writes are done asynchronously in goroutines for performance
//   - Commit atomically renames all files to their sharded destinations
//   - On crash/discard, temp directory is cleaned (no partial writes)
//
// Duplicate key handling: Uses "first-successful-writer-wins" semantics.
// If the same key is Put multiple times within a batch, only the first Put
// is written; subsequent Puts to that key are silently skipped. This is safe
// for content-addressed storage where identical keys guarantee identical values.
// For mutable data requiring last-writer-wins, use a different datastore.
//
// Concurrency: Safe for concurrent calls to Put/Delete/Get/Has/GetSize/Query.
// Not safe to call Commit or Discard concurrently with other operations.
//
// Transaction semantics: Read operations (Get/Has/GetSize/Query) see
// uncommitted writes from the same batch, following standard database
// transaction isolation.
//
// Performance characteristics:
//   - Put: O(1) with async I/O, returns immediately
//   - Get/Has/GetSize: O(1) lookup via putSet map
//   - Commit: O(n) file renames, where n = number of Put operations
//
// IMPORTANT: Batch instances should not be reused after Commit or Discard.
// This follows the go-datastore Batch interface contract which states that
// calling Put/Delete after Commit/Discard has undefined behavior.
// See: https://github.com/ipfs/go-datastore/blob/master/datastore.go
type flatfsBatch struct {
	mu      sync.Mutex
	puts    []datastore.Key            // ordered list for iteration (Commit, Query)
	putSet  map[datastore.Key]struct{} // O(1) lookup for Get/Has/GetSize
	deletes map[datastore.Key]struct{}

	ds      *Datastore
	tempDir string

	// Async write tracking
	asyncWrites     sync.WaitGroup
	asyncMu         sync.Mutex
	asyncFirstError error
	asyncPutGate    chan struct{}
}

func (fs *Datastore) Batch(_ context.Context) (datastore.Batch, error) {
	// Create a unique temp directory for this batch
	// Note: Temp files are not sharded (flat structure) for simplicity and speed.
	// Files are only sharded when renamed to their final destination on Commit.
	tempDir := filepath.Join(fs.tempPath, fmt.Sprintf("batch-%d-%d", time.Now().UnixNano(), rand.Int63()))
	if err := os.Mkdir(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create batch temp directory: %w", err)
	}

	return &flatfsBatch{
		putSet:       make(map[datastore.Key]struct{}),
		deletes:      make(map[datastore.Key]struct{}),
		ds:           fs,
		tempDir:      tempDir,
		asyncPutGate: make(chan struct{}, maxConcurrentPuts),
	}, nil
}

// Put writes val for key to a temporary file asynchronously and returns immediately.
//
// Duplicate keys: If this key was already Put in this batch, the call returns
// immediately without writing (first-successful-writer-wins). This is safe for
// content-addressed data where identical keys have identical values.
//
// CRITICAL: The caller MUST NOT modify or reuse the val byte slice after calling Put.
// The buffer is used asynchronously by a background goroutine. Violating this will
// cause data corruption. This differs from typical Go semantics where buffers can
// be reused after a function returns.
//
// If you need to reuse buffers, copy them before calling Put:
//
//	buf := make([]byte, len(data))
//	copy(buf, data)
//	batch.Put(ctx, key, buf)
//
// Error handling: If an async write fails, the error is captured and returned
// on the next Put/Delete/Commit or any read operation (fail-fast behavior).
func (bt *flatfsBatch) Put(ctx context.Context, key datastore.Key, val []byte) error {
	if !keyIsValid(key) {
		return fmt.Errorf("when putting '%q': %v", key, ErrInvalidKey)
	}

	if err := bt.getAsyncError(); err != nil {
		// If there was an error from a previous async write, return it fast
		// This may be useful if, for example, we are out of disk space.
		return err
	}

	// Acquire semaphore slot before starting another async put.
	select {
	case bt.asyncPutGate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	bt.mu.Lock()

	// Skip duplicate keys to prevent concurrent goroutines from writing to the
	// same temp file. Without this check, two Put calls with the same key
	// would spawn goroutines that race on os.Create/Write, potentially
	// corrupting the file contents.
	if _, exists := bt.putSet[key]; exists {
		bt.mu.Unlock()
		<-bt.asyncPutGate // Release semaphore slot acquired above
		return nil
	}
	noslash := key.String()[1:]
	fileName := noslash + extension
	tempFile := filepath.Join(bt.tempDir, fileName)

	// Track this key immediately
	bt.puts = append(bt.puts, key)
	bt.putSet[key] = struct{}{}

	// Increment wait group for async write
	bt.asyncWrites.Add(1)
	bt.mu.Unlock()

	// Write to temp file asynchronously in a goroutine
	go func(val []byte) {
		defer func() {
			bt.asyncWrites.Done()
			<-bt.asyncPutGate
		}()

		// Ensure temp directory exists (recreate if batch was reused after commit,
		// which is unsupported but we handle it as a precaution)
		if err := os.MkdirAll(filepath.Dir(tempFile), 0755); err != nil {
			bt.setAsyncError(fmt.Errorf("failed to create temp directory: %w", err))
			return
		}

		file, err := createFile(tempFile)
		if err != nil {
			bt.setAsyncError(fmt.Errorf("failed to create temp file: %w", err))
			return
		}
		defer file.Close()

		if _, err := file.Write(val); err != nil {
			os.Remove(tempFile)
			bt.setAsyncError(fmt.Errorf("failed to write to temp file: %w", err))
			return
		}

		if bt.ds.sync {
			if err := syncFile(file); err != nil {
				os.Remove(tempFile)
				bt.setAsyncError(fmt.Errorf("failed to sync temp file: %w", err))
				return
			}
		}

		if err := file.Close(); err != nil {
			os.Remove(tempFile)
			bt.setAsyncError(fmt.Errorf("failed to close temp file: %w", err))
			return
		}
	}(val)

	return nil
}

// setAsyncError saves the first error from an async write operation.
// Only the first error is captured; subsequent errors are ignored.
// This provides fail-fast behavior: once any write fails, subsequent
// operations return that error immediately.
func (bt *flatfsBatch) setAsyncError(err error) {
	bt.asyncMu.Lock()
	defer bt.asyncMu.Unlock()
	if bt.asyncFirstError == nil {
		bt.asyncFirstError = err
	}
}

// getAsyncError returns the first error from an async write operation
func (bt *flatfsBatch) getAsyncError() error {
	bt.asyncMu.Lock()
	defer bt.asyncMu.Unlock()

	err := bt.asyncFirstError
	return err
}

func (bt *flatfsBatch) Delete(ctx context.Context, key datastore.Key) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if err := bt.getAsyncError(); err != nil {
		return err
	}

	if keyIsValid(key) {
		bt.deletes[key] = struct{}{}
	} // otherwise, delete is a no-op anyways.
	return nil
}

// Get retrieves a value from the batch or underlying datastore.
//
// Transaction semantics: Returns uncommitted data written to this batch via Put,
// even before Commit. This allows building IPLD structures that reference blocks
// added earlier in the same batch.
//
// Performance: O(1) lookup via putSet map, plus file read if key is in batch.
func (bt *flatfsBatch) Get(ctx context.Context, key datastore.Key) ([]byte, error) {
	// Wait for all async writes to complete before reading
	bt.asyncWrites.Wait()
	if err := bt.getAsyncError(); err != nil {
		return nil, err
	}
	bt.mu.Lock()
	// Check if key is marked for deletion
	if _, deleted := bt.deletes[key]; deleted {
		bt.mu.Unlock()
		return nil, datastore.ErrNotFound
	}

	// Check if key was added in this batch
	_, inBatch := bt.putSet[key]
	bt.mu.Unlock()

	// If in batch, read from temp directory
	if inBatch {
		noslash := key.String()[1:]
		tempFile := filepath.Join(bt.tempDir, noslash+extension)
		data, err := readFile(tempFile)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	// If not in batch, check main datastore
	return bt.ds.Get(ctx, key)
}

func (bt *flatfsBatch) Has(ctx context.Context, key datastore.Key) (bool, error) {
	// Wait for all async writes to complete before checking
	bt.asyncWrites.Wait()
	if err := bt.getAsyncError(); err != nil {
		return false, err
	}

	bt.mu.Lock()
	// Check if key is marked for deletion
	if _, deleted := bt.deletes[key]; deleted {
		bt.mu.Unlock()
		return false, nil
	}

	// Check if key was added in this batch
	_, inBatch := bt.putSet[key]
	bt.mu.Unlock()

	if inBatch {
		return true, nil
	}

	// If not in batch, check main datastore
	return bt.ds.Has(ctx, key)
}

func (bt *flatfsBatch) GetSize(ctx context.Context, key datastore.Key) (int, error) {
	// Wait for all async writes to complete before checking size
	bt.asyncWrites.Wait()

	if err := bt.getAsyncError(); err != nil {
		return 0, err
	}
	bt.mu.Lock()
	// Check if key is marked for deletion
	if _, deleted := bt.deletes[key]; deleted {
		bt.mu.Unlock()
		return -1, datastore.ErrNotFound
	}

	// Check if key was added in this batch
	_, inBatch := bt.putSet[key]
	bt.mu.Unlock()

	// If in batch, get size from temp directory
	if inBatch {
		noslash := key.String()[1:]
		tempFile := filepath.Join(bt.tempDir, noslash+extension)
		stat, err := os.Stat(tempFile)
		if err != nil {
			return -1, err
		}
		return int(stat.Size()), nil
	}

	// If not in batch, check main datastore
	return bt.ds.GetSize(ctx, key)
}

// Query returns all entries from both the batch and underlying datastore,
// properly merging results to reflect the batch's uncommitted state.
//
// Merge logic:
// - Keys written via Put appear in results (even if not committed)
// - Keys marked for Delete do not appear in results
// - Keys Put multiple times appear only once (last write wins)
// - Main datastore results are excluded if overwritten or deleted in batch
//
// The implementation waits for all async writes to complete before querying.
func (bt *flatfsBatch) Query(ctx context.Context, q query.Query) (query.Results, error) {
	// Wait for all async writes to complete before querying
	bt.asyncWrites.Wait()
	if err := bt.getAsyncError(); err != nil {
		return nil, err
	}
	prefix := datastore.NewKey(q.Prefix).String()
	if prefix != "/" {
		// This datastore can't include keys with multiple components.
		// Therefore, it's always correct to return an empty result when
		// the user requests a filter by prefix.
		return query.ResultsWithEntries(q, nil), nil
	}

	// Get results from main datastore
	mainResults, err := bt.ds.Query(ctx, q)
	if err != nil {
		return nil, err
	}

	// Merge with temp directory results
	results := query.ResultsWithContext(q, func(qctx context.Context, output chan<- query.Result) {
		bt.mu.Lock()
		// Clone deletes and puts to avoid holding lock during query execution
		deletedOrSent := maps.Clone(bt.deletes)
		puts := slices.Clone(bt.puts)
		tempDir := bt.tempDir
		bt.mu.Unlock()

		// First, send results from temp directory (puts)
		for _, key := range puts {
			// Skip if deleted
			if _, deleted := deletedOrSent[key]; deleted {
				continue
			}

			noslash := key.String()[1:]
			tempFile := filepath.Join(tempDir, noslash+extension)

			var result query.Result
			result.Key = key.String()

			if !q.KeysOnly {
				value, err := readFile(tempFile)
				if err != nil {
					if !errors.Is(err, fs.ErrNotExist) {
						result.Error = err
					} else {
						continue // File doesn't exist, skip
					}
				} else {
					result.Value = value
					result.Size = len(value)
				}
			} else if q.ReturnsSizes {
				stat, err := os.Stat(tempFile)
				if err != nil {
					if !errors.Is(err, fs.ErrNotExist) {
						result.Error = err
					} else {
						continue // File doesn't exist, skip
					}
				} else {
					result.Size = int(stat.Size())
				}
			}

			select {
			case output <- result:
				// Mark this key as sent by adding it to deletedOrSent map
				deletedOrSent[key] = struct{}{}
			case <-qctx.Done():
				return
			}
		}

		// Then, send results from main datastore (excluding deleted and already sent)
		mainChan := mainResults.Next()
		for {
			select {
			case result, ok := <-mainChan:
				if !ok {
					return
				}
				if result.Error != nil {
					select {
					case output <- result:
					case <-qctx.Done():
						return
					}
					continue
				}

				key := datastore.NewKey(result.Key)

				// Skip if deleted or already sent from temp
				if _, skip := deletedOrSent[key]; skip {
					continue
				}

				select {
				case output <- result:
				case <-qctx.Done():
					return
				}
			case <-qctx.Done():
				return
			}
		}
	})

	// Apply query filters
	return query.NaiveQueryApply(q, results), nil
}

// Discard discards the batch operations without committing.
// The batch should not be reused after Discard is called.
func (bt *flatfsBatch) Discard(ctx context.Context) error {
	// Wait for any pending async writes to complete
	bt.asyncWrites.Wait()

	bt.mu.Lock()
	defer bt.mu.Unlock()

	// Remove the batch temp directory and all its contents
	if err := os.RemoveAll(bt.tempDir); err != nil {
		log.Warnw("failed to remove batch temp directory on discard", "error", err, "path", bt.tempDir)
	}

	// Reset state as a precaution (batch should not be reused per go-datastore contract)
	bt.puts = nil
	bt.putSet = make(map[datastore.Key]struct{})
	bt.asyncFirstError = nil
	bt.deletes = make(map[datastore.Key]struct{})

	return nil
}

// Commit atomically applies all batch operations to the datastore.
//
// Atomicity guarantee: All Put operations are moved to their final destinations
// only after being written to temp files. If the process crashes before Commit
// completes, the temp directory is cleaned on restart and no partial data remains.
//
// Order of operations:
// 1. Wait for all async Put goroutines to complete
// 2. Check for any async write errors (fail-fast)
// 3. Create all destination shard directories
// 4. Atomically rename temp files to final sharded paths
// 5. Apply all Delete operations
// 6. Sync directories (if sync is enabled)
//
// Concurrency: Uses doWriteOp to handle concurrent commits gracefully.
// If another goroutine commits the same key, the operation succeeds.
func (bt *flatfsBatch) Commit(ctx context.Context) error {
	// Wait for all async write operations to complete
	bt.asyncWrites.Wait()

	if err := bt.getAsyncError(); err != nil {
		// If there was an error from a previous async write, return it fast
		// This may be useful if, for example, we are out of disk space.
		return err
	}

	bt.mu.Lock()
	defer bt.mu.Unlock()

	bt.ds.shutdownLock.RLock()
	defer bt.ds.shutdownLock.RUnlock()
	if bt.ds.shutdown {
		return ErrClosed
	}

	dirsToSync := make(map[string]struct{})

	// First, ensure all destination directories exist
	for _, key := range bt.puts {
		dir, _ := bt.ds.encode(key)
		if _, err := bt.ds.makeDirNoSync(dir); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		dirsToSync[dir] = struct{}{}
	}

	// Move all temp files to their final destinations
	for _, key := range bt.puts {
		noslash := key.String()[1:]
		fileName := noslash + extension
		tempFile := filepath.Join(bt.tempDir, fileName)
		_, finalPath := bt.ds.encode(key)

		// Use the doWriteOp to handle concurrent operations properly
		_, err := bt.ds.doWriteOp(&op{
			typ:  opRename,
			key:  key,
			tmp:  tempFile,
			path: finalPath,
		})
		if err != nil {
			// Clean up remaining temp files on error
			_ = os.RemoveAll(bt.tempDir)
			return fmt.Errorf("failed to rename temp file: %w", err)
		}
		// If doWriteOp returns without error, the operation succeeded
		// (either by us or by a concurrent operation)
	}

	// Handle deletes
	for k := range bt.deletes {
		if err := bt.ds.Delete(ctx, k); err != nil {
			return err
		}
	}

	// Sync directories if needed
	if bt.ds.sync {
		for dir := range dirsToSync {
			if err := syncDir(dir); err != nil {
				return fmt.Errorf("failed to sync directory: %w", err)
			}
		}
		// Sync root directory
		if err := syncDir(bt.ds.path); err != nil {
			return fmt.Errorf("failed to sync root directory: %w", err)
		}
	}

	// Reset state as a precaution (batch should not be reused per go-datastore contract)
	bt.puts = nil
	bt.putSet = make(map[datastore.Key]struct{})
	bt.deletes = make(map[datastore.Key]struct{})

	// Clean up the batch temp directory after successful commit
	if err := os.RemoveAll(bt.tempDir); err != nil {
		log.Warnw("failed to remove batch temp directory after commit", "error", err, "path", bt.tempDir)
	}

	return nil
}
