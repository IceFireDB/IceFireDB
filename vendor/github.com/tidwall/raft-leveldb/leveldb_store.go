package raftleveldb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const maxBatchSize = 1024 * 1024

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("logs")
	dbConf = []byte("conf")
)

// ErrKeyNotFound is returned when a given key does not exist
var ErrKeyNotFound = errors.New("not found")

// ErrClosed is returned when the log is closed
var ErrClosed = errors.New("closed")

// ErrCorrupt is returned when the log is corrup
var ErrCorrupt = errors.New("corrupt")

// var errInvalidLog = errors.New("invalid log")

// LevelDBStore provides access to BoltDB for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type LevelDBStore struct {
	mu     sync.RWMutex
	db     *leveldb.DB // db is the underlying handle to the db.
	path   string      // The path to the Bolt database file
	dur    Level
	batch  leveldb.Batch
	bsize  int
	closed bool
}

// Level is the consistency level
type Level int

// Low, Medium, or High level
const (
	Low    Level = -1
	Medium Level = 0
	High   Level = 1
)

// NewLevelDBStore takes a file path and returns a connected Raft backend.
func NewLevelDBStore(path string, durability Level) (*LevelDBStore, error) {
	var opts opt.Options
	opts.OpenFilesCacheCapacity = 20

	// Try to connect
	db, err := leveldb.OpenFile(path, &opts)
	if err != nil {
		return nil, err
	}

	// Create the new store
	store := &LevelDBStore{
		db:   db,
		path: path,
		dur:  durability,
	}
	if durability <= Low {
		go store.keepSynced()
	}
	return store, nil
}

// keepSynced runs ensure that the data is flushed and fsynced to disk every
// second. This is only expected when the log durability is not set to high.
func (b *LevelDBStore) keepSynced() {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return
		}
		func() {
			defer b.mu.Unlock()
			b.batchFlush(flushSync)
		}()
		time.Sleep(time.Second)
	}
}

func (b *LevelDBStore) batchDelete(key []byte) {
	b.batch.Delete(key)
	b.bsize += len(key)
}

func (b *LevelDBStore) batchPut(key, value []byte) {
	b.batch.Put(key, value)
	b.bsize += len(key) + len(value)
}

type flushKind int

const (
	flushSync       flushKind = 1 // forces a journal sync to disk
	flushBeforeRead flushKind = 2 // forces batch flush always
	flushAfterWrite flushKind = 3 // forces batch flush, if at capacity
)

func (b *LevelDBStore) batchFlush(kind flushKind) error {
	var opt opt.WriteOptions
	opt.Sync = b.dur >= High || kind == flushSync
	var requireWrite bool
	switch kind {
	case flushSync:
		if b.dur < High && b.bsize == 0 {
			// add a __sync__ key to force the sync to the leveldb journal
			b.batchPut([]byte("__sync__"), nil)
		}
		requireWrite = true
	case flushBeforeRead:
		requireWrite = true
	default:
		requireWrite = b.bsize > maxBatchSize || b.dur >= Medium
	}
	if b.bsize == 0 {
		return nil // nothing to flush
	}
	if requireWrite {
		if err := b.db.Write(&b.batch, &opt); err != nil {
			return err
		}
		b.batch.Reset()
		b.bsize = 0
	}
	return nil
}

// Close is used to gracefully close the DB connection.
func (b *LevelDBStore) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if err := b.batchFlush(flushSync); err != nil {
		return err
	}
	if err := b.db.Close(); err != nil {
		return err
	}
	b.closed = true
	return nil
}

// FirstIndex returns the first known index from the Raft log.
func (b *LevelDBStore) FirstIndex() (uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return 0, ErrClosed
	}
	if err := b.batchFlush(flushBeforeRead); err != nil {
		return 0, err
	}
	var n uint64
	iter := b.db.NewIterator(nil, nil)
	for ok := iter.Seek(dbLogs); ok; ok = iter.Next() {
		// Use key/value.
		key := iter.Key()
		if !bytes.HasPrefix(key, dbLogs) {
			break
		}
		n = bytesToUint64(key[len(dbLogs):])
		break
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *LevelDBStore) LastIndex() (uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return 0, ErrClosed
	}
	if err := b.batchFlush(flushBeforeRead); err != nil {
		return 0, err
	}
	var n uint64
	iter := b.db.NewIterator(nil, nil)
	for ok := iter.Last(); ok; ok = iter.Prev() {
		key := iter.Key()
		if !bytes.HasPrefix(key, dbLogs) {
			break
		}
		n = bytesToUint64(key[len(dbLogs):])
		break
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// GetLog is used to retrieve a log from BoltDB at a given index.
func (b *LevelDBStore) GetLog(idx uint64, log *raft.Log) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	if err := b.batchFlush(flushBeforeRead); err != nil {
		return err
	}
	key := append(dbLogs, uint64ToBytes(idx)...)
	val, err := b.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return raft.ErrLogNotFound
		}
		return err
	}
	return decodeLog(val, log)
}

// StoreLog is used to store a single raft log
func (b *LevelDBStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *LevelDBStore) StoreLogs(logs []*raft.Log) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	for _, log := range logs {
		key := append(dbLogs, uint64ToBytes(log.Index)...)
		val := encodeLog(log)
		b.batchPut(key, val)
	}
	if err := b.batchFlush(flushAfterWrite); err != nil {
		return err
	}
	return nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *LevelDBStore) DeleteRange(min, max uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	prefix := append(dbLogs, uint64ToBytes(min)...)
	iter := b.db.NewIterator(nil, nil)
	for ok := iter.Seek(prefix); ok; ok = iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, dbLogs) {
			break
		}
		if bytesToUint64(key[len(dbLogs):]) > max {
			break
		}
		b.batchDelete(key)
	}
	iter.Release()
	err := iter.Error()
	if err != nil {
		return err
	}
	return b.batchFlush(flushAfterWrite)
}

// Set is used to set a key/value set outside of the raft log
func (b *LevelDBStore) Set(k, v []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	key := append(dbConf, k...)
	val := v
	b.batchPut(key, val)
	return b.batchFlush(flushAfterWrite)
}

// Get is used to retrieve a value from the k/v store by key
func (b *LevelDBStore) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	if err := b.batchFlush(flushBeforeRead); err != nil {
		return nil, err
	}
	val, err := b.db.Get(append(dbConf, k...), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return bcopy(val), nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *LevelDBStore) SetUint64(key []byte, val uint64) error {
	return b.Set(key, uint64ToBytes(val))
}

// GetUint64 is like Get, but handles uint64 values
func (b *LevelDBStore) GetUint64(key []byte) (uint64, error) {
	val, err := b.Get(key)
	if err != nil {
		return 0, err
	}
	return bytesToUint64(val), nil
}

func bcopy(b []byte) []byte {
	r := make([]byte, len(b))
	copy(r, b)
	return r
}

// Decode reverses the encode operation on a byte slice input
func decodeLog(buf []byte, log *raft.Log) error {
	if len(buf) < 25 {
		return ErrCorrupt
	}
	log.Index = binary.LittleEndian.Uint64(buf[0:8])
	log.Term = binary.LittleEndian.Uint64(buf[8:16])
	log.Type = raft.LogType(buf[16])
	log.Data = make([]byte, binary.LittleEndian.Uint64(buf[17:25]))
	if len(buf[25:]) < len(log.Data) {
		return ErrCorrupt
	}
	copy(log.Data, buf[25:])
	return nil
}

// Encode writes an encoded object to a new bytes buffer
func encodeLog(log *raft.Log) []byte {
	var buf []byte
	var num = make([]byte, 8)
	binary.LittleEndian.PutUint64(num, log.Index)
	buf = append(buf, num...)
	binary.LittleEndian.PutUint64(num, log.Term)
	buf = append(buf, num...)
	buf = append(buf, byte(log.Type))
	binary.LittleEndian.PutUint64(num, uint64(len(log.Data)))
	buf = append(buf, num...)
	buf = append(buf, log.Data...)
	return buf
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts a uint to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
