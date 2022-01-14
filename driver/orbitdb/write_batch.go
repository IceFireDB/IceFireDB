package orbitdb

import (
        "fmt"
	//"github.com/syndtr/goleveldb/leveldb"
)

type WriteBatch struct {
	db     *DB
	//wbatch *leveldb.Batch
}

func (w *WriteBatch) Put(key, value []byte) {
    fmt.Println("ipfs.WriteBatch.Put  " + string(key))
	w.db.Put(key, value)
	w.db.cache.Del(key)
}

func (w *WriteBatch) Delete(key []byte) {
   fmt.Print("ipfs.WriteBatch.Delete \n")
	w.db.Delete(key)
	w.db.cache.Del(key)
}

func (w *WriteBatch) Commit() error {
    fmt.Print("ipfs.WriteBatch.Commit \n" )
	return w.db.db.Write(nil, nil)
}

func (w *WriteBatch) SyncCommit() error {
     fmt.Print("ipfs.WriteBatch.SyncCommit \n")
	return w.db.db.Write(nil, w.db.syncOpts)
}

func (w *WriteBatch) Rollback() error {
    fmt.Print("ipfs.WriteBatch.Rollback \n")
	//w.wbatch.Reset()
	return nil
}

func (w *WriteBatch) Close() {
    fmt.Print("ipfs.WriteBatch.Close \n")
	//w.wbatch.Reset()
}

func (w *WriteBatch) Data() []byte {
     fmt.Print("ipfs.WriteBatch.Data \n")
	//return w.wbatch.Dump()
	return nil
}
