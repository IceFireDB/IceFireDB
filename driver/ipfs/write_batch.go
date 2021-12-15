package ipfs

import (
	"bytes"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
)

type WriteBatch struct {
	db     *DB
	wbatch *leveldb.Batch
}

func (w *WriteBatch) Put(key, value []byte) {
	//fmt.Printf("ipfs.WriteBatch.Put key=%s , v=%s\n", string(key), string(value))
	buf := bytes.NewBuffer(value)
	cid, err := w.db.remoteShell.Add(buf)
	if err != nil {
		fmt.Print("ipfs.add err\n")
	}
	fmt.Println("cid=", cid)
	w.wbatch.Put(key, []byte(cid)) //w.wbatch.Put(key, value)
	w.db.cache.Del(key)
}

func (w *WriteBatch) Delete(key []byte) {
	//fmt.Print("ipfs.WriteBatch.Delete \n")
	w.wbatch.Delete(key)
	w.db.cache.Del(key)
}

func (w *WriteBatch) Commit() error {
	//fmt.Print("ipfs.WriteBatch.Commit \n" )
	return w.db.db.Write(w.wbatch, nil)
}

func (w *WriteBatch) SyncCommit() error {
	//fmt.Print("ipfs.WriteBatch.SyncCommit \n")
	return w.db.db.Write(w.wbatch, w.db.syncOpts)
}

func (w *WriteBatch) Rollback() error {
	//fmt.Print("ipfs.WriteBatch.Rollback \n")
	w.wbatch.Reset()
	return nil
}

func (w *WriteBatch) Close() {
	//fmt.Print("ipfs.WriteBatch.Close \n")
	w.wbatch.Reset()
}

func (w *WriteBatch) Data() []byte {
	// fmt.Print("ipfs.WriteBatch.Data \n")
	return w.wbatch.Dump()
}
