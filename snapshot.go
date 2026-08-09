package main

import (
	"io"

	"github.com/ledisdb/ledisdb/store/driver"
	"github.com/tidwall/sds"
)

// restoreBatchSize bounds how many key/value pairs accumulate before a flush
// during restore, keeping memory bounded on large snapshots.
const restoreBatchSize = 1000

// writeSnapshot serializes every key/value pair visible in the snapshot to wr
// using the sds stream format. It is backend-agnostic: any driver.ISnapshot works.
func writeSnapshot(s driver.ISnapshot, wr io.Writer) error {
	sw := sds.NewWriter(wr)
	it := s.NewIterator()
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		if err := sw.WriteBytes(it.Key()); err != nil {
			return err
		}
		if err := sw.WriteBytes(it.Value()); err != nil {
			return err
		}
	}
	return sw.Flush()
}

// restoreSnapshot reads an sds stream produced by writeSnapshot and writes it
// into d in bounded batches. After each Commit the batch is reset via Rollback
// so it can be reused safely across both goleveldb and badger write batches.
func restoreSnapshot(d driver.IDB, rd io.Reader) error {
	sr := sds.NewReader(rd)
	wb := d.NewWriteBatch()
	defer wb.Close()

	n := 0
	for {
		key, err := sr.ReadBytes()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		value, err := sr.ReadBytes()
		if err != nil {
			return err
		}
		wb.Put(key, value)
		n++
		if n >= restoreBatchSize {
			if err := wb.Commit(); err != nil {
				return err
			}
			if err := wb.Rollback(); err != nil { // resets the batch for reuse
				return err
			}
			n = 0
		}
	}
	if n > 0 {
		return wb.Commit()
	}
	return nil
}
