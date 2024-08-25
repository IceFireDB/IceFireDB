package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/petar/GoLLRB/llrb"
	cbor "github.com/whyrusleeping/cbor/go"
)

// This index is intended to be efficient for random-access, in-memory lookups
// and is not intended to be an index type that is attached to a CARv2.
// See flatten() for conversion of this data to a known, existing index type.

var (
	errUnsupported      = errors.New("not supported")
	insertionIndexCodec = multicodec.Code(0x300003)
)

type InsertionIndex struct {
	items llrb.LLRB
}

func NewInsertionIndex() *InsertionIndex {
	return &InsertionIndex{}
}

type recordDigest struct {
	digest []byte
	Record
}

func (r recordDigest) Less(than llrb.Item) bool {
	other, ok := than.(recordDigest)
	if !ok {
		return false
	}
	return bytes.Compare(r.digest, other.digest) < 0
}

func newRecordDigest(r Record) recordDigest {
	d, err := multihash.Decode(r.Hash())
	if err != nil {
		panic(err)
	}

	return recordDigest{d.Digest, r}
}

func newRecordFromCid(c cid.Cid, at uint64) recordDigest {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		panic(err)
	}

	return recordDigest{d.Digest, Record{Cid: c, Offset: at}}
}

func (ii *InsertionIndex) InsertNoReplace(key cid.Cid, n uint64) {
	ii.items.InsertNoReplace(newRecordFromCid(key, n))
}

func (ii *InsertionIndex) Get(c cid.Cid) (uint64, error) {
	record, err := ii.getRecord(c)
	if err != nil {
		return 0, err
	}
	return record.Offset, nil
}

func (ii *InsertionIndex) getRecord(c cid.Cid) (Record, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return Record{}, err
	}
	entry := recordDigest{digest: d.Digest}
	e := ii.items.Get(entry)
	if e == nil {
		return Record{}, ErrNotFound
	}
	r, ok := e.(recordDigest)
	if !ok {
		return Record{}, errUnsupported
	}

	return r.Record, nil
}

func (ii *InsertionIndex) GetAll(c cid.Cid, fn func(uint64) bool) error {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return err
	}
	entry := recordDigest{digest: d.Digest}

	any := false
	iter := func(i llrb.Item) bool {
		existing := i.(recordDigest)
		if !bytes.Equal(existing.digest, entry.digest) {
			// We've already looked at all entries with matching digests.
			return false
		}
		any = true
		return fn(existing.Record.Offset)
	}
	ii.items.AscendGreaterOrEqual(entry, iter)
	if !any {
		return ErrNotFound
	}
	return nil
}

func (ii *InsertionIndex) Marshal(w io.Writer) (uint64, error) {
	l := uint64(0)
	if err := binary.Write(w, binary.LittleEndian, int64(ii.items.Len())); err != nil {
		return l, err
	}
	l += 8

	var err error
	iter := func(i llrb.Item) bool {
		if err = cbor.Encode(w, i.(recordDigest).Record); err != nil {
			return false
		}
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)
	return l, err
}

func (ii *InsertionIndex) Unmarshal(r io.Reader) error {
	var length int64
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return err
	}
	d := cbor.NewDecoder(r)
	for i := int64(0); i < length; i++ {
		var rec Record
		if err := d.Decode(&rec); err != nil {
			return err
		}
		ii.items.InsertNoReplace(newRecordDigest(rec))
	}
	return nil
}

func (ii *InsertionIndex) ForEach(f func(multihash.Multihash, uint64) error) error {
	var err error
	ii.items.AscendGreaterOrEqual(ii.items.Min(), func(i llrb.Item) bool {
		r := i.(recordDigest).Record
		err = f(r.Cid.Hash(), r.Offset)
		return err == nil
	})
	return err
}

func (ii *InsertionIndex) ForEachCid(f func(cid.Cid, uint64) error) error {
	var err error
	ii.items.AscendGreaterOrEqual(ii.items.Min(), func(i llrb.Item) bool {
		r := i.(recordDigest).Record
		err = f(r.Cid, r.Offset)
		return err == nil
	})
	return err
}

func (ii *InsertionIndex) Codec() multicodec.Code {
	return insertionIndexCodec
}

func (ii *InsertionIndex) Load(rs []Record) error {
	for _, r := range rs {
		rec := newRecordDigest(r)
		if rec.digest == nil {
			return fmt.Errorf("invalid entry: %v", r)
		}
		ii.items.InsertNoReplace(rec)
	}
	return nil
}

// flatten returns a formatted index in the given codec for more efficient subsequent loading.
func (ii *InsertionIndex) Flatten(codec multicodec.Code) (Index, error) {
	si, err := New(codec)
	if err != nil {
		return nil, err
	}
	rcrds := make([]Record, ii.items.Len())

	idx := 0
	iter := func(i llrb.Item) bool {
		rcrds[idx] = i.(recordDigest).Record
		idx++
		return true
	}
	ii.items.AscendGreaterOrEqual(ii.items.Min(), iter)

	if err := si.Load(rcrds); err != nil {
		return nil, err
	}
	return si, nil
}

// note that hasExactCID is very similar to GetAll,
// but it's separate as it allows us to compare Record.Cid directly,
// whereas GetAll just provides Record.Offset.

func (ii *InsertionIndex) HasExactCID(c cid.Cid) (bool, error) {
	d, err := multihash.Decode(c.Hash())
	if err != nil {
		return false, err
	}
	entry := recordDigest{digest: d.Digest}

	found := false
	iter := func(i llrb.Item) bool {
		existing := i.(recordDigest)
		if !bytes.Equal(existing.digest, entry.digest) {
			// We've already looked at all entries with matching digests.
			return false
		}
		if existing.Record.Cid == c {
			// We found an exact match.
			found = true
			return false
		}
		// Continue looking in ascending order.
		return true
	}
	ii.items.AscendGreaterOrEqual(entry, iter)
	return found, nil
}

func (ii *InsertionIndex) HasMultihash(mh multihash.Multihash) (bool, error) {
	d, err := multihash.Decode(mh)
	if err != nil {
		return false, err
	}
	entry := recordDigest{digest: d.Digest}

	found := false
	iter := func(i llrb.Item) bool {
		existing := i.(recordDigest)
		if !bytes.Equal(existing.digest, entry.digest) {
			// We've already looked at all entries with matching digests.
			return false
		}
		if bytes.Equal(existing.Record.Cid.Hash(), mh) {
			found = true
			return false
		}
		// Continue looking in ascending order.
		return true
	}
	ii.items.AscendGreaterOrEqual(entry, iter)
	return found, nil
}
