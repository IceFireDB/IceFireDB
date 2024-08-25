package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"sync"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
)

// heads manages the current Merkle-CRDT heads.
type heads struct {
	store ds.Datastore
	// cache contains the current contents of the store
	cache     map[cid.Cid]uint64
	cacheMux  sync.RWMutex
	namespace ds.Key
	logger    logging.StandardLogger
}

func newHeads(store ds.Datastore, namespace ds.Key, logger logging.StandardLogger) (*heads, error) {
	hh := &heads{
		store:     store,
		namespace: namespace,
		logger:    logger,
		cache:     make(map[cid.Cid]uint64),
	}
	if err := hh.primeCache(context.Background()); err != nil {
		return nil, err
	}
	return hh, nil
}

func (hh *heads) key(c cid.Cid) ds.Key {
	// /<namespace>/<cid>
	return hh.namespace.Child(dshelp.MultihashToDsKey(c.Hash()))
}

func (hh *heads) write(ctx context.Context, store ds.Write, c cid.Cid, height uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, height)
	if n == 0 {
		return errors.New("error encoding height")
	}
	return store.Put(ctx, hh.key(c), buf[0:n])
}

func (hh *heads) delete(ctx context.Context, store ds.Write, c cid.Cid) error {
	err := store.Delete(ctx, hh.key(c))
	// The go-datastore API currently says Delete doesn't return
	// ErrNotFound, but it used to say otherwise.  Leave this
	// here to be safe.
	if err == ds.ErrNotFound {
		return nil
	}
	return err
}

// IsHead returns if a given cid is among the current heads.
func (hh *heads) IsHead(c cid.Cid) (bool, uint64, error) {
	var height uint64
	var ok bool
	hh.cacheMux.RLock()
	{
		height, ok = hh.cache[c]
	}
	hh.cacheMux.RUnlock()
	return ok, height, nil
}

func (hh *heads) Len() (int, error) {
	var ret int
	hh.cacheMux.RLock()
	{
		ret = len(hh.cache)
	}
	hh.cacheMux.RUnlock()
	return ret, nil
}

// Replace replaces a head with a new cid.
func (hh *heads) Replace(ctx context.Context, h, c cid.Cid, height uint64) error {
	hh.logger.Debugf("replacing DAG head: %s -> %s (new height: %d)", h, c, height)
	var store ds.Write = hh.store

	batchingDs, batching := store.(ds.Batching)
	var err error
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	err = hh.write(ctx, store, c, height)
	if err != nil {
		return err
	}

	hh.cacheMux.Lock()
	defer hh.cacheMux.Unlock()

	if !batching {
		hh.cache[c] = height
	}

	err = hh.delete(ctx, store, h)
	if err != nil {
		return err
	}
	if !batching {
		delete(hh.cache, h)
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
		delete(hh.cache, h)
		hh.cache[c] = height
	}
	return nil
}

func (hh *heads) Add(ctx context.Context, c cid.Cid, height uint64) error {
	hh.logger.Debugf("adding new DAG head: %s (height: %d)", c, height)
	if err := hh.write(ctx, hh.store, c, height); err != nil {
		return err
	}

	hh.cacheMux.Lock()
	{
		hh.cache[c] = height
	}
	hh.cacheMux.Unlock()
	return nil
}

// List returns the list of current heads plus the max height.
func (hh *heads) List() ([]cid.Cid, uint64, error) {
	var maxHeight uint64
	var heads []cid.Cid

	hh.cacheMux.RLock()
	{
		heads = make([]cid.Cid, 0, len(hh.cache))
		for head, height := range hh.cache {
			heads = append(heads, head)
			if height > maxHeight {
				maxHeight = height
			}
		}
	}
	hh.cacheMux.RUnlock()

	sort.Slice(heads, func(i, j int) bool {
		ci := heads[i].Bytes()
		cj := heads[j].Bytes()
		return bytes.Compare(ci, cj) < 0
	})

	return heads, maxHeight, nil
}

// primeCache builds the heads cache based on what's in storage; since
// it is called from the constructor only we don't bother locking.
func (hh *heads) primeCache(ctx context.Context) (ret error) {
	q := query.Query{
		Prefix:   hh.namespace.String(),
		KeysOnly: false,
	}

	results, err := hh.store.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return r.Error
		}
		headKey := ds.NewKey(strings.TrimPrefix(r.Key, hh.namespace.String()))
		headCid, err := dshelp.DsKeyToCidV1(headKey, cid.DagProtobuf)
		if err != nil {
			return err
		}
		height, n := binary.Uvarint(r.Value)
		if n <= 0 {
			return errors.New("error decoding height")
		}

		hh.cache[headCid] = height
	}

	return nil
}
