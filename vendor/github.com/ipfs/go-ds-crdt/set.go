package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"time"

	bloom "github.com/ipfs/bbloom"
	pb "github.com/ipfs/go-ds-crdt/pb"
	logging "github.com/ipfs/go-log/v2"
	goprocess "github.com/jbenet/goprocess"
	multierr "go.uber.org/multierr"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var (
	elemsNs        = "s" // /elements namespace /set/s/<key>/<block>
	tombsNs        = "t" // /tombstones namespace /set/t/<key>/<block>
	keysNs         = "k" // /keys namespace /set/k/<key>/{v,p}
	valueSuffix    = "v" // for /keys namespace
	prioritySuffix = "p"
)

// set implements an Add-Wins Observed-Remove Set using delta-CRDTs
// (https://arxiv.org/abs/1410.2803) and backing all the data in a
// go-datastore. It is fully agnostic to MerkleCRDTs or the delta distribution
// layer.  It chooses the Value with most priority for a Key as the current
// Value. When two values have the same priority, it chooses by alphabetically
// sorting their unique IDs alphabetically.
type set struct {
	store      ds.Datastore
	namespace  ds.Key
	putHook    func(key string, v []byte)
	deleteHook func(key string)
	logger     logging.StandardLogger

	// Avoid merging two things at the same time since
	// we read-write value-priorities in a non-atomic way.
	putElemsMux sync.Mutex

	tombstonesBloom *bloom.Bloom
}

// Tombstones bloom filter options.
// We have optimized the defaults for speed:
// - commit 30 MiB of memory to the filter
// - only hash twice
// - False positive probabily is 1 in 10000 when working with 1M items in the filter.
// See https://hur.st/bloomfilter/?n=&p=0.0001&m=30MiB&k=2
var (
	TombstonesBloomFilterSize   float64 = 30 * 1024 * 1024 * 8 // 30 MiB
	TombstonesBloomFilterHashes float64 = 2
)

func newCRDTSet(
	ctx context.Context,
	d ds.Datastore,
	namespace ds.Key,
	logger logging.StandardLogger,
	putHook func(key string, v []byte),
	deleteHook func(key string),
) (*set, error) {

	blm, err := bloom.New(
		float64(TombstonesBloomFilterSize),
		float64(TombstonesBloomFilterHashes),
	)
	if err != nil {
		return nil, err
	}

	set := &set{
		namespace:       namespace,
		store:           d,
		logger:          logger,
		putHook:         putHook,
		deleteHook:      deleteHook,
		tombstonesBloom: blm,
	}

	return set, set.primeBloomFilter(ctx)
}

// We need to add all <key/block> keys in tombstones to the filter.
func (s *set) primeBloomFilter(ctx context.Context) error {
	tombsPrefix := s.keyPrefix(tombsNs) // /ns/tombs
	q := query.Query{
		Prefix:   tombsPrefix.String(),
		KeysOnly: true,
	}

	t := time.Now()
	nTombs := 0

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return err
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return r.Error
		}

		// Switch from /ns/tombs/key/block to /key/block
		key := ds.NewKey(
			strings.TrimPrefix(r.Key, tombsPrefix.String()))
		// Switch from /key/block to key
		key = key.Parent()
		// fmt.Println("Bloom filter priming with:", key)
		// put the key in the bloom cache
		s.tombstonesBloom.Add(key.Bytes())
		nTombs++
	}
	s.logger.Infof("Tombstones have bloomed: %d tombs. Took: %s", nTombs, time.Since(t))
	return nil
}

// Add returns a new delta-set adding the given key/value.
func (s *set) Add(ctx context.Context, key string, value []byte) *pb.Delta {
	return &pb.Delta{
		Elements: []*pb.Element{
			{
				Key:   key,
				Value: value,
			},
		},
		Tombstones: nil,
	}
}

// Rmv returns a new delta-set removing the given key.
func (s *set) Rmv(ctx context.Context, key string) (*pb.Delta, error) {
	delta := &pb.Delta{}

	// /namespace/<key>/elements
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			return nil, r.Error
		}
		id := strings.TrimPrefix(r.Key, prefix.String())
		if !ds.RawKey(id).IsTopLevel() {
			// our prefix matches blocks from other keys i.e. our
			// prefix is "hello" and we have a different key like
			// "hello/bye" so we have a block id like
			// "bye/<block>". If we got the right key, then the id
			// should be the block id only.
			continue
		}

		// check if its already tombed, which case don't add it to the
		// Rmv delta set.
		deleted, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return nil, err
		}
		if !deleted {
			delta.Tombstones = append(delta.Tombstones, &pb.Element{
				Key: key,
				Id:  id,
			})
		}
	}
	return delta, nil
}

// Element retrieves the value of an element from the CRDT set.
func (s *set) Element(ctx context.Context, key string) ([]byte, error) {
	// We can only GET an element if it's part of the Set (in
	// "elements" and not in "tombstones").

	// As an optimization:
	// * If the key has a value in the store it means:
	//   -> It occurs at least once in "elems"
	//   -> It may or not be tombstoned
	// * If the key does not have a value in the store:
	//   -> It was either never added
	valueK := s.valueKey(key)
	value, err := s.store.Get(ctx, valueK)
	if err != nil { // not found is fine, we just return it
		return value, err
	}

	// We have an existing element. Check if tombstoned.
	inSet, err := s.checkNotTombstoned(ctx, key)
	if err != nil {
		return nil, err
	}

	if !inSet {
		// attempt to remove so next time we do not have to do this
		// lookup.
		// In concurrency, this may delete a key that was just written
		// and should not be deleted.
		// s.store.Delete(valueK)

		return nil, ds.ErrNotFound
	}
	// otherwise return the value
	return value, nil
}

// Elements returns all the elements in the set.
func (s *set) Elements(ctx context.Context, q query.Query) (query.Results, error) {
	// This will cleanup user the query prefix first.
	// This makes sure the use of things like "/../" in the query
	// does not affect our setQuery.
	srcQueryPrefixKey := ds.NewKey(q.Prefix)

	keyNamespacePrefix := s.keyPrefix(keysNs)
	keyNamespacePrefixStr := keyNamespacePrefix.String()
	setQueryPrefix := keyNamespacePrefix.Child(srcQueryPrefixKey).String()
	vSuffix := "/" + valueSuffix

	// We are going to be reading everything in the /set/ namespace which
	// will return items in the form:
	// * /set/<key>/value
	// * /set<key>/priority (a Uvarint)

	// It is clear that KeysOnly=true should be used here when the original
	// query only wants keys.
	//
	// However, there is a question of what is best when the original
	// query wants also values:
	// * KeysOnly: true avoids reading all the priority key values
	//   which are skipped at the cost of doing a separate Get() for the
	//   values (50% of the keys).
	// * KeysOnly: false reads everything from the start. Priorities
	//   and tombstoned values are read for nothing
	//
	// In-mem benchmarking shows no clear winner. Badger docs say that
	// KeysOnly "is several order of magnitudes faster than regular
	// iteration". Contrary to my original feeling, however, live testing
	// with a 50GB badger with millions of keys shows more speed when
	// querying with value. It may be that speed is fully affected by the
	// current state of table compaction as well.
	setQuery := query.Query{
		Prefix:   setQueryPrefix,
		KeysOnly: false,
	}

	// send the result and returns false if we must exit
	sendResult := func(b *query.ResultBuilder, p goprocess.Process, r query.Result) bool {
		select {
		case b.Output <- r:
		case <-p.Closing():
			return false
		}
		return r.Error == nil
	}

	// The code below is very inspired in the Query implementation in
	// flatfs.

	// NewResultBuilder(q) gives us a ResultBuilder with a channel of
	// capacity 1 when using KeysOnly = false, and 128 otherwise.
	//
	// Having a 128-item buffered channel was an improvement to speed up
	// keys-only queries, but there is no explanation on how other
	// non-key only queries would improve.
	// See: https://github.com/ipfs/go-datastore/issues/40
	//
	// I do not see a huge noticeable improvement when forcing a 128 with
	// in-mem stores, but I also don't see how some leeway can make things
	// worse (real-world testing suggest it is not horrible at least).
	//
	// b := query.NewResultBuilder(q)
	b := &query.ResultBuilder{
		Query:  q,
		Output: make(chan query.Result, 128),
	}
	b.Process = goprocess.WithTeardown(func() error {
		close(b.Output)
		return nil
	})

	b.Process.Go(func(p goprocess.Process) {
		results, err := s.store.Query(ctx, setQuery)
		if err != nil {
			sendResult(b, p, query.Result{Error: err})
			return
		}
		defer results.Close()

		var entry query.Entry
		for r := range results.Next() {
			if r.Error != nil {
				sendResult(b, p, query.Result{Error: r.Error})
				return
			}

			// We will be getting keys in the form of
			// /namespace/keys/<key>/v and /namespace/keys/<key>/p
			// We discard anything not ending in /v and sanitize
			// those from:
			// /namespace/keys/<key>/v -> <key>
			if !strings.HasSuffix(r.Key, vSuffix) { // "/v"
				continue
			}

			key := strings.TrimSuffix(
				strings.TrimPrefix(r.Key, keyNamespacePrefixStr),
				"/"+valueSuffix,
			)

			entry.Key = key
			entry.Value = r.Value
			entry.Size = r.Size
			entry.Expiration = r.Expiration
			has, err := s.checkNotTombstoned(ctx, key)
			if err != nil {
				sendResult(b, p, query.Result{Error: err})
				return
			}

			if !has {
				continue
			}
			if q.KeysOnly {
				entry.Size = -1
				entry.Value = nil
			}
			if !sendResult(b, p, query.Result{Entry: entry}) {
				return
			}
		}
	})
	go b.Process.CloseAfterChildren() //nolint
	return b.Results(), nil
}

// InSet returns true if the key belongs to one of the elements in the "elems"
// set, and this element is not tombstoned.
func (s *set) InSet(ctx context.Context, key string) (bool, error) {
	// Optimization: if we do not have a value
	// this key was never added.
	valueK := s.valueKey(key)
	if ok, err := s.store.Has(ctx, valueK); !ok {
		return false, err
	}

	// Otherwise, do the long check.
	return s.checkNotTombstoned(ctx, key)
}

// Returns true when we have a key/block combination in the
// elements set that has not been tombstoned.
//
// Warning: In order to do a quick bloomfilter check, this assumes the key is
// in elems already. Any code calling this function already has verified
// that there is a value-key entry for the key, thus there must necessarily
// be a non-empty set of key/block in elems.
//
// Put otherwise: this code will misbehave when called directly to check if an
// element exists. See Element()/InSet() etc..
func (s *set) checkNotTombstoned(ctx context.Context, key string) (bool, error) {
	// Bloom filter check: has this key been potentially tombstoned?

	// fmt.Println("Bloom filter check:", key)
	if !s.tombstonesBloom.HasTS([]byte(key)) {
		return true, nil
	}

	// /namespace/elems/<key>
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return false, err
	}
	defer results.Close()

	// range all the /namespace/elems/<key>/<block_cid>.
	for r := range results.Next() {
		if r.Error != nil {
			return false, err
		}

		id := strings.TrimPrefix(r.Key, prefix.String())
		if !ds.RawKey(id).IsTopLevel() {
			// our prefix matches blocks from other keys i.e. our
			// prefix is "hello" and we have a different key like
			// "hello/bye" so we have a block id like
			// "bye/<block>". If we got the right key, then the id
			// should be the block id only.
			continue
		}
		// if not tombstoned, we have it
		inTomb, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return false, err
		}
		if !inTomb {
			return true, nil
		}
	}
	return false, nil
}

// /namespace/<key>
func (s *set) keyPrefix(key string) ds.Key {
	return s.namespace.ChildString(key)
}

// /namespace/elems/<key>
func (s *set) elemsPrefix(key string) ds.Key {
	return s.keyPrefix(elemsNs).ChildString(key)
}

// /namespace/tombs/<key>
func (s *set) tombsPrefix(key string) ds.Key {
	return s.keyPrefix(tombsNs).ChildString(key)
}

// /namespace/keys/<key>/value
func (s *set) valueKey(key string) ds.Key {
	return s.keyPrefix(keysNs).ChildString(key).ChildString(valueSuffix)
}

// /namespace/keys/<key>/priority
func (s *set) priorityKey(key string) ds.Key {
	return s.keyPrefix(keysNs).ChildString(key).ChildString(prioritySuffix)
}

func (s *set) getPriority(ctx context.Context, key string) (uint64, error) {
	prioK := s.priorityKey(key)
	data, err := s.store.Get(ctx, prioK)
	if err != nil {
		if err == ds.ErrNotFound {
			return 0, nil
		}
		return 0, err
	}

	prio, n := binary.Uvarint(data)
	if n <= 0 {
		return prio, errors.New("error decoding priority")
	}
	return prio - 1, nil
}

func (s *set) setPriority(ctx context.Context, writeStore ds.Write, key string, prio uint64) error {
	prioK := s.priorityKey(key)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, prio+1)
	if n == 0 {
		return errors.New("error encoding priority")
	}

	return writeStore.Put(ctx, prioK, buf[0:n])
}

// sets a value if priority is higher. When equal, it sets if the
// value is lexicographically higher than the current value.
func (s *set) setValue(ctx context.Context, writeStore ds.Write, key, id string, value []byte, prio uint64) error {
	// If this key was tombstoned already, do not store/update the value
	// at all.
	deleted, err := s.inTombsKeyID(ctx, key, id)
	if err != nil || deleted {
		return err
	}

	curPrio, err := s.getPriority(ctx, key)
	if err != nil {
		return err
	}

	if prio < curPrio {
		return nil
	}
	valueK := s.valueKey(key)

	if prio == curPrio {
		curValue, _ := s.store.Get(ctx, valueK)
		// new value greater than old
		if bytes.Compare(curValue, value) >= 0 {
			return nil
		}
	}

	// store value
	err = writeStore.Put(ctx, valueK, value)
	if err != nil {
		return err
	}

	// store priority
	err = s.setPriority(ctx, writeStore, key, prio)
	if err != nil {
		return err
	}

	// trigger add hook
	s.putHook(key, value)
	return nil
}

// putElems adds items to the "elems" set. It will also set current
// values and priorities for each element. This needs to run in a lock,
// as otherwise races may occur when reading/writing the priorities, resulting
// in bad behaviours.
//
// Technically the lock should only affect the keys that are being written,
// but with the batching optimization the locks would need to be hold until
// the batch is written), and one lock per key might be way worse than a single
// global lock in the end.
func (s *set) putElems(ctx context.Context, elems []*pb.Element, id string, prio uint64) error {
	s.putElemsMux.Lock()
	defer s.putElemsMux.Unlock()

	if len(elems) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	for _, e := range elems {
		e.Id = id // overwrite the identifier as it would come unset
		key := e.GetKey()
		// /namespace/elems/<key>/<id>
		k := s.elemsPrefix(key).ChildString(id)
		err := store.Put(ctx, k, nil)
		if err != nil {
			return err
		}

		// update the value if applicable:
		// * higher priority than we currently have.
		// * not tombstoned before.
		err = s.setValue(ctx, store, key, id, e.GetValue(), prio)
		if err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *set) putTombs(ctx context.Context, tombs []*pb.Element) error {
	if len(tombs) == 0 {
		return nil
	}

	var store ds.Write = s.store
	var err error
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	deletedElems := make(map[string]struct{})
	for _, e := range tombs {
		// /namespace/tombs/<key>/<id>
		elemKey := e.GetKey()
		k := s.tombsPrefix(elemKey).ChildString(e.GetId())
		err := store.Put(ctx, k, nil)
		if err != nil {
			return err
		}
		s.tombstonesBloom.AddTS([]byte(elemKey))
		//fmt.Println("Bloom filter add:", elemKey)
		// run delete hook only once for all
		// versions of the same element tombstoned
		// in this delta
		if _, ok := deletedElems[elemKey]; !ok {
			deletedElems[elemKey] = struct{}{}
			s.deleteHook(elemKey)
		}
	}

	if batching {
		err := store.(ds.Batch).Commit(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *set) Merge(ctx context.Context, d *pb.Delta, id string) error {
	err := s.putTombs(ctx, d.GetTombstones())
	if err != nil {
		return err
	}

	return s.putElems(ctx, d.GetElements(), id, d.GetPriority())
}

// currently unused
// func (s *set) inElemsKeyID(key, id string) (bool, error) {
// 	k := s.elemsPrefix(key).ChildString(id)
// 	return s.store.Has(k)
// }

func (s *set) inTombsKeyID(ctx context.Context, key, id string) (bool, error) {
	k := s.tombsPrefix(key).ChildString(id)
	return s.store.Has(ctx, k)
}

// currently unused
// // inSet returns if the given cid/block is in elems and not in tombs (and
// // thus, it is an element of the set).
// func (s *set) inSetKeyID(key, id string) (bool, error) {
// 	inTombs, err := s.inTombsKeyID(key, id)
// 	if err != nil {
// 		return false, err
// 	}
// 	if inTombs {
// 		return false, nil
// 	}

// 	return s.inElemsKeyID(key, id)
// }

// perform a sync against all the paths associated with a key prefix
func (s *set) datastoreSync(ctx context.Context, prefix ds.Key) error {
	prefixStr := prefix.String()
	toSync := []ds.Key{
		s.elemsPrefix(prefixStr),
		s.tombsPrefix(prefixStr),
		s.keyPrefix(keysNs).Child(prefix), // covers values and priorities
	}

	errs := make([]error, len(toSync))

	for i, k := range toSync {
		if err := s.store.Sync(ctx, k); err != nil {
			errs[i] = err
		}
	}

	return multierr.Combine(errs...)
}
