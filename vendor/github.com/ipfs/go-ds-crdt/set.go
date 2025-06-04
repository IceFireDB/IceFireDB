package crdt

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	dshelp "github.com/ipfs/boxo/datastore/dshelp"
	cid "github.com/ipfs/go-cid"
	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
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
	dagService ipld.DAGService
	namespace  ds.Key
	putHook    func(key string, v []byte)
	deleteHook func(key string)
	logger     logging.StandardLogger

	// Avoid merging two things at the same time since
	// we read-write value-priorities in a non-atomic way.
	putElemsMux sync.Mutex
}

func newCRDTSet(
	ctx context.Context,
	d ds.Datastore,
	namespace ds.Key,
	dagService ipld.DAGService,
	logger logging.StandardLogger,
	putHook func(key string, v []byte),
	deleteHook func(key string),
) (*set, error) {

	set := &set{
		namespace:  namespace,
		store:      d,
		dagService: dagService,
		logger:     logger,
		putHook:    putHook,
		deleteHook: deleteHook,
	}

	return set, nil
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

	// * If the key has a value in the store it means that it has been
	//   written and is alive. putTombs will delete the value if all elems
	//   are tombstoned, or leave the best one.

	valueK := s.valueKey(key)
	value, err := s.store.Get(ctx, valueK)
	if err != nil { // not found is fine, we just return it
		return value, err
	}
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

	// send the result and returns false if we must exit.
	sendResult := func(ctx, qctx context.Context, r query.Result, out chan<- query.Result) bool {
		select {
		case out <- r:
		case <-ctx.Done():
			return false
		case <-qctx.Done():
			return false
		}
		return r.Error == nil
	}

	// The code below was very inspired in the Query implementation in
	// flatfs.

	// Originally we were able to set the output channel capacity and it
	// was set to 128 even though not much difference to 1 could be
	// observed on mem-based testing.

	// Using KeysOnly still gives a 128-item channel.
	// See: https://github.com/ipfs/go-datastore/issues/40
	r := query.ResultsWithContext(q, func(qctx context.Context, out chan<- query.Result) {
		// qctx is a Background context for the query. It is not
		// associated to ctx. It is closed when this function finishes
		// along with the output channel, or when the Results are
		// Closed directly.
		results, err := s.store.Query(ctx, setQuery)
		if err != nil {
			sendResult(ctx, qctx, query.Result{Error: err}, out)
			return
		}
		defer results.Close()

		var entry query.Entry
		for r := range results.Next() {
			if r.Error != nil {
				sendResult(ctx, qctx, query.Result{Error: r.Error}, out)
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

			// The fact that /v is set means it is not tombstoned,
			// as tombstoning removes /v and /p or sets them to
			// the best value.

			if q.KeysOnly {
				entry.Size = -1
				entry.Value = nil
			}
			if !sendResult(ctx, qctx, query.Result{Entry: entry}, out) {
				return
			}
		}
	})

	return r, nil
}

// InSet returns true if the key belongs to one of the elements in the "elems"
// set, and this element is not tombstoned.
func (s *set) InSet(ctx context.Context, key string) (bool, error) {
	// If we do not have a value this key was never added or it was fully
	// tombstoned.
	valueK := s.valueKey(key)
	return s.store.Has(ctx, valueK)
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
	// If this key was tombstoned already, do not store/update the value.
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

// findBestValue looks for all entries for the given key, figures out their
// priority from their delta (skipping the blocks by the given pendingTombIDs)
// and returns the value with the highest priority that is not tombstoned nor
// about to be tombstoned.
func (s *set) findBestValue(ctx context.Context, key string, pendingTombIDs []string) ([]byte, uint64, error) {
	// /namespace/elems/<key>
	prefix := s.elemsPrefix(key)
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := s.store.Query(ctx, q)
	if err != nil {
		return nil, 0, err
	}
	defer results.Close()

	var bestValue []byte
	var bestPriority uint64
	var deltaCid cid.Cid
	ng := crdtNodeGetter{NodeGetter: s.dagService}

	// range all the /namespace/elems/<key>/<block_cid>.
NEXT:
	for r := range results.Next() {
		if r.Error != nil {
			return nil, 0, err
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
		// if block is one of the pending tombIDs, continue
		for _, tombID := range pendingTombIDs {
			if tombID == id {
				continue NEXT
			}
		}

		// if tombstoned, continue
		inTomb, err := s.inTombsKeyID(ctx, key, id)
		if err != nil {
			return nil, 0, err
		}
		if inTomb {
			continue
		}

		// get the block
		mhash, err := dshelp.DsKeyToMultihash(ds.NewKey(id))
		if err != nil {
			return nil, 0, err
		}
		deltaCid = cid.NewCidV1(cid.DagProtobuf, mhash)
		_, delta, err := ng.GetDelta(ctx, deltaCid)
		if err != nil {
			return nil, 0, err
		}

		// discard this delta.
		if delta.Priority < bestPriority {
			continue
		}

		// When equal priority, choose the greatest among values in
		// the delta and current. When higher priority, choose the
		// greatest only among those in the delta.
		var greatestValueInDelta []byte
		for _, elem := range delta.GetElements() {
			if elem.GetKey() != key {
				continue
			}
			v := elem.GetValue()
			if bytes.Compare(greatestValueInDelta, v) < 0 {
				greatestValueInDelta = v
			}
		}

		if delta.Priority > bestPriority {
			bestValue = greatestValueInDelta
			bestPriority = delta.Priority
			continue
		}

		// equal priority
		if bytes.Compare(bestValue, greatestValueInDelta) < 0 {
			bestValue = greatestValueInDelta
		}
	}

	return bestValue, bestPriority, nil
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

	// key -> tombstonedBlockID. Carries the tombstoned blocks for each
	// element in this delta.
	deletedElems := make(map[string][]string)

	for _, e := range tombs {
		// /namespace/tombs/<key>/<id>
		key := e.GetKey()
		id := e.GetId()
		valueK := s.valueKey(key)
		deletedElems[key] = append(deletedElems[key], id)

		// Find best value for element that we are going to delete
		v, p, err := s.findBestValue(ctx, key, deletedElems[key])
		if err != nil {
			return err
		}
		if v == nil {
			store.Delete(ctx, valueK)
			store.Delete(ctx, s.priorityKey(key))
		} else {
			store.Put(ctx, valueK, v)
			s.setPriority(ctx, store, key, p)
		}

		// Write tomb into store.
		k := s.tombsPrefix(key).ChildString(id)
		err = store.Put(ctx, k, nil)
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

	// run delete hook only once for all versions of the same element
	// tombstoned in this delta. Note it may be that the element was not
	// fully deleted and only a different value took its place.
	for del := range deletedElems {
		s.deleteHook(del)
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
