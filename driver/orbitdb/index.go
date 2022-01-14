package orbitdb

import (
	"sync"

	ipfslog "berty.tech/go-ipfs-log"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/operation"
	"github.com/pkg/errors"
)

type kvIndex struct {
	index   map[string][]byte
	muIndex sync.RWMutex
}

func (i *kvIndex) Get(key string) interface{} {
	i.muIndex.RLock()
	defer i.muIndex.RUnlock()

	return i.index[key]
}

func (i *kvIndex) UpdateIndex(oplog ipfslog.Log, _ []ipfslog.Entry) error {
	entries := oplog.Values().Slice()
	size := len(entries)

	handled := map[string]struct{}{}

	i.muIndex.Lock()
	defer i.muIndex.Unlock()

	for idx := range entries {
		item, err := operation.ParseOperation(entries[size-idx-1])
		if err != nil {
			return errors.Wrap(err, "unable to parse log kv operation")
		}

		key := item.GetKey()
		if key == nil {
			// ignoring entries with nil keys
			continue
		}

		if _, ok := handled[*item.GetKey()]; !ok {
			handled[*item.GetKey()] = struct{}{}

			if item.GetOperation() == "PUT" {
				i.index[*item.GetKey()] = item.GetValue()
			} else if item.GetOperation() == "DEL" {
				delete(i.index, *item.GetKey())
			}
		}
	}

	return nil
}

// NewKVIndex Creates a new Index instance for a KeyValue store
func NewKVIndex(_ []byte) iface.StoreIndex {
	return &kvIndex{
		index: map[string][]byte{},
	}
}

var _ iface.IndexConstructor = NewKVIndex
var _ iface.StoreIndex = &kvIndex{}
