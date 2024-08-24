package entry

import (
	"sync"

	"berty.tech/go-ipfs-log/iface"
)

// OrderedMap is an ordered map of entries.
type OrderedMap struct {
	lock   sync.RWMutex
	keys   []string
	values map[string]iface.IPFSLogEntry
}

func (o *OrderedMap) Copy() iface.IPFSLogOrderedEntries {
	o.lock.RLock()
	defer o.lock.RUnlock()

	values := map[string]iface.IPFSLogEntry{}
	for k, v := range o.values {
		values[k] = v
	}

	keys := make([]string, len(o.keys))
	copy(keys, o.keys)

	return &OrderedMap{
		keys:   keys,
		values: values,
	}
}

func (o *OrderedMap) Reverse() iface.IPFSLogOrderedEntries {
	o.lock.Lock()
	defer o.lock.Unlock()

	for i := len(o.keys)/2 - 1; i >= 0; i-- {
		opp := len(o.keys) - 1 - i
		o.keys[i], o.keys[opp] = o.keys[opp], o.keys[i]
	}

	return o
}

// NewOrderedMap creates a new OrderedMap of entries.
func NewOrderedMap() iface.IPFSLogOrderedEntries {
	return &OrderedMap{
		lock:   sync.RWMutex{},
		values: map[string]iface.IPFSLogEntry{},
	}
}

// NewOrderedMapFromEntries creates a new OrderedMap of entries from a slice.
func NewOrderedMapFromEntries(entries []iface.IPFSLogEntry) iface.IPFSLogOrderedEntries {
	orderedMap := NewOrderedMap()

	for _, e := range entries {
		if e == nil || !e.Defined() {
			continue
		}

		orderedMap.Set(e.GetHash().String(), e)
	}

	return orderedMap
}

// Merge will fusion two OrderedMap of entries.
func (o *OrderedMap) Merge(other iface.IPFSLogOrderedEntries) iface.IPFSLogOrderedEntries {
	newMap := NewOrderedMap()

	for _, k := range o.Keys() {
		val, _ := o.Get(k)
		newMap.Set(k, val)
	}

	for _, k := range other.Keys() {
		val, _ := other.Get(k)
		newMap.Set(k, val)
	}

	return newMap
}

// Get retrieves an Entry using its key.
func (o *OrderedMap) Get(key string) (iface.IPFSLogEntry, bool) {
	o.lock.RLock()
	defer o.lock.RUnlock()

	val, exists := o.values[key]
	return val, exists
}

// UnsafeGet retrieves an Entry using its key, returns nil if not found.
func (o *OrderedMap) UnsafeGet(key string) iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	val, _ := o.Get(key)

	return val
}

// Set defines an Entry in the map for a given key.
func (o *OrderedMap) Set(key string, value iface.IPFSLogEntry) {
	o.lock.Lock()
	defer o.lock.Unlock()

	_, exists := o.values[key]
	if !exists {
		o.keys = append(o.keys, key)
	}
	o.values[key] = value
}

// Slice returns an ordered slice of the values existing in the map.
func (o *OrderedMap) Slice() []iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	keysCount := len(o.keys)
	out := make([]iface.IPFSLogEntry, keysCount)

	for i, k := range o.keys {
		out[i] = o.UnsafeGet(k)
	}

	return out
}

// Keys retrieves the ordered list of keys in the map.
func (o *OrderedMap) Keys() []string {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return o.keys
}

// Len gets the length of the map.
func (o *OrderedMap) Len() int {
	o.lock.RLock()
	defer o.lock.RUnlock()

	return len(o.keys)
}

// At gets an item at the given index in the map, returns nil if not found.
func (o *OrderedMap) At(index uint) iface.IPFSLogEntry {
	o.lock.RLock()
	defer o.lock.RUnlock()

	if uint(len(o.keys)) <= index {
		return nil
	}

	key := o.keys[index]

	return o.UnsafeGet(key)
}

var _ iface.IPFSLogOrderedEntries = (*OrderedMap)(nil)
