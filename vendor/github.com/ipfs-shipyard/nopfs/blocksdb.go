package nopfs

import "sync"

// BlocksDB is a key-value store of Entries. Keying may vary depending on
// whether we are indexing IPNS names, CIDs etc.
type BlocksDB struct {
	// TODO: this will eventually need to be replaced by a database with
	// its bloom filters etc. For a few million items this is just fine
	// though.
	blockDB sync.Map
}

// Load returns the Entries for a key.
func (b *BlocksDB) Load(key string) (Entries, bool) {
	val, ok := b.blockDB.Load(key)
	if !ok {
		return nil, false
	}
	entries, ok := val.(Entries)
	if !ok {
		logger.Error("cannot convert to entries")
		return nil, false
	}
	return entries, true
}

// Store stores a new entry with the given key. If there are existing Entries,
// the new Entry will be appended to them.
func (b *BlocksDB) Store(key string, entry Entry) {
	entries, _ := b.Load(key)
	entries = append(entries, entry)
	b.blockDB.Store(key, entries)
}
