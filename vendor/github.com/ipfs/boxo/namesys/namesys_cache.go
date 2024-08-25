package namesys

import (
	"time"

	"github.com/ipfs/boxo/path"
)

type cacheEntry struct {
	val      path.Path     // is the value of this entry
	ttl      time.Duration // is the ttl of this entry
	lastMod  time.Time     // is the last time this entry was modified
	cacheEOL time.Time     // is until when we keep this entry in cache
}

func (ns *namesys) cacheGet(name string) (path.Path, time.Duration, time.Time, bool) {
	// existence of optional mapping defined via IPFS_NS_MAP is checked first
	if ns.staticMap != nil {
		entry, ok := ns.staticMap[name]
		if ok {
			return entry.val, entry.ttl, entry.lastMod, true
		}
	}

	if ns.cache == nil {
		return nil, 0, time.Now(), false
	}

	entry, ok := ns.cache.Get(name)
	if !ok {
		return nil, 0, time.Now(), false
	}

	if time.Now().Before(entry.cacheEOL) {
		return entry.val, entry.ttl, entry.lastMod, true
	}

	// We do not delete the entry from the cache. Removals are handled by the
	// backing cache system. It is useful to keep it since cacheSet can use
	// previously existing values to heuristically update a cache entry.
	return nil, 0, time.Now(), false
}

func (ns *namesys) cacheSet(name string, val path.Path, ttl time.Duration, lastMod time.Time) {
	if ns.cache == nil || ttl <= 0 {
		return
	}

	// Set the current date if there's no lastMod.
	if lastMod.IsZero() {
		lastMod = time.Now()
	}

	// If there's an already cached version with the same path, but
	// different lastMod date, keep the oldest.
	entry, ok := ns.cache.Get(name)
	if ok && entry.val.String() == val.String() {
		if lastMod.After(entry.lastMod) {
			lastMod = entry.lastMod
		}
	}

	// The cache TTL is capped at the configured maxCacheTTL. If not
	// configured, the entry TTL will always be used.
	cacheTTL := ttl
	if ns.maxCacheTTL != nil && cacheTTL > *ns.maxCacheTTL {
		cacheTTL = *ns.maxCacheTTL
	}
	cacheEOL := time.Now().Add(cacheTTL)

	// Add automatically evicts previous entry, so it works for updating.
	ns.cache.Add(name, cacheEntry{
		val:      val,
		ttl:      ttl,
		lastMod:  lastMod,
		cacheEOL: cacheEOL,
	})
}

func (ns *namesys) cacheInvalidate(name string) {
	if ns.cache == nil {
		return
	}

	ns.cache.Remove(name)
}
