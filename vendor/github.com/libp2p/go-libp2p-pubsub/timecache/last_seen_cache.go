package timecache

import (
	"context"
	"sync"
	"time"
)

// LastSeenCache is a time cache that extends the expiry of a seen message when added
// or checked for presence with Has..
type LastSeenCache struct {
	lk  sync.Mutex
	m   map[string]time.Time
	ttl time.Duration

	done func()
}

var _ TimeCache = (*LastSeenCache)(nil)

func newLastSeenCache(ttl time.Duration) *LastSeenCache {
	return newLastSeenCacheWithSweepInterval(ttl, backgroundSweepInterval)
}

func newLastSeenCacheWithSweepInterval(ttl time.Duration, sweepInterval time.Duration) *LastSeenCache {
	tc := &LastSeenCache{
		m:   make(map[string]time.Time),
		ttl: ttl,
	}

	ctx, done := context.WithCancel(context.Background())
	tc.done = done
	go background(ctx, &tc.lk, tc.m, sweepInterval)

	return tc
}

func (tc *LastSeenCache) Done() {
	tc.done()
}

func (tc *LastSeenCache) Add(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	tc.m[s] = time.Now().Add(tc.ttl)

	return !ok
}

func (tc *LastSeenCache) Has(s string) bool {
	tc.lk.Lock()
	defer tc.lk.Unlock()

	_, ok := tc.m[s]
	if ok {
		tc.m[s] = time.Now().Add(tc.ttl)
	}

	return ok
}
