package timecache

import (
	"context"
	"sync"
	"time"
)

const backgroundSweepInterval = time.Minute

func background(ctx context.Context, lk sync.Locker, m map[string]time.Time, tickerDur time.Duration) {
	ticker := time.NewTicker(tickerDur)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			sweep(lk, m, now)

		case <-ctx.Done():
			return
		}
	}
}

func sweep(lk sync.Locker, m map[string]time.Time, now time.Time) {
	lk.Lock()
	defer lk.Unlock()

	for k, expiry := range m {
		if expiry.Before(now) {
			delete(m, k)
		}
	}
}
