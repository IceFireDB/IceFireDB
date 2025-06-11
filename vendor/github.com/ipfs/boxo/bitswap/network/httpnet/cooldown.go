package httpnet

import (
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
)

type cooldownTracker struct {
	maxBackoff time.Duration

	urlsLock sync.RWMutex
	urls     map[string]time.Time

	stop chan struct{}
}

func newCooldownTracker(maxBackoff time.Duration) *cooldownTracker {
	ct := &cooldownTracker{
		maxBackoff: maxBackoff,
		urls:       make(map[string]time.Time),
		stop:       make(chan struct{}),
	}

	go ct.cleaner()
	return ct
}

// every minute clean expired cooldowns.
func (ct *cooldownTracker) cleaner() {
	tick := time.NewTicker(time.Minute)
	for {
		select {
		case <-ct.stop:
			return
		case now := <-tick.C:
			ct.urlsLock.Lock()
			for host, dl := range ct.urls {
				if dl.Before(now) {
					delete(ct.urls, host)
				}
			}
			ct.urlsLock.Unlock()
		}
	}
}

func (ct *cooldownTracker) stopCleaner() {
	close(ct.stop)
}

func (ct *cooldownTracker) setByDate(host string, t time.Time) {
	latestDate := time.Now().Add(ct.maxBackoff)
	if t.After(latestDate) {
		t = latestDate
	}
	ct.urlsLock.Lock()
	ct.urls[host] = t
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) setByDuration(host string, d time.Duration) {
	if d > ct.maxBackoff {
		d = ct.maxBackoff
	}
	ct.urlsLock.Lock()
	ct.urls[host] = time.Now().Add(d)
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) remove(host string) {
	ct.urlsLock.Lock()
	delete(ct.urls, host)
	ct.urlsLock.Unlock()
}

func (ct *cooldownTracker) fillSenderURLs(urls []network.ParsedURL) []*senderURL {
	now := time.Now()
	surls := make([]*senderURL, len(urls))
	ct.urlsLock.RLock()
	{

		for i, u := range urls {
			var cooldown time.Time
			dl, ok := ct.urls[u.URL.Host]
			if ok && now.Before(dl) {
				cooldown = dl
			}
			surls[i] = &senderURL{
				ParsedURL: u,
			}
			surls[i].cooldown.Store(cooldown)

		}
	}
	ct.urlsLock.RUnlock()
	return surls
}
