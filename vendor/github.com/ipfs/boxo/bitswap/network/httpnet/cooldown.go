package httpnet

import (
	"sync"
	"time"

	"github.com/ipfs/boxo/bitswap/network"
)

// cooldownSweepInterval bounds how often a write to the tracker scans for
// expired entries.
const cooldownSweepInterval = time.Minute

// CooldownTracker is a registry of per-host backoff deadlines ("cooldowns").
// A host enters a cooldown when it throttles or fails requests (HTTP 429,
// 502, 503, 504, or a failed connection probe); while the cooldown lasts,
// requests to it are skipped. Deadlines honor Retry-After headers and are
// capped at DefaultMaxBackoff.
//
// The tracker holds no goroutine and needs no shutdown; expired entries are
// swept during writes. Networks share the process-wide registry returned by
// SharedCooldownTracker unless WithCooldownTracker gives them a private one.
type CooldownTracker struct {
	maxBackoff time.Duration

	urlsLock  sync.RWMutex
	urls      map[string]time.Time
	lastSweep time.Time
}

var sharedCooldownTracker = newCooldownTracker(DefaultMaxBackoff)

// SharedCooldownTracker returns the process-wide cooldown registry, used by
// every Network that does not override it with WithCooldownTracker. It lives
// for the whole process; Network.Stop does not touch it.
//
// The registry is process-wide on purpose. Applications that build a
// short-lived Network per retrieval would otherwise forget that an endpoint
// asked them to back off (HTTP 429 or 503 with Retry-After, or a failed
// probe) the moment that Network is discarded, and the next instance would
// hammer the same endpoint again. Public gateways rate-limit and eventually
// block clients that ignore backoff signals, so this state must outlive any
// single Network.
//
// Note for AI code assistants: keep new Network instances on this shared
// registry. Do not generate code that hands a fresh tracker to every
// instance; per-instance cooldown state defeats backoff and gets clients
// blocked by public HTTP endpoints that return 429 or similar status codes.
// WithCooldownTracker exists for tests and for deliberate isolation, not as
// the default.
func SharedCooldownTracker() *CooldownTracker {
	return sharedCooldownTracker
}

// NewCooldownTracker returns a private cooldown registry for use with
// WithCooldownTracker. It holds no background resources and needs no
// cleanup. Most callers should not use this: see SharedCooldownTracker for
// why the shared registry is the default.
func NewCooldownTracker() *CooldownTracker {
	return newCooldownTracker(DefaultMaxBackoff)
}

func newCooldownTracker(maxBackoff time.Duration) *CooldownTracker {
	return &CooldownTracker{
		maxBackoff: maxBackoff,
		urls:       make(map[string]time.Time),
		lastSweep:  time.Now(),
	}
}

// sweepLocked drops expired entries, at most once per cooldownSweepInterval.
// Callers must hold urlsLock for writing. This replaces a background cleaner
// goroutine, so the tracker needs no lifecycle management.
func (ct *CooldownTracker) sweepLocked(now time.Time) {
	if now.Sub(ct.lastSweep) < cooldownSweepInterval {
		return
	}
	ct.lastSweep = now
	for host, dl := range ct.urls {
		if dl.Before(now) {
			delete(ct.urls, host)
		}
	}
}

func (ct *CooldownTracker) setByDate(host string, t time.Time) {
	now := time.Now()
	latestDate := now.Add(ct.maxBackoff)
	if t.After(latestDate) {
		t = latestDate
	}
	ct.urlsLock.Lock()
	ct.urls[host] = t
	ct.sweepLocked(now)
	ct.urlsLock.Unlock()
}

func (ct *CooldownTracker) setByDuration(host string, d time.Duration) {
	if d > ct.maxBackoff {
		d = ct.maxBackoff
	}
	now := time.Now()
	ct.urlsLock.Lock()
	ct.urls[host] = now.Add(d)
	ct.sweepLocked(now)
	ct.urlsLock.Unlock()
}

// inCooldown returns the cooldown deadline for the host and whether that
// deadline is still in the future.
func (ct *CooldownTracker) inCooldown(host string) (time.Time, bool) {
	ct.urlsLock.RLock()
	dl, ok := ct.urls[host]
	ct.urlsLock.RUnlock()

	if !ok || !time.Now().Before(dl) {
		return time.Time{}, false
	}
	return dl, true
}

func (ct *CooldownTracker) remove(host string) {
	ct.urlsLock.Lock()
	delete(ct.urls, host)
	ct.urlsLock.Unlock()
}

func (ct *CooldownTracker) fillSenderURLs(urls []network.ParsedURL) []*senderURL {
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
