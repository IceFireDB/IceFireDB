package entry

import (
	"context"
	"sync"
	"time"

	"berty.tech/go-ipfs-log/identityprovider"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/cbor"
	"github.com/ipfs/go-cid"
	coreiface "github.com/ipfs/kubo/core/coreiface"
	"golang.org/x/sync/semaphore"
)

type taskKind int

const (
	taskKindAdded = iota
	taskKindInProgress
	taskKindDone
)

func noopShouldExclude(_ cid.Cid) bool {
	return false
}

type Fetcher struct {
	length   int
	maxClock int // keep track of the latest clock time during load
	minClock int // keep track of the minimum clock time during load
	muClock  sync.Mutex

	timeout       time.Duration
	io            iface.IO
	provider      identityprovider.Interface
	shouldExclude iface.ExcludeFunc
	tasksCache    map[cid.Cid]taskKind
	condProcess   *sync.Cond
	muProcess     *sync.RWMutex
	sem           *semaphore.Weighted
	ipfs          coreiface.CoreAPI
	progressChan  chan iface.IPFSLogEntry
}

func NewFetcher(ipfs coreiface.CoreAPI, options *FetchOptions) *Fetcher {
	// set default
	length := -1
	if options.Length != nil {
		length = *options.Length
	}

	if options.Concurrency <= 0 {
		options.Concurrency = 32
	}

	if options.IO == nil {
		io, err := cbor.IO(&Entry{}, &LamportClock{})
		if err != nil {
			return nil
		}

		options.IO = io
	}

	if options.ShouldExclude == nil {
		options.ShouldExclude = noopShouldExclude
	}

	muProcess := sync.RWMutex{}

	// create Fetcher
	return &Fetcher{
		io:            options.IO,
		length:        length,
		timeout:       options.Timeout,
		shouldExclude: options.ShouldExclude,
		sem:           semaphore.NewWeighted(int64(options.Concurrency)),
		ipfs:          ipfs,
		progressChan:  options.ProgressChan,
		muProcess:     &muProcess,
		condProcess:   sync.NewCond(&muProcess),
		maxClock:      0,
		minClock:      0,
		tasksCache:    make(map[cid.Cid]taskKind),
	}
}

func (f *Fetcher) Fetch(ctx context.Context, hashes []cid.Cid) []iface.IPFSLogEntry {
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, f.timeout)
		defer cancel()
	}

	return f.processQueue(ctx, hashes)
}

func (f *Fetcher) processQueue(ctx context.Context, hashes []cid.Cid) []iface.IPFSLogEntry {
	results := []iface.IPFSLogEntry{}
	queue := newProcessQueue()

	f.muProcess.Lock()
	f.addHashesToQueue(queue, hashes...)
	taskInProgress := 0
	for queue.Len() > 0 {
		// acquire a process slot limited by concurrency limit
		if err := f.acquireProcessSlot(ctx); err != nil {
			// @FIXME(gfanton): log this
			// fmt.Printf("error while process next: %s\n", err.Error())
			break
		}

		// get next hash
		hash := queue.Next()
		f.tasksCache[hash] = taskKindInProgress

		// run process
		go func(hash cid.Cid) {
			entry, _ := f.fetchEntry(ctx, hash)
			// if err != nil {
			// @FIXME(gfanton): log this
			// fmt.Printf("unable to fetch entry: %s\n", err.Error())
			// }

			// free process slot
			f.processDone()

			f.muProcess.Lock()

			if entry != nil {
				entryHash := entry.GetHash()
				var lastEntry iface.IPFSLogEntry
				if len(results) > 0 {
					lastEntry = results[len(results)-1]
				}

				// update clock
				f.updateClock(ctx, entry, lastEntry)

				// if we don't know this hash yet, add it to result
				cache := f.tasksCache[entryHash]
				if cache == taskKindAdded || cache == taskKindInProgress {
					ts := entry.GetClock().GetTime()
					isLater := len(results) >= f.length && ts >= f.minClock
					if f.length < 0 || len(results) < f.length || isLater {
						results = append(results, entry)
						// signal progress
						if f.progressChan != nil {
							f.progressChan <- entry
						}
					}

					f.tasksCache[entryHash] = taskKindDone

					// add next elems to queue
					f.addNextEntry(ctx, queue, entry, results)
				}
			}

			// mark this process as done
			taskInProgress--

			// signal that a slot is available
			f.condProcess.Signal()

			f.muProcess.Unlock()
		}(hash)

		// increase in progress task counter
		taskInProgress++

		// wait until a task is added or that no running task is in progress
		for queue.Len() == 0 && taskInProgress > 0 {
			f.condProcess.Wait()
		}
	}

	// wait until all process are done/canceled
	for taskInProgress > 0 {
		f.condProcess.Wait()
	}

	f.muProcess.Unlock()

	return results
}

func (f *Fetcher) updateClock(_ context.Context, entry, lastEntry iface.IPFSLogEntry) {
	f.muClock.Lock()

	ts := entry.GetClock().GetTime()

	// Update min/max clocks
	if f.maxClock < ts {
		f.maxClock = ts
	}

	if lastEntry != nil {
		if ts := lastEntry.GetClock().GetTime(); ts < f.minClock {
			f.minClock = ts
		}
	} else {
		f.minClock = f.maxClock
	}

	f.muClock.Unlock()
}

func (f *Fetcher) exclude(hash cid.Cid) (yes bool) {
	if yes = !hash.Defined(); yes {
		return
	}

	// do we have it in the internal cache ?
	if _, yes = f.tasksCache[hash]; yes {
		return
	}

	// should the caller want it ?
	yes = f.shouldExclude(hash)
	return
}

func (f *Fetcher) addNextEntry(_ context.Context, queue processQueue, entry iface.IPFSLogEntry, results []iface.IPFSLogEntry) {
	ts := entry.GetClock().GetTime()

	if f.length < 0 {
		// If we're fetching all entries (length === -1), adds nexts and refs to the queue
		f.addHashesToQueue(queue, entry.GetNext()...)
		f.addHashesToQueue(queue, entry.GetRefs()...)
		return
	}

	// If we're fetching entries up to certain length,
	// fetch the next if result is filled up, to make sure we "check"
	// the next entry if its clock is later than what we have in the result
	if len(results) < f.length || ts > f.minClock || ts == f.minClock {
		for _, h := range entry.GetNext() {
			f.addHashToQueue(queue, f.maxClock-ts, h)
		}
	}
	if len(results)+len(entry.GetRefs()) <= f.length {
		for i, h := range entry.GetRefs() {
			f.addHashToQueue(queue, f.maxClock-ts+((i+1)*i), h)
		}
	}
}

func (f *Fetcher) fetchEntry(ctx context.Context, hash cid.Cid) (entry iface.IPFSLogEntry, err error) {
	// Load the entry
	return FromMultihashWithIO(ctx, f.ipfs, hash, f.provider, f.io)
}

func (f *Fetcher) addHashesToQueue(queue processQueue, hashes ...cid.Cid) (added int) {
	for i, h := range hashes {
		added += f.addHashToQueue(queue, i, h)
	}

	return
}

func (f *Fetcher) addHashToQueue(queue processQueue, index int, hash cid.Cid) (added int) {
	if f.exclude(hash) {
		return
	}

	queue.Add(index, hash)
	f.tasksCache[hash] = taskKindAdded

	return 1

}

func (f *Fetcher) acquireProcessSlot(ctx context.Context) error {
	return f.sem.Acquire(ctx, 1)
}

func (f *Fetcher) processDone() {
	// signal that a process slot is available
	f.sem.Release(1)
}
