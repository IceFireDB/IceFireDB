package reservedpool

import (
	"errors"
	"maps"
	"sync"
)

var (
	ErrClosed        = errors.New("reservedpool: closed")
	ErrUnsatisfiable = errors.New("reservedpool: unsatisfiable request")
)

type Pool[K comparable] struct {
	available    int
	reserve      map[K]int // per-category reserve
	used         map[K]int // currently held per category
	queued       map[K]int // currently queued per category
	mu           sync.Mutex
	cond         *sync.Cond
	reservedOnly bool // if true, only categories with reserves can be used
	closed       bool
}

// New returns a pool with global limit max and the given reserves.
// The sum of reserves must be ≤ max.
func New[K comparable](max int, reserves map[K]int) *Pool[K] {
	p := &Pool[K]{
		available: max,
		reserve:   make(map[K]int, len(reserves)),
		used:      make(map[K]int),
		queued:    make(map[K]int),
	}
	sum := 0
	for i, r := range reserves {
		p.reserve[i] = r
		sum += r
	}
	if sum == max {
		// Reserves use all available slots
		p.reservedOnly = true
	} else if sum > max {
		panic("sum(reserves) > max")
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Acquire blocks until the caller may consume one slot for the given category.
// It returns ctx.Err() if the context is cancelled while waiting.
func (p *Pool[K]) Acquire(cat K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.reservedOnly && p.reserve[cat] == 0 {
		return ErrUnsatisfiable
	}

	waiting := false
	defer func() {
		if waiting {
			if p.queued[cat]--; p.queued[cat] == 0 {
				delete(p.queued, cat)
			}
		}
	}()

	for {
		if p.closed {
			return ErrClosed
		}
		if p.canUse(cat) {
			p.used[cat]++
			p.available--
			return nil
		}
		if !waiting {
			p.queued[cat]++
			waiting = true
		}

		p.cond.Wait() // releases p.mu while blocked
	}
}

// Release frees one slot previously acquired by cat.
func (p *Pool[K]) Release(cat K) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// after Close(), extra releases are ignored
		return
	}
	if p.used[cat] == 0 {
		panic("reservedpool: release with zero usage")
	}
	p.used[cat]--
	p.available++
	p.cond.Broadcast()
}

// Close marks the pool closed and wakes all waiters.
// Further Acquire calls return ErrClosed. Releases are ignored.
func (p *Pool[K]) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	return nil
}

// canUse implements the rule described above (called with p.mu held).
func (p *Pool[K]) canUse(cat K) bool {
	if p.available == 0 {
		return false
	}
	reservedForOthers := 0
	for k, rsv := range p.reserve {
		if k == cat {
			continue
		}
		if deficit := rsv - p.used[k]; deficit > 0 {
			reservedForOthers += deficit
		}
	}
	return p.available > reservedForOthers
}

// Stats represents the current state and capacity information of a reserved
// pool. It provides a snapshot of pool utilization across different worker
// types.
type Stats[K comparable] struct {
	Max      int       // maximum capacity of the pool
	Reserves map[K]int // reserved capacity per key
	Used     map[K]int // currently used capacity per key
	Queued   map[K]int // queued requests per key waiting for capacity
}

// Stats returns a snapshot of the current pool statistics.
func (p *Pool[K]) Stats() Stats[K] {
	p.mu.Lock()
	defer p.mu.Unlock()
	maximum := p.available
	for _, r := range p.used {
		maximum += r
	}
	return Stats[K]{
		Max:      maximum,
		Reserves: maps.Clone(p.reserve),
		Used:     maps.Clone(p.used),
		Queued:   maps.Clone(p.queued),
	}
}
