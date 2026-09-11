package decision

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-peertaskqueue/peertracker"
	"github.com/libp2p/go-libp2p/core/peer"
)

// peerPendingCap caps how much a peer's pending count drives its ordering.
// Without a cap, a peer with a large wantlist permanently outranks peers
// with smaller ones and starves them. See
// https://github.com/ipfs/boxo/issues/1141.
const peerPendingCap = 16

// peerStarvationTimeout caps how long a peer with queued tasks waits before
// the comparator promotes it above non-starved peers. The bound is soft:
// the heap re-evaluates ordering on push/pop, not on wall-clock time.
const peerStarvationTimeout = 10 * time.Second

// peerScheduler records the last time each peer received an envelope. The
// fair comparator reads this state to promote peers that have waited too
// long, or never received an envelope at all.
//
// lastServedAt keys peer.ID to *atomic.Int64 holding the last-served
// nanosecond timestamp. The access pattern is read-heavy (every comparator
// call hits isStarved, once per peer), write-rare (markServed runs once
// per emitted envelope), and the key set churns slowly. sync.Map carries
// the concurrent key lookup; *atomic.Int64 carries the concurrent value
// update without further allocation after the first write per peer.
type peerScheduler struct {
	lastServedAt sync.Map
	// tiebreakSeed randomizes the layer-5 peer.ID tiebreak once per process.
	// Ordering stays transitive within a run (heap invariants hold), but the
	// winner on otherwise-identical state flips across runs, so no peer can
	// craft an ID that permanently outranks everyone.
	tiebreakSeed maphash.Seed
}

func newPeerScheduler() *peerScheduler {
	return &peerScheduler{
		tiebreakSeed: maphash.MakeSeed(),
	}
}

// markServed records the time the engine selected this peer for an
// outbound envelope. The engine calls it on every emitted envelope.
func (s *peerScheduler) markServed(p peer.ID) {
	if s == nil {
		return
	}
	s.recordServedAt(p, time.Now())
}

// recordServedAt is the test-accessible form of markServed. Splitting lets
// tests install deterministic timestamps without bypassing the storage
// layout.
func (s *peerScheduler) recordServedAt(p peer.ID, t time.Time) {
	ns := t.UnixNano()
	if v, ok := s.lastServedAt.Load(p); ok {
		v.(*atomic.Int64).Store(ns)
		return
	}
	fresh := new(atomic.Int64)
	fresh.Store(ns)
	if actual, loaded := s.lastServedAt.LoadOrStore(p, fresh); loaded {
		actual.(*atomic.Int64).Store(ns)
	}
}

// forget drops per-peer state when a peer disconnects.
func (s *peerScheduler) forget(p peer.ID) {
	if s == nil {
		return
	}
	s.lastServedAt.Delete(p)
}

// isStarved reports whether the peer's wait exceeds peerStarvationTimeout.
// A never-served peer (no lastServedAt entry) counts as starved.
func (s *peerScheduler) isStarved(p peer.ID, now time.Time) bool {
	if s == nil {
		return false
	}
	v, ok := s.lastServedAt.Load(p)
	if !ok {
		return true
	}
	return now.UnixNano()-v.(*atomic.Int64).Load() > int64(peerStarvationTimeout)
}

// fairPeerComparator returns a peer comparator that keeps low-pending peers
// from starving behind a few heavy peers (issue #1141). Ordering layers,
// first difference wins:
//
//  1. Empty queue loses (matches upstream rule).
//  2. Starvation override: a never-served peer, or one waiting longer than
//     peerStarvationTimeout, outranks any non-starved peer. A new peer with
//     a single pending want gets its first envelope even while heavy peers
//     have queued work.
//  3. Lower activeWork wins, which keeps active peers busy.
//  4. On active ties, higher min(pending, peerPendingCap) wins. Pending
//     counts past the cap stop contributing, so 1000 pending does not
//     permanently outrank 17.
//  5. Per-process salted hash of peer.ID. Transitive within a run (heap
//     invariants hold), but unpredictable across runs, so no peer can mine
//     an ID that permanently outranks everyone.
func fairPeerComparator(sched *peerScheduler) peertracker.PeerComparator {
	return func(pa, pb *peertracker.PeerTracker) bool {
		paStats := pa.Stats()
		pbStats := pb.Stats()

		// Layer 1: empty queue.
		if paStats.NumPending == 0 {
			return false
		}
		if pbStats.NumPending == 0 {
			return true
		}

		// Layer 2: starvation override.
		now := time.Now()
		aStarved := sched.isStarved(pa.Target(), now)
		bStarved := sched.isStarved(pb.Target(), now)
		if aStarved != bStarved {
			return aStarved
		}

		// Layer 3: lower activeWork wins.
		if paStats.NumActive != pbStats.NumActive {
			return paStats.NumActive < pbStats.NumActive
		}

		// Layer 4: capped pending.
		aCapped := min(paStats.NumPending, peerPendingCap)
		bCapped := min(pbStats.NumPending, peerPendingCap)
		if aCapped != bCapped {
			return aCapped > bCapped
		}

		// Layer 5: salted-hash tiebreak.
		return maphash.String(sched.tiebreakSeed, string(pa.Target())) <
			maphash.String(sched.tiebreakSeed, string(pb.Target()))
	}
}
