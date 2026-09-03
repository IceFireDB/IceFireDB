package walker

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	bbloom "github.com/ipfs/bbloom"
	cid "github.com/ipfs/go-cid"
)

// Default bloom filter parameters.
//
// See [NewBloomTracker] for creating a tracker with a specific FP rate.
const (
	// DefaultBloomFPRate is the target false positive rate, expressed as
	// 1/N (one false positive per N lookups). At the default value of
	// ~1 in 4.75 million (~0.00002%), each CID costs ~4 bytes (32 bits)
	// before ipfs/bbloom's power-of-two rounding.
	//
	// This is low enough for most IPFS deployments. IPFS content
	// typically has multiple providers, so a single node's false
	// positive has no impact on content availability. Any CID skipped
	// by a false positive is caught in the next reprovide cycle.
	//
	// Actual memory depends on how [BloomTracker] chains blooms; see
	// the scaling table in its documentation. As a rough guide, a
	// single bloom sized for N items uses N*32 bits rounded up to the
	// next power of two (e.g. 2M CIDs -> ~8 MB, 10M CIDs -> ~64 MB).
	//
	// Lowering this value (e.g. 1_000_000) uses less memory per CID
	// but skips more CIDs. Raising it (e.g. 10_000_000) uses more
	// memory but skips fewer.
	DefaultBloomFPRate = 4_750_000

	// DefaultBloomInitialCapacity is the number of expected items for
	// the first bloom filter when no persisted count from a previous
	// cycle exists. 2M items produces an ~8 MB bloom at the default FP
	// rate, covering repos up to ~2M CIDs without chain growth.
	DefaultBloomInitialCapacity = 2_000_000

	// BloomGrowthMargin is multiplied with the persisted CID count from
	// the previous reprovide cycle to size the initial bloom. The 1.5x
	// margin provides headroom for repo growth between cycles so that
	// a stable repo does not trigger chain growth on every cycle.
	BloomGrowthMargin = 1.5

	// BloomGrowthFactor determines how much larger each new bloom in the
	// chain is compared to the previous one. 4x keeps the chain short
	// (fewer blooms = less Has() overhead) while converging quickly to
	// the actual repo size.
	BloomGrowthFactor = 4

	// MinBloomCapacity is the smallest expectedItems value accepted by
	// [NewBloomTracker]. ipfs/bbloom derives k probe positions from a
	// single SipHash via double hashing (h + i*l mod size). For small
	// bitsets the stride patterns overlap, pushing the actual FP rate
	// far above the designed target. Empirically, at capacity=1000
	// (32K-bit bitset) with k=22 the FP rate is ~50x worse than
	// theory; at 10000 (512K bits) it matches. 10000 uses ~64 KB of
	// memory while ensuring the actual FP rate matches the design.
	MinBloomCapacity = 10_000
)

// BloomParams derives ipfs/bbloom parameters (bits per element, hash
// location count) from a target false positive rate expressed as 1/N.
//
// The number of hash functions k is derived as round(log2(N)), and
// bits per element as k / ln(2). Because k must be a positive integer,
// not every FP rate is exactly achievable -- the actual rate will be
// equal to or better than the target. Additionally, ipfs/bbloom rounds
// the total bitset to the next power of two, which further improves
// the actual rate.
func BloomParams(fpRate uint) (bitsPerElem uint, hashLocs uint) {
	k := math.Round(math.Log2(float64(fpRate)))
	if k < 1 {
		k = 1
	}
	bpe := k / math.Ln2
	return uint(math.Ceil(bpe)), uint(k)
}

// VisitedTracker tracks which CIDs have been seen during DAG traversal.
// Implementations use c.Hash() (multihash bytes) as the key, so CIDv0
// and CIDv1 of the same content are treated as the same entry.
//
// Implementations may be exact ([MapTracker]) or probabilistic
// ([BloomTracker]). Probabilistic implementations must keep the false
// positive rate negligible for the expected dataset size, or allow
// callers to adjust it (see [NewBloomTracker]).
//
// NOT safe for concurrent use. The provide pipeline runs on a single
// goroutine per reprovide cycle. Adding parallelism requires switching
// to thread-safe variants (bbloom AddTS/HasTS) or external
// synchronization.
type VisitedTracker interface {
	// Visit marks a CID as visited. Returns true if it was NOT
	// previously visited (first visit).
	Visit(c cid.Cid) bool
	// Has returns true if the CID was previously visited.
	Has(c cid.Cid) bool
}

var (
	_ VisitedTracker = (*cid.Set)(nil)
	_ VisitedTracker = (*BloomTracker)(nil)
	_ VisitedTracker = (*MapTracker)(nil)
)

// MapTracker tracks visited CIDs using an in-memory map. Zero false
// positives. Useful for tests and small datasets.
//
// NOT safe for concurrent use.
type MapTracker struct {
	set          map[string]struct{}
	deduplicated uint64
}

// NewMapTracker creates a new map-based visited tracker.
func NewMapTracker() *MapTracker {
	return &MapTracker{set: make(map[string]struct{})}
}

func (m *MapTracker) Visit(c cid.Cid) bool {
	key := string(c.Hash())
	if _, ok := m.set[key]; ok {
		m.deduplicated++
		return false
	}
	m.set[key] = struct{}{}
	return true
}

func (m *MapTracker) Has(c cid.Cid) bool {
	_, ok := m.set[string(c.Hash())]
	return ok
}

// Deduplicated returns the number of Visit calls that returned false
// (CID already seen). Useful for logging how much dedup occurred.
func (m *MapTracker) Deduplicated() uint64 { return m.deduplicated }

// BloomTracker tracks visited CIDs using a chain of bloom filters that
// grows automatically when the current filter becomes saturated.
//
// # Why it exists
//
// When the reprovide system walks pinned DAGs, many pins share the same
// sub-DAGs (e.g. append-only datasets where each version differs by a
// small delta). Without deduplication, the walker re-traverses every
// shared subtree for each pin -- O(pins * total_blocks) I/O. The bloom
// filter lets the walker skip already-visited subtrees in O(1),
// reducing work to O(unique_blocks).
//
// A single fixed-size bloom filter requires knowing the number of CIDs
// upfront. On the very first cycle (or after significant repo growth)
// this count is unknown. BloomTracker solves this by starting with a
// small bloom and automatically appending larger ones when the insert
// count reaches the current filter's designed capacity.
//
// # How it works
//
// BloomTracker maintains an ordered chain of bloom filters [b0, b1, ...].
// Each filter's parameters (bits per element, hash count) are derived
// from the target false positive rate via [BloomParams].
//
//   - Has(c) checks ALL filters in the chain. If any filter reports the
//     CID as present, it returns true. False positives are independent
//     across filters because each uses unique random SipHash keys via
//     [bbloom.NewWithKeys] (generated from crypto/rand). This also means
//     different processes in a cluster hit different false positives, so
//     a CID skipped by one node is still provided by others.
//   - Visit(c) checks all filters first (like Has). If the CID is not
//     found, it adds it to the latest filter and increments the insert
//     counter. When inserts exceed the current filter's capacity, a new
//     filter at BloomGrowthFactor times the capacity is appended.
//     Saturation is detected via a simple integer comparison on every
//     insert (O(1)).
//
// # Scaling behavior (at default FP rate)
//
// With DefaultBloomInitialCapacity = 2M and BloomGrowthFactor = 4x.
// Memory includes ipfs/bbloom's power-of-two rounding of each bitset.
//
//	  2M CIDs:   1 bloom  (~8 MB)
//	 10M CIDs:   2 blooms (~42 MB)
//	 40M CIDs:   3 blooms (~176 MB)
//	100M CIDs:   4 blooms (~713 MB)
//
// On subsequent cycles, the persisted count from the previous cycle
// sizes the initial bloom correctly (with BloomGrowthMargin headroom),
// so the chain typically stays at 1 bloom.
//
// # Concurrency
//
// NOT safe for concurrent use. See [VisitedTracker] for the
// single-goroutine invariant.
type BloomTracker struct {
	chain        []*bbloom.Bloom // oldest to newest
	lastCap      uint64          // designed capacity of the latest bloom
	curInserts   uint64          // inserts into current (latest) bloom
	totalInserts uint64          // inserts across all blooms in chain
	deduplicated uint64          // Visit calls that returned false
	bitsPerElem  uint            // bits per element (derived from FP rate)
	hashLocs     uint            // hash function count (derived from FP rate)
}

// NewBloomTracker creates a bloom filter tracker sized for expectedItems
// at the given false positive rate (expressed as 1/N via fpRate).
//
// The bloom parameters (bits per element, hash count) are derived from
// fpRate via [BloomParams]. Because the hash count must be a positive
// integer, the actual FP rate may be slightly better than the target.
// ipfs/bbloom also rounds the bitset to the next power of two, further
// improving the actual rate.
//
// Returns an error if expectedItems is below [MinBloomCapacity], or
// fpRate is zero.
//
// When inserts exceed the current filter's capacity, a new filter at
// BloomGrowthFactor times the capacity is appended automatically.
//
// NOT safe for concurrent use. See [VisitedTracker] for the
// single-goroutine invariant.
func NewBloomTracker(expectedItems uint, fpRate uint) (*BloomTracker, error) {
	if expectedItems < MinBloomCapacity {
		return nil, fmt.Errorf("bloom tracker: expectedItems must be >= %d (got %d); "+
			"small blooms cause FP rates far above the design target "+
			"because ipfs/bbloom's double-hashing needs a large bitset",
			MinBloomCapacity, expectedItems)
	}
	if fpRate == 0 {
		return nil, errors.New("bloom tracker: fpRate must be > 0")
	}
	bpe, hlocs := BloomParams(fpRate)
	b, err := newBloom(uint64(expectedItems), bpe, hlocs)
	if err != nil {
		return nil, fmt.Errorf("bloom tracker: %w", err)
	}
	log.Infow("bloom tracker created",
		"capacity", expectedItems,
		"fpRate", fmt.Sprintf("1 in %d (~%.6f%%)", fpRate, 100.0/float64(fpRate)),
		"bitsPerElem", bpe,
		"hashFunctions", hlocs)
	return &BloomTracker{
		chain:       []*bbloom.Bloom{b},
		lastCap:     uint64(expectedItems),
		bitsPerElem: bpe,
		hashLocs:    hlocs,
	}, nil
}

func (bt *BloomTracker) Has(c cid.Cid) bool {
	key := []byte(c.Hash())
	// Iterate oldest to newest: frequently-repeated CIDs (e.g. shared
	// sub-DAGs across many pins) land in the earliest filter, so
	// checking old-first finds them with fewer probes. The alternative
	// (newest-first) would help if duplicates cluster near each other
	// in traversal order, but real DAG walks revisit globally popular
	// subtrees more often than recent ones.
	for _, b := range bt.chain {
		if b.Has(key) {
			return true
		}
	}
	return false
}

func (bt *BloomTracker) Visit(c cid.Cid) bool {
	key := []byte(c.Hash())

	// Check earlier blooms for the CID (oldest to newest, same
	// rationale as Has). If any reports it as present (true positive
	// from a prior growth epoch, or rare cross-bloom false positive),
	// skip it.
	earlier := bt.chain[:len(bt.chain)-1]
	for _, b := range earlier {
		if b.Has(key) {
			bt.deduplicated++
			return false
		}
	}

	// Use AddIfNotHas on the current bloom: it atomically hashes,
	// checks, and sets the bits in a single pass. This avoids the
	// false-positive window that exists when Has() and Add() are
	// called separately (a genuinely new CID could match already-set
	// bits from other inserts, causing Has to return true and the CID
	// to be silently skipped).
	cur := bt.chain[len(bt.chain)-1]
	if !cur.AddIfNotHas(key) {
		bt.deduplicated++
		return false
	}
	bt.curInserts++
	bt.totalInserts++
	if bt.curInserts > bt.lastCap {
		bt.grow()
	}
	return true
}

// Count returns the total number of unique CIDs added across all blooms.
// Used to persist the cycle count for sizing the next cycle's bloom.
func (bt *BloomTracker) Count() uint64 { return bt.totalInserts }

// Deduplicated returns the number of Visit calls that returned false
// (CID already seen or bloom false positive). Useful for logging how
// much dedup occurred in a reprovide cycle.
func (bt *BloomTracker) Deduplicated() uint64 { return bt.deduplicated }

// grow appends a new bloom filter to the chain at BloomGrowthFactor
// times the previous capacity with fresh random SipHash keys.
//
// The grown capacity is always >= BloomGrowthFactor * MinBloomCapacity
// because NewBloomTracker enforces expectedItems >= MinBloomCapacity,
// so the double-hashing FP rate issue with small bitsets cannot occur.
func (bt *BloomTracker) grow() {
	newCap := bt.lastCap * BloomGrowthFactor
	b, err := newBloom(newCap, bt.bitsPerElem, bt.hashLocs)
	if err != nil {
		// bitsPerElem and hashLocs are validated at construction time,
		// so this is unreachable unless something is deeply wrong.
		panic(fmt.Sprintf("bloom grow: %v", err))
	}
	log.Infow("bloom tracker autoscaled",
		"prevCapacity", bt.lastCap,
		"newCapacity", newCap,
		"totalInserts", bt.totalInserts,
		"chainLength", len(bt.chain)+1)
	bt.chain = append(bt.chain, b)
	bt.lastCap = newCap
	bt.curInserts = 0
}

// newBloom creates a single bbloom filter with random SipHash keys.
func newBloom(capacity uint64, bitsPerElem, hashLocs uint) (*bbloom.Bloom, error) {
	k0, k1 := randomSipHashKeys()
	return bbloom.NewWithKeys(k0, k1,
		float64(capacity*uint64(bitsPerElem)),
		float64(hashLocs))
}

// randomSipHashKeys generates fresh random SipHash keys via crypto/rand.
// Every bloom instance (across chain growth AND across process restarts)
// gets unique keys. This ensures:
//   - false positives are uncorrelated across blooms in the chain
//   - in a cluster running multiple kubo instances, each node's bloom
//     hits different false positives, so a CID skipped by one node is
//     still provided by others
func randomSipHashKeys() (uint64, uint64) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("bloom: crypto/rand failed: %v", err))
	}
	return binary.LittleEndian.Uint64(buf[:8]),
		binary.LittleEndian.Uint64(buf[8:])
}
