// SPDX-License-Identifier: MIT

package bbloom

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"log"
	"math"
	"math/bits"
	"sync"
)

func getSize(ui64 uint64) (size uint64, exponent uint64) {
	if ui64 < uint64(512) {
		ui64 = uint64(512)
	}
	size = uint64(1)
	for size < ui64 {
		size <<= 1
		exponent++
	}
	return size, exponent
}

func calcSizeByWrongPositives(numEntries, wrongs float64) (uint64, uint64) {
	size := -1 * numEntries * math.Log(wrongs) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(float64(0.69314718056) * size / numEntries)
	return uint64(size), uint64(locs)
}

var (
	// ErrUsage is returned by [New] when it is not called with exactly two arguments.
	ErrUsage = errors.New("usage: New(float64(number_of_entries), float64(number_of_hashlocations)) i.e. New(float64(1000), float64(3)) or New(float64(number_of_entries), float64(ratio_of_false_positives)) i.e. New(float64(1000), float64(0.03))")

	// ErrInvalidParms is returned by [New] when a parameter is negative.
	ErrInvalidParms = errors.New("one of the parameters was outside of allowed range")
)

// New creates a bloom filter with default SipHash keys. It accepts exactly
// two float64 arguments:
//
//   - If the second parameter is < 1 it is treated as a false-positive rate,
//     and the filter is sized automatically.
//     Example: New(float64(1<<16), 0.01) -- 65536 items, 1% FP rate.
//
//   - If the second parameter is >= 1 it is treated as the number of hash
//     locations, and the first parameter is the bitset size.
//     Example: New(650000.0, 7.0) -- 650000-bit filter, 7 hash locations.
//
// The default SipHash keys are publicly known constants. If the filter will
// hold data controlled by untrusted parties, use [NewWithKeys] instead to
// prevent hash-flooding attacks.
func New(params ...float64) (bloomfilter *Bloom, err error) {
	var entries, locs uint64
	if len(params) == 2 {
		if params[0] < 0 || params[1] < 0 {
			return nil, ErrInvalidParms
		}
		if params[1] < 1 {
			entries, locs = calcSizeByWrongPositives(math.Max(params[0], 1), params[1])
		} else {
			entries, locs = uint64(params[0]), uint64(params[1])
		}
	} else {
		return nil, ErrUsage
	}
	size, exponent := getSize(uint64(entries))
	bloomfilter = &Bloom{
		sizeExp:     exponent,
		size:        size - 1,
		setLocs:     locs,
		shift:       64 - exponent,
		bitset:      make([]uint64, size>>6),
		k0:          defaultK0,
		k1:          defaultK1,
		hashVersion: 1,
	}
	return bloomfilter, nil
}

// NewWithKeys creates a bloom filter with caller-provided SipHash keys.
//
// The default keys used by [New] are publicly known constants baked into the
// source code. An attacker who knows the keys can craft inputs that all hash
// to the same bit positions, filling the filter faster than normal and raising
// the false-positive rate. This is a concern when the filter holds data
// chosen by untrusted parties (e.g. content-addressed blocks fetched from
// the network).
//
// Providing random, secret keys (e.g. generated once per node from
// crypto/rand) restores SipHash's anti-collision guarantees and makes such
// attacks infeasible.
//
// The params are interpreted the same way as in [New]. Custom keys are
// preserved across [Bloom.JSONMarshal] / [JSONUnmarshal] round-trips.
// Note: custom keys are included in plaintext in the [Bloom.JSONMarshal]
// output, so treat serialized filters accordingly.
func NewWithKeys(k0, k1 uint64, params ...float64) (*Bloom, error) {
	bf, err := New(params...)
	if err != nil {
		return nil, err
	}
	bf.k0 = k0
	bf.k1 = k1
	return bf, nil
}

// NewWithBoolset creates a bloom filter from a pre-existing bitset, typically
// obtained from a previous [Bloom.JSONMarshal] export or an external source.
// bs is the serialized bitset (big-endian uint64 words) and locs is the
// number of hash locations per entry. The filter uses default SipHash keys;
// use [NewWithBoolsetAndKeys] to restore a filter that was created with
// custom keys.
func NewWithBoolset(bs []byte, locs uint64) (bloomfilter *Bloom) {
	bloomfilter, err := New(float64(len(bs)<<3), float64(locs))
	if err != nil {
		panic(err) // Should never happen
	}
	for i := range bloomfilter.bitset {
		bloomfilter.bitset[i] = binary.BigEndian.Uint64((bs)[i<<3:])
	}
	return bloomfilter
}

// NewWithBoolsetAndKeys creates a bloom filter from a pre-existing bitset
// with caller-provided SipHash keys. This is the constructor to use when
// restoring a filter that was originally created with [NewWithKeys].
// See [NewWithKeys] for why custom keys matter and [NewWithBoolset] for
// how the bitset parameter is interpreted.
func NewWithBoolsetAndKeys(bs []byte, locs, k0, k1 uint64) (bloomfilter *Bloom) {
	bloomfilter = NewWithBoolset(bs, locs)
	bloomfilter.k0 = k0
	bloomfilter.k1 = k1
	return bloomfilter
}

// bloomJSONImExport
// Im/Export structure used by JSONMarshal / JSONUnmarshal
type bloomJSONImExport struct {
	FilterSet []byte
	SetLocs   uint64
	Version   uint8   `json:"Version,omitempty"`
	K0        *uint64 `json:"K0,omitempty"`
	K1        *uint64 `json:"K1,omitempty"`
}

// Bloom is a bloom filter backed by a power-of-two sized bitset.
// The Mtx field is exposed so callers can hold the lock across
// multiple operations when needed.
type Bloom struct {
	// Mtx is exposed so callers can hold the lock across multiple
	// operations (e.g. a Has followed by Add) without racing.
	Mtx     sync.RWMutex
	bitset  []uint64
	sizeExp uint64
	size    uint64
	setLocs uint64
	shift   uint64

	content     uint64
	k0, k1      uint64 // SipHash keys
	hashVersion uint8  // 0 = legacy, 1 = l|=1 fix (issue #11)
}

// ElementsAdded returns the number of elements added to the bloom filter.
func (bl *Bloom) ElementsAdded() uint64 {
	return bl.content
}

// <--- http://www.cse.yorku.ca/~oz/hash.html
// modified Berkeley DB Hash (32bit)
// hash is casted to l, h = 16bit fragments
// func (bl Bloom) absdbm(b *[]byte) (l, h uint64) {
// 	hash := uint64(len(*b))
// 	for _, c := range *b {
// 		hash = uint64(c) + (hash << 6) + (hash << bl.sizeExp) - hash
// 	}
// 	h = hash >> bl.shift
// 	l = hash << bl.shift >> bl.shift
// 	return l, h
// }

// Update: found sipHash of Jean-Philippe Aumasson & Daniel J. Bernstein to be even faster than absdbm()
// https://131002.net/siphash/
// siphash was implemented for Go by Dmitry Chestnykh https://github.com/dchest/siphash

// Add inserts entry into the bloom filter. Not safe for concurrent use;
// see [Bloom.AddTS] for a mutex-protected variant.
func (bl *Bloom) Add(entry []byte) {
	bl.content++
	l, h := bl.sipHash(entry)
	for i := uint64(0); i < (*bl).setLocs; i++ {
		bl.set((h + i*l) & (*bl).size)
	}
}

// AddTS is the thread-safe version of [Bloom.Add]. It acquires a write lock
// for the duration of the operation.
func (bl *Bloom) AddTS(entry []byte) {
	bl.Mtx.Lock()
	bl.Add(entry)
	bl.Mtx.Unlock()
}

// Has reports whether entry is in the filter. A true result may be a
// false positive; a false result is always definitive. Not safe for
// concurrent use; see [Bloom.HasTS].
func (bl *Bloom) Has(entry []byte) bool {
	l, h := bl.sipHash(entry)
	res := true
	for i := uint64(0); i < bl.setLocs; i++ {
		res = res && bl.isSet((h+i*l)&bl.size)
		// Branching here (early escape) is not worth it
		// This is my conclusion from benchmarks
		// (prevents loop unrolling)
		// if !res {
		//   return false
		// }
	}
	return res
}

// HasTS is the thread-safe version of [Bloom.Has]. It acquires a read lock
// for the duration of the operation.
func (bl *Bloom) HasTS(entry []byte) bool {
	bl.Mtx.RLock()
	has := bl.Has(entry[:])
	bl.Mtx.RUnlock()
	return has
}

// AddIfNotHas adds entry only if it is not already present in the filter.
// It returns true if the entry was added, false if it was already present.
// Not safe for concurrent use; see [Bloom.AddIfNotHasTS].
func (bl *Bloom) AddIfNotHas(entry []byte) (added bool) {
	l, h := bl.sipHash(entry)
	contained := true
	for i := uint64(0); i < bl.setLocs; i++ {
		prev := bl.getSet((h + i*l) & bl.size)
		contained = contained && prev
	}
	if !contained {
		bl.content++
	}
	return !contained
}

// AddIfNotHasTS is the thread-safe version of [Bloom.AddIfNotHas].
// It acquires a write lock for the duration of the operation.
func (bl *Bloom) AddIfNotHasTS(entry []byte) (added bool) {
	bl.Mtx.Lock()
	added = bl.AddIfNotHas(entry[:])
	bl.Mtx.Unlock()
	return added
}

// Clear resets the bloom filter, zeroing the bitset and the element counter.
// Not safe for concurrent use; see [Bloom.ClearTS].
func (bl *Bloom) Clear() {
	bs := bl.bitset // important performance optimization.
	for i := range bs {
		bs[i] = 0
	}
	bl.content = 0
}

// ClearTS is the thread-safe version of [Bloom.Clear].
func (bl *Bloom) ClearTS() {
	bl.Mtx.Lock()
	bl.Clear()
	bl.Mtx.Unlock()
}

func (bl *Bloom) set(idx uint64) {
	bl.bitset[idx>>6] |= 1 << (idx % 64)
}

func (bl *Bloom) getSet(idx uint64) bool {
	cur := bl.bitset[idx>>6]
	bit := uint64(1 << (idx % 64))
	bl.bitset[idx>>6] = cur | bit
	return (cur & bit) > 0
}

func (bl *Bloom) isSet(idx uint64) bool {
	return bl.bitset[idx>>6]&(1<<(idx%64)) > 0
}

func (bl *Bloom) marshal() bloomJSONImExport {
	bloomImEx := bloomJSONImExport{}
	bloomImEx.SetLocs = uint64(bl.setLocs)
	bloomImEx.Version = bl.hashVersion
	if bl.k0 != defaultK0 || bl.k1 != defaultK1 {
		k0, k1 := bl.k0, bl.k1
		bloomImEx.K0 = &k0
		bloomImEx.K1 = &k1
	}
	bloomImEx.FilterSet = make([]byte, len(bl.bitset)<<3)
	for i, w := range bl.bitset {
		binary.BigEndian.PutUint64(bloomImEx.FilterSet[i<<3:], w)
	}
	return bloomImEx
}

// JSONMarshal serializes the bloom filter to a JSON byte slice.
// The result can be restored with [JSONUnmarshal].
// Not safe for concurrent use; see [Bloom.JSONMarshalTS].
func (bl *Bloom) JSONMarshal() []byte {
	data, err := json.Marshal(bl.marshal())
	if err != nil {
		log.Fatal("json.Marshal failed: ", err)
	}
	return data
}

// JSONMarshalTS is the thread-safe version of [Bloom.JSONMarshal].
func (bl *Bloom) JSONMarshalTS() []byte {
	bl.Mtx.RLock()
	export := bl.marshal()
	bl.Mtx.RUnlock()
	data, err := json.Marshal(export)
	if err != nil {
		log.Fatal("json.Marshal failed: ", err)
	}
	return data
}

// JSONUnmarshal restores a bloom filter from a JSON byte slice produced by
// [Bloom.JSONMarshal] or [Bloom.JSONMarshalTS].
func JSONUnmarshal(dbData []byte) (*Bloom, error) {
	bloomImEx := bloomJSONImExport{}
	err := json.Unmarshal(dbData, &bloomImEx)
	if err != nil {
		return nil, err
	}
	if (bloomImEx.K0 == nil) != (bloomImEx.K1 == nil) {
		return nil, errors.New("both K0 and K1 must be present or both absent")
	}
	var bf *Bloom
	if bloomImEx.K0 != nil && bloomImEx.K1 != nil {
		bf = NewWithBoolsetAndKeys(bloomImEx.FilterSet, bloomImEx.SetLocs, *bloomImEx.K0, *bloomImEx.K1)
	} else {
		bf = NewWithBoolset(bloomImEx.FilterSet, bloomImEx.SetLocs)
	}
	bf.hashVersion = bloomImEx.Version
	return bf, nil
}

// FillRatio returns the fraction of bits set in the filter (0.0 to 1.0).
// Not safe for concurrent use; see [Bloom.FillRatioTS].
func (bl *Bloom) FillRatio() float64 {
	count := uint64(0)
	for _, b := range bl.bitset {
		count += uint64(bits.OnesCount64(b))
	}
	return float64(count) / float64(bl.size+1)
}

// FillRatioTS is the thread-safe version of [Bloom.FillRatio].
func (bl *Bloom) FillRatioTS() float64 {
	bl.Mtx.RLock()
	fr := bl.FillRatio()
	bl.Mtx.RUnlock()
	return fr
}

// // alternative hashFn
// func (bl Bloom) fnv64a(b *[]byte) (l, h uint64) {
// 	h64 := fnv.New64a()
// 	h64.Write(*b)
// 	hash := h64.Sum64()
// 	h = hash >> 32
// 	l = hash << 32 >> 32
// 	return l, h
// }
//
// // <-- http://partow.net/programming/hashfunctions/index.html
// // citation: An algorithm proposed by Donald E. Knuth in The Art Of Computer Programming Volume 3,
// // under the topic of sorting and search chapter 6.4.
// // modified to fit with boolset-length
// func (bl Bloom) DEKHash(b *[]byte) (l, h uint64) {
// 	hash := uint64(len(*b))
// 	for _, c := range *b {
// 		hash = ((hash << 5) ^ (hash >> bl.shift)) ^ uint64(c)
// 	}
// 	h = hash >> bl.shift
// 	l = hash << bl.sizeExp >> bl.sizeExp
// 	return l, h
// }
