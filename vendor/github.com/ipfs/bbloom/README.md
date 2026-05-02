# bbloom

[![GoDoc](https://pkg.go.dev/badge/github.com/ipfs/bbloom.svg)](https://pkg.go.dev/github.com/ipfs/bbloom)
[![codecov](https://codecov.io/gh/ipfs/bbloom/branch/master/graph/badge.svg)](https://codecov.io/gh/ipfs/bbloom)

A fast bloom filter with a real bitset, JSON serialization, and thread-safe variants.

## Why this fork

Forked from [`AndreasBriese/bbloom`](https://github.com/AndreasBriese/bbloom) in 2019 after the upstream became unmaintained. The fork fixes safety and correctness issues, and adds features needed for production use:

- Caller-provided SipHash keys (`NewWithKeys`) to prevent hash-flooding with untrusted input
- Fixed double-hash step to always be odd, avoiding degenerate probe sequences
- SipHash keys preserved across JSON serialization round-trips
- Proper error handling in deserialization

The library may contain IPFS-specific optimizations but works as a general-purpose bloom filter.

## Usage

```go
// create a bloom filter for 65536 items and 0.1% false-positive rate
bf, _ := bbloom.New(float64(1<<16), float64(0.001))

// or specify size and hash locations explicitly
// bf, _ = bbloom.New(650000.0, 7.0)

// add an item
bf.Add([]byte("butter"))

// check membership
bf.Has([]byte("butter"))    // true
bf.Has([]byte("Butter"))    // false

// add only if not already present
bf.AddIfNotHas([]byte("butter"))  // false (already in set)
bf.AddIfNotHas([]byte("buTTer"))  // true  (new entry)

// thread-safe variants: AddTS, HasTS, AddIfNotHasTS
bf.AddTS([]byte("peanutbutter"))
bf.HasTS([]byte("peanutbutter"))  // true

// JSON serialization
data := bf.JSONMarshal()
restored, _ := bbloom.JSONUnmarshal(data)
restored.Has([]byte("butter"))    // true
```

## Used in IPFS

[Kubo](https://github.com/ipfs/kubo) and [Boxo](https://github.com/ipfs/boxo) use this library where CID deduplication or tracking is needed but the number of CIDs is too large to keep in memory as a map. Two main use cases:

- **Blockstore bloom cache**: answers `Has()` checks without hitting the datastore, filtering out the majority of negative lookups.
- **DAG walker dedup**: tracks visited CIDs during DAG traversal in the provider/reprovide system, keeping memory usage proportional to the bloom filter size rather than the number of blocks walked.

## Benchmarks

See [BENCHMARKS.md](BENCHMARKS.md) for comparison against other bloom filter libraries.

## License

MIT (bbloom) and CC0 (inlined SipHash). See [LICENSE](LICENSE).
