# Benchmarks

All filters configured for 65536 items at 1% false-positive rate.

## 2026

Comparison against [`bits-and-blooms/bloom/v3`](https://github.com/bits-and-blooms/bloom)
and [`greatroar/blobloom`](https://github.com/greatroar/blobloom).

Run on Go 1.25, AMD Ryzen AI MAX+ 395, Linux amd64 (March 2026):

```
go test -bench='BenchmarkPerOp' -benchmem -count=5 -benchtime=2s
```

### Speed (ns/op, lower is better)

| Operation       | ipfs/bbloom | bits-and-blooms/bloom/v3 | greatroar/blobloom |
| --------------- | ----------: | -----------------------: | -----------------: |
| Add             |       ~20.5 |                    ~39.3 |              ~4.95 |
| Has/Test        |       ~20.0 |                    ~36.1 |              ~4.4  |
| AddTS (locked)  |       ~32.6 |                      n/a |                n/a |
| HasTS (rlocked) |       ~23.7 |                      n/a |                n/a |

**Fastest**: blobloom, but it accepts a pre-hashed `uint64`, so its numbers
do not include hashing time. Comparing only libraries that hash from `[]byte`
internally, **bbloom is ~2x faster** than bits-and-blooms.

### Memory (total allocations for 65536 inserts)

| Library                  |   Alloc |
| ------------------------ | ------: |
| ipfs/bbloom              | 133 KiB |
| bits-and-blooms/bloom/v3 |  80 KiB |
| greatroar/blobloom       |  88 KiB |

**Smallest**: bits-and-blooms uses the least memory.
bbloom rounds the bitset up to the next power of two, which uses ~1.7x more
memory but produces a much lower false-positive rate as a side effect.

### False-positive rate (1M lookups of absent keys)

| Library                  | Measured | Target |
| ------------------------ | -------: | -----: |
| ipfs/bbloom              |   0.07%  |  1.00% |
| bits-and-blooms/bloom/v3 |   1.00%  |  1.00% |
| greatroar/blobloom       |   0.65%  |  1.00% |

**Best accuracy**: bbloom at 0.07% -- 14x below the 1% target.
This is a direct consequence of the power-of-two bitset rounding: more bits
means fewer collisions.

### Summary

| Aspect    | Winner                                                           |
| --------- | ---------------------------------------------------------------- |
| Speed     | blobloom (pre-hashed); bbloom (hashing from `[]byte`)            |
| Memory    | bits-and-blooms                                                  |
| Accuracy  | bbloom (0.07% vs 1% target)                                     |
| Built-in thread safety | bbloom (`AddTS`/`HasTS` with mutex)                |

## 2013

Original benchmarks by Andreas Briese on a MacBook Pro 15" (OSX 10.8.5, i7 4-core 2.4 GHz).
Bloom filter size 524288 bits, 7 hash locations, 65536 items, 10 repetitions each:

```
github.com/AndreasBriese/bbloom    Add 65536 items:  6595800 ns (100 ns/op)
github.com/AndreasBriese/bbloom    Has 65536 items:  5986600 ns  (91 ns/op)
github.com/AndreasBriese/bloom     Add 65536 items:  6304684 ns  (96 ns/op)
github.com/AndreasBriese/bloom     Has 65536 items:  6568663 ns (100 ns/op)

github.com/willf/bloom             Add 65536 items: 24367224 ns (371 ns/op)
github.com/willf/bloom            Test 65536 items: 21881142 ns (333 ns/op)
github.com/dataence/bloom/standard Add 65536 items: 23041644 ns (351 ns/op)
github.com/dataence/bloom/standard Check 65536 items: 19153133 ns (292 ns/op)
github.com/cabello/bloom           Add 65536 items: 131921507 ns (2012 ns/op)
github.com/cabello/bloom      Contains 65536 items: 131108962 ns (2000 ns/op)
```

Note: `willf/bloom` has since been renamed to `bits-and-blooms/bloom` and has
improved significantly (see 2026 results above: 371 ns/op -> ~39 ns/op).
