// Package guts provides a low-level interface to the BLAKE3 cryptographic hash
// function.
package guts

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// Various constants.
const (
	FlagChunkStart = 1 << iota
	FlagChunkEnd
	FlagParent
	FlagRoot
	FlagKeyedHash
	FlagDeriveKeyContext
	FlagDeriveKeyMaterial

	BlockSize = 64
	ChunkSize = 1024

	MaxSIMD = 16 // AVX-512 vectors can store 16 words
)

// IV is the BLAKE3 initialization vector.
var IV = [8]uint32{
	0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
	0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
}

// A Node represents a chunk or parent in the BLAKE3 Merkle tree.
type Node struct {
	CV       [8]uint32 // chaining value from previous node
	Block    [16]uint32
	Counter  uint64
	BlockLen uint32
	Flags    uint32
}

// ParentNode returns a Node that incorporates the chaining values of two child
// nodes.
func ParentNode(left, right [8]uint32, key *[8]uint32, flags uint32) Node {
	n := Node{
		CV:       *key,
		Counter:  0,         // counter is reset for parents
		BlockLen: BlockSize, // block is full
		Flags:    flags | FlagParent,
	}
	copy(n.Block[:8], left[:])
	copy(n.Block[8:], right[:])
	return n
}

// Eigentrees appends to trees the sequence of eigentree heights that increment
// counter to counter+chunks.
func Eigentrees(trees []int, counter uint64, chunks uint64) []int {
	for i := counter; i < counter+chunks; {
		bite := min(bits.TrailingZeros64(i), bits.Len64(counter+chunks-i)-1)
		trees = append(trees, bite)
		i += 1 << bite
	}
	return trees
}

var bufPool = sync.Pool{
	New: func() any { return new([MaxSIMD * ChunkSize]byte) },
}

// CompressSlice compresses up to MaxSIMD chunks in parallel and returns their
// root node. It is like CompressBuffer, but accepts a buffer of any size.
func CompressSlice(buf []byte, key *[8]uint32, counter uint64, flags uint32) Node {
	if len(buf) <= ChunkSize {
		return CompressChunk(buf, key, counter, flags)
	} else if cap(buf) >= MaxSIMD*ChunkSize {
		return CompressBuffer((*[MaxSIMD * ChunkSize]byte)(buf[:MaxSIMD*ChunkSize]), len(buf), key, counter, flags)
	}
	// CompressBuffer may read (and ignore) the bytes beyond buflen, so copy
	// buf into a full-size buffer.
	full := bufPool.Get().(*[MaxSIMD * ChunkSize]byte)
	defer bufPool.Put(full)
	return CompressBuffer(full, copy(full[:], buf), key, counter, flags)
}

// minBufsPerWorker is the fewest buffers worth spawning a goroutine for.
const minBufsPerWorker = 2

// CompressBuffers compresses len(cvs) full buffers in parallel, storing their
// chaining values in cvs.
func CompressBuffers(cvs [][8]uint32, buf []byte, key *[8]uint32, counter uint64, flags uint32) {
	compress := func(i int) {
		b := (*[MaxSIMD * ChunkSize]byte)(buf[i*MaxSIMD*ChunkSize:])
		cvs[i] = ChainingValue(CompressBuffer(b, len(b), key, counter+uint64(i)*MaxSIMD, flags))
	}
	workers := min(len(cvs)/minBufsPerWorker, runtime.GOMAXPROCS(0))
	if workers <= 1 {
		for i := range cvs {
			compress(i)
		}
		return
	}
	compressBuffersParallel(len(cvs), workers, compress)
}

func compressBuffersParallel(numBufs, workers int, compress func(i int)) {
	// NOTE: the workers claim one buffer at a time, rather than each taking
	// an equal share, since some cores may be much slower than others.
	var next atomic.Int64
	work := func() {
		for i := int(next.Add(1)) - 1; i < numBufs; i = int(next.Add(1)) - 1 {
			compress(i)
		}
	}
	var wg sync.WaitGroup
	for range workers - 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// NOTE: a goroutine can take a while to start. If it only gets
			// going as the work is about to run out, leave the rest to the
			// caller, rather than making the caller wait for us.
			if numBufs-int(next.Load()) < 2 {
				return
			}
			work()
		}()
	}
	work()
	wg.Wait()
}

// MergeEigentree merges the chaining values of 2^n buffers, returning the
// root node of their eigentree. It overwrites cvs.
func MergeEigentree(cvs [][8]uint32, key *[8]uint32, flags uint32) Node {
	if len(cvs) == 2 {
		return ParentNode(cvs[0], cvs[1], key, flags)
	} else if len(cvs) == MaxSIMD {
		return mergeSubtrees((*[MaxSIMD][8]uint32)(cvs), MaxSIMD, key, flags)
	}
	l := ChainingValue(MergeEigentree(cvs[:len(cvs)/2], key, flags))
	r := ChainingValue(MergeEigentree(cvs[len(cvs)/2:], key, flags))
	return ParentNode(l, r, key, flags)
}

// CompressEigentree compresses a buffer of 2^n chunks in parallel, returning
// their root node.
func CompressEigentree(buf []byte, key *[8]uint32, counter uint64, flags uint32) Node {
	if numChunks := uint64(len(buf) / ChunkSize); bits.OnesCount64(numChunks) != 1 {
		panic("non-power-of-two eigentree size")
	} else if numChunks <= MaxSIMD {
		return CompressSlice(buf, key, counter, flags)
	} else {
		cvs := make([][8]uint32, numChunks/MaxSIMD)
		CompressBuffers(cvs, buf, key, counter, flags)
		return MergeEigentree(cvs, key, flags)
	}
}
