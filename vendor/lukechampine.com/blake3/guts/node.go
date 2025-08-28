// Package guts provides a low-level interface to the BLAKE3 cryptographic hash
// function.
package guts

import (
	"math/bits"
	"sync"
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

// Eigentrees returns the sequence of eigentree heights that increment counter
// to counter+chunks.
func Eigentrees(counter uint64, chunks uint64) (trees []int) {
	for i := counter; i < counter+chunks; {
		bite := min(bits.TrailingZeros64(i), bits.Len64(counter+chunks-i)-1)
		trees = append(trees, bite)
		i += 1 << bite
	}
	return
}

// CompressEigentree compresses a buffer of 2^n chunks in parallel, returning
// their root node.
func CompressEigentree(buf []byte, key *[8]uint32, counter uint64, flags uint32) Node {
	if numChunks := uint64(len(buf) / ChunkSize); bits.OnesCount64(numChunks) != 1 {
		panic("non-power-of-two eigentree size")
	} else if numChunks == 1 {
		return CompressChunk(buf, key, counter, flags)
	} else if numChunks <= MaxSIMD {
		buflen := len(buf)
		if cap(buf) < MaxSIMD*ChunkSize {
			buf = append(buf, make([]byte, MaxSIMD*ChunkSize-len(buf))...)
		}
		return CompressBuffer((*[MaxSIMD * ChunkSize]byte)(buf[:MaxSIMD*ChunkSize]), buflen, key, counter, flags)
	} else {
		cvs := make([][8]uint32, numChunks/MaxSIMD)
		var wg sync.WaitGroup
		for i := range cvs {
			wg.Add(1)
			go func(i uint64) {
				defer wg.Done()
				cvs[i] = ChainingValue(CompressBuffer((*[MaxSIMD * ChunkSize]byte)(buf[i*MaxSIMD*ChunkSize:]), MaxSIMD*ChunkSize, key, counter+(MaxSIMD*i), flags))
			}(uint64(i))
		}
		wg.Wait()

		var rec func(cvs [][8]uint32) Node
		rec = func(cvs [][8]uint32) Node {
			if len(cvs) == 2 {
				return ParentNode(cvs[0], cvs[1], key, flags)
			} else if len(cvs) == MaxSIMD {
				return mergeSubtrees((*[MaxSIMD][8]uint32)(cvs), MaxSIMD, key, flags)
			}
			return ParentNode(ChainingValue(rec(cvs[:len(cvs)/2])), ChainingValue(rec(cvs[len(cvs)/2:])), key, flags)
		}
		return rec(cvs)
	}
}
