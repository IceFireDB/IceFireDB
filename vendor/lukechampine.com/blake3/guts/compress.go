package guts

import (
	"encoding/binary"
	"unsafe"
)

// Each platform provides compressNode, which compresses a single node;
// compressChunkBlocks, which compresses the leading blocks of a chunk (the
// first of which starts the chunk), replacing cv with the result; and three
// kernels that compress many nodes at once:
//
//   - compressChunks stores the chaining values of the first n chunks of buf
//     in cvs.
//   - compressParents merges the first n pairs of cvs, storing the chaining
//     value of each pair's parent node in the first n elements of cvs.
//   - compressBlocks compresses MaxSIMD copies of a node with successive
//     counter values, storing the results in out.
//
// A kernel may process more nodes than requested, clobbering unused elements
// of cvs.

// Compress compresses a node, specified by its fields, into a 16-word output.
// It is equivalent to CompressNode, but avoids copying.
func Compress(out *[16]uint32, cv *[8]uint32, block *[16]uint32, counter uint64, blockLen uint32, flags uint32) {
	compressNode(out, cv, block, counter, blockLen, flags)
}

// CompressNode compresses a node into a 16-word output.
func CompressNode(n Node) (out [16]uint32) {
	compressNode(&out, &n.CV, &n.Block, n.Counter, n.BlockLen, n.Flags)
	return
}

// ChainingValue compresses n and returns the first 8 output words.
func ChainingValue(n Node) [8]uint32 {
	var out [16]uint32
	compressNode(&out, &n.CV, &n.Block, n.Counter, n.BlockLen, n.Flags)
	return [8]uint32(out[:8])
}

// PadBlock returns the words of b, which must be no longer than a block,
// padded with zeros.
func PadBlock(b []byte) (block [16]uint32) {
	if littleEndian {
		copy((*[BlockSize]byte)(unsafe.Pointer(&block))[:], b)
		return
	}
	var bytes [BlockSize]byte
	copy(bytes[:], b)
	return bytesToWordsSlow(&bytes)
}

// CompressBuffer compresses up to MaxSIMD chunks in parallel and returns their
// root node.
func CompressBuffer(buf *[MaxSIMD * ChunkSize]byte, buflen int, key *[8]uint32, counter uint64, flags uint32) Node {
	if buflen <= ChunkSize {
		return CompressChunk(buf[:buflen], key, counter, flags)
	}
	var cvs [MaxSIMD][8]uint32
	numChunks := uint64(buflen / ChunkSize)
	compressChunks(&cvs, buf, int(numChunks), key, counter, flags)
	if rem := buflen % ChunkSize; rem != 0 {
		// the partial chunk must be compressed serially
		cvs[numChunks] = ChainingValue(CompressChunk(buf[buflen-rem:buflen], key, counter+numChunks, flags))
		numChunks++
	}
	return mergeSubtrees(&cvs, numChunks, key, flags)
}

// CompressChunk compresses a single chunk, returning its final (uncompressed)
// node.
func CompressChunk(chunk []byte, key *[8]uint32, counter uint64, flags uint32) Node {
	n := Node{
		CV:       *key,
		Counter:  counter,
		BlockLen: BlockSize,
		Flags:    flags | FlagChunkStart,
	}
	// compress every block but the last
	if rest := (len(chunk) - 1) / BlockSize * BlockSize; rest > 0 {
		compressChunkBlocks(&n.CV, chunk[:rest], counter, flags)
		chunk = chunk[rest:]
		n.Flags &^= FlagChunkStart
	}
	n.Block = PadBlock(chunk)
	n.BlockLen = uint32(len(chunk))
	n.Flags |= FlagChunkEnd
	return n
}

// CompressBlocks compresses MaxSIMD copies of n with successive counter values,
// storing the results in out.
func CompressBlocks(out *[MaxSIMD * BlockSize]byte, n Node) {
	compressBlocks(out, n)
}

// mergeSubtrees merges numCVs chaining values, returning their root node.
func mergeSubtrees(cvs *[MaxSIMD][8]uint32, numCVs uint64, key *[8]uint32, flags uint32) Node {
	for numCVs > 2 {
		// an odd CV is carried up to the next level
		last := cvs[numCVs-1]
		compressParents(cvs, int(numCVs/2), key, flags)
		if numCVs%2 != 0 {
			cvs[numCVs/2] = last
		}
		numCVs = (numCVs + 1) / 2
	}
	return ParentNode(cvs[0], cvs[1], key, flags)
}

// On little-endian platforms, bytes and words can be converted by
// reinterpreting memory.
var littleEndian = binary.NativeEndian.Uint16([]byte{1, 0}) == 1

// BytesToWords converts an array of 64 bytes to an array of 16 words.
func BytesToWords(bytes [64]byte) [16]uint32 {
	// NOTE: the slow path is a separate function so that this one can be
	// inlined.
	if littleEndian {
		return *(*[16]uint32)(unsafe.Pointer(&bytes))
	}
	return bytesToWordsSlow(&bytes)
}

func bytesToWordsSlow(bytes *[64]byte) (words [16]uint32) {
	for i := range words {
		words[i] = binary.LittleEndian.Uint32(bytes[4*i:])
	}
	return
}

// WordsToBytes converts an array of 16 words to an array of 64 bytes.
func WordsToBytes(words [16]uint32) [64]byte {
	if littleEndian {
		return *(*[64]byte)(unsafe.Pointer(&words))
	}
	return wordsToBytesSlow(&words)
}

func wordsToBytesSlow(words *[16]uint32) (bytes [64]byte) {
	for i, w := range words {
		binary.LittleEndian.PutUint32(bytes[4*i:], w)
	}
	return
}
