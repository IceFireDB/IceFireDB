package guts

import (
	"unsafe"
)

//go:generate go run avo/gen.go -out blake3_amd64.s

//go:noescape
func compressChunksAVX512(cvs *[16][8]uint32, buf *[16 * ChunkSize]byte, key *[8]uint32, counter uint64, flags uint32)

//go:noescape
func compressChunksAVX2(cvs *[8][8]uint32, buf *[8 * ChunkSize]byte, key *[8]uint32, counter uint64, flags uint32)

//go:noescape
func compressBlocksAVX512(out *[1024]byte, block *[16]uint32, cv *[8]uint32, counter uint64, blockLen uint32, flags uint32)

//go:noescape
func compressBlocksAVX2(out *[512]byte, msgs *[16]uint32, cv *[8]uint32, counter uint64, blockLen uint32, flags uint32)

//go:noescape
func compressParentsAVX2(parents *[8][8]uint32, cvs *[16][8]uint32, key *[8]uint32, flags uint32)

//go:noescape
func compressNodeSSE41(out *[16]uint32, cv *[8]uint32, block *[16]uint32, counter uint64, blockLen uint32, flags uint32)

func compressNode(out *[16]uint32, cv *[8]uint32, block *[16]uint32, counter uint64, blockLen uint32, flags uint32) {
	if haveSSE41 {
		compressNodeSSE41(out, cv, block, counter, blockLen, flags)
	} else {
		compressNodeGeneric(out, cv, block, counter, blockLen, flags)
	}
}

//go:noescape
func compressChunkPairAVX2(cvs *[2][8]uint32, p0 *byte, p1 *byte, blocks uint64, key *[8]uint32, counter uint64, flags uint32, flagsEnd uint32)

func compressChunkBlocks(cv *[8]uint32, blocks []byte, counter uint64, flags uint32) {
	if !haveAVX2 {
		compressChunkBlocksGeneric(cv, blocks, counter, flags)
		return
	}
	// NOTE: the routine compresses two chunks at once, so pass it the same
	// chunk twice. This is still faster than compressing one block at a time.
	var cvs [2][8]uint32
	compressChunkPairAVX2(&cvs, &blocks[0], &blocks[0], uint64(len(blocks)/BlockSize), cv, counter, flags, 0)
	*cv = cvs[0]
}

func compressChunks(cvs *[MaxSIMD][8]uint32, buf *[MaxSIMD * ChunkSize]byte, n int, key *[8]uint32, counter uint64, flags uint32) {
	switch {
	case haveAVX512 && n >= 3:
		compressChunksAVX512(cvs, buf, key, counter, flags)
	case haveAVX2 && n == 2:
		compressChunkPairAVX2((*[2][8]uint32)(cvs[:2]), &buf[0], &buf[ChunkSize], ChunkSize/BlockSize, key, counter, flags, FlagChunkEnd)
	case haveAVX2 && n >= 3:
		cvHalves := (*[2][8][8]uint32)(unsafe.Pointer(cvs))
		bufHalves := (*[2][8 * ChunkSize]byte)(unsafe.Pointer(buf))
		compressChunksAVX2(&cvHalves[0], &bufHalves[0], key, counter, flags)
		if n > 8 {
			compressChunksAVX2(&cvHalves[1], &bufHalves[1], key, counter+8, flags)
		}
	default:
		compressChunksGeneric(cvs, buf, n, key, counter, flags)
	}
}

func compressParents(cvs *[MaxSIMD][8]uint32, n int, key *[8]uint32, flags uint32) {
	if haveAVX2 && n >= 3 {
		compressParentsAVX2((*[8][8]uint32)(unsafe.Pointer(cvs)), cvs, key, flags)
	} else {
		compressParentsGeneric(cvs, n, key, flags)
	}
}

func compressBlocks(out *[MaxSIMD * BlockSize]byte, n Node) {
	switch {
	case haveAVX512:
		compressBlocksAVX512(out, &n.Block, &n.CV, n.Counter, n.BlockLen, n.Flags)
	case haveAVX2:
		outs := (*[2][512]byte)(unsafe.Pointer(out))
		compressBlocksAVX2(&outs[0], &n.Block, &n.CV, n.Counter, n.BlockLen, n.Flags)
		compressBlocksAVX2(&outs[1], &n.Block, &n.CV, n.Counter+8, n.BlockLen, n.Flags)
	default:
		compressBlocksGeneric(out, n)
	}
}
