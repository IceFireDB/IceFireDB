package guts

import (
	"math/bits"
	"unsafe"
)

func compressNodeGeneric(out *[16]uint32, cv *[8]uint32, block *[16]uint32, counter uint64, blockLen uint32, flags uint32) {
	g := func(a, b, c, d, mx, my uint32) (uint32, uint32, uint32, uint32) {
		a = a + mx + b
		d = bits.RotateLeft32(d^a, -16)
		c += d
		b = bits.RotateLeft32(b^c, -12)
		a = a + my + b
		d = bits.RotateLeft32(d^a, -8)
		c += d
		b = bits.RotateLeft32(b^c, -7)
		return a, b, c, d
	}

	// NOTE: we unroll all of the rounds, as well as the permutations that occur
	// between rounds.

	// round 1 (also initializes state)
	// columns
	s0, s4, s8, s12 := g(cv[0], cv[4], IV[0], uint32(counter), block[0], block[1])
	s1, s5, s9, s13 := g(cv[1], cv[5], IV[1], uint32(counter>>32), block[2], block[3])
	s2, s6, s10, s14 := g(cv[2], cv[6], IV[2], blockLen, block[4], block[5])
	s3, s7, s11, s15 := g(cv[3], cv[7], IV[3], flags, block[6], block[7])
	// diagonals
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[8], block[9])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[10], block[11])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[12], block[13])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[14], block[15])

	// round 2
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[2], block[6])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[3], block[10])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[7], block[0])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[4], block[13])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[1], block[11])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[12], block[5])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[9], block[14])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[15], block[8])

	// round 3
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[3], block[4])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[10], block[12])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[13], block[2])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[7], block[14])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[6], block[5])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[9], block[0])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[11], block[15])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[8], block[1])

	// round 4
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[10], block[7])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[12], block[9])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[14], block[3])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[13], block[15])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[4], block[0])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[11], block[2])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[5], block[8])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[1], block[6])

	// round 5
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[12], block[13])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[9], block[11])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[15], block[10])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[14], block[8])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[7], block[2])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[5], block[3])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[0], block[1])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[6], block[4])

	// round 6
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[9], block[14])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[11], block[5])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[8], block[12])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[15], block[1])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[13], block[3])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[0], block[10])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[2], block[6])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[4], block[7])

	// round 7
	s0, s4, s8, s12 = g(s0, s4, s8, s12, block[11], block[15])
	s1, s5, s9, s13 = g(s1, s5, s9, s13, block[5], block[0])
	s2, s6, s10, s14 = g(s2, s6, s10, s14, block[1], block[9])
	s3, s7, s11, s15 = g(s3, s7, s11, s15, block[8], block[6])
	s0, s5, s10, s15 = g(s0, s5, s10, s15, block[14], block[10])
	s1, s6, s11, s12 = g(s1, s6, s11, s12, block[2], block[12])
	s2, s7, s8, s13 = g(s2, s7, s8, s13, block[3], block[4])
	s3, s4, s9, s14 = g(s3, s4, s9, s14, block[7], block[13])

	// finalization
	*out = [16]uint32{
		s0 ^ s8, s1 ^ s9, s2 ^ s10, s3 ^ s11,
		s4 ^ s12, s5 ^ s13, s6 ^ s14, s7 ^ s15,
		s8 ^ cv[0], s9 ^ cv[1], s10 ^ cv[2], s11 ^ cv[3],
		s12 ^ cv[4], s13 ^ cv[5], s14 ^ cv[6], s15 ^ cv[7],
	}
}

func compressChunkBlocksGeneric(cv *[8]uint32, blocks []byte, counter uint64, flags uint32) {
	n := Node{
		CV:       *cv,
		Counter:  counter,
		BlockLen: BlockSize,
		Flags:    flags | FlagChunkStart,
	}
	for ; len(blocks) > 0; blocks = blocks[BlockSize:] {
		if littleEndian {
			copy((*[BlockSize]byte)(unsafe.Pointer(&n.Block))[:], blocks)
		} else {
			n.Block = BytesToWords([64]byte(blocks))
		}
		var out [16]uint32
		compressNode(&out, &n.CV, &n.Block, n.Counter, n.BlockLen, n.Flags)
		n.CV = [8]uint32(out[:8])
		n.Flags &^= FlagChunkStart
	}
	*cv = n.CV
}

func compressChunksGeneric(cvs *[MaxSIMD][8]uint32, buf *[MaxSIMD * ChunkSize]byte, n int, key *[8]uint32, counter uint64, flags uint32) {
	for i := range cvs[:n] {
		cvs[i] = ChainingValue(CompressChunk(buf[i*ChunkSize:][:ChunkSize], key, counter+uint64(i), flags))
	}
}

func compressParentsGeneric(cvs *[MaxSIMD][8]uint32, n int, key *[8]uint32, flags uint32) {
	for i := range cvs[:n] {
		cvs[i] = ChainingValue(ParentNode(cvs[i*2], cvs[i*2+1], key, flags))
	}
}

func compressBlocksGeneric(out *[MaxSIMD * BlockSize]byte, n Node) {
	for i := 0; i < len(out); i += BlockSize {
		*(*[BlockSize]byte)(out[i:]) = WordsToBytes(CompressNode(n))
		n.Counter++
	}
}
