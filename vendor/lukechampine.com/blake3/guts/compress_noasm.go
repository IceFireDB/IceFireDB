//go:build !amd64 && !arm64

package guts

func compressNode(out *[16]uint32, cv *[8]uint32, block *[16]uint32, counter uint64, blockLen uint32, flags uint32) {
	compressNodeGeneric(out, cv, block, counter, blockLen, flags)
}

func compressChunkBlocks(cv *[8]uint32, blocks []byte, counter uint64, flags uint32) {
	compressChunkBlocksGeneric(cv, blocks, counter, flags)
}

func compressChunks(cvs *[MaxSIMD][8]uint32, buf *[MaxSIMD * ChunkSize]byte, n int, key *[8]uint32, counter uint64, flags uint32) {
	compressChunksGeneric(cvs, buf, n, key, counter, flags)
}

func compressParents(cvs *[MaxSIMD][8]uint32, n int, key *[8]uint32, flags uint32) {
	compressParentsGeneric(cvs, n, key, flags)
}

func compressBlocks(out *[MaxSIMD * BlockSize]byte, n Node) {
	compressBlocksGeneric(out, n)
}
