// Package blake3 implements the BLAKE3 cryptographic hash function.
package blake3 // import "lukechampine.com/blake3"

import (
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"math"
	"math/bits"
	"runtime"
	"sync"

	"lukechampine.com/blake3/bao"
	"lukechampine.com/blake3/guts"
)

// Hasher implements hash.Hash.
type Hasher struct {
	key   [8]uint32
	flags uint32
	size  int // output size, for Sum

	// log(n) set of Merkle subtree roots, at most one per height.
	stack   [64][8]uint32
	counter uint64 // number of chunks hashed; also serves as a bit vector indicating which stack elems are occupied

	// Input is buffered until MaxSIMD chunks can be compressed at once.
	buf    [guts.MaxSIMD * guts.ChunkSize]byte
	buflen int

	// If the input so far ends on a buffer boundary, last is the root node of
	// its final buffer, which is not merged into the stack until more input
	// arrives: if it is the end of the input, it is merged differently.
	last     guts.Node
	haveLast bool
}

func (h *Hasher) hasSubtreeAtHeight(i int) bool {
	return h.counter&(1<<i) != 0
}

func (h *Hasher) pushSubtree(cv [8]uint32, height int) {
	// seek to first open stack slot, merging subtrees as we go
	i := height
	for h.hasSubtreeAtHeight(i) {
		cv = guts.ChainingValue(guts.ParentNode(h.stack[i], cv, &h.key, h.flags))
		i++
	}
	h.stack[i] = cv
	h.counter += 1 << height
}

// rootNode computes the root of the Merkle tree. It does not modify the
// stack.
func (h *Hasher) rootNode() guts.Node {
	if h.haveLast {
		return h.finalize(h.last)
	}
	return h.finalize(guts.CompressBuffer(&h.buf, h.buflen, &h.key, h.counter, h.flags))
}

// finalize merges n, the root of the final subtree, with the stack.
func (h *Hasher) finalize(n guts.Node) guts.Node {
	for i := bits.TrailingZeros64(h.counter); i < bits.Len64(h.counter); i++ {
		if h.hasSubtreeAtHeight(i) {
			n = guts.ParentNode(h.stack[i], guts.ChainingValue(n), &h.key, h.flags)
		}
	}
	n.Flags |= guts.FlagRoot
	return n
}

// compressBuffers compresses the full buffers in p, and pushes the chaining
// value of each eigentree they form onto the stack.
func (h *Hasher) compressBuffers(p []byte) {
	numBufs := len(p) / len(h.buf)
	// NOTE: a few buffers are compressed serially, so their chaining values
	// can live on the stack; more than that are compressed in parallel, which
	// requires allocating.
	var few [3][8]uint32
	if numBufs <= len(few) {
		for i := range few[:numBufs] {
			buf := (*[len(h.buf)]byte)(p[i*len(h.buf):])
			few[i] = guts.ChainingValue(guts.CompressBuffer(buf, len(buf), &h.key, h.counter+uint64(i)*guts.MaxSIMD, h.flags))
		}
		h.pushEigentrees(few[:numBufs])
	} else {
		cvs := make([][8]uint32, numBufs)
		guts.CompressBuffers(cvs, p, &h.key, h.counter, h.flags)
		h.pushEigentrees(cvs)
	}
}

// pushEigentrees merges the chaining values of consecutive buffers into
// eigentrees, and pushes each one onto the stack. It overwrites cvs.
func (h *Hasher) pushEigentrees(cvs [][8]uint32) {
	const bufHeight = 4 // log2(guts.MaxSIMD)
	var trees [64]int
	for _, height := range guts.Eigentrees(trees[:0], h.counter, uint64(len(cvs))*guts.MaxSIMD) {
		tree := cvs[:1<<(height-bufHeight)]
		cv := tree[0]
		if len(tree) > 1 {
			cv = guts.ChainingValue(guts.MergeEigentree(tree, &h.key, h.flags))
		}
		h.pushSubtree(cv, height)
		cvs = cvs[len(tree):]
	}
}

// Write implements hash.Hash.
func (h *Hasher) Write(p []byte) (int, error) {
	const bufHeight = 4 // log2(guts.MaxSIMD)
	lenp := len(p)
	if lenp == 0 {
		return 0, nil
	}

	// more input has arrived, so the last buffer was not the final one
	if h.haveLast {
		h.pushSubtree(guts.ChainingValue(h.last), bufHeight)
		h.haveLast = false
	}

	// fill the buffer, if it is partially full
	if h.buflen > 0 {
		n := copy(h.buf[h.buflen:], p)
		h.buflen += n
		p = p[n:]
		if h.buflen < len(h.buf) {
			return lenp, nil
		}
		node := guts.CompressBuffer(&h.buf, h.buflen, &h.key, h.counter, h.flags)
		h.buflen = 0
		if len(p) == 0 {
			h.last, h.haveLast = node, true
			return lenp, nil
		}
		h.pushSubtree(guts.ChainingValue(node), bufHeight)
	}

	// compress full buffers directly from p
	if len(p) >= len(h.buf) {
		full := p[:len(p)-len(p)%len(h.buf)]
		p = p[len(full):]
		if len(p) == 0 {
			// p ends on a buffer boundary
			final := (*[len(h.buf)]byte)(full[len(full)-len(h.buf):])
			h.compressBuffers(full[:len(full)-len(h.buf)])
			h.last = guts.CompressBuffer(final, len(final), &h.key, h.counter, h.flags)
			h.haveLast = true
			return lenp, nil
		}
		h.compressBuffers(full)
	}

	// buffer the remainder
	h.buflen = copy(h.buf[:], p)

	return lenp, nil
}

// Sum implements hash.Hash.
func (h *Hasher) Sum(b []byte) (sum []byte) {
	// We need to append h.Size() bytes to b. Reuse b's capacity if possible;
	// otherwise, allocate a new slice.
	if total := len(b) + h.Size(); cap(b) >= total {
		sum = b[:total]
	} else {
		sum = make([]byte, total)
		copy(sum, b)
	}
	// Read into the appended portion of sum. Use a low-latency-low-throughput
	// path for small digests (requiring a single compression), and a
	// high-latency-high-throughput path for large digests.
	if dst := sum[len(b):]; len(dst) <= 64 {
		out := guts.WordsToBytes(guts.CompressNode(h.rootNode()))
		copy(dst, out[:])
	} else {
		h.XOF().Read(dst)
	}
	return
}

// Reset implements hash.Hash.
func (h *Hasher) Reset() {
	h.counter = 0
	h.buflen = 0
	h.haveLast = false
}

// BlockSize implements hash.Hash.
func (h *Hasher) BlockSize() int { return 64 }

// Size implements hash.Hash.
func (h *Hasher) Size() int { return h.size }

// XOF returns an OutputReader initialized with the current hash state.
func (h *Hasher) XOF() *OutputReader {
	return &OutputReader{
		n: h.rootNode(),
	}
}

func newHasher(key [8]uint32, flags uint32, size int) *Hasher {
	return &Hasher{
		key:   key,
		flags: flags,
		size:  size,
	}
}

// New returns a Hasher for the specified digest size and key. If key is nil,
// the hash is unkeyed. Otherwise, len(key) must be 32.
func New(size int, key []byte) *Hasher {
	if key == nil {
		return newHasher(guts.IV, 0, size)
	}
	var keyWords [8]uint32
	for i := range keyWords {
		keyWords[i] = binary.LittleEndian.Uint32(key[i*4:])
	}
	return newHasher(keyWords, guts.FlagKeyedHash, size)
}

// Hashers are large, so Sum256 and Sum512 reuse them.
var hasherPool = sync.Pool{
	New: func() any { return New(64, nil) },
}

// sumBlock returns the output of the root node of an input no longer than a
// block.
//
// NOTE: this is written to avoid copying, since at this size even a few
// copies are a measurable fraction of the total.
func sumBlock(b []byte) (words [16]uint32) {
	block := guts.PadBlock(b)
	guts.Compress(&words, &guts.IV, &block, 0, uint32(len(b)), guts.FlagChunkStart|guts.FlagChunkEnd|guts.FlagRoot)
	return
}

// Sum256 returns the unkeyed BLAKE3 hash of b, truncated to 256 bits.
func Sum256(b []byte) (out [32]byte) {
	if len(b) <= guts.BlockSize {
		out512 := guts.WordsToBytes(sumBlock(b))
		return [32]byte(out512[:32])
	}
	out512 := Sum512(b)
	copy(out[:], out512[:])
	return
}

// Sum512 returns the unkeyed BLAKE3 hash of b, truncated to 512 bits.
func Sum512(b []byte) (out [64]byte) {
	var n guts.Node
	if len(b) <= guts.BlockSize {
		return guts.WordsToBytes(sumBlock(b))
	} else if len(b) <= guts.MaxSIMD*guts.ChunkSize {
		n = guts.CompressSlice(b, &guts.IV, 0, 0)
		n.Flags |= guts.FlagRoot
	} else {
		// compress everything but the final buffer, then compress that
		// buffer in place, rather than copying it into the Hasher
		const bufsize = guts.MaxSIMD * guts.ChunkSize
		final := (len(b) - 1) / bufsize * bufsize
		h := hasherPool.Get().(*Hasher)
		h.Reset()
		h.compressBuffers(b[:final])
		n = h.finalize(guts.CompressSlice(b[final:], &h.key, h.counter, h.flags))
		hasherPool.Put(h)
	}
	return guts.WordsToBytes(guts.CompressNode(n))
}

// DeriveKey derives a subkey from ctx and srcKey. ctx should be hardcoded,
// globally unique, and application-specific. A good format for ctx strings is:
//
//	[application] [commit timestamp] [purpose]
//
// e.g.:
//
//	example.com 2019-12-25 16:18:03 session tokens v1
//
// The purpose of these requirements is to ensure that an attacker cannot trick
// two different applications into using the same context string.
func DeriveKey(subKey []byte, ctx string, srcKey []byte) {
	// construct the derivation Hasher
	const derivationIVLen = 32
	h := newHasher(guts.IV, guts.FlagDeriveKeyContext, 32)
	h.Write([]byte(ctx))
	derivationIV := h.Sum(make([]byte, 0, derivationIVLen))
	var ivWords [8]uint32
	for i := range ivWords {
		ivWords[i] = binary.LittleEndian.Uint32(derivationIV[i*4:])
	}
	h = newHasher(ivWords, guts.FlagDeriveKeyMaterial, 0)
	// derive the subKey
	h.Write(srcKey)
	h.XOF().Read(subKey)
}

// An OutputReader produces an seekable stream of 2^64 - 1 pseudorandom output
// bytes.
type OutputReader struct {
	n   guts.Node
	buf [guts.MaxSIMD * guts.BlockSize]byte
	off uint64
}

// Read implements io.Reader. Callers may assume that Read returns len(p), nil
// unless the read would extend beyond the end of the stream.
func (or *OutputReader) Read(p []byte) (int, error) {
	if or.off == math.MaxUint64 {
		return 0, io.EOF
	} else if rem := math.MaxUint64 - or.off; uint64(len(p)) > rem {
		p = p[:rem]
	}
	lenp := len(p)

	// drain existing buffer
	const bufsize = guts.MaxSIMD * guts.BlockSize
	if or.off%bufsize != 0 {
		n := copy(p, or.buf[or.off%bufsize:])
		p = p[n:]
		or.off += uint64(n)
	}

	for len(p) > 0 {
		or.n.Counter = or.off / guts.BlockSize
		if numBufs := len(p) / len(or.buf); numBufs < 1 {
			guts.CompressBlocks(&or.buf, or.n)
			n := copy(p, or.buf[or.off%bufsize:])
			p = p[n:]
			or.off += uint64(n)
		} else if numBufs == 1 {
			guts.CompressBlocks((*[bufsize]byte)(p), or.n)
			p = p[bufsize:]
			or.off += bufsize
		} else {
			// parallelize
			par := min(numBufs, runtime.NumCPU())
			per := uint64(numBufs / par)
			var wg sync.WaitGroup
			for range par {
				wg.Add(1)
				go func(p []byte, n guts.Node) {
					defer wg.Done()
					for i := range per {
						guts.CompressBlocks((*[bufsize]byte)(p[i*bufsize:]), n)
						n.Counter += bufsize / guts.BlockSize
					}
				}(p, or.n)
				p = p[per*bufsize:]
				or.off += per * bufsize
				or.n.Counter = or.off / guts.BlockSize
			}
			wg.Wait()
		}
	}
	return lenp, nil
}

// Seek implements io.Seeker.
func (or *OutputReader) Seek(offset int64, whence int) (int64, error) {
	off := or.off
	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, errors.New("seek position cannot be negative")
		}
		off = uint64(offset)
	case io.SeekCurrent:
		if offset < 0 {
			if uint64(-offset) > off {
				return 0, errors.New("seek position cannot be negative")
			}
			off -= uint64(-offset)
		} else {
			off += uint64(offset)
		}
	case io.SeekEnd:
		off = uint64(offset) - 1
	default:
		panic("invalid whence")
	}
	or.off = off
	or.n.Counter = uint64(off) / guts.BlockSize
	if or.off%(guts.MaxSIMD*guts.BlockSize) != 0 {
		guts.CompressBlocks(&or.buf, or.n)
	}
	// NOTE: or.off >= 2^63 will result in a negative return value.
	// Nothing we can do about this.
	return int64(or.off), nil
}

// ensure that Hasher implements hash.Hash
var _ hash.Hash = (*Hasher)(nil)

// EncodedSize returns the size of a Bao encoding for the provided quantity
// of data.
//
// Deprecated: Use bao.EncodedSize instead.
func BaoEncodedSize(dataLen int, outboard bool) int {
	return bao.EncodedSize(dataLen, 0, outboard)
}

// BaoEncode computes the intermediate BLAKE3 tree hashes of data and writes
// them to dst.
//
// Deprecated: Use bao.Encode instead.
func BaoEncode(dst io.WriterAt, data io.Reader, dataLen int64, outboard bool) ([32]byte, error) {
	return bao.Encode(dst, data, dataLen, 0, outboard)
}

// BaoDecode reads content and tree data from the provided reader(s), and
// streams the verified content to dst.
//
// Deprecated: Use bao.Decode instead.
func BaoDecode(dst io.Writer, data, outboard io.Reader, root [32]byte) (bool, error) {
	return bao.Decode(dst, data, outboard, 0, root)
}

// BaoEncodeBuf returns the Bao encoding and root (i.e. BLAKE3 hash) for data.
//
// Deprecated: Use bao.EncodeBuf instead.
func BaoEncodeBuf(data []byte, outboard bool) ([]byte, [32]byte) {
	return bao.EncodeBuf(data, 0, outboard)
}

// BaoVerifyBuf verifies the Bao encoding and root (i.e. BLAKE3 hash) for data.
//
// Deprecated: Use bao.VerifyBuf instead.
func BaoVerifyBuf(data, outboard []byte, root [32]byte) bool {
	return bao.VerifyBuf(data, outboard, 0, root)
}
