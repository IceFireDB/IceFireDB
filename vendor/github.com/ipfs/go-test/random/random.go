package random

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
)

var (
	initSeed     int64
	globalSeed   atomic.Int64
	globalSeqGen atomic.Uint64
)

func init() {
	SetSeed(time.Now().UTC().UnixNano())
}

// NewRand returns a new pseudo-random number source, seeded with the next
// value of a global sequence.
func NewRand() *rand.Rand {
	return NewSeededRand(globalSeed.Add(1))
}

// NewSeededRand returns a new pseudo-random number source seeded with the
// specified value.
func NewSeededRand(seed int64) *rand.Rand {
	return rand.New(rand.NewSource(seed))
}

// Returns the initial seed used for the pseudo-random number generator, or the
// most recent value set by SetSeed.
func Seed() int64 {
	return initSeed
}

// Sets the seed for the pseudo-random number generator. Calling
// SetSeed(Seed()) each time before generating random items will cause items
// with the same values to be generated.
func SetSeed(seed int64) {
	initSeed = seed
	globalSeed.Store(seed)
	rng := rand.New(rand.NewSource(seed))
	globalSeqGen.Store(rng.Uint64())
}

// Addrs returns a slice of n random unique addresses.
func Addrs(n int) []string {
	addrs := make([]string, n)
	addrSet := make(map[string]struct{})
	rng := NewRand()
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("/ip4/%d.%d.%d.%d/tcp/%d", rng.Int()%255, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(254)+1, rng.Intn(48157)+1024)
		if _, ok := addrSet[addr]; ok {
			i--
			continue
		}
		addrs[i] = addr
	}
	return addrs
}

// BlocksOfSize generates a slice of blocks of the specified byte size.
func BlocksOfSize(n int, size int) []blocks.Block {
	genBlocks := make([]blocks.Block, n)
	for i := range n {
		genBlocks[i] = blocks.NewBlock(Bytes(size))
	}
	return genBlocks
}

// Bytes returns a byte array of the given size with random values.
func Bytes(n int) []byte {
	data := make([]byte, n)
	NewRand().Read(data)
	return data
}

// Cids returns a slice of n random unique CIDs.
func Cids(n int) []cid.Cid {
	cids := make([]cid.Cid, 0, n)
	rng := NewRand()
	for len(cids) < n {
		var b [32]byte
		rng.Read(b[:])
		h, err := multihash.Encode(b[:], multihash.SHA2_256)
		if err != nil {
			panic(err)
		}
		cids = append(cids, cid.NewCidV1(uint64(multicodec.DagJson), h))
	}
	return cids
}

// Identity returns a random unique peer ID, private key, and public key.
func Identity() (peer.ID, crypto.PrivKey, crypto.PubKey) {
	privKey, pubKey, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 256, NewRand())
	if err != nil {
		panic(err)
	}
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		panic(err)
	}
	return peerID, privKey, pubKey
}

// Multiaddrs returns a slice of n random unique Multiaddrs.
func Multiaddrs(n int) []multiaddr.Multiaddr {
	addrs := Addrs(n)
	maddrs := make([]multiaddr.Multiaddr, n)
	for i, addr := range addrs {
		maddr, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			panic(err)
		}
		maddrs[i] = maddr
	}
	return maddrs
}

var httpMultiaddrComponent = multiaddr.StringCast("/http")

// HttpMultiaddrs returns a slice of n random unique Multiaddrs.
func HttpMultiaddrs(n int) []multiaddr.Multiaddr {
	maddrs := Multiaddrs(n)
	for i, ma := range maddrs {
		maddrs[i] = multiaddr.Join(ma, httpMultiaddrComponent)
	}
	return maddrs
}

// Multihashes returns a slice of n random unique Multihashes.
func Multihashes(n int) []multihash.Multihash {
	rng := NewRand()
	mhashes := make([]multihash.Multihash, 0, n)
	for len(mhashes) < n {
		var b [32]byte
		rng.Read(b[:])
		h, err := multihash.Encode(b[:], multihash.SHA2_256)
		if err != nil {
			panic(err.Error())
		}
		mhashes = append(mhashes, h)
	}
	return mhashes
}

// Peers returns a slice of n random peer IDs.
func Peers(n int) []peer.ID {
	peerIDs := make([]peer.ID, n)
	rng := NewRand()
	for i := range n {
		_, publicKey, err := crypto.GenerateEd25519Key(rng)
		if err != nil {
			panic(err)
		}
		peerID, err := peer.IDFromPublicKey(publicKey)
		if err != nil {
			panic(err)
		}
		peerIDs[i] = peerID
	}
	return peerIDs
}

// Sequence returns a series of monotonically increasing numbers, starting at
// the next unique global sequence value. Any current calls to Sequence will
// not generate any overlapping values.
//
// The sequence numbers themselves are not random, only the global starting
// value of the sequence numbers is random. This ensures that all sequences
// generated within a test are unique, assuming < 2^64 values are generated,
// but start out at a random value.
func Sequence(n int) []uint64 {
	if n == 1 {
		return []uint64{globalSeqGen.Add(1)}
	}
	seq := make([]uint64, n)
	seqVal := globalSeqGen.Add(uint64(n)) - uint64(n-1)
	for i := range n {
		seq[i] = seqVal + uint64(i)
	}
	return seq
}

// SequenceNext returns the next unique global sequence value. This is
// equivalent to Sequence(1)[0].
func SequenceNext() uint64 {
	return globalSeqGen.Add(1)
}
