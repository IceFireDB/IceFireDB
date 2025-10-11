package kbucket

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/minio/sha256-simd"
	mh "github.com/multiformats/go-multihash"

	ks "github.com/libp2p/go-libp2p-kbucket/keyspace"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ErrLookupFailure is returned if a routing table query returns no results. This is NOT expected
// behaviour
var ErrLookupFailure = errors.New("failed to find any peer in table")

// PeerIDPreimageMaxCpl is the maximum common prefix length we support for peer
// ID preimage generation, since these are computed in a static way.
const PeerIDPreimageMaxCpl uint = 15

// ID for IpfsDHT is in the XORKeySpace
//
// The type dht.ID signifies that its contents have been hashed from either a
// peer.ID or a util.Key. This unifies the keyspace
type ID []byte

func (id ID) less(other ID) bool {
	a := ks.Key{Space: ks.XORKeySpace, Bytes: id}
	b := ks.Key{Space: ks.XORKeySpace, Bytes: other}
	return a.Less(b)
}

func Xor(a, b ID) ID {
	return ID(ks.Xor(a, b))
}

func CommonPrefixLen(a, b ID) int {
	return ks.ZeroPrefixLen(Xor(a, b))
}

// ConvertPeerID creates a DHT ID by hashing a Peer ID (Multihash)
func ConvertPeerID(id peer.ID) ID {
	hash := sha256.Sum256([]byte(id))
	return hash[:]
}

// ConvertKey creates a DHT ID by hashing a local key (String)
func ConvertKey(id string) ID {
	hash := sha256.Sum256([]byte(id))
	return hash[:]
}

// Closer returns true if a is closer to key than b is
func Closer(a, b peer.ID, key string) bool {
	aid := ConvertPeerID(a)
	bid := ConvertPeerID(b)
	tgt := ConvertKey(key)
	adist := Xor(aid, tgt)
	bdist := Xor(bid, tgt)

	return adist.less(bdist)
}

// GenRandPeerID generates a random peerID sharing a common prefix of length
// `cpl` with the provided ID.
func GenRandPeerIDWithCPL(targetID ID, cpl uint) (peer.ID, error) {
	if cpl > maxCplForRefresh {
		return "", fmt.Errorf("cannot generate peer ID for Cpl greater than %d", PeerIDPreimageMaxCpl)
	}
	localPrefix := binary.BigEndian.Uint16(targetID)
	// For host with ID `L`, an ID `K` belongs to a bucket with ID `B` ONLY IF CommonPrefixLen(L,K) is EXACTLY B.
	// Hence, to achieve a targetPrefix `T`, we must toggle the (T+1)th bit in L & then copy (T+1) bits from L
	// to our randomly generated prefix.
	toggledLocalPrefix := localPrefix ^ (uint16(0x8000) >> cpl)
	randPrefix, err := randUint16()
	if err != nil {
		return "", err
	}

	// Combine the toggled local prefix and the random bits at the correct offset
	// such that ONLY the first `targetCpl` bits match the local ID.
	mask := (^uint16(0)) << (16 - (cpl + 1))
	targetPrefix := (toggledLocalPrefix & mask) | (randPrefix & ^mask)

	// Convert to a known peer ID.
	key := keyPrefixMap[targetPrefix]
	id := [32 + 2]byte{mh.SHA2_256, 32}
	binary.BigEndian.PutUint32(id[2:], key)
	return peer.ID(id[:]), nil
}
