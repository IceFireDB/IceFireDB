package ipns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	mb "github.com/multiformats/go-multibase"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

const (
	// NamespacePrefix is the prefix of the IPNS namespace.
	NamespacePrefix = "/ipns/"
)

// Name represents a [Multihash] of a serialized public key according to the
// [IPNS Name] specifications.
//
// [Multihash]: https://multiformats.io/multihash/
// [IPNS Name]: https://specs.ipfs.tech/ipns/ipns-record/#ipns-name
type Name struct {
	multihash string // binary Multihash without multibase envelope
}

// NameFromString creates a [Name] from the given IPNS Name in its [string representation].
//
// [string representation]: https://specs.ipfs.tech/ipns/ipns-record/#string-representation
func NameFromString(str string) (Name, error) {
	str = strings.TrimPrefix(str, NamespacePrefix)
	pid, err := peer.Decode(str)
	if err != nil {
		return Name{}, err
	}
	return NameFromPeer(pid), nil
}

// NameFromRoutingKey creates a [Name] from the given IPNS Name in its routing key
// representation. See [Name.RoutingKey] for more information.
func NameFromRoutingKey(data []byte) (Name, error) {
	if !bytes.HasPrefix(data, []byte(NamespacePrefix)) {
		return Name{}, ErrInvalidName
	}

	data = bytes.TrimPrefix(data, []byte(NamespacePrefix))
	pid, err := peer.IDFromBytes(data)
	if err != nil {
		return Name{}, err
	}
	return NameFromPeer(pid), nil
}

// NameFromPeer creates a [Name] from the given [peer.ID].
func NameFromPeer(pid peer.ID) Name {
	return Name{multihash: string(pid)}
}

// NameFromCid creates a [Name] from the given [cid.Cid].
func NameFromCid(c cid.Cid) (Name, error) {
	code := mc.Code(c.Type())
	if code != mc.Libp2pKey {
		return Name{}, fmt.Errorf("CID codec %q is not allowed for IPNS Names, use  %q instead", code, mc.Libp2pKey)
	}
	return Name{multihash: string(c.Hash())}, nil
}

// RoutingKey returns the binary IPNS Routing Key for the given [Name]. Note that
// the intended use of this function is for [routing purposes] only. The output of
// this function is binary, not human readable. For a human-readable string, see [Name.Key].
//
// [routing purposes]: https://specs.ipfs.tech/ipns/ipns-record/#routing-record
func (n Name) RoutingKey() []byte {
	var buffer bytes.Buffer
	buffer.WriteString(NamespacePrefix)
	buffer.WriteString(n.multihash) // Note: we append raw multihash bytes (no multibase)
	return buffer.Bytes()
}

// Cid returns [Name] encoded as a [cid.Cid] of the public key. If the IPNS Name
// is invalid (e.g., empty), this will return the empty Cid.
func (n Name) Cid() cid.Cid {
	m, err := mh.Cast([]byte(n.multihash))
	if err != nil {
		return cid.Undef
	}
	return cid.NewCidV1(cid.Libp2pKey, m)
}

// Peer returns [Name] as a [peer.ID].
func (n Name) Peer() peer.ID {
	return peer.ID(n.multihash)
}

// String returns the human-readable IPNS Name, encoded as a CIDv1 with libp2p-key
// multicodec (0x72) with case-insensitive Base36.
func (n Name) String() string {
	name, err := n.Cid().StringOfBase(mb.Base36)
	if err != nil {
		panic(fmt.Errorf("cid.StringOfBase was called with wrong parameters: %w", err))
	}
	return name
}

// UnmarshalJSON implements [json.Unmarshaler] interface. IPNS Name will
// unmarshal from a string via [NameFromString].
func (n *Name) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return err
	}

	v, err := NameFromString(str)
	if err != nil {
		return err
	}

	*n = v
	return nil
}

// MarshalJSON implements [json.Marshaler] interface. IPNS Name will
// marshal as a string using [Name.String].
func (n Name) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.String())
}

// Equal returns whether the records are equal.
func (n Name) Equal(other Name) bool {
	return n.multihash == other.multihash
}

// AsPath returns the IPNS Name as a [path.Path] prefixed by [path.IPNSNamespace].
func (n Name) AsPath() path.Path {
	p, err := path.NewPathFromSegments(path.IPNSNamespace, n.String())
	if err != nil {
		panic(fmt.Errorf("path.NewPathFromSegments was called with invalid parameters: %w", err))
	}
	return p
}
