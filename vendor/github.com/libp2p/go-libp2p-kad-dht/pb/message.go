package dht_pb

import (
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/encoding/protowire"
)

var log = logging.Logger("dht.pb")

// MaxPeerRecordSize bounds the serialized size of a single Message_Peer on the
// wire. A peer record needs only a peer ID, its multiaddrs and a connection
// flag, so 8 KiB — the identify protocol's budget for a peer's entire signed
// self-description — is a generous per-record ceiling: comfortably more than any
// well-formed peer requires, while keeping records a predictable size both on
// the wire and in the peerstore. Addresses past the limit are dropped; the peer
// ID is always kept.
const MaxPeerRecordSize = 8 << 10

// Field numbers within Message.Peer (see pb/dht.proto), used to size a record's
// framed contribution without marshalling it.
const (
	peerIDField         protowire.Number = 1
	peerAddrsField      protowire.Number = 2
	peerConnectionField protowire.Number = 3
)

// peerAddrsTagSize is a repeated address field's tag size, added once per
// address when sizing a record.
var peerAddrsTagSize = protowire.SizeTag(peerAddrsField)

// boundPeerRecordAddrs trims pbp.Addrs in place, dropping trailing addresses
// until the peer ID, addresses and connection flag serialize within
// MaxPeerRecordSize. The peer ID and connection flag are always kept; a record
// already within the limit is left untouched. Applied on both the send and
// receive paths so a peer record keeps a predictable size on the wire and in
// the peerstore. It sizes only these fields, so on a freshly built outbound
// record that is exactly proto.Size; on a decoded inbound record it bounds the
// address list, not any unknown fields present on the record.
func boundPeerRecordAddrs(pbp *Message_Peer) {
	if pbp == nil {
		return
	}
	// SizeBytes covers each length-delimited field's length prefix and bytes;
	// SizeTag covers its field tag. The connection flag is an open proto3 enum,
	// so a remote peer can send a value that serializes to as many as 10 bytes;
	// size it from the actual value rather than assuming a single byte. On the
	// send path Connection is set after bounding, but only ever to a trusted
	// single-byte value, so the finished record stays within the bound.
	connectionFieldSize := protowire.SizeTag(peerConnectionField) +
		protowire.SizeVarint(uint64(int64(pbp.Connection)))
	size := protowire.SizeTag(peerIDField) + protowire.SizeBytes(len(pbp.Id)) + connectionFieldSize
	for i, addr := range pbp.Addrs {
		size += peerAddrsTagSize + protowire.SizeBytes(len(addr))
		if size > MaxPeerRecordSize {
			pbp.Addrs = pbp.Addrs[:i]
			return
		}
	}
}

type PeerRoutingInfo struct {
	peer.AddrInfo
	network.Connectedness
}

// NewMessage constructs a new dht message with given type, key, and level
func NewMessage(typ Message_MessageType, key []byte, level int) *Message {
	m := &Message{
		Type: typ,
		Key:  key,
	}
	m.SetClusterLevel(level)
	return m
}

func peerRoutingInfoToPBPeer(p PeerRoutingInfo) *Message_Peer {
	pbp := &Message_Peer{}

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = []byte(p.ID)
	pbp.Connection = ConnectionType(p.Connectedness)
	boundPeerRecordAddrs(pbp)
	return pbp
}

func peerInfoToPBPeer(p peer.AddrInfo) *Message_Peer {
	pbp := &Message_Peer{}

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	pbp.Id = []byte(p.ID)
	boundPeerRecordAddrs(pbp)
	return pbp
}

// PBPeerToPeer turns a *Message_Peer into its peer.AddrInfo counterpart
func PBPeerToPeerInfo(pbp *Message_Peer) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    peer.ID(pbp.Id),
		Addrs: pbp.Addresses(),
	}
}

// RawPeerInfosToPBPeers converts a slice of Peers into a slice of *Message_Peers,
// ready to go out on the wire.
func RawPeerInfosToPBPeers(peers []peer.AddrInfo) []*Message_Peer {
	pbpeers := make([]*Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerInfoToPBPeer(p)
	}
	return pbpeers
}

// PeerInfoToPBPeer converts a single peer.AddrInfo into a *Message_Peer ready to
// go out on the wire, setting the ConnectionType from the given network.Network.
// It is the one-peer form of PeerInfosToPBPeers.
func PeerInfoToPBPeer(n network.Network, p peer.AddrInfo) *Message_Peer {
	pbp := peerInfoToPBPeer(p)
	pbp.Connection = ConnectionType(n.Connectedness(p.ID))
	return pbp
}

// PeerInfosToPBPeers converts given []peer.AddrInfo into a set of []*Message_Peer,
// which can be written to a message and sent out. The key thing this function
// does (in addition to RawPeerInfosToPBPeers) is set the ConnectionType with
// information from the given network.Network.
func PeerInfosToPBPeers(n network.Network, peers []peer.AddrInfo) []*Message_Peer {
	pbps := make([]*Message_Peer, len(peers))
	for i, p := range peers {
		pbps[i] = PeerInfoToPBPeer(n, p)
	}
	return pbps
}

func PeerRoutingInfosToPBPeers(peers []PeerRoutingInfo) []*Message_Peer {
	pbpeers := make([]*Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerRoutingInfoToPBPeer(p)
	}
	return pbpeers
}

// PBPeersToPeerInfos converts given []*Message_Peer into []peer.AddrInfo.
// Invalid addresses will be silently omitted. Each record's address list is
// bounded to MaxPeerRecordSize before conversion — trimming pbps[i].Addrs in
// place — so a peer record keeps a predictable peerstore footprint.
func PBPeersToPeerInfos(pbps []*Message_Peer) []*peer.AddrInfo {
	peers := make([]*peer.AddrInfo, 0, len(pbps))
	for _, pbp := range pbps {
		boundPeerRecordAddrs(pbp)
		ai := PBPeerToPeerInfo(pbp)
		peers = append(peers, &ai)
	}
	return peers
}

// Addresses returns a multiaddr associated with the Message_Peer entry
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			log.Debugw("error decoding multiaddr for peer", "peer", peer.ID(m.Id), "error", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}
	return maddrs
}

// GetClusterLevel gets and adjusts the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) GetClusterLevel() int {
	level := m.GetClusterLevelRaw() - 1
	if level < 0 {
		return 0
	}
	return int(level)
}

// SetClusterLevel adjusts and sets the cluster level on the message.
// a +/- 1 adjustment is needed to distinguish a valid first level (1) and
// default "no value" protobuf behavior (0)
func (m *Message) SetClusterLevel(level int) {
	lvl := int32(level)
	m.ClusterLevelRaw = lvl + 1
}

// ConnectionType returns a Message_ConnectionType associated with the
// network.Connectedness.
func ConnectionType(c network.Connectedness) Message_ConnectionType {
	switch c {
	default:
		return Message_NOT_CONNECTED
	case network.NotConnected:
		return Message_NOT_CONNECTED
	case network.Connected:
		return Message_CONNECTED
	}
}

// Connectedness returns an network.Connectedness associated with the
// Message_ConnectionType.
func Connectedness(c Message_ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case Message_NOT_CONNECTED:
		return network.NotConnected
	case Message_CONNECTED:
		return network.Connected
	}
}
