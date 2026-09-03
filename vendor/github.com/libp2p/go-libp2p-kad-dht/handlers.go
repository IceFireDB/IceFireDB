package dht

import (
	"bytes"
	"context"
	"errors"
	"iter"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
)

// dhthandler specifies the signature of functions that handle DHT messages.
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_PING:
		return dht.handlePing
	}

	if dht.valueStore != nil {
		switch t {
		case pb.Message_GET_VALUE:
			return dht.handleGetValue
		case pb.Message_PUT_VALUE:
			return dht.handlePutValue
		}
	}

	if dht.providerStore != nil {
		switch t {
		case pb.Message_ADD_PROVIDER:
			return dht.handleAddProvider
		case pb.Message_GET_PROVIDERS:
			return dht.handleGetProviders
		}
	}

	return nil
}

func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	// first, is there even a key?
	k := pmes.GetKey()
	if len(k) == 0 {
		return nil, errors.New("handleGetValue but no key was provided")
	}

	// setup response
	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// Get does not re-verify the record before serving it: it was validated when
	// stored, and the requester validates whatever we return. The burden of
	// checking a record stays on the requester, sparing the serve path a
	// signature verification per GET_VALUE.
	rec, err := dht.valueStore.Get(ctx, string(k))
	if err != nil {
		return nil, err
	}
	resp.Record = rec

	// Find closest peer on given cluster to desired key and reply with that info
	closestPeers := dht.closestPeersToQuery(pmes, p, dht.bucketSize)
	if len(closestPeers) > 0 {
		closestInfos := peerstore.AddrInfos(dht.peerstore, closestPeers)
		for _, pi := range closestInfos {
			logger.Debugf("handleGetValue returning closest peer: '%s'", pi.ID)
			if len(pi.Addrs) < 1 {
				logger.Warnw("no addresses on peer being sent",
					"local", dht.self,
					"to", p,
					"sending", pi.ID,
				)
			}
		}

		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), closestInfos)
	}

	return resp, nil
}

// stripPeerRecords drops the peer-record-bearing fields of a message. The PING
// and PUT_VALUE handlers echo the request back as their response; a peer could
// stuff CloserPeers or ProviderPeers into such a request, and echoing them would
// re-emit peer records that never passed boundPeerRecordAddrs. Neither response
// carries peer records legitimately, so they are safe to drop.
func stripPeerRecords(pmes *pb.Message) {
	pmes.CloserPeers = nil
	pmes.ProviderPeers = nil
}

// Store a value in this peer local storage
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("handlePutValue but no key was provided")
	}

	rec := pmes.GetRecord()
	if rec == nil {
		logger.Debugw("got nil record from", "from", p)
		return nil, errors.New("nil record")
	}

	if !bytes.Equal(pmes.GetKey(), rec.GetKey()) {
		return nil, errors.New("put key doesn't match record key")
	}

	if err := dht.valueStore.Put(ctx, string(rec.GetKey()), rec); err != nil {
		logger.Infow("failed to store PUT_VALUE record", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
		return nil, err
	}

	stripPeerRecords(pmes)
	return pmes, nil
}

func (dht *IpfsDHT) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	logger.Debugf("%s Responding to ping from %s!\n", dht.self, p)
	stripPeerRecords(pmes)
	return pmes, nil
}

func (dht *IpfsDHT) handleFindPeer(ctx context.Context, from peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())

	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("handleFindPeer with empty key")
	}

	targetPid := peer.ID(pmes.GetKey())
	closest := dht.closestPeersToQuery(pmes, from, dht.bucketSize)

	// Prepend targetPid to the front of the list if not already present.
	// targetPid is always the closest key to itself.
	//
	// Per IPFS Kademlia DHT spec: FIND_PEER has a special exception where the
	// target peer MUST be included in the response (if present in peerstore),
	// even if it is self, the requester, or not a DHT server. This allows peers
	// to discover multiaddresses for any peer, not just DHT servers.
	if len(closest) == 0 || closest[0] != targetPid {
		closest = append([]peer.ID{targetPid}, closest...)
	}

	closestinfos := peerstore.AddrInfos(dht.peerstore, closest)
	// possibly an over-allocation but this array is temporary anyways.
	withAddresses := make([]peer.AddrInfo, 0, len(closestinfos))
	for _, pi := range closestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

// providerPeersField is the protobuf field number of Message.provider_peers
// (see pb/dht.proto); used to size each provider record's framed contribution
// when bounding a GET_PROVIDERS response to network.MessageSizeMax.
const providerPeersField protowire.Number = 9

// providerPeersTagSize is the (constant) serialized size of a provider_peers
// field tag, added once per record alongside its length-prefixed bytes.
var providerPeersTagSize = protowire.SizeTag(providerPeersField)

func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, errors.New("handleGetProviders key size too large")
	} else if len(key) == 0 {
		return nil, errors.New("handleGetProviders key is empty")
	}

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// Add the closest DHT servers we know about first and exactly once, so the
	// provider-record budget below accounts for their serialized size.
	closestPeers := dht.closestPeersToQuery(pmes, p, dht.bucketSize)
	if closestPeers != nil {
		infos := peerstore.AddrInfos(dht.peerstore, closestPeers)
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
	}

	// setup providers
	providers, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return nil, err
	}

	// Append provider records until the next would push the serialized message
	// past the transport's soft maximum. GetProviders returns providers in random
	// order, so when the set doesn't all fit the records we keep are an arbitrary
	// subset that varies between requests rather than a fixed prefix.
	appendFittingProviderPeers(resp, func(yield func(*pb.Message_Peer) bool) {
		for _, provider := range providers {
			pbProv := pb.PeerInfoToPBPeer(dht.host.Network(), peer.AddrInfo{
				ID:    provider.ID,
				Addrs: dht.filterAddrs(provider.Addrs),
			})
			if !yield(pbProv) {
				return
			}
		}
	})

	return resp, nil
}

// appendFittingProviderPeers appends records from recs to resp.ProviderPeers,
// stopping at the first one that would push the serialized message past
// network.MessageSizeMax. A record that does not fit is dropped rather than
// served over-size, so a set that cannot fit at all yields no provider records.
// recs is pulled lazily and the running size tracked incrementally (base plus
// each record's framed contribution), so the append is O(n) and pulls nothing
// past the cap.
func appendFittingProviderPeers(resp *pb.Message, recs iter.Seq[*pb.Message_Peer]) {
	size := proto.Size(resp) // key, type and CloserPeers already counted
	for rec := range recs {
		// A repeated embedded message adds its tag, a length prefix and its bytes.
		size += providerPeersTagSize + protowire.SizeBytes(proto.Size(rec))
		if size > network.MessageSizeMax {
			return
		}
		resp.ProviderPeers = append(resp.ProviderPeers, rec)
	}
}

func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, errors.New("handleAddProvider key size too large")
	} else if len(key) == 0 {
		return nil, errors.New("handleAddProvider key is empty")
	}

	logger.Debugw("adding provider", "from", p, "key", internal.LoggableProviderRecordBytes(key))

	// add provider should use the address given in the message
	pinfos := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
	success := false
	for _, pi := range pinfos {
		if pi.ID != p {
			// we should ignore this provider record! not from originator.
			// (we should sign them and check signature later...)
			logger.Debugw("received provider from wrong peer", "from", p, "peer", pi.ID)
			continue
		}

		if len(pi.Addrs) < 1 {
			logger.Debugw("no valid addresses for provider", "from", p)
			continue
		}

		// We run the addrs filter after checking for the length, this allows
		// transient nodes with varying /p2p-circuit addresses to still have their
		// announcement go through.
		addrs := dht.filterAddrs(pi.Addrs)
		if err := dht.providerStore.AddProvider(ctx, key, peer.AddrInfo{ID: pi.ID, Addrs: addrs}); err != nil {
			logger.Warnw("failed to store provider record", "from", p, "key", internal.LoggableProviderRecordBytes(key), "error", err)
			continue
		}
		success = true
	}
	if !success {
		return nil, errors.New("handleAddProvider no valid provider")
	}

	return nil, nil
}
