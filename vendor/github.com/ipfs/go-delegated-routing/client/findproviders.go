package client

import (
	"context"

	"github.com/ipfs/go-cid"
	proto "github.com/ipfs/go-delegated-routing/gen/proto"
	ipns "github.com/ipfs/go-ipns"
	logging "github.com/ipfs/go-log"
	"github.com/libp2p/go-libp2p-core/peer"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multiaddr"
)

var logger = logging.Logger("service/client/delegatedrouting")

type DelegatedRoutingClient interface {
	FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error)
	FindProvidersAsync(ctx context.Context, key cid.Cid) (<-chan FindProvidersAsyncResult, error)
	GetIPNS(ctx context.Context, id []byte) ([]byte, error)
	GetIPNSAsync(ctx context.Context, id []byte) (<-chan GetIPNSAsyncResult, error)
	PutIPNS(ctx context.Context, id []byte, record []byte) error
	PutIPNSAsync(ctx context.Context, id []byte, record []byte) (<-chan PutIPNSAsyncResult, error)
}

type Client struct {
	client    proto.DelegatedRouting_Client
	validator record.Validator
}

func NewClient(c proto.DelegatedRouting_Client) *Client {
	return &Client{client: c, validator: ipns.Validator{}}
}

func (fp *Client) FindProviders(ctx context.Context, key cid.Cid) ([]peer.AddrInfo, error) {
	resps, err := fp.client.FindProviders(ctx, cidsToFindProvidersRequest(key))
	if err != nil {
		return nil, err
	}
	infos := []peer.AddrInfo{}
	for _, resp := range resps {
		infos = append(infos, parseFindProvidersResponse(resp)...)
	}
	return infos, nil
}

type FindProvidersAsyncResult struct {
	AddrInfo []peer.AddrInfo
	Err      error
}

// FindProvidersAsync processes the stream of raw protocol async results into a stream of parsed results.
// Specifically, FindProvidersAsync converts protocol-level provider descriptions into peer address infos.
func (fp *Client) FindProvidersAsync(ctx context.Context, key cid.Cid) (<-chan FindProvidersAsyncResult, error) {
	protoRespCh, err := fp.client.FindProviders_Async(ctx, cidsToFindProvidersRequest(key))
	if err != nil {
		return nil, err
	}

	parsedRespCh := make(chan FindProvidersAsyncResult, 1)
	go func() {
		defer close(parsedRespCh)
		for {
			select {
			case <-ctx.Done():
				return
			case par, ok := <-protoRespCh:
				if !ok {
					return
				}

				var parsedAsyncResp FindProvidersAsyncResult

				parsedAsyncResp.Err = par.Err
				if par.Resp != nil {
					parsedAsyncResp.AddrInfo = parseFindProvidersResponse(par.Resp)
				}

				select {
				case <-ctx.Done():
					return
				case parsedRespCh <- parsedAsyncResp:
				}

			}
		}
	}()

	return parsedRespCh, nil
}

func cidsToFindProvidersRequest(cid cid.Cid) *proto.FindProvidersRequest {
	return &proto.FindProvidersRequest{
		Key: proto.LinkToAny(cid),
	}
}

func parseFindProvidersResponse(resp *proto.FindProvidersResponse) []peer.AddrInfo {
	infos := []peer.AddrInfo{}
	for _, prov := range resp.Providers {
		if !providerSupportsBitswap(prov.ProviderProto) {
			continue
		}
		infos = append(infos, parseProtoNodeToAddrInfo(prov.ProviderNode)...)
	}
	return infos
}

func providerSupportsBitswap(supported proto.TransferProtocolList) bool {
	for _, p := range supported {
		if p.Bitswap != nil {
			return true
		}
	}
	return false
}

func parseProtoNodeToAddrInfo(n proto.Node) []peer.AddrInfo {
	infos := []peer.AddrInfo{}
	if n.Peer == nil { // ignore non-peer nodes
		return nil
	}
	infos = append(infos, ParseNodeAddresses(n.Peer)...)
	return infos
}

// ParseNodeAddresses parses peer node addresses from the protocol structure Peer.
func ParseNodeAddresses(n *proto.Peer) []peer.AddrInfo {
	peerID := peer.ID(n.ID)
	infos := []peer.AddrInfo{}
	for _, addrBytes := range n.Multiaddresses {
		ma, err := multiaddr.NewMultiaddrBytes(addrBytes)
		if err != nil {
			logger.Infof("cannot parse multiaddress (%v)", err)
			continue
		}
		// drop multiaddrs that end in /p2p/peerID
		_, last := multiaddr.SplitLast(ma)
		if last != nil && last.Protocol().Code == multiaddr.P_P2P {
			logger.Infof("dropping provider multiaddress %v ending in /p2p/peerid", ma)
			continue
		}
		infos = append(infos, peer.AddrInfo{ID: peerID, Addrs: []multiaddr.Multiaddr{ma}})
	}
	return infos
}
