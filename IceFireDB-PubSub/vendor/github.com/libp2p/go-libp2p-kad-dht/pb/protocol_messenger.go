package dht_pb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	logging "github.com/ipfs/go-log/v2"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
)

var logger = logging.Logger("dht")

// ProtocolMessenger can be used for sending DHT messages to peers and processing their responses.
// This decouples the wire protocol format from both the DHT protocol implementation and from the implementation of the
// routing.Routing interface.
//
// Note: the ProtocolMessenger's MessageSender still needs to deal with some wire protocol details such as using
// varint-delineated protobufs
type ProtocolMessenger struct {
	m MessageSender
}

type ProtocolMessengerOption func(*ProtocolMessenger) error

// NewProtocolMessenger creates a new ProtocolMessenger that is used for sending DHT messages to peers and processing
// their responses.
func NewProtocolMessenger(msgSender MessageSender, opts ...ProtocolMessengerOption) (*ProtocolMessenger, error) {
	pm := &ProtocolMessenger{
		m: msgSender,
	}

	for _, o := range opts {
		if err := o(pm); err != nil {
			return nil, err
		}
	}

	return pm, nil
}

type MessageSenderWithDisconnect interface {
	MessageSender

	OnDisconnect(context.Context, peer.ID)
}

// MessageSender handles sending wire protocol messages to a given peer
type MessageSender interface {
	// SendRequest sends a peer a message and waits for its response
	SendRequest(ctx context.Context, p peer.ID, pmes *Message) (*Message, error)
	// SendMessage sends a peer a message without waiting on a response
	SendMessage(ctx context.Context, p peer.ID, pmes *Message) error
}

// PutValue asks a peer to store the given key/value pair.
func (pm *ProtocolMessenger) PutValue(ctx context.Context, p peer.ID, rec *recpb.Record) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.PutValue")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("record", rec))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	pmes := NewMessage(Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugw("failed to put value to peer", "to", p, "key", internal.LoggableRecordKeyBytes(rec.Key), "error", err)
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		const errStr = "value not put correctly"
		logger.Infow(errStr, "put-message", pmes, "get-message", rpmes)
		return errors.New(errStr)
	}

	return nil
}

// GetValue asks a peer for the value corresponding to the given key. Also returns the K closest peers to the key
// as described in GetClosestPeers.
func (pm *ProtocolMessenger) GetValue(ctx context.Context, p peer.ID, key string) (record *recpb.Record, closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetValue")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), internal.KeyAsAttribute("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				peers := make([]string, len(closerPeers))
				for i, v := range closerPeers {
					peers[i] = v.String()
				}
				span.SetAttributes(
					attribute.Stringer("record", record),
					attribute.StringSlice("closestPeers", peers),
				)
			}
		}()
	}

	pmes := NewMessage(Message_GET_VALUE, []byte(key), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}

	// Perhaps we were given closer peers
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())

	if rec := respMsg.GetRecord(); rec != nil {
		// Success! We were given the value
		logger.Debug("got value")

		// Check that record matches the one we are looking for (validation of the record does not happen here)
		if !bytes.Equal([]byte(key), rec.GetKey()) {
			logger.Debug("received incorrect record")
			return nil, nil, internal.ErrIncorrectRecord
		}

		return rec, peers, err
	}

	return nil, peers, nil
}

// GetClosestPeers asks a peer to return the K (a DHT-wide parameter) DHT server peers closest in XOR space to the id
// Note: If the peer happens to know another peer whose peerID exactly matches the given id it will return that peer
// even if that peer is not a DHT server node.
func (pm *ProtocolMessenger) GetClosestPeers(ctx context.Context, p peer.ID, id peer.ID) (closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetClosestPeers")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", id))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				peers := make([]string, len(closerPeers))
				for i, v := range closerPeers {
					peers[i] = v.String()
				}
				span.SetAttributes(attribute.StringSlice("peers", peers))
			}
		}()
	}

	pmes := NewMessage(Message_FIND_NODE, []byte(id), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, err
	}
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return peers, nil
}

// PutProvider is deprecated please use [ProtocolMessenger.PutProviderAddrs].
func (pm *ProtocolMessenger) PutProvider(ctx context.Context, p peer.ID, key multihash.Multihash, h host.Host) error {
	return pm.PutProviderAddrs(ctx, p, key, peer.AddrInfo{
		ID:    h.ID(),
		Addrs: h.Addrs(),
	})
}

// PutProviderAddrs asks a peer to store that we are a provider for the given key.
func (pm *ProtocolMessenger) PutProviderAddrs(ctx context.Context, p peer.ID, key multihash.Multihash, self peer.AddrInfo) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.PutProvider")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	// TODO: We may want to limit the type of addresses in our provider records
	// For example, in a WAN-only DHT prohibit sharing non-WAN addresses (e.g. 192.168.0.100)
	if len(self.Addrs) < 1 {
		return fmt.Errorf("no known addresses for self, cannot put provider")
	}

	pmes := NewMessage(Message_ADD_PROVIDER, key, 0)
	pmes.ProviderPeers = RawPeerInfosToPBPeers([]peer.AddrInfo{self})

	return pm.m.SendMessage(ctx, p, pmes)
}

// GetProviders asks a peer for the providers it knows of for a given key. Also returns the K closest peers to the key
// as described in GetClosestPeers.
func (pm *ProtocolMessenger) GetProviders(ctx context.Context, p peer.ID, key multihash.Multihash) (provs []*peer.AddrInfo, closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetProviders")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				provsStr := make([]string, len(provs))
				for i, v := range provs {
					provsStr[i] = v.String()
				}
				closerPeersStr := make([]string, len(provs))
				for i, v := range provs {
					closerPeersStr[i] = v.String()
				}
				span.SetAttributes(attribute.StringSlice("provs", provsStr), attribute.StringSlice("closestPeers", closerPeersStr))
			}
		}()
	}

	pmes := NewMessage(Message_GET_PROVIDERS, key, 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}
	provs = PBPeersToPeerInfos(respMsg.GetProviderPeers())
	closerPeers = PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return provs, closerPeers, nil
}

// Ping sends a ping message to the passed peer and waits for a response.
func (pm *ProtocolMessenger) Ping(ctx context.Context, p peer.ID) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.Ping")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	req := NewMessage(Message_PING, nil, 0)
	resp, err := pm.m.SendRequest(ctx, p, req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	if resp.Type != Message_PING {
		return fmt.Errorf("got unexpected response type: %v", resp.Type)
	}
	return nil
}
