package v1

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"
	"google.golang.org/protobuf/proto"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/ipldutil"
	"github.com/ipfs/go-graphsync/message"
	pb "github.com/ipfs/go-graphsync/message/pb"
	"github.com/ipfs/go-graphsync/message/v1/metadata"
)

const extensionMetadata = string("graphsync/response-metadata")

type v1RequestKey struct {
	p  peer.ID
	id int32
}

// MessageHandler is used to hold per-peer state for each connection. For the v1
// protocol, we need to maintain a mapping of old style integer RequestIDs and
// the newer UUID forms. This happens on a per-peer basis and needs to work
// for both incoming and outgoing messages.
type MessageHandler struct {
	mapLock sync.Mutex
	// each host can have multiple peerIDs, so our integer requestID mapping for
	// protocol v1.0.0 needs to be a combo of peerID and requestID
	fromV1Map map[v1RequestKey]graphsync.RequestID
	toV1Map   map[graphsync.RequestID]int32
	nextIntId int32
}

// NewMessageHandler instantiates a new MessageHandler instance
func NewMessageHandler() *MessageHandler {
	return &MessageHandler{
		fromV1Map: make(map[v1RequestKey]graphsync.RequestID),
		toV1Map:   make(map[graphsync.RequestID]int32),
	}
}

// FromNet can read a v1.0.0 network stream to deserialized a GraphSyncMessage
func (mh *MessageHandler) FromNet(p peer.ID, r io.Reader) (message.GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(p, reader)
}

// FromMsgReader can deserialize a v1.0.0 protobuf message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReader(p peer.ID, r msgio.Reader) (message.GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return message.GraphSyncMessage{}, err
	}

	var pb pb.Message
	err = proto.Unmarshal(msg, &pb)
	r.ReleaseMsg(msg)
	if err != nil {
		return message.GraphSyncMessage{}, err
	}

	return mh.fromProto(p, &pb)
}

// ToNet writes a GraphSyncMessage in its v1.0.0 protobuf format to a writer
func (mh *MessageHandler) ToNet(p peer.ID, gsm message.GraphSyncMessage, w io.Writer) error {
	msg, err := mh.ToProto(p, gsm)
	if err != nil {
		return err
	}
	size := proto.Size(msg)
	buf := pool.Get(size + binary.MaxVarintLen64)
	defer pool.Put(buf)

	n := binary.PutUvarint(buf, uint64(size))

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:n], msg)
	if err != nil {
		return err
	}
	_, err = w.Write(out)
	return err
}

// toProto converts a GraphSyncMessage to its pb.Message equivalent
func (mh *MessageHandler) ToProto(p peer.ID, gsm message.GraphSyncMessage) (*pb.Message, error) {
	mh.mapLock.Lock()
	defer mh.mapLock.Unlock()

	pbm := new(pb.Message)
	requests := gsm.Requests()
	pbm.Requests = make([]*pb.Message_Request, 0, len(requests))
	for _, request := range requests {
		var selector []byte
		var err error
		if request.Selector() != nil {
			selector, err = ipldutil.EncodeNode(request.Selector())
			if err != nil {
				return nil, err
			}
		}
		rid, err := bytesIdToInt(p, mh.fromV1Map, mh.toV1Map, &mh.nextIntId, request.ID().Bytes())
		if err != nil {
			return nil, err
		}
		ext, err := toEncodedExtensions(request, nil)
		if err != nil {
			return nil, err
		}

		pbm.Requests = append(pbm.Requests, &pb.Message_Request{
			Id:         rid,
			Root:       request.Root().Bytes(),
			Selector:   selector,
			Priority:   int32(request.Priority()),
			Cancel:     request.Type() == graphsync.RequestTypeCancel,
			Update:     request.Type() == graphsync.RequestTypeUpdate,
			Extensions: ext,
		})
	}

	responses := gsm.Responses()
	pbm.Responses = make([]*pb.Message_Response, 0, len(responses))
	for _, response := range responses {
		rid, err := bytesIdToInt(p, mh.fromV1Map, mh.toV1Map, &mh.nextIntId, response.RequestID().Bytes())
		if err != nil {
			return nil, err
		}
		ext, err := toEncodedExtensions(response, response.Metadata())
		if err != nil {
			return nil, err
		}
		pbm.Responses = append(pbm.Responses, &pb.Message_Response{
			Id:         rid,
			Status:     int32(response.Status()),
			Extensions: ext,
		})
	}

	blocks := gsm.Blocks()
	pbm.Data = make([]*pb.Message_Block, 0, len(blocks))
	for _, b := range blocks {
		pbm.Data = append(pbm.Data, &pb.Message_Block{
			Prefix: b.Cid().Prefix().Bytes(),
			Data:   b.RawData(),
		})
	}
	return pbm, nil
}

// Mapping from a pb.Message object to a GraphSyncMessage object, including
// RequestID (int / uuid) mapping.
func (mh *MessageHandler) fromProto(p peer.ID, pbm *pb.Message) (message.GraphSyncMessage, error) {
	mh.mapLock.Lock()
	defer mh.mapLock.Unlock()

	requests := make(map[graphsync.RequestID]message.GraphSyncRequest, len(pbm.GetRequests()))
	for _, req := range pbm.Requests {
		if req == nil {
			return message.GraphSyncMessage{}, errors.New("request is nil")
		}
		var err error

		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, req.Id)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		if req.Cancel {
			requests[id] = message.NewCancelRequest(id)
			continue
		}

		exts, metadata, err := fromEncodedExtensions(req.GetExtensions())
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		if metadata != nil {
			return message.GraphSyncMessage{}, fmt.Errorf("received unexpected metadata in request extensions for request id: %s", id)
		}

		if req.Update {
			requests[id] = message.NewUpdateRequest(id, exts...)
			continue
		}

		root, err := cid.Cast(req.Root)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		selector, err := ipldutil.DecodeNode(req.Selector)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		requests[id] = message.NewRequest(id, root, selector, graphsync.Priority(req.Priority), exts...)
	}

	responses := make(map[graphsync.RequestID]message.GraphSyncResponse, len(pbm.GetResponses()))
	for _, res := range pbm.Responses {
		if res == nil {
			return message.GraphSyncMessage{}, errors.New("response is nil")
		}
		id, err := intIdToRequestId(p, mh.fromV1Map, mh.toV1Map, res.Id)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}
		exts, metadata, err := fromEncodedExtensions(res.GetExtensions())
		if err != nil {
			return message.GraphSyncMessage{}, err
		}
		responses[id] = message.NewResponse(id, graphsync.ResponseStatusCode(res.Status), metadata, exts...)
	}

	blks := make(map[cid.Cid]blocks.Block, len(pbm.GetData()))
	for _, b := range pbm.GetData() {
		if b == nil {
			return message.GraphSyncMessage{}, errors.New("block is nil")
		}

		pref, err := cid.PrefixFromBytes(b.GetPrefix())
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		c, err := pref.Sum(b.GetData())
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		blk, err := blocks.NewBlockWithCid(b.GetData(), c)
		if err != nil {
			return message.GraphSyncMessage{}, err
		}

		blks[blk.Cid()] = blk
	}

	return message.NewMessage(requests, responses, blks), nil
}

// Note that even for protocol v1 we now only support DAG-CBOR encoded extension data.
// Anything else will be rejected with an error.
func toEncodedExtensions(part message.MessagePartWithExtensions, linkMetadata graphsync.LinkMetadata) (map[string][]byte, error) {
	names := part.ExtensionNames()
	out := make(map[string][]byte, len(names))
	for _, name := range names {
		data, ok := part.Extension(graphsync.ExtensionName(name))
		if !ok || data == nil {
			out[string(name)] = nil
		} else {
			byts, err := ipldutil.EncodeNode(data)
			if err != nil {
				return nil, err
			}
			out[string(name)] = byts
		}
	}
	if linkMetadata != nil {
		md := make(metadata.Metadata, 0)
		linkMetadata.Iterate(func(c cid.Cid, la graphsync.LinkAction) {
			md = append(md, metadata.Item{Link: c, BlockPresent: la == graphsync.LinkActionPresent})
		})
		mdNode, err := metadata.EncodeMetadata(md)
		if err != nil {
			return nil, err
		}
		mdByts, err := ipldutil.EncodeNode(mdNode)
		if err != nil {
			return nil, err
		}
		out[extensionMetadata] = mdByts
	}
	return out, nil
}

func fromEncodedExtensions(in map[string][]byte) ([]graphsync.ExtensionData, []message.GraphSyncLinkMetadatum, error) {
	if in == nil {
		return []graphsync.ExtensionData{}, nil, nil
	}
	out := make([]graphsync.ExtensionData, 0, len(in))
	var md []message.GraphSyncLinkMetadatum
	for name, data := range in {
		var node datamodel.Node
		var err error
		if len(data) > 0 {
			node, err = ipldutil.DecodeNode(data)
			if err != nil {
				return nil, nil, err
			}
			if name == string(extensionMetadata) {
				mdd, err := metadata.DecodeMetadata(node)
				if err != nil {
					return nil, nil, err
				}
				md = mdd.ToGraphSyncMetadata()
			} else {
				out = append(out, graphsync.ExtensionData{Name: graphsync.ExtensionName(name), Data: node})
			}
		}
	}
	return out, md, nil
}

// Maps a []byte slice form of a RequestID (uuid) to an integer format as used
// by a v1 peer. Inverse of intIdToRequestId()
func bytesIdToInt(p peer.ID, fromV1Map map[v1RequestKey]graphsync.RequestID, toV1Map map[graphsync.RequestID]int32, nextIntId *int32, id []byte) (int32, error) {
	rid, err := graphsync.ParseRequestID(id)
	if err != nil {
		return 0, err
	}
	iid, ok := toV1Map[rid]
	if !ok {
		iid = *nextIntId
		*nextIntId++
		toV1Map[rid] = iid
		fromV1Map[v1RequestKey{p, iid}] = rid
	}
	return iid, nil
}

// Maps an integer form of a RequestID as used by a v1 peer to a native (uuid) form.
// Inverse of bytesIdToInt().
func intIdToRequestId(p peer.ID, fromV1Map map[v1RequestKey]graphsync.RequestID, toV1Map map[graphsync.RequestID]int32, iid int32) (graphsync.RequestID, error) {
	key := v1RequestKey{p, iid}
	rid, ok := fromV1Map[key]
	if !ok {
		rid = graphsync.NewRequestID()
		fromV1Map[key] = rid
		toV1Map[rid] = iid
	}
	return rid, nil
}
