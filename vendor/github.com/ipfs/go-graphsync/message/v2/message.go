package v2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/message/ipldbind"
)

// MessageHandler is used to hold per-peer state for each connection. There is
// no state to hold for the v2 protocol, so this exists to provide a consistent
// interface between the protocol versions.
type MessageHandler struct{}

// NewMessageHandler creates a new MessageHandler
func NewMessageHandler() *MessageHandler {
	return &MessageHandler{}
}

// FromNet can read a network stream to deserialized a GraphSyncMessage
func (mh *MessageHandler) FromNet(p peer.ID, r io.Reader) (message.GraphSyncMessage, error) {
	reader := msgio.NewVarintReaderSize(r, network.MessageSizeMax)
	return mh.FromMsgReader(p, reader)
}

// FromMsgReader can deserialize a DAG-CBOR message into a GraphySyncMessage
func (mh *MessageHandler) FromMsgReader(_ peer.ID, r msgio.Reader) (message.GraphSyncMessage, error) {
	msg, err := r.ReadMsg()
	if err != nil {
		return message.GraphSyncMessage{}, err
	}

	builder := ipldbind.Prototype.Message.Representation().NewBuilder()
	err = dagcbor.Decode(builder, bytes.NewReader(msg))
	if err != nil {
		return message.GraphSyncMessage{}, err
	}
	node := builder.Build()
	ipldGSM, err := ipldbind.SafeUnwrap(node)
	if err != nil {
		return message.GraphSyncMessage{}, err
	}
	return mh.fromIPLD(ipldGSM.(*ipldbind.GraphSyncMessageRoot))
}

// ToProto converts a GraphSyncMessage to its ipldbind.GraphSyncMessageRoot equivalent
func (mh *MessageHandler) toIPLD(gsm message.GraphSyncMessage) (*ipldbind.GraphSyncMessageRoot, error) {
	ibm := new(ipldbind.GraphSyncMessage)
	requests := gsm.Requests()
	if len(requests) > 0 {
		ibmRequests := make([]ipldbind.GraphSyncRequest, 0, len(requests))
		for _, request := range requests {
			req := ipldbind.GraphSyncRequest{
				Id:          request.ID().Bytes(),
				RequestType: request.Type(),
				Extensions:  ipldbind.NewGraphSyncExtensions(request),
			}

			root := request.Root()
			if root != cid.Undef {
				req.Root = &root
			}

			selector := request.Selector()
			if selector != nil {
				req.Selector = &selector
			}

			priority := request.Priority()
			if priority != 0 {
				req.Priority = &priority
			}

			ibmRequests = append(ibmRequests, req)
		}
		ibm.Requests = &ibmRequests
	}

	responses := gsm.Responses()
	if len(responses) > 0 {
		ibmResponses := make([]ipldbind.GraphSyncResponse, 0, len(responses))
		for _, response := range responses {
			glsm, ok := response.Metadata().(message.GraphSyncLinkMetadata)
			if !ok {
				return nil, fmt.Errorf("unexpected metadata type")
			}

			res := ipldbind.GraphSyncResponse{
				Id:         response.RequestID().Bytes(),
				Status:     response.Status(),
				Extensions: ipldbind.NewGraphSyncExtensions(response),
			}

			md := glsm.RawMetadata()
			if len(md) > 0 {
				res.Metadata = &md
			}

			ibmResponses = append(ibmResponses, res)
		}
		ibm.Responses = &ibmResponses
	}

	blocks := gsm.Blocks()
	if len(blocks) > 0 {
		ibmBlocks := make([]ipldbind.GraphSyncBlock, 0, len(blocks))
		for _, b := range blocks {
			ibmBlocks = append(ibmBlocks, ipldbind.GraphSyncBlock{
				Data:   b.RawData(),
				Prefix: b.Cid().Prefix().Bytes(),
			})
		}
		ibm.Blocks = &ibmBlocks
	}

	return &ipldbind.GraphSyncMessageRoot{Gs2: ibm}, nil
}

// ToNet writes a GraphSyncMessage in its DAG-CBOR format to a writer,
// prefixed with a length uvar
func (mh *MessageHandler) ToNet(_ peer.ID, gsm message.GraphSyncMessage, w io.Writer) error {
	msg, err := mh.toIPLD(gsm)
	if err != nil {
		return err
	}

	lbuf := make([]byte, binary.MaxVarintLen64)
	buf := new(bytes.Buffer)
	buf.Write(lbuf)

	node, err := ipldbind.SafeWrap(msg, ipldbind.Prototype.Message.Type())
	if err != nil {
		return err
	}
	err = dagcbor.Encode(node.Representation(), buf)
	if err != nil {
		return err
	}

	lbuflen := binary.PutUvarint(lbuf, uint64(buf.Len()-binary.MaxVarintLen64))
	out := buf.Bytes()
	copy(out[binary.MaxVarintLen64-lbuflen:], lbuf[:lbuflen])
	_, err = w.Write(out[binary.MaxVarintLen64-lbuflen:])

	return err
}

// Mapping from a ipldbind.GraphSyncMessageRoot object to a GraphSyncMessage object
func (mh *MessageHandler) fromIPLD(ibm *ipldbind.GraphSyncMessageRoot) (message.GraphSyncMessage, error) {
	if ibm.Gs2 == nil {
		return message.GraphSyncMessage{}, fmt.Errorf("invalid GraphSyncMessageRoot, no inner message")
	}

	var requests map[graphsync.RequestID]message.GraphSyncRequest
	if ibm.Gs2.Requests != nil {
		requests = make(map[graphsync.RequestID]message.GraphSyncRequest, len(*ibm.Gs2.Requests))
		for _, req := range *ibm.Gs2.Requests {
			id, err := graphsync.ParseRequestID(req.Id)
			if err != nil {
				return message.GraphSyncMessage{}, err
			}

			if req.RequestType == graphsync.RequestTypeCancel {
				requests[id] = message.NewCancelRequest(id)
				continue
			}

			var ext []graphsync.ExtensionData
			if req.Extensions != nil {
				ext = req.Extensions.ToExtensionsList()
			}

			if req.RequestType == graphsync.RequestTypeUpdate {
				requests[id] = message.NewUpdateRequest(id, ext...)
				continue
			}

			root := cid.Undef
			if req.Root != nil {
				root = *req.Root
			}

			var selector datamodel.Node
			if req.Selector != nil {
				selector = *req.Selector
			}

			var priority graphsync.Priority
			if req.Priority != nil {
				priority = graphsync.Priority(*req.Priority)
			}

			requests[id] = message.NewRequest(id, root, selector, priority, ext...)
		}
	}

	var responses map[graphsync.RequestID]message.GraphSyncResponse
	if ibm.Gs2.Responses != nil {
		responses = make(map[graphsync.RequestID]message.GraphSyncResponse, len(*ibm.Gs2.Responses))
		for _, res := range *ibm.Gs2.Responses {
			id, err := graphsync.ParseRequestID(res.Id)
			if err != nil {
				return message.GraphSyncMessage{}, err
			}

			var md []message.GraphSyncLinkMetadatum
			if res.Metadata != nil {
				md = *res.Metadata
			}

			var ext []graphsync.ExtensionData
			if res.Extensions != nil {
				ext = res.Extensions.ToExtensionsList()
			}

			responses[id] = message.NewResponse(id, graphsync.ResponseStatusCode(res.Status), md, ext...)
		}
	}

	var blks map[cid.Cid]blocks.Block
	if ibm.Gs2.Blocks != nil {
		blks = make(map[cid.Cid]blocks.Block, len(*ibm.Gs2.Blocks))
		for _, b := range *ibm.Gs2.Blocks {
			pref, err := cid.PrefixFromBytes(b.Prefix)
			if err != nil {
				return message.GraphSyncMessage{}, err
			}

			c, err := pref.Sum(b.Data)
			if err != nil {
				return message.GraphSyncMessage{}, err
			}

			blk, err := blocks.NewBlockWithCid(b.Data, c)
			if err != nil {
				return message.GraphSyncMessage{}, err
			}

			blks[blk.Cid()] = blk
		}
	}

	return message.NewMessage(requests, responses, blks), nil
}
