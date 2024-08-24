package message

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync"
)

// Builder captures components of a message across multiple
// requests for a given peer and then generates the corresponding
// GraphSync message when ready to send
type Builder struct {
	outgoingBlocks     map[cid.Cid]blocks.Block
	blkSize            uint64
	completedResponses map[graphsync.RequestID]graphsync.ResponseStatusCode
	outgoingResponses  map[graphsync.RequestID][]GraphSyncLinkMetadatum
	extensions         map[graphsync.RequestID][]graphsync.ExtensionData
	requests           map[graphsync.RequestID]GraphSyncRequest
}

// NewBuilder generates a new Builder.
func NewBuilder() *Builder {
	return &Builder{
		requests:           make(map[graphsync.RequestID]GraphSyncRequest),
		outgoingBlocks:     make(map[cid.Cid]blocks.Block),
		completedResponses: make(map[graphsync.RequestID]graphsync.ResponseStatusCode),
		outgoingResponses:  make(map[graphsync.RequestID][]GraphSyncLinkMetadatum),
		extensions:         make(map[graphsync.RequestID][]graphsync.ExtensionData),
	}
}

// AddRequest registers a new request to be added to the message.
func (b *Builder) AddRequest(request GraphSyncRequest) {
	b.requests[request.ID()] = request
}

// AddBlock adds the given block to the message.
func (b *Builder) AddBlock(block blocks.Block) {
	b.blkSize += uint64(len(block.RawData()))
	b.outgoingBlocks[block.Cid()] = block
}

// AddExtensionData adds the given extension data to to the message
func (b *Builder) AddExtensionData(requestID graphsync.RequestID, extension graphsync.ExtensionData) {
	b.extensions[requestID] = append(b.extensions[requestID], extension)
	// make sure this extension goes out in next response even if no links are sent
	_, ok := b.outgoingResponses[requestID]
	if !ok {
		b.outgoingResponses[requestID] = nil
	}
}

// BlockSize returns the total size of all blocks in this message
func (b *Builder) BlockSize() uint64 {
	return b.blkSize
}

// AddLink adds the given link and whether its block is present
// to the message for the given request ID.
func (b *Builder) AddLink(requestID graphsync.RequestID, link ipld.Link, linkAction graphsync.LinkAction) {
	b.outgoingResponses[requestID] = append(b.outgoingResponses[requestID], GraphSyncLinkMetadatum{Link: link.(cidlink.Link).Cid, Action: linkAction})
}

// AddResponseCode marks the given request as completed in the message,
// as well as whether the graphsync request responded with complete or partial
// data.
func (b *Builder) AddResponseCode(requestID graphsync.RequestID, status graphsync.ResponseStatusCode) {
	b.completedResponses[requestID] = status
	// make sure this completion goes out in next response even if no links are sent
	_, ok := b.outgoingResponses[requestID]
	if !ok {
		b.outgoingResponses[requestID] = nil
	}
}

// Empty returns true if there is no content to send
func (b *Builder) Empty() bool {
	return len(b.requests) == 0 && len(b.outgoingBlocks) == 0 && len(b.outgoingResponses) == 0
}

// ScrubResponse removes a response from a message and any blocks only referenced by that response
func (b *Builder) ScrubResponses(requestIDs []graphsync.RequestID) uint64 {
	for _, requestID := range requestIDs {
		delete(b.completedResponses, requestID)
		delete(b.extensions, requestID)
		delete(b.outgoingResponses, requestID)
	}
	oldSize := b.blkSize
	newBlkSize := uint64(0)
	savedBlocks := make(map[cid.Cid]blocks.Block, len(b.outgoingBlocks))
	for _, metadata := range b.outgoingResponses {
		for _, item := range metadata {
			block, willSendBlock := b.outgoingBlocks[item.Link]
			_, alreadySavedBlock := savedBlocks[item.Link]
			if item.Action == graphsync.LinkActionPresent && willSendBlock && !alreadySavedBlock {
				savedBlocks[item.Link] = block
				newBlkSize += uint64(len(block.RawData()))
			}
		}
	}
	b.blkSize = newBlkSize
	b.outgoingBlocks = savedBlocks
	return oldSize - newBlkSize
}

// Build assembles and encodes message data from the added requests, links, and blocks.
func (b *Builder) Build() (GraphSyncMessage, error) {
	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(b.outgoingResponses))
	for requestID, linkMap := range b.outgoingResponses {
		status, isComplete := b.completedResponses[requestID]
		responses[requestID] = NewResponse(requestID, responseCode(status, isComplete), linkMap, b.extensions[requestID]...)
	}
	return GraphSyncMessage{
		b.requests, responses, b.outgoingBlocks,
	}, nil
}

func responseCode(status graphsync.ResponseStatusCode, isComplete bool) graphsync.ResponseStatusCode {
	if !isComplete {
		return graphsync.PartialResponse
	}
	return status
}
