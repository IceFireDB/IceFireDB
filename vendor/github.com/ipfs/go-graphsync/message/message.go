package message

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-msgio"

	"github.com/ipfs/go-graphsync"
)

// MessageHandler provides a consistent interface for maintaining per-peer state
// within the differnet protocol versions
type MessageHandler interface {
	FromNet(peer.ID, io.Reader) (GraphSyncMessage, error)
	FromMsgReader(peer.ID, msgio.Reader) (GraphSyncMessage, error)
	ToNet(peer.ID, GraphSyncMessage, io.Writer) error
}

// MessagePartWithExtensions is an interface for accessing metadata on both
// requests and responses, which have a consistent extension accessor mechanism
type MessagePartWithExtensions interface {
	ExtensionNames() []graphsync.ExtensionName
	Extension(name graphsync.ExtensionName) (datamodel.Node, bool)
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	root        cid.Cid
	selector    ipld.Node
	priority    graphsync.Priority
	id          graphsync.RequestID
	extensions  map[string]datamodel.Node
	requestType graphsync.RequestType
}

// String returns a human-readable form of a GraphSyncRequest
func (gsr GraphSyncRequest) String() string {
	sel := "nil"
	if gsr.selector != nil {
		var buf bytes.Buffer
		dagjson.Encode(gsr.selector, &buf)
		sel = buf.String()
	}
	extStr := strings.Builder{}
	for _, name := range gsr.ExtensionNames() {
		extStr.WriteString(string(name))
		extStr.WriteString("|")
	}
	return fmt.Sprintf("GraphSyncRequest<root=%s, selector=%s, priority=%d, id=%s, type=%v, exts=%s>",
		gsr.root.String(),
		sel,
		gsr.priority,
		gsr.id.String(),
		gsr.requestType,
		extStr.String(),
	)
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	requestID  graphsync.RequestID
	status     graphsync.ResponseStatusCode
	metadata   []GraphSyncLinkMetadatum
	extensions map[string]datamodel.Node
}

// GraphSyncLinkMetadatum is used for holding individual pieces of metadata,
// this is not intended for public consumption and is used within
// GraphSyncLinkMetadata to contain the metadata
type GraphSyncLinkMetadatum struct {
	Link   cid.Cid
	Action graphsync.LinkAction
}

// GraphSyncLinkMetadata is a graphsync.LinkMetadata compatible type that is
// used for holding and accessing the metadata for a request
type GraphSyncLinkMetadata struct {
	linkMetadata []GraphSyncLinkMetadatum
}

// String returns a human-readable form of a GraphSyncResponse
func (gsr GraphSyncResponse) String() string {
	extStr := strings.Builder{}
	for _, name := range gsr.ExtensionNames() {
		extStr.WriteString(string(name))
		extStr.WriteString("|")
	}
	return fmt.Sprintf("GraphSyncResponse<id=%s, status=%d, exts=%s>",
		gsr.requestID.String(),
		gsr.status,
		extStr.String(),
	)
}

// GraphSyncMessage is the internal representation form of a message sent or
// received over the wire
type GraphSyncMessage struct {
	requests  map[graphsync.RequestID]GraphSyncRequest
	responses map[graphsync.RequestID]GraphSyncResponse
	blocks    map[cid.Cid]blocks.Block
}

// NewMessage generates a new message containing the provided requests,
// responses and blocks
func NewMessage(
	requests map[graphsync.RequestID]GraphSyncRequest,
	responses map[graphsync.RequestID]GraphSyncResponse,
	blocks map[cid.Cid]blocks.Block,
) GraphSyncMessage {
	return GraphSyncMessage{requests, responses, blocks}
}

// String returns a human-readable (multi-line) form of a GraphSyncMessage and
// its contents
func (gsm GraphSyncMessage) String() string {
	cts := make([]string, 0)
	for _, req := range gsm.requests {
		cts = append(cts, req.String())
	}
	for _, resp := range gsm.responses {
		cts = append(cts, resp.String())
	}
	for _, c := range gsm.blocks {
		cts = append(cts, fmt.Sprintf("Block<%s>", c.String()))
	}
	return fmt.Sprintf("GraphSyncMessage<\n\t%s\n>", strings.Join(cts, ",\n\t"))
}

// NewRequest builds a new GraphSyncRequest
func NewRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	extensions ...graphsync.ExtensionData) GraphSyncRequest {

	return newRequest(id, root, selector, priority, graphsync.RequestTypeNew, toExtensionsMap(extensions))
}

// NewCancelRequest request generates a request to cancel an in progress request
func NewCancelRequest(id graphsync.RequestID) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, graphsync.RequestTypeCancel, nil)
}

// NewUpdateRequest generates a new request to update an in progress request with the given extensions
func NewUpdateRequest(id graphsync.RequestID, extensions ...graphsync.ExtensionData) GraphSyncRequest {
	return newRequest(id, cid.Cid{}, nil, 0, graphsync.RequestTypeUpdate, toExtensionsMap(extensions))
}

// NewLinkMetadata generates a new graphsync.LinkMetadata compatible object,
// used for accessing the metadata in a message
func NewLinkMetadata(md []GraphSyncLinkMetadatum) GraphSyncLinkMetadata {
	return GraphSyncLinkMetadata{md}
}

func toExtensionsMap(extensions []graphsync.ExtensionData) (extensionsMap map[string]datamodel.Node) {
	if len(extensions) > 0 {
		extensionsMap = make(map[string]datamodel.Node, len(extensions))
		for _, extension := range extensions {
			extensionsMap[string(extension.Name)] = extension.Data
		}
	}
	return
}

func newRequest(id graphsync.RequestID,
	root cid.Cid,
	selector ipld.Node,
	priority graphsync.Priority,
	requestType graphsync.RequestType,
	extensions map[string]datamodel.Node) GraphSyncRequest {

	return GraphSyncRequest{
		id:          id,
		root:        root,
		selector:    selector,
		priority:    priority,
		requestType: requestType,
		extensions:  extensions,
	}
}

// NewResponse builds a new Graphsync response
func NewResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode,
	md []GraphSyncLinkMetadatum,
	extensions ...graphsync.ExtensionData) GraphSyncResponse {

	return newResponse(requestID, status, md, toExtensionsMap(extensions))
}

func newResponse(requestID graphsync.RequestID,
	status graphsync.ResponseStatusCode,
	responseMetadata []GraphSyncLinkMetadatum,
	extensions map[string]datamodel.Node) GraphSyncResponse {

	return GraphSyncResponse{
		requestID:  requestID,
		status:     status,
		metadata:   responseMetadata,
		extensions: extensions,
	}
}

// Empty returns true if this message contains no meaningful content: requests,
// responses, or blocks
func (gsm GraphSyncMessage) Empty() bool {
	return len(gsm.blocks) == 0 && len(gsm.requests) == 0 && len(gsm.responses) == 0
}

// Requests provides a copy of the requests in this message
func (gsm GraphSyncMessage) Requests() []GraphSyncRequest {
	requests := make([]GraphSyncRequest, 0, len(gsm.requests))
	for _, request := range gsm.requests {
		requests = append(requests, request)
	}
	return requests
}

// ResponseCodes returns a list of ResponseStatusCodes contained in the
// responses in this GraphSyncMessage
func (gsm GraphSyncMessage) ResponseCodes() map[graphsync.RequestID]graphsync.ResponseStatusCode {
	codes := make(map[graphsync.RequestID]graphsync.ResponseStatusCode, len(gsm.responses))
	for id, response := range gsm.responses {
		codes[id] = response.Status()
	}
	return codes
}

// Responses provides a copy of the responses in this message
func (gsm GraphSyncMessage) Responses() []GraphSyncResponse {
	responses := make([]GraphSyncResponse, 0, len(gsm.responses))
	for _, response := range gsm.responses {
		responses = append(responses, response)
	}
	return responses
}

// Blocks provides a copy of all of the blocks in this message
func (gsm GraphSyncMessage) Blocks() []blocks.Block {
	bs := make([]blocks.Block, 0, len(gsm.blocks))
	for _, block := range gsm.blocks {
		bs = append(bs, block)
	}
	return bs
}

// Clone returns a shallow copy of this GraphSyncMessage
func (gsm GraphSyncMessage) Clone() GraphSyncMessage {
	requests := make(map[graphsync.RequestID]GraphSyncRequest, len(gsm.requests))
	for id, request := range gsm.requests {
		requests[id] = request
	}
	responses := make(map[graphsync.RequestID]GraphSyncResponse, len(gsm.responses))
	for id, response := range gsm.responses {
		responses[id] = response
	}
	blocks := make(map[cid.Cid]blocks.Block, len(gsm.blocks))
	for cid, block := range gsm.blocks {
		blocks[cid] = block
	}
	return GraphSyncMessage{requests, responses, blocks}
}

// ID Returns the request ID for this Request
func (gsr GraphSyncRequest) ID() graphsync.RequestID { return gsr.id }

// Root returns the CID to the root block of this request
func (gsr GraphSyncRequest) Root() cid.Cid { return gsr.root }

// Selector returns the byte representation of the selector for this request
func (gsr GraphSyncRequest) Selector() ipld.Node { return gsr.selector }

// Priority returns the priority of this request
func (gsr GraphSyncRequest) Priority() graphsync.Priority { return gsr.priority }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncRequest) Extension(name graphsync.ExtensionName) (datamodel.Node, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncRequest) ExtensionNames() []graphsync.ExtensionName {
	var extNames []graphsync.ExtensionName
	for ext := range gsr.extensions {
		extNames = append(extNames, graphsync.ExtensionName(ext))
	}
	return extNames
}

// RequestType returns the type of this request (new, cancel, update, etc.)
func (gsr GraphSyncRequest) Type() graphsync.RequestType { return gsr.requestType }

// RequestID returns the request ID for this response
func (gsr GraphSyncResponse) RequestID() graphsync.RequestID { return gsr.requestID }

// Status returns the status for a response
func (gsr GraphSyncResponse) Status() graphsync.ResponseStatusCode { return gsr.status }

// Extension returns the content for an extension on a response, or errors
// if extension is not present
func (gsr GraphSyncResponse) Extension(name graphsync.ExtensionName) (datamodel.Node, bool) {
	if gsr.extensions == nil {
		return nil, false
	}
	val, ok := gsr.extensions[string(name)]
	if !ok {
		return nil, false
	}
	return val, true
}

// ExtensionNames returns the names of the extensions included in this request
func (gsr GraphSyncResponse) ExtensionNames() []graphsync.ExtensionName {
	var extNames []graphsync.ExtensionName
	for ext := range gsr.extensions {
		extNames = append(extNames, graphsync.ExtensionName(ext))
	}
	return extNames
}

// Metadata returns an instance of a graphsync.LinkMetadata for accessing the
// individual metadatum via an iterator
func (gsr GraphSyncResponse) Metadata() graphsync.LinkMetadata {
	return GraphSyncLinkMetadata{gsr.metadata}
}

// Iterate over the metadata one by one via a graphsync.LinkMetadataIterator
// callback function
func (gslm GraphSyncLinkMetadata) Iterate(iter graphsync.LinkMetadataIterator) {
	for _, md := range gslm.linkMetadata {
		iter(md.Link, md.Action)
	}
}

// Length returns the number of metadata entries
func (gslm GraphSyncLinkMetadata) Length() int64 {
	return int64(len(gslm.linkMetadata))
}

// RawMetadata accesses the raw GraphSyncLinkMetadatum contained in this object,
// this is not exposed via the graphsync.LinkMetadata API and in general the
// Iterate() method should be used instead for accessing the individual metadata
func (gslm GraphSyncLinkMetadata) RawMetadata() []GraphSyncLinkMetadatum {
	return gslm.linkMetadata
}

// ReplaceExtensions merges the extensions given extensions into the request to create a new request,
// but always uses new data
func (gsr GraphSyncRequest) ReplaceExtensions(extensions []graphsync.ExtensionData) GraphSyncRequest {
	req, _ := gsr.MergeExtensions(extensions, func(name graphsync.ExtensionName, oldData datamodel.Node, newData datamodel.Node) (datamodel.Node, error) {
		return newData, nil
	})
	return req
}

// MergeExtensions merges the given list of extensions to produce a new request with the combination of the old request
// plus the new extensions. When an old extension and a new extension are both present, mergeFunc is called to produce
// the result
func (gsr GraphSyncRequest) MergeExtensions(extensions []graphsync.ExtensionData, mergeFunc func(name graphsync.ExtensionName, oldData datamodel.Node, newData datamodel.Node) (datamodel.Node, error)) (GraphSyncRequest, error) {
	if gsr.extensions == nil {
		return newRequest(gsr.id, gsr.root, gsr.selector, gsr.priority, gsr.requestType, toExtensionsMap(extensions)), nil
	}
	newExtensionMap := toExtensionsMap(extensions)
	combinedExtensions := make(map[string]datamodel.Node)
	for name, newData := range newExtensionMap {
		oldData, ok := gsr.extensions[name]
		if !ok {
			combinedExtensions[name] = newData
			continue
		}
		resultData, err := mergeFunc(graphsync.ExtensionName(name), oldData, newData)
		if err != nil {
			return GraphSyncRequest{}, err
		}
		combinedExtensions[name] = resultData
	}

	for name, oldData := range gsr.extensions {
		_, ok := combinedExtensions[name]
		if ok {
			continue
		}
		combinedExtensions[name] = oldData
	}
	return newRequest(gsr.id, gsr.root, gsr.selector, gsr.priority, gsr.requestType, combinedExtensions), nil
}
