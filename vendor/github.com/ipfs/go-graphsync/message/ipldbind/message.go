package ipldbind

import (
	cid "github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"

	"github.com/ipfs/go-graphsync"
	"github.com/ipfs/go-graphsync/message"
)

// GraphSyncExtensions is a container for representing extension data for
// bindnode, it's converted to a graphsync.ExtensionData list by
// ToExtensionsList()
type GraphSyncExtensions struct {
	Keys   []string
	Values map[string]*datamodel.Node
}

// NewGraphSyncExtensions creates GraphSyncExtensions from either a request or
// response object
func NewGraphSyncExtensions(part message.MessagePartWithExtensions) *GraphSyncExtensions {
	names := part.ExtensionNames()
	if len(names) == 0 {
		return nil
	}
	keys := make([]string, 0, len(names))
	values := make(map[string]*datamodel.Node, len(names))
	for _, name := range names {
		keys = append(keys, string(name))
		data, _ := part.Extension(graphsync.ExtensionName(name))
		if data == nil {
			values[string(name)] = nil
		} else {
			values[string(name)] = &data
		}
	}
	return &GraphSyncExtensions{keys, values}
}

// ToExtensionsList creates a list of graphsync.ExtensionData objects from a
// GraphSyncExtensions
func (gse GraphSyncExtensions) ToExtensionsList() []graphsync.ExtensionData {
	exts := make([]graphsync.ExtensionData, 0, len(gse.Values))
	for name, data := range gse.Values {
		if data == nil {
			exts = append(exts, graphsync.ExtensionData{Name: graphsync.ExtensionName(name), Data: nil})
		} else {
			exts = append(exts, graphsync.ExtensionData{Name: graphsync.ExtensionName(name), Data: *data})
		}
	}
	return exts
}

// GraphSyncRequest is a struct to capture data on a request contained in a
// GraphSyncMessage.
type GraphSyncRequest struct {
	Id          []byte
	RequestType graphsync.RequestType
	Priority    *graphsync.Priority
	Root        *cid.Cid
	Selector    *datamodel.Node
	Extensions  *GraphSyncExtensions
}

// GraphSyncResponse is an struct to capture data on a response sent back
// in a GraphSyncMessage.
type GraphSyncResponse struct {
	Id         []byte
	Status     graphsync.ResponseStatusCode
	Metadata   *[]message.GraphSyncLinkMetadatum
	Extensions *GraphSyncExtensions
}

// GraphSyncBlock is a container for representing extension data for bindnode,
// it's converted to a block.Block by the message translation layer
type GraphSyncBlock struct {
	Prefix []byte
	Data   []byte
}

// GraphSyncMessage is a container for representing extension data for bindnode,
// it's converted to a message.GraphSyncMessage by the message translation layer
type GraphSyncMessage struct {
	Requests  *[]GraphSyncRequest
	Responses *[]GraphSyncResponse
	Blocks    *[]GraphSyncBlock
}

type GraphSyncMessageRoot struct {
	Gs2 *GraphSyncMessage
}

// NamedExtension exists just for the purpose of the constructors
type NamedExtension struct {
	Name graphsync.ExtensionName
	Data datamodel.Node
}
