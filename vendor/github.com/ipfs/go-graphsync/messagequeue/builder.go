package messagequeue

import (
	"context"
	"io"

	"github.com/ipfs/go-graphsync"
	gsmsg "github.com/ipfs/go-graphsync/message"
	"github.com/ipfs/go-graphsync/notifications"
)

// Builder wraps a message builder with additional functions related to metadata
// and notifications in the message queue
type Builder struct {
	ctx context.Context
	*gsmsg.Builder
	topic           Topic
	responseStreams map[graphsync.RequestID]io.Closer
	subscribers     map[graphsync.RequestID]notifications.Subscriber
	blockData       map[graphsync.RequestID][]graphsync.BlockData
}

// NewBuilder sets up a new builder for the given topic
func NewBuilder(ctx context.Context, topic Topic) *Builder {
	return &Builder{
		ctx:             ctx,
		Builder:         gsmsg.NewBuilder(),
		topic:           topic,
		responseStreams: make(map[graphsync.RequestID]io.Closer),
		subscribers:     make(map[graphsync.RequestID]notifications.Subscriber),
		blockData:       make(map[graphsync.RequestID][]graphsync.BlockData),
	}
}

func (b *Builder) Context() context.Context {
	return b.ctx
}

// SetResponseStream sets the given response stream to close should the message fail to send
func (b *Builder) SetResponseStream(requestID graphsync.RequestID, stream io.Closer) {
	b.responseStreams[requestID] = stream
}

// SetSubscriber sets the given subscriber to get notified as events occur for this message
func (b *Builder) SetSubscriber(requestID graphsync.RequestID, subscriber notifications.Subscriber) {
	b.subscribers[requestID] = subscriber
}

// AddBlockData add the given block metadata for this message to pass into notifications
func (b *Builder) AddBlockData(requestID graphsync.RequestID, blockData graphsync.BlockData) {
	b.blockData[requestID] = append(b.blockData[requestID], blockData)
}

// ScrubResponse removes the given responses from the message and metadata
func (b *Builder) ScrubResponses(requestIDs []graphsync.RequestID) uint64 {
	for _, requestID := range requestIDs {
		delete(b.responseStreams, requestID)
		delete(b.subscribers, requestID)
		delete(b.blockData, requestID)
	}
	return b.Builder.ScrubResponses(requestIDs)
}

// ResponseStreams inspect current response stream state
func (b *Builder) ResponseStreams() map[graphsync.RequestID]io.Closer {
	return b.responseStreams
}

// Subscribers inspect current subscribers
func (b *Builder) Subscribers() map[graphsync.RequestID]notifications.Subscriber {
	return b.subscribers
}

// BlockData inspects current block data
func (b *Builder) BlockData() map[graphsync.RequestID][]graphsync.BlockData {
	return b.blockData
}

func (b *Builder) build(publisher notifications.Publisher) (gsmsg.GraphSyncMessage, internalMetadata, error) {
	message, err := b.Build()
	if err != nil {
		return gsmsg.GraphSyncMessage{}, internalMetadata{}, err
	}
	for _, subscriber := range b.subscribers {
		publisher.Subscribe(b.topic, subscriber)
	}
	return message, internalMetadata{
		public: Metadata{
			BlockData:     b.blockData,
			ResponseCodes: message.ResponseCodes(),
		},
		ctx:             b.ctx,
		topic:           b.topic,
		msgSize:         b.BlockSize(),
		responseStreams: b.responseStreams,
	}, nil
}
