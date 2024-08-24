package crdt

import (
	"context"
	"strings"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// PubSubBroadcaster implements a Broadcaster using libp2p PubSub.
type PubSubBroadcaster struct {
	ctx   context.Context
	psub  *pubsub.PubSub
	topic *pubsub.Topic
	subs  *pubsub.Subscription
}

// NewPubSubBroadcaster returns a new broadcaster using the given PubSub and
// a topic to subscribe/broadcast to. The given context can be used to cancel
// the broadcaster.
// Please register any topic validators before creating the Broadcaster.
//
// The broadcaster can be shut down by cancelling the given context.
// This must be done before Closing the crdt.Datastore, otherwise things
// may hang.
func NewPubSubBroadcaster(ctx context.Context, psub *pubsub.PubSub, topic string) (*PubSubBroadcaster, error) {
	psubTopic, err := psub.Join(topic)
	if err != nil {
		return nil, err
	}

	subs, err := psubTopic.Subscribe()
	if err != nil {
		return nil, err
	}

	go func(ctx context.Context, subs *pubsub.Subscription) {
		<-ctx.Done()
		subs.Cancel()
	}(ctx, subs)

	return &PubSubBroadcaster{
		ctx:   ctx,
		psub:  psub,
		topic: psubTopic,
		subs:  subs,
	}, nil
}

// Broadcast publishes some data.
func (pbc *PubSubBroadcaster) Broadcast(data []byte) error {
	return pbc.topic.Publish(pbc.ctx, data)
}

// Next returns published data.
func (pbc *PubSubBroadcaster) Next() ([]byte, error) {
	var msg *pubsub.Message
	var err error

	select {
	case <-pbc.ctx.Done():
		return nil, ErrNoMoreBroadcast
	default:
	}

	msg, err = pbc.subs.Next(pbc.ctx)
	if err != nil {
		if strings.Contains(err.Error(), "subscription cancelled") ||
			strings.Contains(err.Error(), "context") {
			return nil, ErrNoMoreBroadcast
		}
		return nil, err
	}

	return msg.GetData(), nil
}
