package p2p

import (
	"context"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
)

// Message represents a pubsub message
type Message struct {
	SenderID string
	Content  string
	msg      *pubsub.Message
}

// GetFrom returns the sender ID
func (m *Message) GetFrom() string {
	return m.SenderID
}

// GetData returns the message content
func (m *Message) GetData() []byte {
	return []byte(m.Content)
}

// outboundBuffer is how many replication messages may be queued for publishing
// before further ones are dropped. Dropping is deliberate: a slow topic must never
// stall a client's write path, which is what an unbuffered send would do.
const outboundBuffer = 256

// PubSub wraps libp2p pubsub with our custom message handling
type PubSub struct {
	*pubsub.PubSub
	Topic    *pubsub.Topic
	Sub      *pubsub.Subscription
	Inbound  chan *Message
	Outbound chan string
}

// JoinPubSub joins a pubsub topic and sets up message handling
func JoinPubSub(p2p *P2P, name string, topicName string) (*PubSub, error) {
	topic, err := p2p.PubSub.Join(topicName)
	if err != nil {
		return nil, err
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}

	ps := &PubSub{
		PubSub:   p2p.PubSub,
		Topic:    topic,
		Sub:      sub,
		Inbound:  make(chan *Message),
		Outbound: make(chan string, outboundBuffer),
	}

	go ps.handleInbound(p2p.Ctx)
	go ps.handleOutbound(p2p.Ctx)

	return ps, nil
}

func (ps *PubSub) handleInbound(ctx context.Context) {
	for {
		msg, err := ps.Sub.Next(ctx)
		if err != nil {
			close(ps.Inbound)
			return
		}
		ps.Inbound <- &Message{
			SenderID: msg.GetFrom().String(),
			Content:  string(msg.GetData()),
			msg:      msg,
		}
	}
}

// Publish queues a payload for the topic without blocking. It reports false when the
// queue is full and the payload was dropped; the caller is expected to count that.
func (ps *PubSub) Publish(payload []byte) bool {
	select {
	case ps.Outbound <- string(payload):
		return true
	default:
		return false
	}
}

func (ps *PubSub) handleOutbound(ctx context.Context) {
	for msg := range ps.Outbound {
		if err := ps.Topic.Publish(ctx, []byte(msg)); err != nil {
			logrus.Errorf("Failed to publish message: %v", err)
		}
	}
}

// Exit closes the subscription and topic. It is safe to call on a node that only got
// part way through joining the topic, which is what a failed startup looks like.
func (ps *PubSub) Exit() {
	if ps.Sub != nil {
		ps.Sub.Cancel()
	}
	if ps.Topic != nil {
		_ = ps.Topic.Close()
	}
}
