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

// PubSub wraps libp2p pubsub with our custom message handling
type PubSub struct {
	*pubsub.PubSub
	Topic *pubsub.Topic
	Sub   *pubsub.Subscription
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
		Outbound: make(chan string),
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

func (ps *PubSub) handleOutbound(ctx context.Context) {
	for msg := range ps.Outbound {
		if err := ps.Topic.Publish(ctx, []byte(msg)); err != nil {
			logrus.Errorf("Failed to publish message: %v", err)
		}
	}
}

// Exit closes the pubsub channels
func (ps *PubSub) Exit() {
	ps.Sub.Cancel()
	ps.Topic.Close()
}
