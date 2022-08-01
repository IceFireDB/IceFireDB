package p2p

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

// Represents the default fallback room and user names
// if they aren't provided when the app is started
const defaultclient = "client"
const defaulttopic = "pubsub"

// A structure that represents a PubSub Chat Room
type PubSub struct {
	// Represents the P2P Host for the PubSub
	Host *P2P

	// Represents the channel of incoming messages
	Inbound chan chatmessage
	// Represents the channel of outgoing messages
	Outbound chan string
	// Represents the channel of chat log messages
	Logs chan chatlog

	// Represents the client of the chat room
	ClientName string
	// Represent the topic of the user in the chat room
	TopicName string
	// Represents the host ID of the peer
	selfid peer.ID

	// Represents the chat room lifecycle context
	psctx context.Context
	// Represents the chat room lifecycle cancellation function
	pscancel context.CancelFunc
	// Represents the PubSub Topic of the PubSub
	pstopic *pubsub.Topic
	// Represents the PubSub Subscription for the topic
	psub *pubsub.Subscription
}

// A structure that represents a chat message
type chatmessage struct {
	Message    string `json:"message"`
	SenderID   string `json:"senderid"`
	SenderName string `json:"sendername"`
}

// A structure that represents a chat log
type chatlog struct {
	logprefix string
	logmsg    string
}

func (c chatlog) String() string {
	return fmt.Sprintf("%s: %s", c.logprefix, c.logmsg)
}

// A constructor function that generates and returns a new
// PubSub for a given P2PHost, username and roomname
func JoinPubSub(p2phost *P2P, clientName string, topicName string) (*PubSub, error) {

	// Create a PubSub topic with the room name
	topic, err := p2phost.PubSub.Join(fmt.Sprintf("pub-sub-p2p-%s", topicName))
	// Check the error
	if err != nil {
		return nil, err
	}

	// Subscribe to the PubSub topic
	sub, err := topic.Subscribe()
	// Check the error
	if err != nil {
		return nil, err
	}

	// Check the provided clientname
	if clientName == "" {
		// Use the default client name
		clientName = defaultclient
	}

	// Check the provided topicname
	if topicName == "" {
		// Use the default topic name
		topicName = defaulttopic
	}

	// Create cancellable context
	pubsubctx, cancel := context.WithCancel(context.Background())

	// Create a PubSub object
	PubSub := &PubSub{
		Host: p2phost,

		Inbound:  make(chan chatmessage),
		Outbound: make(chan string),
		Logs:     make(chan chatlog),

		psctx:    pubsubctx,
		pscancel: cancel,
		pstopic:  topic,
		psub:     sub,

		ClientName: clientName,
		TopicName:  topicName,
		selfid:     p2phost.Host.ID(),
	}

	// Start the subscribe loop
	go PubSub.SubLoop()
	// Start the publish loop
	go PubSub.PubLoop()

	// Return the PubSub
	return PubSub, nil
}

// A method of PubSub that publishes a chatmessage
// to the PubSub topic until the pubsub context closes
func (cr *PubSub) PubLoop() {
	for {
		select {
		case <-cr.psctx.Done():
			return

		case message := <-cr.Outbound:
			// Create a ChatMessage
			m := chatmessage{
				Message:    message,
				SenderID:   cr.selfid.Pretty(),
				SenderName: cr.ClientName,
			}

			// Marshal the ChatMessage into a JSON
			messagebytes, err := json.Marshal(m)
			if err != nil {
				cr.Logs <- chatlog{logprefix: "puberr", logmsg: "could not marshal JSON"}
				continue
			}

			// Publish the message to the topic
			err = cr.pstopic.Publish(cr.psctx, messagebytes)
			if err != nil {
				cr.Logs <- chatlog{logprefix: "puberr", logmsg: "could not publish to topic"}
				continue
			}
		}
	}
}

// A method of PubSub that continously reads from the subscription
// until either the subscription or pubsub context closes.
// The recieved message is parsed sent into the inbound channel
func (cr *PubSub) SubLoop() {
	// Start loop
	for {
		select {
		case <-cr.psctx.Done():
			return

		default:
			// Read a message from the subscription
			message, err := cr.psub.Next(cr.psctx)
			// Check error
			if err != nil {
				// Close the messages queue (subscription has closed)
				close(cr.Inbound)
				cr.Logs <- chatlog{logprefix: "suberr", logmsg: "subscription has closed"}
				return
			}

			// Check if message is from self
			if message.ReceivedFrom == cr.selfid {
				continue
			}

			// Declare a ChatMessage
			cm := &chatmessage{}
			// Unmarshal the message data into a ChatMessage
			err = json.Unmarshal(message.Data, cm)
			if err != nil {
				cr.Logs <- chatlog{logprefix: "suberr", logmsg: "could not unmarshal JSON"}
				continue
			}

			// Send the ChatMessage into the message queue
			cr.Inbound <- *cm
		}
	}
}

// A method of PubSub that returns a list
// of all peer IDs connected to it
func (cr *PubSub) PeerList() []peer.ID {
	// Return the slice of peer IDs connected to chat room topic
	return cr.pstopic.ListPeers()
}

// A method of PubSub that updates the chat
// room by subscribing to the new topic
func (cr *PubSub) Exit() {
	defer cr.pscancel()

	// Cancel the existing subscription
	cr.psub.Cancel()
	// Close the topic handler
	cr.pstopic.Close()
}

// A method of PubSub that updates the chat user name
func (cr *PubSub) UpdateUser(username string) {
	cr.ClientName = username
}
