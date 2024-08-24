package ppubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-PubSub/pkg/router"
	"github.com/IceFireDB/components-go/RESPHandle"
	"github.com/IceFireDB/components-go/p2p"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/sirupsen/logrus"
)

var pss *pubsubStore

// Represents the default fallback room and user names
// if they aren't provided when the app is started
const defaultclient = "client"
const defaulttopic = "pubsub"

func InitPubSub(ctx context.Context, p2p *p2p.P2P) {
	pss = NewPubsubStore(ctx, p2p)
}

type pubsubStore struct {
	sync.RWMutex // synchronize access to shared variables
	ctx          context.Context
	p2p          *p2p.P2P
	join         map[string]*PubSub
	writer       map[string]map[string]*RESPHandle.WriterHandle
}

func NewPubsubStore(ctx context.Context, p2p *p2p.P2P) *pubsubStore {
	s := &pubsubStore{
		ctx:    ctx,
		p2p:    p2p,
		join:   make(map[string]*PubSub),
		writer: make(map[string]map[string]*RESPHandle.WriterHandle),
	}
	return s
}

func Pub(topicName string, message string) error {
	if _, ok := pss.join[topicName]; !ok {
		_, err := JoinPubSub(pss.p2p, "redis-client", topicName)
		if err != nil {
			log.Println(err)
			return err
		}
	}
	ps := pss.join[topicName]
	ps.Outbound <- message
	return nil
}

func Sub(local *RESPHandle.WriterHandle, topicName string) (*PubSub, error) {
	if _, ok := pss.join[topicName]; !ok {
		_, err := JoinPubSub(pss.p2p, "redis-client", topicName)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	lp := fmt.Sprintf("%p", local)
	if _, ok := pss.writer[topicName]; !ok {
		pss.writer[topicName] = make(map[string]*RESPHandle.WriterHandle)
	}
	pss.writer[topicName][lp] = local
	ps := pss.join[topicName]
	return ps, nil
}

// A structure that represents a PubSub Chat Room
type PubSub struct {
	// Represents the P2P Host for the PubSub
	Host *p2p.P2P

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

// A constructor function that generates and returns a new
// PubSub for a given P2PHost, username and roomname
func JoinPubSub(p2phost *p2p.P2P, clientName string, topicName string) (*PubSub, error) {

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

	//// Start the subscribe loop
	//go PubSub.SubLoop()
	//// Start the publish loop
	//go PubSub.PubLoop()

	pss.join[topicName] = PubSub
	go PubSub.PubLoop()
	go PubSub.SubLoop()
	go PubSub.Writer()
	go PubSub.printPeer()

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
			logrus.Infof("Outbound Message: %s", message)
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
// The received message is parsed sent into the inbound channel
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
			//if message.ReceivedFrom == cr.selfid {
			//	continue
			//}

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
			logrus.Infof("Inbound Message: %s", cm.Message)
		}
	}
}

func (cr *PubSub) Writer() {
	for {
		msg := <-cr.Inbound
		for key, item := range pss.writer[cr.TopicName] {
			err := router.WriteBulkStrings(item, []string{"message", cr.TopicName, msg.Message})
			if err != nil {
				fmt.Println("write err key:", key)
				continue
			}
		}
	}
}

func (cr *PubSub) printPeer() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	peersAll := make(map[string]int)
	for {
		<-ticker.C
		peers := cr.PeerList()
		// Iterate over the list of peers
		for _, p := range peers {
			// Generate the pretty version of the peer ID
			peerid := p.Pretty()
			// Add the peer ID to the peer box
			if _, ok := peersAll[peerid]; ok {
				peersAll[peerid]++
			} else {
				fmt.Println("New Peer:", peerid)
				peersAll[peerid] = 0
			}
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
