package p2p

import (
	"strings"
	"testing"
)

// TestPublishNeverBlocks is the guard for the write path: a client's INSERT must not
// be held hostage by the p2p publisher. With the queue full, Publish reports the drop
// instead of blocking.
func TestPublishNeverBlocks(t *testing.T) {
	ps := &PubSub{Outbound: make(chan string, 2)}

	if !ps.Publish([]byte("first")) {
		t.Fatal("first publish rejected")
	}
	if !ps.Publish([]byte("second")) {
		t.Fatal("second publish rejected")
	}
	if ps.Publish([]byte("third")) {
		t.Fatal("publish with a full queue should report a drop")
	}
	if len(ps.Outbound) != 2 {
		t.Fatalf("queue length = %d, want 2", len(ps.Outbound))
	}
}

// TestMessageAccessors checks the accessors the replication path relies on.
func TestMessageAccessors(t *testing.T) {
	msg := &Message{SenderID: "peer-a", Content: `{"v":1}`}
	if msg.GetFrom() != "peer-a" {
		t.Fatalf("sender = %q", msg.GetFrom())
	}
	if !strings.Contains(string(msg.GetData()), `"v":1`) {
		t.Fatalf("payload = %q", msg.GetData())
	}
}

// TestExitToleratesHalfInitialisedNode guards shutdown: Exit runs from the context
// cancellation path, where a node that failed before joining a topic still gets here.
func TestExitToleratesHalfInitialisedNode(t *testing.T) {
	ps := &PubSub{Outbound: make(chan string, 1), Inbound: make(chan *Message, 1)}
	ps.Exit() // must not panic
}
