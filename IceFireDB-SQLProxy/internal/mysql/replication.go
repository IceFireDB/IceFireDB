package mysql

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// replicationProtocolVersion is the envelope version carried over the p2p topic. A
// node refuses envelopes it does not understand instead of guessing at their shape.
const replicationProtocolVersion = 1

// Envelope kinds. "sql" carries one autocommit statement, "txn" carries the
// statements of a committed transaction that must be replayed atomically.
const (
	messageKindSQL = "sql"
	messageKindTxn = "txn"
)

// replicationMessage is what a node publishes to its peers. It replaced a bare SQL
// string so that a message can be identified (de-duplication), authenticated
// (HMAC) and applied as one transaction.
type replicationMessage struct {
	Version    int      `json:"v"`
	ID         string   `json:"id"`
	Kind       string   `json:"kind"`
	Role       string   `json:"role"`
	Statements []string `json:"stmts"`
	SentAt     int64    `json:"ts"`
	MAC        string   `json:"mac,omitempty"`
}

func newReplicationMessage(kind, role string, statements []string) *replicationMessage {
	return &replicationMessage{
		Version:    replicationProtocolVersion,
		ID:         newMessageID(),
		Kind:       kind,
		Role:       role,
		Statements: append([]string(nil), statements...),
		SentAt:     time.Now().UnixNano(),
	}
}

// newMessageID returns a random 128-bit identifier, used to drop duplicate
// deliveries of the same message.
func newMessageID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), len(b))
	}
	return hex.EncodeToString(b[:])
}

func (m *replicationMessage) encode() ([]byte, error) {
	return json.Marshal(m)
}

// decodeReplicationMessage parses a topic payload. legacy is true when the payload is
// not an envelope at all — that is how pre-1.0.0 nodes published, as a bare SQL
// string — and the caller has to fall back to treating the payload as SQL text.
func decodeReplicationMessage(payload []byte) (msg *replicationMessage, legacy bool, err error) {
	if len(payload) == 0 {
		return nil, false, fmt.Errorf("empty replication message")
	}
	if payload[0] != '{' {
		return nil, true, nil
	}
	var m replicationMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return nil, false, fmt.Errorf("decode replication message: %w", err)
	}
	if m.Version != replicationProtocolVersion {
		return nil, false, fmt.Errorf("unsupported replication protocol version %d", m.Version)
	}
	if len(m.Statements) == 0 {
		return nil, false, fmt.Errorf("replication message %q carries no statements", m.ID)
	}
	return &m, false, nil
}

// Limits applied to every inbound message before it is parsed or executed. They are
// constants rather than configuration on purpose: a peer must never be able to make
// this node allocate or execute an unbounded amount of work.
const (
	maxReplicationMessageBytes = 1 << 20 // 1 MiB
	maxReplicationStatements   = 1000
)

// signingTarget is the canonical byte form of a message that the HMAC covers. Fields
// are joined with a separator that cannot appear in a statement, length-prefixed where
// ambiguity is possible.
func (m *replicationMessage) signingTarget() []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n%s\n%d\n%s\n%s\n%d\n", m.Version, m.ID, m.SentAt, m.Kind, m.Role, len(m.Statements))
	for _, stmt := range m.Statements {
		fmt.Fprintf(&b, "%d:", len(stmt))
		b.WriteString(stmt)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// sign authenticates the message with the shared secret.
func (m *replicationMessage) sign(token string) {
	if token == "" {
		m.MAC = ""
		return
	}
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write(m.signingTarget())
	m.MAC = hex.EncodeToString(mac.Sum(nil))
}

// verify checks the message signature in constant time.
func (m *replicationMessage) verify(token string) error {
	if token == "" {
		return nil
	}
	if m.MAC == "" {
		return errors.New("replication message is not signed")
	}
	want, err := hex.DecodeString(m.MAC)
	if err != nil {
		return fmt.Errorf("malformed replication message signature: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write(m.signingTarget())
	if !hmac.Equal(want, mac.Sum(nil)) {
		return errors.New("replication message signature mismatch")
	}
	return nil
}

// dedupCache remembers recently applied message IDs so a re-delivered message is not
// applied twice. It is a bounded ring: replication makes no delivery guarantee, and
// this only removes duplicates, it does not recover losses.
type dedupCache struct {
	mu   sync.Mutex
	size int
	seen map[string]struct{}
	ring []string
	next int
}

func newDedupCache(size int) *dedupCache {
	return &dedupCache{
		size: size,
		seen: make(map[string]struct{}, size),
		ring: make([]string, 0, size),
	}
}

// add records id and reports whether it was new.
func (c *dedupCache) add(id string) bool {
	if id == "" {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.seen[id]; ok {
		return false
	}
	if len(c.ring) == c.size {
		delete(c.seen, c.ring[c.next])
		c.ring[c.next] = id
		c.next = (c.next + 1) % c.size
	} else {
		c.ring = append(c.ring, id)
	}
	c.seen[id] = struct{}{}
	return true
}
