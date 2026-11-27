package mysql

import (
	"errors"
	"fmt"
)

// inboundPolicy decides whether a message that arrived on a topic may be executed on
// this node. Without one, any peer that knows the topic name can run arbitrary SQL
// against the local MySQL — including DROP TABLE — so every check here is load
// bearing.
type inboundPolicy struct {
	// role is the role the topic carries (admin or readonly).
	role string
	// authToken is the shared HMAC secret. Empty keeps the pre-1.0.0 behaviour of
	// accepting unsigned messages, and the caller is expected to have warned.
	authToken string
	// allowedPeers, when non-empty, restricts senders by libp2p peer ID.
	allowedPeers map[string]struct{}
	// dedup drops re-delivered messages.
	dedup *dedupCache
}

func newInboundPolicy(role, authToken string, allowedPeers []string, dedup *dedupCache) *inboundPolicy {
	set := make(map[string]struct{}, len(allowedPeers))
	for _, peer := range allowedPeers {
		if peer != "" {
			set[peer] = struct{}{}
		}
	}
	return &inboundPolicy{role: role, authToken: authToken, allowedPeers: set, dedup: dedup}
}

// accept validates a raw payload and returns the message to apply. On rejection it
// returns the metric reason label; a nil error means the message may be executed.
func (p *inboundPolicy) accept(sender string, payload []byte) (*replicationMessage, string, error) {
	if len(payload) == 0 {
		return nil, rejectMalformed, errors.New("empty replication payload")
	}
	if len(payload) > maxReplicationMessageBytes {
		return nil, rejectOversized, fmt.Errorf("message from %s is %d bytes, limit is %d",
			sender, len(payload), maxReplicationMessageBytes)
	}
	if len(p.allowedPeers) > 0 {
		if _, ok := p.allowedPeers[sender]; !ok {
			return nil, rejectPeer, fmt.Errorf("peer %s is not listed in p2p.allowedPeers", sender)
		}
	}

	msg, legacy, err := decodeReplicationMessage(payload)
	if err != nil {
		return nil, rejectMalformed, err
	}
	if legacy {
		if p.authToken != "" {
			// A pre-1.0.0 peer cannot sign, and an unsigned message cannot be
			// authenticated: refuse it rather than silently downgrading.
			return nil, rejectSignature, fmt.Errorf("unsigned legacy message from %s", sender)
		}
		msg = &replicationMessage{
			Version:    0,
			Kind:       messageKindSQL,
			Role:       p.role,
			Statements: []string{string(payload)},
		}
	} else {
		if err := msg.verify(p.authToken); err != nil {
			return nil, rejectSignature, err
		}
		if msg.Role != p.role {
			return nil, rejectReplicationRole, fmt.Errorf("message role %q does not match topic role %q", msg.Role, p.role)
		}
		if len(msg.Statements) > maxReplicationStatements {
			return nil, rejectOversized, fmt.Errorf("message %s carries %d statements, limit is %d",
				msg.ID, len(msg.Statements), maxReplicationStatements)
		}
	}

	// Only replicated writes may run here: a peer has no business replaying reads or
	// server-state changes through this node.
	for _, statement := range msg.Statements {
		if !classifySQL(statement).replicateSafe() {
			return nil, rejectIllegal, fmt.Errorf("statement is not a replicated write: %.120q", statement)
		}
	}

	if msg.ID != "" && p.dedup != nil && !p.dedup.add(msg.ID) {
		return nil, rejectDuplicate, fmt.Errorf("duplicate message %s", msg.ID)
	}
	return msg, "", nil
}
