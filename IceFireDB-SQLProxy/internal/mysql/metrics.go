package mysql

import "sync/atomic"

// Rejection reasons, used to label the rejected counter.
const (
	rejectReadonly        = "readonly"
	rejectMalformed       = "malformed"
	rejectSignature       = "signature"
	rejectPeer            = "peer"
	rejectOversized       = "oversized"
	rejectIllegal         = "illegal_statement"
	rejectDuplicate       = "duplicate"
	rejectReplicationRole = "role"
)

// counters are the process-wide replication and connection counters. They are
// exported through /metrics so that a best-effort replication stream is at least
// observable when it drops or fails.
type counters struct {
	publishedSQL   atomic.Uint64
	publishedTxn   atomic.Uint64
	publishDropped atomic.Uint64
	publishFailed  atomic.Uint64
	appliedSQL     atomic.Uint64
	appliedTxn     atomic.Uint64
	applyFailed    atomic.Uint64

	rejectedReadonly  atomic.Uint64
	rejectedMalformed atomic.Uint64
	rejectedSignature atomic.Uint64
	rejectedPeer      atomic.Uint64
	rejectedOversized atomic.Uint64
	rejectedIllegal   atomic.Uint64
	rejectedDuplicate atomic.Uint64
	rejectedRole      atomic.Uint64
}

var stats counters

func recordPublished(kind string) {
	if kind == messageKindTxn {
		stats.publishedTxn.Add(1)
		return
	}
	stats.publishedSQL.Add(1)
}

func recordPublishDropped() { stats.publishDropped.Add(1) }
func recordPublishFailed()  { stats.publishFailed.Add(1) }

func recordApplied(kind string) {
	if kind == messageKindTxn {
		stats.appliedTxn.Add(1)
		return
	}
	stats.appliedSQL.Add(1)
}

func recordApplyFailed() { stats.applyFailed.Add(1) }

// recordRejected counts a rejected statement or message by reason.
func recordRejected(reason string) {
	switch reason {
	case rejectReadonly:
		stats.rejectedReadonly.Add(1)
	case rejectSignature:
		stats.rejectedSignature.Add(1)
	case rejectPeer:
		stats.rejectedPeer.Add(1)
	case rejectOversized:
		stats.rejectedOversized.Add(1)
	case rejectIllegal:
		stats.rejectedIllegal.Add(1)
	case rejectDuplicate:
		stats.rejectedDuplicate.Add(1)
	case rejectReplicationRole:
		stats.rejectedRole.Add(1)
	default:
		stats.rejectedMalformed.Add(1)
	}
}
