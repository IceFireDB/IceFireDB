// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package ice

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pion/stun"
)

func newCandidatePair(local, remote Candidate, controlling bool) *CandidatePair {
	return &CandidatePair{
		iceRoleControlling: controlling,
		Remote:             remote,
		Local:              local,
		state:              CandidatePairStateWaiting,
	}
}

// CandidatePair is a combination of a
// local and remote candidate
type CandidatePair struct {
	iceRoleControlling       bool
	Remote                   Candidate
	Local                    Candidate
	bindingRequestCount      uint16
	state                    CandidatePairState
	nominated                bool
	nominateOnBindingSuccess bool

	// stats
	currentRoundTripTime int64 // in ns
	totalRoundTripTime   int64 // in ns
	responsesReceived    uint64
}

func (p *CandidatePair) String() string {
	if p == nil {
		return ""
	}

	return fmt.Sprintf("prio %d (local, prio %d) %s <-> %s (remote, prio %d), state: %s, nominated: %v, nominateOnBindingSuccess: %v",
		p.priority(), p.Local.Priority(), p.Local, p.Remote, p.Remote.Priority(), p.state, p.nominated, p.nominateOnBindingSuccess)
}

func (p *CandidatePair) equal(other *CandidatePair) bool {
	if p == nil && other == nil {
		return true
	}
	if p == nil || other == nil {
		return false
	}
	return p.Local.Equal(other.Local) && p.Remote.Equal(other.Remote)
}

// RFC 5245 - 5.7.2.  Computing Pair Priority and Ordering Pairs
// Let G be the priority for the candidate provided by the controlling
// agent.  Let D be the priority for the candidate provided by the
// controlled agent.
// pair priority = 2^32*MIN(G,D) + 2*MAX(G,D) + (G>D?1:0)
func (p *CandidatePair) priority() uint64 {
	var g, d uint32
	if p.iceRoleControlling {
		g = p.Local.Priority()
		d = p.Remote.Priority()
	} else {
		g = p.Remote.Priority()
		d = p.Local.Priority()
	}

	// Just implement these here rather
	// than fooling around with the math package
	min := func(x, y uint32) uint64 {
		if x < y {
			return uint64(x)
		}
		return uint64(y)
	}
	max := func(x, y uint32) uint64 {
		if x > y {
			return uint64(x)
		}
		return uint64(y)
	}
	cmp := func(x, y uint32) uint64 {
		if x > y {
			return uint64(1)
		}
		return uint64(0)
	}

	// 1<<32 overflows uint32; and if both g && d are
	// maxUint32, this result would overflow uint64
	return (1<<32-1)*min(g, d) + 2*max(g, d) + cmp(g, d)
}

func (p *CandidatePair) Write(b []byte) (int, error) {
	return p.Local.writeTo(b, p.Remote)
}

func (a *Agent) sendSTUN(msg *stun.Message, local, remote Candidate) {
	_, err := local.writeTo(msg.Raw, remote)
	if err != nil {
		a.log.Tracef("Failed to send STUN message: %s", err)
	}
}

// UpdateRoundTripTime sets the current round time of this pair and
// accumulates total round trip time and responses received
func (p *CandidatePair) UpdateRoundTripTime(rtt time.Duration) {
	rttNs := rtt.Nanoseconds()
	atomic.StoreInt64(&p.currentRoundTripTime, rttNs)
	atomic.AddInt64(&p.totalRoundTripTime, rttNs)
	atomic.AddUint64(&p.responsesReceived, 1)
}

// CurrentRoundTripTime returns the current round trip time in seconds
// https://www.w3.org/TR/webrtc-stats/#dom-rtcicecandidatepairstats-currentroundtriptime
func (p *CandidatePair) CurrentRoundTripTime() float64 {
	return time.Duration(atomic.LoadInt64(&p.currentRoundTripTime)).Seconds()
}

// TotalRoundTripTime returns the current round trip time in seconds
// https://www.w3.org/TR/webrtc-stats/#dom-rtcicecandidatepairstats-totalroundtriptime
func (p *CandidatePair) TotalRoundTripTime() float64 {
	return time.Duration(atomic.LoadInt64(&p.totalRoundTripTime)).Seconds()
}

// ResponsesReceived returns the total number of connectivity responses received
// https://www.w3.org/TR/webrtc-stats/#dom-rtcicecandidatepairstats-responsesreceived
func (p *CandidatePair) ResponsesReceived() uint64 {
	return atomic.LoadUint64(&p.responsesReceived)
}
