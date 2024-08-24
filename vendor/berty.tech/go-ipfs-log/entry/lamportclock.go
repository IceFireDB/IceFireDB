package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"bytes"
	"math"

	"berty.tech/go-ipfs-log/iface"
)

type LamportClock struct {
	ID   []byte `json:"id,omitempty"`
	Time int    `json:"time,omitempty"`
}

func (l *LamportClock) Defined() bool {
	return l != nil && len(l.ID) > 0
}

func (l *LamportClock) New() iface.IPFSLogLamportClock {
	return &LamportClock{}
}

func (l *LamportClock) SetID(i []byte) {
	l.ID = i
}

func (l *LamportClock) SetTime(i int) {
	l.Time = i
}

func (l *LamportClock) GetID() []byte {
	return l.ID
}

func (l *LamportClock) GetTime() int {
	return l.Time
}

// Tick increments the time value, returns a new instance of LamportClock.
func (l *LamportClock) Tick() iface.IPFSLogLamportClock {
	l.Time++

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

// Merge fusion two LamportClocks.
func (l *LamportClock) Merge(clock iface.IPFSLogLamportClock) iface.IPFSLogLamportClock {
	l.Time = int(math.Max(float64(l.Time), float64(clock.GetTime())))

	return &LamportClock{
		ID:   l.ID,
		Time: l.Time,
	}
}

// Compare calculate the "distance" based on the clock, ie. lower or greater.
func (l *LamportClock) Compare(b iface.IPFSLogLamportClock) int {
	// TODO: Make it a Golang slice-compatible sort function
	dist := l.Time - b.GetTime()

	// If the sequence number is the same (concurrent events),
	// return the comparison between IDs
	if dist == 0 {
		return bytes.Compare(l.ID, b.GetID())
	}

	return dist
}

// CopyLamportClock returns a copy of a lamport clock
func CopyLamportClock(clock iface.IPFSLogLamportClock) *LamportClock {
	return NewLamportClock(clock.GetID(), clock.GetTime())
}

// NewLamportClock creates a new LamportClock instance.
func NewLamportClock(identity []byte, time int) *LamportClock {
	return &LamportClock{
		ID:   identity,
		Time: time,
	}
}

var _ iface.IPFSLogLamportClock = (*LamportClock)(nil)
