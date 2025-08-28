// package meg implements Regular Expressions for multiaddr Components. It's short for "Megular Expressions"
package meg

// The developer is assumed to be familiar with the Thompson NFA approach to
// regex before making changes to this file. Refer to
// https://swtch.com/~rsc/regexp/regexp1.html for an introduction.

import (
	"fmt"
)

type stateKind = int

const (
	matchAny stateKind = (iota * -1) - 1
	// done MUST be the last stateKind in this list.
	// Anything that is less than done is a split index
	done
)

// MatchState is the Thompson NFA for a regular expression.
type MatchState struct {
	capture CaptureFunc
	// next is is the index of the next state. in the MatchState array.
	next int
	// If codeOrKind is negative, it is a kind.
	// If it is negative, and less than `done`, then it is the index to the next split.
	// This is done to keep the `MatchState` struct small and cache friendly.
	codeOrKind int
}

type CaptureFunc func(Matchable) error

// capture is a linked list of capture funcs with values.
type capture struct {
	f    CaptureFunc
	v    Matchable
	prev *capture
}

type statesAndCaptures struct {
	states   []int
	captures []*capture
}

func (s MatchState) String() string {
	if s.codeOrKind == done {
		return "done"
	}
	if s.codeOrKind < done {
		return fmt.Sprintf("split{left: %d, right: %d}", s.next, decodeSplitIdx(s.codeOrKind))
	}
	return fmt.Sprintf("match{code: %d, next: %d}", s.codeOrKind, s.next)
}

// Matchable is an interface for any thing that can be matched against by this
// package. In the future, we may use multiaddr.Component types directly.
type Matchable interface {
	Code() int
	// Value() returns the string representation of the matchable.
	Value() string
	// RawValue() returns the byte representation of the Value
	RawValue() []byte
	// Bytes() returns the underlying bytes of the matchable. For multiaddr
	// Components, this includes the protocol code and possibly the varint
	// encoded size.
	Bytes() []byte
}

// ListOfMatchable is anything list-like that contains Matchable items.
// This allows us to convert a slice of []T as a []Matchable when *T implements
// Matchable. In the future, this may not be required if Go generics allows us
// to say S ~[]T, and *T implements Matchable. This may also not be required if
// we move this out of its own package and depend on Multiaddr and Components
// directly.
type ListOfMatchable interface {
	Get(i int) Matchable
	Len() int
}

// Match returns whether the given Components match the Matcher
//
// Errors are used to communicate capture errors.
// If the error is non-nil the returned bool will be false.
//
// Components must be a ListOfMatchable to allow us to use a slice of []T as a
// []Matchable when *T implements Matchable.
func Match[L ListOfMatchable](matcher Matcher, components L) (bool, error) {
	states := matcher.states
	startStateIdx := matcher.startIdx

	// Fast case for a small number of states (<128)
	// Avoids allocation of a slice for the visitedBitSet.
	stackBitSet := [2]uint64{}
	visitedBitSet := stackBitSet[:]
	if len(states) > 128 {
		visitedBitSet = make([]uint64, (len(states)+63)/64)
	}

	currentStates := statesAndCaptures{
		states:   make([]int, 0, 16),
		captures: make([]*capture, 0, 16),
	}
	nextStates := statesAndCaptures{
		states:   make([]int, 0, 16),
		captures: make([]*capture, 0, 16),
	}

	currentStates = appendState(currentStates, states, startStateIdx, nil, visitedBitSet)

	for ic := range components.Len() {
		clear(visitedBitSet)
		if len(currentStates.states) == 0 {
			return false, nil
		}
		for i, stateIndex := range currentStates.states {
			s := &states[stateIndex]
			cPtr := components.Get(ic)
			if s.codeOrKind == matchAny || (s.codeOrKind >= 0 && s.codeOrKind == cPtr.Code()) {
				cm := currentStates.captures[i]
				if s.capture != nil {
					next := &capture{
						f: s.capture,
						v: cPtr,
					}
					if cm == nil {
						cm = next
					} else {
						next.prev = cm
						cm = next
					}
					currentStates.captures[i] = cm
				}
				nextStates = appendState(nextStates, states, s.next, cm, visitedBitSet)
			}
		}
		currentStates, nextStates = nextStates, currentStates
		nextStates.states = nextStates.states[:0]
		nextStates.captures = nextStates.captures[:0]
	}

	for i, stateIndex := range currentStates.states {
		s := &states[stateIndex]
		if s.codeOrKind == done {

			// We found a complete path. Run the captures now
			c := currentStates.captures[i]

			// Flip the order of the captures because we see captures from right
			// to left, but users expect them left to right.
			type captureWithVal struct {
				f CaptureFunc
				v Matchable
			}
			reversedCaptures := make([]captureWithVal, 0, 16)
			for c != nil {
				reversedCaptures = append(reversedCaptures, captureWithVal{c.f, c.v})
				c = c.prev
			}
			for i := len(reversedCaptures) - 1; i >= 0; i-- {
				if err := reversedCaptures[i].f(reversedCaptures[i].v); err != nil {
					return false, err
				}
			}
			return true, nil
		}
	}
	return false, nil
}

// appendState is a non-recursive way of appending states to statesAndCaptures.
// If a state is a split, both branches are appended to statesAndCaptures.
func appendState(arr statesAndCaptures, states []MatchState, stateIndex int, c *capture, visitedBitSet []uint64) statesAndCaptures {
	// Local struct to hold state index and the associated capture pointer.
	type task struct {
		idx int
		cap *capture
	}

	// Initialize the stack with the starting task.
	stack := make([]task, 0, 16)
	stack = append(stack, task{stateIndex, c})

	// Process the stack until empty.
	for len(stack) > 0 {
		// Pop the last element (LIFO order).
		n := len(stack) - 1
		t := stack[n]
		stack = stack[:n]

		// If the state index is out of bounds, skip.
		if t.idx >= len(states) {
			continue
		}
		s := &states[t.idx]

		// Check if this state has already been visited.
		if visitedBitSet[t.idx/64]&(1<<(t.idx%64)) != 0 {
			continue
		}
		// Mark the state as visited.
		visitedBitSet[t.idx/64] |= 1 << (t.idx % 64)

		// If it's a split state (the value is less than done) then push both branches.
		if s.codeOrKind < done {
			// Get the second branch from the split.
			splitIdx := decodeSplitIdx(s.codeOrKind)

			// Check if the next branch is a `matchAny`. If it is, we want to
			// deprioritize it to allow for less greedy Any behavior.
			if states[s.next].codeOrKind == matchAny {
				// We want to process the non-matchAny first, so we push the s.next branch first
				stack = append(stack, task{s.next, t.cap})
				stack = append(stack, task{splitIdx, t.cap})
			} else {
				// To process s.next first, push the split branch first.
				stack = append(stack, task{splitIdx, t.cap})
				stack = append(stack, task{s.next, t.cap})
			}
		} else {
			// Otherwise, it's a valid final state -- append it.
			arr.states = append(arr.states, t.idx)
			arr.captures = append(arr.captures, t.cap)
		}
	}
	return arr
}

const splitIdxOffset = -(done - 1)

func encodeSplitIdx(codeOrKind int) int {
	return (codeOrKind + splitIdxOffset) * -1
}

func decodeSplitIdx(splitIdx int) int {
	return (splitIdx * -1) - splitIdxOffset
}
