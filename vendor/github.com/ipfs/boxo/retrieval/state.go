// Package retrieval tracks the state of an IPFS content retrieval as
// it moves through path resolution, provider discovery, connection,
// and data transfer. State lives on the request context and is
// updated by boxo's path resolver, provider query manager, and
// gateway middleware as the retrieval progresses.
//
// Typical consumers:
//
//   - boxo/gateway wraps timeout errors with the State (see
//     [WrapWithState]) so 504 responses include which phase was
//     active and how many providers were found.
//
//   - CLI tools like Kubo can mirror a daemon's State into a local
//     one via the [State.Snapshot] / [State.Apply] / [State.Notify]
//     pub/sub interface to drive a live progress bar during commands
//     like cat, get, or dag export.
//
// Attach with [ContextWithState]; read with [StateFromContext].
package retrieval

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

type contextKey string

// ContextKey is the key used to store State in a context.Context. This can be
// used directly with context.WithValue if needed, though the ContextWithState
// and StateFromContext functions are preferred.
const ContextKey contextKey = "boxo-retrieval-state"

// MaxProvidersSampleSize limits the number of provider peer IDs (both found and failed)
// that are kept as a sample for diagnostic purposes. This prevents unbounded memory growth
// while still providing useful debugging information.
const MaxProvidersSampleSize = 3

// RetrievalPhase represents the current phase of content retrieval.
// Phases progress monotonically - they can only move forward, never backward.
// This helps identify where in the retrieval process a timeout or failure occurred.
type RetrievalPhase int

const (
	// PhaseInitializing indicates the retrieval process has not yet started.
	PhaseInitializing RetrievalPhase = iota
	// PhasePathResolution indicates the system is resolving an IPFS path to determine
	// what content needs to be fetched (e.g., /ipfs/Qm.../path/to/file).
	PhasePathResolution
	// PhaseProviderDiscovery indicates the system is finding peers that have the content.
	PhaseProviderDiscovery
	// PhaseConnecting indicates the system is establishing connections to providers.
	PhaseConnecting
	// PhaseDataRetrieval indicates the system is transferring data to the client.
	PhaseDataRetrieval
)

// String returns a human-readable name for the retrieval phase, used
// in error messages and log output. JSON encoding of phases (in
// [Snapshot]) uses the underlying int.
func (p RetrievalPhase) String() string {
	switch p {
	case PhaseInitializing:
		return "initializing"
	case PhasePathResolution:
		return "path resolution"
	case PhaseProviderDiscovery:
		return "provider discovery"
	case PhaseConnecting:
		return "connecting to providers"
	case PhaseDataRetrieval:
		return "data retrieval"
	default:
		return "unknown"
	}
}

// State tracks diagnostic information about IPFS content retrieval operations.
// It is safe for concurrent use and maintains monotonic stage progression. Use
// ContextWithState to add tracking to a context, and StateFromContext to
// retrieve the state for updates or inspection
type State struct {
	// ProvidersFound tracks the number of providers discovered for the content.
	ProvidersFound atomic.Int32
	// ProvidersAttempted tracks the number of providers we tried to connect to.
	ProvidersAttempted atomic.Int32
	// ProvidersConnected tracks the number of providers successfully connected.
	ProvidersConnected atomic.Int32

	// phase tracks the current retrieval phase (stored as int32)
	phase atomic.Int32

	// mu protects foundProviders, failedProviders slices and CID fields during concurrent access
	mu sync.RWMutex
	// Sample of providers found during discovery (limited to first few for brevity)
	// NOTE: This is only a sample of the first MaxProvidersSampleSize providers, not all of them
	foundProviders []peer.ID
	// Sample of providers that failed (limited to first few for brevity)
	// NOTE: This is only a sample of the first MaxProvidersSampleSize providers, not all of them
	failedProviders []peer.ID

	// CIDs for diagnostic purposes
	// For /ipfs/cid, both will be the same
	// For /ipfs/cid/path/to/file, rootCID is 'cid' and terminalCID is the CID of 'file'
	rootCID     cid.Cid // First CID in the path
	terminalCID cid.Cid // CID of terminating DAG entity on the path

	// notify is a size-1, coalescing channel used to wake subscribers when
	// the State changes. Writers do a non-blocking send; if a wake-up is
	// already pending the send is dropped. Subscribers read the channel and
	// then call Snapshot to read the latest values; intermediate updates
	// between sends are coalesced into a single wake-up. Always non-nil
	// after [NewState].
	notify chan struct{}
}

// NewState creates a new State initialized to PhaseInitializing. The returned
// state is safe for concurrent use.
func NewState() *State {
	rs := &State{notify: make(chan struct{}, 1)}
	rs.phase.Store(int32(PhaseInitializing))
	return rs
}

// SetPhase updates the current retrieval phase to the given phase.
// The phase progression is monotonic - phases can only move forward, never backward.
// If the provided phase is less than or equal to the current phase, this is a no-op.
// This method is safe for concurrent use.
func (rs *State) SetPhase(phase RetrievalPhase) {
	newPhase := int32(phase)
	for {
		current := rs.phase.Load()
		// Only update if the new phase is greater (moving forward)
		if newPhase <= current {
			return
		}
		// Try to update atomically
		if rs.phase.CompareAndSwap(current, newPhase) {
			rs.signal()
			return
		}
		// If CAS failed, another goroutine updated it, loop will check again
	}
}

// GetPhase returns the current retrieval phase.
// This method is safe for concurrent use.
func (rs *State) GetPhase() RetrievalPhase {
	return RetrievalPhase(rs.phase.Load())
}

// appendProviders is a helper to append providers to a sample list with size limit.
// Only the first MaxProvidersSampleSize providers are kept to prevent unbounded memory growth.
// Duplicate peer IDs are automatically filtered out to ensure each peer appears only once.
// Signals subscribers if any peer was actually added.
func (rs *State) appendProviders(list *[]peer.ID, peerIDs ...peer.ID) {
	rs.mu.Lock()
	prev := len(*list)
	for _, peerID := range peerIDs {
		if len(*list) >= MaxProvidersSampleSize {
			break
		}
		if slices.Contains(*list, peerID) {
			continue
		}
		*list = append(*list, peerID)
	}
	changed := len(*list) != prev
	rs.mu.Unlock()
	if changed {
		rs.signal()
	}
}

// AddFoundProvider records a provider peer ID that was discovered during provider search.
// This method is safe for concurrent use.
func (rs *State) AddFoundProvider(peerID peer.ID) {
	rs.appendProviders(&rs.foundProviders, peerID)
}

// AddFailedProvider records a provider peer ID that failed to deliver the requested content.
// This method is safe for concurrent use.
func (rs *State) AddFailedProvider(peerID peer.ID) {
	rs.appendProviders(&rs.failedProviders, peerID)
}

// getProviders is a helper to get a cloned list of providers.
func (rs *State) getProviders(list []peer.ID) []peer.ID {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return slices.Clone(list)
}

// GetFoundProviders returns a sample of found providers (up to MaxProvidersSampleSize).
// This is not all providers, just the first few for diagnostic purposes.
func (rs *State) GetFoundProviders() []peer.ID {
	return rs.getProviders(rs.foundProviders)
}

// GetFailedProviders returns a sample of failed providers (up to MaxProvidersSampleSize).
// This is not all providers, just the first few for diagnostic purposes.
func (rs *State) GetFailedProviders() []peer.ID {
	return rs.getProviders(rs.failedProviders)
}

// SetRootCID sets the root CID (first CID in the path).
// This method is safe for concurrent use.
func (rs *State) SetRootCID(c cid.Cid) {
	rs.mu.Lock()
	changed := !rs.rootCID.Equals(c)
	rs.rootCID = c
	rs.mu.Unlock()
	if changed {
		rs.signal()
	}
}

// SetTerminalCID sets the terminal CID (CID of terminating DAG entity).
// This method is safe for concurrent use.
func (rs *State) SetTerminalCID(c cid.Cid) {
	rs.mu.Lock()
	changed := !rs.terminalCID.Equals(c)
	rs.terminalCID = c
	rs.mu.Unlock()
	if changed {
		rs.signal()
	}
}

// GetRootCID returns the root CID (first CID in the path).
// This method is safe for concurrent use.
func (rs *State) GetRootCID() cid.Cid {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.rootCID
}

// GetTerminalCID returns the terminal CID (CID of terminating DAG entity).
// This method is safe for concurrent use.
func (rs *State) GetTerminalCID() cid.Cid {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.terminalCID
}

// Snapshot is an immutable copy of a [State] at a point in time. It is
// safe to share across goroutines and to serialize as JSON. Receivers
// (e.g. CLIs reading from a streaming endpoint) reconstitute a local
// State by calling [State.Apply] with the snapshot.
//
// JSON encoding uses Go's default field naming (PascalCase, matching
// the struct fields verbatim). Phase is encoded as the underlying
// integer of [RetrievalPhase] (type RetrievalPhase int). Receivers can
// compare against the [PhaseInitializing] / [PhasePathResolution] /
// etc. constants directly, or call [RetrievalPhase.String] for a
// human-readable form.
type Snapshot struct {
	Phase              RetrievalPhase
	ProvidersFound     int32
	ProvidersAttempted int32
	ProvidersConnected int32
	FoundProviders     []peer.ID
	FailedProviders    []peer.ID
	RootCID            cid.Cid
	TerminalCID        cid.Cid
}

// Snapshot returns the current state as an immutable value. Slice fields
// are cloned, so callers may freely retain or modify the result without
// affecting the live State.
//
// Consistency: the read takes the State's lock for slices and CIDs, but
// counter fields ([State.ProvidersFound] etc.) are atomics that other
// writers update without the lock. A concurrent writer that mutates an
// atomic counter while Snapshot is running may produce a snapshot whose
// counters are slightly newer than its slices (or vice versa). For
// observability and progress UI use cases this eventual consistency is
// fine; callers needing a single-instant atomic view across all fields
// would need writers to also take the lock, which would slow them down.
func (rs *State) Snapshot() Snapshot {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return Snapshot{
		Phase:              RetrievalPhase(rs.phase.Load()),
		ProvidersFound:     rs.ProvidersFound.Load(),
		ProvidersAttempted: rs.ProvidersAttempted.Load(),
		ProvidersConnected: rs.ProvidersConnected.Load(),
		FoundProviders:     slices.Clone(rs.foundProviders),
		FailedProviders:    slices.Clone(rs.failedProviders),
		RootCID:            rs.rootCID,
		TerminalCID:        rs.terminalCID,
	}
}

// Apply mirrors a Snapshot onto this State. It is intended for
// receivers that observe a remote State over a transport (e.g. NDJSON
// over HTTP) and want to reflect the remote values into a local State
// that some local UI is observing. Phase progression remains monotonic:
// a snapshot with an earlier phase will not move the local phase
// backwards. All writes happen under one critical section, so observers
// either see the snapshot in full or not at all, and Apply emits
// exactly one wake-up on [State.Notify].
//
// Apply assumes snapshots arrive in causal order from a single
// producer. Out-of-order delivery (e.g. multiple producers, or a
// transport that reorders) is unsupported: counters and CID/slice
// fields are written unconditionally, so a stale snapshot can regress
// them. The retrieval-state pipeline shipped in kubo (one daemon-side
// State, one CLI-side subscriber, ordered NDJSON) satisfies this
// assumption by construction.
func (rs *State) Apply(s Snapshot) {
	// Phase update via the same monotonic CAS loop SetPhase uses, so
	// concurrent SetPhase callers cannot regress the phase via Apply
	// even though SetPhase does not take rs.mu.
	target := int32(s.Phase)
	for {
		cur := rs.phase.Load()
		if target <= cur {
			break
		}
		if rs.phase.CompareAndSwap(cur, target) {
			break
		}
	}

	rs.mu.Lock()
	rs.ProvidersFound.Store(s.ProvidersFound)
	rs.ProvidersAttempted.Store(s.ProvidersAttempted)
	rs.ProvidersConnected.Store(s.ProvidersConnected)
	rs.foundProviders = slices.Clone(s.FoundProviders)
	rs.failedProviders = slices.Clone(s.FailedProviders)
	rs.rootCID = s.RootCID
	rs.terminalCID = s.TerminalCID
	rs.mu.Unlock()
	rs.signal()
}

// Notify returns a size-1 channel that is signalled when the State
// changes. Writes are coalescing: if multiple updates happen between
// successive receives, the receiver wakes once and should call
// [State.Snapshot] to observe the latest values.
//
// Lifecycle: the channel never closes. Subscribers should stop
// receiving by other means, typically a context cancellation in the
// surrounding select:
//
//	for {
//	    select {
//	    case <-ctx.Done():
//	        return
//	    case <-state.Notify():
//	        publish(state.Snapshot())
//	    }
//	}
//
// Single-subscriber: the channel is shared, not fan-out. If two
// goroutines receive on it, each wake-up goes to one of them
// non-deterministically and the other misses it. To support multiple
// subscribers, fan out via your own goroutine: a single reader on
// Notify that broadcasts to a slice of per-subscriber channels.
func (rs *State) Notify() <-chan struct{} {
	return rs.notify
}

// signal performs a non-blocking send on the notification channel. If the
// channel is full (a wake-up is already pending) the send is dropped. Used
// internally by every State write that observers might care about.
func (rs *State) signal() {
	select {
	case rs.notify <- struct{}{}:
	default:
	}
}

// formatPeerIDs converts a slice of peer IDs to a formatted string with a prefix.
// Returns empty string if the slice is empty.
func formatPeerIDs(peers []peer.ID, prefix string) string {
	if len(peers) == 0 {
		return ""
	}
	peerStrings := make([]string, len(peers))
	for i, p := range peers {
		peerStrings[i] = p.String()
	}
	return fmt.Sprintf(", %s: %s", prefix, strings.Join(peerStrings, ", "))
}

// Summary generates a human-readable summary of the retrieval state,
// useful for timeout error messages and diagnostics.
func (rs *State) Summary() string {
	found := rs.ProvidersFound.Load()
	attempted := rs.ProvidersAttempted.Load()
	connected := rs.ProvidersConnected.Load()
	phase := rs.GetPhase()

	if found == 0 {
		return fmt.Sprintf("no providers found for the CID (phase: %s)", phase.String())
	}

	if attempted == 0 {
		return fmt.Sprintf("found %d provider(s) but none could be contacted (phase: %s)", found, phase.String())
	}

	if connected == 0 {
		// When we can't connect, show the found providers instead of failed ones
		// since all attempts effectively failed
		foundProviders := rs.GetFoundProviders()
		peersInfo := formatPeerIDs(foundProviders, "peers")
		return fmt.Sprintf("found %d provider(s), attempted %d, but none were reachable (phase: %s%s)",
			found, attempted, phase.String(), peersInfo)
	}

	failedProviders := rs.GetFailedProviders()
	failedPeersInfo := formatPeerIDs(failedProviders, "failed peers")

	if len(failedProviders) > 0 {
		return fmt.Sprintf("found %d provider(s), connected to %d, but they did not return the requested content (phase: %s%s)",
			found, connected, phase.String(), failedPeersInfo)
	}

	return fmt.Sprintf("timeout occurred after finding %d provider(s) and connecting to %d (phase: %s)",
		found, connected, phase.String())
}

// ContextWithState ensures a State exists in the context. If the context
// already contains a State, it returns the existing one. Otherwise, it creates
// a new State and adds it to the context. This function is idempotent and safe
// to call multiple times.
//
// Example:
//
//	ctx, retrievalState := retrieval.ContextWithState(ctx)
//	// Use retrievalState to track progress
//	retrievalState.SetStage(retrieval.StageProviderDiscovery)
func ContextWithState(ctx context.Context) (context.Context, *State) {
	// Check if context already has a State
	if existing := StateFromContext(ctx); existing != nil {
		return ctx, existing
	}
	// Create new one if not present
	rs := NewState()
	return context.WithValue(ctx, ContextKey, rs), rs
}

// StateFromContext retrieves the State from the context. Returns nil if no
// State is present in the context. This function is typically used by
// subsystems to check if retrieval tracking is enabled and to update the state
// if it is.
//
// Example:
//
//	if retrievalState := retrieval.StateFromContext(ctx); retrievalState != nil {
//	    retrievalState.SetStage(retrieval.StageBlockRetrieval)
//	    retrievalState.ProvidersFound.Add(1)
//	}
func StateFromContext(ctx context.Context) *State {
	if v := ctx.Value(ContextKey); v != nil {
		return v.(*State)
	}
	return nil
}

// Compile-time assertions to ensure ErrorWithState implements the expected interfaces.
var (
	_ error                       = (*ErrorWithState)(nil)
	_ interface{ Unwrap() error } = (*ErrorWithState)(nil)
)

// ErrorWithState wraps an error with retrieval state information.
// It preserves the retrieval diagnostics for programmatic access while
// providing human-readable error messages.
//
// The zero value is not useful; use WrapWithState to create instances.
type ErrorWithState struct {
	// err is the underlying error being wrapped.
	err error
	// state contains the retrieval diagnostic information.
	state *State
}

// Error returns the error message with retrieval diagnostics appended.
// Format: "original error: retrieval: diagnostic summary"
//
// If err is nil, returns a generic message. If state is nil, returns
// just the underlying error message.
func (e *ErrorWithState) Error() string {
	if e.err == nil {
		if e.state != nil {
			return fmt.Sprintf("retrieval error: %s", e.state.Summary())
		}
		return "retrieval error with no underlying cause"
	}
	if e.state != nil {
		return fmt.Sprintf("%s: retrieval: %s", e.err.Error(), e.state.Summary())
	}
	return e.err.Error()
}

// Unwrap returns the wrapped error, allowing errors.Is and errors.As to work
// with the underlying error.
func (e *ErrorWithState) Unwrap() error {
	return e.err
}

// State returns the retrieval state associated with this error. This allows
// callers to access detailed diagnostics for custom handling.
func (e *ErrorWithState) State() *State {
	return e.state
}

// WrapWithState wraps an error with retrieval state from the context.
// It returns an *ErrorWithState that preserves the state for custom handling.
//
// The error is ALWAYS wrapped if retrieval state exists in the context,
// because even "no providers found" is meaningful diagnostic information.
// If the error is already an *ErrorWithState, it returns it unchanged to
// avoid double-wrapping.
//
// Example usage in a gateway or IPFS implementation:
//
//	func fetchBlock(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
//	    block, err := blockService.GetBlock(ctx, cid)
//	    if err != nil {
//	        // Wrap error with retrieval diagnostics if available
//	        return nil, retrieval.WrapWithState(ctx, err)
//	    }
//	    return block, nil
//	}
//
// Callers can then extract the state for custom handling:
//
//	var errWithState *retrieval.ErrorWithState
//	if errors.As(err, &errWithState) {
//	    state := errWithState.State()
//	    if state.ProvidersFound.Load() == 0 {
//	        // Handle "content not in network" case specially
//	    }
//	}
func WrapWithState(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// Check if already wrapped
	var existingErr *ErrorWithState
	if errors.As(err, &existingErr) {
		return err
	}

	if state := StateFromContext(ctx); state != nil {
		// Always wrap if we have retrieval state - even "no providers" is meaningful
		return &ErrorWithState{
			err:   err,
			state: state,
		}
	}
	return err
}
