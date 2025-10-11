// Package retrieval provides state tracking for IPFS content retrieval operations.
// It enables detailed diagnostics about the retrieval process, including which stage
// failed (path resolution, provider discovery, connection, or block retrieval) and
// statistics about provider interactions. This information is particularly useful
// for debugging timeout errors and understanding retrieval performance.
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

// String returns a human-readable name for the retrieval phase.
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
}

// NewState creates a new State initialized to PhaseInitializing. The returned
// state is safe for concurrent use.
func NewState() *State {
	rs := &State{}
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
// This follows the idiomatic append pattern but operates on internal state.
func (rs *State) appendProviders(list *[]peer.ID, peerIDs ...peer.ID) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if len(*list) >= MaxProvidersSampleSize {
		return
	}
	remaining := MaxProvidersSampleSize - len(*list)
	if len(peerIDs) > remaining {
		peerIDs = peerIDs[:remaining]
	}
	*list = append(*list, peerIDs...)
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
	defer rs.mu.Unlock()
	rs.rootCID = c
}

// SetTerminalCID sets the terminal CID (CID of terminating DAG entity).
// This method is safe for concurrent use.
func (rs *State) SetTerminalCID(c cid.Cid) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.terminalCID = c
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
