package sessioninterestmanager

import (
	"slices"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
)

// SessionInterestManager records the CIDs that each session is interested in.
type SessionInterestManager struct {
	lk    sync.RWMutex
	wants map[cid.Cid]map[uint64]struct{}
}

// New initializes a new SessionInterestManager.
func New() *SessionInterestManager {
	return &SessionInterestManager{
		// Map of cids -> set of sessions that want this cid
		wants: make(map[cid.Cid]map[uint64]struct{}),
	}
}

// RecordSessionInteres records CIDs that a session is interested in. This is
// called when the client asks the session for blocks.
func (sim *SessionInterestManager) RecordSessionInterest(ses uint64, ks []cid.Cid) {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// For each key
	for _, c := range ks {
		// Record that the session wants the blocks
		if want, ok := sim.wants[c]; ok {
			want[ses] = struct{}{}
		} else {
			sim.wants[c] = map[uint64]struct{}{ses: {}}
		}
	}
}

// RemoveSession removes the keys that no session is interested in any more.
// This is called when the session shuts down.
func (sim *SessionInterestManager) RemoveSession(ses uint64) []cid.Cid {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// The keys that no session is interested in
	var deletedKs []cid.Cid

	// For each known key
	for c := range sim.wants {
		// Remove the session from the list of sessions that want the key
		delete(sim.wants[c], ses)

		// If there are no more sessions that want the key
		if len(sim.wants[c]) == 0 {
			// Clean up the list memory
			delete(sim.wants, c)
			// Add the key to the list of keys that no session is interested in
			deletedKs = append(deletedKs, c)
		}
	}

	return deletedKs
}

// RemoveSessionWants is called when the session receives blocks, to remove the
// session's interest in CIDs.
func (sim *SessionInterestManager) RemoveSessionWants(ses uint64, ks []cid.Cid) []cid.Cid {
	sim.lk.Lock()
	defer sim.lk.Unlock()

	// The keys that no session is interested in
	deletedKs := make([]cid.Cid, 0, len(ks))

	// For each key
	for _, c := range ks {
		// If there is a list of sessions that want the key
		if _, ok := sim.wants[c]; ok {
			// Remove the session from the list of sessions that want the key
			delete(sim.wants[c], ses)

			// If there are no more sessions that want the key
			if len(sim.wants[c]) == 0 {
				// Clean up the list memory
				delete(sim.wants, c)
				// Add the key to the list of keys that no session is interested in
				deletedKs = append(deletedKs, c)
			}
		}
	}

	return deletedKs
}

// FilterSessionInterested is called by the session to filter the sets of keys
// for those that the session is interested in.
func (sim *SessionInterestManager) FilterSessionInterested(ses uint64, ksets ...[]cid.Cid) [][]cid.Cid {
	kres := make([][]cid.Cid, len(ksets))

	sim.lk.RLock()
	defer sim.lk.RUnlock()

	// For each set of keys
	for i, ks := range ksets {
		// The set of keys that at least one session is interested in
		var has []cid.Cid

		// For each key in the list
		for _, c := range ks {
			// If the session is interested, add the key to the set
			if _, ok := sim.wants[c][ses]; ok {
				has = append(has, c)
			}
		}
		kres[i] = has
	}
	return kres
}

// SplitWantedUnwanted is called when bitswap receives blocks, to discard
// unwanted blocks.
func (sim *SessionInterestManager) SplitWantedUnwanted(blks []blocks.Block) ([]blocks.Block, []blocks.Block) {
	// Get the wanted block keys as a set
	wantedKs := cid.NewSet()

	sim.lk.RLock()

	for _, b := range blks {
		c := b.Cid()
		// For each session that wants the key.
		for ses := range sim.wants[c] {
			if _, ok := sim.wants[c][ses]; ok {
				// Add the key to the set
				wantedKs.Add(c)
			}
		}
	}

	sim.lk.RUnlock()

	// Separate the blocks into wanted and unwanted
	wantedBlks := make([]blocks.Block, 0, len(blks))
	var notWantedBlks []blocks.Block
	for _, b := range blks {
		if wantedKs.Has(b.Cid()) {
			wantedBlks = append(wantedBlks, b)
		} else {
			notWantedBlks = append(notWantedBlks, b)
		}
	}
	return wantedBlks, notWantedBlks
}

// InterestedSessions returns a list of sessions that are interested in any of
// the CIDs in the given sets of CIDs. This is called when SessionManager
// receives a message, to find out which sessions are interested in the
// message.
func (sim *SessionInterestManager) InterestedSessions(keySets ...[]cid.Cid) []uint64 {
	sesSet := make(map[uint64]struct{})

	sim.lk.RLock()

	// Create a set of sessions that are interested in the keys
	for _, keySet := range keySets {
		for _, c := range keySet {
			for s := range sim.wants[c] {
				sesSet[s] = struct{}{}
			}
		}
	}

	sim.lk.RUnlock()

	// Convert the set into a list
	ses := make([]uint64, 0, len(sesSet))
	for s := range sesSet {
		ses = append(ses, s)
	}
	return ses
}

// FilterInterests keeps only keys that are wanted by at least one session.
//
// IMPORTANT: FilterInterests filters the given Cid slices in place, modifying
// their contents. If the caller needs to preserve a copy of the lists it
// should make a copy before calling FilterInterests.
func (sim *SessionInterestManager) FilterInterests(keySets ...[]cid.Cid) [][]cid.Cid {
	sim.lk.RLock()
	defer sim.lk.RUnlock()

	// For each set of keys.
	for i, ks := range keySets {
		keySets[i] = slices.DeleteFunc(ks, func(c cid.Cid) bool {
			// If any session wants the key, keep it.
			_, ok := sim.wants[c]
			return !ok
		})
	}
	return keySets
}
