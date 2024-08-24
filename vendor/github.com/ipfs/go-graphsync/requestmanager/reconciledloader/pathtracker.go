package reconciledloader

import (
	"github.com/ipld/go-ipld-prime/datamodel"

	"github.com/ipfs/go-graphsync"
)

// pathTracker is just a simple utility to track whether we're on a missing
// path for the remote
type pathTracker struct {
	lastUnfollowedRemotePath datamodel.Path
}

// stillOnUnfollowedRemotePath determines whether the next link load will be from
// a path missing from the remote
// if it won't be, based on the linear nature of selector traversals, it wipes
// the last missing state
func (pt *pathTracker) stillOnUnfollowedRemotePath(newPath datamodel.Path) bool {
	// is there a known missing path?
	if pt.lastUnfollowedRemotePath.Len() == 0 {
		return false
	}
	// are we still on it?
	if newPath.Len() <= pt.lastUnfollowedRemotePath.Len() {
		// if not, reset to no known missing remote path
		pt.lastUnfollowedRemotePath = datamodel.NewPath(nil)
		return false
	}
	// otherwise we're on a missing path
	return true
}

// recordRemoteLoadAttempt records the results of attempting to load from the remote
// at the given path
func (pt *pathTracker) recordRemoteLoadAttempt(currentPath datamodel.Path, action graphsync.LinkAction) {
	// if the last remote link was missing
	if !action.DidFollowLink() {
		// record the last known missing path
		pt.lastUnfollowedRemotePath = currentPath
	}
}
