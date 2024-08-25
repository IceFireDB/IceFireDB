package nopfs

import (
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"go.uber.org/multierr"
)

// A Blocker binds together multiple Denylists and can decide whether a path
// or a CID is blocked.
type Blocker struct {
	Denylists map[string]*Denylist
}

// NewBlocker creates a Blocker using the given denylist file paths.
// For default denylist locations, you can use GetDenylistFiles().
// TODO: Options.
func NewBlocker(files []string) (*Blocker, error) {

	blocker := Blocker{
		Denylists: make(map[string]*Denylist),
	}

	var errors error
	for _, fname := range files {
		dl, err := NewDenylist(fname, true)
		if err != nil {
			errors = multierr.Append(errors, err)
			logger.Errorf("error opening and processing %s: %s", fname, err)

			continue
		}
		blocker.Denylists[fname] = dl
	}

	if n := len(multierr.Errors(errors)); n > 0 && n == len(files) {
		return nil, errors
	}
	return &blocker, nil
}

// Close stops all denylists from being processed and watched for updates.
func (blocker *Blocker) Close() error {
	var err error
	for _, dl := range blocker.Denylists {
		err = multierr.Append(err, dl.Close())
	}
	return err
}

// IsCidBlocked returns blocking status for a CID. A CID is blocked when a
// Denylist reports it as blocked. A CID is not blocked when no denylist
// reports it as blocked or it is explicitally allowed. Lookup stops as soon
// as a defined "blocked" or "allowed" status is found.
//
// Lookup for "allowed" or "blocked" status happens in order of the denylist,
// thus the denylist position during Blocker creation affects which one has
// preference.
//
// Note that StatusResponse.Path will be unset. See Denylist.IsCidBlocked()
// for more info.
func (blocker *Blocker) IsCidBlocked(c cid.Cid) StatusResponse {
	for _, dl := range blocker.Denylists {
		resp := dl.IsCidBlocked(c)
		if resp.Status != StatusNotFound {
			return resp
		}
	}
	return StatusResponse{
		Cid:    c,
		Status: StatusNotFound,
	}
}

// IsPathBlocked returns blocking status for an IPFS Path. A Path is blocked
// when a Denylist reports it as blocked. A Path is not blocked when no
// denylist reports it as blocked or it is explicitally allowed. Lookup stops
// as soon as a defined "blocked" or "allowed" status is found.
//
// Lookup for "allowed" or "blocked" status happens in order of the denylist,
// thus the denylist position during Blocker creation affects which one has
// preference.
//
// Note that StatusResponse.Cid will be unset. See Denylist.IsPathBlocked()
// for more info.
func (blocker *Blocker) IsPathBlocked(p path.Path) StatusResponse {
	for _, dl := range blocker.Denylists {
		resp := dl.IsPathBlocked(p)
		if resp.Status != StatusNotFound {
			return resp
		}
	}
	return StatusResponse{
		Path:   p,
		Status: StatusNotFound,
	}
}
