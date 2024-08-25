package nopfs

import (
	"fmt"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
)

// Status values
const (
	StatusNotFound Status = iota
	StatusBlocked
	StatusAllowed
	StatusErrored
)

// Status represent represents whether an item is blocked, allowed or simply
// not found in a Denylist.
type Status int

func (st Status) String() string {
	switch st {
	case StatusNotFound:
		return "not found"
	case StatusBlocked:
		return "blocked"
	case StatusAllowed:
		return "allowed"
	case StatusErrored:
		return "errored"
	}
	return "unknown"
}

// StatusResponse provides full information for a content-block lookup,
// including the Filename and the Entry, when an associated rule is found.
type StatusResponse struct {
	Cid      cid.Cid
	Path     path.Path
	Status   Status
	Filename string
	Entry    Entry
	Error    error
}

// String provides a string with the details of a StatusResponse.
func (r StatusResponse) String() string {
	if err := r.Error; err != nil {
		return err.Error()
	}

	path := ""
	if c := r.Cid; c.Defined() {
		path = c.String()
	} else {
		path = r.Path.String()
	}

	return fmt.Sprintf("%s: %s (%s:%d)",
		path, r.Status,
		r.Filename, r.Entry.Line,
	)
}

// ToError returns nil if the Status of the StatusResponse is Allowed or Not Found.
// When the status is Blocked or Errored, it returns a StatusError.
func (r StatusResponse) ToError() *StatusError {
	if r.Status != StatusBlocked && r.Status != StatusErrored {
		return nil
	}

	return &StatusError{Response: r}
}

// StatusError implements the error interface and can be used to provide
// information about a blocked-status in the form of an error.
type StatusError struct {
	Response StatusResponse
}

func (err *StatusError) Error() string {
	if err := err.Response.Error; err != nil {
		return err.Error()
	}
	if c := err.Response.Cid; c.Defined() {
		return c.String() + " is blocked and cannot be provided"
	}
	return err.Response.Path.String() + " is blocked and cannot be provided"
}
