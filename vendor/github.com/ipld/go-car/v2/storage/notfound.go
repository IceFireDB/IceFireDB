package storage

import "github.com/ipfs/go-cid"

// compatible with the go-ipld-format ErrNotFound, match against
// interface{NotFound() bool}
// this could go into go-ipld-prime, but for now we'll just exercise the
// feature-test pattern

type ErrNotFound struct {
	Cid cid.Cid
}

func (e ErrNotFound) Error() string {
	if e.Cid == cid.Undef {
		return "ipld: could not find node"
	}
	return "ipld: could not find " + e.Cid.String()
}

func (e ErrNotFound) NotFound() bool {
	return true
}

func (e ErrNotFound) Is(err error) bool {
	switch err.(type) {
	case ErrNotFound:
		return true
	default:
		return false
	}
}

func IsNotFound(err error) bool {
	if nf, ok := err.(interface{ NotFound() bool }); ok {
		return nf.NotFound()
	}
	return false
}
