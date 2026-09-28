package ipldgit

import (
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mh "github.com/multiformats/go-multihash"
)

// gitSHALen is the length of a git object hash. A reference of any other length
// cannot name a git object, and cidToSha assumes this width when it turns a CID
// back into one.
const gitSHALen = 20

func shaToCid(sha []byte) (cid.Cid, error) {
	if len(sha) != gitSHALen {
		return cid.Undef, fmt.Errorf("invalid git sha of %d bytes, expected %d", len(sha), gitSHALen)
	}
	h, err := mh.Encode(sha, mh.SHA1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.GitRaw, h), nil
}

func cidToSha(c cid.Cid) []byte {
	h := c.Hash()
	return h[len(h)-20:]
}

func sha(l ipld.Link) []byte {
	cl, ok := l.(cidlink.Link)
	if !ok {
		return nil
	}
	return cidToSha(cl.Cid)
}

func (l Link) sha() []byte {
	cl, ok := l.x.(cidlink.Link)
	if !ok {
		return nil
	}
	return cidToSha(cl.Cid)
}

func (l Tree_Link) sha() []byte {
	cl, ok := l.x.(cidlink.Link)
	if !ok {
		return nil
	}
	return cidToSha(cl.Cid)
}

func (l Commit_Link) sha() []byte {
	cl, ok := l.x.(cidlink.Link)
	if !ok {
		return nil
	}
	return cidToSha(cl.Cid)
}
