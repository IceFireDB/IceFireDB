package traversalrecord

import (
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/ipfs/go-graphsync"
)

// TraversalRecord records the links traversed by a selector and their paths in a space efficient manner
type TraversalRecord struct {
	link          *cid.Cid
	successful    bool
	childSegments map[datamodel.PathSegment]int
	children      []*traversalLink
}

type traversalLink struct {
	segment datamodel.PathSegment
	*TraversalRecord
}

// NewTraversalRecord returns a new traversal record
func NewTraversalRecord() *TraversalRecord {
	return &TraversalRecord{
		childSegments: make(map[datamodel.PathSegment]int),
	}
}

// RecordNextStep records the next step in the traversal into the tree
// based on its path, link, and whether the load was successful or not
func (tr *TraversalRecord) RecordNextStep(p []datamodel.PathSegment, link cid.Cid, successful bool) {
	if len(p) == 0 {
		tr.link = &link
		tr.successful = successful
		return
	}
	if _, ok := tr.childSegments[p[0]]; !ok {
		child := traversalLink{
			TraversalRecord: NewTraversalRecord(),
			segment:         p[0],
		}
		tr.childSegments[p[0]] = len(tr.children)
		tr.children = append(tr.children, &child)
	}
	tr.children[tr.childSegments[p[0]]].RecordNextStep(p[1:], link, successful)
}

// AllLinks returns all links traversed for a given record
func (tr *TraversalRecord) AllLinks() []cid.Cid {
	if len(tr.children) == 0 {
		return []cid.Cid{*tr.link}
	}
	links := make([]cid.Cid, 0)
	if tr.link != nil {
		links = append(links, *tr.link)
	}
	for _, v := range tr.children {
		links = append(links, v.AllLinks()...)
	}
	return links
}

// GetLinks returns all links starting at the path in the tree rooted at 'root'
func (tr *TraversalRecord) GetLinks(root datamodel.Path) []cid.Cid {
	segs := root.Segments()
	switch len(segs) {
	case 0:
		if tr.link != nil {
			return []cid.Cid{*tr.link}
		}
		return []cid.Cid{}
	case 1:
		// base case 1: get all paths below this child.
		next := segs[0]
		if childIndex, ok := tr.childSegments[next]; ok {
			return tr.children[childIndex].AllLinks()
		}
		return []cid.Cid{}
	default:
	}

	next := segs[0]
	if _, ok := tr.childSegments[next]; !ok {
		// base case 2: not registered sub-path.
		return []cid.Cid{}
	}
	return tr.children[tr.childSegments[next]].GetLinks(datamodel.NewPathNocopy(segs[1:]))
}

// Verifier allows you to verify series of links loads matches a previous traversal
// order when those loads are successful
// At any point it can reconstruct the current path.
type Verifier struct {
	stack []*traversalLink
}

func NewVerifier(root *TraversalRecord) *Verifier {
	v := &Verifier{
		stack: []*traversalLink{{TraversalRecord: root}},
	}
	v.appendUntilLink()
	return v
}

func (v *Verifier) tip() *traversalLink {
	if len(v.stack) == 0 {
		return nil
	}
	return v.stack[len(v.stack)-1]
}

func (v *Verifier) appendUntilLink() {
	for v.tip().link == nil && len(v.tip().children) > 0 {
		v.stack = append(v.stack, v.tip().children[0])
	}
}

func (v *Verifier) nextLink(exploreChildren bool) {
	last := v.tip()
	if len(last.children) > 0 && exploreChildren {
		v.stack = append(v.stack, last.children[0])
		v.appendUntilLink()
		return
	}
	// pop the stack
	v.stack = v.stack[:len(v.stack)-1]
	if len(v.stack) == 0 {
		return
	}
	parent := v.tip()
	// find this segments index
	childIndex := parent.childSegments[last.segment]
	// if this is the last child, parents next sibling
	if childIndex == len(parent.children)-1 {
		v.nextLink(false)
		return
	}
	// otherwise go to next sibling
	v.stack = append(v.stack, parent.children[childIndex+1])
	v.appendUntilLink()
}

func (v *Verifier) CurrentPath() datamodel.Path {
	if v.Done() {
		return datamodel.NewPathNocopy(nil)
	}
	segments := make([]datamodel.PathSegment, 0, len(v.stack)-1)
	for i, seg := range v.stack {
		if i == 0 {
			continue
		}
		segments = append(segments, seg.segment)
	}
	return datamodel.NewPathNocopy(segments)
}

func (v *Verifier) Done() bool {
	return len(v.stack) == 0 || (len(v.stack) == 1 && v.stack[0].link == nil)
}

func (v *Verifier) VerifyNext(link cid.Cid, successful bool) error {
	if v.Done() {
		return errors.New("nothing left to verify")
	}
	next := v.tip()
	if !next.link.Equals(link) {
		return graphsync.RemoteIncorrectResponseError{
			LocalLink:  cidlink.Link{Cid: *next.link},
			RemoteLink: cidlink.Link{Cid: link},
			Path:       v.CurrentPath(),
		}
	}
	if !next.successful && successful {
		return errors.New("verifying against tree with additional data not possible")
	}
	v.nextLink(successful)
	return nil
}
