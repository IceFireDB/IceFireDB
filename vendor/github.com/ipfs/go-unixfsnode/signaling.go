package unixfsnode

import (
	"strings"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

func AddUnixFSReificationToLinkSystem(lsys *ipld.LinkSystem) {
	if lsys.KnownReifiers == nil {
		lsys.KnownReifiers = make(map[string]linking.NodeReifier)
	}
	lsys.KnownReifiers["unixfs"] = Reify
}

// UnixFSPathSelector creates a selector for a file/path inside of a UnixFS directory
// if reification is setup on a link system
func UnixFSPathSelector(path string) datamodel.Node {
	segments := strings.Split(path, "/")
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	selectorSoFar := ssb.ExploreInterpretAs("unixfs", ssb.Matcher())
	for i := len(segments) - 1; i >= 0; i-- {
		selectorSoFar = ssb.ExploreInterpretAs("unixfs",
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments[i], selectorSoFar)
			}),
		)
	}
	return selectorSoFar.Node()
}
