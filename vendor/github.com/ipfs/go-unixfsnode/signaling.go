package unixfsnode

import (
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// ExploreAllRecursivelySelector is a selector that will explore all nodes. It
// is the same selector as selectorparse.CommonSelector_ExploreAllRecursively
// but it is precompiled for use with UnixFSPathSelectorBuilder().
var ExploreAllRecursivelySelector = specBuilder(func(ssb builder.SelectorSpecBuilder) builder.SelectorSpec {
	return ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	)
})

// MatchUnixFSPreloadSelector is a selector that will match a single node,
// similar to selectorparse.CommonSelector_MatchPoint, but uses the
// "unixfs-preload" ADL to load sharded files and directories as a single node.
// Can be used to shallow load an entire UnixFS directory listing, sharded or
// not, but not its contents.
// MatchUnixfsPreloadSelector is precompiled for use with
// UnixFSPathSelectorBuilder().
//
// NOTE: This selector may be deprecated in a future release. Users should
// instead use MatchUnixFSEntitySelector instead, which is intended to have the
// same effect but doesn't use the "unixfs-preload" ADL.
var MatchUnixFSPreloadSelector = specBuilder(func(ssb builder.SelectorSpecBuilder) builder.SelectorSpec {
	return ssb.ExploreInterpretAs("unixfs-preload", ssb.Matcher())
})

// MatchUnixFSEntitySelector is a selector that will match a single node and its
// direct children.
//
// For UnixFS files, this will match the file and its blocks.
//
// For UnixFS directories, and will iterate through the list of child links but
// will not iterate _into_ the child directories.
var MatchUnixFSEntitySelector = specBuilder(func(ssb builder.SelectorSpecBuilder) builder.SelectorSpec {
	return ssb.ExploreInterpretAs("unixfs", ssb.ExploreUnion(ssb.Matcher(),
		ssb.ExploreRecursive(
			selector.RecursionLimitDepth(1),
			ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		),
	))
})

// MatchUnixFSSelector is a selector that will match a single node, similar to
// selectorparse.CommonSelector_MatchPoint, but uses the "unixfs" ADL to load
// as UnixFS data. Unlike MatchUnixFSPreloadSelector, this selector will not
// preload all blocks in sharded directories or files. Use
// MatchUnixFSPreloadSelector where the blocks that constitute the full UnixFS
// resource being selected are important to load.
var MatchUnixFSSelector = specBuilder(func(ssb builder.SelectorSpecBuilder) builder.SelectorSpec {
	return ssb.ExploreInterpretAs("unixfs", ssb.Matcher())
})

// BytesConsumingMatcher is a traversal.WalkMatching matcher function that
// consumes the bytes of a LargeBytesNode where one is matched. Use this in
// conjunction with the Match* selectors in this package to ensure that all
// blocks of sharded files are loaded during a traversal, or that the subset
// of blocks required to fulful a range selector are loaded.
func BytesConsumingMatcher(p traversal.Progress, n datamodel.Node) error {
	if lbn, ok := n.(datamodel.LargeBytesNode); ok {
		rdr, err := lbn.AsLargeBytes()
		if err != nil {
			return err
		}
		_, err = io.Copy(io.Discard, rdr)
		return err
	}
	return nil
}

// AddUnixFSReificationToLinkSystem will add both unixfs and unixfs-preload
// reifiers to a LinkSystem. This is primarily useful for traversals that use
// an interpretAs clause, such as Match* selectors in this package.
func AddUnixFSReificationToLinkSystem(lsys *ipld.LinkSystem) {
	if lsys.KnownReifiers == nil {
		lsys.KnownReifiers = make(map[string]linking.NodeReifier)
	}
	lsys.KnownReifiers["unixfs"] = Reify
	lsys.KnownReifiers["unixfs-preload"] = nonLazyReify
}

// UnixFSPathSelector creates a selector for IPLD path to a UnixFS resource if
// UnixFS reification is setup on a LinkSystem being used for traversal.
//
// Use UnixFSPathSelectorBuilder for more control over the selector, this
// function is the same as calling
//
//	UnixFSPathSelectorBuilder(path, MatchUnixFSSelector, false)
func UnixFSPathSelector(path string) datamodel.Node {
	return UnixFSPathSelectorBuilder(path, MatchUnixFSSelector, false)
}

// UnixFSPathSelectorBuilder creates a selector for IPLD path to a UnixFS
// resource if UnixFS reification is setup on a LinkSystem being used for
// traversal.
//
// The path is interpreted according to
// github.com/ipld/go-ipld-prime/datamodel/Path rules,
// i.e.
//   - leading and trailing slashes are ignored
//   - redundant slashes are ignored
//   - the segment `..` is a field named `..`, same with `.`
//
// targetSelector is the selector to apply to the final node in the path.
// Use ExploreAllRecursivelySelector to explore (i.e. load the blocks) all of
// the content from the terminus of the path. Use MatchUnixFSPreloadSelector to
// match the terminus of the path, but preload all blocks in sharded files and
// directories. Use MatchUnixFSSelector to match the terminus of the path, but
// not preload any blocks if the terminus is sharded. Or any other custom
// SelectorSpec can be supplied.
//
// If matchPath is false, the selector will explore, not match, so it's useful
// for traversals where block loads are important, not where the matcher visitor
// callback is important. if matchPath is true, the selector will match the
// nodes along the path while exploring them.
func UnixFSPathSelectorBuilder(path string, targetSelector builder.SelectorSpec, matchPath bool) ipld.Node {
	segments := ipld.ParsePath(path)

	ss := targetSelector
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	for segments.Len() > 0 {
		// Wrap selector in ExploreFields as we walk back up through the path.
		// We can assume each segment to be a unixfs path section, so we
		// InterpretAs to make sure the node is reified through go-unixfsnode
		// (if possible) and we can traverse through according to unixfs pathing
		// rather than bare IPLD pathing - which also gives us the ability to
		// traverse through HAMT shards.
		ss = ssb.ExploreInterpretAs("unixfs", ssb.ExploreFields(
			func(efsb builder.ExploreFieldsSpecBuilder) {
				efsb.Insert(segments.Last().String(), ss)
			},
		))
		if matchPath {
			ss = ssb.ExploreUnion(ssb.Matcher(), ss)
		}
		segments = segments.Pop()
	}

	return ss.Node()
}

func specBuilder(b func(ssb builder.SelectorSpecBuilder) builder.SelectorSpec) builder.SelectorSpec {
	return b(builder.NewSelectorSpecBuilder(basicnode.Prototype.Any))
}
