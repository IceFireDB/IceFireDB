/*
selectorparse package contains some helpful functions for parsing the serial form of Selectors.

Some common selectors are also exported as pre-compiled variables,
both for convenience of use and to be readable as examples.
*/
package selectorparse

import (
	"strings"

	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

// ParseJSONSelector accepts a string of json which will be parsed as a selector,
// and returns a datamodel.Node of the parsed Data Model.
// The returned datamodel.Node is suitable to hand to `selector.CompileSelector`,
// or, could be composed programmatically with other Data Model selector clauses
// and then compiled later.
//
// The selector will be checked for compileability, and an error returned if it is not.
func ParseJSONSelector(jsonStr string) (datamodel.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(nb, strings.NewReader(jsonStr)); err != nil {
		return nil, err
	}
	// Compile it, because that's where all of our error checking is right now.
	// ... but throw that result away, because the point of this method is to return nodes that you can compose further.
	// Ideally, we'd have just used Schemas for this check,
	// which would be cheaper than running the full compile,
	// and also more correct (because it would let us parse incomplete phrases that won't compile alone),
	// but that's not currently how the Selectors code is implemented.  Future work!
	n := nb.Build()
	if _, err := selector.CompileSelector(n); err != nil {
		return nil, err
	}
	return n, nil
}

// ParseJSONSelector accepts a string of json which will be parsed as a selector,
// and returns a compiled and ready-to-run Selector.
//
// ParseJSONSelector is functionally equivalent to combining ParseJSONSelector and CompileSelector into one step.
func ParseAndCompileJSONSelector(jsonStr string) (selector.Selector, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(nb, strings.NewReader(jsonStr)); err != nil {
		return nil, err
	}
	if s, err := selector.CompileSelector(nb.Build()); err != nil {
		return nil, err
	} else {
		return s, nil
	}
}

func must(s datamodel.Node, e error) datamodel.Node {
	if e != nil {
		panic(e)
	}
	return s
}

// CommonSelector_MatchPoint is a selector that matches exactly one thing: the first node it touches.
// It doesn't walk anywhere at all.
//
// This is not a very useful selector, but is an example of how selectors can be written.
var CommonSelector_MatchPoint = must(ParseJSONSelector(`{".":{}}`))

// CommonSelector_MatchChildren will examine the node it is applied to,
// walk to each of its children, and match the children.
// It does not recurse.
// Note that the root node itself is visited (necessarily!) but it is not "matched".
var CommonSelector_MatchChildren = must(ParseJSONSelector(`{"a":{">":{".":{}}}}`))

// CommonSelector_ExploreAllRecursively is a selector that walks over a graph of data,
// recursively, without limit (!) until it reaches every part of the graph.
// (This is safe to assume will halt eventually, because in IPLD, we work with DAGs --
// although it still may be a bad idea to do this in practice,
// because you could accidentally do this on terabytes of linked data, and that would still take a while!)
//
// It does not actually _match_ anything at all.
// That means if you're intercepting block loads (e.g. you're looking at calls to LinkSystem.StorageReadOpener), you'll see them;
// and if you're using `traversal.AdvVisitFn`, you'll still hear about nodes visited during the exploration;
// however, if you're using just `traversal.VisitFn`, nothing is considered "matched", so that callback will never be called.
var CommonSelector_ExploreAllRecursively = must(ParseJSONSelector(`{"R":{"l":{"none":{}},":>":{"a":{">":{"@":{}}}}}}`))

// CommonSelector_MatchAllRecursively is like CommonSelector_ExploreAllRecursively, but also matching everything it touches.
// The first thing inside the recursion is an ExploreUnion clause (which means the selection continues with multiple logical paths);
// the first thing inside that union clause is a Matcher clause;
// the second thing inside that union is the ExploreAll clause, which gets us deeper, and then that contains the ExploreRecursiveEdge.
var CommonSelector_MatchAllRecursively = must(ParseJSONSelector(`{"R":{"l":{"none":{}},":>":{"|":[{".":{}},{"a":{">":{"@":{}}}}]}}}`))
