// go-ipld-prime is a series of go interfaces for manipulating IPLD data.
//
// See https://github.com/ipld/specs for more information about the basics
// of "What is IPLD?".
//
// See https://github.com/ipld/go-ipld-prime/tree/master/doc/README.md
// for more documentation about go-ipld-prime's architecture and usage.
//
// Here in the godoc, the first couple of types to look at should be:
//
//   - Node
//   - NodeBuilder (and NodeAssembler)
//
// These types provide a generic description of the data model.
//
// If working with linked data (data which is split into multiple
// trees of Nodes, loaded separately, and connected by some kind of
// "link" reference), the next types you should look at are:
//
//   - Link
//   - LinkBuilder
//   - Loader
//   - Storer
//
// All of these types are interfaces.  There are several implementations you
// can choose; we've provided some in subpackages, or you can bring your own.
//
// Particularly interesting subpackages include:
//
//   - node/* -- various Node + NodeBuilder implementations
//   - node/basic -- the first Node implementation you should try
//   - codec/* -- functions for serializing and deserializing Nodes
//   - linking/* -- various Link + LinkBuilder implementations
//   - traversal -- functions for walking Node graphs (including
//        automatic link loading) and visiting
//   - must -- helpful functions for streamlining error handling
//   - fluent -- alternative Node interfaces that flip errors to panics
//   - schema -- interfaces for working with IPLD Schemas and Nodes
//        which use Schema types and constraints
//
// Note that since interfaces in this package are the core of the library,
// choices made here maximize correctness and performance -- these choices
// are *not* always the choices that would maximize ergonomics.
// (Ergonomics can come on top; performance generally can't.)
// You can check out the 'must' or 'fluent' packages for more ergonomics;
// 'traversal' provides some ergnomics features for certain uses;
// any use of schemas with codegen tooling will provide more ergnomic options;
// or you can make your own function decorators that do what *you* need.
//
package ipld
