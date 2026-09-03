// Package fetcher defines interfaces for reading data from a DAG.
//
// A [Fetcher] traverses node graphs using IPLD selectors, optionally crossing
// block boundaries. Reads may be local or remote, using data exchange protocols
// like Bitswap. Use [Factory] to create session-scoped fetchers.
package fetcher
