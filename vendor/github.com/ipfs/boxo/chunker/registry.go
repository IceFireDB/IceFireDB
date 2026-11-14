package chunk

import (
	"io"
	"strings"
	"sync"
)

// SplitterFunc creates a [Splitter] from a reader and a specification
// string such as "mychunker-param1-param2". It is used to register
// custom chunkers via [Register] so they become available globally
// through [FromString]. The function is responsible for parsing and
// validating any parameters encoded in the string.
type SplitterFunc func(r io.Reader, chunker string) (Splitter, error)

var (
	splittersMu sync.RWMutex
	splitters   = map[string]SplitterFunc{}
)

func init() {
	Register("size", parseSizeString)
	Register("rabin", parseRabinString)
	Register("buzhash", parseBuzhashString)
}

// Register makes a custom chunker available to [FromString] under the given
// name. The name is matched against the portion of the chunker string before
// the first dash. For example, passing "mychunker-128" to [FromString]
// selects the chunker registered as "mychunker", and the [SplitterFunc]
// receives the full string "mychunker-128" so it can parse its own parameters.
//
// Register is typically called from an init function:
//
//	func init() {
//	    chunk.Register("mychunker", func(r io.Reader, s string) (chunk.Splitter, error) {
//	        // parse parameters from s, return a Splitter
//	    })
//	}
//
// Register panics if name is empty, contains a dash, fn is nil, or a
// chunker with the same name is already registered. This follows the
// convention established by [database/sql.Register].
//
// Register is safe for concurrent use.
func Register(name string, fn SplitterFunc) {
	splittersMu.Lock()
	defer splittersMu.Unlock()
	if name == "" {
		panic("chunk: Register name is empty")
	}
	if strings.Contains(name, "-") {
		panic("chunk: Register name must not contain a dash: " + name)
	}
	if fn == nil {
		panic("chunk: Register fn is nil")
	}
	if _, dup := splitters[name]; dup {
		panic("chunk: Register called twice for chunker " + name)
	}
	splitters[name] = fn
}
