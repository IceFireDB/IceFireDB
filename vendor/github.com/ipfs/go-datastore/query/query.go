package query

import (
	"context"
	"fmt"
	"time"
)

const (
	NormalBufSize   = 1
	KeysOnlyBufSize = 128
)

/*
Query represents storage for any key-value pair.

tl;dr:

	queries are supported across datastores.
	Cheap on top of relational dbs, and expensive otherwise.
	Pick the right tool for the job!

In addition to the key-value store get and set semantics, datastore
provides an interface to retrieve multiple records at a time through
the use of queries. The datastore Query model gleans a common set of
operations performed when querying. To avoid pasting here years of
database research, let’s summarize the operations datastore supports.

Query Operations, applied in-order:

  - prefix - scope the query to a given path prefix
  - filters - select a subset of values by applying constraints
  - orders - sort the results by applying sort conditions, hierarchically.
  - offset - skip a number of results (for efficient pagination)
  - limit - impose a numeric limit on the number of results

Datastore combines these operations into a simple Query class that allows
applications to define their constraints in a simple, generic, way without
introducing datastore specific calls, languages, etc.

However, take heed: not all datastores support efficiently performing these
operations. Pick a datastore based on your needs. If you need efficient look-ups,
go for a simple key/value store. If you need efficient queries, consider an SQL
backed datastore.

Notes:

  - Prefix: When a query filters by prefix, it selects keys that are strict
    children of the prefix. For example, a prefix "/foo" would select "/foo/bar"
    but not "/foobar" or "/foo",
  - Orders: Orders are applied hierarchically. Results are sorted by the first
    ordering, then entries equal under the first ordering are sorted with the
    second ordering, etc.
  - Limits & Offset: Limits and offsets are applied after everything else.
*/
type Query struct {
	Prefix            string   // namespaces the query to results whose keys have Prefix
	Filters           []Filter // filter results. apply sequentially
	Orders            []Order  // order results. apply hierarchically
	Limit             int      // maximum number of results
	Offset            int      // skip given number of results
	KeysOnly          bool     // return only keys.
	ReturnExpirations bool     // return expirations (see TTLDatastore)
	ReturnsSizes      bool     // always return sizes. If not set, datastore impl can return
	//                         // it anyway if it doesn't involve a performance cost. If KeysOnly
	//                         // is not set, Size should always be set.
}

// String returns a string representation of the Query for debugging/validation
// purposes. Do not use it for SQL queries.
func (q Query) String() string {
	s := "SELECT keys"
	if !q.KeysOnly {
		s += ",vals"
	}
	if q.ReturnExpirations {
		s += ",exps"
	}

	s += " "

	if q.Prefix != "" {
		s += fmt.Sprintf("FROM %q ", q.Prefix)
	}

	if len(q.Filters) > 0 {
		s += fmt.Sprintf("FILTER [%s", q.Filters[0])
		for _, f := range q.Filters[1:] {
			s += fmt.Sprintf(", %s", f)
		}
		s += "] "
	}

	if len(q.Orders) > 0 {
		s += fmt.Sprintf("ORDER [%s", q.Orders[0])
		for _, f := range q.Orders[1:] {
			s += fmt.Sprintf(", %s", f)
		}
		s += "] "
	}

	if q.Offset > 0 {
		s += fmt.Sprintf("OFFSET %d ", q.Offset)
	}

	if q.Limit > 0 {
		s += fmt.Sprintf("LIMIT %d ", q.Limit)
	}
	// Will always end with a space, strip it.
	return s[:len(s)-1]
}

// Entry is a query result entry.
type Entry struct {
	Key        string    // cant be ds.Key because circular imports ...!!!
	Value      []byte    // Will be nil if KeysOnly has been passed.
	Expiration time.Time // Entry expiration timestamp if requested and supported (see TTLDatastore).
	Size       int       // Might be -1 if the datastore doesn't support listing the size with KeysOnly
	//                   // or if ReturnsSizes is not set
}

// Result is a special entry that includes an error, so that the client
// may be warned about internal errors. If Error is non-nil, Entry must be
// empty.
type Result struct {
	Entry

	Error error
}

// Results is a set of Query results. This is the interface for clients.
// Example:
//
//	qr, _ := myds.Query(q)
//	for r := range qr.Next() {
//	  if r.Error != nil {
//	    // handle.
//	    break
//	  }
//
//	  fmt.Println(r.Entry.Key, r.Entry.Value)
//	}
//
// or, wait on all results at once:
//
//	qr, _ := myds.Query(q)
//	es, _ := qr.Rest()
//	for _, e := range es {
//	  	fmt.Println(e.Key, e.Value)
//	}
type Results interface {
	Query() Query             // the query these Results correspond to
	Next() <-chan Result      // returns a channel to wait for the next result
	NextSync() (Result, bool) // blocks and waits to return the next result, second parameter returns false when results are exhausted
	Rest() ([]Entry, error)   // waits till processing finishes, returns all entries at once.
	Close() error             // client may call Close to signal early exit
	Done() <-chan struct{}    // signals that Results is closed
}

// results implements Results
type results struct {
	query Query
	res   <-chan Result

	cancel context.CancelFunc
	closed chan struct{}
}

func (r *results) Next() <-chan Result {
	return r.res
}

func (r *results) NextSync() (Result, bool) {
	val, ok := <-r.res
	return val, ok
}

func (r *results) Rest() ([]Entry, error) {
	var es []Entry
	for e := range r.res {
		if e.Error != nil {
			return es, e.Error
		}
		es = append(es, e.Entry)
	}
	<-r.Done() // wait till the processing finishes.
	return es, nil
}

func (r *results) Close() error {
	r.cancel()
	<-r.closed
	return nil
}

func (r *results) Query() Query {
	return r.query
}

func (r *results) Done() <-chan struct{} {
	return r.closed
}

// ResultsWithContext returns a Results object with the results generated by
// the passed proc function called in a separate goroutine.
func ResultsWithContext(q Query, proc func(context.Context, chan<- Result)) Results {
	bufSize := NormalBufSize
	if q.KeysOnly {
		bufSize = KeysOnlyBufSize
	}
	output := make(chan Result, bufSize)
	closed := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		proc(ctx, output)
		close(output)
		close(closed)
	}()

	return &results{
		query:  q,
		res:    output,
		cancel: cancel,
		closed: closed,
	}
}

// ResultsWithEntries returns a Results object from a list of entries
func ResultsWithEntries(q Query, res []Entry) Results {
	i := 0
	return ResultsFromIterator(q, Iterator{
		Next: func() (Result, bool) {
			if i >= len(res) {
				return Result{}, false
			}
			next := res[i]
			i++
			return Result{Entry: next}, true
		},
	})
}

func ResultsReplaceQuery(r Results, q Query) Results {
	switch r := r.(type) {
	case *results:
		// note: not using field names to make sure all fields are copied
		return &results{q, r.res, r.cancel, r.closed}
	case *resultsIter:
		// note: not using field names to make sure all fields are copied
		oldr := r.results
		if oldr != nil {
			oldr = &results{q, oldr.res, oldr.cancel, oldr.closed}
		}
		return &resultsIter{q, r.next, r.close, oldr}
	default:
		panic("unknown results type")
	}
}

//
// ResultFromIterator provides an alternative way to to construct
// results without the use of channels.
//

func ResultsFromIterator(q Query, iter Iterator) Results {
	if iter.Close == nil {
		iter.Close = noopClose
	}
	return &resultsIter{
		query: q,
		next:  iter.Next,
		close: iter.Close,
	}
}

func noopClose() error { return nil }

type Iterator struct {
	Next  func() (Result, bool)
	Close func() error // note: might be called more than once
}

type resultsIter struct {
	query   Query
	next    func() (Result, bool)
	close   func() error
	results *results
}

func (r *resultsIter) Next() <-chan Result {
	r.collectResults()
	return r.results.Next()
}

func (r *resultsIter) NextSync() (Result, bool) {
	if r.results != nil {
		return r.results.NextSync()
	}
	res, ok := r.next()
	if !ok {
		r.close()
	}
	return res, ok
}

func (r *resultsIter) Rest() ([]Entry, error) {
	var es []Entry
	for {
		e, ok := r.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			return es, e.Error
		}
		es = append(es, e.Entry)
	}
	return es, nil
}

func (r *resultsIter) Close() error {
	if r.results != nil {
		// Close results collector. It will call r.close().
		r.results.Close()
	} else {
		// Call r.close() since there is no collector to call it when closed.
		r.close()
	}
	return nil
}

func (r *resultsIter) Query() Query {
	return r.query
}

func (r *resultsIter) Done() <-chan struct{} {
	r.collectResults()
	return r.results.Done()
}

func (r *resultsIter) collectResults() {
	if r.results != nil {
		return
	}

	// go consume all the entries and add them to the results.
	r.results = ResultsWithContext(r.query, func(ctx context.Context, out chan<- Result) {
		defer r.close()
		for {
			e, ok := r.next()
			if !ok {
				return
			}
			select {
			case out <- e:
			case <-ctx.Done(): // client told us to close early
				return
			}
		}
	}).(*results)
}
