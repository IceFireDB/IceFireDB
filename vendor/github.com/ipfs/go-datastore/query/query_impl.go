package query

import (
	"path"
)

// NaiveFilter applies a filter to the results.
func NaiveFilter(qr Results, filter Filter) Results {
	return ResultsFromIterator(qr.Query(), Iterator{
		Next: func() (Result, bool) {
			for {
				e, ok := qr.NextSync()
				if !ok {
					return Result{}, false
				}
				if e.Error != nil || filter.Filter(e.Entry) {
					return e, true
				}
			}
		},
		Close: func() error {
			return qr.Close()
		},
	})
}

// NaiveLimit truncates the results to a given int limit
func NaiveLimit(qr Results, limit int) Results {
	if limit == 0 {
		// 0 means no limit
		return qr
	}
	closed := false
	return ResultsFromIterator(qr.Query(), Iterator{
		Next: func() (Result, bool) {
			if limit == 0 {
				if !closed {
					closed = true
					qr.Close()
				}
				return Result{}, false
			}
			limit--
			return qr.NextSync()
		},
		Close: func() error {
			if closed {
				return nil
			}
			closed = true
			return qr.Close()
		},
	})
}

// NaiveOffset skips a given number of results
func NaiveOffset(qr Results, offset int) Results {
	return ResultsFromIterator(qr.Query(), Iterator{
		Next: func() (Result, bool) {
			for ; offset > 0; offset-- {
				res, ok := qr.NextSync()
				if !ok || res.Error != nil {
					return res, ok
				}
			}
			return qr.NextSync()
		},
		Close: func() error {
			return qr.Close()
		},
	})
}

// NaiveOrder reorders results according to given orders.
// WARNING: this is the only non-stream friendly operation!
func NaiveOrder(qr Results, orders ...Order) Results {
	// Short circuit.
	if len(orders) == 0 {
		return qr
	}

	var entries []Entry
	var errs []Result
	for res := range qr.Next() {
		if res.Error != nil {
			errs = append(errs, res)
			continue
		}
		entries = append(entries, res.Entry)
	}

	Sort(orders, entries)

	return ResultsFromIterator(qr.Query(), Iterator{
		Next: func() (Result, bool) {
			if len(errs) != 0 {
				errResult := errs[0]
				errs = errs[1:]
				return errResult, true
			}
			if len(entries) == 0 {
				return Result{}, false
			}
			next := entries[0]
			entries = entries[1:]
			return Result{Entry: next}, true
		},
	})
}

func NaiveQueryApply(q Query, qr Results) Results {
	if q.Prefix != "" {
		// Clean the prefix as a key and append / so a prefix of /bar
		// only finds /bar/baz, not /barbaz.
		prefix := q.Prefix
		if len(prefix) == 0 {
			prefix = "/"
		} else {
			if prefix[0] != '/' {
				prefix = "/" + prefix
			}
			prefix = path.Clean(prefix)
		}
		// If the prefix is empty, ignore it.
		if prefix != "/" {
			qr = NaiveFilter(qr, FilterKeyPrefix{prefix + "/"})
		}
	}
	for _, f := range q.Filters {
		qr = NaiveFilter(qr, f)
	}
	if len(q.Orders) > 0 {
		qr = NaiveOrder(qr, q.Orders...)
	}
	if q.Offset != 0 {
		qr = NaiveOffset(qr, q.Offset)
	}
	if q.Limit != 0 {
		qr = NaiveLimit(qr, q.Limit)
	}
	return qr
}

func ResultEntriesFrom(keys []string, vals [][]byte) []Entry {
	re := make([]Entry, len(keys))
	for i, k := range keys {
		re[i] = Entry{Key: k, Size: len(vals[i]), Value: vals[i]}
	}
	return re
}
