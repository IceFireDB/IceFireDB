package duckdb

/*
#include <duckdb.h>
*/
import "C"

// Row represents one row in duckdb. It references the internal vectors.
type Row struct {
	chunk      *DataChunk
	r          C.idx_t
	projection []int
}

// IsProjected returns whether the column is projected.
func (r Row) IsProjected(colIdx int) bool {
	return r.projection[colIdx] != -1
}

// SetRowValue sets the value at colIdx to val.
// Returns an error on failure, and nil for non-projected columns.
func SetRowValue[T any](row Row, colIdx int, val T) error {
	projectedIdx := row.projection[colIdx]
	if projectedIdx < 0 || projectedIdx >= len(row.chunk.columns) {
		return nil
	}
	vec := row.chunk.columns[projectedIdx]
	return setVectorVal(&vec, row.r, val)
}

// SetRowValue sets the value at colIdx to val. Returns an error on failure.
func (r Row) SetRowValue(colIdx int, val any) error {
	if !r.IsProjected(colIdx) {
		return nil
	}
	return r.chunk.SetValue(colIdx, int(r.r), val)
}
