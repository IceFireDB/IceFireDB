package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"encoding/json"
	"math/big"
	"time"
	"unsafe"
)

// fnGetVectorValue is the getter callback function for any (nested) vector.
type fnGetVectorValue func(vec *vector, rowIdx C.idx_t) any

func (vec *vector) getNull(rowIdx C.idx_t) bool {
	mask := unsafe.Pointer(vec.mask)
	if mask == nil {
		return false
	}

	entryIdx := rowIdx / 64
	idxInEntry := rowIdx % 64
	maskPtr := (*[1 << 31]C.uint64_t)(mask)
	isValid := maskPtr[entryIdx] & (C.uint64_t(1) << idxInEntry)
	return uint64(isValid) == 0
}

func getPrimitive[T any](vec *vector, rowIdx C.idx_t) T {
	xs := (*[1 << 31]T)(vec.ptr)
	return xs[rowIdx]
}

func (vec *vector) getTS(t Type, rowIdx C.idx_t) time.Time {
	val := getPrimitive[C.duckdb_timestamp](vec, rowIdx)
	return getTS(t, val)
}

func getTS(t Type, ts C.duckdb_timestamp) time.Time {
	switch t {
	case TYPE_TIMESTAMP:
		return time.UnixMicro(int64(ts.micros)).UTC()
	case TYPE_TIMESTAMP_S:
		return time.Unix(int64(ts.micros), 0).UTC()
	case TYPE_TIMESTAMP_MS:
		return time.UnixMilli(int64(ts.micros)).UTC()
	case TYPE_TIMESTAMP_NS:
		return time.Unix(0, int64(ts.micros)).UTC()
	case TYPE_TIMESTAMP_TZ:
		return time.UnixMicro(int64(ts.micros)).UTC()
	}
	return time.Time{}
}

func (vec *vector) getDate(rowIdx C.idx_t) time.Time {
	date := getPrimitive[C.duckdb_date](vec, rowIdx)
	return getDate(date)
}

func getDate(date C.duckdb_date) time.Time {
	d := C.duckdb_from_date(date)
	return time.Date(int(d.year), time.Month(d.month), int(d.day), 0, 0, 0, 0, time.UTC)
}

func (vec *vector) getTime(rowIdx C.idx_t) time.Time {
	switch vec.Type {
	case TYPE_TIME:
		val := getPrimitive[C.duckdb_time](vec, rowIdx)
		return getTime(val)
	case TYPE_TIME_TZ:
		ti := getPrimitive[C.duckdb_time_tz](vec, rowIdx)
		return getTimeTZ(ti)
	}
	return time.Time{}
}

func getTime(ti C.duckdb_time) time.Time {
	unix := time.UnixMicro(int64(ti.micros)).UTC()
	return time.Date(1, time.January, 1, unix.Hour(), unix.Minute(), unix.Second(), unix.Nanosecond(), time.UTC)
}

func getTimeTZ(ti C.duckdb_time_tz) time.Time {
	timeTZ := C.duckdb_from_time_tz(ti)
	hour := int(timeTZ.time.hour)
	minute := int(timeTZ.time.min)
	sec := int(timeTZ.time.sec)
	// TIMETZ has microsecond precision.
	nanos := int(timeTZ.time.micros) * 1000
	loc := time.FixedZone("", int(timeTZ.offset))
	return time.Date(1, time.January, 1, hour, minute, sec, nanos, loc).UTC()
}

func (vec *vector) getInterval(rowIdx C.idx_t) Interval {
	interval := getPrimitive[C.duckdb_interval](vec, rowIdx)
	return getInterval(interval)
}

func getInterval(interval C.duckdb_interval) Interval {
	return Interval{
		Days:   int32(interval.days),
		Months: int32(interval.months),
		Micros: int64(interval.micros),
	}
}

func (vec *vector) getHugeint(rowIdx C.idx_t) *big.Int {
	hugeInt := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
	return hugeIntToNative(hugeInt)
}

func (vec *vector) getBytes(rowIdx C.idx_t) any {
	cStr := getPrimitive[duckdb_string_t](vec, rowIdx)

	var blob []byte
	if cStr.length <= stringInlineLength {
		// Inlined data is stored from byte 4 to stringInlineLength + 4.
		blob = C.GoBytes(unsafe.Pointer(&cStr.prefix), C.int(cStr.length))
	} else {
		// Any strings exceeding stringInlineLength are stored as a pointer in `ptr`.
		blob = C.GoBytes(unsafe.Pointer(cStr.ptr), C.int(cStr.length))
	}

	if vec.Type == TYPE_VARCHAR {
		return string(blob)
	}
	return blob
}

func (vec *vector) getJSON(rowIdx C.idx_t) any {
	bytes := vec.getBytes(rowIdx).(string)
	var value any
	_ = json.Unmarshal([]byte(bytes), &value)
	return value
}

func (vec *vector) getDecimal(rowIdx C.idx_t) Decimal {
	var val *big.Int
	switch vec.internalType {
	case TYPE_SMALLINT:
		v := getPrimitive[int16](vec, rowIdx)
		val = big.NewInt(int64(v))
	case TYPE_INTEGER:
		v := getPrimitive[int32](vec, rowIdx)
		val = big.NewInt(int64(v))
	case TYPE_BIGINT:
		v := getPrimitive[int64](vec, rowIdx)
		val = big.NewInt(v)
	case TYPE_HUGEINT:
		v := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
		val = hugeIntToNative(C.duckdb_hugeint{
			lower: v.lower,
			upper: v.upper,
		})
	}

	return Decimal{Width: vec.decimalWidth, Scale: vec.decimalScale, Value: val}
}

func (vec *vector) getEnum(rowIdx C.idx_t) string {
	var idx uint64
	switch vec.internalType {
	case TYPE_UTINYINT:
		idx = uint64(getPrimitive[uint8](vec, rowIdx))
	case TYPE_USMALLINT:
		idx = uint64(getPrimitive[uint16](vec, rowIdx))
	case TYPE_UINTEGER:
		idx = uint64(getPrimitive[uint32](vec, rowIdx))
	case TYPE_UBIGINT:
		idx = getPrimitive[uint64](vec, rowIdx)
	}

	logicalType := C.duckdb_vector_get_column_type(vec.duckdbVector)
	defer C.duckdb_destroy_logical_type(&logicalType)

	val := C.duckdb_enum_dictionary_value(logicalType, (C.idx_t)(idx))
	defer C.duckdb_free(unsafe.Pointer(val))
	return C.GoString(val)
}

func (vec *vector) getList(rowIdx C.idx_t) []any {
	entry := getPrimitive[duckdb_list_entry_t](vec, rowIdx)
	return vec.getSliceChild(entry.offset, entry.length)
}

func (vec *vector) getStruct(rowIdx C.idx_t) map[string]any {
	m := map[string]any{}
	for i := 0; i < len(vec.childVectors); i++ {
		child := &vec.childVectors[i]
		val := child.getFn(child, rowIdx)
		m[vec.structEntries[i].Name()] = val
	}
	return m
}

func (vec *vector) getMap(rowIdx C.idx_t) Map {
	list := vec.getList(rowIdx)

	m := Map{}
	for i := 0; i < len(list); i++ {
		mapItem := list[i].(map[string]any)
		key := mapItem[mapKeysField()]
		val := mapItem[mapValuesField()]
		m[key] = val
	}
	return m
}

func (vec *vector) getArray(rowIdx C.idx_t) []any {
	length := C.idx_t(vec.arrayLength)
	return vec.getSliceChild(rowIdx*length, length)
}

func (vec *vector) getSliceChild(offset C.idx_t, length C.idx_t) []any {
	slice := make([]any, 0, length)
	child := &vec.childVectors[0]

	// Fill the slice with all child values.
	for i := C.idx_t(0); i < length; i++ {
		val := child.getFn(child, i+offset)
		slice = append(slice, val)
	}
	return slice
}
