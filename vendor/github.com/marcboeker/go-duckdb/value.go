package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"unsafe"
)

func getValue(info TypeInfo, v C.duckdb_value) (any, error) {
	t := info.InternalType()
	switch t {
	case TYPE_BOOLEAN:
		return bool(C.duckdb_get_bool(v)), nil
	case TYPE_TINYINT:
		return int8(C.duckdb_get_int8(v)), nil
	case TYPE_SMALLINT:
		return int16(C.duckdb_get_int16(v)), nil
	case TYPE_INTEGER:
		return int32(C.duckdb_get_int32(v)), nil
	case TYPE_BIGINT:
		return int64(C.duckdb_get_int64(v)), nil
	case TYPE_UTINYINT:
		return uint8(C.duckdb_get_uint8(v)), nil
	case TYPE_USMALLINT:
		return uint16(C.duckdb_get_uint16(v)), nil
	case TYPE_UINTEGER:
		return uint32(C.duckdb_get_uint32(v)), nil
	case TYPE_UBIGINT:
		return uint64(C.duckdb_get_uint64(v)), nil
	case TYPE_FLOAT:
		return float32(C.duckdb_get_float(v)), nil
	case TYPE_DOUBLE:
		return float64(C.duckdb_get_double(v)), nil
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS:
		// DuckDB's C API does not yet support get_timestamp_s|ms|ns.
		return nil, unsupportedTypeError(typeToStringMap[t])
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		ts := C.duckdb_get_timestamp(v)
		return getTS(t, ts), nil
	case TYPE_DATE:
		date := C.duckdb_get_date(v)
		return getDate(date), nil
	case TYPE_TIME:
		ti := C.duckdb_get_time(v)
		return getTime(ti), nil
	case TYPE_TIME_TZ:
		ti := C.duckdb_get_time_tz(v)
		return getTimeTZ(ti), nil
	case TYPE_INTERVAL:
		interval := C.duckdb_get_interval(v)
		return getInterval(interval), nil
	case TYPE_HUGEINT:
		hugeint := C.duckdb_get_hugeint(v)
		return hugeIntToNative(hugeint), nil
	case TYPE_VARCHAR:
		str := C.duckdb_get_varchar(v)
		ret := C.GoString(str)
		C.duckdb_free(unsafe.Pointer(str))
		return ret, nil
	default:
		return nil, unsupportedTypeError(typeToStringMap[t])
	}
}
