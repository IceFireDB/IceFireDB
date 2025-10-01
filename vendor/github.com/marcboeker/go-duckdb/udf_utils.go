package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"runtime"
	"runtime/cgo"
	"unsafe"
)

// Helpers for passing values to C and back.

type pinnedValue[T any] struct {
	pinner *runtime.Pinner
	value  T
}

type unpinner interface {
	unpin()
}

func (v pinnedValue[T]) unpin() {
	v.pinner.Unpin()
}

func getPinned[T any](handle unsafe.Pointer) T {
	h := *(*cgo.Handle)(handle)
	return h.Value().(pinnedValue[T]).value
}

// Set error helpers.

func setBindError(info C.duckdb_bind_info, msg string) {
	err := C.CString(msg)
	defer C.duckdb_free(unsafe.Pointer(err))
	C.duckdb_bind_set_error(info, err)
}

func setFuncError(function_info C.duckdb_function_info, msg string) {
	err := C.CString(msg)
	defer C.duckdb_free(unsafe.Pointer(err))
	C.duckdb_scalar_function_set_error(function_info, err)
}

// Data deletion handlers.

//export udf_delete_callback
func udf_delete_callback(info unsafe.Pointer) {
	h := (*cgo.Handle)(info)
	h.Value().(unpinner).unpin()
	h.Delete()
}
