package duckdb

/*
#include <duckdb.h>

void table_udf_bind_row(duckdb_bind_info info);
void table_udf_bind_chunk(duckdb_bind_info info);
void table_udf_bind_parallel_row(duckdb_bind_info info);
void table_udf_bind_parallel_chunk(duckdb_bind_info info);

void table_udf_init(duckdb_init_info info);
void table_udf_init_parallel(duckdb_init_info info);
void table_udf_local_init(duckdb_init_info info);

// See https://golang.org/issue/19837
void table_udf_row_callback(duckdb_function_info, duckdb_data_chunk);
void table_udf_chunk_callback(duckdb_function_info, duckdb_data_chunk);

// See https://golang.org/issue/19835.
typedef void (*init)(duckdb_function_info);
typedef void (*bind)(duckdb_function_info);
typedef void (*callback)(duckdb_function_info, duckdb_data_chunk);

void udf_delete_callback(void *);
*/
import "C"

import (
	"database/sql"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

type (
	// ColumnInfo contains the metadata of a column.
	ColumnInfo struct {
		// The column Name.
		Name string
		// The column type T.
		T TypeInfo
	}

	// CardinalityInfo contains the cardinality of a (table) function.
	// If it is impossible or difficult to determine the exact cardinality, an approximate cardinality may be used.
	CardinalityInfo struct {
		// The absolute Cardinality.
		Cardinality uint
		// IsExact indicates whether the cardinality is exact.
		Exact bool
	}

	// ParallelTableSourceInfo contains information for initializing a parallelism-aware table source.
	ParallelTableSourceInfo struct {
		// MaxThreads is the maximum number of threads on which to run the table source function.
		// If set to 0, it uses DuckDB's default thread configuration.
		MaxThreads int
	}

	tableFunctionData struct {
		fun        any
		projection []int
	}

	tableSource interface {
		// ColumnInfos returns column information for each column of the table function.
		ColumnInfos() []ColumnInfo
		// Cardinality returns the cardinality information of the table function.
		// Optionally, if no cardinality exists, it may return nil.
		Cardinality() *CardinalityInfo
	}

	parallelTableSource interface {
		tableSource
		// Init the table source.
		// Additionally, it returns information for the parallelism-aware table source.
		Init() ParallelTableSourceInfo
		// NewLocalState returns a thread-local execution state.
		// It must return a pointer or a reference type for correct state updates.
		// go-duckdb does not prevent non-reference values.
		NewLocalState() any
	}

	sequentialTableSource interface {
		tableSource
		// Init the table source.
		Init()
	}

	// A RowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the RowTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillRow method with a single thread.
	RowTableSource interface {
		sequentialTableSource
		// FillRow takes a Row and fills it with values.
		// It returns true, if there are more rows to fill.
		FillRow(Row) (bool, error)
	}

	// A ParallelRowTableSource represents anything that produces rows in a non-vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelRowTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillRow method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillRow must use synchronisation
	// primitives to avoid race conditions.
	ParallelRowTableSource interface {
		parallelTableSource
		// FillRow takes a Row and fills it with values.
		// It returns true, if there are more rows to fill.
		FillRow(any, Row) (bool, error)
	}

	// A ChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ChunkTableSource, go-duckdb requests the rows.
	// It sequentially calls the FillChunk method with a single thread.
	ChunkTableSource interface {
		sequentialTableSource
		// FillChunk takes a Chunk and fills it with values.
		// It returns true, if there are more chunks to fill.
		FillChunk(DataChunk) error
	}

	// A ParallelChunkTableSource represents anything that produces rows in a vectorised way.
	// The cardinality is requested before function initialization.
	// After initializing the ParallelChunkTableSource, go-duckdb requests the rows.
	// It simultaneously calls the FillChunk method with multiple threads.
	// If ParallelTableSourceInfo.MaxThreads is greater than one, FillChunk must use synchronization
	// primitives to avoid race conditions.
	ParallelChunkTableSource interface {
		parallelTableSource
		// FillChunk takes a Chunk and fills it with values.
		// It returns true, if there are more chunks to fill.
		FillChunk(any, DataChunk) error
	}

	// TableFunctionConfig contains any information passed to DuckDB when registering the table function.
	TableFunctionConfig struct {
		// The Arguments of the table function.
		Arguments []TypeInfo
		// The NamedArguments of the table function.
		NamedArguments map[string]TypeInfo
	}

	// TableFunction implements different table function types:
	// RowTableFunction, ParallelRowTableFunction, ChunkTableFunction, and ParallelChunkTableFunction.
	TableFunction interface {
		RowTableFunction | ParallelRowTableFunction | ChunkTableFunction | ParallelChunkTableFunction
	}

	// A RowTableFunction is a type which can be bound to return a RowTableSource.
	RowTableFunction = tableFunction[RowTableSource]
	// A ParallelRowTableFunction is a type which can be bound to return a ParallelRowTableSource.
	ParallelRowTableFunction = tableFunction[ParallelRowTableSource]
	// A ChunkTableFunction is a type which can be bound to return a ChunkTableSource.
	ChunkTableFunction = tableFunction[ChunkTableSource]
	// A ParallelChunkTableFunction is a type which can be bound to return a ParallelChunkTableSource.
	ParallelChunkTableFunction = tableFunction[ParallelChunkTableSource]

	tableFunction[T any] struct {
		// Config returns the table function configuration, including the function arguments.
		Config TableFunctionConfig
		// BindArguments binds the arguments and returns a TableSource.
		BindArguments func(named map[string]any, args ...any) (T, error)
	}
)

func (tfd *tableFunctionData) setColumnCount(info C.duckdb_init_info) {
	count := C.duckdb_init_get_column_count(info)
	for i := 0; i < int(count); i++ {
		srcPos := C.duckdb_init_get_column_index(info, C.idx_t(i))
		tfd.projection[int(srcPos)] = i
	}
}

//export table_udf_bind_row
func table_udf_bind_row(info C.duckdb_bind_info) {
	udfBindTyped[RowTableSource](info)
}

//export table_udf_bind_chunk
func table_udf_bind_chunk(info C.duckdb_bind_info) {
	udfBindTyped[ChunkTableSource](info)
}

//export table_udf_bind_parallel_row
func table_udf_bind_parallel_row(info C.duckdb_bind_info) {
	udfBindTyped[ParallelRowTableSource](info)
}

//export table_udf_bind_parallel_chunk
func table_udf_bind_parallel_chunk(info C.duckdb_bind_info) {
	udfBindTyped[ParallelChunkTableSource](info)
}

func udfBindTyped[T tableSource](info C.duckdb_bind_info) {
	f := getPinned[tableFunction[T]](C.duckdb_bind_get_extra_info(info))
	config := f.Config

	argCount := len(config.Arguments)
	args := make([]any, argCount)
	namedArgs := make(map[string]any)

	for i, t := range config.Arguments {
		value := C.duckdb_bind_get_parameter(info, C.idx_t(i))
		var err error
		args[i], err = getValue(t, value)
		C.duckdb_destroy_value(&value)

		if err != nil {
			setBindError(info, err.Error())
			return
		}
	}

	for name, t := range config.NamedArguments {
		argName := C.CString(name)
		value := C.duckdb_bind_get_named_parameter(info, argName)
		C.duckdb_free(unsafe.Pointer(argName))

		var err error
		namedArgs[name], err = getValue(t, value)
		C.duckdb_destroy_value(&value)

		if err != nil {
			setBindError(info, err.Error())
			return
		}
	}

	instance, err := f.BindArguments(namedArgs, args...)
	if err != nil {
		setBindError(info, err.Error())
		return
	}

	columnInfos := instance.ColumnInfos()
	instanceData := tableFunctionData{
		fun:        instance,
		projection: make([]int, len(columnInfos)),
	}

	for i, v := range columnInfos {
		if v.T == nil {
			setBindError(info, errTableUDFColumnTypeIsNil.Error())
			return
		}

		logicalType := v.T.logicalType()
		name := C.CString(v.Name)
		C.duckdb_bind_add_result_column(info, name, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
		C.duckdb_free(unsafe.Pointer(name))

		instanceData.projection[i] = -1
	}

	cardinality := instance.Cardinality()
	if cardinality != nil {
		C.duckdb_bind_set_cardinality(info, C.idx_t(cardinality.Cardinality), C.bool(cardinality.Exact))
	}

	pinnedInstanceData := pinnedValue[tableFunctionData]{
		pinner: &runtime.Pinner{},
		value:  instanceData,
	}

	h := cgo.NewHandle(pinnedInstanceData)
	pinnedInstanceData.pinner.Pin(&h)
	C.duckdb_bind_set_bind_data(info, unsafe.Pointer(&h), C.duckdb_delete_callback_t(C.udf_delete_callback))
}

//export table_udf_init
func table_udf_init(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	instance.setColumnCount(info)
	instance.fun.(sequentialTableSource).Init()
}

//export table_udf_init_parallel
func table_udf_init_parallel(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	instance.setColumnCount(info)
	initData := instance.fun.(parallelTableSource).Init()
	maxThreads := C.idx_t(initData.MaxThreads)
	C.duckdb_init_set_max_threads(info, maxThreads)
}

//export table_udf_local_init
func table_udf_local_init(info C.duckdb_init_info) {
	instance := getPinned[tableFunctionData](C.duckdb_init_get_bind_data(info))
	localState := pinnedValue[any]{
		pinner: &runtime.Pinner{},
		value:  instance.fun.(parallelTableSource).NewLocalState(),
	}
	h := cgo.NewHandle(localState)
	localState.pinner.Pin(&h)
	C.duckdb_init_set_init_data(info, unsafe.Pointer(&h), C.duckdb_delete_callback_t(C.udf_delete_callback))
}

//export table_udf_row_callback
func table_udf_row_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	instance := getPinned[tableFunctionData](C.duckdb_function_get_bind_data(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		setFuncError(info, err.Error())
		return
	}

	row := Row{
		chunk:      &chunk,
		projection: instance.projection,
	}
	maxSize := C.duckdb_vector_size()

	switch fun := instance.fun.(type) {
	case RowTableSource:
		// At the end of the loop row.r must be the index of the last row.
		for row.r = 0; row.r < maxSize; row.r++ {
			next, errRow := fun.FillRow(row)
			if errRow != nil {
				setFuncError(info, errRow.Error())
				break
			}
			if !next {
				break
			}
		}
	case ParallelRowTableSource:
		// At the end of the loop row.r must be the index of the last row.
		localState := getPinned[any](C.duckdb_function_get_local_init_data(info))
		for row.r = 0; row.r < maxSize; row.r++ {
			next, errRow := fun.FillRow(localState, row)
			if errRow != nil {
				setFuncError(info, errRow.Error())
				break
			}
			if !next {
				break
			}
		}
	}
	C.duckdb_data_chunk_set_size(output, row.r)
}

//export table_udf_chunk_callback
func table_udf_chunk_callback(info C.duckdb_function_info, output C.duckdb_data_chunk) {
	instance := getPinned[tableFunctionData](C.duckdb_function_get_bind_data(info))

	var chunk DataChunk
	err := chunk.initFromDuckDataChunk(output, true)
	if err != nil {
		setFuncError(info, err.Error())
		return
	}

	switch fun := instance.fun.(type) {
	case ChunkTableSource:
		err = fun.FillChunk(chunk)
	case ParallelChunkTableSource:
		localState := getPinned[*any](C.duckdb_function_get_local_init_data(info))
		err = fun.FillChunk(localState, chunk)
	}
	if err != nil {
		setFuncError(info, err.Error())
	}
}

// RegisterTableUDF registers a user-defined table function.
// Projection pushdown is enabled by default.
func RegisterTableUDF[TFT TableFunction](c *sql.Conn, name string, f TFT) error {
	if name == "" {
		return getError(errAPI, errTableUDFNoName)
	}
	function := C.duckdb_create_table_function()

	// Set the name.
	cName := C.CString(name)
	defer C.duckdb_free(unsafe.Pointer(cName))
	C.duckdb_table_function_set_name(function, cName)

	var config TableFunctionConfig

	// Pin the table function f.
	value := pinnedValue[TFT]{
		pinner: &runtime.Pinner{},
		value:  f,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the table function f.
	C.duckdb_table_function_set_extra_info(
		function,
		unsafe.Pointer(&h),
		C.duckdb_delete_callback_t(C.udf_delete_callback))
	C.duckdb_table_function_supports_projection_pushdown(function, C.bool(true))

	// Set the config.
	var x any = f
	switch tableFunc := x.(type) {
	case RowTableFunction:
		C.duckdb_table_function_set_init(function, C.init(C.table_udf_init))
		C.duckdb_table_function_set_bind(function, C.bind(C.table_udf_bind_row))
		C.duckdb_table_function_set_function(function, C.callback(C.table_udf_row_callback))

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ChunkTableFunction:
		C.duckdb_table_function_set_init(function, C.init(C.table_udf_init))
		C.duckdb_table_function_set_bind(function, C.bind(C.table_udf_bind_chunk))
		C.duckdb_table_function_set_function(function, C.callback(C.table_udf_chunk_callback))

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ParallelRowTableFunction:
		C.duckdb_table_function_set_init(function, C.init(C.table_udf_init_parallel))
		C.duckdb_table_function_set_bind(function, C.bind(C.table_udf_bind_parallel_row))
		C.duckdb_table_function_set_function(function, C.callback(C.table_udf_row_callback))
		C.duckdb_table_function_set_local_init(function, C.init(C.table_udf_local_init))

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	case ParallelChunkTableFunction:
		C.duckdb_table_function_set_init(function, C.init(C.table_udf_init_parallel))
		C.duckdb_table_function_set_bind(function, C.bind(C.table_udf_bind_parallel_chunk))
		C.duckdb_table_function_set_function(function, C.callback(C.table_udf_chunk_callback))
		C.duckdb_table_function_set_local_init(function, C.init(C.table_udf_local_init))

		config = tableFunc.Config
		if tableFunc.BindArguments == nil {
			return getError(errAPI, errTableUDFMissingBindArgs)
		}

	default:
		return getError(errInternal, nil)
	}

	// Set the arguments.
	for _, t := range config.Arguments {
		if t == nil {
			return getError(errAPI, errTableUDFArgumentIsNil)
		}
		logicalType := t.logicalType()
		C.duckdb_table_function_add_parameter(function, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
	}

	// Set the named arguments.
	for arg, t := range config.NamedArguments {
		if t == nil {
			return getError(errAPI, errTableUDFArgumentIsNil)
		}
		logicalType := t.logicalType()
		cArg := C.CString(arg)
		C.duckdb_table_function_add_named_parameter(function, cArg, logicalType)
		C.duckdb_destroy_logical_type(&logicalType)
		C.duckdb_free(unsafe.Pointer(cArg))
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*Conn)
		state := C.duckdb_register_table_function(con.duckdbCon, function)
		C.duckdb_destroy_table_function(&function)
		if state == C.DuckDBError {
			return getError(errAPI, errTableUDFCreate)
		}
		return nil
	})
	return err
}
