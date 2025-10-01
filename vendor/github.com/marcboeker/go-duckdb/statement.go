package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"unsafe"
)

type StmtType C.duckdb_statement_type

const (
	STATEMENT_TYPE_INVALID      StmtType = C.DUCKDB_STATEMENT_TYPE_INVALID
	STATEMENT_TYPE_SELECT       StmtType = C.DUCKDB_STATEMENT_TYPE_SELECT
	STATEMENT_TYPE_INSERT       StmtType = C.DUCKDB_STATEMENT_TYPE_INSERT
	STATEMENT_TYPE_UPDATE       StmtType = C.DUCKDB_STATEMENT_TYPE_UPDATE
	STATEMENT_TYPE_EXPLAIN      StmtType = C.DUCKDB_STATEMENT_TYPE_EXPLAIN
	STATEMENT_TYPE_DELETE       StmtType = C.DUCKDB_STATEMENT_TYPE_DELETE
	STATEMENT_TYPE_PREPARE      StmtType = C.DUCKDB_STATEMENT_TYPE_PREPARE
	STATEMENT_TYPE_CREATE       StmtType = C.DUCKDB_STATEMENT_TYPE_CREATE
	STATEMENT_TYPE_EXECUTE      StmtType = C.DUCKDB_STATEMENT_TYPE_EXECUTE
	STATEMENT_TYPE_ALTER        StmtType = C.DUCKDB_STATEMENT_TYPE_ALTER
	STATEMENT_TYPE_TRANSACTION  StmtType = C.DUCKDB_STATEMENT_TYPE_TRANSACTION
	STATEMENT_TYPE_COPY         StmtType = C.DUCKDB_STATEMENT_TYPE_COPY
	STATEMENT_TYPE_ANALYZE      StmtType = C.DUCKDB_STATEMENT_TYPE_ANALYZE
	STATEMENT_TYPE_VARIABLE_SET StmtType = C.DUCKDB_STATEMENT_TYPE_VARIABLE_SET
	STATEMENT_TYPE_CREATE_FUNC  StmtType = C.DUCKDB_STATEMENT_TYPE_CREATE_FUNC
	STATEMENT_TYPE_DROP         StmtType = C.DUCKDB_STATEMENT_TYPE_DROP
	STATEMENT_TYPE_EXPORT       StmtType = C.DUCKDB_STATEMENT_TYPE_EXPORT
	STATEMENT_TYPE_PRAGMA       StmtType = C.DUCKDB_STATEMENT_TYPE_PRAGMA
	STATEMENT_TYPE_VACUUM       StmtType = C.DUCKDB_STATEMENT_TYPE_VACUUM
	STATEMENT_TYPE_CALL         StmtType = C.DUCKDB_STATEMENT_TYPE_CALL
	STATEMENT_TYPE_SET          StmtType = C.DUCKDB_STATEMENT_TYPE_SET
	STATEMENT_TYPE_LOAD         StmtType = C.DUCKDB_STATEMENT_TYPE_LOAD
	STATEMENT_TYPE_RELATION     StmtType = C.DUCKDB_STATEMENT_TYPE_RELATION
	STATEMENT_TYPE_EXTENSION    StmtType = C.DUCKDB_STATEMENT_TYPE_EXTENSION
	STATEMENT_TYPE_LOGICAL_PLAN StmtType = C.DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN
	STATEMENT_TYPE_ATTACH       StmtType = C.DUCKDB_STATEMENT_TYPE_ATTACH
	STATEMENT_TYPE_DETACH       StmtType = C.DUCKDB_STATEMENT_TYPE_DETACH
	STATEMENT_TYPE_MULTI        StmtType = C.DUCKDB_STATEMENT_TYPE_MULTI
)

// Stmt implements the driver.Stmt interface.
type Stmt struct {
	c                *Conn
	stmt             *C.duckdb_prepared_statement
	closeOnRowsClose bool
	bound            bool
	closed           bool
	rows             bool
}

// Close the statement.
// Implements the driver.Stmt interface.
func (s *Stmt) Close() error {
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	C.duckdb_destroy_prepare(s.stmt)
	return nil
}

// NumInput returns the number of placeholder parameters.
// Implements the driver.Stmt interface.
func (s *Stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	count := C.duckdb_nparams(*s.stmt)
	return int(count)
}

// ParamName returns the name of the parameter at the given index (1-based).
func (s *Stmt) ParamName(n int) (string, error) {
	if s.closed {
		return "", errClosedStmt
	}
	if s.stmt == nil {
		return "", errUninitializedStmt
	}

	count := C.duckdb_nparams(*s.stmt)
	if C.idx_t(n) == 0 || C.idx_t(n) > count {
		return "", getError(errAPI, paramIndexError(n, uint64(count)))
	}

	cStr := C.duckdb_parameter_name(*s.stmt, C.idx_t(n))
	name := C.GoString(cStr)
	C.duckdb_free(unsafe.Pointer(cStr))
	return name, nil
}

// ParamType returns the expected type of the parameter at the given index (1-based).
func (s *Stmt) ParamType(n int) (Type, error) {
	if s.closed {
		return TYPE_INVALID, errClosedStmt
	}
	if s.stmt == nil {
		return TYPE_INVALID, errUninitializedStmt
	}

	count := C.duckdb_nparams(*s.stmt)
	if C.idx_t(n) == 0 || C.idx_t(n) > count {
		return TYPE_INVALID, getError(errAPI, paramIndexError(n, uint64(count)))
	}

	return Type(C.duckdb_param_type(*s.stmt, C.idx_t(n))), nil
}

// StatementType returns the type of the statement.
func (s *Stmt) StatementType() (StmtType, error) {
	if s.closed {
		return STATEMENT_TYPE_INVALID, errClosedStmt
	}
	if s.stmt == nil {
		return STATEMENT_TYPE_INVALID, errUninitializedStmt
	}

	return StmtType(C.duckdb_prepared_statement_type(*s.stmt)), nil
}

// Bind the parameters to the statement.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) Bind(args []driver.NamedValue) error {
	if s.closed {
		return errors.Join(errCouldNotBind, errClosedStmt)
	}
	if s.stmt == nil {
		return errors.Join(errCouldNotBind, errUninitializedStmt)
	}
	return s.bind(args)
}

func (s *Stmt) bindHugeint(val *big.Int, n int) (C.duckdb_state, error) {
	hugeint, err := hugeIntFromNative(val)
	if err != nil {
		return C.DuckDBError, err
	}
	state := C.duckdb_bind_hugeint(*s.stmt, C.idx_t(n+1), hugeint)
	return state, nil
}

func (s *Stmt) bindString(val string, n int) (C.duckdb_state, error) {
	v := C.CString(val)
	state := C.duckdb_bind_varchar(*s.stmt, C.idx_t(n+1), v)
	C.duckdb_free(unsafe.Pointer(v))
	return state, nil
}

func (s *Stmt) bindBlob(val []byte, n int) (C.duckdb_state, error) {
	v := C.CBytes(val)
	state := C.duckdb_bind_blob(*s.stmt, C.idx_t(n+1), v, C.uint64_t(len(val)))
	C.duckdb_free(unsafe.Pointer(v))
	return state, nil
}

func (s *Stmt) bindInterval(val Interval, n int) (C.duckdb_state, error) {
	v := C.duckdb_interval{
		months: C.int32_t(val.Months),
		days:   C.int32_t(val.Days),
		micros: C.int64_t(val.Micros),
	}
	state := C.duckdb_bind_interval(*s.stmt, C.idx_t(n+1), v)
	return state, nil
}

func (s *Stmt) bindTimestamp(val driver.NamedValue, t Type, n int) (C.duckdb_state, error) {
	ts, err := getCTimestamp(t, val.Value)
	if err != nil {
		return C.DuckDBError, err
	}
	state := C.duckdb_bind_timestamp(*s.stmt, C.idx_t(n+1), ts)
	return state, nil
}

func (s *Stmt) bindDate(val driver.NamedValue, n int) (C.duckdb_state, error) {
	date, err := getCDate(val.Value)
	if err != nil {
		return C.DuckDBError, err
	}
	state := C.duckdb_bind_date(*s.stmt, C.idx_t(n+1), date)
	return state, nil
}

func (s *Stmt) bindTime(val driver.NamedValue, t Type, n int) (C.duckdb_state, error) {
	ticks, err := getTimeTicks(val.Value)
	if err != nil {
		return C.DuckDBError, err
	}

	if t == TYPE_TIME {
		var ti C.duckdb_time
		ti.micros = C.int64_t(ticks)
		state := C.duckdb_bind_time(*s.stmt, C.idx_t(n+1), ti)
		return state, nil
	}

	// TYPE_TIME_TZ: The UTC offset is 0.
	ti := C.duckdb_create_time_tz(C.int64_t(ticks), 0)
	v := C.duckdb_create_time_tz_value(ti)
	state := C.duckdb_bind_value(*s.stmt, C.idx_t(n+1), v)
	C.duckdb_destroy_value(&v)
	return state, nil
}

func (s *Stmt) bindComplexValue(val driver.NamedValue, n int) (C.duckdb_state, error) {
	t, err := s.ParamType(n + 1)
	if err != nil {
		return C.DuckDBError, err
	}
	if name, ok := unsupportedTypeToStringMap[t]; ok {
		return C.DuckDBError, addIndexToError(unsupportedTypeError(name), n+1)
	}

	switch t {
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_TZ:
		return s.bindTimestamp(val, t, n)
	case TYPE_DATE:
		return s.bindDate(val, n)
	case TYPE_TIME, TYPE_TIME_TZ:
		return s.bindTime(val, t, n)
	case TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_LIST, TYPE_STRUCT, TYPE_MAP,
		TYPE_ARRAY, TYPE_ENUM:
		// FIXME: for timestamps: distinguish between timestamp[_s|ms|ns] once available.
		// FIXME: for other types: duckdb_param_logical_type once available, then create duckdb_value + duckdb_bind_value
		// FIXME: for other types: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[t]
		return C.DuckDBError, addIndexToError(unsupportedTypeError(name), n+1)
	}
	return C.DuckDBError, addIndexToError(unsupportedTypeError(unknownTypeErrMsg), n+1)
}

func (s *Stmt) bindValue(val driver.NamedValue, n int) (C.duckdb_state, error) {
	switch v := val.Value.(type) {
	case bool:
		return C.duckdb_bind_boolean(*s.stmt, C.idx_t(n+1), C.bool(v)), nil
	case int8:
		return C.duckdb_bind_int8(*s.stmt, C.idx_t(n+1), C.int8_t(v)), nil
	case int16:
		return C.duckdb_bind_int16(*s.stmt, C.idx_t(n+1), C.int16_t(v)), nil
	case int32:
		return C.duckdb_bind_int32(*s.stmt, C.idx_t(n+1), C.int32_t(v)), nil
	case int64:
		return C.duckdb_bind_int64(*s.stmt, C.idx_t(n+1), C.int64_t(v)), nil
	case int:
		return C.duckdb_bind_int64(*s.stmt, C.idx_t(n+1), C.int64_t(v)), nil
	case *big.Int:
		return s.bindHugeint(v, n)
	case Decimal:
		// FIXME: implement NamedValueChecker to support custom data types.
		name := typeToStringMap[TYPE_DECIMAL]
		return C.DuckDBError, addIndexToError(unsupportedTypeError(name), n+1)
	case uint8:
		return C.duckdb_bind_uint8(*s.stmt, C.idx_t(n+1), C.uint8_t(v)), nil
	case uint16:
		return C.duckdb_bind_uint16(*s.stmt, C.idx_t(n+1), C.uint16_t(v)), nil
	case uint32:
		return C.duckdb_bind_uint32(*s.stmt, C.idx_t(n+1), C.uint32_t(v)), nil
	case uint64:
		return C.duckdb_bind_uint64(*s.stmt, C.idx_t(n+1), C.uint64_t(v)), nil
	case float32:
		return C.duckdb_bind_float(*s.stmt, C.idx_t(n+1), C.float(v)), nil
	case float64:
		return C.duckdb_bind_double(*s.stmt, C.idx_t(n+1), C.double(v)), nil
	case string:
		return s.bindString(v, n)
	case []byte:
		return s.bindBlob(v, n)
	case Interval:
		return s.bindInterval(v, n)
	case nil:
		return C.duckdb_bind_null(*s.stmt, C.idx_t(n+1)), nil
	}
	return s.bindComplexValue(val, n)
}

func (s *Stmt) bind(args []driver.NamedValue) error {
	if s.NumInput() > len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	// relaxed length check allow for unused parameters.
	for i := 0; i < s.NumInput(); i++ {
		cStr := C.duckdb_parameter_name(*s.stmt, C.idx_t(i+1))
		name := C.GoString(cStr)
		C.duckdb_free(unsafe.Pointer(cStr))

		// fallback on index position
		arg := args[i]

		// override with ordinal if set
		for _, v := range args {
			if v.Ordinal == i+1 {
				arg = v
			}
		}

		// override with name if set
		for _, v := range args {
			if v.Name == name {
				arg = v
			}
		}

		state, err := s.bindValue(arg, i)
		if state == C.DuckDBError {
			err = errors.Join(err, duckdbError(C.duckdb_prepare_error(*s.stmt)))
			return errors.Join(errCouldNotBind, err)
		}
	}

	s.bound = true
	return nil
}

// Deprecated: Use ExecContext instead.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), argsToNamedArgs(args))
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.StmtExecContext interface.
func (s *Stmt) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_result(res)

	ra := int64(C.duckdb_value_int64(res, 0, 0))
	return &result{ra}, nil
}

// ExecBound executes a bound query that doesn't return rows, such as an INSERT or UPDATE.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) ExecBound(ctx context.Context) (driver.Result, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_result(res)

	ra := int64(C.duckdb_value_int64(res, 0, 0))
	return &result{ra}, nil
}

// Deprecated: Use QueryContext instead.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), argsToNamedArgs(args))
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.StmtQueryContext interface.
func (s *Stmt) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// QueryBound executes a bound query that may return rows, such as a SELECT.
// It can only be used after Bind has been called.
// WARNING: This is a low-level API and should be used with caution.
func (s *Stmt) QueryBound(ctx context.Context) (driver.Rows, error) {
	if s.closed {
		return nil, errClosedCon
	}
	if s.rows {
		return nil, errActiveRows
	}
	if !s.bound {
		return nil, errNotBound
	}

	res, err := s.executeBound(ctx)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// This method executes the query in steps and checks if context is cancelled before executing each step.
// It uses Pending Result Interface C APIs to achieve this. Reference - https://duckdb.org/docs/api/c/api#pending-result-interface
func (s *Stmt) execute(ctx context.Context, args []driver.NamedValue) (*C.duckdb_result, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext with active Rows")
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}
	return s.executeBound(ctx)
}

func (s *Stmt) executeBound(ctx context.Context) (*C.duckdb_result, error) {
	var pendingRes C.duckdb_pending_result
	if state := C.duckdb_pending_prepared(*s.stmt, &pendingRes); state == C.DuckDBError {
		dbErr := getDuckDBError(C.GoString(C.duckdb_pending_error(pendingRes)))
		C.duckdb_destroy_pending(&pendingRes)
		return nil, dbErr
	}
	defer C.duckdb_destroy_pending(&pendingRes)

	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			C.duckdb_interrupt(s.c.duckdbCon)
			close(bgDoneCh)
			return
		case <-mainDoneCh:
			close(bgDoneCh)
			return
		}
	}()

	var res C.duckdb_result
	state := C.duckdb_execute_pending(pendingRes, &res)
	close(mainDoneCh)
	// also wait for background goroutine to finish
	// sometimes the bg goroutine is not scheduled immediately and by that time if another query is running on this connection
	// it can cancel that query so need to wait for it to finish as well
	<-bgDoneCh
	if state == C.DuckDBError {
		if ctx.Err() != nil {
			C.duckdb_destroy_result(&res)
			return nil, ctx.Err()
		}

		err := getDuckDBError(C.GoString(C.duckdb_result_error(&res)))
		C.duckdb_destroy_result(&res)
		return nil, err
	}

	return &res, nil
}

func argsToNamedArgs(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for n, param := range values {
		args[n].Value = param
		args[n].Ordinal = n + 1
	}
	return args
}
