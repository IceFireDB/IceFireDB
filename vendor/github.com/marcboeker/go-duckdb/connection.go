package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"unsafe"
)

// Conn holds a connection to a DuckDB database.
// It implements the driver.Conn interface.
type Conn struct {
	duckdbCon C.duckdb_connection
	closed    bool
	tx        bool
}

// CheckNamedValue implements the driver.NamedValueChecker interface.
func (c *Conn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case *big.Int, Interval:
		return nil
	}
	return driver.ErrSkip
}

// ExecContext executes a query that doesn't return rows, such as an INSERT or UPDATE.
// It implements the driver.ExecerContext interface.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	prepared, err := c.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	res, err := prepared.ExecContext(ctx, args)
	errClose := prepared.Close()
	if err != nil {
		if errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}
	if errClose != nil {
		return nil, errClose
	}
	return res, nil
}

// QueryContext executes a query that may return rows, such as a SELECT.
// It implements the driver.QueryerContext interface.
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	prepared, err := c.prepareStmts(ctx, query)
	if err != nil {
		return nil, err
	}

	r, err := prepared.QueryContext(ctx, args)
	if err != nil {
		errClose := prepared.Close()
		if errClose != nil {
			return nil, errors.Join(err, errClose)
		}
		return nil, err
	}

	// We must close the prepared statement after closing the rows r.
	prepared.closeOnRowsClose = true
	return r, nil
}

// PrepareContext returns a prepared statement, bound to this connection.
// It implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.prepareStmts(ctx, query)
}

// Prepare returns a prepared statement, bound to this connection.
// It implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed {
		return nil, errors.Join(errPrepare, errClosedCon)
	}

	stmts, count, err := c.extractStmts(query)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_extracted(&stmts)

	if count != 1 {
		return nil, errors.Join(errPrepare, errMissingPrepareContext)
	}
	return c.prepareExtractedStmt(stmts, 0)
}

// Begin is deprecated: Use BeginTx instead.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.
// It implements the driver.ConnBeginTx interface.
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.tx {
		return nil, errors.Join(errBeginTx, errMultipleTx)
	}

	if opts.ReadOnly {
		return nil, errors.Join(errBeginTx, errReadOnlyTxNotSupported)
	}

	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault:
	default:
		return nil, errors.Join(errBeginTx, errIsolationLevelNotSupported)
	}

	if _, err := c.ExecContext(ctx, `BEGIN TRANSACTION`, nil); err != nil {
		return nil, err
	}

	c.tx = true
	return &tx{c}, nil
}

// Close closes the connection to the database.
// It implements the driver.Conn interface.
func (c *Conn) Close() error {
	if c.closed {
		return errClosedCon
	}
	c.closed = true
	C.duckdb_disconnect(&c.duckdbCon)
	return nil
}

func (c *Conn) extractStmts(query string) (C.duckdb_extracted_statements, C.idx_t, error) {
	cQuery := C.CString(query)
	defer C.duckdb_free(unsafe.Pointer(cQuery))

	var stmts C.duckdb_extracted_statements
	count := C.duckdb_extract_statements(c.duckdbCon, cQuery, &stmts)

	if count == 0 {
		errMsg := C.GoString(C.duckdb_extract_statements_error(stmts))
		C.duckdb_destroy_extracted(&stmts)
		if errMsg != "" {
			return nil, 0, getDuckDBError(errMsg)
		}
		return nil, 0, errEmptyQuery
	}

	return stmts, count, nil
}

func (c *Conn) prepareExtractedStmt(stmts C.duckdb_extracted_statements, i C.idx_t) (*Stmt, error) {
	var s C.duckdb_prepared_statement
	state := C.duckdb_prepare_extracted_statement(c.duckdbCon, stmts, i, &s)

	if state == C.DuckDBError {
		err := getDuckDBError(C.GoString(C.duckdb_prepare_error(s)))
		C.duckdb_destroy_prepare(&s)
		return nil, err
	}

	return &Stmt{c: c, stmt: &s}, nil
}

func (c *Conn) prepareStmts(ctx context.Context, query string) (*Stmt, error) {
	if c.closed {
		return nil, errClosedCon
	}

	stmts, count, errExtract := c.extractStmts(query)
	if errExtract != nil {
		return nil, errExtract
	}
	defer C.duckdb_destroy_extracted(&stmts)

	for i := C.idx_t(0); i < count-1; i++ {
		prepared, err := c.prepareExtractedStmt(stmts, i)
		if err != nil {
			return nil, err
		}

		// Execute the statement without any arguments and ignore the result.
		_, execErr := prepared.ExecContext(ctx, nil)
		closeErr := prepared.Close()
		if execErr != nil {
			return nil, execErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return c.prepareExtractedStmt(stmts, count-1)
}
