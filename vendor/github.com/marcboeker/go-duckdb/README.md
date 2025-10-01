# Go SQL driver for [DuckDB](https://github.com/duckdb/duckdb)

The DuckDB driver conforms to the built-in `database/sql` interface.

![Tests status](https://github.com/marcboeker/go-duckdb/actions/workflows/tests.yaml/badge.svg)
[![GoDoc](https://godoc.org/github.com/marcboeker/go-duckdb?status.svg)](https://pkg.go.dev/github.com/marcboeker/go-duckdb)

```diff
! DuckDB just (Feb 5th, 2025) released its latest version (1.2.0).
! go-duckdb is working on moving to that version within the next few days.
! However, we expect some breaking changes around pre-built static libraries and the arrow dependency.
! Therefore, it is likely that go-duckdb moves to v2 to support DuckDB v1.2.0.
```

## Installation

```
go get github.com/marcboeker/go-duckdb
```

### Windows

You must have the correct version of gcc and the necessary runtime libraries installed on Windows.
One method to do this is using msys64.
To begin, install msys64 using their installer.
Once you installed msys64, open a msys64 shell and run:

```
pacman -S mingw-w64-ucrt-x86_64-gcc
```

Select "yes" when necessary; it is okay if the shell closes.
Then, add gcc to the path using whatever method you prefer.
In powershell this is `$env:PATH = "C:\msys64\ucrt64\bin:$env:PATH"`.
After, you can compile this package in Windows.

## Usage

_Note: For readability, we omit error handling in most examples._

`go-duckdb` hooks into the `database/sql` interface provided by the Go `stdlib`.
To open a connection, specify the driver type as `duckdb`.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()
```

The above lines create an in-memory instance of DuckDB.
To open a persistent database, specify a file path to the database file.
If the file does not exist, then DuckDB creates it.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db")
defer db.Close()
```

If you want to set specific [config options for DuckDB](https://duckdb.org/docs/sql/configuration), 
you can add them as query style parameters in the form of `name=value` pairs to the DSN.

```go
db, err := sql.Open("duckdb", "/path/to/foo.db?access_mode=read_only&threads=4")
defer db.Close()
```

Alternatively, you can use [sql.OpenDB](https://cs.opensource.google/go/go/+/refs/tags/go1.23.0:src/database/sql/sql.go;l=824).
That way, you can perform initialization steps in a callback function before opening the database.
Here's an example that configures some parameters when opening a database with `sql.OpenDB(connector)`.

```go
connector, err := duckdb.NewConnector("/path/to/foo.db?access_mode=read_only&threads=4", func(execer driver.ExecerContext) error {
    bootQueries := []string{
        "SET schema=main",
        "SET search_path=main",
    }

    for _, query := range bootQueries {
        _, err = execer.ExecContext(context.Background(), query, nil)
        if err != nil {
			return err
        }
    }
    return nil
})

db := sql.OpenDB(connector)
defer db.Close()
```

Please refer to the [database/sql](https://godoc.org/database/sql) documentation for further instructions on usage.

## Linking DuckDB

By default, `go-duckdb` statically links pre-build DuckDB libraries into your binary.
Statically linking DuckDB increases your binary size.

`go-duckdb` bundles the following pre-compiled static libraries.
However, due to GitHub file size restrictions (100MB) and Go repository size limitations (500MB), these might change in the future.
- MacOS: amd64, arm64.
- Linux: amd64, arm64.
- FreeBSD: amd64.
- Windows: amd64.

### Linking a custom static library

If none of the pre-build libraries satisfy your needs, you can build a custom static library; see `deps.yaml`.

*Note: The DuckDB team is currently working on deploying pre-built static libraries as part of their releases and nightly builds.
Once available, you can also download these libraries. They bundle the default extensions for duckdb releases.*

Once a static library (`libduckdb_bundle.a`) is available, you can build your project like this.
```
CGO_LDFLAGS="-lc++ -lduckdb_bundle -L/path/to/folder/with/lib" go build -tags=duckdb_use_static_lib
```

### Dynamic linking

Alternatively, you can dynamically link DuckDB by passing `-tags=duckdb_use_lib` to `go build`.
You must have a copy of `libduckdb` available on your system (`.so` on Linux or `.dylib` on macOS), 
which you can download from the DuckDB [releases page](https://github.com/duckdb/duckdb/releases).
For example:

```sh
# On Linux.
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go build -tags=duckdb_use_lib main.go
LD_LIBRARY_PATH=/path/to/libs ./main

# On macOS.
CGO_ENABLED=1 CGO_LDFLAGS="-L/path/to/libs" go build -tags=duckdb_use_lib main.go
DYLD_LIBRARY_PATH=/path/to/libs ./main
```

## Notes and FAQs

**`undefined: conn`**

Some people encounter an `undefined: conn` error when building this package.
This error is due to the Go compiler determining that CGO is unavailable.
This error can happen due to a few issues.

The first cause, as noted in the [comment here](https://github.com/marcboeker/go-duckdb/issues/275#issuecomment-2355712997), 
might be that the `buildtools` are not installed.
To fix this for ubuntu, you can install them using:
```
sudo apt-get update && sudo apt-get install build-essential
```

Another cause can be cross-compilation since the Go compiler automatically disables CGO when cross-compiling.
To enable CGO when cross-compiling, use `CC={C cross compiler} CGO_ENABLED=1 {command}` to force-enable CGO and set the right cross-compiler.

**`TIMESTAMP vs. TIMESTAMP_TZ`**

In the C API, DuckDB stores both `TIMESTAMP` and `TIMESTAMP_TZ` as `duckdb_timestamp`, which holds the number of
microseconds elapsed since January 1, 1970, UTC (i.e., an instant without offset information).
When passing a `time.Time` to go-duckdb, go-duckdb transforms it to an instant with `UnixMicro()`,
even when using `TIMESTAMP_TZ`. Later, scanning either type of value returns an instant, as SQL types do not model
time zone information for individual values.

## Memory Allocation

DuckDB lives in process.
Therefore, all its memory lives in the driver. 
All allocations live in the host process, which is the Go application. 
Especially for long-running applications, it is crucial to call the corresponding `Close`-functions as specified in [database/sql](https://godoc.org/database/sql). 
The following is a list of examples.

```go
db, err := sql.Open("duckdb", "")
defer db.Close()

conn, err := db.Conn(context.Background())
defer conn.Close()

rows, err := conn.QueryContext(context.Background(), "SELECT 42")
// Alternatively, rows.Next() has to return false.
rows.Close()

appender, err := NewAppenderFromConn(conn, "", "test")
defer appender.Close()

// If not passed to sql.OpenDB.
connector, err := NewConnector("", nil)
defer connector.Close()
```

## DuckDB Appender API

If you want to use the [DuckDB Appender API](https://duckdb.org/docs/data/appender.html), you can obtain a new `Appender` by passing a DuckDB connection to `NewAppenderFromConn()`.
See `examples/appender.go` for a complete example.

```go
connector, err := duckdb.NewConnector("test.db", nil)
defer connector.Close()

conn, err := connector.Connect(context.Background())
defer conn.Close()

// Obtain an appender from the connection.
// NOTE: The table 'test_tbl' must exist in test.db.
appender, err := NewAppenderFromConn(conn, "", "test_tbl")
defer appender.Close()

err = appender.AppendRow(...)
```

## DuckDB Profiling API

This section describes using the [DuckDB Profiling API](https://duckdb.org/docs/dev/profiling.html).
DuckDB's profiling information is connection-local.
The following example walks you through the necessary steps to obtain the `ProfilingInfo` type, which contains all available metrics.
Please refer to the [DuckDB documentation](https://duckdb.org/docs/dev/profiling.html) on configuring and collecting specific metrics.

- First, you need to obtain a connection.
- Then, you enable profiling for the connection.
- Now, for each subsequent query on this connection, DuckDB will collect profiling information.
    - Optionally, you can turn off profiling at any point.
- Next, you execute the query for which you want to obtain profiling information.
- Finally, directly after executing the query, retrieve any available profiling information.

```Go
db, err := sql.Open("duckdb", "")
con, err := db.Conn(context.Background())

_, err = con.ExecContext(context.Background(), `PRAGMA enable_profiling = 'no_output'`)
_, err = con.ExecContext(context.Background(), `PRAGMA profiling_mode = 'detailed'`)

res, err := con.QueryContext(context.Background(), `SELECT 42`)
info, err := GetProfilingInfo(con)
err = res.Close()

_, err = con.ExecContext(context.Background(), `PRAGMA disable_profiling`)
err = con.Close()
err = db.Close()
```

## DuckDB Apache Arrow Interface

If you want to use the [DuckDB Arrow Interface](https://duckdb.org/docs/api/c/api#arrow-interface), you can obtain a new `Arrow` by passing a DuckDB connection to `NewArrowFromConn()`.

```go
connector, err := duckdb.NewConnector("", nil)
defer connector.Close()

conn, err := connector.Connect(context.Background())
defer conn.Close()

// Obtain the Arrow from the connection.
arrow, err := duckdb.NewArrowFromConn(conn)

rdr, err := arrow.QueryContext(context.Background(), "SELECT * FROM generate_series(1, 10)")
defer rdr.Release()

for rdr.Next() {
  // Process each record.
}
```

The Arrow interface is a heavy dependency.
If you do not need it, you can disable it by passing `-tags=no_duckdb_arrow` to `go build`.

*Note: This will be made opt-in in V2.*

```sh
go build -tags="no_duckdb_arrow"
```

## Vendoring

If you want to vendor a module containing `go-duckdb`, please use `modvendor` to include the missing header files and libraries.
See issue [#174](https://github.com/marcboeker/go-duckdb/issues/174#issuecomment-1979097864) for more details.

1. `go install github.com/goware/modvendor@latest`
2. `go mod vendor`
3. `modvendor -copy="**/*.a **/*.h" -v`

Now, you can build your module as usual.

## DuckDB Extensions

`go-duckdb` statically builds the `JSON` extension for its pre-compiled libraries.
Additionally, automatic extension loading is enabled.
The extensions available differ between the pre-compiled libraries.
Thus, if you fail to install and load an extension, you might have to link a custom DuckDB.

Specifically, for MingW (Windows), there are no distributed extensions (yet).
You can statically include them by extending the `BUILD_EXTENSIONS="json"` variable in the `Makefile`.
