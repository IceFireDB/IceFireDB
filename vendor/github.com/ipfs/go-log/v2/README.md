# go-log

[![](https://img.shields.io/badge/project-IPFS-blue.svg?style=flat-square)](https://ipfs.io/)
[![GoDoc](https://pkg.go.dev/badge/github.com/ipfs/go-log/v2.svg)](https://pkg.go.dev/github.com/ipfs/go-log/v2)

> The logging library used by IPFS Kubo

go-log wraps [zap](https://github.com/uber-go/zap) to provide per-subsystem level control and optional log/slog integration for unified logging across IPFS/libp2p components.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Environment Variables](#environment-variables)
- [Slog Integration](#slog-integration)
  - [Application Setup (Required)](#application-setup-required)
  - [For library authors](#for-library-authors)
    - [Approach 1: Duck-typing detection (automatic)](#approach-1-duck-typing-detection-automatic)
    - [Approach 2: Explicit handler passing (manual)](#approach-2-explicit-handler-passing-manual)
- [Contribute](#contribute)
- [License](#license)

## Install

```sh
go get github.com/ipfs/go-log
```

## Usage

Once the package is imported under the name `logging`, an instance of `EventLogger` can be created like so:

```go
var log = logging.Logger("subsystem name")
```

It can then be used to emit log messages in plain printf-style messages at seven standard levels:

Levels may be set for all loggers:

```go
lvl, err := logging.LevelFromString("error")
if err != nil {
	panic(err)
}
logging.SetAllLoggers(lvl)
```

or individually:

```go
err := logging.SetLogLevel("net:pubsub", "info")
if err != nil {
	panic(err)
}
```

or by regular expression:

```go
err := logging.SetLogLevelRegex("net:.*", "info")
if err != nil {
	panic(err)
}
```

## Environment Variables

This package can be configured through various environment variables.

#### `GOLOG_LOG_LEVEL`

Specifies the log-level, both globally and on a per-subsystem basis.

For example, the following will set the global minimum log level to `error`, but reduce the minimum
log level for `subsystem1` to `info` and reduce the minimum log level for `subsystem2` to debug.

```bash
export GOLOG_LOG_LEVEL="error,subsystem1=info,subsystem2=debug"
```

`IPFS_LOGGING` is a deprecated alias for this environment variable.

#### `GOLOG_FILE`

Specifies that logs should be written to the specified file. If this option is _not_ specified, logs are written to standard error.

```bash
export GOLOG_FILE="/path/to/my/file.log"
```

#### `GOLOG_OUTPUT`

Specifies where logging output should be written. Can take one or more of the following values, combined with `+`:

- `stdout` -- write logs to standard out.
- `stderr` -- write logs to standard error.
- `file` -- write logs to the file specified by `GOLOG_FILE`

For example, if you want to log to both a file and standard error:

```bash
export GOLOG_FILE="/path/to/my/file.log"
export GOLOG_OUTPUT="stderr+file"
```

Setting _only_ `GOLOG_FILE` will prevent logs from being written to standard error.

#### `GOLOG_LOG_FMT`

Specifies the log message format. It supports the following values:

- `color` -- human readable, colorized (ANSI) output
- `nocolor` -- human readable, plain-text output.
- `json` -- structured JSON.

For example, to log structured JSON (for easier parsing):

```bash
export GOLOG_LOG_FMT="json"
```

The logging format defaults to `color` when the output is a terminal, and `nocolor` otherwise.

`IPFS_LOGGING_FMT` is a deprecated alias for this environment variable.

#### `GOLOG_LOG_LABELS`

Specifies a set of labels that should be added to all log messages as comma-separated key-value
pairs. For example, the following add `{"app": "example_app", "dc": "sjc-1"}` to every log entry.

```bash
export GOLOG_LOG_LABELS="app=example_app,dc=sjc-1"
```

#### `GOLOG_CAPTURE_DEFAULT_SLOG`

By default, go-log does NOT automatically install its slog handler as `slog.Default()`. Applications should explicitly call `slog.SetDefault(slog.New(golog.SlogHandler()))` for slog integration (see Slog Integration section below).

Alternatively, you can enable automatic installation by setting:

```bash
export GOLOG_CAPTURE_DEFAULT_SLOG="true"
```

When enabled, go-log automatically installs its handler as `slog.Default()` during `SetupLogging()`, which allows libraries using `slog` to automatically use go-log's formatting and dynamic level control.

## Slog Integration

go-log provides integration with Go's `log/slog` package for unified log management. This provides:

1. **Unified formatting**: slog logs use the same format as go-log (color/nocolor/json)
2. **Dynamic level control**: slog loggers respect `SetLogLevel()` and environment variables
3. **Subsystem-aware filtering**: slog loggers with subsystem attributes get per-subsystem level control

**Note**: This slog bridge exists as an intermediate solution while go-log uses zap internally. In the future, go-log may migrate from zap to native slog, which would simplify this integration.

### Application Setup (Required)

For slog-based logging to use go-log's formatting and level control, applications must explicitly set go-log's handler as the slog default:

```go
import (
	"log/slog"
	golog "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/gologshim"
)

func init() {
	// Set go-log's handler as the application-wide slog default.
	// This ensures all slog-based logging uses go-log's formatting.
	slog.SetDefault(slog.New(golog.SlogHandler()))

	// Wire libraries that use explicit handler passing (like go-libp2p).
	// This ensures proper subsystem attribution for per-logger level control.
	gologshim.SetDefaultHandler(golog.SlogHandler())
}
```

This two-layer approach ensures:
- **Application-level**: All slog usage (application code + libraries) flows through go-log
- **Library-level**: Libraries with explicit wiring (like go-libp2p) include proper subsystem attributes

### How it works

When configured as shown above, slog-based libraries gain unified formatting and dynamic level control.

**Attributes added by libraries:**
- `logger`: Subsystem name (e.g., "foo", "bar", "baz")
- Any additional labels from `GOLOG_LOG_LABELS`

Example:
```go
var log = logging.Logger("foo")  // gologshim
log.Debug("operation failed", "err", err)
```

When integrated with go-log, output is formatted by go-log (JSON format shown here, also supports color/nocolor):
```json
{
  "level": "debug",
  "ts": "2025-10-27T12:34:56.789+0100",
  "logger": "foo",
  "caller": "foo/foo.go:72",
  "msg": "operation failed",
  "err": "connection refused"
}
```

### Controlling slog logger levels

These loggers respect go-log's level configuration:

```bash
# Via environment variable (before daemon starts)
export GOLOG_LOG_LEVEL="error,foo=debug"

# Via API (while daemon is running)
logging.SetLogLevel("foo", "debug")
```

This works even if the logger is created lazily or hasn't been created yet. Level settings are preserved and applied when the logger is first used.

### Direct slog usage without subsystem

When using slog.Default() directly without adding a "logger" attribute, logs still work but have limitations:

**What works:**
- Logs appear in output with go-log's formatting (JSON/color/nocolor)
- Uses global log level from `GOLOG_LOG_LEVEL` fallback or `SetAllLoggers()`

**Limitations:**
- No subsystem-specific level control via `SetLogLevel("subsystem", "level")`
- Empty logger name in output
- Less efficient (no early atomic level filtering)

**Example:**
```go
// Direct slog usage - uses global level only
slog.Info("message")  // LoggerName = "", uses global level

// Library with subsystem - subsystem-aware
log := mylib.Logger("foo")
log.Info("message")  // LoggerName = "foo", uses subsystem level
```

For libraries, use the "logger" attribute pattern to enable per-subsystem control.

### Why "logger" attribute?

go-log uses `"logger"` as the attribute key for subsystem names to maintain backward compatibility with its existing Zap-based output format:

- Maintains compatibility with existing go-log output format
- Existing tooling, dashboards, and log processors already parse the "logger" field
- Simplifies migration path from Zap to slog bridge

Libraries integrating with go-log should use this same attribute key to ensure proper subsystem-aware level control.

### For library authors

Libraries using slog can integrate with go-log without adding go-log as a dependency. There are two approaches:

#### Approach 1: Duck-typing detection (automatic)

Detect go-log's slog bridge via an interface marker to avoid requiring go-log in library's go.mod:

```go
// In your library's logging package
func Logger(subsystem string) *slog.Logger {
    // Check if slog.Default() is go-log's bridge.
    // This works when applications call slog.SetDefault(slog.New(golog.SlogHandler())).
    handler := slog.Default().Handler()

    type goLogBridge interface {
        GoLogBridge()
    }
    if _, ok := handler.(goLogBridge); ok {
        // go-log's bridge is active - use it with subsystem attribute
        h := handler.WithAttrs([]slog.Attr{
            slog.String("logger", subsystem),
        })
        return slog.New(h)
    }

    // Standalone handler when go-log is not present
    return slog.New(createStandaloneHandler(subsystem))
}
```

Usage in your library:
```go
var log = mylib.Logger("foo")
log.Debug("operation completed", "key", value)
```

This pattern allows libraries to automatically integrate when the application has set up go-log's handler, without requiring go-log as a dependency.

#### Approach 2: Explicit handler passing (manual)

Alternatively, expose a way for applications to provide a handler explicitly:

```go
// In your library's logging package
var defaultHandler atomic.Pointer[slog.Handler]

func SetDefaultHandler(handler slog.Handler) {
    defaultHandler.Store(&handler)
}

func Logger(subsystem string) *slog.Logger {
    if h := defaultHandler.Load(); h != nil {
        // Use provided handler with subsystem attribute
        return slog.New((*h).WithAttrs([]slog.Attr{
            slog.String("logger", subsystem),
        }))
    }
    // Standalone handler when go-log is not present
    return slog.New(createStandaloneHandler(subsystem))
}
```

Usage in your library:
```go
var log = mylib.Logger("bar")
log.Info("started service", "addr", addr)
```

**Application side** must explicitly wire it, for example, go-libp2p requires:

```go
import (
    golog "github.com/ipfs/go-log/v2"
    "github.com/libp2p/go-libp2p/gologshim"
)

func init() {
    // Use go-log's SlogHandler() to get the bridge directly.
    // This works regardless of GOLOG_CAPTURE_DEFAULT_SLOG setting.
    gologshim.SetDefaultHandler(golog.SlogHandler())
}
```

**Tradeoff**: Approach 2 requires manual coordination in every application, while Approach 1 works automatically when applications set up `slog.Default()`.

For a complete example, see [go-libp2p's gologshim](https://github.com/libp2p/go-libp2p/blob/master/gologshim/gologshim.go).

### Enabling automatic slog capture (opt-in)

**Note**: This is mostly used during development, when a library author decides between Approach 1 or 2 for proper (subsystem-aware) integration with go-log.

You can enable automatic installation of go-log's handler during `SetupLogging()`:

```bash
export GOLOG_CAPTURE_DEFAULT_SLOG="true"
```

When enabled, go-log automatically installs its handler as `slog.Default()`, which allows slog-based libraries to automatically use go-log's formatting without explicit application setup.

## Contribute

Feel free to join in. All welcome. Open an [issue](https://github.com/ipfs/go-log/issues)!

This repository falls under the IPFS [Code of Conduct](https://github.com/ipfs/community/blob/master/code-of-conduct.md).

### Want to hack on IPFS?

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/CONTRIBUTING.md)

## License

MIT
