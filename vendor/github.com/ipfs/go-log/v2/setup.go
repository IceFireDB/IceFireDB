package log

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mattn/go-isatty"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var config Config

func init() {
	SetupLogging(configFromEnv())
}

// Logging environment variables
const (
	// IPFS_* prefixed env vars kept for backwards compatibility
	// for this release. They will not be available in the next
	// release.
	//
	// GOLOG_* env vars take precedences over IPFS_* env vars.
	envIPFSLogging    = "IPFS_LOGGING"
	envIPFSLoggingFmt = "IPFS_LOGGING_FMT"

	envLogging    = "GOLOG_LOG_LEVEL"
	envLoggingFmt = "GOLOG_LOG_FMT"

	envLoggingFile = "GOLOG_FILE" // /path/to/file
	envLoggingURL  = "GOLOG_URL"  // url that will be processed by sink in the zap

	envLoggingOutput = "GOLOG_OUTPUT"               // possible values: stdout|stderr|file combine multiple values with '+'
	envLoggingLabels = "GOLOG_LOG_LABELS"           // comma-separated key-value pairs, i.e. "app=example_app,dc=sjc-1"
	envCaptureSlog   = "GOLOG_CAPTURE_DEFAULT_SLOG" // set to "true" to enable routing slog logs through go-log's zap core
)

type LogFormat int

const (
	ColorizedOutput LogFormat = iota
	PlaintextOutput
	JSONOutput
)

type Config struct {
	// Format overrides the format of the log output. Defaults to ColorizedOutput
	Format LogFormat

	// Level is the default minimum enabled logging level.
	Level LogLevel

	// SubsystemLevels are the default levels per-subsystem. When unspecified, defaults to Level.
	SubsystemLevels map[string]LogLevel

	// Stderr indicates whether logs should be written to stderr.
	Stderr bool

	// Stdout indicates whether logs should be written to stdout.
	Stdout bool

	// File is a path to a file that logs will be written to.
	File string

	// URL with schema supported by zap. Use zap.RegisterSink
	URL string

	// Labels is a set of key-values to apply to all loggers
	Labels map[string]string
}

// ErrNoSuchLogger is returned when the util pkg is asked for a non existant logger
var ErrNoSuchLogger = errors.New("error: No such logger")

var loggerMutex sync.RWMutex // guards access to global logger state

// loggers is the set of loggers in the system
var loggers = make(map[string]*zap.SugaredLogger)
var levels = make(map[string]zap.AtomicLevel)

// primaryFormat is the format of the primary core used for logging
var primaryFormat LogFormat = ColorizedOutput

// defaultLevel is the default log level
// New loggers will be set to `defaultLevel` when created
var defaultLevel LogLevel = LevelError

// primaryCore is the primary logging core
var primaryCore zapcore.Core

// loggerCore is the base for all loggers created by this package
var loggerCore = &lockedMultiCore{}

// slogBridge is go-log's slog.Handler that routes slog logs through zap.
// It's always created during SetupLogging, even when GOLOG_CAPTURE_DEFAULT_SLOG=false.
// This allows applications to explicitly wire slog-based libraries (like go-libp2p)
// to go-log using SlogHandler(), regardless of whether it's installed as slog.Default().
var slogBridge atomic.Pointer[slog.Handler]

// GetConfig returns a copy of the saved config. It can be inspected, modified,
// and re-applied using a subsequent call to SetupLogging().
func GetConfig() Config {
	return config
}

// SetupLogging will initialize the logger backend and set the flags.
// TODO calling this in `init` pushes all configuration to env variables
// - move it out of `init`? then we need to change all the code (js-ipfs, go-ipfs) to call this explicitly
// - have it look for a config file? need to define what that is
func SetupLogging(cfg Config) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	config = cfg

	primaryFormat = cfg.Format
	defaultLevel = cfg.Level

	outputPaths := []string{}

	if cfg.Stderr {
		outputPaths = append(outputPaths, "stderr")
	}
	if cfg.Stdout {
		outputPaths = append(outputPaths, "stdout")
	}

	// check if we log to a file
	if len(cfg.File) > 0 {
		if path, err := normalizePath(cfg.File); err != nil {
			fmt.Fprintf(os.Stderr, "failed to resolve log path '%q', logging to %s\n", cfg.File, outputPaths)
		} else {
			outputPaths = append(outputPaths, path)
		}
	}
	if len(cfg.URL) > 0 {
		outputPaths = append(outputPaths, cfg.URL)
	}

	ws, _, err := zap.Open(outputPaths...)
	if err != nil {
		panic(fmt.Sprintf("unable to open logging output: %v", err))
	}

	newPrimaryCore := newCore(primaryFormat, ws, LevelDebug) // the main core needs to log everything.

	for k, v := range cfg.Labels {
		newPrimaryCore = newPrimaryCore.With([]zap.Field{zap.String(k, v)})
	}

	setPrimaryCore(newPrimaryCore)
	setAllLoggers(defaultLevel)

	for name, level := range cfg.SubsystemLevels {
		if leveler, ok := levels[name]; ok {
			leveler.SetLevel(zapcore.Level(level))
		} else {
			levels[name] = zap.NewAtomicLevelAt(zapcore.Level(level))
		}
	}

	// Create the slog bridge (always available via SlogHandler()).
	// This allows applications to explicitly wire slog-based libraries to go-log
	// regardless of GOLOG_CAPTURE_DEFAULT_SLOG setting.
	bridge := newZapToSlogBridge(loggerCore)
	slogBridge.Store(&bridge)

	// Install the bridge as slog.Default() if explicitly enabled.
	// When enabled, libraries using slog automatically use go-log's formatting.
	// Libraries can also opt-in to dynamic per-logger level control if they include "logger" attribute.
	if os.Getenv(envCaptureSlog) == "true" {
		captureSlogDefault(bridge)
	}
}

// SlogHandler returns go-log's slog.Handler for explicit wiring.
// This allows applications to integrate slog-based logging with go-log's
// formatting and level control.
//
// Example usage in an application's init():
//
//	import (
//	    "log/slog"
//	    golog "github.com/ipfs/go-log/v2"
//	    "github.com/libp2p/go-libp2p/gologshim"
//	)
//
//	func init() {
//	    // Set go-log's slog handler as the application-wide default.
//	    // This ensures all slog-based logging uses go-log's formatting.
//	    slog.SetDefault(slog.New(golog.SlogHandler()))
//
//	    // Wire go-log's slog bridge to go-libp2p's gologshim.
//	    // This provides go-libp2p loggers with the "logger" attribute
//	    // for per-subsystem level control.
//	    gologshim.SetDefaultHandler(golog.SlogHandler())
//	}
func SlogHandler() slog.Handler {
	if h := slogBridge.Load(); h != nil {
		return *h
	}
	// Should never happen since SetupLogging() is called in init()
	panic("go-log: SlogHandler called before SetupLogging")
}

// captureSlogDefault installs go-log's slog bridge as slog.Default()
func captureSlogDefault(bridge slog.Handler) {
	// Check if slog.Default() is already customized (not stdlib default)
	// and warn the user that we're replacing it
	defaultHandler := slog.Default().Handler()
	if _, isGoLogBridge := defaultHandler.(interface{ GoLogBridge() }); !isGoLogBridge {
		// Not a go-log bridge, check if it's a custom handler
		// We detect custom handlers by checking if it's not a standard text/json handler
		// This is imperfect but reasonably safe - custom handlers are likely wrapped or different types
		handlerType := fmt.Sprintf("%T", defaultHandler)
		if !strings.Contains(handlerType, "slog.defaultHandler") &&
			!strings.Contains(handlerType, "slog.commonHandler") {

			fmt.Fprintf(os.Stderr, "WARN: go-log is overriding custom slog.Default() handler (%s) to ensure logs from slog-based libraries are captured and formatted consistently. This prevents missing logs or stderr pollution. Set GOLOG_CAPTURE_DEFAULT_SLOG=false to disable this behavior.\n", handlerType)

		}
	}

	slog.SetDefault(slog.New(bridge))
}

// SetPrimaryCore changes the primary logging core. If the SetupLogging was
// called then the previously configured core will be replaced.
func SetPrimaryCore(core zapcore.Core) {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	setPrimaryCore(core)
}

func setPrimaryCore(core zapcore.Core) {
	if primaryCore != nil {
		loggerCore.ReplaceCore(primaryCore, core)
	} else {
		loggerCore.AddCore(core)
	}
	primaryCore = core
}

// SetDebugLogging calls SetAllLoggers with logging.DEBUG
func SetDebugLogging() {
	SetAllLoggers(LevelDebug)
}

// SetAllLoggers changes the logging level of all loggers to lvl
func SetAllLoggers(lvl LogLevel) {
	loggerMutex.RLock()
	defer loggerMutex.RUnlock()

	setAllLoggers(lvl)
}

func setAllLoggers(lvl LogLevel) {
	for _, l := range levels {
		l.SetLevel(zapcore.Level(lvl))
	}
}

// SetLogLevel changes the log level of a specific subsystem.
// name=="*" changes all subsystems.
//
// This function works for both native go-log loggers and slog-based loggers
// (e.g., from go-libp2p via gologshim). If the subsystem doesn't exist yet,
// a level entry is created and will be applied when the logger is created.
func SetLogLevel(name, level string) error {
	lvl, err := Parse(level)
	if err != nil {
		return err
	}

	// wildcard, change all
	if name == "*" {
		SetAllLoggers(lvl)
		defaultLevel = lvl
		return nil
	}

	loggerMutex.Lock()
	defer loggerMutex.Unlock()

	// Get or create atomic level for this subsystem
	atomicLevel, ok := levels[name]
	if !ok {
		atomicLevel = zap.NewAtomicLevelAt(zapcore.Level(lvl))
		levels[name] = atomicLevel
	} else {
		atomicLevel.SetLevel(zapcore.Level(lvl))
	}

	return nil
}

// SetLogLevelRegex sets all loggers to level `l` that match expression `e`.
// An error is returned if `e` fails to compile.
func SetLogLevelRegex(e, l string) error {
	lvl, err := Parse(l)
	if err != nil {
		return err
	}

	rem, err := regexp.Compile(e)
	if err != nil {
		return err
	}

	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	for name := range loggers {
		if rem.MatchString(name) {
			levels[name].SetLevel(zapcore.Level(lvl))
		}
	}
	return nil
}

// GetSubsystems returns a slice containing the
// names of the current loggers
func GetSubsystems() []string {
	loggerMutex.RLock()
	defer loggerMutex.RUnlock()
	subs := make([]string, 0, len(loggers))

	for k := range loggers {
		subs = append(subs, k)
	}
	return subs
}

func getLogger(name string) *zap.SugaredLogger {
	loggerMutex.Lock()
	defer loggerMutex.Unlock()
	log, ok := loggers[name]
	if !ok {
		level, ok := levels[name]
		if !ok {
			level = zap.NewAtomicLevelAt(zapcore.Level(defaultLevel))
			levels[name] = level
		}
		log = zap.New(loggerCore).
			WithOptions(
				zap.IncreaseLevel(level),
				zap.AddCaller(),
			).
			Named(name).
			Sugar()

		loggers[name] = log
	}

	return log
}

// configFromEnv returns a Config with defaults populated using environment variables.
func configFromEnv() Config {
	cfg := Config{
		Format:          ColorizedOutput,
		Stderr:          true,
		Level:           LevelError,
		SubsystemLevels: map[string]LogLevel{},
		Labels:          map[string]string{},
	}

	format := os.Getenv(envLoggingFmt)
	if format == "" {
		format = os.Getenv(envIPFSLoggingFmt)
	}

	var noExplicitFormat bool

	switch format {
	case "color":
		cfg.Format = ColorizedOutput
	case "nocolor":
		cfg.Format = PlaintextOutput
	case "json":
		cfg.Format = JSONOutput
	default:
		if format != "" {
			fmt.Fprintf(os.Stderr, "ignoring unrecognized log format '%s'\n", format)
		}
		noExplicitFormat = true
	}

	lvl := os.Getenv(envLogging)
	if lvl == "" {
		lvl = os.Getenv(envIPFSLogging)
	}
	if lvl != "" {
		for _, kvs := range strings.Split(lvl, ",") {
			kv := strings.SplitN(kvs, "=", 2)
			lvl, err := Parse(kv[len(kv)-1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "error setting log level %q: %s\n", kvs, err)
				continue
			}
			switch len(kv) {
			case 1:
				cfg.Level = lvl
			case 2:
				cfg.SubsystemLevels[kv[0]] = lvl
			}
		}
	}

	cfg.File = os.Getenv(envLoggingFile)
	// Disable stderr logging when a file is specified
	// https://github.com/ipfs/go-log/issues/83
	if cfg.File != "" {
		cfg.Stderr = false
	}

	var stderrOpt, stdoutOpt bool
	cfg.URL = os.Getenv(envLoggingURL)
	output := os.Getenv(envLoggingOutput)
	outputOptions := strings.Split(output, "+")
	for _, opt := range outputOptions {
		switch opt {
		case "stdout":
			stdoutOpt = true
		case "stderr":
			stderrOpt = true
		case "file":
			if cfg.File == "" {
				fmt.Fprint(os.Stderr, "please specify a GOLOG_FILE value to write to")
			}
		case "url":
			if cfg.URL == "" {
				fmt.Fprint(os.Stderr, "please specify a GOLOG_URL value to write to")
			}
		}
	}

	if stdoutOpt || stderrOpt {
		cfg.Stdout = stdoutOpt
		cfg.Stderr = stderrOpt
	}

	// Check that neither of the requested Std* nor the file are TTYs
	// At this stage (configFromEnv) we do not have a uniform list to examine yet
	if noExplicitFormat &&
		!(cfg.Stdout && isTerm(os.Stdout)) &&
		!(cfg.Stderr && isTerm(os.Stderr)) &&
		// check this last: expensive
		!(cfg.File != "" && pathIsTerm(cfg.File)) {
		cfg.Format = PlaintextOutput
	}

	labels := os.Getenv(envLoggingLabels)
	if labels != "" {
		labelKVs := strings.Split(labels, ",")
		for _, label := range labelKVs {
			kv := strings.Split(label, "=")
			if len(kv) != 2 {
				fmt.Fprint(os.Stderr, "invalid label k=v: ", label)
				continue
			}
			cfg.Labels[kv[0]] = kv[1]
		}
	}

	return cfg
}

func isTerm(f *os.File) bool {
	return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
}

func pathIsTerm(p string) bool {
	// !!!no!!! O_CREAT, if we fail - we fail
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if f != nil {
		defer f.Close() // nolint:errcheck
	}
	return err == nil && isTerm(f)
}
