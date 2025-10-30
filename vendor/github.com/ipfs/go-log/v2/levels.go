package log

import "go.uber.org/zap/zapcore"

// LogLevel represents a log severity level. Use the package variables as an
// enum.
type LogLevel zapcore.Level

// DefaultName is the subsystem name that identifies the default log level.
const DefaultName = ""

// String returns the name of a LogLevel.
func (lvl LogLevel) String() string {
	return zapcore.Level(lvl).String()
}

var (
	LevelDebug  = LogLevel(zapcore.DebugLevel)
	LevelInfo   = LogLevel(zapcore.InfoLevel)
	LevelWarn   = LogLevel(zapcore.WarnLevel)
	LevelError  = LogLevel(zapcore.ErrorLevel)
	LevelDPanic = LogLevel(zapcore.DPanicLevel)
	LevelPanic  = LogLevel(zapcore.PanicLevel)
	LevelFatal  = LogLevel(zapcore.FatalLevel)
)

// Parse parses a string-based level and returns the corresponding LogLevel. An
// error is returned of the string is not the name of a supported LogLevel.
func Parse(name string) (LogLevel, error) {
	var lvl zapcore.Level
	err := lvl.Set(name)
	return LogLevel(lvl), err
}

// LevelFromString parses a string-based level and returns the corresponding
// LogLevel.
//
// This function is maintained for v1 compatibility only and will be removed in a
// future version. New code should use Parse instead.
func LevelFromString(level string) (LogLevel, error) {
	return Parse(level)
}

// DefaultLevel returns the current default LogLevel.
func DefaultLevel() LogLevel {
	loggerMutex.RLock()
	lvl := defaultLevel
	loggerMutex.RUnlock()
	return lvl
}

// SubsystemLevelName returns the current log level name for a given subsystem.
// An empty name, "", returns the default LogLevel name.
func SubsystemLevelName(subsys string) (string, error) {
	if subsys == DefaultName {
		return DefaultLevel().String(), nil
	}
	lvl, ok := levels[subsys]
	if !ok {
		return "", ErrNoSuchLogger
	}
	return lvl.Level().String(), nil
}

// SubsystemLevelNames returns a map of all facility names to their current log
// levels as strings. The map includes the default log level identified by the
// defaultName string as the map key.
func SubsystemLevelNames() map[string]string {
	result := make(map[string]string, len(levels)+1)

	result[DefaultName] = DefaultLevel().String()

	for name, level := range levels {
		result[name] = level.Level().String()
	}

	return result
}
