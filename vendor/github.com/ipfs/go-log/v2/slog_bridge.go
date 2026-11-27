package log

import (
	"context"
	"log/slog"
	"math"
	"runtime"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// subsystemAttrKey is the attribute key used by gologshim to pass subsystem names
const subsystemAttrKey = "logger"

// zapToSlogBridge adapts a zapcore.Core to implement slog.Handler.
// This allows slog logs to be processed through zap's formatting.
// IMPORTANT: core field maintains a reference to the LIVE loggerCore, not a snapshot.
// This ensures logs reach cores added dynamically (e.g., pipe readers from ipfs log tail).
type zapToSlogBridge struct {
	core          zapcore.Core
	subsystemName string          // Used for LoggerName in zap entries
	fields        []zapcore.Field // Fields accumulated from WithAttrs calls
}

// newZapToSlogBridge creates a new slog.Handler that writes to the given zap core.
func newZapToSlogBridge(core zapcore.Core) slog.Handler {
	return &zapToSlogBridge{core: core}
}

// subsystemAwareHandler wraps zapToSlogBridge and provides early level filtering
// using go-log's per-subsystem atomic levels. This avoids expensive conversions
// for filtered log messages.
type subsystemAwareHandler struct {
	bridge      *zapToSlogBridge
	subsystem   string
	atomicLevel zap.AtomicLevel
}

// GoLogBridge is a marker method that allows libraries to detect go-log's slog bridge
// at runtime via duck typing, without adding go-log to their dependency tree.
// This enables automatic integration when go-log is present in the application.
func (h *subsystemAwareHandler) GoLogBridge() {}

func (h *subsystemAwareHandler) Enabled(_ context.Context, level slog.Level) bool {
	// Fast path - check subsystem level FIRST before expensive conversions
	zapLevel := slogLevelToZap(level)
	if !h.atomicLevel.Enabled(zapLevel) {
		return false
	}
	return h.bridge.core.Enabled(zapLevel)
}

func (h *subsystemAwareHandler) Handle(ctx context.Context, record slog.Record) error {
	return h.bridge.Handle(ctx, record)
}

func (h *subsystemAwareHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Convert attrs to zap fields, filtering out subsystem key if present
	fields := make([]zapcore.Field, 0, len(h.bridge.fields)+len(attrs))
	fields = append(fields, h.bridge.fields...) // Preserve existing fields
	for _, attr := range attrs {
		if attr.Key != subsystemAttrKey {
			fields = append(fields, slogAttrToZapField(attr))
		}
	}

	return &subsystemAwareHandler{
		bridge: &zapToSlogBridge{
			core:          h.bridge.core, // Keep reference to LIVE loggerCore
			subsystemName: h.subsystem,
			fields:        fields, // Accumulate fields
		},
		subsystem:   h.subsystem,
		atomicLevel: h.atomicLevel,
	}
}

// TODO: same no-op as zapToSlogBridge.WithGroup; see TODO there.
func (h *subsystemAwareHandler) WithGroup(name string) slog.Handler {
	return &subsystemAwareHandler{
		bridge: &zapToSlogBridge{
			core:          h.bridge.core,
			subsystemName: h.subsystem,
			fields:        h.bridge.fields, // Preserve fields
		},
		subsystem:   h.subsystem,
		atomicLevel: h.atomicLevel,
	}
}

// getOrCreateAtomicLevel returns the atomic level for a subsystem,
// creating it with the default level if it doesn't exist.
func getOrCreateAtomicLevel(subsystem string) zap.AtomicLevel {
	loggerMutex.RLock()
	atomicLevel, exists := levels[subsystem]
	loggerMutex.RUnlock()

	if !exists {
		loggerMutex.Lock()
		// Check again in case another goroutine created it
		if atomicLevel, exists = levels[subsystem]; !exists {
			atomicLevel = zap.NewAtomicLevelAt(zapcore.Level(defaultLevel))
			levels[subsystem] = atomicLevel
		}
		loggerMutex.Unlock()
	}

	return atomicLevel
}

// GoLogBridge is a marker method that allows libraries to detect go-log's slog bridge
// at runtime via duck typing, without adding go-log to their dependency tree.
// This enables automatic integration when go-log is present in the application.
func (h *zapToSlogBridge) GoLogBridge() {}

// Enabled implements slog.Handler.
func (h *zapToSlogBridge) Enabled(_ context.Context, level slog.Level) bool {
	return h.core.Enabled(slogLevelToZap(level))
}

// Handle implements slog.Handler.
func (h *zapToSlogBridge) Handle(_ context.Context, record slog.Record) error {
	// Convert slog.Record to zap fields, prepending stored fields from WithAttrs
	fields := make([]zapcore.Field, 0, len(h.fields)+record.NumAttrs())
	fields = append(fields, h.fields...) // Add stored fields first
	record.Attrs(func(attr slog.Attr) bool {
		fields = append(fields, slogAttrToZapField(attr))
		return true
	})

	// Resolve PC to file:line for accurate source location
	var caller zapcore.EntryCaller
	if record.PC != 0 {
		frames := runtime.CallersFrames([]uintptr{record.PC})
		frame, _ := frames.Next()
		caller = zapcore.NewEntryCaller(record.PC, frame.File, frame.Line, frame.PC != 0)
	} else {
		caller = zapcore.NewEntryCaller(0, "", 0, false)
	}

	// Create zap entry
	entry := zapcore.Entry{
		Level:      slogLevelToZap(record.Level),
		Time:       record.Time,
		Message:    record.Message,
		LoggerName: h.subsystemName,
		Caller:     caller,
	}

	// Use Check() to respect each core's level filtering (important for pipe readers)
	ce := h.core.Check(entry, nil)
	if ce != nil {
		ce.Write(fields...)
	}
	return nil
}

// WithAttrs implements slog.Handler.
func (h *zapToSlogBridge) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Check if subsystem attribute present (gologshim adds this)
	var subsystem string
	fields := make([]zapcore.Field, 0, len(h.fields)+len(attrs))
	fields = append(fields, h.fields...) // Preserve existing fields

	for _, attr := range attrs {
		if attr.Key == subsystemAttrKey {
			subsystem = attr.Value.String()
			// Don't include as a field - will use as LoggerName
		} else {
			fields = append(fields, slogAttrToZapField(attr))
		}
	}

	newBridge := &zapToSlogBridge{
		core:          h.core, // Keep reference to LIVE loggerCore, don't snapshot
		subsystemName: subsystem,
		fields:        fields, // Accumulate fields to apply during Handle()
	}

	// If subsystem specified, wrap with level-aware handler for early filtering
	if subsystem != "" {
		atomicLevel := getOrCreateAtomicLevel(subsystem)
		return &subsystemAwareHandler{
			bridge:      newBridge,
			subsystem:   subsystem,
			atomicLevel: atomicLevel,
		}
	}

	return newBridge
}

// WithGroup implements slog.Handler.
//
// TODO: Handler.WithGroup is a no-op. Inline slog.Group(...) attrs render as
// nested objects via slogGroup, but attrs added after a Handler.WithGroup
// call are not nested under the group name. Full support needs deferred
// attr conversion plus a group-frame stack walked at Handle time, with the
// subsystem-key filter applied only at depth 0.
func (h *zapToSlogBridge) WithGroup(name string) slog.Handler {
	return &zapToSlogBridge{
		core:          h.core,
		subsystemName: h.subsystemName, // Preserve subsystem
		fields:        h.fields,        // Preserve fields
	}
}

// slogLevelToZap converts slog.Level to zapcore.Level.
func slogLevelToZap(level slog.Level) zapcore.Level {
	switch {
	case level >= slog.LevelError:
		return zapcore.ErrorLevel
	case level >= slog.LevelWarn:
		return zapcore.WarnLevel
	case level >= slog.LevelInfo:
		return zapcore.InfoLevel
	default:
		return zapcore.DebugLevel
	}
}

// slogAttrToZapField converts slog.Attr to zapcore.Field.
func slogAttrToZapField(attr slog.Attr) zapcore.Field {
	key := attr.Key
	// slog: handlers must resolve LogValuer at the leaf.
	value := attr.Value.Resolve()

	switch value.Kind() {
	case slog.KindString:
		return zapcore.Field{Key: key, Type: zapcore.StringType, String: value.String()}
	case slog.KindInt64:
		return zapcore.Field{Key: key, Type: zapcore.Int64Type, Integer: value.Int64()}
	case slog.KindUint64:
		return zapcore.Field{Key: key, Type: zapcore.Uint64Type, Integer: int64(value.Uint64())}
	case slog.KindFloat64:
		return zapcore.Field{Key: key, Type: zapcore.Float64Type, Integer: int64(math.Float64bits(value.Float64()))}
	case slog.KindBool:
		return zapcore.Field{Key: key, Type: zapcore.BoolType, Integer: boolToInt64(value.Bool())}
	case slog.KindDuration:
		return zapcore.Field{Key: key, Type: zapcore.DurationType, Integer: value.Duration().Nanoseconds()}
	case slog.KindTime:
		return zapcore.Field{Key: key, Type: zapcore.TimeType, Integer: value.Time().UnixNano(), Interface: value.Time().Location()}
	case slog.KindGroup:
		g := value.Group()
		if len(g) == 0 {
			// slog: a Group with no Attrs is ignored.
			return zap.Skip()
		}
		if key == "" {
			// slog: a Group with an empty key is inlined into its parent.
			return zap.Inline(slogGroup(g))
		}
		return zap.Object(key, slogGroup(g))
	case slog.KindAny:
		return zapcore.Field{Key: key, Type: zapcore.ReflectType, Interface: value.Any()}
	default:
		// Fallback for complex types
		return zapcore.Field{Key: key, Type: zapcore.ReflectType, Interface: value.Any()}
	}
}

// slogGroup adapts a slog group's attrs as a zapcore.ObjectMarshaler so nested
// fields render as a structured object rather than reflected []slog.Attr.
type slogGroup []slog.Attr

func (g slogGroup) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for _, attr := range g {
		v := attr.Value.Resolve()
		// slog inlines a Group whose key is empty into the enclosing object.
		if attr.Key == "" && v.Kind() == slog.KindGroup {
			if err := slogGroup(v.Group()).MarshalLogObject(enc); err != nil {
				return err
			}
			continue
		}
		addSlogAttrToObjectEncoder(enc, slog.Attr{Key: attr.Key, Value: v})
	}
	return nil
}

// addSlogAttrToObjectEncoder mirrors slogAttrToZapField for nested object
// encoding; keep the two switches in sync when adding new slog.Kind cases.
func addSlogAttrToObjectEncoder(enc zapcore.ObjectEncoder, attr slog.Attr) {
	key := attr.Key
	// slog: handlers must resolve LogValuer at the leaf.
	value := attr.Value.Resolve()
	switch value.Kind() {
	case slog.KindString:
		enc.AddString(key, value.String())
	case slog.KindInt64:
		enc.AddInt64(key, value.Int64())
	case slog.KindUint64:
		enc.AddUint64(key, value.Uint64())
	case slog.KindFloat64:
		enc.AddFloat64(key, value.Float64())
	case slog.KindBool:
		enc.AddBool(key, value.Bool())
	case slog.KindDuration:
		enc.AddDuration(key, value.Duration())
	case slog.KindTime:
		enc.AddTime(key, value.Time())
	case slog.KindGroup:
		g := value.Group()
		if len(g) == 0 {
			return
		}
		_ = enc.AddObject(key, slogGroup(g))
	default:
		_ = enc.AddReflected(key, value.Any())
	}
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
