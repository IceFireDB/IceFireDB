// Package tracing creates OpenTelemetry span exporters from environment
// variables.
//
// [NewSpanExporters] reads OTEL_TRACES_EXPORTER and returns the corresponding
// exporters. Supported values:
//
//   - "otlp": OTLP exporter (protocol selected by OTEL_EXPORTER_OTLP_PROTOCOL,
//     defaults to "http/protobuf"; also supports "grpc")
//   - "stdout": Pretty-printed traces on standard output
//   - "file": JSON traces written to OTEL_EXPORTER_FILE_PATH (defaults to traces.json)
//   - "none": Disables trace export
//
// Multiple exporters can be enabled by separating values with commas.
package tracing
