package tracing

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/exporters/zipkin"
	"go.opentelemetry.io/otel/sdk/trace"
)

// NewSpanExporters returns a list of trace.SpanExporter based on the OTEL_*
// environment variables which are standardized. Unfortunately, they are not yet
// supported by opentelemetry-go by default. Once they are, we can likely remove
// most of this code.
//
// Specs:
// - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/configuration/sdk-environment-variables.md#exporter-selection
// - https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md
func NewSpanExporters(ctx context.Context) ([]trace.SpanExporter, error) {
	var exporters []trace.SpanExporter
	for _, exporterStr := range strings.Split(os.Getenv("OTEL_TRACES_EXPORTER"), ",") {
		switch exporterStr {
		case "otlp":
			protocol := "http/protobuf"
			if v := os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"); v != "" {
				protocol = v
			}
			if v := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"); v != "" {
				protocol = v
			}

			switch protocol {
			case "http/protobuf":
				exporter, err := otlptracehttp.New(ctx)
				if err != nil {
					return nil, fmt.Errorf("building OTLP HTTP exporter: %w", err)
				}
				exporters = append(exporters, exporter)
			case "grpc":
				exporter, err := otlptracegrpc.New(ctx)
				if err != nil {
					return nil, fmt.Errorf("building OTLP gRPC exporter: %w", err)
				}
				exporters = append(exporters, exporter)
			default:
				return nil, fmt.Errorf("unknown or unsupported OTLP exporter '%s'", exporterStr)
			}
		case "zipkin":
			exporter, err := zipkin.New("")
			if err != nil {
				return nil, fmt.Errorf("building Zipkin exporter: %w", err)
			}
			exporters = append(exporters, exporter)
		case "file":
			// This is not part of the spec, but provided for convenience
			// so that you don't have to setup a collector,
			// and because we don't support the stdout exporter.
			filePath := os.Getenv("OTEL_EXPORTER_FILE_PATH")
			if filePath == "" {
				cwd, err := os.Getwd()
				if err != nil {
					return nil, fmt.Errorf("finding working directory for the OpenTelemetry file exporter: %w", err)
				}
				filePath = path.Join(cwd, "traces.json")
			}
			exporter, err := newFileExporter(filePath)
			if err != nil {
				return nil, err
			}
			exporters = append(exporters, exporter)
		case "none":
			continue
		case "":
			continue
		case "stdout":
			exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
			if err != nil {
				return nil, err
			}
			exporters = append(exporters, exporter)
		default:
			return nil, fmt.Errorf("unknown or unsupported exporter '%s'", exporterStr)
		}
	}
	return exporters, nil
}
