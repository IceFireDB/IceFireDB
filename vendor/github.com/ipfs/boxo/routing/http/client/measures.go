package client

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	distMS     = view.Distribution(0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000)
	distLength = view.Distribution(0, 1, 2, 5, 10, 11, 12, 15, 20, 50, 100, 200, 500)

	measureLatency = stats.Int64("routing_http_client_latency", "the latency of operations by the routing HTTP client", stats.UnitMilliseconds)
	measureLength  = stats.Int64("routing_http_client_length", "the number of elements in a response collection", stats.UnitDimensionless)

	keyOperation  = tag.MustNewKey("operation")
	keyHost       = tag.MustNewKey("host")
	keyStatusCode = tag.MustNewKey("code")
	keyError      = tag.MustNewKey("error")
	keyMediaType  = tag.MustNewKey("mediatype")

	ViewLatency = &view.View{
		Measure:     measureLatency,
		Aggregation: distMS,
		TagKeys:     []tag.Key{keyOperation, keyHost, keyStatusCode, keyError},
	}
	ViewLength = &view.View{
		Measure:     measureLength,
		Aggregation: distLength,
		TagKeys:     []tag.Key{keyOperation, keyHost},
	}

	OpenCensusViews = []*view.View{
		ViewLatency,
		ViewLength,
	}
)

type measurement struct {
	mediaType  string
	operation  string
	err        error
	latency    time.Duration
	statusCode int
	host       string
	length     int
}

func (m measurement) record(ctx context.Context) {
	muts := []tag.Mutator{
		tag.Upsert(keyHost, m.host),
		tag.Upsert(keyOperation, m.operation),
		tag.Upsert(keyStatusCode, strconv.Itoa(m.statusCode)),
		tag.Upsert(keyError, metricsErrStr(m.err)),
		tag.Upsert(keyMediaType, m.mediaType),
	}
	stats.RecordWithTags(ctx, muts, measureLatency.M(m.latency.Milliseconds()))
	stats.RecordWithTags(ctx, muts, measureLength.M(int64(m.length)))
}

func newMeasurement(operation string) *measurement {
	return &measurement{
		operation: operation,
		host:      "None",
		mediaType: "None",
	}
}

// metricsErrStr converts an error into a string that can be used as a metric label.
// Errs are mapped to strings explicitly to avoid accidental high dimensionality.
func metricsErrStr(err error) string {
	if err == nil {
		return "None"
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return "HTTP"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return "DNSNotFound"
		}
		if dnsErr.IsTimeout {
			return "DNSTimeout"
		}
		return "DNS"
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "NetTimeout"
		}
		return "Net"
	}

	return "Other"
}
