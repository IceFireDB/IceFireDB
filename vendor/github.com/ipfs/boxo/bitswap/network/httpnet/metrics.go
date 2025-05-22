package httpnet

import (
	"context"
	"strconv"

	imetrics "github.com/ipfs/go-metrics-interface"
)

var durationHistogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480, 960, 1920}

var blockSizesHistogramBuckets = []float64{1, 128 << 10, 256 << 10, 512 << 10, 1024 << 10, 2048 << 10, 4092 << 10}

func requestsInFlight(ctx context.Context) imetrics.Gauge {
	return imetrics.NewCtx(ctx, "requests_in_flight", "Current number of in-flight requests").Gauge()
}

func requestsTotal(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_total", "Total request count").Counter()
}

func requestsFailure(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_failure", "Failed (no response, dial error etc) requests count").Counter()
}

func requestSentBytes(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "request_sent_bytes", "Total bytes sent on requests").Counter()
}

func requestTime(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "request_duration_seconds", "Histogram of request durations").Histogram(durationHistogramBuckets)
}

func requestsBodyFailure(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "requests_body_failure", "Failure count when reading response body").Counter()
}

func responseSizes(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "response_bytes", "Histogram of http response sizes").Histogram(blockSizesHistogramBuckets)
}

func wantlistsTotal(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "wantlists_total", "Total number of wantlists sent").Counter()
}

func wantlistsItemsTotal(ctx context.Context) imetrics.Counter {
	return imetrics.NewCtx(ctx, "wantlists_items_total", "Total number of elements in sent wantlists").Counter()
}

func wantlistsSeconds(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "wantlists_seconds", "Number of seconds spent sending wantlists").Histogram(durationHistogramBuckets)
}

func status(ctx context.Context) imetrics.CounterVec {
	return imetrics.NewCtx(ctx, "status", "Request status count").CounterVec([]string{"method", "status", "host"})
}

type metrics struct {
	trackedEndpoints map[string]struct{}

	RequestsInFlight    imetrics.Gauge
	RequestsTotal       imetrics.Counter
	RequestsFailure     imetrics.Counter
	RequestsSentBytes   imetrics.Counter
	WantlistsTotal      imetrics.Counter
	WantlistsItemsTotal imetrics.Counter
	WantlistsSeconds    imetrics.Histogram
	ResponseSizes       imetrics.Histogram
	RequestsBodyFailure imetrics.Counter
	Status              imetrics.CounterVec
	RequestTime         imetrics.Histogram
}

func newMetrics(endpoints map[string]struct{}) *metrics {
	ctx := imetrics.CtxScope(context.Background(), "exchange_httpnet")

	return &metrics{
		trackedEndpoints:    endpoints,
		RequestsInFlight:    requestsInFlight(ctx),
		RequestsTotal:       requestsTotal(ctx),
		RequestsSentBytes:   requestSentBytes(ctx),
		RequestsFailure:     requestsFailure(ctx),
		RequestsBodyFailure: requestsBodyFailure(ctx),
		WantlistsTotal:      wantlistsTotal(ctx),
		WantlistsItemsTotal: wantlistsItemsTotal(ctx),
		WantlistsSeconds:    wantlistsSeconds(ctx),
		ResponseSizes:       responseSizes(ctx),
		// labels: method, status, host
		Status:      status(ctx),
		RequestTime: requestTime(ctx),
	}
}

func (m *metrics) updateStatusCounter(method string, statusCode int, host string) {
	m.RequestsTotal.Inc()
	// Track all for the moment.
	// if _, ok := m.trackedEndpoints[host]; !ok {
	// 	host = "other"
	// }

	var statusStr string

	switch statusCode {
	case 200, 400, 403, 404, 410, 429, 451, 500, 502, 504:
		statusStr = strconv.Itoa(statusCode)
	default:
		statusStr = "other"
	}

	m.Status.WithLabelValues(method, statusStr, host).Inc()
}
