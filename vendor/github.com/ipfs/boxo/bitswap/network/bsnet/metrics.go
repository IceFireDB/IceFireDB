package bsnet

import (
	"context"

	imetrics "github.com/ipfs/go-metrics-interface"
)

var durationHistogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480, 960, 1920}

var blockSizesHistogramBuckets = []float64{1, 128 << 10, 256 << 10, 512 << 10, 1024 << 10, 2048 << 10, 4092 << 10}

func requestsInFlight(ctx context.Context) imetrics.Gauge {
	return imetrics.NewCtx(ctx, "requests_in_flight", "Current number of in-flight requests").Gauge()
}

func responseSizes(ctx context.Context) imetrics.Histogram {
	return imetrics.NewCtx(ctx, "response_bytes", "Histogram of bitswap response sizes").Histogram(blockSizesHistogramBuckets)
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

type metrics struct {
	RequestsInFlight    imetrics.Gauge
	WantlistsTotal      imetrics.Counter
	WantlistsItemsTotal imetrics.Counter
	WantlistsSeconds    imetrics.Histogram
	ResponseSizes       imetrics.Histogram
}

func newMetrics() *metrics {
	ctx := imetrics.CtxScope(context.Background(), "exchange_bitswap")

	return &metrics{
		RequestsInFlight:    requestsInFlight(ctx),
		WantlistsTotal:      wantlistsTotal(ctx),
		WantlistsItemsTotal: wantlistsItemsTotal(ctx),
		WantlistsSeconds:    wantlistsSeconds(ctx),
		ResponseSizes:       responseSizes(ctx),
	}
}
