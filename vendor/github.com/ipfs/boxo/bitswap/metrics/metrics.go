package metrics

import (
	"context"

	"github.com/ipfs/go-metrics-interface"
)

var (
	// the 1<<18+15 is to observe old file chunks that are 1<<18 + 14 in size
	metricsBuckets = []float64{1 << 6, 1 << 10, 1 << 14, 1 << 18, 1<<18 + 15, 1 << 22}

	timeMetricsBuckets = []float64{1, 10, 30, 60, 90, 120, 600}
)

func DupHist(ctx context.Context) metrics.Histogram {
	return metrics.NewCtx(ctx, "recv_dup_blocks_bytes", "Summary of duplicate data blocks received").Histogram(metricsBuckets)
}

func AllHist(ctx context.Context) metrics.Histogram {
	return metrics.NewCtx(ctx, "recv_all_blocks_bytes", "Summary of all data blocks received").Histogram(metricsBuckets)
}

func SentHist(ctx context.Context) metrics.Histogram {
	return metrics.NewCtx(ctx, "sent_all_blocks_bytes", "Histogram of blocks sent by this bitswap").Histogram(metricsBuckets)
}

func SendTimeHist(ctx context.Context) metrics.Histogram {
	return metrics.NewCtx(ctx, "send_times", "Histogram of how long it takes to send messages in this bitswap").Histogram(timeMetricsBuckets)
}

func PendingEngineGauge(ctx context.Context) metrics.Gauge {
	return metrics.NewCtx(ctx, "pending_tasks", "Total number of pending tasks").Gauge()
}

func ActiveEngineGauge(ctx context.Context) metrics.Gauge {
	return metrics.NewCtx(ctx, "active_tasks", "Total number of active tasks").Gauge()
}

func PendingBlocksGauge(ctx context.Context) metrics.Gauge {
	return metrics.NewCtx(ctx, "pending_block_tasks", "Total number of pending blockstore tasks").Gauge()
}

func ActiveBlocksGauge(ctx context.Context) metrics.Gauge {
	return metrics.NewCtx(ctx, "active_block_tasks", "Total number of active blockstore tasks").Gauge()
}
