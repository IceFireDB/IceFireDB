// Package metrics provides a unified interface for different metrics
// backends (prometheus etc.).
package metrics

import (
	"time"
)

// Counter is a metric that can only be incremented.
type Counter interface {
	Inc()
	Add(float64) // Only positive
}

// CounterVec is a counter with tags.
type CounterVec interface {
	WithLabelValues(lvs ...string) Counter
}

// Gauge is a metric that can be increased and decreased.
type Gauge interface {
	Set(float64) // Introduced discontinuity
	Inc()
	Dec()
	Add(float64)
	Sub(float64)
}

// Histogram metric.
type Histogram interface {
	Observe(float64) // Adds observation to Histogram
}

type Summary interface {
	Observe(float64) // Adds observation to Summary
}

// SummaryOpts allow specifying options for Summary. See: http://godoc.org/github.com/prometheus/client_golang/prometheus#SummaryOpts .
type SummaryOpts struct {
	Objectives map[float64]float64
	MaxAge     time.Duration
	AgeBuckets uint32
	BufCap     uint32
}

// Creator can be used to create different types of metrics.
type Creator interface {
	Counter() Counter
	CounterVec(labelNames []string) CounterVec
	Gauge() Gauge
	Histogram(buckets []float64) Histogram

	// opts cannot be nil, use empty summary instance
	Summary(opts SummaryOpts) Summary
}
