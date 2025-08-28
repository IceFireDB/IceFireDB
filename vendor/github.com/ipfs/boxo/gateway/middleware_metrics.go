package gateway

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Use the main gateway logger for consistency
// var log is defined in handler.go

// middlewareMetrics holds metrics used by gateway middleware
type middlewareMetrics struct {
	// General HTTP response counter
	httpResponsesTotal *prometheus.CounterVec

	// Middleware-specific metrics
	retrievalTimeouts  *prometheus.CounterVec
	concurrentRequests prometheus.Gauge
}

// recordResponse records an HTTP response with the given status code
func (m *middlewareMetrics) recordResponse(statusCode int) {
	if m != nil && m.httpResponsesTotal != nil {
		code := strconv.Itoa(statusCode)
		m.httpResponsesTotal.With(prometheus.Labels{"code": code}).Inc()
	}
}

// recordTimeout records both HTTP response and timeout-specific metrics
func (m *middlewareMetrics) recordTimeout(statusCode int, truncated bool) {
	if m == nil {
		return
	}
	m.recordResponse(statusCode)
	if m.retrievalTimeouts != nil {
		code := strconv.Itoa(statusCode)
		m.retrievalTimeouts.With(prometheus.Labels{
			"code":      code,
			"truncated": strconv.FormatBool(truncated),
		}).Inc()
	}
}

// incConcurrentRequests increments the concurrent requests gauge
func (m *middlewareMetrics) incConcurrentRequests() {
	if m != nil && m.concurrentRequests != nil {
		m.concurrentRequests.Inc()
	}
}

// decConcurrentRequests decrements the concurrent requests gauge
func (m *middlewareMetrics) decConcurrentRequests() {
	if m != nil && m.concurrentRequests != nil {
		m.concurrentRequests.Dec()
	}
}

// newMiddlewareMetrics creates and initializes metrics used by middleware.
//
// Metrics Design:
//
// All counter metrics include a "code" label with the HTTP status code for consistency.
//
// gw_responses_total{code="XXX"}
//   - Comprehensive counter of ALL HTTP responses by status code
//   - Includes responses from handler AND middleware
//   - 429 responses only come from rate limiting middleware
//
// gw_retrieval_timeouts_total{code="XXX",truncated="true|false"}
//   - code: The HTTP status code (504 for clean timeout, original code for truncation)
//   - truncated: Whether response was truncated mid-stream
//
// gw_concurrent_requests
//   - Gauge of currently processing requests (no code label as it's a gauge)
//
// Example Prometheus queries:
//   - Rate limited requests: sum(rate(gw_responses_total{code="429"}[5m]))
//   - Truncated 200 responses: sum(rate(gw_retrieval_timeouts_total{code="200",truncated="true"}[5m]))
func newMiddlewareMetrics(reg prometheus.Registerer) *middlewareMetrics {
	metrics := &middlewareMetrics{}

	// Initialize the HTTP responses counter
	metrics.httpResponsesTotal = registerOrGetMetric(
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "ipfs",
				Subsystem: "http",
				Name:      "gw_responses_total",
				Help:      "Total number of HTTP responses sent by the gateway, labeled by status code.",
			},
			[]string{"code"},
		),
		"gw_responses_total",
		reg,
	).(*prometheus.CounterVec)

	// Initialize retrieval timeout metrics
	metrics.retrievalTimeouts = registerOrGetMetric(
		prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "ipfs",
				Subsystem: "http",
				Name:      "gw_retrieval_timeouts_total",
				Help:      "Total number of requests that timed out due to slow content retrieval.",
			},
			[]string{"code", "truncated"}, // code: HTTP status, truncated: "true"/"false"
		),
		"gw_retrieval_timeouts_total",
		reg,
	).(*prometheus.CounterVec)

	// Initialize the concurrent requests gauge
	metrics.concurrentRequests = registerOrGetMetric(
		prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "ipfs",
				Subsystem: "http",
				Name:      "gw_concurrent_requests",
				Help:      "Number of HTTP requests currently being processed by the gateway.",
			},
		),
		"gw_concurrent_requests",
		reg,
	).(prometheus.Gauge)

	return metrics
}

// withResponseMetrics wraps an http.Handler to record response status codes
func withResponseMetrics(handler http.Handler, metrics *middlewareMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap the ResponseWriter to capture status code
		mrw := &metricsResponseWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK, // Default to StatusOK if WriteHeader not called
		}

		// Serve the request
		handler.ServeHTTP(mrw, r)

		// Record the response metric
		metrics.recordResponse(mrw.statusCode)
	})
}

// metricsResponseWriter wraps http.ResponseWriter to capture status code
type metricsResponseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (w *metricsResponseWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.statusCode = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *metricsResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

// Hijack implements http.Hijacker if the underlying ResponseWriter supports it
func (w *metricsResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, errors.New("ResponseWriter does not support hijacking")
}

// Flush implements http.Flusher if the underlying ResponseWriter supports it
func (w *metricsResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// registerOrGetMetric is a helper that registers a Prometheus metric and handles AlreadyRegisteredError
func registerOrGetMetric(metric prometheus.Collector, name string, reg prometheus.Registerer) prometheus.Collector {
	if err := reg.Register(metric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector
		}
		log.Errorf("failed to register %s: %v", name, err)
	}
	return metric
}
