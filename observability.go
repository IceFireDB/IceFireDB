package main

import (
	"log"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// observabilityMux builds the HTTP handler for the observability endpoints.
// readyFn reports whether the node is ready to serve (storage initialized).
func observabilityMux(readyFn func() bool) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "icefiredb_connected_clients",
			Help: "Number of currently connected RESP clients.",
		}, func() float64 { return float64(atomic.LoadInt64(&respClientNum)) }),
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if readyFn() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// startObservabilityServer runs the observability endpoints on addr in a
// goroutine. ready reports node readiness (storage initialized).
func startObservabilityServer(addr string, ready func() bool) {
	go func() {
		log.Printf("observability server listening on %s (/healthz /readyz /metrics)", addr)
		if err := http.ListenAndServe(addr, observabilityMux(ready)); err != nil {
			log.Printf("observability server stopped: %v", err)
		}
	}()
}
