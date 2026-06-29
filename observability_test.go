package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzAlwaysOK(t *testing.T) {
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return false }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rr.Code)
	}
}

func TestReadyzReflectsReadiness(t *testing.T) {
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return false }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz (not ready) = %d, want 503", rr.Code)
	}
	rr = httptest.NewRecorder()
	observabilityMux(func() bool { return true }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/readyz (ready) = %d, want 200", rr.Code)
	}
}

func TestMetricsServed(t *testing.T) {
	rr := httptest.NewRecorder()
	observabilityMux(func() bool { return true }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct == "" {
		t.Fatalf("/metrics missing Content-Type")
	}
}
