package mysql

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
)

func noopLog(string, ...any) {}

func TestMetricsExposition(t *testing.T) {
	m := &mysqlProxy{startedAt: time.Now()}

	before := stats.publishedSQL.Load()
	recordPublished(messageKindSQL)
	after := stats.publishedSQL.Load()

	rec := httptest.NewRecorder()
	m.handleMetrics(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE sqlproxy_replication_published_total counter",
		"# TYPE sqlproxy_replication_rejected_total counter",
		"# TYPE sqlproxy_upstream_pool_connections gauge",
		`sqlproxy_build_info{version="1.0.0"} 1`,
		`sqlproxy_replication_rejected_total{reason="signature"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output is missing %q\n%s", want, body)
		}
	}

	if after != before+1 {
		t.Fatalf("counter did not advance: before=%d after=%d", before, after)
	}
	line := `sqlproxy_replication_published_total{kind="sql"} ` + strconv.FormatUint(after, 10)
	if !strings.Contains(body, line) {
		t.Errorf("published counter not exported as expected (%q)\n%s", line, body)
	}
}

func TestHealthWithoutPools(t *testing.T) {
	m := &mysqlProxy{startedAt: time.Now()}

	rec := httptest.NewRecorder()
	m.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var report healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if report.Status != "ok" {
		t.Errorf("status = %q, want ok", report.Status)
	}
	if report.Version != Version {
		t.Errorf("version = %q, want %q", report.Version, Version)
	}
	if report.Pools[roleAdmin].Configured {
		t.Error("admin pool reported as configured without a pool")
	}
}

// TestHealthDegradedWhenUpstreamIsDown is the reason /healthz exists: a proxy whose
// MySQL is unreachable must say so instead of answering 200.
func TestHealthDegradedWhenUpstreamIsDown(t *testing.T) {
	oldProbe := probeTimeout
	probeTimeout = 200 * time.Millisecond
	defer func() { probeTimeout = oldProbe }()

	pool, err := client.NewPool(noopLog, 0, 1, 1, "127.0.0.1:1", "user", "pass", "db")
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	m := &mysqlProxy{startedAt: time.Now(), adminPool: pool}

	rec := httptest.NewRecorder()
	m.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	var report healthReport
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if report.Status != "degraded" {
		t.Errorf("status = %q, want degraded", report.Status)
	}
	if report.Pools[roleAdmin].Reachable {
		t.Error("unreachable pool reported as reachable")
	}
}

// TestObservabilityDisabledByDefault keeps the pre-1.0.0 configuration behaviour: with
// debug.enable false nothing listens.
func TestObservabilityDisabledByDefault(t *testing.T) {
	if got := config.Get().Debug.Enable; got {
		t.Skip("config defaults to debug enabled in this build")
	}

	stop, err := startObservability(&mysqlProxy{})
	if err != nil {
		t.Fatalf("startObservability: %v", err)
	}
	stop()
}
