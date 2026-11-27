package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/config"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLProxy/pkg/mysql/client"
	"github.com/sirupsen/logrus"
)

// defaultDebugPort is used when debug.enable is set without a usable port.
const defaultDebugPort = 17878

// probeTimeout bounds the upstream checks a health probe performs. It is a variable so
// tests can shorten it.
var probeTimeout = 2 * time.Second

// startObservability serves /healthz and /metrics on 127.0.0.1:<debug.port> when
// debug.enable is set. The listener is loopback-only on purpose: these endpoints expose
// operational detail, not a public API. The returned function stops the server.
func startObservability(m *mysqlProxy) (func(), error) {
	cfg := config.Get().Debug
	if !cfg.Enable {
		return func() {}, nil
	}

	port := cfg.Port
	if port <= 0 || port > 65535 {
		port = defaultDebugPort
	}
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("debug listener on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", m.handleHealth)
	mux.HandleFunc("/metrics", m.handleMetrics)

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("debug server stopped: %v", err)
		}
	}()
	logrus.Infof("debug endpoints on http://%s/healthz and http://%s/metrics", addr, addr)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}, nil
}

// healthReport is the /healthz payload.
type healthReport struct {
	Status        string                `json:"status"`
	Version       string                `json:"version"`
	UptimeSeconds int64                 `json:"uptimeSeconds"`
	P2P           p2pHealth             `json:"p2p"`
	Pools         map[string]poolHealth `json:"pools"`
}

type p2pHealth struct {
	Enabled bool     `json:"enabled"`
	Peers   int      `json:"peers"`
	PeerID  string   `json:"peerId,omitempty"`
	Addrs   []string `json:"addrs,omitempty"`
}

type poolHealth struct {
	Configured bool   `json:"configured"`
	Reachable  bool   `json:"reachable"`
	Total      int    `json:"total"`
	Idle       int    `json:"idle"`
	Error      string `json:"error,omitempty"`
}

func (m *mysqlProxy) handleHealth(w http.ResponseWriter, r *http.Request) {
	report := healthReport{
		Status:        "ok",
		Version:       Version,
		UptimeSeconds: int64(time.Since(m.startedAt).Seconds()),
		Pools: map[string]poolHealth{
			roleAdmin:    m.probePool(m.adminPool),
			roleReadonly: m.probePool(m.readonlyPool),
		},
	}

	if p2pChans != nil && p2pChans.adminHost != nil && p2pChans.adminHost.Host != nil {
		report.P2P.Enabled = true
		report.P2P.Peers = len(p2pChans.adminHost.Host.Network().Peers())
		report.P2P.PeerID = p2pChans.adminHost.PeerID()
		report.P2P.Addrs = p2pChans.adminHost.Addrs()
	}

	for _, pool := range report.Pools {
		if pool.Configured && !pool.Reachable {
			report.Status = "degraded"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if report.Status != "ok" {
		status = http.StatusServiceUnavailable
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(report)
}

// probePool reports pool statistics and whether an upstream connection is usable right
// now.
func (m *mysqlProxy) probePool(pool *client.Pool) poolHealth {
	if pool == nil {
		return poolHealth{Configured: false}
	}

	var stats client.ConnectionStats
	pool.GetStats(&stats)
	health := poolHealth{Configured: true, Total: stats.TotalCount, Idle: stats.IdleCount}

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	conn, err := pool.GetConn(ctx)
	if err != nil {
		health.Error = err.Error()
		return health
	}
	if err := conn.Ping(); err != nil {
		health.Error = err.Error()
		pool.DropConn(conn)
		return health
	}
	health.Reachable = true
	pool.PutConn(conn)
	return health
}

// handleMetrics renders the Prometheus text exposition format by hand: the counters are
// already atomic values, and a client library would be a large dependency for a dozen
// lines of output.
func (m *mysqlProxy) handleMetrics(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder

	writeCounter(&b, "sqlproxy_replication_published_total", "Replication messages queued for publishing.",
		sample("kind", messageKindSQL, stats.publishedSQL.Load()),
		sample("kind", messageKindTxn, stats.publishedTxn.Load()))
	writeCounter(&b, "sqlproxy_replication_publish_dropped_total", "Replication messages dropped because the publish queue was full.",
		sample("kind", "all", stats.publishDropped.Load()))
	writeCounter(&b, "sqlproxy_replication_publish_failed_total", "Replication messages that could not be encoded.",
		sample("kind", "all", stats.publishFailed.Load()))
	writeCounter(&b, "sqlproxy_replication_applied_total", "Inbound replication messages applied locally.",
		sample("kind", messageKindSQL, stats.appliedSQL.Load()),
		sample("kind", messageKindTxn, stats.appliedTxn.Load()))
	writeCounter(&b, "sqlproxy_replication_apply_failed_total", "Inbound replication messages that failed to apply.",
		sample("kind", "all", stats.applyFailed.Load()))

	b.WriteString("# HELP sqlproxy_replication_rejected_total Inbound messages or statements rejected before execution.\n")
	b.WriteString("# TYPE sqlproxy_replication_rejected_total counter\n")
	for _, entry := range []struct {
		reason string
		value  uint64
	}{
		{rejectReadonly, stats.rejectedReadonly.Load()},
		{rejectMalformed, stats.rejectedMalformed.Load()},
		{rejectSignature, stats.rejectedSignature.Load()},
		{rejectPeer, stats.rejectedPeer.Load()},
		{rejectOversized, stats.rejectedOversized.Load()},
		{rejectIllegal, stats.rejectedIllegal.Load()},
		{rejectDuplicate, stats.rejectedDuplicate.Load()},
		{rejectReplicationRole, stats.rejectedRole.Load()},
	} {
		fmt.Fprintf(&b, "sqlproxy_replication_rejected_total{reason=%q} %d\n", entry.reason, entry.value)
	}

	b.WriteString("# HELP sqlproxy_upstream_pool_connections Upstream MySQL connections by pool and state.\n")
	b.WriteString("# TYPE sqlproxy_upstream_pool_connections gauge\n")
	for _, pool := range []struct {
		name string
		pool *client.Pool
	}{{roleAdmin, m.adminPool}, {roleReadonly, m.readonlyPool}} {
		if pool.pool == nil {
			continue
		}
		var s client.ConnectionStats
		pool.pool.GetStats(&s)
		fmt.Fprintf(&b, "sqlproxy_upstream_pool_connections{pool=%q,state=\"total\"} %d\n", pool.name, s.TotalCount)
		fmt.Fprintf(&b, "sqlproxy_upstream_pool_connections{pool=%q,state=\"idle\"} %d\n", pool.name, s.IdleCount)
	}

	peers := 0
	if p2pChans != nil && p2pChans.adminHost != nil && p2pChans.adminHost.Host != nil {
		peers = len(p2pChans.adminHost.Host.Network().Peers())
	}
	b.WriteString("# HELP sqlproxy_p2p_peers Connected p2p peers.\n")
	b.WriteString("# TYPE sqlproxy_p2p_peers gauge\n")
	fmt.Fprintf(&b, "sqlproxy_p2p_peers %d\n", peers)

	b.WriteString("# HELP sqlproxy_build_info Build metadata.\n")
	b.WriteString("# TYPE sqlproxy_build_info gauge\n")
	fmt.Fprintf(&b, "sqlproxy_build_info{version=%q} 1\n", Version)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

// metricSample is one labelled value of a counter family.
type metricSample struct {
	label string
	name  string
	value uint64
}

func sample(label, name string, value uint64) metricSample {
	return metricSample{label: label, name: name, value: value}
}

// writeCounter writes one counter family with its samples.
func writeCounter(b *strings.Builder, name, help string, samples ...metricSample) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	for _, s := range samples {
		fmt.Fprintf(b, "%s{%s=%q} %d\n", name, s.label, s.name, s.value)
	}
}
