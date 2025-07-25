package gateway

import (
	"context"
	"io"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	prometheus "github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Duration histograms measure things like API call execution, how long returning specific
// CID/path, how long CAR fetch form backend took, etc.
// We use fixed definition here, as we don't want to break existing buckets if we need to add more.
var defaultDurationHistogramBuckets = []float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 240, 480, 960, 1920}

type ipfsBackendWithMetrics struct {
	backend           IPFSBackend
	backendCallMetric *prometheus.HistogramVec
}

func newIPFSBackendWithMetrics(backend IPFSBackend) *ipfsBackendWithMetrics {
	// We can add buckets as a parameter in the future, but for now using static defaults
	// suggested in https://github.com/ipfs/kubo/issues/8441

	backendCallMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ipfs",
			Subsystem: "gw_backend",
			Name:      "api_call_duration_seconds",
			Help:      "The time spent in IPFSBackend API calls that returned success.",
			Buckets:   defaultDurationHistogramBuckets,
		},
		[]string{"name", "result"},
	)

	if err := prometheus.Register(backendCallMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			backendCallMetric = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			log.Errorf("failed to register ipfs_gw_backend_api_call_duration_seconds: %v", err)
		}
	}

	return &ipfsBackendWithMetrics{backend, backendCallMetric}
}

func (b *ipfsBackendWithMetrics) updateBackendCallMetric(name string, err error, begin time.Time) {
	end := time.Since(begin).Seconds()
	if err == nil {
		b.backendCallMetric.WithLabelValues(name, "success").Observe(end)
	} else {
		b.backendCallMetric.WithLabelValues(name, "failure").Observe(end)
	}
}

func (b *ipfsBackendWithMetrics) Get(ctx context.Context, path path.ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	begin := time.Now()
	name := "IPFSBackend.Get"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String()), attribute.Int("ranges", len(ranges))))
	defer span.End()

	md, f, err := b.backend.Get(ctx, path, ranges...)

	b.updateBackendCallMetric(name, err, begin)
	return md, f, err
}

func (b *ipfsBackendWithMetrics) GetAll(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	begin := time.Now()
	name := "IPFSBackend.GetAll"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.backend.GetAll(ctx, path)

	b.updateBackendCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) GetBlock(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	begin := time.Now()
	name := "IPFSBackend.GetBlock"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.backend.GetBlock(ctx, path)

	b.updateBackendCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) Head(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	begin := time.Now()
	name := "IPFSBackend.Head"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, n, err := b.backend.Head(ctx, path)

	b.updateBackendCallMetric(name, err, begin)
	return md, n, err
}

func (b *ipfsBackendWithMetrics) ResolvePath(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, error) {
	begin := time.Now()
	name := "IPFSBackend.ResolvePath"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, err := b.backend.ResolvePath(ctx, path)

	b.updateBackendCallMetric(name, err, begin)
	return md, err
}

func (b *ipfsBackendWithMetrics) GetCAR(ctx context.Context, path path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	begin := time.Now()
	name := "IPFSBackend.GetCAR"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	md, rc, err := b.backend.GetCAR(ctx, path, params)
	b.updateBackendCallMetric(name, err, begin)
	return md, rc, err
}

func (b *ipfsBackendWithMetrics) IsCached(ctx context.Context, path path.Path) bool {
	begin := time.Now()
	name := "IPFSBackend.IsCached"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	bln := b.backend.IsCached(ctx, path)

	b.updateBackendCallMetric(name, nil, begin)
	return bln
}

func (b *ipfsBackendWithMetrics) GetIPNSRecord(ctx context.Context, cid cid.Cid) ([]byte, error) {
	begin := time.Now()
	name := "IPFSBackend.GetIPNSRecord"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("cid", cid.String())))
	defer span.End()

	r, err := b.backend.GetIPNSRecord(ctx, cid)

	b.updateBackendCallMetric(name, err, begin)
	return r, err
}

func (b *ipfsBackendWithMetrics) ResolveMutable(ctx context.Context, path path.Path) (path.ImmutablePath, time.Duration, time.Time, error) {
	begin := time.Now()
	name := "IPFSBackend.ResolveMutable"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	p, ttl, lastMod, err := b.backend.ResolveMutable(ctx, path)

	b.updateBackendCallMetric(name, err, begin)
	return p, ttl, lastMod, err
}

func (b *ipfsBackendWithMetrics) GetDNSLinkRecord(ctx context.Context, fqdn string) (path.Path, error) {
	begin := time.Now()
	name := "IPFSBackend.GetDNSLinkRecord"
	ctx, span := spanTrace(ctx, name, trace.WithAttributes(attribute.String("fqdn", fqdn)))
	defer span.End()

	p, err := b.backend.GetDNSLinkRecord(ctx, fqdn)

	b.updateBackendCallMetric(name, err, begin)
	return p, err
}

var (
	_ IPFSBackend     = (*ipfsBackendWithMetrics)(nil)
	_ WithContextHint = (*ipfsBackendWithMetrics)(nil)
)

func (b *ipfsBackendWithMetrics) WrapContextForRequest(ctx context.Context) context.Context {
	if withCtxWrap, ok := b.backend.(WithContextHint); ok {
		return withCtxWrap.WrapContextForRequest(ctx)
	}
	return ctx
}

func newHandlerWithMetrics(c *Config, backend IPFSBackend) *handler {
	i := &handler{
		config:  c,
		backend: newIPFSBackendWithMetrics(backend),

		// Response-type specific metrics
		// ----------------------------
		requestTypeMetric: newRequestTypeMetric(
			"gw_request_types",
			"The number of requests per implicit or explicit request type.",
		),
		// Generic: time it takes to execute a successful gateway request (all request types)
		getMetric: newHistogramMetric(
			"gw_get_duration_seconds",
			"The time to GET a successful response to a request (all content types).",
		),
		// UnixFS: time it takes to return a file
		unixfsFileGetMetric: newHistogramMetric(
			"gw_unixfs_file_get_duration_seconds",
			"The time to serve an entire UnixFS file from the gateway.",
		),
		// UnixFS: time it takes to find and serve an index.html file on behalf of a directory.
		unixfsDirIndexGetMetric: newHistogramMetric(
			"gw_unixfs_dir_indexhtml_get_duration_seconds",
			"The time to serve an index.html file on behalf of a directory from the gateway. This is a subset of gw_unixfs_file_get_duration_seconds.",
		),
		// UnixFS: time it takes to generate static HTML with directory listing
		unixfsGenDirListingGetMetric: newHistogramMetric(
			"gw_unixfs_gen_dir_listing_get_duration_seconds",
			"The time to serve a generated UnixFS HTML directory listing from the gateway.",
		),
		// CAR: time it takes to return requested CAR stream
		carStreamGetMetric: newHistogramMetric(
			"gw_car_stream_get_duration_seconds",
			"The time to GET an entire CAR stream from the gateway.",
		),
		carStreamFailMetric: newHistogramMetric(
			"gw_car_stream_fail_duration_seconds",
			"How long a CAR was streamed before failing mid-stream.",
		),
		// Block: time it takes to return requested Block
		rawBlockGetMetric: newHistogramMetric(
			"gw_raw_block_get_duration_seconds",
			"The time to GET an entire raw Block from the gateway.",
		),
		// TAR: time it takes to return requested TAR stream
		tarStreamGetMetric: newHistogramMetric(
			"gw_tar_stream_get_duration_seconds",
			"The time to GET an entire TAR stream from the gateway.",
		),
		// TAR: time it takes to return requested TAR stream
		tarStreamFailMetric: newHistogramMetric(
			"gw_tar_stream_fail_duration_seconds",
			"How long a TAR was streamed before failing mid-stream.",
		),
		// JSON/CBOR: time it takes to return requested DAG-JSON/-CBOR document
		jsoncborDocumentGetMetric: newHistogramMetric(
			"gw_jsoncbor_get_duration_seconds",
			"The time to GET an entire DAG-JSON/CBOR block from the gateway.",
		),
		// IPNS Record: time it takes to return IPNS record
		ipnsRecordGetMetric: newHistogramMetric(
			"gw_ipns_record_get_duration_seconds",
			"The time to GET an entire IPNS Record from the gateway.",
		),
	}
	return i
}

func newRequestTypeMetric(name string, help string) *prometheus.CounterVec {
	// We can add buckets as a parameter in the future, but for now using static defaults
	// suggested in https://github.com/ipfs/kubo/issues/8441
	metric := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ipfs",
			Subsystem: "http",
			Name:      name,
			Help:      help,
		},
		[]string{"gateway", "type"},
	)
	if err := prometheus.Register(metric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			metric = are.ExistingCollector.(*prometheus.CounterVec)
		} else {
			log.Errorf("failed to register ipfs_http_%s: %v", name, err)
		}
	}
	return metric
}

func newHistogramMetric(name string, help string) *prometheus.HistogramVec {
	// We can add buckets as a parameter in the future, but for now using static defaults
	// suggested in https://github.com/ipfs/kubo/issues/8441
	histogramMetric := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ipfs",
			Subsystem: "http",
			Name:      name,
			Help:      help,
			Buckets:   defaultDurationHistogramBuckets,
		},
		[]string{"gateway"},
	)
	if err := prometheus.Register(histogramMetric); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			histogramMetric = are.ExistingCollector.(*prometheus.HistogramVec)
		} else {
			log.Errorf("failed to register ipfs_http_%s: %v", name, err)
		}
	}
	return histogramMetric
}

var tracer = otel.Tracer("boxo/gateway")

func spanTrace(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, "Gateway."+spanName, opts...)
}

// registerMetric registers metrics in registry or logs an error.
//
// Registration may error if metric is alreadyregistered. we are not using
// MustRegister here to allow people to run tests in parallel without having to
// write tedious  glue code that creates unique registry for each unit test
func registerMetric(registry prometheus.Registerer, metric prometheus.Collector) {
	if err := registry.Register(metric); err != nil {
		log.Errorf("failed to register %v: %v", metric, err)
	}
}
