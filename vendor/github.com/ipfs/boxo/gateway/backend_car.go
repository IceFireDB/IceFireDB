package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	ufsData "github.com/ipfs/go-unixfsnode/data"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/multiformats/go-multicodec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

var ErrFetcherUnexpectedEOF = fmt.Errorf("failed to fetch IPLD data")

type CarBackend struct {
	baseBackend
	fetcher         CarFetcher
	pc              traversal.LinkTargetNodePrototypeChooser
	metrics         *CarBackendMetrics
	getBlockTimeout time.Duration
}

type CarBackendMetrics struct {
	contextAlreadyCancelledMetric prometheus.Counter
	carFetchAttemptMetric         prometheus.Counter
	carBlocksFetchedMetric        prometheus.Counter
	carParamsMetric               *prometheus.CounterVec

	bytesRangeStartMetric prometheus.Histogram
	bytesRangeSizeMetric  prometheus.Histogram
}

// NewCarBackend returns an [IPFSBackend] backed by a [CarFetcher].
func NewCarBackend(f CarFetcher, opts ...BackendOption) (*CarBackend, error) {
	compiledOptions := backendOptions{
		getBlockTimeout: DefaultGetBlockTimeout,
	}
	for _, o := range opts {
		if err := o(&compiledOptions); err != nil {
			return nil, err
		}
	}

	// Setup the [baseBackend] which takes care of some shared functionality, such
	// as resolving /ipns links.
	baseBackend, err := newBaseBackend(compiledOptions.vs, compiledOptions.ns)
	if err != nil {
		return nil, err
	}

	var promReg prometheus.Registerer = prometheus.DefaultRegisterer
	if compiledOptions.promRegistry != nil {
		promReg = compiledOptions.promRegistry
	}

	return &CarBackend{
		baseBackend:     baseBackend,
		fetcher:         f,
		metrics:         registerCarBackendMetrics(promReg),
		getBlockTimeout: compiledOptions.getBlockTimeout,
		pc: dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
			if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
				return tlnkNd.LinkTargetNodePrototype(), nil
			}
			return basicnode.Prototype.Any, nil
		}),
	}, nil
}

// NewRemoteCarBackend creates a new [CarBackend] instance backed by one or more
// gateways. These gateways must support partial CAR requests, as described in
// [IPIP-402], as well as IPNS Record requests. See [NewRemoteCarFetcher] and
// [NewRemoteValueStore] for more details.
//
// If you want to create a more custom [CarBackend] with only remote IPNS Record
// resolution, or only remote CAR fetching, we recommend using [NewCarBackend]
// directly.
//
// [IPIP-402]: https://specs.ipfs.tech/ipips/ipip-0402/
func NewRemoteCarBackend(gatewayURL []string, httpClient *http.Client, opts ...BackendOption) (*CarBackend, error) {
	carFetcher, err := NewRemoteCarFetcher(gatewayURL, httpClient)
	if err != nil {
		return nil, err
	}

	valueStore, err := NewRemoteValueStore(gatewayURL, httpClient)
	if err != nil {
		return nil, err
	}

	return NewCarBackend(carFetcher, append(opts, WithValueStore(valueStore))...)
}

func registerCarBackendMetrics(promReg prometheus.Registerer) *CarBackendMetrics {
	// make sure we have functional registry
	if promReg == nil {
		promReg = prometheus.DefaultRegisterer
	}

	// How many CAR Fetch attempts we had? Need this to calculate % of various car request types.
	// We only count attempts here, because success/failure with/without retries are provided by caboose:
	// - ipfs_caboose_fetch_duration_car_success_count
	// - ipfs_caboose_fetch_duration_car_failure_count
	// - ipfs_caboose_fetch_duration_car_peer_success_count
	// - ipfs_caboose_fetch_duration_car_peer_failure_count
	carFetchAttemptMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "car_fetch_attempts",
		Help:      "The number of times a CAR fetch was attempted by IPFSBackend.",
	})
	registerMetric(promReg, carFetchAttemptMetric)

	contextAlreadyCancelledMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "car_fetch_context_already_cancelled",
		Help:      "The number of times context is already cancelled when a CAR fetch was attempted by IPFSBackend.",
	})
	registerMetric(promReg, contextAlreadyCancelledMetric)

	// How many blocks were read via CARs?
	// Need this as a baseline to reason about error ratio vs raw_block_recovery_attempts.
	carBlocksFetchedMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "car_blocks_fetched",
		Help:      "The number of blocks successfully read via CAR fetch.",
	})
	registerMetric(promReg, carBlocksFetchedMetric)

	carParamsMetric := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "car_fetch_params",
		Help:      "How many times specific CAR parameter was used during CAR data fetch.",
	}, []string{"dagScope", "entityRanges"}) // we use 'ranges' instead of 'bytes' here because we only count the number of ranges present
	registerMetric(promReg, carParamsMetric)

	bytesRangeStartMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "range_request_start",
		Help:      "Tracks where did the range request start.",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 24), // 1024 bytes to 8 GiB
	})
	registerMetric(promReg, bytesRangeStartMetric)

	bytesRangeSizeMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "ipfs",
		Subsystem: "gw_car_backend",
		Name:      "range_request_size",
		Help:      "Tracks the size of range requests.",
		Buckets:   prometheus.ExponentialBuckets(256*1024, 2, 10), // From 256KiB to 100MiB
	})
	registerMetric(promReg, bytesRangeSizeMetric)

	return &CarBackendMetrics{
		contextAlreadyCancelledMetric,
		carFetchAttemptMetric,
		carBlocksFetchedMetric,
		carParamsMetric,
		bytesRangeStartMetric,
		bytesRangeSizeMetric,
	}
}

func (api *CarBackend) fetchCAR(ctx context.Context, p path.ImmutablePath, params CarParams, cb DataCallback) error {
	api.metrics.carFetchAttemptMetric.Inc()
	var ipldError error
	fetchErr := api.fetcher.Fetch(ctx, p, params, func(p path.ImmutablePath, reader io.Reader) error {
		return checkRetryableError(&ipldError, func() error {
			return cb(p, reader)
		})
	})

	if ipldError != nil {
		fetchErr = ipldError
	} else if fetchErr != nil {
		fetchErr = blockstoreErrToGatewayErr(fetchErr)
	}

	return fetchErr
}

// resolvePathWithRootsAndBlock takes a path and linksystem and returns the set of non-terminal cids, the terminal cid, the remainder, and the block corresponding to the terminal cid
func resolvePathWithRootsAndBlock(ctx context.Context, p path.ImmutablePath, unixFSLsys *ipld.LinkSystem) (ContentPathMetadata, blocks.Block, error) {
	md, terminalBlk, err := resolvePathToLastWithRoots(ctx, p, unixFSLsys)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	terminalCid := md.LastSegment.RootCid()

	if terminalBlk == nil {
		lctx := ipld.LinkContext{Ctx: ctx}
		lnk := cidlink.Link{Cid: terminalCid}
		blockData, err := unixFSLsys.LoadRaw(lctx, lnk)
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}
		terminalBlk, err = blocks.NewBlockWithCid(blockData, terminalCid)
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}
	}

	return md, terminalBlk, err
}

// resolvePathToLastWithRoots takes a path and linksystem and returns the set of non-terminal cids, the terminal cid,
// the remainder pathing, the last block loaded, and the last node loaded.
//
// Note: the block returned will be nil if the terminal element is a link or the path is just a CID
func resolvePathToLastWithRoots(ctx context.Context, p path.ImmutablePath, unixFSLsys *ipld.LinkSystem) (ContentPathMetadata, blocks.Block, error) {
	root, segments := p.RootCid(), p.Segments()[2:]
	if len(segments) == 0 {
		return ContentPathMetadata{
			PathSegmentRoots: []cid.Cid{},
			LastSegment:      p,
		}, nil, nil
	}

	unixFSLsys.NodeReifier = unixfsnode.Reify
	defer func() { unixFSLsys.NodeReifier = nil }()

	var cids []cid.Cid
	cids = append(cids, root)

	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	loadNode := func(ctx context.Context, c cid.Cid) (blocks.Block, ipld.Node, error) {
		lctx := ipld.LinkContext{Ctx: ctx}
		rootLnk := cidlink.Link{Cid: c}
		np, err := pc(rootLnk, lctx)
		if err != nil {
			return nil, nil, err
		}
		nd, blockData, err := unixFSLsys.LoadPlusRaw(lctx, rootLnk, np)
		if err != nil {
			return nil, nil, err
		}
		blk, err := blocks.NewBlockWithCid(blockData, c)
		if err != nil {
			return nil, nil, err
		}
		return blk, nd, nil
	}

	nextBlk, nextNd, err := loadNode(ctx, root)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	depth := 0
	for i, elem := range segments {
		nextNd, err = nextNd.LookupBySegment(ipld.ParsePathSegment(elem))
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}
		if nextNd.Kind() == ipld.Kind_Link {
			depth = 0
			lnk, err := nextNd.AsLink()
			if err != nil {
				return ContentPathMetadata{}, nil, err
			}
			cidLnk, ok := lnk.(cidlink.Link)
			if !ok {
				return ContentPathMetadata{}, nil, fmt.Errorf("link is not a cidlink: %v", cidLnk)
			}
			cids = append(cids, cidLnk.Cid)

			if i < len(segments)-1 {
				nextBlk, nextNd, err = loadNode(ctx, cidLnk.Cid)
				if err != nil {
					return ContentPathMetadata{}, nil, err
				}
			}
		} else {
			depth++
		}
	}

	// if last node is not a link, just return it's cid, add path to remainder and return
	if nextNd.Kind() != ipld.Kind_Link {
		md, err := contentMetadataFromRootsAndRemainder(cids, segments[len(segments)-depth:])
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}

		// return the cid and the remainder of the path
		return md, nextBlk, nil
	}

	md, err := contentMetadataFromRootsAndRemainder(cids, nil)
	return md, nil, err
}

func contentMetadataFromRootsAndRemainder(roots []cid.Cid, remainder []string) (ContentPathMetadata, error) {
	if len(roots) == 0 {
		return ContentPathMetadata{}, errors.New("invalid pathRoots given with length 0")
	}

	p, err := path.Join(path.FromCid(roots[len(roots)-1]), remainder...)
	if err != nil {
		return ContentPathMetadata{}, err
	}

	imPath, err := path.NewImmutablePath(p)
	if err != nil {
		return ContentPathMetadata{}, err
	}

	md := ContentPathMetadata{
		PathSegmentRoots:     roots[:len(roots)-1],
		LastSegmentRemainder: remainder,
		LastSegment:          imPath,
	}
	return md, nil
}

var errNotUnixFS = fmt.Errorf("data was not unixfs")

func (api *CarBackend) Get(ctx context.Context, path path.ImmutablePath, byteRanges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	rangeCount := len(byteRanges)
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": strconv.Itoa(rangeCount)}).Inc()

	carParams := CarParams{Scope: DagScopeEntity}

	// fetch CAR with &bytes= to get minimal set of blocks for the request
	// Note: majority of requests have 0 or max 1 ranges. if there are more ranges than one,
	// that is a niche edge cache we don't prefetch as CAR and use fallback blockstore instead.
	if rangeCount > 0 {
		r := byteRanges[0]
		carParams.Range = &DagByteRange{
			From: int64(r.From),
		}

		// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
		api.metrics.bytesRangeStartMetric.Observe(float64(r.From))

		if r.To != nil {
			carParams.Range.To = r.To

			// TODO: move to boxo or to loadRequestIntoSharedBlockstoreAndBlocksGateway after we pass params in a humane way
			api.metrics.bytesRangeSizeMetric.Observe(float64(*r.To) - float64(r.From) + 1)
		}
	}

	md, terminalElem, err := fetchWithPartialRetries(ctx, path, carParams, loadTerminalEntity, api.metrics, api.fetchCAR, api.getBlockTimeout)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	var resp *GetResponse

	switch typedTerminalElem := terminalElem.(type) {
	case *GetResponse:
		resp = typedTerminalElem
	case *backpressuredFile:
		resp = NewGetResponseFromReader(typedTerminalElem, typedTerminalElem.size)
	case *backpressuredHAMTDirIterNoRecursion:
		ch := make(chan unixfs.LinkResult)
		go func() {
			defer close(ch)
			for typedTerminalElem.Next() {
				l := typedTerminalElem.Link()
				select {
				case ch <- l:
				case <-ctx.Done():
					return
				}
			}
			if err := typedTerminalElem.Err(); err != nil {
				select {
				case ch <- unixfs.LinkResult{Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
		resp = NewGetResponseFromDirectoryListing(typedTerminalElem.dagSize, ch, nil)
	default:
		return ContentPathMetadata{}, nil, fmt.Errorf("invalid data type")
	}

	return md, resp, nil
}

// loadTerminalEntity returns either a [*GetResponse], [*backpressuredFile], or [*backpressuredHAMTDirIterNoRecursion]
func loadTerminalEntity(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem, params CarParams, getLsys lsysGetter) (interface{}, error) {
	var err error
	if lsys == nil {
		lsys, err = getLsys(ctx, c, params)
		if err != nil {
			return nil, err
		}
	}

	lctx := ipld.LinkContext{Ctx: ctx}

	if c.Type() != uint64(multicodec.DagPb) {
		var blockData []byte

		if blk != nil {
			blockData = blk.RawData()
		} else {
			blockData, err = lsys.LoadRaw(lctx, cidlink.Link{Cid: c})
			if err != nil {
				return nil, err
			}
		}

		f := files.NewBytesFile(blockData)
		if params.Range != nil && params.Range.From != 0 {
			if _, err := f.Seek(params.Range.From, io.SeekStart); err != nil {
				return nil, err
			}
		}

		return NewGetResponseFromReader(f, int64(len(blockData))), nil
	}

	blockData, pbn, ufsFieldData, fieldNum, err := loadUnixFSBase(ctx, c, blk, lsys)
	if err != nil {
		return nil, err
	}

	switch fieldNum {
	case ufsData.Data_Symlink:
		if !ufsFieldData.FieldData().Exists() {
			return nil, fmt.Errorf("invalid UnixFS symlink object")
		}
		lnkTarget := string(ufsFieldData.FieldData().Must().Bytes())
		f := NewGetResponseFromSymlink(files.NewLinkFile(lnkTarget, nil).(*files.Symlink), int64(len(lnkTarget)))
		return f, nil
	case ufsData.Data_Metadata:
		return nil, fmt.Errorf("UnixFS Metadata unsupported")
	case ufsData.Data_HAMTShard, ufsData.Data_Directory:
		blk, err := blocks.NewBlockWithCid(blockData, c)
		if err != nil {
			return nil, fmt.Errorf("could not create block: %w", err)
		}
		dirRootNd, err := merkledag.ProtoNodeConverter(blk, pbn)
		if err != nil {
			return nil, fmt.Errorf("could not create dag-pb universal block from UnixFS directory root: %w", err)
		}
		pn, ok := dirRootNd.(*merkledag.ProtoNode)
		if !ok {
			return nil, fmt.Errorf("could not create dag-pb node from UnixFS directory root: %w", err)
		}

		dirDagSize, err := pn.Size()
		if err != nil {
			return nil, fmt.Errorf("could not get cumulative size from dag-pb node: %w", err)
		}

		switch fieldNum {
		case ufsData.Data_Directory:
			ch := make(chan unixfs.LinkResult, pbn.Links.Length())
			defer close(ch)
			iter := pbn.Links.Iterator()
			for !iter.Done() {
				_, v := iter.Next()
				c := v.Hash.Link().(cidlink.Link).Cid
				var name string
				var size int64
				if v.Name.Exists() {
					name = v.Name.Must().String()
				}
				if v.Tsize.Exists() {
					size = v.Tsize.Must().Int()
				}
				lnk := unixfs.LinkResult{Link: &format.Link{
					Name: name,
					Size: uint64(size),
					Cid:  c,
				}}
				ch <- lnk
			}
			return NewGetResponseFromDirectoryListing(dirDagSize, ch, nil), nil
		case ufsData.Data_HAMTShard:
			dirNd, err := unixfsnode.Reify(lctx, pbn, lsys)
			if err != nil {
				return nil, fmt.Errorf("could not reify sharded directory: %w", err)
			}

			d := &backpressuredHAMTDirIterNoRecursion{
				dagSize:   dirDagSize,
				linksItr:  dirNd.MapIterator(),
				dirCid:    c,
				lsys:      lsys,
				getLsys:   getLsys,
				ctx:       ctx,
				closed:    make(chan error),
				hasClosed: false,
			}
			return d, nil
		default:
			return nil, fmt.Errorf("not a basic or HAMT directory: should be unreachable")
		}
	case ufsData.Data_Raw, ufsData.Data_File:
		nd, err := unixfsnode.Reify(lctx, pbn, lsys)
		if err != nil {
			return nil, err
		}

		fnd, ok := nd.(datamodel.LargeBytesNode)
		if !ok {
			return nil, fmt.Errorf("could not process file since it did not present as large bytes")
		}
		f, err := fnd.AsLargeBytes()
		if err != nil {
			return nil, err
		}

		fileSize, err := f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("unable to get UnixFS file size: %w", err)
		}

		from := int64(0)
		var byteRange DagByteRange
		if params.Range != nil {
			from = params.Range.From
			byteRange = *params.Range
		}
		_, err = f.Seek(from, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("unable to get reset UnixFS file reader: %w", err)
		}

		return &backpressuredFile{ctx: ctx, fileCid: c, byteRange: byteRange, size: fileSize, f: f, getLsys: getLsys, closed: make(chan error)}, nil
	default:
		return nil, fmt.Errorf("unknown UnixFS field type")
	}
}

func (api *CarBackend) GetAll(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "all", "entityRanges": "0"}).Inc()
	return fetchWithPartialRetries(ctx, path, CarParams{Scope: DagScopeAll}, loadTerminalUnixFSElementWithRecursiveDirectories, api.metrics, api.fetchCAR, api.getBlockTimeout)
}

type (
	loadTerminalElement[T any] func(ctx context.Context, c cid.Cid, blk blocks.Block, lsys *ipld.LinkSystem, params CarParams, getLsys lsysGetter) (T, error)
	fetchCarFn                 = func(ctx context.Context, path path.ImmutablePath, params CarParams, cb DataCallback) error
)

type terminalPathType[T any] struct {
	resp T
	err  error
	md   ContentPathMetadata
}

type nextReq struct {
	c      cid.Cid
	params CarParams
}

func fetchWithPartialRetries[T any](ctx context.Context, p path.ImmutablePath, initialParams CarParams, resolveTerminalElementFn loadTerminalElement[T], metrics *CarBackendMetrics, fetchCAR fetchCarFn, timeout time.Duration) (ContentPathMetadata, T, error) {
	var zeroReturnType T

	terminalPathElementCh := make(chan terminalPathType[T], 1)

	go func() {
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()

		hasSentAsyncData := false
		var closeCh <-chan error

		sendRequest := make(chan nextReq, 1)
		sendResponse := make(chan *ipld.LinkSystem, 1)
		getLsys := func(ctx context.Context, c cid.Cid, params CarParams) (*ipld.LinkSystem, error) {
			select {
			case sendRequest <- nextReq{c: c, params: params}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			select {
			case lsys := <-sendResponse:
				return lsys, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		params := initialParams

		err := fetchCAR(cctx, p, params, func(_ path.ImmutablePath, reader io.Reader) error {
			gb, err := carToLinearBlockGetter(cctx, reader, timeout, metrics)
			if err != nil {
				return err
			}

			lsys := getCarLinksystem(gb)

			if hasSentAsyncData {
				_, _, err = resolvePathToLastWithRoots(cctx, p, lsys)
				if err != nil {
					return err
				}

				select {
				case sendResponse <- lsys:
				case <-cctx.Done():
					return cctx.Err()
				}
			} else {
				// First resolve the path since we always need to.
				md, terminalBlk, err := resolvePathWithRootsAndBlock(cctx, p, lsys)
				if err != nil {
					return err
				}

				if len(md.LastSegmentRemainder) > 0 {
					terminalPathElementCh <- terminalPathType[T]{err: errNotUnixFS}
					return nil
				}

				if hasSentAsyncData {
					select {
					case sendResponse <- lsys:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				terminalCid := md.LastSegment.RootCid()

				nd, err := resolveTerminalElementFn(cctx, terminalCid, terminalBlk, lsys, params, getLsys)
				if err != nil {
					return err
				}

				ndAc, ok := any(nd).(awaitCloser)
				if !ok {
					terminalPathElementCh <- terminalPathType[T]{
						resp: nd,
						md:   md,
					}
					return nil
				}

				hasSentAsyncData = true
				terminalPathElementCh <- terminalPathType[T]{
					resp: nd,
					md:   md,
				}

				closeCh = ndAc.AwaitClose()
			}

			select {
			case closeErr := <-closeCh:
				return closeErr
			case req := <-sendRequest:
				// set path and params for next iteration
				p = path.FromCid(req.c)
				if err != nil {
					return err
				}
				params = req.params
				return ErrPartialResponse{StillNeed: []CarResource{{Path: p, Params: params}}}
			case <-cctx.Done():
				return cctx.Err()
			}
		})

		if !hasSentAsyncData && err != nil {
			terminalPathElementCh <- terminalPathType[T]{err: err}
			return
		}

		if err != nil {
			lsys := getCarLinksystem(func(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
				return nil, multierr.Append(ErrFetcherUnexpectedEOF, format.ErrNotFound{Cid: cid})
			})
			for {
				select {
				case <-closeCh:
					return
				case <-sendRequest:
				case sendResponse <- lsys:
				case <-cctx.Done():
					return
				}
			}
		}
	}()

	select {
	case t := <-terminalPathElementCh:
		if t.err != nil {
			return ContentPathMetadata{}, zeroReturnType, t.err
		}
		return t.md, t.resp, nil
	case <-ctx.Done():
		return ContentPathMetadata{}, zeroReturnType, ctx.Err()
	}
}

func (api *CarBackend) GetBlock(ctx context.Context, p path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()

	var md ContentPathMetadata
	var f files.File
	// TODO: if path is `/ipfs/cid`, we should use ?format=raw
	err := api.fetchCAR(ctx, p, CarParams{Scope: DagScopeBlock}, func(_ path.ImmutablePath, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.getBlockTimeout, api.metrics)
		if err != nil {
			return err
		}
		lsys := getCarLinksystem(gb)

		// First resolve the path since we always need to.
		var terminalBlk blocks.Block
		md, terminalBlk, err = resolvePathToLastWithRoots(ctx, p, lsys)
		if err != nil {
			return err
		}

		var blockData []byte
		if terminalBlk != nil {
			blockData = terminalBlk.RawData()
		} else {
			lctx := ipld.LinkContext{Ctx: ctx}
			lnk := cidlink.Link{Cid: md.LastSegment.RootCid()}
			blockData, err = lsys.LoadRaw(lctx, lnk)
			if err != nil {
				return err
			}
		}

		f = files.NewBytesFile(blockData)
		return nil
	})
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	return md, f, nil
}

func (api *CarBackend) Head(ctx context.Context, p path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "entity", "entityRanges": "1"}).Inc()

	// TODO:  we probably want to move this either to boxo, or at least to loadRequestIntoSharedBlockstoreAndBlocksGateway
	api.metrics.bytesRangeStartMetric.Observe(0)
	api.metrics.bytesRangeSizeMetric.Observe(3071)

	var md ContentPathMetadata
	var n *HeadResponse
	// TODO: fallback to dynamic fetches in case we haven't requested enough data
	rangeTo := int64(3071)
	err := api.fetchCAR(ctx, p, CarParams{Scope: DagScopeEntity, Range: &DagByteRange{From: 0, To: &rangeTo}}, func(_ path.ImmutablePath, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.getBlockTimeout, api.metrics)
		if err != nil {
			return err
		}
		lsys := getCarLinksystem(gb)

		// First resolve the path since we always need to.
		var terminalBlk blocks.Block
		md, terminalBlk, err = resolvePathWithRootsAndBlock(ctx, p, lsys)
		if err != nil {
			return err
		}

		terminalCid := md.LastSegment.RootCid()
		lctx := ipld.LinkContext{Ctx: ctx}
		pathTerminalCidLink := cidlink.Link{Cid: terminalCid}

		// Load the block at the root of the terminal path element
		dataBytes := terminalBlk.RawData()

		// It's not UnixFS if there is a remainder or it's not dag-pb
		if len(md.LastSegmentRemainder) > 0 || terminalCid.Type() != uint64(multicodec.DagPb) {
			n = NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
			return nil
		}

		// Let's figure out if the terminal element is valid UnixFS and if so what kind
		np, err := api.pc(pathTerminalCidLink, lctx)
		if err != nil {
			return err
		}

		nodeDecoder, err := lsys.DecoderChooser(pathTerminalCidLink)
		if err != nil {
			return err
		}

		nb := np.NewBuilder()
		err = nodeDecoder(nb, bytes.NewReader(dataBytes))
		if err != nil {
			return err
		}
		lastCidNode := nb.Build()

		if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
			// This shouldn't be possible since we already checked for dag-pb usage
			return fmt.Errorf("node was not go-codec-dagpb node")
		} else if !pbn.FieldData().Exists() {
			// If it's not valid UnixFS then just return the block bytes
			n = NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
			return nil
		} else if unixfsFieldData, decodeErr := ufsData.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
			// If it's not valid UnixFS then just return the block bytes
			n = NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
			return nil
		} else {
			switch fieldNum := unixfsFieldData.FieldDataType().Int(); fieldNum {
			case ufsData.Data_Directory, ufsData.Data_HAMTShard:
				dirRootNd, err := merkledag.ProtoNodeConverter(terminalBlk, lastCidNode)
				if err != nil {
					return fmt.Errorf("could not create dag-pb universal block from UnixFS directory root: %w", err)
				}
				pn, ok := dirRootNd.(*merkledag.ProtoNode)
				if !ok {
					return fmt.Errorf("could not create dag-pb node from UnixFS directory root: %w", err)
				}

				sz, err := pn.Size()
				if err != nil {
					return fmt.Errorf("could not get cumulative size from dag-pb node: %w", err)
				}

				n = NewHeadResponseForDirectory(int64(sz))
				return nil
			case ufsData.Data_Symlink:
				fd := unixfsFieldData.FieldData()
				if fd.Exists() {
					n = NewHeadResponseForSymlink(int64(len(fd.Must().Bytes())))
					return nil
				}
				// If there is no target then it's invalid so just return the block
				NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
				return nil
			case ufsData.Data_Metadata:
				n = NewHeadResponseForFile(files.NewBytesFile(dataBytes), int64(len(dataBytes)))
				return nil
			case ufsData.Data_Raw, ufsData.Data_File:
				ufsNode, err := unixfsnode.Reify(lctx, pbn, lsys)
				if err != nil {
					return err
				}
				fileNode, ok := ufsNode.(datamodel.LargeBytesNode)
				if !ok {
					return fmt.Errorf("data not a large bytes node despite being UnixFS bytes")
				}
				f, err := fileNode.AsLargeBytes()
				if err != nil {
					return err
				}

				fileSize, err := f.Seek(0, io.SeekEnd)
				if err != nil {
					return fmt.Errorf("unable to get UnixFS file size: %w", err)
				}
				_, err = f.Seek(0, io.SeekStart)
				if err != nil {
					return fmt.Errorf("unable to get reset UnixFS file reader: %w", err)
				}

				out, err := io.ReadAll(io.LimitReader(f, 3072))
				if errors.Is(err, io.EOF) {
					n = NewHeadResponseForFile(files.NewBytesFile(out), fileSize)
					return nil
				}
				return err
			}
		}
		return nil
	})
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	return md, n, nil
}

func (api *CarBackend) ResolvePath(ctx context.Context, p path.ImmutablePath) (ContentPathMetadata, error) {
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": "block", "entityRanges": "0"}).Inc()

	var md ContentPathMetadata
	err := api.fetchCAR(ctx, p, CarParams{Scope: DagScopeBlock}, func(_ path.ImmutablePath, reader io.Reader) error {
		gb, err := carToLinearBlockGetter(ctx, reader, api.getBlockTimeout, api.metrics)
		if err != nil {
			return err
		}
		lsys := getCarLinksystem(gb)

		// First resolve the path since we always need to.
		md, _, err = resolvePathToLastWithRoots(ctx, p, lsys)
		if err != nil {
			return err
		}

		return err
	})
	if err != nil {
		return ContentPathMetadata{}, err
	}

	return md, nil
}

func (api *CarBackend) GetCAR(ctx context.Context, p path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	numRanges := "0"
	if params.Range != nil {
		numRanges = "1"
	}
	api.metrics.carParamsMetric.With(prometheus.Labels{"dagScope": string(params.Scope), "entityRanges": numRanges}).Inc()
	rootCid, err := getRootCid(p)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	switch params.Order {
	case DagOrderUnspecified, DagOrderUnknown, DagOrderDFS:
	default:
		return ContentPathMetadata{}, nil, fmt.Errorf("unsupported dag order %q", params.Order)
	}

	r, w := io.Pipe()
	go func() {
		numBlocksSent := 0
		var cw storage.WritableCar
		var blockBuffer []blocks.Block
		err = api.fetchCAR(ctx, p, params, func(_ path.ImmutablePath, reader io.Reader) error {
			numBlocksThisCall := 0
			gb, err := carToLinearBlockGetter(ctx, reader, api.getBlockTimeout, api.metrics)
			if err != nil {
				return err
			}
			teeBlock := func(ctx context.Context, c cid.Cid) (blocks.Block, error) {
				blk, err := gb(ctx, c)
				if err != nil {
					return nil, err
				}
				if numBlocksThisCall >= numBlocksSent {
					if cw == nil {
						blockBuffer = append(blockBuffer, blk)
					} else {
						err = cw.Put(ctx, blk.Cid().KeyString(), blk.RawData())
						if err != nil {
							return nil, fmt.Errorf("error writing car block: %w", err)
						}
					}
					numBlocksSent++
				}
				numBlocksThisCall++
				return blk, nil
			}
			l := getCarLinksystem(teeBlock)

			var isNotFound bool

			// First resolve the path since we always need to.
			md, terminalBlk, err := resolvePathWithRootsAndBlock(ctx, p, l)
			if err != nil {
				if isErrNotFound(err) {
					isNotFound = true
				} else {
					return err
				}
			}

			if len(md.LastSegmentRemainder) > 0 {
				return nil
			}

			if cw == nil {
				var roots []cid.Cid
				if isNotFound {
					roots = emptyRoot
				} else {
					roots = []cid.Cid{md.LastSegment.RootCid()}
				}

				cw, err = storage.NewWritable(w, roots, carv2.WriteAsCarV1(true), carv2.AllowDuplicatePuts(params.Duplicates.Bool()))
				if err != nil {
					// io.PipeWriter.CloseWithError always returns nil.
					_ = w.CloseWithError(err)
					return nil
				}
				for _, blk := range blockBuffer {
					err = cw.Put(ctx, blk.Cid().KeyString(), blk.RawData())
					if err != nil {
						_ = w.CloseWithError(fmt.Errorf("error writing car block: %w", err))
						return nil
					}
				}
				blockBuffer = nil
			}

			if !isNotFound {
				params.Duplicates = DuplicateBlocksIncluded
				err = walkGatewaySimpleSelector(ctx, terminalBlk.Cid(), terminalBlk, []string{}, params, l)
				if err != nil {
					return err
				}
			}

			return nil
		})

		_ = w.CloseWithError(err)
	}()

	return ContentPathMetadata{
		PathSegmentRoots: []cid.Cid{rootCid},
		LastSegment:      path.FromCid(rootCid),
		ContentType:      "",
	}, r, nil
}

func getRootCid(imPath path.ImmutablePath) (cid.Cid, error) {
	imPathStr := imPath.String()
	if !strings.HasPrefix(imPathStr, "/ipfs/") {
		return cid.Undef, fmt.Errorf("path does not have /ipfs/ prefix")
	}

	firstSegment, _, _ := strings.Cut(imPathStr[6:], "/")
	rootCid, err := cid.Decode(firstSegment)
	if err != nil {
		return cid.Undef, err
	}

	return rootCid, nil
}

func (api *CarBackend) IsCached(ctx context.Context, path path.Path) bool {
	return false
}

var _ IPFSBackend = (*CarBackend)(nil)

func checkRetryableError(e *error, fn func() error) error {
	err := fn()
	retry, processedErr := isRetryableError(err)
	if retry {
		return processedErr
	}
	*e = processedErr
	return nil
}

func isRetryableError(err error) (bool, error) {
	if errors.Is(err, ErrFetcherUnexpectedEOF) {
		return false, err
	}

	if format.IsNotFound(err) {
		return true, err
	}
	initialErr := err

	// Checks if err is of a type that does not implement the .Is interface and
	// cannot be directly compared to. Therefore, errors.Is cannot be used.
	for {
		_, ok := err.(*resolver.ErrNoLink)
		if ok {
			return false, err
		}

		_, ok = err.(datamodel.ErrWrongKind)
		if ok {
			return false, err
		}

		_, ok = err.(datamodel.ErrNotExists)
		if ok {
			return false, err
		}

		errNoSuchField, ok := err.(schema.ErrNoSuchField)
		if ok {
			// Convert into a more general error type so the gateway code can know what this means
			// TODO: Have either a more generally usable error type system for IPLD errors (e.g. a base type indicating that data cannot exist)
			// or at least have one that is specific to the gateway consumer and part of the Backend contract instead of this being implicit
			err = datamodel.ErrNotExists{Segment: errNoSuchField.Field}
			return false, err
		}

		err = errors.Unwrap(err)
		if err == nil {
			return true, initialErr
		}
	}
}

// blockstoreErrToGatewayErr translates underlying blockstore error into one that gateway code will return as HTTP 502 or 504
// it also makes sure Retry-After hint from remote blockstore will be passed to HTTP client, if present.
func blockstoreErrToGatewayErr(err error) error {
	if errors.Is(err, &ErrorStatusCode{}) ||
		errors.Is(err, &ErrorRetryAfter{}) {
		// already correct error
		return err
	}

	// All timeouts should produce 504 Gateway Timeout
	if errors.Is(err, context.DeadlineExceeded) ||
		// Unfortunately this is not an exported type so we have to check for the content.
		strings.Contains(err.Error(), "Client.Timeout exceeded") {
		return fmt.Errorf("%w: %s", ErrGatewayTimeout, err.Error())
	}

	// (Saturn) errors that support the RetryAfter interface need to be converted
	// to the correct gateway error, such that the HTTP header is set.
	for v := err; v != nil; v = errors.Unwrap(v) {
		if r, ok := v.(interface{ RetryAfter() time.Duration }); ok {
			return NewErrorRetryAfter(err, r.RetryAfter())
		}
	}

	// everything else returns 502 Bad Gateway
	return fmt.Errorf("%w: %s", ErrBadGateway, err.Error())
}
