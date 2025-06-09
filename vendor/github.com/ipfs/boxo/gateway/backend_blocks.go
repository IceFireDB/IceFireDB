package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	blockstore "github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/fetcher"
	bsfetcher "github.com/ipfs/boxo/fetcher/impl/blockservice"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	ufile "github.com/ipfs/boxo/ipld/unixfs/file"
	uio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"

	// Ensure basic codecs are registered.
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	mc "github.com/multiformats/go-multicodec"
)

// BlocksBackend is an [IPFSBackend] implementation based on a [blockservice.BlockService].
type BlocksBackend struct {
	baseBackend
	blockStore   blockstore.Blockstore
	blockService blockservice.BlockService
	dagService   format.DAGService
	resolver     resolver.Resolver
}

var _ IPFSBackend = (*BlocksBackend)(nil)

// NewBlocksBackend creates a new [BlocksBackend] backed by a [blockservice.BlockService].
func NewBlocksBackend(blockService blockservice.BlockService, opts ...BackendOption) (*BlocksBackend, error) {
	var compiledOptions backendOptions
	for _, o := range opts {
		if err := o(&compiledOptions); err != nil {
			return nil, err
		}
	}

	// Setup the DAG services, which use the CAR block store.
	dagService := merkledag.NewDAGService(blockService)

	// Setup the [resolver.Resolver] if not provided.
	r := compiledOptions.r
	if r == nil {
		fetcherCfg := bsfetcher.NewFetcherConfig(blockService)
		fetcherCfg.PrototypeChooser = dagpb.AddSupportToChooser(bsfetcher.DefaultPrototypeChooser)
		fetcher := fetcherCfg.WithReifier(unixfsnode.Reify)
		r = resolver.NewBasicResolver(fetcher)
	}

	// Setup the [baseBackend] which takes care of some shared functionality, such
	// as resolving /ipns links.
	baseBackend, err := newBaseBackend(compiledOptions.vs, compiledOptions.ns)
	if err != nil {
		return nil, err
	}

	return &BlocksBackend{
		baseBackend:  baseBackend,
		blockStore:   blockService.Blockstore(),
		blockService: blockService,
		dagService:   dagService,
		resolver:     r,
	}, nil
}

// NewRemoteBlocksBackend creates a new [BlocksBackend] backed by one or more
// gateways. These gateways must support RAW block requests and IPNS Record
// requests. See [NewRemoteBlockstore] and [NewRemoteValueStore] for more details.
//
// To create a more custom [BlocksBackend], please use [NewBlocksBackend] directly.
func NewRemoteBlocksBackend(gatewayURL []string, httpClient *http.Client, opts ...BackendOption) (*BlocksBackend, error) {
	blockStore, err := NewRemoteBlockstore(gatewayURL, httpClient)
	if err != nil {
		return nil, err
	}

	valueStore, err := NewRemoteValueStore(gatewayURL, httpClient)
	if err != nil {
		return nil, err
	}

	blockService := blockservice.New(blockStore, offline.Exchange(blockStore))
	return NewBlocksBackend(blockService, append(opts, WithValueStore(valueStore))...)
}

func (bb *BlocksBackend) Get(ctx context.Context, path path.ImmutablePath, ranges ...ByteRange) (ContentPathMetadata, *GetResponse, error) {
	md, nd, err := bb.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	// Only a single range is supported in responses to HTTP Range Requests.
	// When more than one is passed in the Range header, this library will
	// return a response for the first one and ignores remaining ones.
	var ra *ByteRange
	if len(ranges) > 0 {
		ra = &ranges[0]
	}

	rootCodec := nd.Cid().Prefix().GetCodec()

	// This covers both Raw blocks and terminal IPLD codecs like dag-cbor and dag-json
	// Note: while only cbor, json, dag-cbor, and dag-json are currently supported by gateways this could change
	// Note: For the raw codec we return just the relevant range rather than the entire block
	if rootCodec != uint64(mc.DagPb) {
		f := files.NewBytesFile(nd.RawData())

		fileSize, err := f.Size()
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}

		if rootCodec == uint64(mc.Raw) {
			if err := seekToRangeStart(f, ra); err != nil {
				return ContentPathMetadata{}, nil, err
			}
		}

		return md, NewGetResponseFromReader(f, fileSize), nil
	}

	// This code path covers full graph, single file/directory, and range requests
	f, err := ufile.NewUnixfsFile(ctx, bb.dagService, nd)
	// Note: there is an assumption here that non-UnixFS dag-pb should not be returned which is currently valid
	if err != nil {
		return md, nil, err
	}

	// Set modification time in ContentPathMetadata if found in dag-pb's optional mtime field (UnixFS 1.5)
	mtime := f.ModTime()
	if !mtime.IsZero() {
		md.ModTime = mtime
	}

	if d, ok := f.(files.Directory); ok {
		dir, err := uio.NewDirectoryFromNode(bb.dagService, nd)
		if err != nil {
			return md, nil, err
		}
		sz, err := d.Size()
		if err != nil {
			return ContentPathMetadata{}, nil, fmt.Errorf("could not get cumulative directory DAG size: %w", err)
		}
		if sz < 0 {
			return ContentPathMetadata{}, nil, errors.New("directory cumulative DAG size cannot be negative")
		}
		return md, NewGetResponseFromDirectoryListing(uint64(sz), dir.EnumLinksAsync(ctx), nil), nil
	}
	if file, ok := f.(files.File); ok {
		fileSize, err := f.Size()
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}

		if err := seekToRangeStart(file, ra); err != nil {
			return ContentPathMetadata{}, nil, err
		}

		if s, ok := f.(*files.Symlink); ok {
			return md, NewGetResponseFromSymlink(s, fileSize), nil
		}

		return md, NewGetResponseFromReader(file, fileSize), nil
	}

	return ContentPathMetadata{}, nil, fmt.Errorf("data was not a valid file or directory: %w", ErrInternalServerError) // TODO: should there be a gateway invalid content type to abstract over the various IPLD error types?
}

func (bb *BlocksBackend) GetAll(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.Node, error) {
	md, nd, err := bb.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	// This code path covers full graph, single file/directory, and range requests
	n, err := ufile.NewUnixfsFile(ctx, bb.dagService, nd)
	if err != nil {
		return md, nil, err
	}
	return md, n, nil
}

func (bb *BlocksBackend) GetBlock(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, files.File, error) {
	roots, lastSeg, remainder, err := bb.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	md := ContentPathMetadata{
		PathSegmentRoots:     roots,
		LastSegment:          lastSeg,
		LastSegmentRemainder: remainder,
	}

	lastRoot := lastSeg.RootCid()

	b, err := bb.blockService.GetBlock(ctx, lastRoot)
	if err != nil {
		return md, nil, err
	}

	return md, files.NewBytesFile(b.RawData()), nil
}

func (bb *BlocksBackend) Head(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, *HeadResponse, error) {
	md, nd, err := bb.getNode(ctx, path)
	if err != nil {
		return md, nil, err
	}

	rootCodec := nd.Cid().Prefix().GetCodec()
	if rootCodec != uint64(mc.DagPb) {
		return md, NewHeadResponseForFile(files.NewBytesFile(nd.RawData()), int64(len(nd.RawData()))), nil
	}

	// TODO: We're not handling non-UnixFS dag-pb. There's a bit of a discrepancy
	// between what we want from a HEAD request and a Resolve request here and we're using this for both
	fileNode, err := ufile.NewUnixfsFile(ctx, bb.dagService, nd)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	// Set modification time in ContentPathMetadata if found in dag-pb's optional mtime field (UnixFS 1.5)
	mtime := fileNode.ModTime()
	if !mtime.IsZero() {
		md.ModTime = mtime
	}

	sz, err := fileNode.Size()
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	if _, ok := fileNode.(files.Directory); ok {
		return md, NewHeadResponseForDirectory(sz), nil
	}

	if _, ok := fileNode.(*files.Symlink); ok {
		return md, NewHeadResponseForSymlink(sz), nil
	}

	if f, ok := fileNode.(files.File); ok {
		return md, NewHeadResponseForFile(f, sz), nil
	}

	return ContentPathMetadata{}, nil, errors.New("unsupported UnixFS file type")
}

// emptyRoot is a CAR root with the empty identity CID. CAR files are recommended
// to always include a CID in their root, even if it's just the empty CID.
// https://ipld.io/specs/transport/car/carv1/#number-of-roots
var emptyRoot = []cid.Cid{cid.MustParse("bafkqaaa")}

func (bb *BlocksBackend) GetCAR(ctx context.Context, p path.ImmutablePath, params CarParams) (ContentPathMetadata, io.ReadCloser, error) {
	pathMetadata, resolveErr := bb.ResolvePath(ctx, p)
	if resolveErr != nil {
		rootCid, err := cid.Decode(strings.Split(p.String(), "/")[2])
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}

		var buf bytes.Buffer
		cw, err := storage.NewWritable(&buf, emptyRoot, car.WriteAsCarV1(true))
		if err != nil {
			return ContentPathMetadata{}, nil, err
		}

		blockGetter := merkledag.NewDAGService(bb.blockService).Session(ctx)

		blockGetter = &nodeGetterToCarExporer{
			ng: blockGetter,
			cw: cw,
		}

		// Setup the UnixFS resolver.
		f := newNodeGetterFetcherSingleUseFactory(ctx, blockGetter)
		pathResolver := resolver.NewBasicResolver(f)
		_, _, err = pathResolver.ResolveToLastNode(ctx, p)

		if isErrNotFound(err) {
			return ContentPathMetadata{
				PathSegmentRoots: nil,
				LastSegment:      path.FromCid(rootCid),
				ContentType:      "",
			}, io.NopCloser(&buf), nil
		} else if err != nil {
			return ContentPathMetadata{}, nil, err
		} else {
			return ContentPathMetadata{}, nil, resolveErr
		}
	}

	if p.Namespace() != path.IPFSNamespace {
		return ContentPathMetadata{}, nil, errors.New("path does not have /ipfs/ prefix")
	}

	r, w := io.Pipe()
	go func() {
		cw, err := storage.NewWritable(
			w,
			[]cid.Cid{pathMetadata.LastSegment.RootCid()},
			car.WriteAsCarV1(true),
			car.AllowDuplicatePuts(params.Duplicates.Bool()),
		)
		if err != nil {
			// io.PipeWriter.CloseWithError always returns nil.
			_ = w.CloseWithError(err)
			return
		}

		blockGetter := merkledag.NewDAGService(bb.blockService).Session(ctx)

		blockGetter = &nodeGetterToCarExporer{
			ng: blockGetter,
			cw: cw,
		}

		// Setup the UnixFS resolver.
		f := newNodeGetterFetcherSingleUseFactory(ctx, blockGetter)
		pathResolver := resolver.NewBasicResolver(f)

		lsys := cidlink.DefaultLinkSystem()
		unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)
		lsys.StorageReadOpener = blockOpener(ctx, blockGetter)

		// First resolve the path since we always need to.
		lastCid, remainder, err := pathResolver.ResolveToLastNode(ctx, p)
		if err != nil {
			// io.PipeWriter.CloseWithError always returns nil.
			_ = w.CloseWithError(err)
			return
		}

		// TODO: support selectors passed as request param: https://github.com/ipfs/kubo/issues/8769
		// TODO: this is very slow if blocks are remote due to linear traversal. Do we need deterministic traversals here?
		carWriteErr := walkGatewaySimpleSelector(ctx, lastCid, nil, remainder, params, &lsys)

		// io.PipeWriter.CloseWithError always returns nil.
		_ = w.CloseWithError(carWriteErr)
	}()

	return pathMetadata, r, nil
}

// walkGatewaySimpleSelector walks the subgraph described by the path and terminal element parameters
func walkGatewaySimpleSelector(ctx context.Context, lastCid cid.Cid, terminalBlk blocks.Block, remainder []string, params CarParams, lsys *ipld.LinkSystem) error {
	lctx := ipld.LinkContext{Ctx: ctx}
	pathTerminalCidLink := cidlink.Link{Cid: lastCid}

	// If the scope is the block, now we only need to retrieve the root block of the last element of the path.
	if params.Scope == DagScopeBlock {
		_, err := lsys.LoadRaw(lctx, pathTerminalCidLink)
		return err
	}

	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	np, err := pc(pathTerminalCidLink, lctx)
	if err != nil {
		return err
	}

	var lastCidNode datamodel.Node
	if terminalBlk != nil {
		decoder, err := lsys.DecoderChooser(pathTerminalCidLink)
		if err != nil {
			return err
		}
		nb := np.NewBuilder()
		blockData := terminalBlk.RawData()
		if err := decoder(nb, bytes.NewReader(blockData)); err != nil {
			return err
		}
		lastCidNode = nb.Build()
	} else {
		lastCidNode, err = lsys.Load(lctx, pathTerminalCidLink, np)
		if err != nil {
			return err
		}
	}

	// If we're asking for everything then give it
	if params.Scope == DagScopeAll {
		sel, err := selector.ParseSelector(selectorparse.CommonSelector_ExploreAllRecursively)
		if err != nil {
			return err
		}

		progress := traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            ctx,
				LinkSystem:                     *lsys,
				LinkTargetNodePrototypeChooser: bsfetcher.DefaultPrototypeChooser,
				LinkVisitOnlyOnce:              !params.Duplicates.Bool(),
			},
		}

		if err := progress.WalkMatching(lastCidNode, sel, func(progress traversal.Progress, node datamodel.Node) error {
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	// From now on, dag-scope=entity!
	// Since we need more of the graph load it to figure out what we have
	// This includes determining if the terminal node is UnixFS or not
	if pbn, ok := lastCidNode.(dagpb.PBNode); !ok {
		// If it's not valid dag-pb then we're done
		return nil
	} else if len(remainder) > 0 {
		// If we're trying to path into dag-pb node that's invalid and we're done
		return nil
	} else if !pbn.FieldData().Exists() {
		// If it's not valid UnixFS then we're done
		return nil
	} else if unixfsFieldData, decodeErr := data.DecodeUnixFSData(pbn.Data.Must().Bytes()); decodeErr != nil {
		// If it's not valid dag-pb and UnixFS then we're done
		return nil
	} else {
		switch unixfsFieldData.FieldDataType().Int() {
		case data.Data_Directory, data.Data_Symlink:
			// These types are non-recursive so we're done
			return nil
		case data.Data_Raw, data.Data_Metadata:
			// TODO: for now, we decided to return nil here. The different implementations are inconsistent
			// and UnixFS is not properly specified: https://github.com/ipfs/specs/issues/316.
			// 		- Is Data_Raw different from Data_File?
			//		- Data_Metadata is handled differently in boxo/ipld/unixfs and go-unixfsnode.
			return nil
		case data.Data_HAMTShard:
			// Return all elements in the map
			_, err := lsys.KnownReifiers["unixfs-preload"](lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}
			return nil
		case data.Data_File:
			nd, err := unixfsnode.Reify(lctx, lastCidNode, lsys)
			if err != nil {
				return err
			}

			fnd, ok := nd.(datamodel.LargeBytesNode)
			if !ok {
				return errors.New("could not process file since it did not present as large bytes")
			}
			f, err := fnd.AsLargeBytes()
			if err != nil {
				return err
			}

			// Get the entity range. If it's empty, assume the defaults (whole file).
			entityRange := params.Range
			if entityRange == nil {
				entityRange = &DagByteRange{
					From: 0,
				}
			}

			from := entityRange.From

			// If we're starting to read based on the end of the file, find out where that is.
			var fileLength int64
			foundFileLength := false
			if entityRange.From < 0 {
				fileLength, err = f.Seek(0, io.SeekEnd)
				if err != nil {
					return err
				}
				from = fileLength + entityRange.From
				if from < 0 {
					from = 0
				}
				foundFileLength = true
			}

			// If we're reading until the end of the file then do it
			if entityRange.To == nil {
				if _, err := f.Seek(from, io.SeekStart); err != nil {
					return err
				}
				_, err = io.Copy(io.Discard, f)
				return err
			}

			to := *entityRange.To
			if (*entityRange.To) < 0 {
				if !foundFileLength {
					fileLength, err = f.Seek(0, io.SeekEnd)
					if err != nil {
						return err
					}
				}
				to = fileLength + *entityRange.To
			}

			numToRead := 1 + to - from
			if numToRead < 0 {
				return errors.New("tried to read less than zero bytes")
			}

			if _, err := f.Seek(from, io.SeekStart); err != nil {
				return err
			}
			_, err = io.CopyN(io.Discard, f, numToRead)
			return err
		default:
			// Not a supported type, so we're done
			return nil
		}
	}
}

func (bb *BlocksBackend) getNode(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, format.Node, error) {
	roots, lastSeg, remainder, err := bb.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	md := ContentPathMetadata{
		PathSegmentRoots:     roots,
		LastSegment:          lastSeg,
		LastSegmentRemainder: remainder,
	}

	lastRoot := lastSeg.RootCid()

	nd, err := bb.dagService.Get(ctx, lastRoot)
	if err != nil {
		return ContentPathMetadata{}, nil, err
	}

	return md, nd, err
}

func (bb *BlocksBackend) getPathRoots(ctx context.Context, contentPath path.ImmutablePath) ([]cid.Cid, path.ImmutablePath, []string, error) {
	/*
		These are logical roots where each CID represent one path segment
		and resolves to either a directory or the root block of a file.
		The main purpose of this header is allow HTTP caches to do smarter decisions
		around cache invalidation (eg. keep specific subdirectory/file if it did not change)
		A good example is Wikipedia, which is HAMT-sharded, but we only care about
		logical roots that represent each segment of the human-readable content
		path:
		Given contentPath = /ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey
		rootCidList is a generated by doing `ipfs resolve -r` on each sub path:
			/ipns/en.wikipedia-on-ipfs.org → bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze
			/ipns/en.wikipedia-on-ipfs.org/wiki/ → bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4
			/ipns/en.wikipedia-on-ipfs.org/wiki/Block_of_Wikipedia_in_Turkey → bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma
		The result is an ordered array of values:
			X-Ipfs-Roots: bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze,bafybeihn2f7lhumh4grizksi2fl233cyszqadkn424ptjajfenykpsaiw4,bafkreibn6euazfvoghepcm4efzqx5l3hieof2frhp254hio5y7n3hv5rma
		Note that while the top one will change every time any article is changed,
		the last root (responsible for specific article) may not change at all.
	*/
	var sp strings.Builder
	var pathRoots []cid.Cid
	contentPathStr := contentPath.String()
	pathSegments := strings.Split(contentPathStr[6:], "/")
	sp.WriteString(contentPathStr[:5]) // /ipfs or /ipns
	var (
		lastPath  path.ImmutablePath
		remainder []string
	)
	for _, root := range pathSegments {
		if root == "" {
			continue
		}
		sp.WriteString("/")
		sp.WriteString(root)
		p, err := path.NewPath(sp.String())
		if err != nil {
			return nil, path.ImmutablePath{}, nil, err
		}
		resolvedSubPath, remainderSubPath, err := bb.resolvePath(ctx, p)
		if err != nil {
			// TODO: should we be more explicit here and is this part of the IPFSBackend contract?
			// The issue here was that we returned datamodel.ErrWrongKind instead of this resolver error
			if isErrNotFound(err) {
				return nil, path.ImmutablePath{}, nil, &resolver.ErrNoLink{Name: root, Node: lastPath.RootCid()}
			}
			return nil, path.ImmutablePath{}, nil, err
		}
		lastPath = resolvedSubPath
		remainder = remainderSubPath
		pathRoots = append(pathRoots, lastPath.RootCid())
	}

	pathRoots = pathRoots[:len(pathRoots)-1]
	return pathRoots, lastPath, remainder, nil
}

func (bb *BlocksBackend) IsCached(ctx context.Context, p path.Path) bool {
	rp, _, err := bb.resolvePath(ctx, p)
	if err != nil {
		return false
	}

	has, _ := bb.blockStore.Has(ctx, rp.RootCid())
	return has
}

var _ WithContextHint = (*BlocksBackend)(nil)

func (bb *BlocksBackend) WrapContextForRequest(ctx context.Context) context.Context {
	return blockservice.ContextWithSession(ctx, bb.blockService)
}

func (bb *BlocksBackend) ResolvePath(ctx context.Context, path path.ImmutablePath) (ContentPathMetadata, error) {
	roots, lastSeg, remainder, err := bb.getPathRoots(ctx, path)
	if err != nil {
		return ContentPathMetadata{}, err
	}
	md := ContentPathMetadata{
		PathSegmentRoots:     roots,
		LastSegment:          lastSeg,
		LastSegmentRemainder: remainder,
	}
	return md, nil
}

func (bb *BlocksBackend) resolvePath(ctx context.Context, p path.Path) (path.ImmutablePath, []string, error) {
	var err error
	if p.Namespace() == path.IPNSNamespace {
		p, _, _, err = bb.baseBackend.ResolveMutable(ctx, p)
		if err != nil {
			return path.ImmutablePath{}, nil, err
		}
	}

	if p.Namespace() != path.IPFSNamespace {
		return path.ImmutablePath{}, nil, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}

	imPath, err := path.NewImmutablePath(p)
	if err != nil {
		return path.ImmutablePath{}, nil, err
	}

	node, remainder, err := bb.resolver.ResolveToLastNode(ctx, imPath)
	if err != nil {
		return path.ImmutablePath{}, nil, err
	}

	p, err = path.Join(path.FromCid(node), remainder...)
	if err != nil {
		return path.ImmutablePath{}, nil, err
	}

	imPath, err = path.NewImmutablePath(p)
	if err != nil {
		return path.ImmutablePath{}, nil, err
	}

	return imPath, remainder, nil
}

type nodeGetterToCarExporer struct {
	ng format.NodeGetter
	cw storage.WritableCar
}

func (n *nodeGetterToCarExporer) Get(ctx context.Context, c cid.Cid) (format.Node, error) {
	nd, err := n.ng.Get(ctx, c)
	if err != nil {
		return nil, err
	}

	if err := n.trySendBlock(ctx, nd); err != nil {
		return nil, err
	}

	return nd, nil
}

func (n *nodeGetterToCarExporer) GetMany(ctx context.Context, cids []cid.Cid) <-chan *format.NodeOption {
	ndCh := n.ng.GetMany(ctx, cids)
	outCh := make(chan *format.NodeOption)
	go func() {
		defer close(outCh)
		for nd := range ndCh {
			if nd.Err == nil {
				if err := n.trySendBlock(ctx, nd.Node); err != nil {
					select {
					case outCh <- &format.NodeOption{Err: err}:
					case <-ctx.Done():
					}
					return
				}
				select {
				case outCh <- nd:
				case <-ctx.Done():
				}
			}
		}
	}()
	return outCh
}

func (n *nodeGetterToCarExporer) trySendBlock(ctx context.Context, block blocks.Block) error {
	return n.cw.Put(ctx, block.Cid().KeyString(), block.RawData())
}

var _ format.NodeGetter = (*nodeGetterToCarExporer)(nil)

type nodeGetterFetcherSingleUseFactory struct {
	linkSystem   ipld.LinkSystem
	protoChooser traversal.LinkTargetNodePrototypeChooser
}

func newNodeGetterFetcherSingleUseFactory(ctx context.Context, ng format.NodeGetter) *nodeGetterFetcherSingleUseFactory {
	ls := cidlink.DefaultLinkSystem()
	ls.TrustedStorage = true
	ls.StorageReadOpener = blockOpener(ctx, ng)
	ls.NodeReifier = unixfsnode.Reify

	pc := dagpb.AddSupportToChooser(func(lnk ipld.Link, lnkCtx ipld.LinkContext) (ipld.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})

	return &nodeGetterFetcherSingleUseFactory{ls, pc}
}

func (n *nodeGetterFetcherSingleUseFactory) NewSession(ctx context.Context) fetcher.Fetcher {
	return n
}

func (n *nodeGetterFetcherSingleUseFactory) NodeMatching(ctx context.Context, root ipld.Node, selector ipld.Node, cb fetcher.FetchCallback) error {
	return n.nodeMatching(ctx, n.blankProgress(ctx), root, selector, cb)
}

func (n *nodeGetterFetcherSingleUseFactory) BlockOfType(ctx context.Context, link ipld.Link, nodePrototype ipld.NodePrototype) (ipld.Node, error) {
	return n.linkSystem.Load(ipld.LinkContext{}, link, nodePrototype)
}

func (n *nodeGetterFetcherSingleUseFactory) BlockMatchingOfType(ctx context.Context, root ipld.Link, selector ipld.Node, nodePrototype ipld.NodePrototype, cb fetcher.FetchCallback) error {
	// retrieve first node
	prototype, err := n.PrototypeFromLink(root)
	if err != nil {
		return err
	}
	node, err := n.BlockOfType(ctx, root, prototype)
	if err != nil {
		return err
	}

	progress := n.blankProgress(ctx)
	progress.LastBlock.Link = root
	return n.nodeMatching(ctx, progress, node, selector, cb)
}

func (n *nodeGetterFetcherSingleUseFactory) PrototypeFromLink(lnk ipld.Link) (ipld.NodePrototype, error) {
	return n.protoChooser(lnk, ipld.LinkContext{})
}

func (n *nodeGetterFetcherSingleUseFactory) nodeMatching(ctx context.Context, initialProgress traversal.Progress, node ipld.Node, match ipld.Node, cb fetcher.FetchCallback) error {
	matchSelector, err := selector.ParseSelector(match)
	if err != nil {
		return err
	}
	return initialProgress.WalkMatching(node, matchSelector, func(prog traversal.Progress, n ipld.Node) error {
		return cb(fetcher.FetchResult{
			Node:          n,
			Path:          prog.Path,
			LastBlockPath: prog.LastBlock.Path,
			LastBlockLink: prog.LastBlock.Link,
		})
	})
}

func (n *nodeGetterFetcherSingleUseFactory) blankProgress(ctx context.Context) traversal.Progress {
	return traversal.Progress{
		Cfg: &traversal.Config{
			LinkSystem:                     n.linkSystem,
			LinkTargetNodePrototypeChooser: n.protoChooser,
		},
	}
}

func blockOpener(ctx context.Context, ng format.NodeGetter) ipld.BlockReadOpener {
	return func(_ ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		cidLink, ok := lnk.(cidlink.Link)
		if !ok {
			return nil, fmt.Errorf("invalid link type for loading: %v", lnk)
		}

		blk, err := ng.Get(ctx, cidLink.Cid)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(blk.RawData()), nil
	}
}

var (
	_ fetcher.Fetcher = (*nodeGetterFetcherSingleUseFactory)(nil)
	_ fetcher.Factory = (*nodeGetterFetcherSingleUseFactory)(nil)
)
