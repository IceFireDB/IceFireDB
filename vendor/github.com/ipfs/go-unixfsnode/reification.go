package unixfsnode

import (
	"context"
	"fmt"

	"github.com/ipfs/go-unixfsnode/data"
	"github.com/ipfs/go-unixfsnode/directory"
	"github.com/ipfs/go-unixfsnode/file"
	"github.com/ipfs/go-unixfsnode/hamt"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
)

// Reify looks at an ipld Node and tries to interpret it as a UnixFSNode
// if successful, it returns the UnixFSNode
func Reify(lnkCtx ipld.LinkContext, maybePBNodeRoot ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
	return doReify(lnkCtx, maybePBNodeRoot, lsys, true)
}

// nonLazyReify works like reify but will load all of a directory or file as it reaches them.
func nonLazyReify(lnkCtx ipld.LinkContext, maybePBNodeRoot ipld.Node, lsys *ipld.LinkSystem) (ipld.Node, error) {
	return doReify(lnkCtx, maybePBNodeRoot, lsys, false)
}

func doReify(lnkCtx ipld.LinkContext, maybePBNodeRoot ipld.Node, lsys *ipld.LinkSystem, lazy bool) (ipld.Node, error) {
	pbNode, ok := maybePBNodeRoot.(dagpb.PBNode)
	if !ok {
		return maybePBNodeRoot, nil
	}
	if !pbNode.FieldData().Exists() {
		// no data field, therefore, not UnixFS
		return defaultReifier(lnkCtx.Ctx, pbNode, lsys)
	}
	data, err := data.DecodeUnixFSData(pbNode.Data.Must().Bytes())
	if err != nil {
		// we could not decode the UnixFS data, therefore, not UnixFS
		return defaultReifier(lnkCtx.Ctx, pbNode, lsys)
	}
	var builder reifyTypeFunc
	if lazy {
		builder, ok = lazyReifyFuncs[data.FieldDataType().Int()]
	} else {
		builder, ok = reifyFuncs[data.FieldDataType().Int()]
	}
	if !ok {
		return nil, fmt.Errorf("no reification for this UnixFS node type")
	}
	return builder(lnkCtx.Ctx, pbNode, data, lsys)
}

type reifyTypeFunc func(context.Context, dagpb.PBNode, data.UnixFSData, *ipld.LinkSystem) (ipld.Node, error)

var reifyFuncs = map[int64]reifyTypeFunc{
	data.Data_File:      unixFSFileReifierWithPreload,
	data.Data_Metadata:  defaultUnixFSReifier,
	data.Data_Raw:       unixFSFileReifier,
	data.Data_Symlink:   defaultUnixFSReifier,
	data.Data_Directory: directory.NewUnixFSBasicDir,
	data.Data_HAMTShard: hamt.NewUnixFSHAMTShardWithPreload,
}
var lazyReifyFuncs = map[int64]reifyTypeFunc{
	data.Data_File:      unixFSFileReifier,
	data.Data_Metadata:  defaultUnixFSReifier,
	data.Data_Raw:       unixFSFileReifier,
	data.Data_Symlink:   defaultUnixFSReifier,
	data.Data_Directory: directory.NewUnixFSBasicDir,
	data.Data_HAMTShard: hamt.NewUnixFSHAMTShard,
}

// treat non-unixFS nodes like directories -- allow them to lookup by link
// TODO: Make this a separate node as directories gain more functionality
func defaultReifier(_ context.Context, substrate dagpb.PBNode, _ *ipld.LinkSystem) (ipld.Node, error) {
	return &_PathedPBNode{_substrate: substrate}, nil
}

func unixFSFileReifier(ctx context.Context, substrate dagpb.PBNode, _ data.UnixFSData, ls *ipld.LinkSystem) (ipld.Node, error) {
	return file.NewUnixFSFile(ctx, substrate, ls)
}

func unixFSFileReifierWithPreload(ctx context.Context, substrate dagpb.PBNode, _ data.UnixFSData, ls *ipld.LinkSystem) (ipld.Node, error) {
	return file.NewUnixFSFileWithPreload(ctx, substrate, ls)
}

func defaultUnixFSReifier(ctx context.Context, substrate dagpb.PBNode, _ data.UnixFSData, ls *ipld.LinkSystem) (ipld.Node, error) {
	return defaultReifier(ctx, substrate, ls)
}

var _ ipld.NodeReifier = Reify
