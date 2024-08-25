package io

import (
	"context"

	"berty.tech/go-ipfs-log/entry"
	"berty.tech/go-ipfs-log/iface"
	"berty.tech/go-ipfs-log/io/cbor"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	coreiface "github.com/ipfs/kubo/core/coreiface"
)

type CBOROptions = cbor.Options

func ReadCBOR(ctx context.Context, ipfs coreiface.CoreAPI, c cid.Cid) (format.Node, error) {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		return nil, err
	}

	return io.Read(ctx, ipfs, c)
}

func WriteCBOR(ctx context.Context, ipfs coreiface.CoreAPI, obj interface{}, opts *iface.WriteOpts) (cid.Cid, error) {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		return cid.Undef, err
	}

	return io.Write(ctx, ipfs, obj, opts)
}

func CBOR() *cbor.IOCbor {
	io, err := cbor.IO(&entry.Entry{}, &entry.LamportClock{})
	if err != nil {
		panic(err)
	}

	return io
}
