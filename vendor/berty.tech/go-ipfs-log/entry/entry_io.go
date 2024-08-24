package entry // import "berty.tech/go-ipfs-log/entry"

import (
	"context"

	"github.com/ipfs/go-cid"
	core_iface "github.com/ipfs/interface-go-ipfs-core"

	"berty.tech/go-ipfs-log/iface"
)

type FetchOptions = iface.FetchOptions

// FetchParallel has the same comportement than FetchAll, we keep it for retrop
// compatibility purpose
func FetchParallel(ctx context.Context, ipfs core_iface.CoreAPI, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(ipfs, options)
	return fetcher.Fetch(ctx, hashes)
}

// FetchAll gets entries from their CIDs.
func FetchAll(ctx context.Context, ipfs core_iface.CoreAPI, hashes []cid.Cid, options *FetchOptions) []iface.IPFSLogEntry {
	fetcher := NewFetcher(ipfs, options)
	return fetcher.Fetch(ctx, hashes)
}
