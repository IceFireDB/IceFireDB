package getter

import (
	"context"
	"errors"

	"github.com/ipfs/boxo/bitswap/client/internal"
	notifications "github.com/ipfs/boxo/bitswap/client/internal/notifications"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var log = logging.Logger("bitswap/client/getter")

// GetBlocksFunc is any function that can take an array of CIDs and return a
// channel of incoming blocks.
type GetBlocksFunc func(context.Context, []cid.Cid) (<-chan blocks.Block, error)

// SyncGetBlock takes a block cid and an async function for getting several
// blocks that returns a channel, and uses that function to return the
// block synchronously.
func SyncGetBlock(p context.Context, k cid.Cid, gb GetBlocksFunc) (blocks.Block, error) {
	p, span := internal.StartSpan(p, "Getter.SyncGetBlock")
	defer span.End()

	if !k.Defined() {
		log.Error("undefined cid in GetBlock")
		return nil, ipld.ErrNotFound{Cid: k}
	}

	// Any async work initiated by this function must end when this function
	// returns. To ensure this, derive a new context. Note that it is okay to
	// listen on parent in this scope, but NOT okay to pass |parent| to
	// functions called by this one. Otherwise those functions won't return
	// when this context's cancel func is executed. This is difficult to
	// enforce. May this comment keep you safe.
	ctx, cancel := context.WithCancel(p)
	defer cancel()

	promise, err := gb(ctx, []cid.Cid{k})
	if err != nil {
		return nil, err
	}

	select {
	case block, ok := <-promise:
		if !ok {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				return nil, errors.New("promise channel was closed")
			}
		}
		return block, nil
	case <-p.Done():
		return nil, p.Err()
	}
}

// WantFunc is any function that can express a want for set of blocks.
type WantFunc func(context.Context, []cid.Cid)

// AsyncGetBlocks take a set of block cids, a pubsub channel for incoming
// blocks, a want function, and a close function, and returns a channel of
// incoming blocks.
func AsyncGetBlocks(ctx, sessctx context.Context, keys []cid.Cid, notif notifications.PubSub, want WantFunc, cwants func([]cid.Cid)) (<-chan blocks.Block, error) {
	ctx, span := internal.StartSpan(ctx, "Getter.AsyncGetBlocks")
	defer span.End()

	out := make(chan blocks.Block)

	// If there are no keys supplied, just return a closed channel
	if len(keys) == 0 {
		close(out)
		return out, nil
	}

	// Use a PubSub notifier to listen for incoming blocks for each key
	remaining := cid.NewSet()
	promise := notif.Subscribe(ctx, keys...)
	for _, k := range keys {
		remaining.Add(k)
	}

	// Send the want request for the keys to the network
	want(ctx, keys)

	go handleIncoming(ctx, sessctx, remaining, promise, out, cwants)
	return out, nil
}

// Listens for incoming blocks, passing them to the out channel.
// If the context is cancelled or the incoming channel closes, calls cfun with
// any keys corresponding to blocks that were never received.
func handleIncoming(ctx, sessctx context.Context, remaining *cid.Set, in <-chan blocks.Block, out chan blocks.Block, cfun func([]cid.Cid)) {
	ctx, span := internal.StartSpan(ctx, "Getter.handleIncoming") // ProbeLab: don't delete/change span without notice
	defer span.End()

	// Clean up before exiting this function, and call the cancel function on
	// any remaining keys
	defer func() {
		close(out)
		// can't just defer this call on its own, arguments are resolved *when* the defer is created
		cfun(remaining.Keys())
	}()

	ctxDone := ctx.Done()
	sessDone := sessctx.Done()
	for {
		select {
		case blk, ok := <-in:
			// If the channel is closed, we're done (note that PubSub closes
			// the channel once all the keys have been received)
			if !ok {
				return
			}

			span.AddEvent("received block", trace.WithAttributes(attribute.String("cid", blk.Cid().String()))) // ProbeLab: don't delete/change without notice

			remaining.Remove(blk.Cid())
			select {
			case out <- blk:
			case <-ctxDone:
				return
			case <-sessDone:
				return
			}
		case <-ctxDone:
			return
		case <-sessDone:
			return
		}
	}
}
