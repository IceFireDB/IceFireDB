package ipldutil

import (
	"context"
	"errors"
	"io"
	"sync"

	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
)

/* TODO: This traverser creates an extra go-routine and is quite complicated, in order to give calling code control of
a selector traversal. If it were implemented inside of go-ipld-primes traversal library, with access to private functions,
it could be done without an extra go-routine, avoiding the possibility of races and simplifying implementation. This has
been documented here: https://github.com/ipld/go-ipld-prime/issues/213 -- and when this issue is implemented, this traverser
can go away */

var defaultLinkSystem = cidlink.DefaultLinkSystem()

func defaultVisitor(traversal.Progress, ipld.Node, traversal.VisitReason) error { return nil }

// ContextCancelError is a sentinel that indicates the passed in context
// was cancelled
type ContextCancelError struct{}

func (cp ContextCancelError) Error() string {
	return "context cancelled"
}

// IsContextCancelErr checks whther the given err is ContextCancelError or has a one wrapped.
// See: errors.Is.
func IsContextCancelErr(err error) bool {
	return errors.Is(err, ContextCancelError{})
}

// TraversalBuilder defines parameters for an iterative traversal
type TraversalBuilder struct {
	Root       ipld.Link
	Selector   ipld.Node
	Visitor    traversal.AdvVisitFn
	LinkSystem ipld.LinkSystem
	Chooser    traversal.LinkTargetNodePrototypeChooser
	Budget     *traversal.Budget
}

// Traverser is an interface for performing a selector traversal that operates iteratively --
// it stops and waits for a manual load every time a block boundary is encountered
type Traverser interface {
	// IsComplete returns the completion state (boolean) and if so, the final
	// error result from IPLD.
	//
	// Note that CurrentRequest will block if the traverser is performing an
	// IPLD load.
	IsComplete() (bool, error)

	// CurrentRequest returns the parameters for the current block load waiting
	// to be fulfilled in order to advance further.
	//
	// Note that CurrentRequest will block if the traverser is performing an
	// IPLD load.
	CurrentRequest() (ipld.Link, ipld.LinkContext)

	// Advance advances the traversal successfully by supplying the given reader
	// as the result of the next IPLD load.
	Advance(reader io.Reader) error

	// Error errors the traversal by returning the given error as the result of
	// the next IPLD load.
	Error(err error)

	// Shutdown cancels the traversal
	Shutdown(ctx context.Context)

	// NBlocksTraversed returns the number of blocks successfully traversed
	NBlocksTraversed() int
}

type nextResponse struct {
	input io.Reader
	err   error
}

// Start initiates the traversal (run in a go routine because the regular
// selector traversal expects a call back)
func (tb TraversalBuilder) Start(parentCtx context.Context) Traverser {
	ctx, cancel := context.WithCancel(parentCtx)
	t := &traverser{
		ctx:        ctx,
		cancel:     cancel,
		root:       tb.Root,
		selector:   tb.Selector,
		linkSystem: tb.LinkSystem,
		budget:     tb.Budget,
		responses:  make(chan nextResponse),
		stopped:    make(chan struct{}),
	}
	if tb.Visitor != nil {
		t.visitor = tb.Visitor
	} else {
		t.visitor = defaultVisitor
	}
	if tb.Chooser != nil {
		t.chooser = tb.Chooser
	} else {
		t.chooser = dagpb.AddSupportToChooser(basicnode.Chooser)
	}
	if tb.LinkSystem.DecoderChooser == nil {
		t.linkSystem.DecoderChooser = defaultLinkSystem.DecoderChooser
	}
	if tb.LinkSystem.EncoderChooser == nil {
		t.linkSystem.EncoderChooser = defaultLinkSystem.EncoderChooser
	}
	if tb.LinkSystem.HasherChooser == nil {
		t.linkSystem.HasherChooser = defaultLinkSystem.HasherChooser
	}
	t.linkSystem.StorageReadOpener = t.loader
	t.start()
	return t
}

// traverser is a class to perform a selector traversal that stops every time a new block is loaded
// and waits for manual input (in the form of advance or error)
type traverser struct {
	blocksCount int
	ctx         context.Context
	cancel      context.CancelFunc
	root        ipld.Link
	selector    ipld.Node
	visitor     traversal.AdvVisitFn
	linkSystem  ipld.LinkSystem
	chooser     traversal.LinkTargetNodePrototypeChooser
	budget      *traversal.Budget

	// stateMu is held while a block is being loaded.
	// It is released when a StorageReadOpener callback is received,
	// so that the user can inspect the state and use Advance or Error.
	// Advance/Error grab the mutex and let StorageReadOpener return.
	// The four state fields are only safe to read while the mutex isn't held.
	stateMu        sync.Mutex
	isDone         bool
	completionErr  error
	currentLink    ipld.Link
	currentContext ipld.LinkContext

	// responses blocks LinkSystem block loads (in the method "loader")
	// until the next Advance or Error method call.
	responses chan nextResponse

	// stopped is closed when the traverser is stopped,
	// due to being finishing, cancelled, or shut down.
	stopped chan struct{}
}

func (t *traverser) NBlocksTraversed() int {
	return t.blocksCount
}

func (t *traverser) loader(lnkCtx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
	// A StorageReadOpener call came in; update the state and release the lock.
	// We can't simply unlock the mutex inside the <-t.responses case,
	// as then we'd deadlock with the other side trying to send.
	// The other side can't lock after sending to t.responses,
	// as otherwise the load might start before the mutex is held.
	t.currentLink = lnk
	t.currentContext = lnkCtx
	t.stateMu.Unlock()

	select {
	case <-t.ctx.Done():
		// We got cancelled, so we won't load this block via the responses chan.
		// Lock the mutex again, until writeDone gives the user their final error.
		t.stateMu.Lock()
		return nil, ContextCancelError{}
	case response := <-t.responses:
		return response.input, response.err
	}
}

func (t *traverser) writeDone(err error) {
	t.isDone = true
	t.completionErr = err
	t.currentContext = ipld.LinkContext{Ctx: t.ctx}

	// The traversal is done, so there won't be another StorageReadOpener call.
	// Unlock the state so the user can use IsComplete etc.
	t.stateMu.Unlock()
}

func (t *traverser) start() {
	// Grab the state mutex until the first StorageReadOpener call comes in.
	t.stateMu.Lock()

	go func() {
		defer close(t.stopped)
		ns, err := t.chooser(t.root, ipld.LinkContext{Ctx: t.ctx})
		if err != nil {
			t.writeDone(err)
			return
		}
		if t.budget != nil {
			t.budget.LinkBudget--
			if t.budget.LinkBudget <= 0 {
				t.writeDone(&traversal.ErrBudgetExceeded{BudgetKind: "link", Link: t.root})
				return
			}
		}
		nd, err := t.linkSystem.Load(ipld.LinkContext{Ctx: t.ctx}, t.root, ns)
		if err != nil {
			t.writeDone(err)
			return
		}

		sel, err := selector.ParseSelector(t.selector)
		if err != nil {
			t.writeDone(err)
			return
		}
		err = traversal.Progress{
			Cfg: &traversal.Config{
				Ctx:                            t.ctx,
				LinkSystem:                     t.linkSystem,
				LinkTargetNodePrototypeChooser: t.chooser,
			},
			Budget: t.budget,
		}.WalkAdv(nd, sel, t.visitor)
		t.writeDone(err)
	}()
}

// Shutdown cancels the traverser's context as passed to Start,
// and blocks until the traverser is fully stopped
// or until ctx is cancelled.
func (t *traverser) Shutdown(ctx context.Context) {
	t.cancel()
	select {
	case <-ctx.Done():
	case <-t.stopped:
	}
}

func (t *traverser) IsComplete() (bool, error) {
	// If the state is currently held due to an ongoing block load,
	// block until it's finished or until the traverser stops,
	// which then enables us to read the fields directly.
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.isDone, t.completionErr
}

func (t *traverser) CurrentRequest() (ipld.Link, ipld.LinkContext) {
	// Just like IsComplete.
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.currentLink, t.currentContext
}

func (t *traverser) Advance(reader io.Reader) error {
	// Just like IsComplete, block until we're ready to load another block.
	// We leave it to the other goroutine to unlock the mutex,
	// once the next StorageReadOpener call comes in or the traversal is done.
	t.stateMu.Lock()

	if t.isDone {
		// The other goroutine won't unlock, so we have to unlock.
		t.stateMu.Unlock()
		return errors.New("cannot advance when done")
	}

	select {
	case <-t.ctx.Done():
		// The other goroutine won't unlock, so we have to unlock.
		t.stateMu.Unlock()
		return ContextCancelError{}
	case t.responses <- nextResponse{input: reader}:
	}

	t.blocksCount++
	return nil
}

func (t *traverser) Error(err error) {
	// Just like Advance.
	t.stateMu.Lock()

	if t.isDone {
		// The other goroutine won't unlock, so we have to unlock.
		t.stateMu.Unlock()
		return
	}

	select {
	case <-t.ctx.Done():
		// The other goroutine won't unlock, so we have to unlock.
		t.stateMu.Unlock()
	case t.responses <- nextResponse{err: err}:
	}
}
