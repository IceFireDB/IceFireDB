package mfs

import (
	"context"
	"errors"
	"sync"
	"time"

	cid "github.com/ipfs/go-cid"
)

// closeTimeout is how long to wait for current publishing to finish before
// shutting down the republisher.
const closeTimeout = 5 * time.Second

// PubFunc is the user-defined function that determines exactly what
// logic entails "publishing" a `Cid` value.
type PubFunc func(context.Context, cid.Cid) error

// Republisher manages when to publish a given entry.
type Republisher struct {
	pubfunc          PubFunc
	update           chan cid.Cid
	immediatePublish chan chan struct{}

	cancel    func()
	closeOnce sync.Once
	stopped   chan struct{}
}

// NewRepublisher creates a new Republisher object to republish the given root
// using the given short and long time intervals.
func NewRepublisher(pf PubFunc, tshort, tlong time.Duration, lastPublished cid.Cid) *Republisher {
	ctx, cancel := context.WithCancel(context.Background())
	rp := &Republisher{
		update:           make(chan cid.Cid, 1),
		pubfunc:          pf,
		immediatePublish: make(chan chan struct{}),
		cancel:           cancel,
		stopped:          make(chan struct{}),
	}

	go rp.run(ctx, tshort, tlong, lastPublished)

	return rp
}

// WaitPub waits for the current value to be published (or returns early
// if it already has).
func (rp *Republisher) WaitPub(ctx context.Context) error {
	wait := make(chan struct{})
	select {
	case rp.immediatePublish <- wait:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close tells the republisher to stop and waits for it to stop.
func (rp *Republisher) Close() error {
	var err error
	rp.closeOnce.Do(func() {
		// Wait a short amount of time for any current publishing to finish.
		ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
		err = rp.WaitPub(ctx)
		if errors.Is(err, context.DeadlineExceeded) {
			err = errors.New("mfs/republisher: timed out waiting to publish during close")
		}
		cancel()
		// Shutdown the publisher.
		rp.cancel()
	})
	// Wait for publisher to stop and then return.
	<-rp.stopped
	return err
}

// Update the current value. The value will be published after a delay but each
// consecutive call to Update may extend this delay up to TimeoutLong.
func (rp *Republisher) Update(c cid.Cid) {
	select {
	case <-rp.update:
		select {
		case rp.update <- c:
		default:
			// Don't try again. If we hit this case, there's a
			// concurrent publish and we can safely let that
			// concurrent publish win.
		}
	case rp.update <- c:
	}
}

// Run contains the core logic of the `Republisher`. It calls the user-defined
// `pubfunc` function whenever the `Cid` value is updated to a *new* value.
// Since calling the `pubfunc` may be slow, updates are batched
//
// Algorithm:
//  1. When receiving the first update after publishing, set a `longer` timer
//  2. When receiving any update, reset the `quick` timer
//  3. If either the `quick` timeout or the `longer` timeout elapses, call
//     `publish` with the latest updated value.
//
// The `longer` timer ensures that publishing is delayed by at most that
// duration. The `quick` timer allows publishing sooner if there are no more
// updates available.
//
// In other words, the quick timeout means there are no more values to put into
// the "batch", so do update. The long timeout means there are that the "batch"
// is full, so do update, even though there are still values (no quick timeout
// yet) arriving.
//
// If a publish fails, retry repeatedly every `longer` timeout.
func (rp *Republisher) run(ctx context.Context, timeoutShort, timeoutLong time.Duration, lastPublished cid.Cid) {
	defer close(rp.stopped)

	quick := time.NewTimer(0)
	if !quick.Stop() {
		<-quick.C
	}
	longer := time.NewTimer(0)
	if !longer.Stop() {
		<-longer.C
	}

	immediatePublish := rp.immediatePublish
	var toPublish cid.Cid
	var waiter chan struct{}

	for {
		select {
		case <-ctx.Done():
			return
		case newValue := <-rp.update:
			// Skip already published values.
			if lastPublished.Equals(newValue) {
				// Break to the end of the switch to cleanup any
				// timers.
				toPublish = cid.Undef
				break
			}

			// If not already waiting to publish something, reset the long
			// timeout.
			if !toPublish.Defined() {
				longer.Reset(timeoutLong)
			}

			// Always reset the short timeout.
			quick.Reset(timeoutShort)

			// Finally, set the new value to publish.
			toPublish = newValue
			// Wait for a newer value or the quick timer.
			continue
		case waiter = <-immediatePublish:
			// Make sure to grab the *latest* value to publish.
			select {
			case toPublish = <-rp.update:
			default:
			}

			// Avoid publishing duplicate values
			if lastPublished.Equals(toPublish) {
				toPublish = cid.Undef
			}
		case <-quick.C:
			// Waited a short time for more updates and no more received.
		case <-longer.C:
			// Keep getting updates and now it is time to send what has been
			// received so far.
		}

		// Cleanup, publish, and close waiters.

		// 1. Stop any timers.
		quick.Stop()
		longer.Stop()

		// Do not use the `if !t.Stop() { ... }` idiom as these timers may not
		// be running.
		//
		// TODO: remove after go1.23 required.
		select {
		case <-quick.C:
		default:
		}
		select {
		case <-longer.C:
		default:
		}

		// 2. If there is a value to publish then publish it now.
		if toPublish.Defined() {
			err := rp.pubfunc(ctx, toPublish)
			if err != nil {
				// Republish failed, so retry after waiting for long timeout.
				//
				// Instead of entering a retry loop here, go back to waiting
				// for more values and retrying to publish after the lomg
				// timeout. Keep using the current waiter until it has been
				// notified of a successful publish.
				//
				// Reset the long timer as it effectively becomes the retry
				// timeout.
				longer.Reset(timeoutLong)
				// Stop reading waiters from immediatePublish while retrying,
				// This causes the current waiter to be notified only after a
				// successful call to pubfunc, and is what constitutes a retry.
				immediatePublish = nil
				continue
			}
			lastPublished = toPublish
			toPublish = cid.Undef
			// Resume reading waiters,
			immediatePublish = rp.immediatePublish
		}

		// 3. Notify anything waiting in `WaitPub` on successful call to
		// pubfunc or if nothing to publish.
		if waiter != nil {
			close(waiter)
			waiter = nil
		}
	}
}
