// Package connectivity provides thread-safe network connectivity checking with
// state management and callback notifications for online/offline transitions.
package connectivity

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	initialBackoffDelay = 100 * time.Millisecond
	maxBackoffDelay     = time.Minute
)

// ConnectivityChecker provides a thread-safe way to verify the connectivity of
// a node, and triggers wake-up callbacks when the node changes connectivity
// state. The `checkFunc` callback used to verify network connectivity is user
// supplied.
//
// State Machine starting when `Start()` is called:
//  1. OFFLINE state:
//     - Calls `checkFunc` with exponential backoff until node is found ONLINE.
//     - Calls to `TriggerCheck()` are ignored while OFFLINE.
//     - When `checkFunc` returns true, state changes to ONLINE and
//     `onOnline()` callback is called.
//  2. ONLINE state:
//     - Calls to `TriggerCheck()` will call `checkFunc` only if at least
//     `onlineCheckInterval` has passed since the last check.
//     - If `TriggerCheck()` returns false, switch state to DISCONNECTED.
//  3. DISCONNECTED state:
//     - Calls `checkFunc` with exponential backoff until node is found ONLINE.
//     - Calls to `TriggerCheck()` are ignored while DISCONNECTED.
//     - When `checkFunc` returns true, state changes to ONLINE and
//     `onOnline()` callback is called.
//     - After `offlineDelay` has passed in DISCONNECTED state, state changes
//     to OFFLINE and `onOffline()` callback is called.
//
// The State Machine starts in OFFLINE state by default, and in DISCONNECTED
// state if WithStartDisconnected Option is used.
type ConnectivityChecker struct {
	done      chan struct{}
	closed    bool
	closeOnce sync.Once
	mutex     sync.Mutex

	online            atomic.Bool
	startDisconnected bool

	lastCheck           time.Time
	onlineCheckInterval time.Duration // minimum check interval when online
	lastStateChange     time.Time
	lastStateChangeLk   sync.Mutex

	checkFunc func() bool // function to check whether node is online

	onOnline       func()
	onDisconnected func()
	onOffline      func()
	offlineDelay   time.Duration
}

// New creates a new ConnectivityChecker instance.
func New(checkFunc func() bool, opts ...Option) (*ConnectivityChecker, error) {
	cfg, err := getOpts(opts)
	if err != nil {
		return nil, err
	}
	c := &ConnectivityChecker{
		done:                make(chan struct{}),
		checkFunc:           checkFunc,
		startDisconnected:   cfg.startDisconnected,
		onlineCheckInterval: cfg.onlineCheckInterval,
		onOnline:            cfg.onOnline,
		onDisconnected:      cfg.onDisconnected,
		onOffline:           cfg.onOffline,
		offlineDelay:        cfg.offlineDelay,
	}
	return c, nil
}

// SetCallbacks sets the onOnline and onOffline callbacks after construction.
// This allows breaking circular dependencies during initialization.
//
// SetCallbacks must be called before Start().
func (c *ConnectivityChecker) SetCallbacks(onOnline, onDisconnected, onOffline func()) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return
	}
	c.onOnline = onOnline
	c.onDisconnected = onDisconnected
	c.onOffline = onOffline
}

// Start the ConnectivityChecker in Offline or Disconnected state, by begining
// connectivity probes, until the node is found Online.
//
// If SetCallbacks() is used, Start() must be called after SetCallbacks().
func (c *ConnectivityChecker) Start() {
	c.mutex.Lock()
	// Start probing until the node comes online
	go func() {
		defer c.mutex.Unlock()

		if c.probe() {
			// Node is already online
			return
		}
		// Wait for node to come online
		c.probeLoop(c.startDisconnected)
	}()
}

// Close stops any running connectivity checks and prevents future ones.
func (c *ConnectivityChecker) Close() error {
	c.closeOnce.Do(func() {
		close(c.done)
		c.mutex.Lock()
		c.closed = true
		c.mutex.Unlock()
	})
	return nil
}

// IsOnline returns true if the node is currently online, false otherwise.
func (c *ConnectivityChecker) IsOnline() bool {
	return c.online.Load()
}

// LastStateChange returns the timestamp of the last state change.
func (c *ConnectivityChecker) LastStateChange() time.Time {
	c.lastStateChangeLk.Lock()
	defer c.lastStateChangeLk.Unlock()
	return c.lastStateChange
}

// stateChanged should be called whenever the connectivity state changes.
func (c *ConnectivityChecker) stateChanged() {
	c.lastStateChangeLk.Lock()
	defer c.lastStateChangeLk.Unlock()
	c.lastStateChange = time.Now()
}

// TriggerCheck triggers an asynchronous connectivity check.
//
// * If a check is already running, does nothing.
// * If a check was already performed within the last `onlineCheckInterval`, does nothing.
// * If after running the check the node is still online, update the last check timestamp.
// * If the node is found offline, enter the loop:
//   - Perform connectivity check every `offlineCheckInterval`.
//   - Exit if context is cancelled, or ConnectivityChecker is closed.
//   - When node is found back online, run the `backOnlineNotify` callback.
func (c *ConnectivityChecker) TriggerCheck() {
	if !c.mutex.TryLock() {
		return // check already in progress
	}
	if c.closed {
		c.mutex.Unlock()
		return
	}
	if c.online.Load() && time.Since(c.lastCheck) < c.onlineCheckInterval {
		c.mutex.Unlock()
		return // last check was too recent
	}

	go func() {
		defer c.mutex.Unlock()

		if c.checkFunc() {
			c.lastCheck = time.Now()
			return
		}

		// Online -> Disconnected
		c.stateChanged()
		c.online.Store(false)
		if c.onDisconnected != nil {
			c.onDisconnected()
		}

		// Start periodic checks until node comes back Online
		c.probeLoop(true)
	}()
}

// probeLoop runs connectivity probes with exponential backoff until the node
// comes back Online, or the ConnectivityChecker is closed.
func (c *ConnectivityChecker) probeLoop(disconnected bool) {
	var offlineC <-chan time.Time
	if disconnected {
		if c.offlineDelay == 0 {
			// Online -> Offline
			c.stateChanged()
			if c.onOffline != nil {
				c.onOffline()
			}
		} else {
			offlineTimer := time.NewTimer(c.offlineDelay)
			defer offlineTimer.Stop()
			offlineC = offlineTimer.C
		}
	}

	delay := initialBackoffDelay
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-timer.C:
			if c.probe() {
				return
			}
			delay = min(2*delay, maxBackoffDelay)
			timer.Reset(delay)
		case <-offlineC:
			// Disconnected -> Offline
			c.stateChanged()
			if c.onOffline != nil {
				c.onOffline()
			}
		}
	}
}

// probe runs the connectivity check function once, and if the node is found
// Online, updates the state and runs the onOnline callback.
func (c *ConnectivityChecker) probe() bool {
	if c.checkFunc() {
		select {
		case <-c.done:
		default:
			// Node is back Online.
			c.online.Store(true)

			c.lastCheck = time.Now()
			c.stateChanged()
			if c.onOnline != nil {
				c.onOnline()
			}
		}
		return true
	}
	return false
}
