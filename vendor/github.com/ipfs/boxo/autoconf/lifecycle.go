package autoconf

import (
	"context"
	"fmt"
	"time"
)

// Start primes the cache with the latest config and starts a background updater.
// It returns the primed config for immediate use.
// The updater can be stopped either by cancelling the context or calling Stop().
// If no callbacks were configured via WithOnNewVersion, WithOnRefresh, or WithOnRefreshError,
// default callbacks will be used for logging.
// Returns an error if the updater is already running.
func (c *Client) Start(ctx context.Context) (*Config, error) {
	// Prime cache first with a reasonable timeout
	timeout := c.httpClient.Timeout
	if timeout == 0 {
		// Client has no timeout (infinite), use a sensible default for priming
		timeout = DefaultTimeout
	}
	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	config := c.GetCachedOrRefresh(ctxWithTimeout)
	cancel()

	// Lock to ensure thread-safe updater lifecycle management
	c.updaterMu.Lock()
	defer c.updaterMu.Unlock()

	// Check if updater is already running
	if c.updater != nil {
		return config, fmt.Errorf("background updater is already running")
	}

	// Set default callbacks if not configured (protected by mutex)
	if c.onNewVersion == nil {
		c.onNewVersion = func(oldVersion, newVersion int64, configURL string) {
			log.Errorf("new autoconf version %d published at %s - restart to apply updates", newVersion, configURL)
		}
	}
	if c.onRefresh == nil {
		c.onRefresh = func(resp *Response) {
			log.Debugf("refreshed autoconf metadata: version %s, fetch time %s", resp.Version, resp.FetchTime.Format(time.RFC3339))
		}
	}
	if c.onRefreshError == nil {
		c.onRefreshError = func(err error) {
			log.Errorf("autoconf refresh error: %v", err)
		}
	}

	// Create background updater using client's callbacks
	updater := newBackgroundUpdater(c)

	// Start the updater - it will automatically stop when context is cancelled
	if err := updater.Start(ctx); err != nil {
		return config, fmt.Errorf("failed to start background updater: %w", err)
	}

	// Store updater reference for Stop() method
	c.updater = updater

	// Log which URLs we're checking
	if len(c.urls) == 1 {
		log.Infof("Started autoconf background updater checking %s every %s", c.urls[0], c.refreshInterval)
	} else {
		log.Infof("Started autoconf background updater checking %d URLs (load-balanced) every %s", len(c.urls), c.refreshInterval)
	}
	return config, nil
}

// Stop gracefully stops the background updater if it's running.
// This is an alternative to cancelling the context passed to Start().
// It's safe to call Stop() multiple times.
func (c *Client) Stop() {
	c.updaterMu.Lock()
	defer c.updaterMu.Unlock()

	if c.updater != nil {
		c.updater.Stop()
		c.updater = nil
		log.Infof("Stopped autoconf background updater")
	}
}
