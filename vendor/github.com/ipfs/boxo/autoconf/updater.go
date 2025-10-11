package autoconf

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	// Backoff configuration constants for failed update retries
	backoffBaseInterval = time.Minute    // Base backoff interval (1 minute)
	backoffMaxInterval  = 24 * time.Hour // Maximum backoff interval (24 hours)
	backoffMaxMinutes   = 24 * 60        // Maximum backoff in minutes (1440 minutes = 24 hours)
)

// backgroundUpdater handles periodic autoconf refresh checks
type backgroundUpdater struct {
	client         *Client
	onNewVersion   func(oldVersion, newVersion int64, configURL string)
	onRefresh      func(*Response)
	onRefreshError func(error)

	// Internal state
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// newBackgroundUpdater creates a new background updater
func newBackgroundUpdater(client *Client) *backgroundUpdater {
	if client == nil {
		panic("autoconf: client cannot be nil")
	}

	return &backgroundUpdater{
		client:         client,
		onNewVersion:   client.onNewVersion,
		onRefresh:      client.onRefresh,
		onRefreshError: client.onRefreshError,
	}
}

// Start begins the background updater
func (u *backgroundUpdater) Start(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.started {
		return fmt.Errorf("background updater is already started")
	}

	u.ctx, u.cancel = context.WithCancel(ctx)
	u.started = true

	u.wg.Add(1)
	go u.runUpdater()

	log.Debugf("started autoconf background updater")
	return nil
}

// Stop gracefully stops the background updater
func (u *backgroundUpdater) Stop() {
	u.mu.Lock()
	defer u.mu.Unlock()

	if !u.started {
		return
	}

	u.cancel()
	u.wg.Wait()
	u.started = false

	log.Debug("stopped autoconf background updater")
}

// runUpdater is the main updater loop
func (u *backgroundUpdater) runUpdater() {
	defer u.wg.Done()

	ticker := time.NewTicker(u.client.refreshInterval)
	defer ticker.Stop()

	failureCount := 0

	for {
		select {
		case <-u.ctx.Done():
			log.Debug("autoconf updater shutting down: context cancelled")
			return
		case <-ticker.C:
			// Attempt to update autoconf
			err := u.performUpdate()
			if err != nil {
				failureCount++
				backoff := u.calculateBackoffDelay(failureCount)

				if u.onRefreshError != nil {
					u.onRefreshError(fmt.Errorf("autoconf background refresh failed (attempt %d): %w, retrying in %v", failureCount, err, backoff))
				}

				// Stop regular ticker and wait for backoff
				ticker.Stop()
				select {
				case <-u.ctx.Done():
					return
				case <-time.After(backoff):
					// Reset ticker after backoff
					ticker = time.NewTicker(u.client.refreshInterval)
				}
			} else {
				// Success - reset failure count
				if failureCount > 0 {
					log.Debugf("autoconf background refresh succeeded after retries")
					failureCount = 0
				} else {
					log.Debug("autoconf background refresh succeeded")
				}
			}
		}
	}
}

// performUpdate performs a single background autoconf refresh check
func (u *backgroundUpdater) performUpdate() error {
	log.Debug("background refresh check starting")

	// Get the current cached version before fetching
	cacheDir, cacheDirErr := u.client.getCacheDir()
	var oldVersion int64 = 0
	if cacheDirErr == nil {
		oldConfig, err := u.client.getCachedConfig(cacheDir)
		if err == nil && oldConfig != nil {
			oldVersion = oldConfig.AutoConfVersion
		}
	}

	// Get config with metadata, using the client's refresh interval
	resp, err := u.client.getLatest(u.ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch autoconf: %w", err)
	}

	// Check if we got a new version and notify via callback
	if !resp.FromCache() && resp.Config.AutoConfVersion != oldVersion {
		if oldVersion == 0 {
			log.Infof("fetched autoconf version %d", resp.Config.AutoConfVersion)
		} else {
			log.Infof("fetched autoconf version %d (updated from %d)", resp.Config.AutoConfVersion, oldVersion)
		}
		if u.onNewVersion != nil {
			// Pass the selected URL that was used for this fetch
			configURL := u.client.selectURL()
			u.onNewVersion(oldVersion, resp.Config.AutoConfVersion, configURL)
		}
	}

	// Notify refresh callback for metadata persistence
	if u.onRefresh != nil {
		u.onRefresh(resp)
	}

	return nil
}

// calculateBackoffDelay calculates exponential backoff delay capped at 24 hours
func (u *backgroundUpdater) calculateBackoffDelay(failureCount int) time.Duration {
	// Start with 1 minute, double each time: 1m, 2m, 4m, 8m, 16m, 32m, 1h4m, 2h8m, 4h16m, 8h32m, 17h4m
	// Cap at 24 hours
	if failureCount <= 0 {
		return backoffBaseInterval
	}

	// Calculate exponential backoff: 1 << failureCount minutes
	backoffMinutes := min(1<<failureCount, backoffMaxMinutes)

	return time.Duration(backoffMinutes) * backoffBaseInterval
}
