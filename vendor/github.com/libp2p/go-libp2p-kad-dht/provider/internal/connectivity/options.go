package connectivity

import (
	"fmt"
	"time"
)

type config struct {
	onlineCheckInterval time.Duration // minimum check interval when online

	offlineDelay time.Duration

	onOffline func()
	onOnline  func()
}

type Option func(opt *config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option) (config, error) {
	cfg := config{
		onlineCheckInterval: 1 * time.Minute,
		offlineDelay:        2 * time.Hour,
	}

	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("connectivity option %d error: %s", i, err)
		}
	}
	return cfg, nil
}

// WithOnlineCheckInterval sets the minimum interval between online checks.
// This is used to throttle the number of connectivity checks when the node is
// online.
func WithOnlineCheckInterval(d time.Duration) Option {
	return func(cfg *config) error {
		if d <= 0 {
			return fmt.Errorf("online check interval must be positive, got %s", d)
		}
		cfg.onlineCheckInterval = d
		return nil
	}
}

func WithOfflineDelay(d time.Duration) Option {
	return func(cfg *config) error {
		if d < 0 {
			return fmt.Errorf("offline delay must be non-negative, got %s", d)
		}
		cfg.offlineDelay = d
		return nil
	}
}

func WithOnOffline(f func()) Option {
	return func(cfg *config) error {
		cfg.onOffline = f
		return nil
	}
}

func WithOnOnline(f func()) Option {
	return func(cfg *config) error {
		cfg.onOnline = f
		return nil
	}
}
