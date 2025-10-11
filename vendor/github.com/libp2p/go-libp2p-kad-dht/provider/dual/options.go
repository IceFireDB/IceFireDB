package dual

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/dual"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/provider"
	"github.com/libp2p/go-libp2p-kad-dht/provider/keystore"
)

const (
	lanID uint8 = iota
	wanID
)

type config struct {
	keystore keystore.Keystore

	reprovideInterval [2]time.Duration // [0] = LAN, [1] = WAN
	maxReprovideDelay [2]time.Duration

	offlineDelay                     [2]time.Duration
	connectivityCheckOnlineInterval  [2]time.Duration
	connectivityCheckOfflineInterval [2]time.Duration

	maxWorkers               [2]int
	dedicatedPeriodicWorkers [2]int
	dedicatedBurstWorkers    [2]int
	maxProvideConnsPerWorker [2]int

	msgSenders [2]pb.MessageSender
}

type Option func(opt *config) error

// getOpts creates a config and applies Options to it.
func getOpts(opts []Option, d *dual.DHT) (config, error) {
	cfg := config{
		reprovideInterval: [2]time.Duration{amino.DefaultReprovideInterval, amino.DefaultReprovideInterval},
		maxReprovideDelay: [2]time.Duration{provider.DefaultMaxReprovideDelay, provider.DefaultMaxReprovideDelay},

		offlineDelay:                    [2]time.Duration{provider.DefaultOfflineDelay, provider.DefaultOfflineDelay},
		connectivityCheckOnlineInterval: [2]time.Duration{provider.DefaultConnectivityCheckOnlineInterval, provider.DefaultConnectivityCheckOnlineInterval},

		maxWorkers:               [2]int{4, 4},
		dedicatedPeriodicWorkers: [2]int{2, 2},
		dedicatedBurstWorkers:    [2]int{1, 1},
		maxProvideConnsPerWorker: [2]int{20, 20},
	}

	// Apply options
	for i, opt := range opts {
		if err := opt(&cfg); err != nil {
			return config{}, fmt.Errorf("dual dht provider option %d failed: %w", i, err)
		}
	}

	// Resolve defaults
	if cfg.msgSenders[lanID] == nil {
		cfg.msgSenders[lanID] = d.LAN.MessageSender()
	}
	if cfg.msgSenders[wanID] == nil {
		cfg.msgSenders[wanID] = d.WAN.MessageSender()
	}

	// Validate config
	if cfg.dedicatedPeriodicWorkers[lanID]+cfg.dedicatedBurstWorkers[lanID] > cfg.maxWorkers[lanID] {
		return config{}, errors.New("provider config: total dedicated workers exceed max workers")
	}
	if cfg.dedicatedPeriodicWorkers[wanID]+cfg.dedicatedBurstWorkers[wanID] > cfg.maxWorkers[wanID] {
		return config{}, errors.New("provider config: total dedicated workers exceed max workers")
	}
	return cfg, nil
}

func WithKeystore(ks keystore.Keystore) Option {
	return func(cfg *config) error {
		if ks == nil {
			return errors.New("provider config: keystore cannot be nil")
		}
		cfg.keystore = ks
		return nil
	}
}

func withReprovideInterval(reprovideInterval time.Duration, dhts ...uint8) Option {
	return func(cfg *config) error {
		if reprovideInterval <= 0 {
			return fmt.Errorf("reprovide interval must be positive, got %s", reprovideInterval)
		}
		for _, dht := range dhts {
			cfg.reprovideInterval[dht] = reprovideInterval
		}
		return nil
	}
}

func WithReprovideInterval(reprovideInterval time.Duration) Option {
	return withReprovideInterval(reprovideInterval, lanID, wanID)
}

func WithReprovideIntervalLAN(reprovideInterval time.Duration) Option {
	return withReprovideInterval(reprovideInterval, lanID)
}

func WithReprovideIntervalWAN(reprovideInterval time.Duration) Option {
	return withReprovideInterval(reprovideInterval, wanID)
}

func withMaxReprovideDelay(maxReprovideDelay time.Duration, dhts ...uint8) Option {
	return func(cfg *config) error {
		if maxReprovideDelay <= 0 {
			return fmt.Errorf("max reprovide delay must be positive, got %s", maxReprovideDelay)
		}
		for _, dht := range dhts {
			cfg.maxReprovideDelay[dht] = maxReprovideDelay
		}
		return nil
	}
}

func WithMaxReprovideDelay(maxReprovideDelay time.Duration) Option {
	return withMaxReprovideDelay(maxReprovideDelay, lanID, wanID)
}

func WithMaxReprovideDelayLAN(maxReprovideDelay time.Duration) Option {
	return withMaxReprovideDelay(maxReprovideDelay, lanID)
}

func WithMaxReprovideDelayWAN(maxReprovideDelay time.Duration) Option {
	return withMaxReprovideDelay(maxReprovideDelay, wanID)
}

func withOfflineDelay(offlineDelay time.Duration, dhts ...uint8) Option {
	return func(cfg *config) error {
		if offlineDelay < 0 {
			return fmt.Errorf("invalid offline delay %s", offlineDelay)
		}
		for _, dht := range dhts {
			cfg.offlineDelay[dht] = offlineDelay
		}
		return nil
	}
}

func WithOfflineDelay(offlineDelay time.Duration) Option {
	return withOfflineDelay(offlineDelay, lanID, wanID)
}

func WithOfflineDelayLAN(offlineDelay time.Duration) Option {
	return withOfflineDelay(offlineDelay, lanID)
}

func WithOfflineDelayWAN(offlineDelay time.Duration) Option {
	return withOfflineDelay(offlineDelay, wanID)
}

func withConnectivityCheckOnlineInterval(onlineInterval time.Duration, dhts ...uint8) Option {
	return func(cfg *config) error {
		if onlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check online interval %s", onlineInterval)
		}
		for _, dht := range dhts {
			cfg.connectivityCheckOnlineInterval[dht] = onlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOnlineInterval(onlineInterval time.Duration) Option {
	return withConnectivityCheckOnlineInterval(onlineInterval, lanID, wanID)
}

func WithConnectivityCheckOnlineIntervalLAN(onlineInterval time.Duration) Option {
	return withConnectivityCheckOnlineInterval(onlineInterval, lanID)
}

func WithConnectivityCheckOnlineIntervalWAN(onlineInterval time.Duration) Option {
	return withConnectivityCheckOnlineInterval(onlineInterval, wanID)
}

func withConnectivityCheckOfflineInterval(offlineInterval time.Duration, dhts ...uint8) Option {
	return func(cfg *config) error {
		if offlineInterval <= 0 {
			return fmt.Errorf("invalid connectivity check offline interval %s", offlineInterval)
		}
		for _, dht := range dhts {
			cfg.connectivityCheckOfflineInterval[dht] = offlineInterval
		}
		return nil
	}
}

func WithConnectivityCheckOfflineInterval(offlineInterval time.Duration) Option {
	return withConnectivityCheckOfflineInterval(offlineInterval, lanID, wanID)
}

func WithConnectivityCheckOfflineIntervalLAN(offlineInterval time.Duration) Option {
	return withConnectivityCheckOfflineInterval(offlineInterval, lanID)
}

func WithConnectivityCheckOfflineIntervalWAN(offlineInterval time.Duration) Option {
	return withConnectivityCheckOfflineInterval(offlineInterval, wanID)
}

func withMaxWorkers(maxWorkers int, dhts ...uint8) Option {
	return func(cfg *config) error {
		if maxWorkers <= 0 {
			return fmt.Errorf("invalid max workers %d", maxWorkers)
		}
		for _, dht := range dhts {
			cfg.maxWorkers[dht] = maxWorkers
		}
		return nil
	}
}

func WithMaxWorkers(maxWorkers int) Option {
	return withMaxWorkers(maxWorkers, lanID, wanID)
}

func WithMaxWorkersLAN(maxWorkers int) Option {
	return withMaxWorkers(maxWorkers, lanID)
}

func WithMaxWorkersWAN(maxWorkers int) Option {
	return withMaxWorkers(maxWorkers, wanID)
}

func withDedicatedPeriodicWorkers(dedicatedPeriodicWorkers int, dhts ...uint8) Option {
	return func(cfg *config) error {
		if dedicatedPeriodicWorkers < 0 {
			return fmt.Errorf("invalid dedicated periodic workers %d", dedicatedPeriodicWorkers)
		}
		for _, dht := range dhts {
			cfg.dedicatedPeriodicWorkers[dht] = dedicatedPeriodicWorkers
		}
		return nil
	}
}

func WithDedicatedPeriodicWorkers(dedicatedPeriodicWorkers int) Option {
	return withDedicatedPeriodicWorkers(dedicatedPeriodicWorkers, lanID, wanID)
}

func WithDedicatedPeriodicWorkersLAN(dedicatedPeriodicWorkers int) Option {
	return withDedicatedPeriodicWorkers(dedicatedPeriodicWorkers, lanID)
}

func WithDedicatedPeriodicWorkersWAN(dedicatedPeriodicWorkers int) Option {
	return withDedicatedPeriodicWorkers(dedicatedPeriodicWorkers, wanID)
}

func withDedicatedBurstWorkers(dedicatedBurstWorkers int, dhts ...uint8) Option {
	return func(cfg *config) error {
		if dedicatedBurstWorkers < 0 {
			return fmt.Errorf("invalid dedicated burst workers %d", dedicatedBurstWorkers)
		}
		for _, dht := range dhts {
			cfg.dedicatedBurstWorkers[dht] = dedicatedBurstWorkers
		}
		return nil
	}
}

func WithDedicatedBurstWorkers(dedicatedBurstWorkers int) Option {
	return withDedicatedBurstWorkers(dedicatedBurstWorkers, lanID, wanID)
}

func WithDedicatedBurstWorkersLAN(dedicatedBurstWorkers int) Option {
	return withDedicatedBurstWorkers(dedicatedBurstWorkers, lanID)
}

func WithDedicatedBurstWorkersWAN(dedicatedBurstWorkers int) Option {
	return withDedicatedBurstWorkers(dedicatedBurstWorkers, wanID)
}

func withMaxProvideConnsPerWorker(maxProvideConnsPerWorker int, dhts ...uint8) Option {
	return func(cfg *config) error {
		if maxProvideConnsPerWorker <= 0 {
			return fmt.Errorf("invalid max provide conns per worker %d", maxProvideConnsPerWorker)
		}
		for _, dht := range dhts {
			cfg.maxProvideConnsPerWorker[dht] = maxProvideConnsPerWorker
		}
		return nil
	}
}

func WithMaxProvideConnsPerWorker(maxProvideConnsPerWorker int) Option {
	return withMaxProvideConnsPerWorker(maxProvideConnsPerWorker, lanID, wanID)
}

func WithMaxProvideConnsPerWorkerLAN(maxProvideConnsPerWorker int) Option {
	return withMaxProvideConnsPerWorker(maxProvideConnsPerWorker, lanID)
}

func WithMaxProvideConnsPerWorkerWAN(maxProvideConnsPerWorker int) Option {
	return withMaxProvideConnsPerWorker(maxProvideConnsPerWorker, wanID)
}

func withMessageSender(msgSender pb.MessageSender, dhts ...uint8) Option {
	return func(cfg *config) error {
		if msgSender == nil {
			return errors.New("provider config: message sender cannot be nil")
		}
		for _, dht := range dhts {
			cfg.msgSenders[dht] = msgSender
		}
		return nil
	}
}

func WithMessageSenderLAN(msgSender pb.MessageSender) Option {
	return withMessageSender(msgSender, lanID)
}

func WithMessageSenderWAN(msgSender pb.MessageSender) Option {
	return withMessageSender(msgSender, wanID)
}
