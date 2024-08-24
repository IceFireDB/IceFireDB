package autorelay

import (
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

type config struct {
	peerChan     <-chan peer.AddrInfo
	staticRelays []peer.AddrInfo
	// see WithMinCandidates
	minCandidates int
	// see WithMaxCandidates
	maxCandidates int
	// Delay until we obtain reservations with relays, if we have less than minCandidates candidates.
	// See WithBootDelay.
	bootDelay time.Duration
	// backoff is the time we wait after failing to obtain a reservation with a candidate
	backoff time.Duration
	// If we fail to obtain a reservation more than maxAttempts, we stop trying.
	maxAttempts int
	// Number of relays we strive to obtain a reservation with.
	desiredRelays    int
	setMinCandidates bool
	enableCircuitV1  bool
}

var defaultConfig = config{
	minCandidates: 4,
	maxCandidates: 20,
	bootDelay:     3 * time.Minute,
	backoff:       time.Hour,
	maxAttempts:   3,
	desiredRelays: 2,
}

var errStaticRelaysMinCandidates = errors.New("cannot use WithMinCandidates and WithStaticRelays")

// DefaultRelays are the known PL-operated v1 relays; will be decommissioned in 2022.
var DefaultRelays = []string{
	"/ip4/147.75.80.110/tcp/4001/p2p/QmbFgm5zan8P6eWWmeyfncR5feYEMPbht5b1FW1C37aQ7y",
	"/ip4/147.75.80.110/udp/4001/quic/p2p/QmbFgm5zan8P6eWWmeyfncR5feYEMPbht5b1FW1C37aQ7y",
	"/ip4/147.75.195.153/tcp/4001/p2p/QmW9m57aiBDHAkKj9nmFSEn7ZqrcF1fZS4bipsTCHburei",
	"/ip4/147.75.195.153/udp/4001/quic/p2p/QmW9m57aiBDHAkKj9nmFSEn7ZqrcF1fZS4bipsTCHburei",
	"/ip4/147.75.70.221/tcp/4001/p2p/Qme8g49gm3q4Acp7xWBKg3nAa9fxZ1YmyDJdyGgoG6LsXh",
	"/ip4/147.75.70.221/udp/4001/quic/p2p/Qme8g49gm3q4Acp7xWBKg3nAa9fxZ1YmyDJdyGgoG6LsXh",
}

var defaultStaticRelays []peer.AddrInfo

func init() {
	for _, s := range DefaultRelays {
		pi, err := peer.AddrInfoFromString(s)
		if err != nil {
			panic(fmt.Sprintf("failed to initialize default static relays: %s", err))
		}
		defaultStaticRelays = append(defaultStaticRelays, *pi)
	}
}

type Option func(*config) error

func WithStaticRelays(static []peer.AddrInfo) Option {
	return func(c *config) error {
		if c.setMinCandidates {
			return errStaticRelaysMinCandidates
		}
		if len(c.staticRelays) > 0 {
			return errors.New("can't set static relays, static relays already configured")
		}
		c.minCandidates = len(static)
		c.staticRelays = static
		return nil
	}
}

func WithDefaultStaticRelays() Option {
	return WithStaticRelays(defaultStaticRelays)
}

func WithPeerSource(peerChan <-chan peer.AddrInfo) Option {
	return func(c *config) error {
		c.peerChan = peerChan
		return nil
	}
}

// WithNumRelays sets the number of relays we strive to obtain reservations with.
func WithNumRelays(n int) Option {
	return func(c *config) error {
		c.desiredRelays = n
		return nil
	}
}

// WithMaxCandidates sets the number of relay candidates that we buffer.
func WithMaxCandidates(n int) Option {
	return func(c *config) error {
		c.maxCandidates = n
		return nil
	}
}

// WithMinCandidates sets the minimum number of relay candidates we collect before to get a reservation
// with any of them (unless we've been running for longer than the boot delay).
// This is to make sure that we don't just randomly connect to the first candidate that we discover.
func WithMinCandidates(n int) Option {
	return func(c *config) error {
		if len(c.staticRelays) > 0 {
			return errStaticRelaysMinCandidates
		}
		c.minCandidates = n
		c.setMinCandidates = true
		return nil
	}
}

// WithBootDelay set the boot delay for finding relays.
// We won't attempt any reservation if we've have less than a minimum number of candidates.
// This prevents us to connect to the "first best" relay, and allows us to carefully select the relay.
// However, in case we haven't found enough relays after the boot delay, we use what we have.
func WithBootDelay(d time.Duration) Option {
	return func(c *config) error {
		c.bootDelay = d
		return nil
	}
}

// WithBackoff sets the time we wait after failing to obtain a reservation with a candidate.
func WithBackoff(d time.Duration) Option {
	return func(c *config) error {
		c.backoff = d
		return nil
	}
}

// WithMaxAttempts sets the number of times we attempt to obtain a reservation with a candidate.
// If we still fail to obtain a reservation, this candidate is dropped.
func WithMaxAttempts(n int) Option {
	return func(c *config) error {
		c.maxAttempts = n
		return nil
	}
}

// WithCircuitV1Support enables support for circuit v1 relays.
func WithCircuitV1Support() Option {
	return func(c *config) error {
		c.enableCircuitV1 = true
		return nil
	}
}
