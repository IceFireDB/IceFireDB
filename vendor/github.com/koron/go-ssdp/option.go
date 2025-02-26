package ssdp

import "github.com/koron/go-ssdp/internal/multicast"

type config struct {
	multicastConfig
	advertiseConfig
}

func opts2config(opts []Option) (cfg config, err error) {
	for _, o := range opts {
		err := o.apply(&cfg)
		if err != nil {
			return config{}, err
		}
	}
	return cfg, nil
}

type multicastConfig struct {
	ttl   int
	sysIf bool
}

func (mc multicastConfig) options() (opts []multicast.ConnOption) {
	if mc.ttl > 0 {
		opts = append(opts, multicast.ConnTTL(mc.ttl))
	}
	if mc.sysIf {
		opts = append(opts, multicast.ConnSystemAssginedInterface())
	}
	return opts
}

type advertiseConfig struct {
	addHost bool
}

// Option is option set for SSDP API.
type Option interface {
	apply(c *config) error
}

type optionFunc func(*config) error

func (of optionFunc) apply(c *config) error {
	return of(c)
}

// TTL returns as Option that set TTL for multicast packets.
func TTL(ttl int) Option {
	return optionFunc(func(c *config) error {
		c.ttl = ttl
		return nil
	})
}

// OnlySystemInterface returns as Option that using only a system assigned
// multicast interface.
func OnlySystemInterface() Option {
	return optionFunc(func(c *config) error {
		c.sysIf = true
		return nil
	})
}

// AdvertiseHost returns as Option that add HOST header to response for
// M-SEARCH requests.
// This option works with Advertise() function only.
// This is added to support SmartThings.
// See https://github.com/koron/go-ssdp/issues/30 for details.
func AdvertiseHost() Option {
	return optionFunc(func(c *config) error {
		c.addHost = true
		return nil
	})
}
