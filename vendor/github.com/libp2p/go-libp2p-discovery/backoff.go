package discovery

import (
	"math/rand"
	"time"

	dbackoff "github.com/libp2p/go-libp2p/p2p/discovery/backoff"
)

// Deprecated: use go-libp2p/p2p/discovery/backoff.BackoffFactory instead.
type BackoffFactory = dbackoff.BackoffFactory

// BackoffStrategy describes how backoff will be implemented. BackoffStratgies are stateful.
// Deprecated: use go-libp2p/p2p/discovery/backoff.BackoffStrategy instead.
type BackoffStrategy = dbackoff.BackoffStrategy

// Jitter implementations taken roughly from https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

// Jitter must return a duration between min and max. Min must be lower than, or equal to, max.
// Deprecated: use go-libp2p/p2p/discovery/backoff.Jitter instead.
type Jitter = dbackoff.Jitter

// FullJitter returns a random number uniformly chose from the range [min, boundedDur].
// boundedDur is the duration bounded between min and max.
// Deprecated: use go-libp2p/p2p/discovery/backoff.FullJitter instead.
func FullJitter(duration, min, max time.Duration, rng *rand.Rand) time.Duration {
	return dbackoff.FullJitter(duration, min, max, rng)
}

// NoJitter returns the duration bounded between min and max
// Deprecated: use go-libp2p/p2p/discovery/backoff.NoJitter instead.
func NoJitter(duration, min, max time.Duration, rng *rand.Rand) time.Duration {
	return dbackoff.NoJitter(duration, min, max, rng)
}

// NewFixedBackoff creates a BackoffFactory with a constant backoff duration
// Deprecated: use go-libp2p/p2p/discovery/backoff.NewFixedBackoff instead.
func NewFixedBackoff(delay time.Duration) BackoffFactory {
	return dbackoff.NewFixedBackoff(delay)
}

// NewPolynomialBackoff creates a BackoffFactory with backoff of the form c0*x^0, c1*x^1, ...cn*x^n where x is the attempt number
// jitter is the function for adding randomness around the backoff
// timeUnits are the units of time the polynomial is evaluated in
// polyCoefs is the array of polynomial coefficients from [c0, c1, ... cn]
// Deprecated: use go-libp2p/p2p/discovery/backoff.NewPolynomialBackoff instead.
func NewPolynomialBackoff(min, max time.Duration, jitter Jitter,
	timeUnits time.Duration, polyCoefs []float64, rngSrc rand.Source) BackoffFactory {
	return dbackoff.NewPolynomialBackoff(min, max, jitter, timeUnits, polyCoefs, rngSrc)
}

// NewExponentialBackoff creates a BackoffFactory with backoff of the form base^x + offset where x is the attempt number
// jitter is the function for adding randomness around the backoff
// timeUnits are the units of time the base^x is evaluated in
// Deprecated: use go-libp2p/p2p/discovery/backoff.NewExponentialBackoff instead.
func NewExponentialBackoff(min, max time.Duration, jitter Jitter,
	timeUnits time.Duration, base float64, offset time.Duration, rngSrc rand.Source) BackoffFactory {
	return dbackoff.NewExponentialBackoff(min, max, jitter, timeUnits, base, offset, rngSrc)
}

// NewExponentialDecorrelatedJitter creates a BackoffFactory with backoff of the roughly of the form base^x where x is the attempt number.
// Delays start at the minimum duration and after each attempt delay = rand(min, delay * base), bounded by the max
// See https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ for more information
// Deprecated: use go-libp2p/p2p/discovery/backoff.NewExponentialDecorrelatedJitter instead.
func NewExponentialDecorrelatedJitter(min, max time.Duration, base float64, rngSrc rand.Source) BackoffFactory {
	return dbackoff.NewExponentialDecorrelatedJitter(min, max, base, rngSrc)
}
