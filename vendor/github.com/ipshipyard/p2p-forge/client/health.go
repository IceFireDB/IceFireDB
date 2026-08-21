package client

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	// HealthCheckPath is the well-known liveness endpoint exposed by p2p-forge
	// registration brokers. A healthy broker responds with HTTP 204.
	HealthCheckPath = "/v1/health"

	// healthCheckTimeout bounds a single broker health probe. Generous on
	// purpose: the check runs in a background goroutine, so a slow node or
	// link delays nothing user-visible, while a timeout that is too tight
	// would misreport a healthy broker as down.
	healthCheckTimeout = 15 * time.Second

	// healthCheckRetryInterval is the minimum wait between health checks
	// while the broker keeps failing them.
	healthCheckRetryInterval = 1 * time.Hour

	// healthCheckRetryIntervalMax caps how far a broker-supplied Retry-After
	// header can push out the next health check, so a misconfigured broker
	// cannot silence a client indefinitely.
	healthCheckRetryIntervalMax = 24 * time.Hour
)

// CheckBrokerHealth probes HealthCheckPath of the registration broker at
// registrationEndpoint and returns nil only when the broker confirms it is
// healthy with HTTP 204. An empty userAgent defaults to this module's version
// string, a nil httpClient to http.DefaultClient. The probe is bounded by an
// internal timeout on top of ctx.
func CheckBrokerHealth(ctx context.Context, registrationEndpoint string, userAgent string, httpClient *http.Client) error {
	_, err := checkBrokerHealth(ctx, registrationEndpoint, userAgent, httpClient)
	return err
}

// checkBrokerHealth implements CheckBrokerHealth and additionally returns the
// broker-requested wait from a Retry-After header on a failing response
// (zero when absent, invalid, or in the past).
func checkBrokerHealth(ctx context.Context, registrationEndpoint string, userAgent string, httpClient *http.Client) (retryAfter time.Duration, err error) {
	if userAgent == "" {
		userAgent = defaultUserAgent
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	ctx, cancel := context.WithTimeout(ctx, healthCheckTimeout)
	defer cancel()

	healthURL := strings.TrimSuffix(registrationEndpoint, "/") + HealthCheckPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", userAgent)

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("GET %s: expected HTTP %d, got %s", healthURL, http.StatusNoContent, resp.Status)
	}
	return 0, nil
}

// parseRetryAfter reads a Retry-After value in either RFC 9110 form,
// delay-seconds or HTTP-date, returning zero when it is absent, invalid, or
// in the past.
func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return max(0, time.Duration(secs)*time.Second)
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(0, time.Until(t))
	}
	return 0
}

// nextHealthCheckDelay returns how long to wait before the next health check
// after a failing one: at least healthCheckRetryInterval, stretched by a
// broker-supplied Retry-After up to healthCheckRetryIntervalMax.
func nextHealthCheckDelay(retryAfter time.Duration) time.Duration {
	return max(healthCheckRetryInterval, min(retryAfter, healthCheckRetryIntervalMax))
}

// waitForHealthyBroker blocks until the registration broker confirms it is
// healthy, and returns false when ctx is canceled first. One cheap GET per
// interval replaces doomed ACME attempts while the broker is down, and lets
// certificate setup start automatically once the broker recovers.
func (m *P2PForgeCertMgr) waitForHealthyBroker(ctx context.Context, log *zap.SugaredLogger) bool {
	for {
		retryAfter, err := checkBrokerHealth(ctx, m.forgeRegistrationEndpoint, m.userAgent, m.httpClient)
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		wait := nextHealthCheckDelay(retryAfter)
		log.Errorf("registration broker at %s did not confirm it is healthy (%s); certificate setup postponed, next health check in %s", m.forgeRegistrationEndpoint, err, wait)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
