package gateway

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ipfs/boxo/retrieval"
)

const truncationMessage = "\n\n[Gateway Error: Response truncated - unable to retrieve remaining data within timeout period]"

// ErrRetrievalTimeout is returned on ResponseWriter Write calls after a retrieval timeout
var ErrRetrievalTimeout = errors.New("gateway: retrieval timeout")

// withRetrievalTimeout wraps an http.Handler with a timeout that enforces:
// 1. Maximum time to first byte (initial retrieval)
// 2. Maximum time between subsequent non-empty writes (timeout resets on each write)
// If no data is written within the specified duration, the connection is
// terminated with a 504 Gateway Timeout.
//
// Parameters:
//   - handler: The HTTP handler to wrap with retrieval timeout
//   - timeout: Maximum duration between writes (0 disables timeout)
//   - c: Optional configuration for controlling error page rendering (can be nil)
func withRetrievalTimeout(handler http.Handler, timeout time.Duration, c *Config, metrics *middlewareMetrics) http.Handler {
	if timeout <= 0 {
		return handler
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add retrieval.State to request context for tracking provider diagnostics
		ctx, retrievalState := retrieval.ContextWithState(r.Context())
		r = r.WithContext(ctx)

		// Create channels for coordination
		handlerDone := make(chan struct{})
		timeoutChan := make(chan struct{})

		// Create a custom response writer that tracks writes
		tw := &timeoutWriter{
			ResponseWriter: w,
			headers:        make(http.Header),
			timeout:        timeout,
			timer:          time.NewTimer(timeout),
			request:        r,
			config:         c,
		}

		// Start the timeout monitoring goroutine
		go func() {
			select {
			case <-tw.timer.C:
				tw.mu.Lock()
				defer tw.mu.Unlock()

				if !tw.timedOut && !tw.handlerComplete {
					tw.timedOut = true
					log.Debugw("retrieval timeout triggered",
						"path", r.URL.Path,
						"headerSent", tw.wroteHeader,
						"bytesWritten", tw.bytesWritten)

					if !tw.wroteHeader {
						// Headers not sent yet, we can send 504
						// Mark headers as written to prevent the handler from modifying headers
						tw.wroteHeader = true
						tw.headerCode = http.StatusGatewayTimeout

						metrics.recordTimeout(http.StatusGatewayTimeout, false)
						message := "Unable to retrieve content within timeout period"

						// Include diagnostic context if available
						if retrievalState != nil {
							message = fmt.Sprintf("%s: %s", message, retrievalState.Summary())
						}

						log.Debugw("sending 504 gateway timeout",
							"path", tw.request.URL.Path,
							"method", tw.request.Method,
							"remoteAddr", tw.request.RemoteAddr,
							"timeoutReason", message)

						// Write error response directly to ResponseWriter (safe because handler uses tw.headers)
						writeErrorResponse(tw.ResponseWriter, tw.request, tw.config, http.StatusGatewayTimeout, message)
					} else {
						// Headers already sent, response is being truncated
						statusCode := tw.headerCode
						if statusCode == 0 {
							statusCode = http.StatusOK
						}
						metrics.recordTimeout(statusCode, true)

						// Try to write truncation message (best effort)
						fmt.Fprint(tw.ResponseWriter, truncationMessage)

						// Try to hijack and force connection reset
						if hijacker, ok := tw.ResponseWriter.(http.Hijacker); ok {
							conn, _, err := hijacker.Hijack()
							if err == nil {
								tw.hijacked = true
								// Force TCP RST instead of graceful close
								if tcpConn, ok := conn.(*net.TCPConn); ok {
									tcpConn.SetLinger(0)
								}
								conn.Close()
								log.Debugw("response truncated due to timeout",
									"path", tw.request.URL.Path,
									"method", tw.request.Method,
									"remoteAddr", tw.request.RemoteAddr,
									"status", statusCode,
									"bytesWritten", tw.bytesWritten)
							} else {
								log.Warnw("failed to hijack connection for timeout reset",
									"path", tw.request.URL.Path,
									"error", err)
							}
						}
					}

					// Signal timeout to potentially waiting handler
					close(timeoutChan)
				}

			case <-handlerDone:
				// Handler completed, stop timer and exit
				tw.timer.Stop()
			}
		}()

		// Run handler in a goroutine so we can interrupt it on timeout
		go func() {
			defer close(handlerDone) // Always signal completion, even on panic
			handler.ServeHTTP(tw, r)
			tw.mu.Lock()
			tw.handlerComplete = true
			tw.mu.Unlock()
		}()

		// Wait for either handler completion or timeout
		select {
		case <-handlerDone:
			// Handler completed normally
		case <-timeoutChan:
			// Timeout occurred, response already sent by timeout goroutine
		}
	})
}

// timeoutWriter wraps http.ResponseWriter to track write activity
type timeoutWriter struct {
	http.ResponseWriter
	headers         http.Header // Separate header map for handler to use
	timeout         time.Duration
	timer           *time.Timer
	mu              sync.Mutex
	timedOut        bool
	wroteHeader     bool
	headerCode      int
	hijacked        bool
	handlerComplete bool
	bytesWritten    int64
	request         *http.Request
	config          *Config
}

// copyHeaders safely copies headers from our isolated map to the ResponseWriter.
// This prevents race conditions by ensuring the handler only modifies tw.headers
// while the timeout goroutine can safely write to the underlying ResponseWriter.
func (tw *timeoutWriter) copyHeaders() {
	if tw.wroteHeader {
		return
	}

	// Copy headers from our map to the underlying ResponseWriter
	dst := tw.ResponseWriter.Header()
	for k, vv := range tw.headers {
		// Create a new slice to avoid sharing the underlying array.
		// dst[k][:0] reuses the destination's capacity if it exists,
		// or creates a new slice if dst[k] is nil, then appends all values.
		// This ensures we don't share the underlying array between maps.
		dst[k] = append(dst[k][:0], vv...)
	}

	tw.wroteHeader = true
}

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return 0, ErrRetrievalTimeout
	}

	// Reset timer on non-empty write
	if len(p) > 0 {
		// Set data retrieval phase on first data write
		if tw.bytesWritten == 0 {
			if retrievalState := retrieval.StateFromContext(tw.request.Context()); retrievalState != nil {
				retrievalState.SetPhase(retrieval.PhaseDataRetrieval)
			}
		}
		tw.timer.Reset(tw.timeout)
		tw.bytesWritten += int64(len(p))
	}

	// Ensure headers are written before first body write
	if !tw.wroteHeader {
		tw.copyHeaders()
		if tw.headerCode == 0 {
			tw.headerCode = http.StatusOK
		}
	}

	n, err := tw.ResponseWriter.Write(p)
	return n, err
}

func (tw *timeoutWriter) WriteHeader(statusCode int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut || tw.wroteHeader {
		return
	}

	tw.copyHeaders()
	tw.headerCode = statusCode
	tw.ResponseWriter.WriteHeader(statusCode)
}

// Header returns the header map that will be sent by WriteHeader.
// This returns tw.headers (not the underlying ResponseWriter's headers)
// to prevent concurrent map access between handler and timeout goroutines.
func (tw *timeoutWriter) Header() http.Header {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut || tw.wroteHeader {
		return make(http.Header)
	}

	return tw.headers
}

// Flush implements http.Flusher
func (tw *timeoutWriter) Flush() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.timedOut {
		return
	}

	if f, ok := tw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
