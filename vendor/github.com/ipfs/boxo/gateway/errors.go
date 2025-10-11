package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/gateway/assets"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/path/resolver"
	"github.com/ipfs/boxo/retrieval"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/schema"
)

var (
	ErrInternalServerError = NewErrorStatusCodeFromStatus(http.StatusInternalServerError)
	ErrGatewayTimeout      = NewErrorStatusCodeFromStatus(http.StatusGatewayTimeout)
	ErrBadGateway          = NewErrorStatusCodeFromStatus(http.StatusBadGateway)
	ErrServiceUnavailable  = NewErrorStatusCodeFromStatus(http.StatusServiceUnavailable)
	ErrTooManyRequests     = NewErrorStatusCodeFromStatus(http.StatusTooManyRequests)

	// Errors for user input validation (prevent user input in logs)
	errUnsupportedFormat        = errors.New("unsupported response format requested")
	errConversionNotSupported   = errors.New("converting to requested format is not supported")
	errInvalidURIQueryParameter = errors.New("failed to parse uri query parameter")
	errInvalidURIScheme         = errors.New("uri query parameter scheme must be ipfs or ipns")
	errUnsupportedCarScope      = errors.New("unsupported application/vnd.ipld.car scope parameter")
	errUnsupportedCarOrder      = errors.New("unsupported application/vnd.ipld.car order parameter")
	errUnsupportedCarDups       = errors.New("unsupported application/vnd.ipld.car dups parameter")
	errIndexNotReadable         = errors.New("index.html could not be read: not a file")
)

// ErrorRetryAfter wraps any error with "retry after" hint. When an error of this type
// returned to the gateway handler by an [IPFSBackend], the retry after value will be
// passed to the HTTP client in a [Retry-After] HTTP header.
//
// [Retry-After]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
type ErrorRetryAfter struct {
	Err        error
	RetryAfter time.Duration
}

func NewErrorRetryAfter(err error, retryAfter time.Duration) *ErrorRetryAfter {
	if err == nil {
		err = ErrServiceUnavailable
	}
	if retryAfter < 0 {
		retryAfter = 0
	}
	return &ErrorRetryAfter{
		RetryAfter: retryAfter,
		Err:        err,
	}
}

func (e *ErrorRetryAfter) Error() string {
	var text string
	if e.Err != nil {
		text = e.Err.Error()
	}
	if e.RetryAfter != 0 {
		text += ", retry after " + e.humanizedRoundSeconds()
	}
	return text
}

func (e *ErrorRetryAfter) Unwrap() error {
	return e.Err
}

func (e *ErrorRetryAfter) Is(err error) bool {
	switch err.(type) {
	case *ErrorRetryAfter:
		return true
	default:
		return false
	}
}

// RetryAfterHeader returns the [Retry-After] header value as a string, representing the number
// of seconds to wait before making a new request, rounded to the nearest second.
// This function follows the [Retry-After] header definition as specified in RFC 9110.
//
// [Retry-After]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After
func (e *ErrorRetryAfter) RetryAfterHeader() string {
	return strconv.Itoa(int(e.roundSeconds().Seconds()))
}

func (e *ErrorRetryAfter) roundSeconds() time.Duration {
	return e.RetryAfter.Round(time.Second)
}

func (e *ErrorRetryAfter) humanizedRoundSeconds() string {
	return e.roundSeconds().String()
}

// ErrorStatusCode wraps any error with a specific HTTP status code. When an error
// of this type is returned to the gateway handler by an [IPFSBackend], the status
// code will be used for the response status.
type ErrorStatusCode struct {
	StatusCode int
	Err        error
}

func NewErrorStatusCodeFromStatus(statusCode int) *ErrorStatusCode {
	return NewErrorStatusCode(errors.New(http.StatusText(statusCode)), statusCode)
}

func NewErrorStatusCode(err error, statusCode int) *ErrorStatusCode {
	return &ErrorStatusCode{
		Err:        err,
		StatusCode: statusCode,
	}
}

func (e *ErrorStatusCode) Is(err error) bool {
	switch err.(type) {
	case *ErrorStatusCode:
		return true
	default:
		return false
	}
}

func (e *ErrorStatusCode) Error() string {
	var text string
	if e.Err != nil {
		text = e.Err.Error()
	}
	return text
}

func (e *ErrorStatusCode) Unwrap() error {
	return e.Err
}

// ErrInvalidResponse can be returned from a [DataCallback] to indicate that
// the data provided for the requested resource was explicitly 'incorrect',
// for example, when received blocks did not belong to the requested dag,
// or non-car-conforming data was returned.
type ErrInvalidResponse struct {
	Message string
}

func (e ErrInvalidResponse) Error() string {
	return e.Message
}

// ErrPartialResponse can be returned from a [DataCallback] to indicate that some of the requested resource
// was successfully fetched, and that instead of retrying the full resource, that there are
// one or more more specific resources that should be fetched (via StillNeed) to complete the request.
//
// This primitive allows for resume mechanism that is useful when a big CAR
// stream gets truncated due to network error, HTTP middleware timeout, etc,
// but some useful blocks were received and should not be fetched again.
type ErrPartialResponse struct {
	error
	StillNeed []CarResource
}

type CarResource struct {
	Path   path.ImmutablePath
	Params CarParams
}

func (epr ErrPartialResponse) Error() string {
	if epr.error != nil {
		return fmt.Sprintf("partial response: %s", epr.error.Error())
	}
	return "received a partial CAR response from the backend"
}

// writeErrorResponse writes an error response with the given status code and message.
// It returns HTML or plain text based on the Accept header and DisableHTMLErrors config.
func writeErrorResponse(w http.ResponseWriter, r *http.Request, c *Config, statusCode int, message string) {
	// Check if HTML response is appropriate
	acceptsHTML := false
	if c != nil && !c.DisableHTMLErrors {
		acceptsHTML = strings.Contains(r.Header.Get("Accept"), "text/html")
	}

	if acceptsHTML {
		// Extract CIDs from RetrievalState for diagnostic purposes (504 errors)
		var rootCID, failedCID string
		if statusCode == http.StatusGatewayTimeout && c != nil && c.DiagnosticServiceURL != "" {
			if retrievalState := retrieval.StateFromContext(r.Context()); retrievalState != nil {
				// Get root CID (first CID in the path)
				if root := retrievalState.GetRootCID(); root.Defined() {
					rootCID = root.String()
				}
				// Get terminal CID (CID that failed to retrieve)
				if terminal := retrievalState.GetTerminalCID(); terminal.Defined() {
					failedCID = terminal.String()
				} else {
					// If no terminal CID, use root CID as the failed CID
					failedCID = rootCID
				}
			}
		}

		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(statusCode)
		err := assets.ErrorTemplate.Execute(w, assets.ErrorTemplateData{
			GlobalData: assets.GlobalData{
				Menu: c.Menu,
			},
			StatusCode:           statusCode,
			StatusText:           http.StatusText(statusCode),
			Error:                message,
			DiagnosticServiceURL: c.DiagnosticServiceURL,
			RootCID:              rootCID,
			FailedCID:            failedCID,
		})
		if err != nil {
			fmt.Fprintf(w, "error during body generation: %v", err)
		}
	} else {
		// plain text response
		http.Error(w, message, statusCode)
	}
}

func webError(w http.ResponseWriter, r *http.Request, c *Config, err error, defaultCode int) {
	code := defaultCode

	// Pass Retry-After hint to the client
	var era *ErrorRetryAfter
	if errors.As(err, &era) {
		if era.RetryAfter > 0 {
			w.Header().Set("Retry-After", era.RetryAfterHeader())
			// Adjust defaultCode if needed
			if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable {
				code = http.StatusTooManyRequests
			}
		}
		err = era.Unwrap()
	}

	// Handle status code
	switch {
	case errors.Is(err, &cid.ErrInvalidCid{}):
		code = http.StatusBadRequest
	case isErrContentBlocked(err):
		code = http.StatusGone
	case isErrNotFound(err):
		code = http.StatusNotFound
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
	}

	// Handle explicit code in ErrorResponse
	var gwErr *ErrorStatusCode
	if errors.As(err, &gwErr) {
		code = gwErr.StatusCode
	}

	log.Debugw("serving error response",
		"path", r.URL.Path,
		"method", r.Method,
		"error", err,
		"code", code)

	writeErrorResponse(w, r, c, code, err.Error())
}

// isErrNotFound returns true for IPLD errors that should return 4xx errors (e.g. the path doesn't exist, the data is
// the wrong type, etc.), rather than issues with just finding and retrieving the data.
func isErrNotFound(err error) bool {
	if errors.Is(err, &resolver.ErrNoLink{}) || errors.Is(err, schema.ErrNoSuchField{}) {
		return true
	}

	if ipld.IsNotFound(err) {
		return true
	}

	// Checks if err is of a type that does not implement the .Is interface and
	// cannot be directly compared to. Therefore, errors.Is cannot be used.
	for {
		_, ok := err.(datamodel.ErrWrongKind)
		if ok {
			return true
		}

		_, ok = err.(datamodel.ErrNotExists)
		if ok {
			return true
		}

		err = errors.Unwrap(err)
		if err == nil {
			return false
		}
	}
}

// isErrContentBlocked returns true for content filtering system errors
func isErrContentBlocked(err error) bool {
	// TODO: we match error message to avoid pulling nopfs as a dependency
	// Ref. https://github.com/ipfs-shipyard/nopfs/blob/cde3b5ba964c13e977f4a95f3bd8ca7d7710fbda/status.go#L87-L89
	return strings.Contains(err.Error(), "blocked and cannot be provided")
}
