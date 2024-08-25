package client

import (
	"fmt"
	"io"
)

type HTTPError struct {
	StatusCode int
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP error with StatusCode=%d: %s", e.StatusCode, e.Body)
}

func httpError(statusCode int, body io.Reader) error {
	bodyBytes, err := io.ReadAll(io.LimitReader(body, 1024))
	if err != nil {
		logger.Warnw("could not read body bytes from error response", "Error", err)
		bodyBytes = []byte("unable to read body")
	}
	return &HTTPError{
		StatusCode: statusCode,
		Body:       string(bodyBytes),
	}
}
