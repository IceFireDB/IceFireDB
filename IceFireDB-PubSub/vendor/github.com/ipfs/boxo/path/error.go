package path

import (
	"errors"
	"fmt"
)

var (
	ErrExpectedImmutable      = errors.New("path was expected to be immutable")
	ErrInsufficientComponents = errors.New("path does not have enough components")
	ErrUnknownNamespace       = errors.New("unknown namespace")
)

type ErrInvalidPath struct {
	err  error
	path string
}

func (e *ErrInvalidPath) Error() string {
	return fmt.Sprintf("invalid path %q: %s", e.path, e.err)
}

func (e *ErrInvalidPath) Unwrap() error {
	return e.err
}

func (e *ErrInvalidPath) Is(err error) bool {
	switch err.(type) {
	case *ErrInvalidPath:
		return true
	default:
		return false
	}
}
