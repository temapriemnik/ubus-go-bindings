package ubus

import (
	"errors"
	"fmt"
)

// Status is a native ubus completion status.
type Status int

// Native ubus status values.
const (
	StatusOK Status = iota
	StatusInvalidCommand
	StatusInvalidArgument
	StatusMethodNotFound
	StatusNotFound
	StatusNoData
	StatusPermissionDenied
	StatusTimeout
	StatusNotSupported
	StatusUnknownError
	StatusConnectionFailed
	StatusNoMemory
	StatusParseError
	StatusSystemError
)

// Package sentinel errors. Native status errors support errors.Is with the
// corresponding sentinel.
var (
	ErrUnsupported      = errors.New("ubus: unsupported platform or build mode")
	ErrClosed           = errors.New("ubus: connection closed")
	ErrDisconnected     = errors.New("ubus: disconnected")
	ErrTimeout          = errors.New("ubus: request timed out")
	ErrInvalidArgument  = errors.New("ubus: invalid argument")
	ErrNotFound         = errors.New("ubus: object or method not found")
	ErrPermissionDenied = errors.New("ubus: permission denied")
)

// StatusError reports a native ubus status and optionally wraps a local cause.
// Payload data is deliberately excluded because it may contain secrets.
type StatusError struct {
	// Status is the native ubus completion status.
	Status Status
	// Message identifies the operation without including payload data.
	Message string
	// Cause is an optional local error available through errors.Unwrap.
	Cause error
}

// Error implements error.
func (e *StatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message != "" {
		return fmt.Sprintf("ubus: %s: %s", e.Status, e.Message)
	}
	return "ubus: " + e.Status.String()
}

// Unwrap returns the local cause, if any.
func (e *StatusError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Is matches both wrapped causes and the package sentinel for Status.
func (e *StatusError) Is(target error) bool {
	if e == nil {
		return false
	}
	sentinel := sentinelForStatus(e.Status)
	return errors.Is(e.Cause, target) || sentinel != nil && target == sentinel
}

// String returns the native status description.
func (s Status) String() string {
	switch s {
	case StatusOK:
		return "success"
	case StatusInvalidCommand:
		return "invalid command"
	case StatusInvalidArgument:
		return "invalid argument"
	case StatusMethodNotFound:
		return "method not found"
	case StatusNotFound:
		return "not found"
	case StatusNoData:
		return "no response"
	case StatusPermissionDenied:
		return "permission denied"
	case StatusTimeout:
		return "request timed out"
	case StatusNotSupported:
		return "operation not supported"
	case StatusUnknownError:
		return "unknown error"
	case StatusConnectionFailed:
		return "connection failed"
	case StatusNoMemory:
		return "out of memory"
	case StatusParseError:
		return "parse error"
	case StatusSystemError:
		return "system error"
	default:
		return fmt.Sprintf("status %d", int(s))
	}
}

func sentinelForStatus(status Status) error {
	switch status {
	case StatusInvalidArgument, StatusParseError:
		return ErrInvalidArgument
	case StatusMethodNotFound, StatusNotFound:
		return ErrNotFound
	case StatusPermissionDenied:
		return ErrPermissionDenied
	case StatusTimeout:
		return ErrTimeout
	case StatusConnectionFailed:
		return ErrDisconnected
	case StatusNotSupported:
		return ErrUnsupported
	default:
		return nil
	}
}

func statusFromError(err error) Status {
	if err == nil {
		return StatusOK
	}
	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.Status
	}
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return StatusInvalidArgument
	case errors.Is(err, ErrNotFound):
		return StatusNotFound
	case errors.Is(err, ErrPermissionDenied):
		return StatusPermissionDenied
	case errors.Is(err, ErrTimeout):
		return StatusTimeout
	case errors.Is(err, ErrUnsupported):
		return StatusNotSupported
	case errors.Is(err, ErrDisconnected), errors.Is(err, ErrClosed):
		return StatusConnectionFailed
	default:
		return StatusUnknownError
	}
}
