package ubus

import (
	"errors"
	"testing"
)

func TestStatusErrorMatchesSentinel(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status Status
		target error
	}{
		{StatusInvalidArgument, ErrInvalidArgument},
		{StatusParseError, ErrInvalidArgument},
		{StatusMethodNotFound, ErrNotFound},
		{StatusNotFound, ErrNotFound},
		{StatusPermissionDenied, ErrPermissionDenied},
		{StatusTimeout, ErrTimeout},
		{StatusConnectionFailed, ErrDisconnected},
		{StatusNotSupported, ErrUnsupported},
	}
	for _, test := range tests {
		err := &StatusError{Status: test.status}
		if !errors.Is(err, test.target) {
			t.Fatalf("status %v does not match %v", test.status, test.target)
		}
	}
}

func TestStatusErrorDoesNotMatchNil(t *testing.T) {
	t.Parallel()
	if errors.Is(&StatusError{Status: StatusOK}, nil) {
		t.Fatal("non-nil status error matched nil")
	}
}

func TestNilStatusErrorMethods(t *testing.T) {
	t.Parallel()
	var err *StatusError
	if err.Unwrap() != nil || err.Is(ErrInvalidArgument) {
		t.Fatal("nil StatusError exposed a cause or matched a sentinel")
	}
}
