package ubus

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDialRejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	tests := []Options{
		{ReconnectDelay: -time.Second},
		{DefaultTimeout: -time.Second},
		{CallbackQueueLen: -1},
	}
	for _, options := range tests {
		if _, err := Dial(context.Background(), options); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("options %+v returned %v", options, err)
		}
	}
}

func TestDialRejectsNilContext(t *testing.T) {
	t.Parallel()
	if _, err := Dial(nil, Options{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context returned %v", err)
	}
}
