package ubus

import (
	"errors"
	"testing"
)

func TestLookupIDRejectsEmptyPath(t *testing.T) {
	t.Parallel()
	var conn *Conn
	if _, err := conn.LookupID(nil, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty path returned %v", err)
	}
}
