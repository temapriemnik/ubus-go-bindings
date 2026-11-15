//go:build !linux || !cgo

package native

import "context"

// Open returns ErrUnsupported when Linux cgo support is unavailable.
func Open(context.Context, Options, Callbacks) (Engine, error) { return nil, ErrUnsupported }
