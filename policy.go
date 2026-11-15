package ubus

import (
	"fmt"
	"strings"
)

// FieldType is a native blobmsg field type used in an ubus method signature.
type FieldType uint8

// Native blobmsg field types.
const (
	Unspecified FieldType = iota
	Array
	Table
	String
	Int64
	Int32
	Int16
	Int8
	Bool
	Double
)

// Field describes one named method argument. Required is enforced by the Go
// wrapper because native blobmsg_policy has no required-field flag.
type Field struct {
	// Name is the blobmsg field name exposed in the native signature.
	Name string
	// Type is the native blobmsg field type.
	Type FieldType
	// Required makes the Go wrapper reject requests that omit this field.
	Required bool
}

// Policy is the ordered native signature of an ubus method.
type Policy []Field

// MethodTags is a bit set of native ubus method tags.
type MethodTags uint64

// Native ubus method tags.
const (
	TagStatus MethodTags = 1 << iota
	TagAdmin
	TagPrivate
)

// MethodOptions configures a method's native signature, tags, and field mask.
// A zero Mask exposes all fields. Otherwise bit n controls Policy[n].
type MethodOptions struct {
	// Policy defines the ordered native blobmsg signature.
	Policy Policy
	// Tags contains native ubus method tags.
	Tags MethodTags
	// Mask selects visible Policy entries by bit index. Zero exposes all entries.
	Mask uint64
}

func validateMethodOptions(options MethodOptions) error {
	seen := make(map[string]struct{}, len(options.Policy))
	for index, field := range options.Policy {
		if strings.TrimSpace(field.Name) == "" {
			return fmt.Errorf("%w: policy field %d has an empty name", ErrInvalidArgument, index)
		}
		if field.Type > Double {
			return fmt.Errorf("%w: policy field %q has invalid type %d", ErrInvalidArgument, field.Name, field.Type)
		}
		if _, ok := seen[field.Name]; ok {
			return fmt.Errorf("%w: duplicate policy field %q", ErrInvalidArgument, field.Name)
		}
		seen[field.Name] = struct{}{}
	}
	return nil
}
