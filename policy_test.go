package ubus

import (
	"errors"
	"testing"
)

func TestValidateMethodOptions(t *testing.T) {
	t.Parallel()
	valid := MethodOptions{Policy: Policy{
		{Name: "name", Type: String, Required: true},
		{Name: "count", Type: Int32},
	}}
	if err := validateMethodOptions(valid); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}

	tests := []MethodOptions{
		{Policy: Policy{{Name: "", Type: String}}},
		{Policy: Policy{{Name: "same", Type: String}, {Name: "same", Type: Int32}}},
		{Policy: Policy{{Name: "bad", Type: FieldType(255)}}},
	}
	for _, options := range tests {
		if err := validateMethodOptions(options); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("invalid policy returned %v", err)
		}
	}
}

func TestValidateRequired(t *testing.T) {
	t.Parallel()
	policy := Policy{{Name: "required", Type: String, Required: true}}
	if err := validateRequired(policy, []byte(`{"required":"yes"}`)); err != nil {
		t.Fatalf("present required field rejected: %v", err)
	}
	if err := validateRequired(policy, []byte(`{}`)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("missing required field returned %v", err)
	}
}
