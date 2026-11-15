package ubus

import (
	"context"
	"encoding/json"
	"fmt"
)

// Method is an immutable ubus method definition created by NewMethod or
// NewRawMethod. Its zero value is invalid.
type Method struct {
	name    string
	options MethodOptions
	handler RawHandler
}

// RawHandler handles a native request without exposing C memory. Returning a
// StatusError controls the native completion status.
type RawHandler func(context.Context, *Request) (Reply, error)

// NewMethod creates a typed method. Request JSON is decoded into RequestType;
// the returned ResponseType is encoded as the ubus reply body.
func NewMethod[RequestType, ResponseType any](
	name string,
	options MethodOptions,
	handler func(context.Context, RequestType) (ResponseType, error),
) Method {
	options = cloneMethodOptions(options)
	if handler == nil {
		return Method{name: name, options: options}
	}
	return Method{
		name:    name,
		options: options,
		handler: func(ctx context.Context, request *Request) (Reply, error) {
			var input RequestType
			if err := json.Unmarshal(normalizeJSON(request.Body), &input); err != nil {
				return Reply{}, &StatusError{Status: StatusInvalidArgument, Message: "decode request", Cause: err}
			}
			output, err := handler(ctx, input)
			if err != nil {
				return Reply{}, err
			}
			body, err := json.Marshal(output)
			if err != nil {
				return Reply{}, fmt.Errorf("encode response: %w", err)
			}
			return Reply{Body: body}, nil
		},
	}
}

// NewRawMethod creates a method whose handler receives and returns raw JSON.
func NewRawMethod(name string, options MethodOptions, handler RawHandler) Method {
	return Method{name: name, options: cloneMethodOptions(options), handler: handler}
}

func cloneMethodOptions(options MethodOptions) MethodOptions {
	options.Policy = append(Policy(nil), options.Policy...)
	return options
}

func normalizeJSON(body json.RawMessage) json.RawMessage {
	if len(body) == 0 || string(body) == "null" {
		return json.RawMessage(`{}`)
	}
	return body
}
