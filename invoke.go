package ubus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/temapriemnik/ubus-go-bindings/internal/native"
)

// ObjectInfo describes one object returned by a native ubus lookup.
type ObjectInfo struct {
	// ID is the connection-local native object ID.
	ID uint32
	// TypeID is the connection-local native object type ID.
	TypeID uint32
	// Path is the registered object name.
	Path string
	// Signature is the native method signature represented as JSON.
	Signature json.RawMessage
}

// Lookup returns all objects matching path. An empty path lists every object.
func (c *Conn) Lookup(ctx context.Context, path string) ([]ObjectInfo, error) {
	callCtx, cancel, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	objects, err := c.engine.Lookup(callCtx, path)
	if err != nil {
		return nil, mapNativeError("lookup", err)
	}
	result := make([]ObjectInfo, 0, len(objects))
	for _, object := range objects {
		result = append(result, ObjectInfo{
			ID:        object.ID,
			TypeID:    object.TypeID,
			Path:      object.Path,
			Signature: json.RawMessage(object.Signature),
		})
	}
	return result, nil
}

// LookupID resolves one exact object path to its connection-local native ID.
func (c *Conn) LookupID(ctx context.Context, path string) (uint32, error) {
	if path == "" {
		return 0, fmt.Errorf("%w: object path is required", ErrInvalidArgument)
	}
	objects, err := c.Lookup(ctx, path)
	if err != nil {
		return 0, err
	}
	for _, object := range objects {
		if object.Path == path {
			return object.ID, nil
		}
	}
	return 0, &StatusError{Status: StatusNotFound, Message: "lookup id"}
}

// Invoke encodes request as JSON, invokes an object method, and decodes its
// response. It does not automatically retry after reconnection.
func (c *Conn) Invoke(ctx context.Context, object, method string, request, response any) error {
	_, err := c.invoke(ctx, object, method, request, nil, response)
	return err
}

// InvokeRaw invokes an object method using validated raw JSON.
func (c *Conn) InvokeRaw(ctx context.Context, object, method string, request json.RawMessage) (json.RawMessage, error) {
	if object == "" || method == "" {
		return nil, fmt.Errorf("%w: object and method are required", ErrInvalidArgument)
	}
	if !json.Valid(normalizeJSON(request)) {
		return nil, fmt.Errorf("%w: invalid request JSON", ErrInvalidArgument)
	}
	callCtx, cancel, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	result, err := c.engine.Invoke(callCtx, object, method, string(normalizeJSON(request)), -1)
	if err != nil {
		return nil, mapNativeError("invoke", err)
	}
	if result.FD >= 0 {
		_ = os.NewFile(uintptr(result.FD), "ubus-unclaimed-reply").Close()
	}
	return json.RawMessage(result.JSON), nil
}

// InvokeWithFD invokes a method with a descriptor and returns an optional reply
// descriptor. The request file remains owned by the caller; the returned file
// is owned by the caller and must be closed.
func (c *Conn) InvokeWithFD(
	ctx context.Context,
	object string,
	method string,
	request any,
	requestFD *os.File,
	response any,
) (*os.File, error) {
	return c.invoke(ctx, object, method, request, requestFD, response)
}

func (c *Conn) invoke(ctx context.Context, object, method string, request any, requestFD *os.File, response any) (*os.File, error) {
	if object == "" || method == "" {
		return nil, fmt.Errorf("%w: object and method are required", ErrInvalidArgument)
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("%w: encode request: %v", ErrInvalidArgument, err)
	}
	body = normalizeJSON(body)
	callCtx, cancel, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	fd := -1
	if requestFD != nil {
		fd = int(requestFD.Fd())
	}
	result, err := c.engine.Invoke(callCtx, object, method, string(body), fd)
	if err != nil {
		return nil, mapNativeError("invoke", err)
	}
	var responseFile *os.File
	if result.FD >= 0 {
		responseFile = os.NewFile(uintptr(result.FD), "ubus-reply")
	}
	if response != nil && len(result.JSON) != 0 {
		if err := json.Unmarshal([]byte(result.JSON), response); err != nil {
			if responseFile != nil {
				_ = responseFile.Close()
			}
			return nil, fmt.Errorf("decode response: %w", err)
		}
	}
	return responseFile, nil
}

func nativeMethodSpecs(methods []Method) []native.MethodSpec {
	result := make([]native.MethodSpec, 0, len(methods))
	for _, method := range methods {
		fields := make([]native.FieldSpec, 0, len(method.options.Policy))
		for _, field := range method.options.Policy {
			// libubox aliases BLOBMSG_TYPE_BOOL to BLOBMSG_TYPE_INT8, and
			// BLOBMSG_TYPE_DOUBLE therefore has numeric value 8, not 9.
			fieldType := uint8(field.Type)
			switch field.Type {
			case Bool:
				fieldType = uint8(Int8)
			case Double:
				fieldType = 8
			}
			fields = append(fields, native.FieldSpec{Name: field.Name, Type: fieldType})
		}
		result = append(result, native.MethodSpec{
			Name:   method.name,
			Fields: fields,
			Tags:   uint64(method.options.Tags),
			Mask:   method.options.Mask,
		})
	}
	return result
}
