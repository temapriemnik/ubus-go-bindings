package ubus

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func TestTypedMethod(t *testing.T) {
	t.Parallel()
	type request struct {
		Value string `json:"value"`
	}
	type response struct {
		Value string `json:"value"`
	}
	method := NewMethod("echo", MethodOptions{}, func(_ context.Context, input request) (response, error) {
		return response{Value: input.Value}, nil
	})
	reply, err := method.handler(context.Background(), &Request{Body: json.RawMessage(`{"value":"ok"}`)})
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if string(reply.Body) != `{"value":"ok"}` {
		t.Fatalf("unexpected reply: %s", reply.Body)
	}
}

func TestTypedMethodRejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	method := NewMethod("decode", MethodOptions{}, func(_ context.Context, input struct{}) (struct{}, error) {
		return struct{}{}, nil
	})
	_, err := method.handler(context.Background(), &Request{Body: json.RawMessage(`{`)})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid JSON returned %v", err)
	}
}

func TestMethodCopiesPolicy(t *testing.T) {
	t.Parallel()
	policy := Policy{{Name: "value", Type: String, Required: true}}
	method := NewRawMethod("copy", MethodOptions{Policy: policy}, func(context.Context, *Request) (Reply, error) {
		return Reply{}, nil
	})
	policy[0].Name = "changed"
	if got := method.options.Policy[0].Name; got != "value" {
		t.Fatalf("method policy changed through caller slice: %q", got)
	}
}

func TestRequestTakeFD(t *testing.T) {
	t.Parallel()
	readFile, writeFile, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readFile.Close()
	request := &Request{fd: writeFile}
	taken, ok := request.TakeFD()
	if !ok || taken != writeFile {
		t.Fatal("TakeFD did not transfer the descriptor")
	}
	request.closeFD()
	if err := taken.Close(); err != nil {
		t.Fatalf("caller could not close transferred descriptor: %v", err)
	}
	if _, ok := request.TakeFD(); ok {
		t.Fatal("TakeFD transferred the descriptor twice")
	}
}
