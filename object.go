package ubus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/artem/ubus-go-bindings/internal/native"
)

// Object describes an object and its methods before publication.
type Object struct {
	// Name is the unique ubus object path to publish.
	Name string
	// Methods is the non-empty set of methods exposed by the object.
	Methods []Method
}

// Publication is a native object registered on a Conn.
type Publication struct {
	conn    *Conn
	id      uint64
	name    string
	methods map[string]Method
	ctx     context.Context
	cancel  context.CancelFunc

	closeOnce         sync.Once
	localOnce         sync.Once
	stateMu           sync.Mutex
	closed            bool
	hasSubscribers    atomic.Bool
	subscriberChanges chan bool
}

// Publish registers object with ubusd. Definitions are retained and restored
// by libubus after a successful connection recovery.
func (c *Conn) Publish(ctx context.Context, object Object) (*Publication, error) {
	if strings.TrimSpace(object.Name) == "" {
		return nil, fmt.Errorf("%w: object name is required", ErrInvalidArgument)
	}
	if len(object.Methods) == 0 {
		return nil, fmt.Errorf("%w: object %q has no methods", ErrInvalidArgument, object.Name)
	}
	methods := make(map[string]Method, len(object.Methods))
	for index, method := range object.Methods {
		if strings.TrimSpace(method.name) == "" || method.handler == nil {
			return nil, fmt.Errorf("%w: method %d is incomplete", ErrInvalidArgument, index)
		}
		if _, exists := methods[method.name]; exists {
			return nil, fmt.Errorf("%w: duplicate method %q", ErrInvalidArgument, method.name)
		}
		if err := validateMethodOptions(method.options); err != nil {
			return nil, fmt.Errorf("method %q: %w", method.name, err)
		}
		methods[method.name] = method
	}
	callCtx, cancelCall, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancelCall()

	publicationCtx, cancel := context.WithCancel(c.ctx)
	publication := &Publication{
		conn:              c,
		id:                c.allocateID(),
		name:              object.Name,
		methods:           methods,
		ctx:               publicationCtx,
		cancel:            cancel,
		subscriberChanges: make(chan bool, 1),
	}
	c.mu.Lock()
	c.publications[publication.id] = publication
	c.mu.Unlock()
	if err := c.engine.AddObject(callCtx, publication.id, object.Name, nativeMethodSpecs(object.Methods)); err != nil {
		c.mu.Lock()
		delete(c.publications, publication.id)
		c.mu.Unlock()
		publication.closeLocal()
		return nil, mapNativeError("publish", err)
	}
	return publication, nil
}

// Close unregisters the object and cancels its handler contexts. It is
// idempotent.
func (p *Publication) Close(ctx context.Context) error {
	if p == nil || p.conn == nil {
		return nil
	}
	var closeErr error
	p.closeOnce.Do(func() {
		callCtx, cancel, err := p.conn.callContext(ctx)
		if err == nil {
			closeErr = mapNativeError("remove object", p.conn.engine.RemoveObject(callCtx, p.id))
			cancel()
		} else if err != ErrClosed {
			closeErr = err
		}
		p.conn.mu.Lock()
		delete(p.conn.publications, p.id)
		p.conn.mu.Unlock()
		p.closeLocal()
	})
	return closeErr
}

// HasSubscribers reports the most recently observed native subscriber state.
func (p *Publication) HasSubscribers() bool {
	return p != nil && p.hasSubscribers.Load()
}

// SubscriberChanges reports native transitions between zero and nonzero
// subscribers. The channel closes with the publication.
func (p *Publication) SubscriberChanges() <-chan bool {
	if p == nil {
		closed := make(chan bool)
		close(closed)
		return closed
	}
	return p.subscriberChanges
}

func (p *Publication) closeLocal() {
	p.localOnce.Do(func() {
		p.cancel()
		p.stateMu.Lock()
		p.closed = true
		close(p.subscriberChanges)
		p.stateMu.Unlock()
	})
}

func (p *Publication) setSubscribers(active bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.closed || p.hasSubscribers.Swap(active) == active {
		return
	}
	select {
	case p.subscriberChanges <- active:
	default:
		select {
		case <-p.subscriberChanges:
		default:
		}
		select {
		case p.subscriberChanges <- active:
		default:
		}
	}
}

func (c *Conn) dispatchMethod(incoming native.Incoming) {
	c.mu.RLock()
	publication := c.publications[incoming.OwnerID]
	c.mu.RUnlock()
	if publication == nil {
		if incoming.FD >= 0 {
			_ = os.NewFile(uintptr(incoming.FD), "ubus-orphan-request").Close()
		}
		go c.completeIncoming(incoming.ID, Reply{}, &StatusError{Status: StatusNotFound})
		return
	}
	method, ok := publication.methods[incoming.Method]
	if !ok {
		if incoming.FD >= 0 {
			_ = os.NewFile(uintptr(incoming.FD), "ubus-unknown-method").Close()
		}
		go c.completeIncoming(incoming.ID, Reply{}, &StatusError{Status: StatusMethodNotFound})
		return
	}
	request := requestFromNative(incoming)
	go func() {
		defer request.closeFD()
		var reply Reply
		var handlerErr error
		defer func() {
			c.recoverHandler(publication.name+"."+incoming.Method, &reply, &handlerErr)
			c.completeIncoming(incoming.ID, reply, handlerErr)
		}()
		if err := validateRequired(method.options.Policy, request.Body); err != nil {
			handlerErr = err
			return
		}
		reply, handlerErr = method.handler(publication.ctx, request)
	}()
}

func requestFromNative(incoming native.Incoming) *Request {
	request := &Request{
		Object: incoming.Object,
		Method: incoming.Method,
		Peer:   incoming.Peer,
		ACL: ACLKey{
			User:   incoming.User,
			Group:  incoming.Group,
			Object: incoming.ACLObj,
		},
		Body: json.RawMessage(incoming.JSON),
	}
	if incoming.FD >= 0 {
		request.fd = os.NewFile(uintptr(incoming.FD), "ubus-request")
	}
	return request
}

func validateRequired(policy Policy, body json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(normalizeJSON(body), &fields); err != nil {
		return &StatusError{Status: StatusInvalidArgument, Message: "decode request", Cause: err}
	}
	for _, field := range policy {
		if !field.Required {
			continue
		}
		if _, exists := fields[field.Name]; !exists {
			return &StatusError{Status: StatusInvalidArgument, Message: "missing required field " + field.Name}
		}
	}
	return nil
}
