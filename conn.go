package ubus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/artem/ubus-go-bindings/internal/native"
)

const (
	defaultTimeout  = 30 * time.Second
	defaultQueueLen = 64
)

var activeConnection struct {
	sync.Mutex
	conn *Conn
}

// Options controls connection behavior. Socket defaults to the native ubus
// socket. DefaultTimeout applies only when a call context has no deadline.
type Options struct {
	// Socket is the ubusd Unix socket path. Empty uses libubus's default.
	Socket string
	// Reconnect restores long-lived registrations after connection loss.
	Reconnect bool
	// ReconnectDelay is the delay between reconnect attempts. Zero is one second.
	ReconnectDelay time.Duration
	// DefaultTimeout applies when an operation context has no deadline. Zero is
	// 30 seconds.
	DefaultTimeout time.Duration
	// CallbackQueueLen is the default bounded callback queue size. Zero is 64.
	CallbackQueueLen int
	// Logger receives lifecycle, drop, and recovered-panic diagnostics. Payloads
	// and monitor records are not logged by the package.
	Logger *slog.Logger
}

// Conn is a native ubus connection. Its public methods are safe for concurrent
// use. Version 1 permits one active Conn per process because uloop is global.
type Conn struct {
	engine native.Engine
	logger *slog.Logger

	defaultTimeout time.Duration
	queueLen       int
	reconnect      bool
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	closeOnce      sync.Once
	nextID         atomic.Uint64

	mu            sync.RWMutex
	err           error
	publications  map[uint64]*Publication
	listeners     map[uint64]*EventListener
	subscriptions map[uint64]*Subscription
	monitor       *Monitor
}

type connCallbacks struct {
	conn *Conn
}

// Dial connects to ubusd and starts the private uloop thread. It waits for the
// initial native connection and honors ctx while opening the socket.
func Dial(ctx context.Context, options Options) (*Conn, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if options.ReconnectDelay < 0 || options.DefaultTimeout < 0 || options.CallbackQueueLen < 0 {
		return nil, fmt.Errorf("%w: negative option value", ErrInvalidArgument)
	}

	activeConnection.Lock()
	defer activeConnection.Unlock()
	if activeConnection.conn != nil {
		return nil, fmt.Errorf("%w: another connection is active", ErrInvalidArgument)
	}

	if options.DefaultTimeout == 0 {
		options.DefaultTimeout = defaultTimeout
	}
	if options.CallbackQueueLen == 0 {
		options.CallbackQueueLen = defaultQueueLen
	}
	if options.ReconnectDelay == 0 {
		options.ReconnectDelay = time.Second
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	connCtx, cancel := context.WithCancel(context.Background())
	conn := &Conn{
		logger:         options.Logger,
		defaultTimeout: options.DefaultTimeout,
		queueLen:       options.CallbackQueueLen,
		reconnect:      options.Reconnect,
		ctx:            connCtx,
		cancel:         cancel,
		done:           make(chan struct{}),
		publications:   make(map[uint64]*Publication),
		listeners:      make(map[uint64]*EventListener),
		subscriptions:  make(map[uint64]*Subscription),
	}
	engine, err := native.Open(ctx, native.Options{
		Socket:         options.Socket,
		Reconnect:      options.Reconnect,
		ReconnectDelay: options.ReconnectDelay,
	}, connCallbacks{conn: conn})
	if err != nil {
		cancel()
		return nil, mapNativeError("dial", err)
	}
	conn.engine = engine
	activeConnection.conn = conn
	return conn, nil
}

// Close stops callbacks, closes the native connection, and releases all
// library-owned descriptors. It is idempotent.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}
	var closeErr error
	c.closeOnce.Do(func() {
		c.cancel()
		if c.engine != nil {
			closeErr = mapNativeError("close", c.engine.Close())
		}

		c.mu.Lock()
		for _, publication := range c.publications {
			publication.closeLocal()
		}
		for _, listener := range c.listeners {
			listener.closeLocal()
		}
		for _, subscription := range c.subscriptions {
			subscription.closeLocal()
		}
		if c.monitor != nil {
			c.monitor.closeLocal()
			c.monitor = nil
		}
		c.publications = nil
		c.listeners = nil
		c.subscriptions = nil
		c.mu.Unlock()

		activeConnection.Lock()
		if activeConnection.conn == c {
			activeConnection.conn = nil
		}
		activeConnection.Unlock()
		close(c.done)
	})
	return closeErr
}

// Done is closed after Close finishes.
func (c *Conn) Done() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return c.done
}

// Err returns the terminal connection error. Temporary disconnects handled by
// automatic reconnection are reported by the affected operation instead.
func (c *Conn) Err() error {
	if c == nil {
		return ErrClosed
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *Conn) callContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	select {
	case <-c.done:
		return nil, nil, ErrClosed
	default:
	}
	if _, ok := ctx.Deadline(); ok || c.defaultTimeout == 0 {
		return ctx, func() {}, nil
	}
	callCtx, cancel := context.WithTimeout(ctx, c.defaultTimeout)
	return callCtx, cancel, nil
}

func (c *Conn) allocateID() uint64 { return c.nextID.Add(1) }

func mapNativeError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, native.ErrUnsupported) {
		return ErrUnsupported
	}
	if errors.Is(err, native.ErrClosed) {
		return ErrClosed
	}
	var nativeStatus *native.StatusError
	if errors.As(err, &nativeStatus) {
		return &StatusError{Status: Status(nativeStatus.Status), Message: op, Cause: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", op, ErrTimeout)
	}
	return fmt.Errorf("%s: %w", op, err)
}

func (callbacks connCallbacks) ConnectionState(connected bool, status int) {
	c := callbacks.conn
	c.mu.Lock()
	if !connected && !c.reconnect && status != int(StatusOK) {
		c.err = &StatusError{Status: Status(status), Message: "connection state"}
	} else if connected {
		c.err = nil
	}
	c.mu.Unlock()
	if !connected && !c.reconnect {
		go func() { _ = c.Close() }()
	}
}

func (callbacks connCallbacks) Incoming(incoming native.Incoming) {
	c := callbacks.conn
	switch incoming.Kind {
	case native.IncomingMethod:
		c.dispatchMethod(incoming)
	case native.IncomingNotification:
		c.dispatchNotification(incoming)
	default:
		if incoming.FD >= 0 {
			_ = os.NewFile(uintptr(incoming.FD), "ubus-unknown-request").Close()
		}
		go c.completeIncoming(incoming.ID, Reply{}, &StatusError{Status: StatusInvalidArgument})
	}
}

func (callbacks connCallbacks) Event(listenerID uint64, name, body string) {
	c := callbacks.conn
	c.mu.RLock()
	listener := c.listeners[listenerID]
	c.mu.RUnlock()
	if listener != nil {
		listener.deliver(Event{Name: name, Body: json.RawMessage(body)})
	}
}

func (callbacks connCallbacks) SubscriptionState(id uint64, connected bool, targetID uint32, status int) {
	c := callbacks.conn
	c.mu.RLock()
	subscription := c.subscriptions[id]
	c.mu.RUnlock()
	if subscription != nil {
		var reason error
		if status != int(StatusOK) {
			reason = &StatusError{Status: Status(status), Message: "subscription state"}
		}
		subscription.deliverState(SubscriptionState{Connected: connected, TargetID: targetID, Reason: reason})
	}
}

func (callbacks connCallbacks) SubscribersChanged(id uint64, active bool) {
	c := callbacks.conn
	c.mu.RLock()
	publication := c.publications[id]
	c.mu.RUnlock()
	if publication != nil {
		publication.setSubscribers(active)
	}
}

func (callbacks connCallbacks) MonitorRecord(record native.MonitorRecord) {
	c := callbacks.conn
	c.mu.RLock()
	monitor := c.monitor
	c.mu.RUnlock()
	if monitor != nil {
		monitor.deliver(record)
	}
}

func (c *Conn) completeIncoming(id uint64, reply Reply, handlerErr error) {
	status := statusFromError(handlerErr)
	body := normalizeJSON(reply.Body)
	fd := -1
	if reply.FD != nil {
		fd = int(reply.FD.Fd())
		defer reply.FD.Close()
	}
	// Deferred replies must not inherit a short caller deadline. If the
	// connection is already closed, the engine returns ErrClosed immediately.
	if err := c.engine.CompleteIncoming(context.Background(), id, int(status), string(body), fd); err != nil && !errors.Is(err, ErrClosed) {
		c.logger.Warn("failed to complete ubus request", "status", status, "error", mapNativeError("complete request", err))
	}
}

func (c *Conn) recoverHandler(name string, reply *Reply, handlerErr *error) {
	if recovered := recover(); recovered != nil {
		*reply = Reply{}
		*handlerErr = &StatusError{Status: StatusUnknownError, Message: "handler panic"}
		c.logger.Error("ubus handler panic", "handler", name, "panic", recovered, "stack", string(debug.Stack()))
	}
}
