package ubus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Event is one broadcast ubus event.
type Event struct {
	// Name is the concrete event name delivered by ubusd.
	Name string
	// Body is a copied JSON representation of the native blobmsg payload.
	Body json.RawMessage
}

// EventHandler receives events outside the native uloop thread. Its context is
// cancelled when the listener or connection closes.
type EventHandler func(context.Context, Event)

// EventListener is a registered ubus event pattern.
type EventListener struct {
	conn      *Conn
	id        uint64
	pattern   string
	handler   EventHandler
	ctx       context.Context
	cancel    context.CancelFunc
	queue     chan Event
	dropped   atomic.Uint64
	closeOnce sync.Once
}

// Listen registers a native ubus event pattern. ctx controls registration only;
// the returned listener remains active until Close or connection shutdown. An
// empty pattern listens to all events. Events are delivered through a bounded
// queue so slow handlers cannot block uloop.
func (c *Conn) Listen(ctx context.Context, pattern string, handler EventHandler) (*EventListener, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: nil event handler", ErrInvalidArgument)
	}
	callCtx, cancelCall, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancelCall()
	listenerCtx, cancel := context.WithCancel(c.ctx)
	listener := &EventListener{
		conn: c, id: c.allocateID(), pattern: pattern, handler: handler,
		ctx: listenerCtx, cancel: cancel, queue: make(chan Event, c.queueLen),
	}
	c.mu.Lock()
	c.listeners[listener.id] = listener
	c.mu.Unlock()
	if err := c.engine.AddEvent(callCtx, listener.id, pattern); err != nil {
		c.mu.Lock()
		delete(c.listeners, listener.id)
		c.mu.Unlock()
		listener.closeLocal()
		return nil, mapNativeError("listen", err)
	}
	go listener.run()
	return listener, nil
}

// SendEvent broadcasts a native ubus event.
func (c *Conn) SendEvent(ctx context.Context, name string, body any) error {
	if name == "" {
		return fmt.Errorf("%w: event name is required", ErrInvalidArgument)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%w: encode event: %v", ErrInvalidArgument, err)
	}
	callCtx, cancel, err := c.callContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	return mapNativeError("send event", c.engine.SendEvent(callCtx, name, string(normalizeJSON(payload))))
}

// Close unregisters the event pattern. It is idempotent.
func (l *EventListener) Close(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}
	var closeErr error
	l.closeOnce.Do(func() {
		callCtx, cancel, err := l.conn.callContext(ctx)
		if err == nil {
			closeErr = mapNativeError("remove event listener", l.conn.engine.RemoveEvent(callCtx, l.id))
			cancel()
		} else if err != ErrClosed {
			closeErr = err
		}
		l.conn.mu.Lock()
		delete(l.conn.listeners, l.id)
		l.conn.mu.Unlock()
		l.closeLocal()
	})
	return closeErr
}

// Dropped reports events discarded because the handler queue was full.
func (l *EventListener) Dropped() uint64 {
	if l == nil {
		return 0
	}
	return l.dropped.Load()
}

func (l *EventListener) deliver(event Event) {
	select {
	case l.queue <- event:
	default:
		count := l.dropped.Add(1)
		l.conn.logger.Warn("dropping ubus event for slow handler", "pattern", l.pattern, "dropped", count)
	}
}

func (l *EventListener) run() {
	for {
		select {
		case <-l.ctx.Done():
			return
		case event := <-l.queue:
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						l.conn.logger.Error("ubus event handler panic", "pattern", l.pattern, "panic", recovered)
					}
				}()
				l.handler(l.ctx, event)
			}()
		}
	}
}

func (l *EventListener) closeLocal() { l.cancel() }
