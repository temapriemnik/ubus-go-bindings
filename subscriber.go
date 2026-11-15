package ubus

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/artem/ubus-go-bindings/internal/native"
)

// Notification is an object-specific ubus notification.
type Notification struct {
	// Object is the subscribed object name.
	Object string
	// Type is the notification method name.
	Type string
	// Body is a copied JSON representation of the native blobmsg payload.
	Body json.RawMessage
}

// NotificationHandler handles a notification outside the native uloop thread.
type NotificationHandler func(context.Context, Notification) (Reply, error)

// SubscriptionState reports target lookup, removal, and reconnection changes.
type SubscriptionState struct {
	// Connected reports whether the subscription currently has a live target.
	Connected bool
	// TargetID is the last native target ID associated with this transition.
	TargetID uint32
	// Reason describes a failed lookup, target removal, or resubscription.
	Reason error
}

// Subscription is an object-specific native ubus subscription.
type Subscription struct {
	conn      *Conn
	id        uint64
	object    string
	handler   NotificationHandler
	ctx       context.Context
	cancel    context.CancelFunc
	requests  chan native.Incoming
	changes   chan SubscriptionState
	closeOnce sync.Once
	localOnce sync.Once
	stateMu   sync.Mutex
	closed    bool
}

// Subscribe registers for notifications from object. ctx controls registration
// only; the returned subscription remains active until Close or connection
// shutdown. The target name is retained and resolved again after reconnection.
func (c *Conn) Subscribe(ctx context.Context, object string, handler NotificationHandler) (*Subscription, error) {
	if object == "" || handler == nil {
		return nil, fmt.Errorf("%w: object and handler are required", ErrInvalidArgument)
	}
	callCtx, cancelCall, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancelCall()
	subCtx, cancel := context.WithCancel(c.ctx)
	subscription := &Subscription{
		conn: c, id: c.allocateID(), object: object, handler: handler,
		ctx: subCtx, cancel: cancel,
		requests: make(chan native.Incoming, c.queueLen),
		changes:  make(chan SubscriptionState, c.queueLen),
	}
	c.mu.Lock()
	c.subscriptions[subscription.id] = subscription
	c.mu.Unlock()
	if err := c.engine.AddSubscription(callCtx, subscription.id, object); err != nil {
		c.mu.Lock()
		delete(c.subscriptions, subscription.id)
		c.mu.Unlock()
		subscription.closeLocal()
		return nil, mapNativeError("subscribe", err)
	}
	go subscription.run()
	return subscription, nil
}

// Changes returns a stream of target state transitions. It closes only when
// the subscription closes, not when the target temporarily disappears.
func (s *Subscription) Changes() <-chan SubscriptionState {
	if s == nil {
		closed := make(chan SubscriptionState)
		close(closed)
		return closed
	}
	return s.changes
}

// Close unregisters the subscriber and closes Changes. It is idempotent.
func (s *Subscription) Close(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return nil
	}
	var closeErr error
	s.closeOnce.Do(func() {
		callCtx, cancel, err := s.conn.callContext(ctx)
		if err == nil {
			closeErr = mapNativeError("remove subscription", s.conn.engine.RemoveSubscription(callCtx, s.id))
			cancel()
		} else if err != ErrClosed {
			closeErr = err
		}
		s.conn.mu.Lock()
		delete(s.conn.subscriptions, s.id)
		s.conn.mu.Unlock()
		s.closeLocal()
	})
	return closeErr
}

func (c *Conn) dispatchNotification(incoming native.Incoming) {
	c.mu.RLock()
	subscription := c.subscriptions[incoming.OwnerID]
	c.mu.RUnlock()
	if subscription == nil {
		if incoming.FD >= 0 {
			_ = os.NewFile(uintptr(incoming.FD), "ubus-orphan-notification").Close()
		}
		go c.completeIncoming(incoming.ID, Reply{}, &StatusError{Status: StatusNotFound})
		return
	}
	if !subscription.enqueue(incoming) {
		if incoming.FD >= 0 {
			_ = os.NewFile(uintptr(incoming.FD), "ubus-dropped-notification").Close()
		}
		c.logger.Warn("dropping ubus notification", "object", subscription.object)
		go c.completeIncoming(incoming.ID, Reply{}, &StatusError{Status: StatusUnknownError, Message: "handler queue full or closed"})
	}
}

func (s *Subscription) enqueue(incoming native.Incoming) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.requests <- incoming:
		return true
	default:
		return false
	}
}

func (s *Subscription) run() {
	for {
		select {
		case <-s.ctx.Done():
			s.rejectQueued()
			return
		case incoming := <-s.requests:
			s.handle(incoming)
		}
	}
}

func (s *Subscription) handle(incoming native.Incoming) {
	if incoming.FD >= 0 {
		_ = os.NewFile(uintptr(incoming.FD), "ubus-notification-fd").Close()
	}
	var reply Reply
	var handlerErr error
	func() {
		defer s.conn.recoverHandler(s.object+" notification", &reply, &handlerErr)
		reply, handlerErr = s.handler(s.ctx, Notification{
			Object: incoming.Object,
			Type:   incoming.Method,
			Body:   json.RawMessage(incoming.JSON),
		})
	}()
	s.conn.completeIncoming(incoming.ID, reply, handlerErr)
}

func (s *Subscription) rejectQueued() {
	for {
		select {
		case incoming := <-s.requests:
			if incoming.FD >= 0 {
				_ = os.NewFile(uintptr(incoming.FD), "ubus-cancelled-notification").Close()
			}
			s.conn.completeIncoming(incoming.ID, Reply{}, &StatusError{
				Status:  StatusConnectionFailed,
				Message: "subscription closed",
			})
		default:
			return
		}
	}
}

func (s *Subscription) deliverState(state SubscriptionState) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.changes <- state:
	default:
		select {
		case <-s.changes:
		default:
		}
		select {
		case s.changes <- state:
		default:
		}
	}
}

func (s *Subscription) closeLocal() {
	s.localOnce.Do(func() {
		s.cancel()
		s.stateMu.Lock()
		s.closed = true
		close(s.changes)
		s.stateMu.Unlock()
	})
}
