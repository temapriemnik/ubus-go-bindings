// Package native contains the private libubus bridge used by the public module.
package native

import (
	"context"
	"errors"
	"strconv"
	"time"
)

var (
	// ErrUnsupported is returned when native libubus support was not compiled.
	ErrUnsupported = errors.New("native ubus is unavailable")
	// ErrClosed is returned after the native event loop has begun shutting down.
	ErrClosed = errors.New("native ubus engine is closed")
)

// StatusError carries a native ubus status from the bridge.
type StatusError struct {
	Op     string
	Status int
}

func (e *StatusError) Error() string { return e.Op + ": ubus status " + strconv.Itoa(e.Status) }

// Options controls the native event loop and reconnection behavior.
type Options struct {
	Socket         string
	Reconnect      bool
	ReconnectDelay time.Duration
}

// LookupObject is one copied native lookup result.
type LookupObject struct {
	ID        uint32
	TypeID    uint32
	Path      string
	Signature string
}

// FieldSpec is one native blobmsg policy entry.
type FieldSpec struct {
	Name string
	Type uint8
}

// MethodSpec is one method registered on a native object.
type MethodSpec struct {
	Name   string
	Fields []FieldSpec
	Tags   uint64
	Mask   uint64
}

// InvokeResult contains copied response JSON and an owned descriptor.
type InvokeResult struct {
	JSON string
	FD   int
}

// NotifyResult contains one subscriber's native completion status.
type NotifyResult struct {
	Subscriber uint32
	Status     int
}

// NotifyReport separates attributable statuses from unattributed reply data.
type NotifyReport struct {
	Results []NotifyResult
	Replies []string
}

// IncomingKind identifies the owner and semantics of an incoming request.
type IncomingKind uint8

const (
	// IncomingMethod is a call to a published object method.
	IncomingMethod IncomingKind = iota
	// IncomingNotification is a notification delivered to a subscriber.
	IncomingNotification
)

// Incoming is a fully copied request awaiting deferred completion.
type Incoming struct {
	ID      uint64
	OwnerID uint64
	Kind    IncomingKind
	Object  string
	Method  string
	Peer    uint32
	User    string
	Group   string
	ACLObj  string
	JSON    string
	FD      int
}

// MonitorRecord is one parsed native monitor envelope.
type MonitorRecord struct {
	Sequence uint32
	Client   uint32
	Peer     uint32
	Send     bool
	Type     uint32
	JSON     string
}

// Callbacks receives copied native events. Implementations must return quickly
// and must not run user code on the uloop thread.
type Callbacks interface {
	ConnectionState(connected bool, status int)
	Incoming(Incoming)
	Event(listenerID uint64, name, body string)
	SubscriptionState(subscriptionID uint64, connected bool, targetID uint32, status int)
	SubscribersChanged(publicationID uint64, active bool)
	MonitorRecord(MonitorRecord)
}

// Engine is the internal, platform-specific native connection.
type Engine interface {
	Close() error
	Lookup(context.Context, string) ([]LookupObject, error)
	Invoke(context.Context, string, string, string, int) (InvokeResult, error)
	AddObject(context.Context, uint64, string, []MethodSpec) error
	RemoveObject(context.Context, uint64) error
	CompleteIncoming(context.Context, uint64, int, string, int) error
	AddEvent(context.Context, uint64, string) error
	RemoveEvent(context.Context, uint64) error
	SendEvent(context.Context, string, string) error
	AddSubscription(context.Context, uint64, string) error
	RemoveSubscription(context.Context, uint64) error
	Notify(context.Context, uint64, string, string, bool) (NotifyReport, error)
	StartMonitor(context.Context) error
	StopMonitor(context.Context) error
}
