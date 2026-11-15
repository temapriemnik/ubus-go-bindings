package ubus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/temapriemnik/ubus-go-bindings/internal/native"
)

// Direction is the transport direction of a monitored ubus message.
type Direction uint8

const (
	// Received is a message received by the monitored client.
	Received Direction = iota
	// Sent is a message sent by the monitored client.
	Sent
)

// MonitorRecord is one parsed native ubus monitor message. Body contains the
// raw message attributes represented as JSON.
type MonitorRecord struct {
	// Sequence is the native ubus message sequence number.
	Sequence uint32
	// Client is the monitored client ID.
	Client uint32
	// Peer is the monitored peer ID.
	Peer uint32
	// Direction reports whether the client sent or received the message.
	Direction Direction
	// Type is the numeric native ubus message type.
	Type uint32
	// Body contains the parsed native message attributes.
	Body json.RawMessage
}

// MonitorOptions filters raw monitor delivery. Empty Directions and Types
// accept all records. QueueLen defaults to the connection callback queue size.
type MonitorOptions struct {
	// Directions restricts delivery by transport direction. Empty accepts both.
	Directions []Direction
	// Types restricts delivery by native message type. Empty accepts all.
	Types []uint32
	// QueueLen is the bounded record queue size. Zero uses the connection default.
	QueueLen int
}

// Monitor is the single privileged raw monitor active on a connection. Records
// may contain credentials and configuration secrets.
type Monitor struct {
	conn       *Conn
	records    chan MonitorRecord
	directions map[Direction]struct{}
	types      map[uint32]struct{}
	dropped    atomic.Uint64
	closeOnce  sync.Once
	localOnce  sync.Once
	stateMu    sync.Mutex
	closed     bool
}

// Monitor starts raw native message monitoring. ctx controls startup only; the
// returned monitor remains active until Close or connection shutdown. Only one
// Monitor may be active on a connection at a time.
func (c *Conn) Monitor(ctx context.Context, options MonitorOptions) (*Monitor, error) {
	if options.QueueLen < 0 {
		return nil, fmt.Errorf("%w: negative monitor queue length", ErrInvalidArgument)
	}
	if options.QueueLen == 0 {
		options.QueueLen = c.queueLen
	}
	directions := make(map[Direction]struct{}, len(options.Directions))
	for _, direction := range options.Directions {
		if direction != Received && direction != Sent {
			return nil, fmt.Errorf("%w: invalid monitor direction %d", ErrInvalidArgument, direction)
		}
		directions[direction] = struct{}{}
	}
	types := make(map[uint32]struct{}, len(options.Types))
	for _, messageType := range options.Types {
		types[messageType] = struct{}{}
	}
	callCtx, cancel, err := c.callContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	monitor := &Monitor{
		conn: c, records: make(chan MonitorRecord, options.QueueLen),
		directions: directions, types: types,
	}
	c.mu.Lock()
	if c.monitor != nil {
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: a monitor is already active", ErrInvalidArgument)
	}
	c.monitor = monitor
	c.mu.Unlock()
	if err := c.engine.StartMonitor(callCtx); err != nil {
		c.mu.Lock()
		if c.monitor == monitor {
			c.monitor = nil
		}
		c.mu.Unlock()
		monitor.closeLocal()
		return nil, mapNativeError("start monitor", err)
	}
	return monitor, nil
}

// Records returns the bounded raw monitor stream. The channel closes with the
// Monitor.
func (m *Monitor) Records() <-chan MonitorRecord {
	if m == nil {
		closed := make(chan MonitorRecord)
		close(closed)
		return closed
	}
	return m.records
}

// Dropped returns the number of records discarded because Records was full.
func (m *Monitor) Dropped() uint64 {
	if m == nil {
		return 0
	}
	return m.dropped.Load()
}

// Close stops native monitoring and closes Records. It is idempotent.
func (m *Monitor) Close(ctx context.Context) error {
	if m == nil || m.conn == nil {
		return nil
	}
	var closeErr error
	m.closeOnce.Do(func() {
		callCtx, cancel, err := m.conn.callContext(ctx)
		if err == nil {
			closeErr = mapNativeError("stop monitor", m.conn.engine.StopMonitor(callCtx))
			cancel()
		} else if err != ErrClosed {
			closeErr = err
		}
		m.conn.mu.Lock()
		if m.conn.monitor == m {
			m.conn.monitor = nil
		}
		m.conn.mu.Unlock()
		m.closeLocal()
	})
	return closeErr
}

func (m *Monitor) deliver(record native.MonitorRecord) {
	direction := Received
	if record.Send {
		direction = Sent
	}
	if len(m.directions) != 0 {
		if _, ok := m.directions[direction]; !ok {
			return
		}
	}
	if len(m.types) != 0 {
		if _, ok := m.types[record.Type]; !ok {
			return
		}
	}
	converted := MonitorRecord{
		Sequence: record.Sequence, Client: record.Client, Peer: record.Peer,
		Direction: direction, Type: record.Type, Body: json.RawMessage(record.JSON),
	}
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	if m.closed {
		return
	}
	select {
	case m.records <- converted:
	default:
		m.dropped.Add(1)
	}
}

func (m *Monitor) closeLocal() {
	m.localOnce.Do(func() {
		m.stateMu.Lock()
		m.closed = true
		close(m.records)
		m.stateMu.Unlock()
	})
}
