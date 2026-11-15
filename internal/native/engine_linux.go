//go:build linux && cgo

package native

/*
#cgo linux LDFLAGS: -lubus -lubox -lblobmsg_json -ljson-c
#include <stdlib.h>
#include "bridge.h"
*/
import "C"

import (
	"context"
	"errors"
	"runtime"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const maxDrainTasks = 64

type loopTask func(*C.struct_sy_ubus_bridge)

type invokeCompletion struct {
	result InvokeResult
	status int
}

type notifyCompletion struct {
	report NotifyReport
	status int
}

type lookupState struct {
	objects []LookupObject
}

type engine struct {
	callbacks Callbacks
	handle    cgo.Handle
	done      chan struct{}
	started   chan error
	nextID    atomic.Uint64
	closeOnce sync.Once

	mu      sync.Mutex
	bridge  *C.struct_sy_ubus_bridge
	tasks   []loopTask
	closing bool

	lookupMu sync.Mutex
	lookups  map[uint64]*lookupState

	invokeMu sync.Mutex
	invokes  map[uint64]chan invokeCompletion

	notifyMu sync.Mutex
	notifies map[uint64]struct {
		report NotifyReport
		done   chan notifyCompletion
	}
}

// Open creates the Linux cgo engine and waits for its initial ubus connection.
func Open(ctx context.Context, options Options, callbacks Callbacks) (Engine, error) {
	if ctx == nil {
		return nil, errors.New("native: nil context")
	}
	if callbacks == nil {
		return nil, errors.New("native: nil callbacks")
	}
	e := &engine{
		callbacks: callbacks,
		done:      make(chan struct{}),
		started:   make(chan error, 1),
		lookups:   make(map[uint64]*lookupState),
		invokes:   make(map[uint64]chan invokeCompletion),
		notifies: make(map[uint64]struct {
			report NotifyReport
			done   chan notifyCompletion
		}),
	}
	e.handle = cgo.NewHandle(e)
	go e.run(options)
	select {
	case err := <-e.started:
		if err != nil {
			return nil, err
		}
		return e, nil
	case <-ctx.Done():
		go func() {
			if err := <-e.started; err == nil {
				_ = e.Close()
			}
		}()
		return nil, ctx.Err()
	}
}

func (e *engine) run(options Options) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	var socket *C.char
	if options.Socket != "" {
		socket = C.CString(options.Socket)
		defer C.free(unsafe.Pointer(socket))
	}
	delay := options.ReconnectDelay / time.Millisecond
	if delay <= 0 {
		delay = 1000
	}
	if delay > time.Duration(^uint32(0)>>1) {
		delay = time.Duration(^uint32(0) >> 1)
	}
	var status C.int
	bridge := C.sy_ubus_bridge_new(
		C.uintptr_t(e.handle),
		socket,
		boolInt(options.Reconnect),
		C.int(delay),
		&status,
	)
	if bridge == nil {
		e.started <- &StatusError{Op: "connect", Status: int(status)}
		e.handle.Delete()
		close(e.done)
		return
	}
	e.mu.Lock()
	e.bridge = bridge
	e.mu.Unlock()
	e.started <- nil

	C.sy_ubus_bridge_run(bridge)
	C.sy_ubus_bridge_free(bridge)
	e.mu.Lock()
	e.bridge = nil
	e.closing = true
	e.mu.Unlock()
	close(e.done)
	e.handle.Delete()
}

func boolInt(value bool) C.int {
	if value {
		return 1
	}
	return 0
}

func (e *engine) nextOperationID() uint64 { return e.nextID.Add(1) }

func (e *engine) enqueue(ctx context.Context, task loopTask) error {
	if ctx == nil {
		return errors.New("native: nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	if e.closing || e.bridge == nil {
		e.mu.Unlock()
		return ErrClosed
	}
	e.tasks = append(e.tasks, task)
	bridge := e.bridge
	e.mu.Unlock()
	C.sy_ubus_bridge_wake(bridge)
	return nil
}

func (e *engine) statusTask(ctx context.Context, operation func(*C.struct_sy_ubus_bridge) C.int) error {
	result := make(chan int, 1)
	err := e.enqueue(ctx, func(bridge *C.struct_sy_ubus_bridge) {
		if err := ctx.Err(); err != nil {
			result <- -1
			return
		}
		result <- int(operation(bridge))
	})
	if err != nil {
		return err
	}
	select {
	case status := <-result:
		if status == -1 {
			return ctx.Err()
		}
		return errorForStatus("operation", status)
	case <-ctx.Done():
		// A synchronous libubus function cannot be interrupted once entered.
		// Keep captured C arguments alive until that function or the loop exits.
		select {
		case <-result:
		case <-e.done:
		}
		return ctx.Err()
	case <-e.done:
		return ErrClosed
	}
}

func errorForStatus(operation string, status int) error {
	if status == 0 {
		return nil
	}
	return &StatusError{Op: operation, Status: status}
}

func (e *engine) Close() error {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		if e.bridge == nil {
			e.closing = true
			e.mu.Unlock()
			return
		}
		e.closing = true
		bridge := e.bridge
		e.tasks = append(e.tasks, func(loopBridge *C.struct_sy_ubus_bridge) {
			C.sy_ubus_bridge_stop(loopBridge)
		})
		e.mu.Unlock()
		C.sy_ubus_bridge_wake(bridge)
	})
	<-e.done
	return nil
}

func (e *engine) Lookup(ctx context.Context, path string) ([]LookupObject, error) {
	id := e.nextOperationID()
	state := &lookupState{}
	e.lookupMu.Lock()
	e.lookups[id] = state
	e.lookupMu.Unlock()
	defer func() {
		e.lookupMu.Lock()
		delete(e.lookups, id)
		e.lookupMu.Unlock()
	}()
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	if err := e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_lookup(bridge, C.uint64_t(id), cPath)
	}); err != nil {
		return nil, err
	}
	e.lookupMu.Lock()
	objects := append([]LookupObject(nil), state.objects...)
	e.lookupMu.Unlock()
	return objects, nil
}

func (e *engine) Invoke(ctx context.Context, object, method, json string, fd int) (InvokeResult, error) {
	id := e.nextOperationID()
	done := make(chan invokeCompletion, 1)
	e.invokeMu.Lock()
	e.invokes[id] = done
	e.invokeMu.Unlock()
	remove := func() {
		e.invokeMu.Lock()
		delete(e.invokes, id)
		e.invokeMu.Unlock()
	}
	cObject := C.CString(object)
	cMethod := C.CString(method)
	cJSON := C.CString(json)
	defer C.free(unsafe.Pointer(cObject))
	defer C.free(unsafe.Pointer(cMethod))
	defer C.free(unsafe.Pointer(cJSON))
	if err := e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_invoke(bridge, C.uint64_t(id), cObject, cMethod, cJSON, C.int(fd))
	}); err != nil {
		remove()
		return InvokeResult{}, err
	}
	select {
	case completion := <-done:
		remove()
		if err := errorForStatus("invoke", completion.status); err != nil {
			if completion.result.FD >= 0 {
				_ = syscall.Close(completion.result.FD)
			}
			return InvokeResult{}, err
		}
		return completion.result, nil
	case <-ctx.Done():
		_ = e.statusTask(context.Background(), func(bridge *C.struct_sy_ubus_bridge) C.int {
			return C.sy_ubus_cancel_invoke(bridge, C.uint64_t(id))
		})
		remove()
		select {
		case completion := <-done:
			if completion.result.FD >= 0 {
				_ = syscall.Close(completion.result.FD)
			}
		default:
		}
		return InvokeResult{}, ctx.Err()
	case <-e.done:
		remove()
		return InvokeResult{}, ErrClosed
	}
}

func (e *engine) AddObject(ctx context.Context, id uint64, name string, methods []MethodSpec) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		object := C.sy_ubus_object_new(bridge, C.uint64_t(id), cName, C.size_t(len(methods)))
		if object == nil {
			return C.int(11)
		}
		committed := false
		defer func() {
			if !committed {
				C.sy_ubus_object_discard(object)
			}
		}()
		for methodIndex, method := range methods {
			cMethod := C.CString(method.Name)
			status := C.sy_ubus_object_set_method(object, C.size_t(methodIndex), cMethod,
				C.uint64_t(method.Tags), C.uint64_t(method.Mask), C.size_t(len(method.Fields)))
			C.free(unsafe.Pointer(cMethod))
			if status != 0 {
				return status
			}
			for fieldIndex, field := range method.Fields {
				cField := C.CString(field.Name)
				status = C.sy_ubus_object_set_field(object, C.size_t(methodIndex),
					C.size_t(fieldIndex), cField, C.uint8_t(field.Type))
				C.free(unsafe.Pointer(cField))
				if status != 0 {
					return status
				}
			}
		}
		status := C.sy_ubus_object_add(object)
		if status == 0 {
			committed = true
		}
		return status
	})
}

func (e *engine) RemoveObject(ctx context.Context, id uint64) error {
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_object_remove(bridge, C.uint64_t(id))
	})
}

func (e *engine) CompleteIncoming(ctx context.Context, id uint64, status int, json string, fd int) error {
	cJSON := C.CString(json)
	defer C.free(unsafe.Pointer(cJSON))
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_complete_incoming(bridge, C.uint64_t(id), C.int(status), cJSON, C.int(fd))
	})
}

func (e *engine) AddEvent(ctx context.Context, id uint64, pattern string) error {
	cPattern := C.CString(pattern)
	defer C.free(unsafe.Pointer(cPattern))
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_event_add(bridge, C.uint64_t(id), cPattern)
	})
}

func (e *engine) RemoveEvent(ctx context.Context, id uint64) error {
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_event_remove(bridge, C.uint64_t(id))
	})
}

func (e *engine) SendEvent(ctx context.Context, name, json string) error {
	cName := C.CString(name)
	cJSON := C.CString(json)
	defer C.free(unsafe.Pointer(cName))
	defer C.free(unsafe.Pointer(cJSON))
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_event_send(bridge, cName, cJSON)
	})
}

func (e *engine) AddSubscription(ctx context.Context, id uint64, object string) error {
	cObject := C.CString(object)
	defer C.free(unsafe.Pointer(cObject))
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_subscription_add(bridge, C.uint64_t(id), cObject)
	})
}

func (e *engine) RemoveSubscription(ctx context.Context, id uint64) error {
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_subscription_remove(bridge, C.uint64_t(id))
	})
}

func (e *engine) Notify(ctx context.Context, publicationID uint64, typeName, json string, wait bool) (NotifyReport, error) {
	id := e.nextOperationID()
	cType := C.CString(typeName)
	cJSON := C.CString(json)
	defer C.free(unsafe.Pointer(cType))
	defer C.free(unsafe.Pointer(cJSON))
	if !wait {
		err := e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
			return C.sy_ubus_notify(bridge, C.uint64_t(id), C.uint64_t(publicationID), cType, cJSON, 0)
		})
		return NotifyReport{}, err
	}
	done := make(chan notifyCompletion, 1)
	e.notifyMu.Lock()
	e.notifies[id] = struct {
		report NotifyReport
		done   chan notifyCompletion
	}{done: done}
	e.notifyMu.Unlock()
	remove := func() {
		e.notifyMu.Lock()
		delete(e.notifies, id)
		e.notifyMu.Unlock()
	}
	if err := e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_notify(bridge, C.uint64_t(id), C.uint64_t(publicationID), cType, cJSON, 1)
	}); err != nil {
		remove()
		return NotifyReport{}, err
	}
	select {
	case completion := <-done:
		remove()
		return completion.report, errorForStatus("notify", completion.status)
	case <-ctx.Done():
		_ = e.statusTask(context.Background(), func(bridge *C.struct_sy_ubus_bridge) C.int {
			return C.sy_ubus_cancel_notify(bridge, C.uint64_t(id))
		})
		remove()
		return NotifyReport{}, ctx.Err()
	case <-e.done:
		remove()
		return NotifyReport{}, ErrClosed
	}
}

func (e *engine) StartMonitor(ctx context.Context) error {
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_monitor_start(bridge)
	})
}

func (e *engine) StopMonitor(ctx context.Context) error {
	return e.statusTask(ctx, func(bridge *C.struct_sy_ubus_bridge) C.int {
		return C.sy_ubus_monitor_stop(bridge)
	})
}

func engineFromHandle(handle C.uintptr_t) *engine {
	return cgo.Handle(handle).Value().(*engine)
}

func goString(value *C.char) string {
	if value == nil {
		return ""
	}
	return C.GoString(value)
}

//export goSyUbusDrain
func goSyUbusDrain(handle C.uintptr_t) {
	e := engineFromHandle(handle)
	for count := 0; count < maxDrainTasks; count++ {
		e.mu.Lock()
		if len(e.tasks) == 0 {
			e.mu.Unlock()
			return
		}
		task := e.tasks[0]
		e.tasks[0] = nil
		e.tasks = e.tasks[1:]
		bridge := e.bridge
		e.mu.Unlock()
		task(bridge)
	}
	e.mu.Lock()
	remaining := len(e.tasks) != 0
	bridge := e.bridge
	e.mu.Unlock()
	if remaining && bridge != nil {
		C.sy_ubus_bridge_wake(bridge)
	}
}

//export goSyUbusConnectionState
func goSyUbusConnectionState(handle C.uintptr_t, connected, status C.int) {
	e := engineFromHandle(handle)
	func() {
		defer func() { _ = recover() }()
		e.callbacks.ConnectionState(connected != 0, int(status))
	}()
}

//export goSyUbusLookupObject
func goSyUbusLookupObject(handle C.uintptr_t, operationID C.uint64_t,
	id, typeID C.uint32_t, path, signature *C.char,
) {
	e := engineFromHandle(handle)
	e.lookupMu.Lock()
	if state := e.lookups[uint64(operationID)]; state != nil {
		state.objects = append(state.objects, LookupObject{
			ID: uint32(id), TypeID: uint32(typeID), Path: goString(path),
			Signature: goString(signature),
		})
	}
	e.lookupMu.Unlock()
}

//export goSyUbusInvokeComplete
func goSyUbusInvokeComplete(handle C.uintptr_t, operationID C.uint64_t,
	status C.int, json *C.char, fd C.int,
) {
	e := engineFromHandle(handle)
	e.invokeMu.Lock()
	done := e.invokes[uint64(operationID)]
	e.invokeMu.Unlock()
	completion := invokeCompletion{
		result: InvokeResult{JSON: goString(json), FD: int(fd)},
		status: int(status),
	}
	if done == nil {
		if fd >= 0 {
			_ = syscall.Close(int(fd))
		}
		return
	}
	select {
	case done <- completion:
	default:
		if fd >= 0 {
			_ = syscall.Close(int(fd))
		}
	}
}

//export goSyUbusIncoming
func goSyUbusIncoming(handle C.uintptr_t, requestID, ownerID C.uint64_t,
	kind C.int, object, method *C.char, peer C.uint32_t,
	user, group, aclObject, json *C.char, fd C.int,
) {
	e := engineFromHandle(handle)
	incoming := Incoming{
		ID: uint64(requestID), OwnerID: uint64(ownerID), Kind: IncomingKind(kind),
		Object: goString(object), Method: goString(method), Peer: uint32(peer),
		User: goString(user), Group: goString(group), ACLObj: goString(aclObject),
		JSON: goString(json), FD: int(fd),
	}
	owned := true
	func() {
		defer func() { _ = recover() }()
		e.callbacks.Incoming(incoming)
		owned = false
	}()
	if owned && fd >= 0 {
		_ = syscall.Close(int(fd))
	}
}

//export goSyUbusEvent
func goSyUbusEvent(handle C.uintptr_t, listenerID C.uint64_t, name, json *C.char) {
	e := engineFromHandle(handle)
	func() {
		defer func() { _ = recover() }()
		e.callbacks.Event(uint64(listenerID), goString(name), goString(json))
	}()
}

//export goSyUbusSubscriptionState
func goSyUbusSubscriptionState(handle C.uintptr_t, subscriptionID C.uint64_t,
	connected C.int, targetID C.uint32_t, status C.int,
) {
	e := engineFromHandle(handle)
	func() {
		defer func() { _ = recover() }()
		e.callbacks.SubscriptionState(uint64(subscriptionID), connected != 0,
			uint32(targetID), int(status))
	}()
}

//export goSyUbusSubscribersChanged
func goSyUbusSubscribersChanged(handle C.uintptr_t, publicationID C.uint64_t, active C.int) {
	e := engineFromHandle(handle)
	func() {
		defer func() { _ = recover() }()
		e.callbacks.SubscribersChanged(uint64(publicationID), active != 0)
	}()
}

//export goSyUbusNotifyResult
func goSyUbusNotifyResult(handle C.uintptr_t, operationID C.uint64_t,
	subscriber C.uint32_t, status C.int,
) {
	e := engineFromHandle(handle)
	e.notifyMu.Lock()
	state, ok := e.notifies[uint64(operationID)]
	if ok {
		state.report.Results = append(state.report.Results, NotifyResult{
			Subscriber: uint32(subscriber), Status: int(status),
		})
		e.notifies[uint64(operationID)] = state
	}
	e.notifyMu.Unlock()
}

//export goSyUbusNotifyReply
func goSyUbusNotifyReply(handle C.uintptr_t, operationID C.uint64_t, json *C.char) {
	e := engineFromHandle(handle)
	e.notifyMu.Lock()
	state, ok := e.notifies[uint64(operationID)]
	if ok {
		state.report.Replies = append(state.report.Replies, goString(json))
		e.notifies[uint64(operationID)] = state
	}
	e.notifyMu.Unlock()
}

//export goSyUbusNotifyComplete
func goSyUbusNotifyComplete(handle C.uintptr_t, operationID C.uint64_t, status C.int) {
	e := engineFromHandle(handle)
	e.notifyMu.Lock()
	state, ok := e.notifies[uint64(operationID)]
	e.notifyMu.Unlock()
	if !ok {
		return
	}
	completion := notifyCompletion{
		report: NotifyReport{
			Results: append([]NotifyResult(nil), state.report.Results...),
			Replies: append([]string(nil), state.report.Replies...),
		},
		status: int(status),
	}
	select {
	case state.done <- completion:
	default:
	}
}

//export goSyUbusMonitor
func goSyUbusMonitor(handle C.uintptr_t, sequence, client, peer C.uint32_t,
	send C.int, messageType C.uint32_t, json *C.char,
) {
	e := engineFromHandle(handle)
	record := MonitorRecord{
		Sequence: uint32(sequence), Client: uint32(client), Peer: uint32(peer),
		Send: send != 0, Type: uint32(messageType), JSON: goString(json),
	}
	func() {
		defer func() { _ = recover() }()
		e.callbacks.MonitorRecord(record)
	}()
}
