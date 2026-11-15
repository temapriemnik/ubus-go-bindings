//go:build integration && linux && cgo

package ubus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *synchronizedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buffer.Reset()
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

type ubusdProcess struct {
	t       *testing.T
	loader  string
	libPath string
	binary  string
	cli     string
	fixture string
	socket  string
	cmd     *exec.Cmd
	peer    *exec.Cmd
	output  synchronizedBuffer
}

func newUbusdProcess(t *testing.T) *ubusdProcess {
	t.Helper()
	loader := os.Getenv("GO_UBUS_TEST_LOADER")
	libPath := os.Getenv("GO_UBUS_TEST_LIBPATH")
	binary := os.Getenv("GO_UBUS_TEST_UBUSD")
	cli := os.Getenv("GO_UBUS_TEST_UBUS")
	fixture := os.Getenv("GO_UBUS_TEST_FIXTURE")
	if loader == "" || libPath == "" || binary == "" || cli == "" || fixture == "" {
		t.Skip("native integration environment is not configured")
	}
	return &ubusdProcess{
		t: t, loader: loader, libPath: libPath, binary: binary, cli: cli, fixture: fixture,
		socket: filepath.Join(t.TempDir(), "ubus.sock"),
	}
}

func (p *ubusdProcess) startFixture() {
	p.t.Helper()
	p.peer = exec.Command(p.loader, "--library-path", p.libPath, p.fixture, "-s", p.socket)
	p.peer.Stdout = &p.output
	p.peer.Stderr = &p.output
	if err := p.peer.Start(); err != nil {
		p.t.Fatalf("start native fixture: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		command := exec.Command(p.loader, "--library-path", p.libPath, p.cli,
			"-s", p.socket, "list", "fixture.control")
		if err := command.Run(); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.stopFixture()
	p.t.Fatalf("native fixture did not publish its object: %s", p.output.String())
}

func (p *ubusdProcess) sendEvent(name, body string) {
	p.t.Helper()
	command := exec.Command(p.loader, "--library-path", p.libPath, p.cli,
		"-s", p.socket, "send", name, body)
	if output, err := command.CombinedOutput(); err != nil {
		p.t.Fatalf("send external event: %v: %s", err, output)
	}
}

func (p *ubusdProcess) call(object, method, body string) []byte {
	p.t.Helper()
	command := exec.Command(p.loader, "--library-path", p.libPath, p.cli,
		"-s", p.socket, "call", object, method, body)
	output, err := command.CombinedOutput()
	if err != nil {
		p.t.Fatalf("native call %s.%s: %v: %s", object, method, err, output)
	}
	return output
}

func (p *ubusdProcess) start() {
	p.t.Helper()
	p.output.Reset()
	p.cmd = exec.Command(p.loader, "--library-path", p.libPath, p.binary, "-s", p.socket)
	p.cmd.Stdout = &p.output
	p.cmd.Stderr = &p.output
	if err := p.cmd.Start(); err != nil {
		p.t.Fatalf("start ubusd: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		command := exec.Command(p.loader, "--library-path", p.libPath, p.cli,
			"-s", p.socket, "list")
		if err := command.Run(); err == nil {
			return
		}
		if p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
			p.t.Fatalf("ubusd exited: %s", p.output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	p.stop()
	p.t.Fatalf("ubusd socket was not created: %s", p.output.String())
}

func (p *ubusdProcess) stop() {
	p.stopFixture()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.cmd = nil
}

func (p *ubusdProcess) stopFixture() {
	if p.peer == nil || p.peer.Process == nil {
		return
	}
	_ = p.peer.Process.Kill()
	_ = p.peer.Wait()
	p.peer = nil
}

func TestNativeIntegration(t *testing.T) {
	daemon := newUbusdProcess(t)
	daemon.start()
	daemon.startFixture()
	defer daemon.stop()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := Dial(ctx, Options{
		Socket: daemon.socket, Reconnect: true, ReconnectDelay: 50 * time.Millisecond,
		DefaultTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	events := make(chan Event, 4)
	listenerRegistrationCtx, cancelListenerRegistration := context.WithCancel(ctx)
	listener, err := conn.Listen(listenerRegistrationCtx, "go.test.*", func(_ context.Context, event Event) {
		events <- event
	})
	cancelListenerRegistration()
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close(context.Background())

	notifications := make(chan Notification, 4)
	replyOriginalFiles := make(chan *os.File, 1)
	var notificationCount int
	var notificationMu sync.Mutex
	publication, err := conn.Publish(ctx, Object{
		Name: "go.test.object",
		Methods: []Method{
			NewRawMethod("echo", MethodOptions{Policy: Policy{
				{Name: "value", Type: String, Required: true},
			}}, func(_ context.Context, request *Request) (Reply, error) {
				return Reply{Body: append(json.RawMessage(nil), request.Body...)}, nil
			}),
			NewRawMethod("write_fd", MethodOptions{}, func(_ context.Context, request *Request) (Reply, error) {
				file, ok := request.TakeFD()
				if !ok {
					return Reply{}, &StatusError{Status: StatusInvalidArgument, Message: "missing fd"}
				}
				defer file.Close()
				_, err := file.Write([]byte("fd-ok"))
				return Reply{Body: json.RawMessage(`{"written":true}`)}, err
			}),
			NewRawMethod("reply_fd", MethodOptions{}, func(_ context.Context, _ *Request) (Reply, error) {
				readFile, writeFile, err := os.Pipe()
				if err != nil {
					return Reply{}, err
				}
				if _, err := writeFile.Write([]byte("reply-fd")); err != nil {
					_ = readFile.Close()
					_ = writeFile.Close()
					return Reply{}, err
				}
				_ = writeFile.Close()
				replyOriginalFiles <- readFile
				return Reply{Body: json.RawMessage(`{"returned":true}`), FD: readFile}, nil
			}),
		},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer publication.Close(context.Background())

	objects, err := conn.Lookup(ctx, "go.test.object")
	if err != nil || len(objects) != 1 {
		t.Fatalf("lookup: objects=%v err=%v", objects, err)
	}
	if id, err := conn.LookupID(ctx, "go.test.object"); err != nil || id != objects[0].ID {
		t.Fatalf("lookup id: id=%d want=%d err=%v", id, objects[0].ID, err)
	}
	if _, err := conn.LookupID(ctx, "go.test.missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing lookup id returned %v", err)
	}
	if _, err := conn.InvokeRaw(ctx, "go.test.object", "missing",
		json.RawMessage(`{}`)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing method returned %v", err)
	}
	if _, err := conn.InvokeRaw(ctx, "go.test.missing", "missing",
		json.RawMessage(`{}`)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object returned %v", err)
	}
	response, err := conn.InvokeRaw(ctx, "go.test.object", "echo", json.RawMessage(`{"value":"hello"}`))
	if err != nil || string(response) != `{"value":"hello"}` {
		t.Fatalf("invoke echo: response=%s err=%v", response, err)
	}
	var nativeResponse map[string]string
	if err := json.Unmarshal(daemon.call("go.test.object", "echo", `{"value":"native"}`), &nativeResponse); err != nil {
		t.Fatalf("decode native CLI response: %v", err)
	}
	if nativeResponse["value"] != "native" {
		t.Fatalf("unexpected native CLI response: %+v", nativeResponse)
	}
	_, err = conn.InvokeRaw(ctx, "go.test.object", "echo", json.RawMessage(`{}`))
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("required field returned %v", err)
	}

	readFile, writeFile, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readFile.Close()
	if _, err := conn.InvokeWithFD(ctx, "go.test.object", "write_fd", map[string]any{}, writeFile, nil); err != nil {
		t.Fatalf("invoke with fd: %v", err)
	}
	_ = writeFile.Close()
	fdData := make([]byte, len("fd-ok"))
	if _, err := readFile.Read(fdData); err != nil || string(fdData) != "fd-ok" {
		t.Fatalf("read transferred fd: data=%q err=%v", fdData, err)
	}
	var replyBody struct {
		Returned bool `json:"returned"`
	}
	replyFile, err := conn.InvokeWithFD(ctx, "go.test.object", "reply_fd", map[string]any{}, nil, &replyBody)
	if err != nil || replyFile == nil || !replyBody.Returned {
		t.Fatalf("invoke for reply fd: file=%v body=%+v err=%v", replyFile, replyBody, err)
	}
	replyData, err := io.ReadAll(replyFile)
	_ = replyFile.Close()
	if err != nil || string(replyData) != "reply-fd" {
		t.Fatalf("read reply fd: data=%q err=%v", replyData, err)
	}
	originalReplyFile := <-replyOriginalFiles
	if _, err := originalReplyFile.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("library did not close transferred Reply.FD: %v", err)
	}

	if err := conn.SendEvent(ctx, "go.test.event", map[string]string{"value": "event"}); err != nil {
		t.Fatalf("send event: %v", err)
	}
	// ubusd intentionally does not loop an event back to a listener owned by
	// the sending client, so a second native client verifies delivery.
	daemon.sendEvent("go.test.event", `{"value":"external"}`)
	select {
	case event := <-events:
		if event.Name != "go.test.event" {
			t.Fatalf("unexpected event: %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("event was not delivered")
	}

	subscriptionRegistrationCtx, cancelSubscriptionRegistration := context.WithCancel(ctx)
	subscription, err := conn.Subscribe(subscriptionRegistrationCtx, "fixture.control", func(_ context.Context, notification Notification) (Reply, error) {
		notificationMu.Lock()
		notificationCount++
		notificationMu.Unlock()
		notifications <- notification
		return Reply{Body: json.RawMessage(`{"ack":true}`)}, nil
	})
	cancelSubscriptionRegistration()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer subscription.Close(context.Background())
	select {
	case state := <-subscription.Changes():
		if !state.Connected || state.TargetID == 0 {
			t.Fatalf("unexpected subscription state: %+v", state)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial subscription state was not delivered")
	}
	if _, err := conn.InvokeRaw(ctx, "fixture.control", "watch",
		json.RawMessage(fmt.Sprintf(`{"id":%d}`, objects[0].ID))); err != nil {
		t.Fatalf("fixture watch: %v", err)
	}
	if _, err := conn.InvokeRaw(ctx, "fixture.control", "emit", json.RawMessage(`{"active":true}`)); err != nil {
		t.Fatalf("fixture emit: %v", err)
	}
	select {
	case notification := <-notifications:
		if notification.Type != "changed" {
			t.Fatalf("unexpected notification: %+v", notification)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("notification was not delivered")
	}
	report, err := publication.Notify(ctx, "changed", map[string]bool{"active": true})
	if err != nil || len(report.Results) != 1 || report.Results[0].Status != StatusOK {
		t.Fatalf("notify: report=%+v err=%v", report, err)
	}
	if len(report.Replies) != 1 || !bytes.Contains(report.Replies[0], []byte(`"peer":"fixture"`)) {
		t.Fatalf("unexpected unattributed notify replies: %+v", report.Replies)
	}
	monitorRegistrationCtx, cancelMonitorRegistration := context.WithCancel(ctx)
	monitor, err := conn.Monitor(monitorRegistrationCtx, MonitorOptions{QueueLen: 128})
	cancelMonitorRegistration()
	if err != nil {
		t.Fatalf("start monitor: %v", err)
	}
	if _, err := conn.InvokeRaw(ctx, "go.test.object", "echo", json.RawMessage(`{"value":"monitor"}`)); err != nil {
		t.Fatalf("monitored invoke: %v", err)
	}
	monitorDeadline := time.After(3 * time.Second)
	monitorSeen := false
	for !monitorSeen {
		select {
		case record := <-monitor.Records():
			monitorSeen = record.Type != 0 && len(record.Body) != 0
		case <-monitorDeadline:
			t.Fatal("monitor record was not delivered")
		}
	}

	temporaryPublication, err := conn.Publish(ctx, Object{
		Name: "go.test.temporary",
		Methods: []Method{NewRawMethod("ping", MethodOptions{},
			func(context.Context, *Request) (Reply, error) { return Reply{}, nil })},
	})
	if err != nil {
		t.Fatalf("publish temporary object: %v", err)
	}
	temporaryListener, err := conn.Listen(ctx, "go.temporary.*", func(context.Context, Event) {})
	if err != nil {
		t.Fatalf("register temporary listener: %v", err)
	}
	temporarySubscription, err := conn.Subscribe(ctx, "fixture.control",
		func(context.Context, Notification) (Reply, error) { return Reply{}, nil })
	if err != nil {
		t.Fatalf("register temporary subscription: %v", err)
	}
	select {
	case state := <-temporarySubscription.Changes():
		if !state.Connected {
			t.Fatalf("temporary subscription did not connect: %+v", state)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("temporary subscription state was not delivered")
	}

	daemon.stop()
	disconnectDeadline := time.Now().Add(3 * time.Second)
	for {
		probeCtx, probeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, probeErr := conn.Lookup(probeCtx, "go.test.object")
		probeCancel()
		if errors.Is(probeErr, ErrDisconnected) {
			break
		}
		if time.Now().After(disconnectDeadline) {
			t.Fatalf("connection loss was not observed: %v", probeErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	disconnectedCtx, disconnectedCancel := context.WithTimeout(context.Background(), time.Second)
	defer disconnectedCancel()
	if err := temporaryPublication.Close(disconnectedCtx); err != nil {
		t.Fatalf("close publication while disconnected: %v", err)
	}
	if err := temporaryListener.Close(disconnectedCtx); err != nil {
		t.Fatalf("close listener while disconnected: %v", err)
	}
	if err := temporarySubscription.Close(disconnectedCtx); err != nil {
		t.Fatalf("close subscription while disconnected: %v", err)
	}
	daemon.start()
	daemon.startFixture()
	reconnectDeadline := time.Now().Add(5 * time.Second)
	for {
		callCtx, callCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, invokeErr := conn.InvokeRaw(callCtx, "go.test.object", "echo", json.RawMessage(`{"value":"reconnected"}`))
		callCancel()
		if invokeErr == nil {
			break
		}
		if time.Now().After(reconnectDeadline) {
			t.Fatalf("connection did not recover: %v", invokeErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := conn.SendEvent(ctx, "go.test.after-reconnect", map[string]bool{"ok": true}); err != nil {
		t.Fatalf("event after reconnect: %v", err)
	}
	daemon.sendEvent("go.test.after-reconnect", `{"ok":true}`)
	select {
	case <-events:
	case <-time.After(3 * time.Second):
		t.Fatal("event listener was not restored")
	}
	reconnectedObjects, err := conn.Lookup(ctx, "go.test.object")
	if err != nil || len(reconnectedObjects) != 1 {
		t.Fatalf("lookup after reconnect: objects=%v err=%v", reconnectedObjects, err)
	}
	temporaryObjects, err := conn.Lookup(ctx, "go.test.temporary")
	if !errors.Is(err, ErrNotFound) || len(temporaryObjects) != 0 {
		t.Fatalf("closed object restored after reconnect: objects=%v err=%v", temporaryObjects, err)
	}
	if _, err := conn.InvokeRaw(ctx, "fixture.control", "watch",
		json.RawMessage(fmt.Sprintf(`{"id":%d}`, reconnectedObjects[0].ID))); err != nil {
		t.Fatalf("fixture watch object %d after reconnect: %v", reconnectedObjects[0].ID, err)
	}
	notificationMu.Lock()
	countBefore := notificationCount
	notificationMu.Unlock()
	if _, err := conn.InvokeRaw(ctx, "fixture.control", "emit", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("fixture emit after reconnect: %v", err)
	}
	if _, err := publication.Notify(ctx, "after-reconnect", map[string]bool{"ok": true}); err != nil {
		t.Fatalf("notify after reconnect: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		notificationMu.Lock()
		count := notificationCount
		notificationMu.Unlock()
		if count > countBefore {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription was not restored")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := conn.InvokeRaw(ctx, "go.test.object", "echo",
		json.RawMessage(`{"value":"monitor-after-reconnect"}`)); err != nil {
		t.Fatalf("monitored invoke after reconnect: %v", err)
	}
	monitorDeadline = time.After(3 * time.Second)
	for {
		select {
		case record := <-monitor.Records():
			if bytes.Contains(record.Body, []byte("monitor-after-reconnect")) {
				if err := monitor.Close(ctx); err != nil {
					t.Fatalf("stop monitor: %v", err)
				}
				return
			}
		case <-monitorDeadline:
			t.Fatal("monitor was not restored")
		}
	}
}

func TestIntegrationEnvironmentDescription(t *testing.T) {
	if os.Getenv("GO_UBUS_TEST_LOADER") == "" {
		t.Skip("native integration environment is not configured")
	}
	t.Log(fmt.Sprintf("loader=%s", os.Getenv("GO_UBUS_TEST_LOADER")))
}

func TestDialAfterClose(t *testing.T) {
	daemon := newUbusdProcess(t)
	daemon.start()
	defer daemon.stop()

	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := Dial(ctx, Options{
			Socket:         daemon.socket,
			DefaultTimeout: time.Second,
		})
		if err != nil {
			cancel()
			t.Fatalf("dial attempt %d: %v", attempt, err)
		}
		if _, err := conn.Lookup(ctx, ""); err != nil {
			_ = conn.Close()
			cancel()
			t.Fatalf("lookup attempt %d: %v", attempt, err)
		}
		if err := conn.Close(); err != nil {
			cancel()
			t.Fatalf("close attempt %d: %v", attempt, err)
		}
		if err := conn.Close(); err != nil {
			cancel()
			t.Fatalf("second close attempt %d: %v", attempt, err)
		}
		cancel()
	}
}
