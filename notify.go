package ubus

import (
	"context"
	"encoding/json"
	"fmt"
)

// NotifyResult is one subscriber's native completion status.
type NotifyResult struct {
	// Subscriber is the native subscriber object ID.
	Subscriber uint32
	// Status is the subscriber's completion status.
	Status Status
}

// NotifyReport contains the statuses and reply payloads collected by Notify.
// Native libubus does not identify the subscriber that sent each reply, so
// Replies preserves arrival order without associating payloads with Results.
type NotifyReport struct {
	// Results contains one native completion status per responding subscriber.
	Results []NotifyResult
	// Replies contains copied subscriber payloads in native arrival order.
	Replies []json.RawMessage
}

// Notify sends a notification and waits for native subscriber statuses and
// replies until completion or until ctx ends. It never retries after a
// disconnect.
func (p *Publication) Notify(ctx context.Context, typeName string, body any) (NotifyReport, error) {
	return p.notify(ctx, typeName, body, true)
}

// NotifyNow sends a native no-reply notification. It waits only until the
// message has been accepted by the local ubus transport.
func (p *Publication) NotifyNow(typeName string, body any) error {
	if p == nil || p.conn == nil {
		return ErrClosed
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.conn.defaultTimeout)
	defer cancel()
	_, err := p.notify(ctx, typeName, body, false)
	return err
}

func (p *Publication) notify(ctx context.Context, typeName string, body any, wait bool) (NotifyReport, error) {
	if p == nil || p.conn == nil {
		return NotifyReport{}, ErrClosed
	}
	if typeName == "" {
		return NotifyReport{}, fmt.Errorf("%w: notification type is required", ErrInvalidArgument)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return NotifyReport{}, fmt.Errorf("%w: encode notification: %v", ErrInvalidArgument, err)
	}
	callCtx, cancel, err := p.conn.callContext(ctx)
	if err != nil {
		return NotifyReport{}, err
	}
	defer cancel()
	report, err := p.conn.engine.Notify(callCtx, p.id, typeName, string(normalizeJSON(payload)), wait)
	if err != nil {
		return NotifyReport{}, mapNativeError("notify", err)
	}
	converted := NotifyReport{
		Results: make([]NotifyResult, 0, len(report.Results)),
		Replies: make([]json.RawMessage, 0, len(report.Replies)),
	}
	for _, result := range report.Results {
		converted.Results = append(converted.Results, NotifyResult{
			Subscriber: result.Subscriber,
			Status:     Status(result.Status),
		})
	}
	for _, reply := range report.Replies {
		converted.Replies = append(converted.Replies, json.RawMessage(reply))
	}
	return converted, nil
}
