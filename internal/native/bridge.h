//go:build linux && cgo

#ifndef GO_UBUS_BRIDGE_H
#define GO_UBUS_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

struct sy_ubus_bridge;
struct sy_ubus_object;

struct sy_ubus_bridge *sy_ubus_bridge_new(uintptr_t handle,
	const char *socket_path, int reconnect, int reconnect_delay_ms, int *status);
void sy_ubus_bridge_run(struct sy_ubus_bridge *bridge);
void sy_ubus_bridge_wake(struct sy_ubus_bridge *bridge);
void sy_ubus_bridge_stop(struct sy_ubus_bridge *bridge);
void sy_ubus_bridge_free(struct sy_ubus_bridge *bridge);

int sy_ubus_lookup(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	const char *path);
int sy_ubus_invoke(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	const char *object, const char *method, const char *json, int fd);
int sy_ubus_cancel_invoke(struct sy_ubus_bridge *bridge, uint64_t operation_id);

struct sy_ubus_object *sy_ubus_object_new(struct sy_ubus_bridge *bridge,
	uint64_t object_id, const char *name, size_t method_count);
int sy_ubus_object_set_method(struct sy_ubus_object *object, size_t method_index,
	const char *name, uint64_t tags, uint64_t mask, size_t field_count);
int sy_ubus_object_set_field(struct sy_ubus_object *object, size_t method_index,
	size_t field_index, const char *name, uint8_t type);
int sy_ubus_object_add(struct sy_ubus_object *object);
void sy_ubus_object_discard(struct sy_ubus_object *object);
int sy_ubus_object_remove(struct sy_ubus_bridge *bridge, uint64_t object_id);

int sy_ubus_complete_incoming(struct sy_ubus_bridge *bridge, uint64_t request_id,
	int status, const char *json, int fd);

int sy_ubus_event_add(struct sy_ubus_bridge *bridge, uint64_t listener_id,
	const char *pattern);
int sy_ubus_event_remove(struct sy_ubus_bridge *bridge, uint64_t listener_id);
int sy_ubus_event_send(struct sy_ubus_bridge *bridge, const char *name,
	const char *json);

int sy_ubus_subscription_add(struct sy_ubus_bridge *bridge,
	uint64_t subscription_id, const char *object);
int sy_ubus_subscription_remove(struct sy_ubus_bridge *bridge,
	uint64_t subscription_id);

int sy_ubus_notify(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	uint64_t publication_id, const char *type, const char *json, int wait);
int sy_ubus_cancel_notify(struct sy_ubus_bridge *bridge, uint64_t operation_id);

int sy_ubus_monitor_start(struct sy_ubus_bridge *bridge);
int sy_ubus_monitor_stop(struct sy_ubus_bridge *bridge);

#endif
