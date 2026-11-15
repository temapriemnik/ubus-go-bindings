//go:build linux && cgo

#include "bridge.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <libubox/blob.h>
#include <libubox/blobmsg.h>
#include <libubox/blobmsg_json.h>
#include <libubox/list.h>
#include <libubox/uloop.h>
#include <libubus.h>
#include <ubusmsg.h>

extern void goSyUbusDrain(uintptr_t handle);
extern void goSyUbusConnectionState(uintptr_t handle, int connected, int status);
extern void goSyUbusLookupObject(uintptr_t handle, uint64_t operation_id,
	uint32_t id, uint32_t type_id, const char *path, const char *signature);
extern void goSyUbusInvokeComplete(uintptr_t handle, uint64_t operation_id,
	int status, const char *json, int fd);
extern void goSyUbusIncoming(uintptr_t handle, uint64_t request_id,
	uint64_t owner_id, int kind, const char *object, const char *method,
	uint32_t peer, const char *user, const char *group, const char *acl_object,
	const char *json, int fd);
extern void goSyUbusEvent(uintptr_t handle, uint64_t listener_id,
	const char *name, const char *json);
extern void goSyUbusSubscriptionState(uintptr_t handle, uint64_t subscription_id,
	int connected, uint32_t target_id, int status);
extern void goSyUbusSubscribersChanged(uintptr_t handle, uint64_t publication_id,
	int active);
extern void goSyUbusNotifyResult(uintptr_t handle, uint64_t operation_id,
	uint32_t subscriber, int status);
extern void goSyUbusNotifyReply(uintptr_t handle, uint64_t operation_id,
	const char *json);
extern void goSyUbusNotifyComplete(uintptr_t handle, uint64_t operation_id,
	int status);
extern void goSyUbusMonitor(uintptr_t handle, uint32_t sequence,
	uint32_t client, uint32_t peer, int send, uint32_t type, const char *json);

enum {
	SY_INCOMING_METHOD = 0,
	SY_INCOMING_NOTIFICATION = 1,
};

struct sy_ubus_bridge;

struct sy_ubus_request {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t operation_id;
	struct ubus_request request;
	char *json;
	int fd;
};

struct sy_ubus_deferred {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t request_id;
	struct ubus_request_data request;
};

struct sy_ubus_object {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t object_id;
	struct ubus_object object;
	struct ubus_object_type type;
	struct ubus_method *methods;
	size_t method_count;
	char *name;
};

struct sy_ubus_event {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t listener_id;
	struct ubus_event_handler handler;
	char *pattern;
};

struct sy_ubus_subscription {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t subscription_id;
	struct ubus_subscriber subscriber;
	char *target_name;
	uint32_t target_id;
};

struct sy_ubus_notify_request {
	struct list_head list;
	struct sy_ubus_bridge *bridge;
	uint64_t operation_id;
	struct ubus_notify_request request;
};

struct sy_ubus_bridge {
	uintptr_t handle;
	struct ubus_context *context;
	struct uloop_fd wake;
	struct uloop_timeout reconnect_timer;
	struct uloop_timeout subscription_timer;
	struct list_head requests;
	struct list_head deferred;
	struct list_head objects;
	struct list_head events;
	struct list_head subscriptions;
	struct list_head notifications;
	char *socket_path;
	uint64_t next_request_id;
	int reconnect_delay_ms;
	bool reconnect;
	bool connected;
	bool closing;
	bool monitor_active;
};

struct sy_lookup_data {
	struct sy_ubus_bridge *bridge;
	uint64_t operation_id;
};

static struct sy_ubus_bridge *sy_active_bridge;

static const char *sy_nonnull(const char *value)
{
	return value ? value : "";
}

static char *sy_json_from_blob(struct blob_attr *message)
{
	char *json;

	if (!message)
		return strdup("{}");
	json = blobmsg_format_json(message, true);
	return json ? json : strdup("{}");
}

/*
 * libubus retains every registered object in context->objects so it can
 * publish it again during ubus_reconnect(). A logical registration removed
 * while disconnected must be detached locally before its storage is freed.
 */
static void sy_forget_native_object(struct ubus_context *context,
	struct ubus_object *object)
{
	if (!context || !object || !object->id)
		return;
	avl_delete(&context->objects, &object->avl);
	object->id = 0;
	if (object->type)
		object->type->id = 0;
}

static void sy_unregister_or_forget_object(struct sy_ubus_bridge *bridge,
	struct ubus_object *object)
{
	if (!bridge || !object || !object->id)
		return;
	if (bridge->connected)
		(void)ubus_remove_object(bridge->context, object);
	if (object->id)
		sy_forget_native_object(bridge->context, object);
}

static int sy_blob_from_json(const char *json, struct blob_buf *buffer)
{
	if (!json || !*json)
		json = "{}";
	memset(buffer, 0, sizeof(*buffer));
	if (blob_buf_init(buffer, 0))
		return UBUS_STATUS_NO_MEMORY;
	if (!blobmsg_add_json_from_string(buffer, json)) {
		blob_buf_free(buffer);
		return UBUS_STATUS_PARSE_ERROR;
	}
	return UBUS_STATUS_OK;
}

static void sy_wake_callback(struct uloop_fd *descriptor, unsigned int events)
{
	struct sy_ubus_bridge *bridge = container_of(descriptor,
		struct sy_ubus_bridge, wake);
	uint64_t value;

	(void)events;
	while (read(descriptor->fd, &value, sizeof(value)) < 0 && errno == EINTR)
		;
	goSyUbusDrain(bridge->handle);
}

void sy_ubus_bridge_wake(struct sy_ubus_bridge *bridge)
{
	uint64_t value = 1;

	if (!bridge || bridge->wake.fd < 0)
		return;
	while (write(bridge->wake.fd, &value, sizeof(value)) < 0 && errno == EINTR)
		;
}

static struct sy_ubus_object *sy_find_object(struct sy_ubus_bridge *bridge,
	uint64_t object_id)
{
	struct sy_ubus_object *object;

	list_for_each_entry(object, &bridge->objects, list)
		if (object->object_id == object_id)
			return object;
	return NULL;
}

static struct sy_ubus_request *sy_find_request(struct sy_ubus_bridge *bridge,
	uint64_t operation_id)
{
	struct sy_ubus_request *request;

	list_for_each_entry(request, &bridge->requests, list)
		if (request->operation_id == operation_id)
			return request;
	return NULL;
}

static struct sy_ubus_notify_request *sy_find_notification(
	struct sy_ubus_bridge *bridge, uint64_t operation_id)
{
	struct sy_ubus_notify_request *request;

	list_for_each_entry(request, &bridge->notifications, list)
		if (request->operation_id == operation_id)
			return request;
	return NULL;
}

static void sy_free_method_storage(struct sy_ubus_object *object)
{
	size_t method_index;

	if (!object || !object->methods)
		return;
	for (method_index = 0; method_index < object->method_count; method_index++) {
		struct ubus_method *method = &object->methods[method_index];
		int field_index;

		free((char *)method->name);
		for (field_index = 0; field_index < method->n_policy; field_index++)
			free((char *)method->policy[field_index].name);
		free((void *)method->policy);
	}
	free(object->methods);
}

static void sy_free_object(struct sy_ubus_object *object)
{
	if (!object)
		return;
	sy_free_method_storage(object);
	free(object->name);
	free(object);
}

static int sy_validate_method_message(const struct ubus_method *method,
	struct blob_attr *message)
{
	struct blob_attr *attribute;
	size_t remaining;

	if (!message)
		return UBUS_STATUS_INVALID_ARGUMENT;
	blobmsg_for_each_attr(attribute, message, remaining) {
		int index;
		const char *name = blobmsg_name(attribute);

		for (index = 0; index < method->n_policy; index++) {
			if (strcmp(name, method->policy[index].name))
				continue;
			if (method->policy[index].type != BLOBMSG_TYPE_UNSPEC &&
			    blobmsg_type(attribute) != (int)method->policy[index].type)
				return UBUS_STATUS_INVALID_ARGUMENT;
			break;
		}
	}
	return UBUS_STATUS_OK;
}

static int sy_dispatch_incoming(struct sy_ubus_bridge *bridge, uint64_t owner_id,
	int kind, const char *object, struct ubus_request_data *request,
	const char *method, struct blob_attr *message)
{
	struct sy_ubus_deferred *deferred;
	char *json;
	int fd;

	deferred = calloc(1, sizeof(*deferred));
	if (!deferred)
		return UBUS_STATUS_NO_MEMORY;
	json = sy_json_from_blob(message);
	if (!json) {
		free(deferred);
		return UBUS_STATUS_NO_MEMORY;
	}
	deferred->bridge = bridge;
	deferred->request_id = ++bridge->next_request_id;
	ubus_defer_request(bridge->context, request, &deferred->request);
	list_add_tail(&deferred->list, &bridge->deferred);

	fd = ubus_request_get_caller_fd(request);
	if (fd >= 0)
		fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
	goSyUbusIncoming(bridge->handle, deferred->request_id, owner_id, kind,
		sy_nonnull(object), sy_nonnull(method), request->peer,
		sy_nonnull(request->acl.user), sy_nonnull(request->acl.group),
		sy_nonnull(request->acl.object), json, fd);
	free(json);
	return UBUS_STATUS_OK;
}

static int sy_method_callback(struct ubus_context *context,
	struct ubus_object *native_object, struct ubus_request_data *request,
	const char *method_name, struct blob_attr *message)
{
	struct sy_ubus_object *object = container_of(native_object,
		struct sy_ubus_object, object);
	const struct ubus_method *method = NULL;
	size_t index;
	int status;

	(void)context;
	for (index = 0; index < object->method_count; index++) {
		if (!strcmp(method_name, object->methods[index].name)) {
			method = &object->methods[index];
			break;
		}
	}
	if (!method)
		return UBUS_STATUS_METHOD_NOT_FOUND;
	status = sy_validate_method_message(method, message);
	if (status)
		return status;
	return sy_dispatch_incoming(object->bridge, object->object_id,
		SY_INCOMING_METHOD, object->name, request, method_name, message);
}

static void sy_subscribers_callback(struct ubus_context *context,
	struct ubus_object *native_object)
{
	struct sy_ubus_object *object = container_of(native_object,
		struct sy_ubus_object, object);

	(void)context;
	goSyUbusSubscribersChanged(object->bridge->handle, object->object_id,
		native_object->has_subscribers ? 1 : 0);
}

struct sy_ubus_object *sy_ubus_object_new(struct sy_ubus_bridge *bridge,
	uint64_t object_id, const char *name, size_t method_count)
{
	struct sy_ubus_object *object;

	if (!bridge || !name || !*name || !method_count || method_count > INT_MAX)
		return NULL;
	object = calloc(1, sizeof(*object));
	if (!object)
		return NULL;
	object->methods = calloc(method_count, sizeof(*object->methods));
	object->name = strdup(name);
	if (!object->methods || !object->name) {
		sy_free_object(object);
		return NULL;
	}
	object->bridge = bridge;
	object->object_id = object_id;
	object->method_count = method_count;
	object->type.name = object->name;
	object->type.methods = object->methods;
	object->type.n_methods = (int)method_count;
	object->object.name = object->name;
	object->object.type = &object->type;
	object->object.methods = object->methods;
	object->object.n_methods = (int)method_count;
	object->object.subscribe_cb = sy_subscribers_callback;
	return object;
}

int sy_ubus_object_set_method(struct sy_ubus_object *object, size_t method_index,
	const char *name, uint64_t tags, uint64_t mask, size_t field_count)
{
	struct ubus_method *method;

	if (!object || method_index >= object->method_count || !name || !*name)
		return UBUS_STATUS_INVALID_ARGUMENT;
	if (field_count > INT_MAX)
		return UBUS_STATUS_INVALID_ARGUMENT;
#if ULONG_MAX < UINT64_MAX
	if (tags > ULONG_MAX || mask > ULONG_MAX)
		return UBUS_STATUS_INVALID_ARGUMENT;
#endif
	method = &object->methods[method_index];
	if (method->name)
		return UBUS_STATUS_INVALID_ARGUMENT;
	method->name = strdup(name);
	if (!method->name)
		return UBUS_STATUS_NO_MEMORY;
	if (field_count) {
		method->policy = calloc(field_count, sizeof(*method->policy));
		if (!method->policy)
			return UBUS_STATUS_NO_MEMORY;
	}
	method->n_policy = (int)field_count;
	method->handler = sy_method_callback;
	method->tags = (unsigned long)tags;
	method->mask = (unsigned long)mask;
	return UBUS_STATUS_OK;
}

int sy_ubus_object_set_field(struct sy_ubus_object *object, size_t method_index,
	size_t field_index, const char *name, uint8_t type)
{
	struct blobmsg_policy *policy;

	if (!object || method_index >= object->method_count || !name || !*name)
		return UBUS_STATUS_INVALID_ARGUMENT;
	if (field_index >= (size_t)object->methods[method_index].n_policy)
		return UBUS_STATUS_INVALID_ARGUMENT;
	policy = (struct blobmsg_policy *)object->methods[method_index].policy;
	if (policy[field_index].name)
		return UBUS_STATUS_INVALID_ARGUMENT;
	policy[field_index].name = strdup(name);
	if (!policy[field_index].name)
		return UBUS_STATUS_NO_MEMORY;
	policy[field_index].type = (enum blobmsg_type)type;
	return UBUS_STATUS_OK;
}

int sy_ubus_object_add(struct sy_ubus_object *object)
{
	int status;

	if (!object || !object->bridge || !object->bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	status = ubus_add_object(object->bridge->context, &object->object);
	if (status) {
		sy_unregister_or_forget_object(object->bridge, &object->object);
		return status;
	}
	list_add_tail(&object->list, &object->bridge->objects);
	return UBUS_STATUS_OK;
}

void sy_ubus_object_discard(struct sy_ubus_object *object)
{
	sy_free_object(object);
}

int sy_ubus_object_remove(struct sy_ubus_bridge *bridge, uint64_t object_id)
{
	struct sy_ubus_object *object;
	int status = UBUS_STATUS_OK;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	object = sy_find_object(bridge, object_id);
	if (!object)
		return UBUS_STATUS_NOT_FOUND;
	if (bridge->connected)
		status = ubus_remove_object(bridge->context, &object->object);
	if (status)
		return status;
	if (!bridge->connected)
		sy_forget_native_object(bridge->context, &object->object);
	list_del(&object->list);
	sy_free_object(object);
	return UBUS_STATUS_OK;
}

static void sy_lookup_callback(struct ubus_context *context,
	struct ubus_object_data *object, void *private_data)
{
	struct sy_lookup_data *lookup = private_data;
	char *signature = sy_json_from_blob(object->signature);

	(void)context;
	goSyUbusLookupObject(lookup->bridge->handle, lookup->operation_id,
		object->id, object->type_id, sy_nonnull(object->path), signature);
	free(signature);
}

int sy_ubus_lookup(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	const char *path)
{
	struct sy_lookup_data lookup = {
		.bridge = bridge,
		.operation_id = operation_id,
	};

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	return ubus_lookup(bridge->context, path && *path ? path : NULL,
		sy_lookup_callback, &lookup);
}

static void sy_invoke_data_callback(struct ubus_request *native_request,
	int type, struct blob_attr *message)
{
	struct sy_ubus_request *request = container_of(native_request,
		struct sy_ubus_request, request);

	(void)type;
	free(request->json);
	request->json = sy_json_from_blob(message);
}

static void sy_invoke_fd_callback(struct ubus_request *native_request, int fd)
{
	struct sy_ubus_request *request = container_of(native_request,
		struct sy_ubus_request, request);

	if (request->fd >= 0)
		close(request->fd);
	fcntl(fd, F_SETFD, fcntl(fd, F_GETFD) | FD_CLOEXEC);
	request->fd = fd;
}

static void sy_invoke_complete_callback(struct ubus_request *native_request,
	int status)
{
	struct sy_ubus_request *request = container_of(native_request,
		struct sy_ubus_request, request);
	int fd = request->fd;

	list_del(&request->list);
	request->fd = -1;
	goSyUbusInvokeComplete(request->bridge->handle, request->operation_id,
		status, sy_nonnull(request->json), fd);
	free(request->json);
	free(request);
}

int sy_ubus_invoke(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	const char *object, const char *method, const char *json, int fd)
{
	struct sy_ubus_request *request;
	struct blob_buf buffer;
	uint32_t object_id = 0;
	int send_fd = -1;
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	status = ubus_lookup_id(bridge->context, object, &object_id);
	if (status)
		return status;
	status = sy_blob_from_json(json, &buffer);
	if (status)
		return status;
	request = calloc(1, sizeof(*request));
	if (!request) {
		blob_buf_free(&buffer);
		return UBUS_STATUS_NO_MEMORY;
	}
	request->bridge = bridge;
	request->operation_id = operation_id;
	request->fd = -1;
	if (fd >= 0) {
		send_fd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
		if (send_fd < 0) {
			blob_buf_free(&buffer);
			free(request);
			return UBUS_STATUS_SYSTEM_ERROR;
		}
	}
	status = ubus_invoke_async_fd(bridge->context, object_id, method,
		buffer.head, &request->request, send_fd);
	if (send_fd >= 0)
		close(send_fd);
	blob_buf_free(&buffer);
	if (status) {
		free(request);
		return status;
	}
	request->request.data_cb = sy_invoke_data_callback;
	request->request.fd_cb = sy_invoke_fd_callback;
	request->request.complete_cb = sy_invoke_complete_callback;
	list_add_tail(&request->list, &bridge->requests);
	ubus_complete_request_async(bridge->context, &request->request);
	return UBUS_STATUS_OK;
}

int sy_ubus_cancel_invoke(struct sy_ubus_bridge *bridge, uint64_t operation_id)
{
	struct sy_ubus_request *request;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	request = sy_find_request(bridge, operation_id);
	if (!request)
		return UBUS_STATUS_NOT_FOUND;
	ubus_abort_request(bridge->context, &request->request);
	list_del(&request->list);
	if (request->fd >= 0)
		close(request->fd);
	free(request->json);
	free(request);
	return UBUS_STATUS_OK;
}

int sy_ubus_complete_incoming(struct sy_ubus_bridge *bridge, uint64_t request_id,
	int status, const char *json, int fd)
{
	struct sy_ubus_deferred *deferred;
	struct blob_buf buffer;
	int send_fd = -1;
	int parse_status;
	bool buffer_ready = false;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	list_for_each_entry(deferred, &bridge->deferred, list)
		if (deferred->request_id == request_id)
			goto found;
	return UBUS_STATUS_NOT_FOUND;

found:
	if (!bridge->connected) {
		list_del(&deferred->list);
		free(deferred);
		return UBUS_STATUS_CONNECTION_FAILED;
	}
	parse_status = sy_blob_from_json(json, &buffer);
	if (!parse_status) {
		buffer_ready = true;
		parse_status = ubus_send_reply(bridge->context, &deferred->request,
			buffer.head);
	}
	if (!parse_status && fd >= 0) {
		send_fd = fcntl(fd, F_DUPFD_CLOEXEC, 0);
		if (send_fd < 0)
			parse_status = UBUS_STATUS_SYSTEM_ERROR;
		else
			ubus_request_set_fd(bridge->context, &deferred->request, send_fd);
	}
	if (parse_status)
		status = parse_status;
	/*
	 * ubus_complete_deferred_request() takes ownership of req->fd and closes
	 * it. Do not close send_fd here: a second close could hit a reused fd.
	 */
	ubus_complete_deferred_request(bridge->context, &deferred->request, status);
	if (buffer_ready)
		blob_buf_free(&buffer);
	list_del(&deferred->list);
	free(deferred);
	return UBUS_STATUS_OK;
}

static void sy_event_callback(struct ubus_context *context,
	struct ubus_event_handler *native_handler, const char *type,
	struct blob_attr *message)
{
	struct sy_ubus_event *event = container_of(native_handler,
		struct sy_ubus_event, handler);
	char *json = sy_json_from_blob(message);

	(void)context;
	goSyUbusEvent(event->bridge->handle, event->listener_id,
		sy_nonnull(type), json);
	free(json);
}

int sy_ubus_event_add(struct sy_ubus_bridge *bridge, uint64_t listener_id,
	const char *pattern)
{
	struct sy_ubus_event *event;
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	event = calloc(1, sizeof(*event));
	if (!event)
		return UBUS_STATUS_NO_MEMORY;
	event->pattern = strdup(sy_nonnull(pattern));
	if (!event->pattern) {
		free(event);
		return UBUS_STATUS_NO_MEMORY;
	}
	event->bridge = bridge;
	event->listener_id = listener_id;
	event->handler.cb = sy_event_callback;
	status = ubus_register_event_handler(bridge->context, &event->handler,
		event->pattern[0] ? event->pattern : NULL);
	if (status) {
		sy_unregister_or_forget_object(bridge, &event->handler.obj);
		free(event->pattern);
		free(event);
		return status;
	}
	list_add_tail(&event->list, &bridge->events);
	return UBUS_STATUS_OK;
}

int sy_ubus_event_remove(struct sy_ubus_bridge *bridge, uint64_t listener_id)
{
	struct sy_ubus_event *event;
	int status = UBUS_STATUS_OK;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	list_for_each_entry(event, &bridge->events, list)
		if (event->listener_id == listener_id)
			goto found;
	return UBUS_STATUS_NOT_FOUND;

found:
	if (bridge->connected)
		status = ubus_unregister_event_handler(bridge->context, &event->handler);
	if (status)
		return status;
	if (!bridge->connected)
		sy_forget_native_object(bridge->context, &event->handler.obj);
	list_del(&event->list);
	free(event->pattern);
	free(event);
	return UBUS_STATUS_OK;
}

int sy_ubus_event_send(struct sy_ubus_bridge *bridge, const char *name,
	const char *json)
{
	struct blob_buf buffer;
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	status = sy_blob_from_json(json, &buffer);
	if (status)
		return status;
	status = ubus_send_event(bridge->context, name, buffer.head);
	blob_buf_free(&buffer);
	return status;
}

static int sy_subscription_callback(struct ubus_context *context,
	struct ubus_object *native_object, struct ubus_request_data *request,
	const char *method, struct blob_attr *message)
{
	struct ubus_subscriber *native_subscriber = container_of(native_object,
		struct ubus_subscriber, obj);
	struct sy_ubus_subscription *subscription = container_of(native_subscriber,
		struct sy_ubus_subscription, subscriber);

	(void)context;
	return sy_dispatch_incoming(subscription->bridge,
		subscription->subscription_id, SY_INCOMING_NOTIFICATION,
		subscription->target_name, request, method, message);
}

static void sy_schedule_subscription_retry(struct sy_ubus_bridge *bridge)
{
	if (!bridge->closing)
		uloop_timeout_set(&bridge->subscription_timer,
			bridge->reconnect_delay_ms);
}

static void sy_subscription_removed(struct ubus_context *context,
	struct ubus_subscriber *native_subscriber, uint32_t target_id)
{
	struct sy_ubus_subscription *subscription = container_of(native_subscriber,
		struct sy_ubus_subscription, subscriber);

	(void)context;
	subscription->target_id = 0;
	goSyUbusSubscriptionState(subscription->bridge->handle,
		subscription->subscription_id, 0, target_id, UBUS_STATUS_NOT_FOUND);
	sy_schedule_subscription_retry(subscription->bridge);
}

static int sy_subscribe_target(struct sy_ubus_subscription *subscription,
	bool report_failure)
{
	uint32_t target_id = 0;
	int status;

	status = ubus_lookup_id(subscription->bridge->context,
		subscription->target_name, &target_id);
	if (!status)
		status = ubus_subscribe(subscription->bridge->context,
			&subscription->subscriber, target_id);
	if (!status) {
		subscription->target_id = target_id;
		goSyUbusSubscriptionState(subscription->bridge->handle,
			subscription->subscription_id, 1, target_id, UBUS_STATUS_OK);
	} else if (report_failure) {
		goSyUbusSubscriptionState(subscription->bridge->handle,
			subscription->subscription_id, 0, 0, status);
	}
	return status;
}

static void sy_subscription_retry_callback(struct uloop_timeout *timeout)
{
	struct sy_ubus_bridge *bridge = container_of(timeout,
		struct sy_ubus_bridge, subscription_timer);
	struct sy_ubus_subscription *subscription;
	bool retry = false;

	if (!bridge->connected || bridge->closing)
		return;
	list_for_each_entry(subscription, &bridge->subscriptions, list) {
		if (subscription->target_id)
			continue;
		if (sy_subscribe_target(subscription, false))
			retry = true;
	}
	if (retry)
		sy_schedule_subscription_retry(bridge);
}

int sy_ubus_subscription_add(struct sy_ubus_bridge *bridge,
	uint64_t subscription_id, const char *object)
{
	struct sy_ubus_subscription *subscription;
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	subscription = calloc(1, sizeof(*subscription));
	if (!subscription)
		return UBUS_STATUS_NO_MEMORY;
	subscription->target_name = strdup(object);
	if (!subscription->target_name) {
		free(subscription);
		return UBUS_STATUS_NO_MEMORY;
	}
	subscription->bridge = bridge;
	subscription->subscription_id = subscription_id;
	subscription->subscriber.cb = sy_subscription_callback;
	subscription->subscriber.remove_cb = sy_subscription_removed;
	status = ubus_register_subscriber(bridge->context,
		&subscription->subscriber);
	if (!status)
		status = sy_subscribe_target(subscription, true);
	if (status) {
		sy_unregister_or_forget_object(bridge,
			&subscription->subscriber.obj);
		free(subscription->target_name);
		free(subscription);
		return status;
	}
	list_add_tail(&subscription->list, &bridge->subscriptions);
	return UBUS_STATUS_OK;
}

int sy_ubus_subscription_remove(struct sy_ubus_bridge *bridge,
	uint64_t subscription_id)
{
	struct sy_ubus_subscription *subscription;
	int status = UBUS_STATUS_OK;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	list_for_each_entry(subscription, &bridge->subscriptions, list)
		if (subscription->subscription_id == subscription_id)
			goto found;
	return UBUS_STATUS_NOT_FOUND;

found:
	if (bridge->connected && subscription->target_id)
		status = ubus_unsubscribe(bridge->context, &subscription->subscriber,
			subscription->target_id);
	if (!status && bridge->connected)
		status = ubus_unregister_subscriber(bridge->context,
			&subscription->subscriber);
	if (status)
		return status;
	if (!bridge->connected)
		sy_forget_native_object(bridge->context,
			&subscription->subscriber.obj);
	list_del(&subscription->list);
	free(subscription->target_name);
	free(subscription);
	return UBUS_STATUS_OK;
}

static void sy_notify_data_callback(struct ubus_notify_request *native_request,
	int type, struct blob_attr *message)
{
	struct sy_ubus_notify_request *request = container_of(native_request,
		struct sy_ubus_notify_request, request);
	char *json;

	(void)type;
	json = sy_json_from_blob(message);
	goSyUbusNotifyReply(request->bridge->handle, request->operation_id,
		sy_nonnull(json));
	free(json);
}

static void sy_notify_status_callback(struct ubus_notify_request *native_request,
	int index, int status)
{
	struct sy_ubus_notify_request *request = container_of(native_request,
		struct sy_ubus_notify_request, request);
	uint32_t subscriber = 0;

	if (index >= 0 && index <= UBUS_MAX_NOTIFY_PEERS)
		subscriber = native_request->id[index];
	goSyUbusNotifyResult(request->bridge->handle, request->operation_id,
		subscriber, status);
}

static void sy_notify_complete_callback(struct ubus_notify_request *native_request,
	int index, int status)
{
	struct sy_ubus_notify_request *request = container_of(native_request,
		struct sy_ubus_notify_request, request);

	(void)index;
	list_del(&request->list);
	goSyUbusNotifyComplete(request->bridge->handle, request->operation_id,
		status);
	free(request);
}

int sy_ubus_notify(struct sy_ubus_bridge *bridge, uint64_t operation_id,
	uint64_t publication_id, const char *type, const char *json, int wait)
{
	struct sy_ubus_object *object;
	struct sy_ubus_notify_request *request;
	struct blob_buf buffer;
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	object = sy_find_object(bridge, publication_id);
	if (!object)
		return UBUS_STATUS_NOT_FOUND;
	status = sy_blob_from_json(json, &buffer);
	if (status)
		return status;
	if (!wait) {
		status = ubus_notify(bridge->context, &object->object, type,
			buffer.head, -1);
		blob_buf_free(&buffer);
		return status;
	}
	request = calloc(1, sizeof(*request));
	if (!request) {
		blob_buf_free(&buffer);
		return UBUS_STATUS_NO_MEMORY;
	}
	request->bridge = bridge;
	request->operation_id = operation_id;
	status = ubus_notify_async(bridge->context, &object->object, type,
		buffer.head, &request->request);
	blob_buf_free(&buffer);
	if (status) {
		free(request);
		return status;
	}
	request->request.status_cb = sy_notify_status_callback;
	request->request.data_cb = sy_notify_data_callback;
	request->request.complete_cb = sy_notify_complete_callback;
	list_add_tail(&request->list, &bridge->notifications);
	ubus_complete_request_async(bridge->context, &request->request.req);
	return UBUS_STATUS_OK;
}

int sy_ubus_cancel_notify(struct sy_ubus_bridge *bridge, uint64_t operation_id)
{
	struct sy_ubus_notify_request *request;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	request = sy_find_notification(bridge, operation_id);
	if (!request)
		return UBUS_STATUS_NOT_FOUND;
	ubus_abort_request(bridge->context, &request->request.req);
	list_del(&request->list);
	free(request);
	return UBUS_STATUS_OK;
}

static char *sy_monitor_data_json(struct blob_attr *data)
{
	static const struct blob_attr_info policy[UBUS_ATTR_MAX] = {
		[UBUS_ATTR_STATUS] = { .type = BLOB_ATTR_INT32 },
		[UBUS_ATTR_OBJPATH] = { .type = BLOB_ATTR_STRING },
		[UBUS_ATTR_OBJID] = { .type = BLOB_ATTR_INT32 },
		[UBUS_ATTR_METHOD] = { .type = BLOB_ATTR_STRING },
		[UBUS_ATTR_OBJTYPE] = { .type = BLOB_ATTR_INT32 },
		[UBUS_ATTR_SIGNATURE] = { .type = BLOB_ATTR_NESTED },
		[UBUS_ATTR_DATA] = { .type = BLOB_ATTR_NESTED },
		[UBUS_ATTR_ACTIVE] = { .type = BLOB_ATTR_INT8 },
		[UBUS_ATTR_NO_REPLY] = { .type = BLOB_ATTR_INT8 },
		[UBUS_ATTR_USER] = { .type = BLOB_ATTR_STRING },
		[UBUS_ATTR_GROUP] = { .type = BLOB_ATTR_STRING },
	};
	static const char *const names[UBUS_ATTR_MAX] = {
		[UBUS_ATTR_STATUS] = "status",
		[UBUS_ATTR_OBJPATH] = "objpath",
		[UBUS_ATTR_OBJID] = "objid",
		[UBUS_ATTR_METHOD] = "method",
		[UBUS_ATTR_OBJTYPE] = "objtype",
		[UBUS_ATTR_SIGNATURE] = "signature",
		[UBUS_ATTR_DATA] = "data",
		[UBUS_ATTR_ACTIVE] = "active",
		[UBUS_ATTR_NO_REPLY] = "no_reply",
		[UBUS_ATTR_USER] = "user",
		[UBUS_ATTR_GROUP] = "group",
	};
	struct blob_attr *attributes[UBUS_ATTR_MAX];
	struct blob_buf output = {};
	char *json;
	int index;

	memset(attributes, 0, sizeof(attributes));
	blob_parse(data, attributes, policy, UBUS_ATTR_MAX);
	blob_buf_init(&output, 0);
	for (index = 0; index < UBUS_ATTR_MAX; index++) {
		struct blob_attr *value = attributes[index];
		const char *name = names[index];

		if (!value || !name)
			continue;
		switch (policy[index].type) {
		case BLOB_ATTR_INT32:
			blobmsg_add_u32(&output, name, blob_get_int32(value));
			break;
		case BLOB_ATTR_STRING:
			blobmsg_add_string(&output, name, blob_data(value));
			break;
		case BLOB_ATTR_INT8:
			blobmsg_add_u8(&output, name, !!blob_get_int8(value));
			break;
		case BLOB_ATTR_NESTED:
			blobmsg_add_field(&output, BLOBMSG_TYPE_TABLE, name,
				blobmsg_data(value), blobmsg_data_len(value));
			break;
		}
	}
	json = blobmsg_format_json(output.head, true);
	blob_buf_free(&output);
	return json ? json : strdup("{}");
}

static void sy_monitor_callback(struct ubus_context *context, uint32_t sequence,
	struct blob_attr *message)
{
	static const struct blob_attr_info policy[UBUS_MONITOR_MAX] = {
		[UBUS_MONITOR_CLIENT] = { .type = BLOB_ATTR_INT32 },
		[UBUS_MONITOR_PEER] = { .type = BLOB_ATTR_INT32 },
		[UBUS_MONITOR_SEND] = { .type = BLOB_ATTR_INT8 },
		[UBUS_MONITOR_TYPE] = { .type = BLOB_ATTR_INT32 },
		[UBUS_MONITOR_DATA] = { .type = BLOB_ATTR_NESTED },
	};
	struct sy_ubus_bridge *bridge;
	struct blob_attr *attributes[UBUS_MONITOR_MAX];
	char *json;

	(void)context;
	memset(attributes, 0, sizeof(attributes));
	blob_parse_untrusted(message, blob_raw_len(message), attributes, policy,
		UBUS_MONITOR_MAX);
	if (!attributes[UBUS_MONITOR_CLIENT] || !attributes[UBUS_MONITOR_PEER] ||
	    !attributes[UBUS_MONITOR_SEND] || !attributes[UBUS_MONITOR_TYPE] ||
	    !attributes[UBUS_MONITOR_DATA])
		return;
	/* The active bridge is unique because uloop and Conn are process-global. */
	bridge = sy_active_bridge;
	if (!bridge)
		return;
	json = sy_monitor_data_json(attributes[UBUS_MONITOR_DATA]);
	goSyUbusMonitor(bridge->handle, sequence,
		blob_get_int32(attributes[UBUS_MONITOR_CLIENT]),
		blob_get_int32(attributes[UBUS_MONITOR_PEER]),
		!!blob_get_int8(attributes[UBUS_MONITOR_SEND]),
		blob_get_int32(attributes[UBUS_MONITOR_TYPE]), json);
	free(json);
}

int sy_ubus_monitor_start(struct sy_ubus_bridge *bridge)
{
	int status;

	if (!bridge || !bridge->connected)
		return UBUS_STATUS_CONNECTION_FAILED;
	if (bridge->monitor_active)
		return UBUS_STATUS_INVALID_ARGUMENT;
	bridge->context->monitor_cb = sy_monitor_callback;
	status = ubus_monitor_start(bridge->context);
	if (status) {
		bridge->context->monitor_cb = NULL;
		return status;
	}
	bridge->monitor_active = true;
	return UBUS_STATUS_OK;
}

int sy_ubus_monitor_stop(struct sy_ubus_bridge *bridge)
{
	int status;

	if (!bridge)
		return UBUS_STATUS_CONNECTION_FAILED;
	if (!bridge->monitor_active)
		return UBUS_STATUS_OK;
	if (!bridge->connected) {
		bridge->monitor_active = false;
		bridge->context->monitor_cb = NULL;
		return UBUS_STATUS_OK;
	}
	status = ubus_monitor_stop(bridge->context);
	if (!status) {
		bridge->monitor_active = false;
		bridge->context->monitor_cb = NULL;
	}
	return status;
}

static void sy_fail_pending(struct sy_ubus_bridge *bridge, int status)
{
	struct sy_ubus_request *request, *request_tmp;
	struct sy_ubus_notify_request *notification, *notification_tmp;
	struct sy_ubus_deferred *deferred, *deferred_tmp;

	list_for_each_entry_safe(request, request_tmp, &bridge->requests, list) {
		ubus_abort_request(bridge->context, &request->request);
		list_del(&request->list);
		goSyUbusInvokeComplete(bridge->handle, request->operation_id, status,
			sy_nonnull(request->json), request->fd);
		request->fd = -1;
		free(request->json);
		free(request);
	}
	list_for_each_entry_safe(notification, notification_tmp,
		&bridge->notifications, list) {
		ubus_abort_request(bridge->context, &notification->request.req);
		list_del(&notification->list);
		goSyUbusNotifyComplete(bridge->handle, notification->operation_id,
			status);
		free(notification);
	}
	list_for_each_entry_safe(deferred, deferred_tmp, &bridge->deferred, list) {
		list_del(&deferred->list);
		free(deferred);
	}
}

static void sy_restore_registrations(struct sy_ubus_bridge *bridge)
{
	struct sy_ubus_event *event;
	struct sy_ubus_subscription *subscription;
	bool retry = false;

	list_for_each_entry(event, &bridge->events, list)
		ubus_register_event_handler(bridge->context, &event->handler,
			event->pattern[0] ? event->pattern : NULL);
	list_for_each_entry(subscription, &bridge->subscriptions, list) {
		subscription->target_id = 0;
		if (sy_subscribe_target(subscription, true))
			retry = true;
	}
	if (retry)
		sy_schedule_subscription_retry(bridge);
	if (bridge->monitor_active) {
		bridge->context->monitor_cb = sy_monitor_callback;
		ubus_monitor_start(bridge->context);
	}
}

static void sy_reconnect_callback(struct uloop_timeout *timeout)
{
	struct sy_ubus_bridge *bridge = container_of(timeout,
		struct sy_ubus_bridge, reconnect_timer);
	int status;

	if (bridge->closing)
		return;
	status = ubus_reconnect(bridge->context, bridge->socket_path);
	if (status) {
		uloop_timeout_set(&bridge->reconnect_timer,
			bridge->reconnect_delay_ms);
		return;
	}
	ubus_add_uloop(bridge->context);
	bridge->connected = true;
	sy_restore_registrations(bridge);
	goSyUbusConnectionState(bridge->handle, 1, UBUS_STATUS_OK);
}

static void sy_connection_lost(struct ubus_context *context)
{
	struct sy_ubus_bridge *bridge = sy_active_bridge;

	if (!bridge || bridge->context != context)
		return;
	bridge->connected = false;
	sy_fail_pending(bridge, UBUS_STATUS_CONNECTION_FAILED);
	goSyUbusConnectionState(bridge->handle, 0,
		UBUS_STATUS_CONNECTION_FAILED);
	if (bridge->closing || !bridge->reconnect) {
		uloop_end();
		return;
	}
	uloop_timeout_set(&bridge->reconnect_timer, bridge->reconnect_delay_ms);
}

struct sy_ubus_bridge *sy_ubus_bridge_new(uintptr_t handle,
	const char *socket_path, int reconnect, int reconnect_delay_ms, int *status)
{
	struct sy_ubus_bridge *bridge;

	if (status)
		*status = UBUS_STATUS_UNKNOWN_ERROR;
	bridge = calloc(1, sizeof(*bridge));
	if (!bridge) {
		if (status)
			*status = UBUS_STATUS_NO_MEMORY;
		return NULL;
	}
	bridge->handle = handle;
	bridge->wake.fd = -1;
	bridge->reconnect = !!reconnect;
	bridge->reconnect_delay_ms = reconnect_delay_ms > 0 ? reconnect_delay_ms : 1000;
	if (socket_path && *socket_path) {
		bridge->socket_path = strdup(socket_path);
		if (!bridge->socket_path) {
			free(bridge);
			if (status)
				*status = UBUS_STATUS_NO_MEMORY;
			return NULL;
		}
	}
	INIT_LIST_HEAD(&bridge->requests);
	INIT_LIST_HEAD(&bridge->deferred);
	INIT_LIST_HEAD(&bridge->objects);
	INIT_LIST_HEAD(&bridge->events);
	INIT_LIST_HEAD(&bridge->subscriptions);
	INIT_LIST_HEAD(&bridge->notifications);
	bridge->context = ubus_connect(bridge->socket_path);
	if (!bridge->context) {
		uloop_done();
		free(bridge->socket_path);
		free(bridge);
		if (status)
			*status = UBUS_STATUS_CONNECTION_FAILED;
		return NULL;
	}
	bridge->wake.fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
	if (bridge->wake.fd < 0) {
		ubus_free(bridge->context);
		uloop_done();
		free(bridge->socket_path);
		free(bridge);
		if (status)
			*status = UBUS_STATUS_SYSTEM_ERROR;
		return NULL;
	}
	bridge->wake.cb = sy_wake_callback;
	bridge->reconnect_timer.cb = sy_reconnect_callback;
	bridge->subscription_timer.cb = sy_subscription_retry_callback;
	bridge->context->connection_lost = sy_connection_lost;
	bridge->connected = true;
	sy_active_bridge = bridge;
	ubus_add_uloop(bridge->context);
	uloop_fd_add(&bridge->wake, ULOOP_READ);
	if (status)
		*status = UBUS_STATUS_OK;
	return bridge;
}

void sy_ubus_bridge_run(struct sy_ubus_bridge *bridge)
{
	if (bridge)
		uloop_run();
}

void sy_ubus_bridge_stop(struct sy_ubus_bridge *bridge)
{
	if (!bridge || bridge->closing)
		return;
	bridge->closing = true;
	uloop_timeout_cancel(&bridge->reconnect_timer);
	uloop_timeout_cancel(&bridge->subscription_timer);
	sy_fail_pending(bridge, UBUS_STATUS_CONNECTION_FAILED);
	uloop_end();
}

void sy_ubus_bridge_free(struct sy_ubus_bridge *bridge)
{
	struct sy_ubus_object *object, *object_tmp;
	struct sy_ubus_event *event, *event_tmp;
	struct sy_ubus_subscription *subscription, *subscription_tmp;

	if (!bridge)
		return;
	if (sy_active_bridge == bridge)
		sy_active_bridge = NULL;
	uloop_timeout_cancel(&bridge->reconnect_timer);
	uloop_timeout_cancel(&bridge->subscription_timer);
	if (bridge->wake.registered)
		uloop_fd_delete(&bridge->wake);
	if (bridge->wake.fd >= 0)
		close(bridge->wake.fd);
	bridge->context->monitor_cb = NULL;
	ubus_free(bridge->context);
	list_for_each_entry_safe(object, object_tmp, &bridge->objects, list) {
		list_del(&object->list);
		sy_free_object(object);
	}
	list_for_each_entry_safe(event, event_tmp, &bridge->events, list) {
		list_del(&event->list);
		free(event->pattern);
		free(event);
	}
	list_for_each_entry_safe(subscription, subscription_tmp,
		&bridge->subscriptions, list) {
		list_del(&subscription->list);
		free(subscription->target_name);
		free(subscription);
	}
	uloop_done();
	free(bridge->socket_path);
	free(bridge);
}
