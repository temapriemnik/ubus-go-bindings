// A native libubus peer used only by the integration test suite.

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <libubox/blobmsg.h>
#include <libubox/uloop.h>
#include <libubus.h>

static struct ubus_context *fixture_context;
static struct ubus_subscriber fixture_subscriber;

enum {
	WATCH_ID,
	WATCH_MAX,
};

static const struct blobmsg_policy watch_policy[WATCH_MAX] = {
	[WATCH_ID] = { .name = "id", .type = BLOBMSG_TYPE_UNSPEC },
};

static int fixture_notification(struct ubus_context *context,
	struct ubus_object *object, struct ubus_request_data *request,
	const char *method, struct blob_attr *message)
{
	struct blob_buf reply = {};
	int status;

	(void)object;
	(void)method;
	(void)message;
	if (blob_buf_init(&reply, 0))
		return UBUS_STATUS_NO_MEMORY;
	blobmsg_add_string(&reply, "peer", "fixture");
	status = ubus_send_reply(context, request, reply.head);
	blob_buf_free(&reply);
	return status;
}

static int fixture_watch(struct ubus_context *context,
	struct ubus_object *object, struct ubus_request_data *request,
	const char *method, struct blob_attr *message)
{
	struct blob_attr *attributes[WATCH_MAX];
	uint64_t target_id;

	(void)object;
	(void)request;
	(void)method;
	blobmsg_parse(watch_policy, WATCH_MAX, attributes,
		blob_data(message), blob_len(message));
	if (!attributes[WATCH_ID])
		return UBUS_STATUS_INVALID_ARGUMENT;
	if (blobmsg_type(attributes[WATCH_ID]) == BLOBMSG_TYPE_INT32)
		target_id = blobmsg_get_u32(attributes[WATCH_ID]);
	else if (blobmsg_type(attributes[WATCH_ID]) == BLOBMSG_TYPE_INT64)
		target_id = blobmsg_get_u64(attributes[WATCH_ID]);
	else
		return UBUS_STATUS_INVALID_ARGUMENT;
	if (target_id > UINT32_MAX)
		return UBUS_STATUS_INVALID_ARGUMENT;
	return ubus_subscribe(context, &fixture_subscriber,
		(uint32_t)target_id);
}

static int fixture_emit(struct ubus_context *context,
	struct ubus_object *object, struct ubus_request_data *request,
	const char *method, struct blob_attr *message)
{
	(void)request;
	(void)method;
	return ubus_notify(context, object, "changed", message, 2000);
}

static const struct ubus_method fixture_methods[] = {
	UBUS_METHOD("watch", fixture_watch, watch_policy),
	UBUS_METHOD_NOARG("emit", fixture_emit),
};

static struct ubus_object_type fixture_type =
	UBUS_OBJECT_TYPE("fixture.control", fixture_methods);

static struct ubus_object fixture_object = {
	.name = "fixture.control",
	.type = &fixture_type,
	.methods = fixture_methods,
	.n_methods = ARRAY_SIZE(fixture_methods),
};

int main(int argc, char **argv)
{
	const char *socket_path = NULL;
	int option;
	int status;

	while ((option = getopt(argc, argv, "s:")) != -1) {
		if (option == 's')
			socket_path = optarg;
	}
	uloop_init();
	fixture_context = ubus_connect(socket_path);
	if (!fixture_context)
		return 1;
	ubus_add_uloop(fixture_context);
	fixture_subscriber.cb = fixture_notification;
	status = ubus_register_subscriber(fixture_context, &fixture_subscriber);
	if (!status)
		status = ubus_add_object(fixture_context, &fixture_object);
	if (status) {
		fprintf(stderr, "fixture registration failed: %s\n",
			ubus_strerror(status));
		ubus_free(fixture_context);
		uloop_done();
		return 1;
	}
	uloop_run();
	ubus_free(fixture_context);
	uloop_done();
	return 0;
}
