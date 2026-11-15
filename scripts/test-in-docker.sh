#!/bin/sh
set -eu

# The integration tests expect a native ubusd/ubus build and a small C peer.
# Keep this script usable from the pinned golang:*-bookworm test image.
cc -O2 -Wall -Wextra -o /tmp/ubus-fixture /src/tests/testdata/fixture.c \
    -I/usr/local/include -L/usr/local/lib \
    -lubus -lubox -lblobmsg_json -ljson-c

export GO_UBUS_TEST_LOADER="${GO_UBUS_TEST_LOADER:-/lib64/ld-linux-x86-64.so.2}"
export GO_UBUS_TEST_LIBPATH="${GO_UBUS_TEST_LIBPATH:-/usr/local/lib}"
export GO_UBUS_TEST_UBUSD="${GO_UBUS_TEST_UBUSD:-/usr/local/sbin/ubusd}"
export GO_UBUS_TEST_UBUS="${GO_UBUS_TEST_UBUS:-/usr/local/bin/ubus}"
export GO_UBUS_TEST_FIXTURE="${GO_UBUS_TEST_FIXTURE:-/tmp/ubus-fixture}"

exec go test -race -count=1 -tags=integration ./...
