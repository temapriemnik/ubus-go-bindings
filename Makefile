SHELL := /bin/sh

GO ?= go
GOFMT ?= gofumpt
DOCKER ?= docker
IMAGE ?= ubus-go-bindings-test:dev
DOCKERFILE ?= docker/testenv/Dockerfile
CROSS_GOARCH ?= arm64
CROSS_CC ?=
CROSS_CFLAGS ?=
CROSS_LDFLAGS ?=

# Native versions pinned by the test image and documented in README.md.
LIBUBOX_COMMIT := 49056d178f42da98048a5d4c23f83a6f6bc6dd80
UBUS_COMMIT := 60e04048a0e2f3e33651c19e62861b41be4c290f

.PHONY: all build cross test test-nocgo test-integration testenv fmt fmt-check vet check clean

all: check

build: testenv
	$(DOCKER) run --rm -v "$(CURDIR):/src:ro" -w /src $(IMAGE) go build ./...

# Example:
#   make cross CROSS_CC=$STAGING_DIR/toolchain-*/bin/aarch64-openwrt-linux-musl-gcc \
#     CROSS_CFLAGS=-I$STAGING_DIR/target-*/usr/include \
#     CROSS_LDFLAGS=-L$STAGING_DIR/target-*/usr/lib
cross:
	@test -n "$(CROSS_CC)" || { echo "CROSS_CC is required" >&2; exit 1; }
	CGO_ENABLED=1 GOOS=linux GOARCH=$(CROSS_GOARCH) CC="$(CROSS_CC)" \
		CGO_CFLAGS="$(CROSS_CFLAGS)" CGO_LDFLAGS="$(CROSS_LDFLAGS)" \
		$(GO) build ./...

testenv:
	$(DOCKER) build \
		--build-arg LIBUBOX_COMMIT=$(LIBUBOX_COMMIT) \
		--build-arg UBUS_COMMIT=$(UBUS_COMMIT) \
		-t $(IMAGE) \
		-f $(DOCKERFILE) .

# Full test suite, including ubusd/libubus integration, inside the pinned image.
test test-integration: testenv
	$(DOCKER) run --rm --user 0:0 -v "$(CURDIR):/src:ro" -w /src $(IMAGE) ./scripts/test-in-docker.sh

# Verify the unsupported build (no cgo / no native headers) on the host.
test-nocgo:
	CGO_ENABLED=0 $(GO) test ./...

fmt:
	$(GOFMT) -l -w .

fmt-check:
	@test -z "$$($(GOFMT) -l .)" || { $(GOFMT) -l .; exit 1; }

vet: testenv
	$(DOCKER) run --rm -v "$(CURDIR):/src:ro" -w /src $(IMAGE) go vet -tags=integration ./...

check: fmt-check test-nocgo test vet

clean:
	$(DOCKER) image rm -f $(IMAGE) 2>/dev/null || true
