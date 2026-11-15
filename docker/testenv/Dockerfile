# Pinned native test environment for ubus-go-bindings.
# OpenWrt ubus/libubox revisions are pinned in the build args below.
FROM golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0 AS native

ARG DEBIAN_FRONTEND=noninteractive
ARG LIBUBOX_COMMIT=49056d178f42da98048a5d4c23f83a6f6bc6dd80
ARG UBUS_COMMIT=60e04048a0e2f3e33651c19e62861b41be4c290f

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        binutils \
        cmake \
        git \
        libjson-c-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
RUN git clone https://github.com/openwrt/libubox.git libubox \
    && git -C libubox checkout "${LIBUBOX_COMMIT}" \
    && cmake -S libubox -B libubox/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DBUILD_LUA=OFF \
        -DBUILD_EXAMPLES=OFF \
    && cmake --build libubox/build -j"$(nproc)" \
    && cmake --install libubox/build

RUN git clone https://github.com/openwrt/ubus.git ubus \
    && git -C ubus checkout "${UBUS_COMMIT}" \
    && cmake -S ubus -B ubus/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DCMAKE_PREFIX_PATH=/usr/local \
        -DBUILD_LUA=OFF \
        -DBUILD_EXAMPLES=OFF \
    && cmake --build ubus/build -j"$(nproc)" \
    && cmake --install ubus/build

FROM golang:1.27.3-bookworm@sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        binutils \
        cmake \
        libjson-c-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY --from=native /usr/local /usr/local
RUN ldconfig \
    && useradd --create-home --uid 1000 --shell /bin/bash ubus \
    && mkdir -p /tmp/ubus-build \
    && chown ubus:ubus /tmp/ubus-build

ENV CGO_ENABLED=1 \
    GOFLAGS=-mod=readonly \
    GOCACHE=/tmp/ubus-build/go-cache \
    GOMODCACHE=/tmp/ubus-build/go-mod \
    GOPATH=/tmp/ubus-build/go \
    PATH=/usr/local/bin:/usr/local/sbin:/usr/local/go/bin:/usr/bin:/bin

USER ubus
WORKDIR /src

# Print the exact native and compiler versions in build logs and README checks.
RUN printf 'gcc: '; gcc --version | head -1; \
    printf 'go: '; go version; \
    printf 'ubus: '; strings /usr/local/lib/libubus.so | grep -m1 -E '^20[0-9]{2}-[0-9]{2}-[0-9]{2}' || true
