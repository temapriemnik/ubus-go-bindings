// Package ubus provides a Go interface to OpenWrt's native libubus API.
//
// The package is a cgo binding over the official libubus/libubox headers; it
// does not implement the ubus wire protocol. A Conn owns one native ubus
// connection and serializes all libubus calls on its private uloop thread.
// Public methods are safe for concurrent use unless their documentation says
// otherwise.
//
// One Conn per process is supported because uloop is process-global. Native
// callbacks are copied into Go-owned data before user handlers run.
package ubus
