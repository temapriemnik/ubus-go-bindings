package ubus

import (
	"encoding/json"
	"os"
	"sync"
)

// ACLKey contains caller identity metadata attached by ubusd. Fields may be
// empty when the caller has no rpcd ACL identity.
type ACLKey struct {
	// User is the caller identity supplied by ubusd.
	User string
	// Group is the caller group supplied by ubusd.
	Group string
	// Object is the ACL object name supplied by ubusd.
	Object string
}

// Request is a copied native method or notification request. It never exposes
// libubus or blobmsg pointers.
type Request struct {
	// Object is the published or subscribed object name.
	Object string
	// Method is the invoked method or notification type.
	Method string
	// Peer is the connection-local native caller ID.
	Peer uint32
	// ACL contains optional caller identity metadata.
	ACL ACLKey
	// Body is a copied JSON representation of the native blobmsg payload.
	Body json.RawMessage

	fdMu sync.Mutex
	fd   *os.File
}

// TakeFD transfers ownership of the incoming descriptor to the handler. The
// caller must close the returned file. If TakeFD is not called, the library
// closes the descriptor after the handler returns.
func (r *Request) TakeFD() (*os.File, bool) {
	if r == nil {
		return nil, false
	}
	r.fdMu.Lock()
	defer r.fdMu.Unlock()
	if r.fd == nil {
		return nil, false
	}
	file := r.fd
	r.fd = nil
	return file, true
}

func (r *Request) closeFD() {
	r.fdMu.Lock()
	defer r.fdMu.Unlock()
	if r.fd != nil {
		_ = r.fd.Close()
		r.fd = nil
	}
}

// Reply is a method or notification response. Returning FD transfers ownership
// to the library, which closes it after the native bridge duplicates it or the
// completion attempt fails.
type Reply struct {
	// Body is encoded into the native blobmsg reply.
	Body json.RawMessage
	// FD is the optional descriptor whose ownership transfers to the library.
	FD *os.File
}
