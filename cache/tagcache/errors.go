package tagcache

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound not found
	ErrNotFound = errors.New("tagcache: key not found")
	// ErrExists exists
	ErrExists = errors.New("tagcache: key exists")
	// ErrNotStored not stored
	ErrNotStored = errors.New("tagcache: key not stored")
	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("tagcache: compare-and-swap conflict")

	// ErrPoolExhausted is returned from a pool connection method (Store, Get,
	// Delete, IncrDecr, Err) when the maximum number of database connections
	// in the pool has been reached.
	ErrPoolExhausted = errors.New("tagcache: connection pool exhausted")
	// ErrPoolClosed pool closed
	ErrPoolClosed = errors.New("tagcache: connection pool closed")
	// ErrConnClosed conn closed
	ErrConnClosed = errors.New("tagcache: connection closed")
	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("tagcache: malformed key is too long or contains invalid characters")
	// ErrValueSize item value size must less than 1mb
	ErrValueSize = errors.New("tagcache: item value size must not greater than 1mb")
	// ErrStat stat error for monitor
	ErrStat = errors.New("tagcache unexpected errors")
	// ErrItem item nil.
	ErrItem = errors.New("tagcache: item object nil")
	// ErrItemObject object type Assertion failed
	ErrItemObject = errors.New("tagcache: item object protobuf type assertion failed")
)

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("tagcache: %s (possible server error or unsupported concurrent read by application)", string(pe))
}
