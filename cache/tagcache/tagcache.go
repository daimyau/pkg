package tagcache

import (
	"context"

	"github.com/go-kratos/kratos/pkg/cache/memcache"
	"github.com/go-kratos/kratos/pkg/cache/redis"
)

const (
	// Flag, 15(encoding) bit+ 17(compress) bit

	// FlagRAW default flag.
	FlagRAW = uint32(0)
	// FlagGOB gob encoding.
	FlagGOB = uint32(1) << 0
	// FlagJSON json encoding.
	FlagJSON = uint32(1) << 1
	// FlagProtobuf protobuf
	FlagProtobuf = uint32(1) << 2

	_flagEncoding = uint32(0xFFFF8000)

	// FlagGzip gzip compress.
	FlagGzip = uint32(1) << 15

	// left mv 31??? not work!!!
	flagLargeValue = uint32(1) << 30
)

// Tagcache Tagcache
type Tagcache struct {
	conn RedisConn
}

// Reply is the result of Get
type Reply struct {
	err    error
	item   *memcache.Item
	conn   RedisConn
	closed bool
}

// Replies is the result of GetMulti
type Replies struct {
	err       error
	items     map[string]*memcache.Item
	usedItems map[string]struct{}
	conn      RedisConn
	closed    bool
}

// New get a tagcache client
func New(cfg *redis.Config) (*Tagcache, error) {
	rc, err := NewRedisConn(cfg)
	if err != nil {
		return nil, err
	}

	return &Tagcache{conn: rc}, nil
}

// Close close connection
func (tc *Tagcache) Close() error {
	return tc.conn.Close()
}

// Conn direct get a connection
func (tc *Tagcache) Conn() RedisConn {
	return tc.conn
}

// Set writes the given item, unconditionally.
func (tc *Tagcache) Set(ctx context.Context, item *memcache.Item) (err error) {
	conn := tc.conn
	err = conn.SetContext(ctx, item)
	conn.Close()
	return
}

// Add writes the given item, if no value already exists for its key.
func (tc *Tagcache) Add(ctx context.Context, item *memcache.Item) (err error) {
	conn := tc.conn
	err = conn.AddContext(ctx, item)
	conn.Close()
	return
}

// Replace writes the given item, but only if the server *does* already hold data for this key.
func (tc *Tagcache) Replace(ctx context.Context, item *memcache.Item) (err error) {
	conn := tc.conn
	err = conn.ReplaceContext(ctx, item)
	conn.Close()
	return
}

// CompareAndSwap writes the given item that was previously returned by Get
func (tc *Tagcache) CompareAndSwap(ctx context.Context, item *memcache.Item) (err error) {
	conn := tc.conn
	err = conn.CompareAndSwapContext(ctx, item)
	conn.Close()
	return
}

// Get sends a command to the server for gets data.
func (tc *Tagcache) Get(ctx context.Context, key string) *Reply {
	conn := tc.conn
	item, err := conn.GetContext(ctx, key)
	if err != nil {
		conn.Close()
	}
	return &Reply{err: err, item: item, conn: tc.conn}
}

// GetMulti is a batch version of Get
func (tc *Tagcache) GetMulti(ctx context.Context, keys []string) (*Replies, error) {
	conn := tc.conn
	items, err := conn.GetMultiContext(ctx, keys)
	rs := &Replies{err: err, items: items, conn: tc.conn, usedItems: make(map[string]struct{}, len(keys))}
	if (err != nil) || (len(items) == 0) {
		rs.Close()
	}
	return rs, err
}

// Close close rows.
func (rs *Replies) Close() (err error) {
	if !rs.closed {
		err = rs.conn.Close()
		rs.closed = true
	}
	return
}

// Scan converts value, read from key in rows
func (rs *Replies) Scan(key string, v interface{}) (err error) {
	if rs.err != nil {
		return rs.err
	}
	item, ok := rs.items[key]
	if !ok {
		rs.Close()
		return memcache.ErrNotFound
	}
	rs.usedItems[key] = struct{}{}
	err = rs.conn.Scan(item, v)
	if (err != nil) || (len(rs.items) == len(rs.usedItems)) {
		rs.Close()
	}
	return
}

// Keys keys of result
func (rs *Replies) Keys() (keys []string) {
	keys = make([]string, 0, len(rs.items))
	for key := range rs.items {
		keys = append(keys, key)
	}
	return
}

// Touch updates the expiry for the given key.
func (tc *Tagcache) Touch(ctx context.Context, key string, timeout int32) (err error) {
	conn := tc.conn
	err = conn.TouchContext(ctx, key, timeout)
	conn.Close()
	return
}

// Delete deletes the item with the provided key.
func (tc *Tagcache) Delete(ctx context.Context, key string) (err error) {
	conn := tc.conn
	err = conn.DeleteContext(ctx, key)
	conn.Close()
	return
}

// Increment atomically increments key by delta.
func (tc *Tagcache) Increment(ctx context.Context, key string, delta uint64) (newValue uint64, err error) {
	conn := tc.conn
	newValue, err = conn.IncrementContext(ctx, key, delta)
	conn.Close()
	return
}

// Decrement atomically decrements key by delta.
func (tc *Tagcache) Decrement(ctx context.Context, key string, delta uint64) (newValue uint64, err error) {
	conn := tc.conn
	newValue, err = conn.DecrementContext(ctx, key, delta)
	conn.Close()
	return
}
