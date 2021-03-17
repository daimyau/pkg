package tagcache

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/pkg/cache/memcache"
	"github.com/go-kratos/kratos/pkg/cache/redis"
	xtime "github.com/go-kratos/kratos/pkg/time"
	pkgerr "github.com/pkg/errors"
)

var _ memcache.Conn = &RedisConn{}

type RedisConn struct {
	conn redis.Conn
	ed   *encodeDecode
}

func NewRedisConn(c *redis.Config) (RedisConn, error) {
	if c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 {
		panic("must config tagcache timeout")
	}
	if c.SlowLog <= 0 {
		c.SlowLog = xtime.Duration(250 * time.Millisecond)
	}

	ops := []redis.DialOption{
		redis.DialConnectTimeout(time.Duration(c.DialTimeout)),
		redis.DialReadTimeout(time.Duration(c.ReadTimeout)),
		redis.DialWriteTimeout(time.Duration(c.WriteTimeout)),
		redis.DialPassword(c.Auth),
		redis.DialDatabase(c.Db),
	}

	conn, err := redis.Dial(c.Proto, c.Addr, ops...)
	if err != nil {
		return RedisConn{}, err
	}

	return RedisConn{conn: conn, ed: newEncodeDecoder()}, err
}

// Err err.
func (rc *RedisConn) Err() error {
	return rc.conn.Err()
}

// Close closes the connection.
func (rc *RedisConn) Close() error {
	return rc.conn.Close()
}

// Add writes the given item, if no value already exists for its key.
func (rc *RedisConn) Add(item *memcache.Item) error {
	return rc.AddContext(context.TODO(), item)
}

// Set writes the given item, unconditionally.
func (rc *RedisConn) Set(item *memcache.Item) error {
	return rc.SetContext(context.TODO(), item)
}

// Replace writes the given item, but only if the server *does* already
// hold data for this key.
func (rc *RedisConn) Replace(item *memcache.Item) error {
	return rc.ReplaceContext(context.TODO(), item)
}

// Get sends a command to the server for gets data.
func (rc *RedisConn) Get(key string) (*memcache.Item, error) {
	return rc.GetContext(context.TODO(), key)
}

// GetMulti is a batch version of Get.
func (rc *RedisConn) GetMulti(keys []string) (map[string]*memcache.Item, error) {
	return rc.GetMultiContext(context.TODO(), keys)
}

// Delete deletes the item with the provided key.
func (rc *RedisConn) Delete(key string) error {
	return rc.DeleteContext(context.TODO(), key)
}

// Increment atomically increments key by delta.
func (rc *RedisConn) Increment(key string, delta uint64) (newValue uint64, err error) {
	return rc.IncrementContext(context.TODO(), key, delta)
}

// Decrement atomically decrements key by delta.
func (rc *RedisConn) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return rc.DecrementContext(context.TODO(), key, delta)
}

// CompareAndSwap writes the given item that was previously returned by
// Get, if the value was neither modified or evicted between the Get and
// the CompareAndSwap calls.
func (rc *RedisConn) CompareAndSwap(item *memcache.Item) error {
	return rc.CompareAndSwapContext(context.TODO(), item)
}

// Touch updates the expiry for the given key.
func (rc *RedisConn) Touch(key string, seconds int32) (err error) {
	return rc.TouchContext(context.TODO(), key, seconds)
}

// Scan converts value read from the memcache into the following
// common Go types and special types:
//
//    *string
//    *[]byte
//    *interface{}
//
func (rc *RedisConn) Scan(item *memcache.Item, v interface{}) (err error) {
	return pkgerr.WithStack(rc.ed.decode(item, v))
}

// Add writes the given item, if no value already exists for its key.
func (rc *RedisConn) AddContext(ctx context.Context, item *memcache.Item) error {
	_, err := rc.conn.Do("SET", item.Key, item.Value)
	return err
}

// Set writes the given item, unconditionally.
func (rc *RedisConn) SetContext(ctx context.Context, item *memcache.Item) error {
	_, err := rc.conn.Do("SET", item.Key, item.Value)
	return err
}

// Replace writes the given item, but only if the server *does* already
// hold data for this key.
func (rc *RedisConn) ReplaceContext(ctx context.Context, item *memcache.Item) error {
	_, err := rc.conn.Do("SET", item.Key, item.Value)
	return err
}

// Get sends a command to the server for gets data.
func (rc *RedisConn) GetContext(ctx context.Context, key string) (*memcache.Item, error) {
	if !legalKey(key) {
		return nil, memcache.ErrMalformedKey
	}

	result, err := redis.Bytes(rc.conn.Do("GET", key))
	if err != nil {
		return nil, err
	}

	return &memcache.Item{Key: key, Value: result}, nil
}

// GetMulti is a batch version of Get.
func (rc *RedisConn) GetMultiContext(ctx context.Context, keys []string) (map[string]*memcache.Item, error) {
	ks := make([]interface{}, 0)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, memcache.ErrMalformedKey
		}

		ks = append(ks, key)
	}

	results, err := redis.ByteSlices(rc.conn.Do("MGET", ks...))
	if err != nil {
		return nil, err
	}

	list := make(map[string]*memcache.Item, 0)
	for k, v := range results {
		list[keys[k]] = &memcache.Item{Key: keys[k], Value: v}
	}

	return list, nil
}

// Delete deletes the item with the provided key.
func (rc *RedisConn) DeleteContext(ctx context.Context, key string) error {
	if !legalKey(key) {
		return memcache.ErrMalformedKey
	}

	_, err := rc.conn.Do("DEL", key)
	if err != nil {
		return err
	}

	return nil
}

// Increment atomically increments key by delta.
func (rc *RedisConn) IncrementContext(ctx context.Context, key string, delta uint64) (newValue uint64, err error) {
	if !legalKey(key) {
		return 0, memcache.ErrMalformedKey
	}

	newValue, err = redis.Uint64(rc.conn.Do("INCRBY", key, delta))
	if err != nil {
		return 0, err
	}

	return
}

// Decrement atomically decrements key by delta.
func (rc *RedisConn) DecrementContext(ctx context.Context, key string, delta uint64) (newValue uint64, err error) {
	if !legalKey(key) {
		return 0, memcache.ErrMalformedKey
	}

	newValue, err = redis.Uint64(rc.conn.Do("DECRBY", key, delta))
	if err != nil {
		return 0, err
	}

	return
}

// CompareAndSwap writes the given item that was previously returned by
// Get, if the value was neither modified or evicted between the Get and
// the CompareAndSwap calls.
func (rc *RedisConn) CompareAndSwapContext(ctx context.Context, item *memcache.Item) error {
	return pkgerr.Errorf("Invalid command operation")
}

// Touch updates the expiry for the given key.
func (rc *RedisConn) TouchContext(ctx context.Context, key string, seconds int32) (err error) {
	if !legalKey(key) {
		return memcache.ErrMalformedKey
	}

	_, err = rc.conn.Do("EXPIRE", key, seconds)
	if err != nil {
		return err
	}

	return nil
}
