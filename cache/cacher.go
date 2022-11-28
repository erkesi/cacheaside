package cache

import (
	"context"
	"time"
)

type KV struct {
	Key  string
	Val  interface{}
	Data []byte
}

// HCacher hash
type HCacher interface {
	HMSet(ctx context.Context, key string, ttl time.Duration, kvs ...*KV) error
	HMGet(ctx context.Context, key string, fields ...string) (map[string][]byte, error)
	HDel(ctx context.Context, key string) error
	HMDel(ctx context.Context, key string, fields ...string) error
}

// Cacher string
type Cacher interface {
	MSet(ctx context.Context, ttl time.Duration, kvs ...*KV) error
	MGet(ctx context.Context, keys ...string) (map[string][]byte, error)
	MDel(ctx context.Context, keys ...string) error
}
