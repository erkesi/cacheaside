package cache

import (
	"context"
	"time"
)

type KV struct {
	Key string
	Val interface{}
}

type TTL struct {
	Logic time.Duration
	Real  time.Duration
}

type HCacher interface {
	HMSet(ctx context.Context, key string, ttl *TTL, kvs ...*KV) error
	HMGet(ctx context.Context, key string, ttl *TTL, subKeys ...string) (map[string]interface{}, error)
	HDel(ctx context.Context, key string) error
	HMDel(ctx context.Context, key string, subKeys ...string) error
}

type Cacher interface {
	MSet(ctx context.Context, ttl *TTL, kvs ...*KV) error
	MGet(ctx context.Context, ttl *TTL, keys ...string) (map[string]interface{}, error)
	MDel(ctx context.Context, keys ...string) error
}
