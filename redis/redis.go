package redis

import (
	"context"
	"time"

	"github.com/erkesi/cacheaside/cache"
	redis "github.com/go-redis/redis/v6"
)

type RedisWrap struct {
	cli *redis.Client
}

func NewRedisWrap(cli *redis.Client) *RedisWrap{
	return &RedisWrap{
		cli: cli
	}
}

func (r *RedisWrap) HMSet(ctx context.Context, key string, ttl time.Duration, kvs ...*cache.KV) error {
	return nil
}

func (r *RedisWrap) HMGet(ctx context.Context, key string, ttl time.Duration, subKeys ...string) (map[string]interface{}, error) {
	return nil,nil
}

func (r *Redis) HDel(ctx context.Context, key string) error {
	return nil

}

func (r *RedisWrap) HMDel(ctx context.Context, key string, subKeys ...string) error {
	return nil
}

func (r *RedisWrap) MSet(ctx context.Context, ttl time.Duration, kvs ...*cache.KV) error {
	return nil
}

func (r *RedisWrap) MGet(ctx context.Context, ttl time.Duration, keys ...string) (map[string]interface{}, error) {
	return nil,nil
}

func (r *RedisWrap) MDel(ctx context.Context, keys ...string) error {
	return nil
}
