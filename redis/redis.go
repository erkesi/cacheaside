package redis

import (
	"context"
	"time"

	"github.com/erkesi/cacheaside/cache"
	"github.com/go-redis/redis"
)

type RedisWrap struct {
	cli *redis.Client
}

func NewRedisWrap(cli *redis.Client) *RedisWrap {
	return &RedisWrap{
		cli: cli,
	}
}

func (r *RedisWrap) MSet(ctx context.Context, ttl time.Duration, kvs ...*cache.KV) error {
	if len(kvs) == 0 {
		return nil
	}
	pipeline := r.cli.WithContext(ctx).Pipeline()
	defer func() {
		_ = pipeline.Close()
	}()
	resList := make([]*redis.StatusCmd, len(kvs))
	for i, kv := range kvs {
		resList[i] = pipeline.Set(kv.Key, kv.Data, ttl)
	}
	_, err := pipeline.Exec()
	if err != nil {
		return err
	}
	for _, res := range resList {
		if res.Err() != nil {
			return res.Err()
		}
	}
	return nil
}

func (r *RedisWrap) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	pipeline := r.cli.WithContext(ctx).Pipeline()
	defer func() {
		_ = pipeline.Close()
	}()

	resList := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		resList[i] = pipeline.Get(key)
	}
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}
	key2Data := make(map[string][]byte, len(keys))
	for i, res := range resList {
		data, err := res.Bytes()
		if err == nil {
			key2Data[keys[i]] = data
			continue
		}
		if err == redis.Nil {
			key2Data[keys[i]] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
	}
	return key2Data, nil
}

func (r *RedisWrap) MDel(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return r.cli.WithContext(ctx).Del(keys...).Err()
}

func (r *RedisWrap) HMSet(ctx context.Context, key string, ttl time.Duration, kvs ...*cache.KV) error {
	if len(kvs) == 0 {
		return nil
	}
	pipeline := r.cli.WithContext(ctx).Pipeline()
	defer func() {
		_ = pipeline.Close()
	}()
	var resList []redis.Cmder
	for _, kv := range kvs {
		resList = append(resList, pipeline.Set(kv.Key, kv.Data, ttl))
	}
	resList = append(resList, pipeline.Expire(key, ttl))
	_, err := pipeline.Exec()
	if err != nil {
		return err
	}
	for _, res := range resList {
		if res.Err() != nil {
			return res.Err()
		}
	}
	return nil
}

func (r *RedisWrap) HMGet(ctx context.Context, key string, fields ...string) (map[string][]byte, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	pipeline := r.cli.WithContext(ctx).Pipeline()
	defer func() {
		_ = pipeline.Close()
	}()

	resList := make([]*redis.StringCmd, len(fields))
	for i, field := range fields {
		resList[i] = pipeline.HGet(key, field)
	}
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}
	key2Data := make(map[string][]byte, len(fields))
	for i, res := range resList {
		data, err := res.Bytes()
		if err == nil {
			key2Data[fields[i]] = data
			continue
		}
		if err == redis.Nil {
			key2Data[fields[i]] = nil
			continue
		}
		if err != nil {
			return nil, err
		}
	}
	return key2Data, nil
}

func (r *RedisWrap) HDel(ctx context.Context, key string) error {
	return r.cli.WithContext(ctx).Del(key).Err()
}

func (r *RedisWrap) HMDel(ctx context.Context, key string, fields ...string) error {
	return r.cli.WithContext(ctx).HDel(key, fields...).Err()
}
