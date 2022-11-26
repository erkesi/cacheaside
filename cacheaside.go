package cacheaside

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/erkesi/cacheaside/cache"
	"github.com/erkesi/cacheaside/code"
	"golang.org/x/sync/singleflight"
)

type FetchSource func(ctx context.Context, keys []string,
	extra ...interface{}) ([]interface{}, error)

type GenCacheKey func(ctx context.Context, v interface{},
	extra ...interface{}) (string, error)

type CacheAside struct {
	code        code.Coder
	cache       cache.Cacher
	hcache      cache.HCacher
	ttl         time.Duration
	fetchSource FetchSource
	genCacheKey GenCacheKey
	sfg         singleflight.Group
}

func NewCacheAside(code code.Coder, cache cache.Cacher) *CacheAside {
	return &CacheAside{
		code:  code,
		cache: cache,
	}
}

func NewHCacheAside(code code.Coder, hcache cache.HCacher) *CacheAside {
	return &CacheAside{
		code:   code,
		hcache: hcache,
	}
}

func (ca *CacheAside) Fetch(genCacheKey GenCacheKey,
	fetchSource FetchSource, ttl time.Duration) *CacheAsideFetcher {
	return &CacheAsideFetcher{
		ca: &CacheAside{
			code:  ca.code,
			cache: ca.cache,
			ttl:   ttl,
		},
	}
}

type CacheAsideFetcher struct {
	ca *CacheAside
}

func (caf *CacheAsideFetcher) Get(ctx context.Context, key string,
	extra ...interface{}) (interface{}, error) {
	m, err := caf.MGet(ctx, []string{key}, extra...)
	if err != nil {
		return nil, err
	}
	return m[0], nil
}

func (caf *CacheAsideFetcher) MGet(ctx context.Context, keys []string,
	extra ...interface{}) ([]interface{}, error) {
	if err := caf.checkCache(); err != nil {
		return nil, err
	}
	sort.Strings(keys)
	val, err, _ := caf.ca.sfg.Do(strings.Join(keys, ","), func() (v interface{}, e error) {
		m, err := caf.ca.cache.MGet(ctx, caf.ca.ttl, keys...)
		if err != nil {
			return nil, err
		}

		missKVs, missM, err := caf.fetchSourceMiss(ctx, keys, m, extra...)
		if err != nil {
			return nil, err
		}

		err = caf.ca.cache.MSet(ctx, caf.ca.ttl, missKVs...)
		if err != nil {
			return nil, err
		}

		return caf.merge(keys, m, missM), nil
	})
	return val.([]interface{}), err
}

func (caf *CacheAsideFetcher) MDel(ctx context.Context, keys ...string) error {
	if err := caf.checkCache(); err != nil {
		return err
	}
	return caf.ca.cache.MDel(ctx, keys...)
}

func (caf *CacheAsideFetcher) HGet(ctx context.Context, key, subKey string,
	extra ...interface{}) (interface{}, error) {
	vals, err := caf.HMGet(ctx, key, []string{subKey}, extra...)
	if err != nil {
		return nil, err
	}
	return vals[0], nil
}

func (caf *CacheAsideFetcher) HMGet(ctx context.Context, key string, subKeys []string,
	extra ...interface{}) ([]interface{}, error) {
	if err := caf.checkHCache(); err != nil {
		return nil, err
	}
	sort.Strings(subKeys)
	val, err, _ := caf.ca.sfg.Do(fmt.Sprintf("%s[%s]", key, strings.Join(subKeys, ",")),
		func() (v interface{}, e error) {
			m, err := caf.ca.hcache.HMGet(ctx, key, caf.ca.ttl, subKeys...)
			if err != nil {
				return nil, err
			}

			missKVs, missM, err := caf.fetchSourceMiss(ctx, subKeys, m, extra...)
			if err != nil {
				return nil, err
			}

			err = caf.ca.hcache.HMSet(ctx, key, caf.ca.ttl, missKVs...)
			if err != nil {
				return nil, err
			}
			return caf.merge(subKeys, m, missM), nil
		})
	return val.([]interface{}), err
}

func (caf *CacheAsideFetcher) HMDel(ctx context.Context, key string, subKeys ...string) error {
	if err := caf.checkHCache(); err != nil {
		return err
	}
	return caf.ca.hcache.HMDel(ctx, key, subKeys...)
}

func (caf *CacheAsideFetcher) HDel(ctx context.Context, key string) error {
	if err := caf.checkHCache(); err != nil {
		return err
	}
	return caf.ca.hcache.HDel(ctx, key)
}

func (caf *CacheAsideFetcher) fetchSourceMiss(ctx context.Context, keys []string,
	m map[string]interface{}, extra ...interface{}) ([]*cache.KV, map[string]interface{}, error) {
	var missKeys []string
	for _, key := range keys {
		if _, ok := m[key]; ok {
			continue
		}
		missKeys = append(missKeys, key)
	}

	vals, err := caf.ca.fetchSource(ctx, missKeys, extra...)
	if err != nil {
		return nil, nil, err
	}

	missM := make(map[string]interface{})
	for _, v := range vals {
		key, err := caf.ca.genCacheKey(ctx, v, extra...)
		if err != nil {
			return nil, nil, err
		}
		missM[key] = v
	}

	var missKVs []*cache.KV
	for _, key := range missKeys {
		val := missM[key]
		data, err := caf.ca.code.Encode(val)
		if err != nil {
			return nil, nil, err
		}
		missKVs = append(missKVs, &cache.KV{
			Key:  key,
			Val:  missM[key],
			Data: data,
		})
	}
	return missKVs, missM, nil
}

func (caf *CacheAsideFetcher) merge(keys []string, m, missM map[string]interface{}) []interface{} {
	var res []interface{}
	for _, key := range keys {
		var v interface{}
		if vt, ok := m[key]; ok {
			v = vt
		} else if vt, ok := missM[key]; ok {
			v = vt
		}
		res = append(res, v)
	}
	return res
}

func (caf *CacheAsideFetcher) checkCache() error {
	if caf.ca.cache == nil {
		return errors.New("cacheaside: cache is nil")
	}
	return caf.check()
}

func (caf *CacheAsideFetcher) checkHCache() error {
	if caf.ca.hcache == nil {
		return errors.New("cacheaside: hcache is nil")
	}
	return caf.check()
}

func (caf *CacheAsideFetcher) check() error {
	if caf.ca.code == nil {
		return errors.New("cacheaside: code is nil")
	}
	if caf.ca.fetchSource == nil {
		return errors.New("cacheaside: fetchSource is nil")
	}
	if caf.ca.genCacheKey == nil {
		return errors.New("cacheaside: genCacheKey is nil")
	}
	return nil
}
