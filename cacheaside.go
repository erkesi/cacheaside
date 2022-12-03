package cacheaside

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

func (caf *CacheAsideFetcher) Get(ctx context.Context, key string, res interface{},
	extra ...interface{}) error {
	return caf.MGet(ctx, []string{key}, res, extra...)
}

func (caf *CacheAsideFetcher) MGet(ctx context.Context, keys []string, res interface{},
	extra ...interface{}) error {
	if err := caf.checkCache(); err != nil {
		return err
	}

	tmpResType, tmpResVal, err := caf.resRelVal(len(keys), res)
	if err != nil {
		return err
	}

    m, err := caf.ca.cache.MGet(ctx, caf.ca.ttl, keys...)
	if err != nil {
		return err
	}

	missKVs, missM, err := caf.fetchSourceMiss(ctx, keys, m, extra...)
	if err != nil {
		return err
	}

	err = caf.ca.cache.MSet(ctx, caf.ca.ttl, missKVs...)
	if err != nil {
		return err
	}

	return caf.merge(keys, m, missM, tmpResType, tmpResVal)
}

func (caf *CacheAsideFetcher) MDel(ctx context.Context, keys ...string) error {
	if err := caf.checkCache(); err != nil {
		return err
	}
	return caf.ca.cache.MDel(ctx, keys...)
}

func (caf *CacheAsideFetcher) HGet(ctx context.Context, key, field string, res interface{},
	extra ...interface{}) error {
	return caf.HMGet(ctx, key, []string{field}, res, extra...)
}

func (caf *CacheAsideFetcher) HMGet(ctx context.Context, key string, fields []string, res interface{},
	extra ...interface{}) error {
	if err := caf.checkHCache(); err != nil {
		return err
	}
	tmpResType, tmpResVal, err := caf.resRelVal(len(fields), res)
	if err != nil {
		return err
	}

    m, err := caf.ca.hcache.HMGet(ctx, key,  caf.ca.ttl, fields...)
	if err != nil {
		return err
	}

	missKVs, missM, err := caf.fetchSourceMiss(ctx, fields, m, extra...)
	if err != nil {
		return err
	}

	err = caf.ca.hcache.HMSet(ctx, key, caf.ca.ttl, missKVs...)
	if err != nil {
		return err
	}
	return caf.merge(fields, m, missM, tmpResType, tmpResVal)
}

func (caf *CacheAsideFetcher) HMDel(ctx context.Context, key string, fields ...string) error {
	if err := caf.checkHCache(); err != nil {
		return err
	}
	return caf.ca.hcache.HMDel(ctx, key, fields...)
}

func (caf *CacheAsideFetcher) HDel(ctx context.Context, key string) error {
	if err := caf.checkHCache(); err != nil {
		return err
	}
	return caf.ca.hcache.HDel(ctx, key)
}

func (caf *CacheAsideFetcher) fetchSourceMiss(ctx context.Context, keys []string,
	m map[string][]byte, extra ...interface{}) ([]*cache.KV, map[string]interface{}, error) {
	var missKeys []string
	for _, key := range keys {
		if _, ok := m[key]; ok {
			continue
		}
		missKeys = append(missKeys, key)
	}
	sort.Strings(missKeys)
	vals, err, _ := caf.ca.sfg.Do(fmt.Sprintf("[%s]", strings.Join(missKeys, ",")),
		func() (v interface{}, e error) {
			return caf.ca.fetchSource(ctx, missKeys, extra...)
		})
	if err != nil {
		return nil, nil, err
	}
	missM := make(map[string]interface{})
	for _, v := range vals.([]interface{}) {
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

func (caf *CacheAsideFetcher) merge(keys []string, m map[string][]byte, missM map[string]interface{},
	rt reflect.Type, res reflect.Value) error {
	key2RefVal := make(map[string]reflect.Value)
	for k, data := range m {
		if len(data) == 0 {
			key2RefVal[k] = reflect.Zero(rt)
			continue
		}
		v := reflect.New(caf.indirectType(rt))
		err := caf.ca.code.Decode(data, v)
		if err != nil {
			return err
		}
		key2RefVal[k] = v
	}
	for i, key := range keys {
		var rv reflect.Value
		b := false
		if vt, ok := key2RefVal[key]; ok {
			rv = vt
			b = true
		} else if vt, ok := missM[key]; ok {
			rv = reflect.ValueOf(vt)
			b = true
		}
		if !b {
			continue
		}
		if res.Kind() == reflect.Slice {
			res.Index(i).Set(rv)
		} else {
			res.Set(rv)
		}
	}
	return nil
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

func (caf *CacheAsideFetcher) indirectType(rt reflect.Type) reflect.Type {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt
}
func (caf *CacheAsideFetcher) resRelVal(size int, res interface{}) (reflect.Type, reflect.Value, error) {
	rv := reflect.ValueOf(res)
	if rv.Kind() != reflect.Ptr {
		return rv.Type(), rv, errors.New("cacheaside: res must be valid pointer")
	}
	tmpResVal := rv
	tmpResType := rv.Type()
	if rv.Elem().Kind() == reflect.Slice {
		tmpResVal = rv.Elem()
		tmpResType = rv.Type().Elem()
		if tmpResVal.Len() < size {
			newSlice := reflect.MakeSlice(reflect.SliceOf(tmpResType), size, size)
			tmpResVal.Set(newSlice)
		}
	}
	return tmpResType, tmpResVal, nil
}
