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

type FetchSourceHash func(ctx context.Context, key string, fields []string,
	extra ...interface{}) ([]interface{}, error)

type GenCacheHashField func(ctx context.Context, v interface{},
	extra ...interface{}) (string, error)

type CacheAside struct {
	code   code.Coder
	cache  cache.Cacher
	hcache cache.HCacher
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

func (ca *CacheAside) Fetch(fetchSource FetchSource, genCacheKey GenCacheKey, ttl time.Duration) *Fetcher {
	return &Fetcher{
		_Fetcher: &_Fetcher{
			ca: &CacheAside{
				code:  ca.code,
				cache: ca.cache,
			},
			ttl: ttl,
		},
		fetchSource: fetchSource,
		genCacheKey: genCacheKey,
	}
}

func (ca *CacheAside) HFetch(fetchSource FetchSourceHash, genCacheHashField GenCacheHashField, ttl time.Duration) *HFetcher {
	return &HFetcher{
		_Fetcher: &_Fetcher{
			ca: &CacheAside{
				code:   ca.code,
				hcache: ca.hcache,
			},
			ttl: ttl,
		},
		fetchSource:       fetchSource,
		genCacheHashField: genCacheHashField,
	}
}

type _Fetcher struct {
	ca  *CacheAside
	ttl time.Duration
	sfg singleflight.Group
}

type Fetcher struct {
	*_Fetcher
	fetchSource FetchSource
	genCacheKey GenCacheKey
}

type HFetcher struct {
	*_Fetcher
	fetchSource       FetchSourceHash
	genCacheHashField GenCacheHashField
}

func (f *Fetcher) Get(ctx context.Context, key string, res interface{},
	extra ...interface{}) (bool, error) {
	return f.mget(ctx, []string{key}, res, extra...)
}

func (f *Fetcher) MGet(ctx context.Context, keys []string, res interface{},
	extra ...interface{}) error {
	_, err := f.mget(ctx, keys, res, extra...)
	return err
}

func (f *Fetcher) mget(ctx context.Context, keys []string, res interface{},
	extra ...interface{}) (bool, error) {
	if err := f.check(); err != nil {
		return false, err
	}

	tmpResType, tmpResVal, err := f.resRelVal(len(keys), res)
	if err != nil {
		return false, err
	}

	m, err := f.ca.cache.MGet(ctx, keys...)
	if err != nil {
		return false, err
	}

	missKVs, missM, err := f.fetchSourceMiss(ctx, keys, m, extra...)
	if err != nil {
		return false, err
	}

	err = f.ca.cache.MSet(ctx, f.ttl, missKVs...)
	if err != nil {
		return false, err
	}
	return f.merge(keys, m, missM, tmpResType, tmpResVal)
}

func (f *Fetcher) MDel(ctx context.Context, keys ...string) error {
	if err := f.check(); err != nil {
		return err
	}
	return f.ca.cache.MDel(ctx, keys...)
}

func (hf *HFetcher) HGet(ctx context.Context, key, field string, res interface{},
	extra ...interface{}) error {
	return hf.HMGet(ctx, key, []string{field}, res, extra...)
}

func (hf *HFetcher) HMGet(ctx context.Context, key string, fields []string, res interface{},
	extra ...interface{}) error {
	if err := hf.check(); err != nil {
		return err
	}
	tmpResType, tmpResVal, err := hf.resRelVal(len(fields), res)
	if err != nil {
		return err
	}

	m, err := hf.ca.hcache.HMGet(ctx, key, fields...)
	if err != nil {
		return err
	}

	missKVs, missM, err := hf.fetchSourceMiss(ctx, key, fields, m, extra...)
	if err != nil {
		return err
	}
	if len(missKVs) > 0 {
		err = hf.ca.hcache.HMSet(ctx, key, hf.ttl, missKVs...)
		if err != nil {
			return err
		}
	}
	_, err = hf.merge(fields, m, missM, tmpResType, tmpResVal)
	return err
}

func (hf *HFetcher) HMDel(ctx context.Context, key string, fields ...string) error {
	if err := hf.check(); err != nil {
		return err
	}
	return hf.ca.hcache.HMDel(ctx, key, fields...)
}

func (hf *HFetcher) HDel(ctx context.Context, key string) error {
	if err := hf.check(); err != nil {
		return err
	}
	return hf.ca.hcache.HDel(ctx, key)
}

func (f *Fetcher) fetchSourceMiss(ctx context.Context, keys []string,
	m map[string][]byte, extra ...interface{}) ([]*cache.KV, map[string]interface{}, error) {
	var missKeys []string
	for _, key := range keys {
		if _, ok := m[key]; ok {
			continue
		}
		missKeys = append(missKeys, key)
	}
	if len(missKeys) == 0 {
		return nil, nil, nil
	}
	sort.Strings(missKeys)
	vals, err, _ := f.sfg.Do(fmt.Sprintf("[%s]", strings.Join(missKeys, ",")),
		func() (v interface{}, e error) {
			return f.fetchSource(ctx, missKeys, extra...)
		})
	if err != nil {
		return nil, nil, err
	}
	missM := make(map[string]interface{})
	for _, v := range vals.([]interface{}) {
		key, err := f.genCacheKey(ctx, v, extra...)
		if err != nil {
			return nil, nil, err
		}
		missM[key] = v
	}

	var missKVs []*cache.KV
	for _, key := range missKeys {
		var data []byte
		val := missM[key]
		if val != nil {
			data, err = f.ca.code.Encode(val)
			if err != nil {
				return nil, nil, err
			}
		}
		missKVs = append(missKVs, &cache.KV{
			Key:  key,
			Val:  missM[key],
			Data: data,
		})
	}
	return missKVs, missM, nil
}

func (hf *HFetcher) fetchSourceMiss(ctx context.Context, key string, fields []string,
	m map[string][]byte, extra ...interface{}) ([]*cache.KV, map[string]interface{}, error) {
	var missFields []string
	for _, key := range fields {
		if _, ok := m[key]; ok {
			continue
		}
		missFields = append(missFields, key)
	}
	if len(missFields) == 0 {
		return nil, nil, nil
	}
	sort.Strings(missFields)
	vals, err, _ := hf.sfg.Do(fmt.Sprintf("[%s]", strings.Join(missFields, ",")),
		func() (v interface{}, e error) {
			return hf.fetchSource(ctx, key, missFields, extra...)
		})
	if err != nil {
		return nil, nil, err
	}
	missM := make(map[string]interface{})
	for _, v := range vals.([]interface{}) {
		key, err := hf.genCacheHashField(ctx, v, extra...)
		if err != nil {
			return nil, nil, err
		}
		missM[key] = v
	}

	var missKVs []*cache.KV
	for _, key := range missFields {
		var data []byte
		val := missM[key]
		if val != nil {
			data, err = hf.ca.code.Encode(val)
			if err != nil {
				return nil, nil, err
			}
		}
		missKVs = append(missKVs, &cache.KV{
			Key:  key,
			Val:  missM[key],
			Data: data,
		})
	}
	return missKVs, missM, nil
}

func (_f *_Fetcher) merge(keys []string, exsitM map[string][]byte, missM map[string]interface{},
	rt reflect.Type, res reflect.Value) (bool, error) {
	key2RefVal := make(map[string]reflect.Value)
	for k, data := range exsitM {
		if len(data) == 0 {
			continue
		}
		v := reflect.New(_f.indirectType(rt))
		err := _f.ca.code.Decode(data, v.Interface())
		if err != nil {
			return false, err
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
			// fmt.Printf("%s-%v-%v\n", key, res.Index(i).Interface(), rv.Interface())
			res.Index(i).Set(rv)
		} else {
			res.Elem().Set(rv.Elem())
			return true, nil
		}
	}
	return false, nil
}

func (hf *HFetcher) check() error {
	if hf.fetchSource == nil {
		return errors.New("cacheaside: fetchSource is nil")
	}
	if hf.genCacheHashField == nil {
		return errors.New("cacheaside: genCacheHashField is nil")
	}
	return hf._check(false, true)
}

func (_f *_Fetcher) _check(isCache, isHCache bool) error {
	if _f.ca.code == nil {
		return errors.New("cacheaside: code is nil")
	}
	if _f.ca.cache == nil && isCache {
		return errors.New("cacheaside: cache is nil")
	}
	if _f.ca.hcache == nil && isHCache {
		return errors.New("cacheaside: hcache is nil")
	}
	return nil
}

func (f *Fetcher) check() error {
	if f.fetchSource == nil {
		return errors.New("cacheaside: fetchSource is nil")
	}
	if f.genCacheKey == nil {
		return errors.New("cacheaside: genCacheKey is nil")
	}
	return f._check(true, false)
}

func (_f *_Fetcher) indirectType(rt reflect.Type) reflect.Type {
	for rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt
}
func (_f *_Fetcher) resRelVal(size int, res interface{}) (reflect.Type, reflect.Value, error) {
	rv := reflect.ValueOf(res)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return rv.Type(), rv, errors.New("cacheaside: res must be valid pointer")
	}
	tmpResVal := rv
	tmpResType := rv.Type()
	if rv.Elem().Kind() == reflect.Slice {
		tmpResVal = rv.Elem()
		tmpResType = tmpResVal.Type().Elem()
		if tmpResVal.Len() < size {
			newSlice := reflect.MakeSlice(reflect.SliceOf(tmpResType), size, size)
			tmpResVal.Set(newSlice)
		}
	}
	return tmpResType, tmpResVal, nil
}
