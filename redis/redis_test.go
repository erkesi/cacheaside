package redis

import (
	"context"
	"encoding/json"
	"github.com/erkesi/cacheaside/cache"
	"github.com/go-redis/redis"
	"reflect"
	"testing"
	"time"
)

var redisWarp *RedisWrap

func TestMain(m *testing.M) {
	cli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	redisWarp = &RedisWrap{cli: cli}
	m.Run()
}

func TestHCache(t *testing.T) {
	type User struct {
		Extra map[string]string
	}

	genUser := func(key string, field string) *User {
		return &User{
			Extra: map[string]string{field: field},
		}
	}
	key := "m1"
	field2User := make(map[string]*User)
	field2User["Name"] = genUser(key, "Name")
	field2User["Age"] = genUser(key, "Age")
	field2Bs := make(map[string][]byte)

	var kvs []*cache.KV
	for k, v := range field2User {
		bs, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		field2Bs[k] = bs
		kvs = append(kvs, &cache.KV{
			Key:  k,
			Val:  v,
			Data: bs,
		})
	}
	ctx := context.Background()
	err := redisWarp.HMDel(ctx, key, "Name", "Age")
	if err != nil {
		t.Fatal(err)
	}
    err = redisWarp.HDel(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
    err = redisWarp.HMSet(ctx, key, time.Hour, kvs...)
	if err != nil {
		t.Fatal(err)
	}
    key2Bs, err := redisWarp.HMGet(ctx, key, "Name", "Age", "Addr")
	if err != nil {
		t.Fatal(err)
	}
	if len(key2Bs) != 2 {
		t.Fatal("len(key2Bs)!=2")
	}
	if !reflect.DeepEqual(key2Bs, field2Bs) {
		t.Fatal("!reflect.DeepEqual(key2Bs, field2Bs)")
	}
    err = redisWarp.HMDel(ctx, key, "Name", "Age")
    if err != nil {
        t.Fatal(err)
    }
    err = redisWarp.HDel(ctx, key)
    if err != nil {
        t.Fatal(err)
    }
}

func TestCache(t *testing.T) {
	type User struct {
		Id   string
		Name string
		Age  int
	}

	genUser := func(key string) *User {
		return &User{
			Id:   key,
			Name: "name" + key,
			Age:  20,
		}
	}

	id2User := make(map[string]*User)
	id2User["0"] = nil
	id2User["1"] = genUser("1")

	id2Bs := make(map[string][]byte)

	var kvs []*cache.KV

	for k, v := range id2User {
		bs, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		id2Bs[k] = bs
		kvs = append(kvs, &cache.KV{
			Key:  k,
			Val:  v,
			Data: bs,
		})
	}

	ctx := context.Background()
	err := redisWarp.MDel(ctx, "0", "1", "nil")
	if err != nil {
		t.Fatal(err)
	}
	err = redisWarp.MSet(ctx, time.Hour, kvs...)
	if err != nil {
		t.Fatal(err)
	}
	key2Bs, err := redisWarp.MGet(ctx, "0", "1", "nil")
	if err != nil {
		t.Fatal(err)
	}
	if len(key2Bs) != 2 {
		t.Fatal("len(key2Bs)!=2")
	}
	if !reflect.DeepEqual(key2Bs, id2Bs) {
		t.Fatal("!reflect.DeepEqual(key2Bs, id2Bs)")
	}
    err = redisWarp.MDel(ctx, "0", "1", "nil")
    if err != nil {
        t.Fatal(err)
    }
    err = redisWarp.MSet(ctx, time.Hour, kvs...)
    if err != nil {
        t.Fatal(err)
    }
}
