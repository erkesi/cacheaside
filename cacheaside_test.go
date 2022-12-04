package cacheaside

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/erkesi/cacheaside/cache"
	"github.com/erkesi/cacheaside/code"
	"github.com/golang/mock/gomock"
	"reflect"
	"testing"
	"time"
)

func TestHCache(t *testing.T) {
	type User struct {
		Extra map[string]string
	}

	genUser := func(key string, field string) *User {
		return &User{
			Extra: map[string]string{field: field},
		}
	}

	id2User := make(map[string]*User)
	id2User["1-Name"] = genUser("1", "Name")
	id2User["1-Age"] = genUser("1", "Age")

	ctrl := gomock.NewController(t)
	// Assert that Bar() is invoked.
	defer ctrl.Finish()

	mhcache := cache.NewMockHCacher(ctrl)
	mhcache.EXPECT().HMGet(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key string, fields ...string) (map[string][]byte, error) {
		key2bs := make(map[string][]byte)
		for _, field := range fields {
			if u, ok := id2User[fmt.Sprintf("%s-%s", key, field)]; ok {
				if u == nil {
					key2bs[key] = nil
					continue
				}
				bs, err := json.Marshal(u)
				if err != nil {
					return nil, err
				}
				key2bs[key] = bs
			}
		}
		return key2bs, nil
	})

    mhcache.EXPECT().HDel(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key string) error {
        if key != "1"  {
            ctrl.T.Fatalf("%v", "not equal")
        }
        return nil
    })

	mhcache.EXPECT().HMDel(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, key string, fields ...string) error {
		if key != "1" || len(fields) != 2 || fields[0] != "Name" || fields[1] != "Age" {
			ctrl.T.Fatalf("%v", "not equal")
		}
		return nil
	})
	mhcache.EXPECT().HMSet(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, key string, ttl time.Duration, kvs ...*cache.KV) error {
			fmt.Println(ttl)
			for _, kv := range kvs {
				fmt.Printf("Mset: kv:%v-%v-%v\n\n", kv.Key, string(kv.Data), kv.Val)
				if kv.Key != "nil" {
					bs, err := json.Marshal(genUser(key, kv.Key))
					if err != nil {
						ctrl.T.Fatalf("%v", err)
					}
					if string(kv.Data) != string(bs) {
						ctrl.T.Fatalf("%v", "not equal")
					}
				} else if len(kv.Data) != 0 {
					ctrl.T.Fatalf("%v", "not equal")
				}
			}
			return nil
		})

	ca := NewHCacheAside(&code.Json{}, mhcache)

	caf := ca.HFetch(func(ctx context.Context, key string, fields []string, extra ...interface{}) ([]interface{}, error) {
		var res []interface{}
		for _, field := range fields {
			if field == "nil" {
				continue
			}
			res = append(res, genUser(key, field))
		}
		return res, nil
	}, func(ctx context.Context, v interface{}, extra ...interface{}) (string, error) {
		for k := range v.(*User).Extra {
			return k, nil
		}
		ctrl.T.Fatalf("%v", "gen hash field error")
		return "", nil
	}, time.Hour)

	var us []*User
	err := caf.HMGet(context.Background(), "1", []string{"nil", "Name", "Age"}, &us)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(us)
	if len(us) != 3 || us[0] != nil || us[1].Extra["Name"] != "Name" || us[2].Extra["Age"] != "Age" {
		t.Fatal("us not equal nil")
	}
	err = caf.HMDel(context.Background(),"1", "Name", "Age")
	if err != nil {
		t.Fatal(err)
	}

    err = caf.HDel(context.Background(),"1")
    if err != nil {
        t.Fatal(err)
    }
}

func TestGet(t *testing.T) {

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

	ctrl := gomock.NewController(t)
	// Assert that Bar() is invoked.
	defer ctrl.Finish()

	mcache := cache.NewMockCacher(ctrl)
	mcache.EXPECT().MGet(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, keys ...string) (map[string][]byte, error) {
		key2bs := make(map[string][]byte)
		for _, key := range keys {
			if u, ok := id2User[key]; ok {
				bs, err := json.Marshal(u)
				if err != nil {
					return nil, err
				}
				key2bs[key] = bs
			}
		}
		return key2bs, nil
	}).AnyTimes()

	mcache.EXPECT().MSet(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ttl time.Duration, kvs ...*cache.KV) error {
			for _, kv := range kvs {
				fmt.Printf("Mset: kv:%v-%v-%v\n\n", kv.Key, string(kv.Data), kv.Val)
				if kv.Key != "nil" {
					bs, err := json.Marshal(genUser(kv.Key))
					if err != nil {
						ctrl.T.Fatalf("%v", err)
					}
					if string(kv.Data) != string(bs) {
						ctrl.T.Fatalf("%v", "not equal")
					}
				} else if len(kv.Data) != 0 {
					ctrl.T.Fatalf("%v", "not equal")
				}
			}
			return nil
		}).AnyTimes()

	ca := NewCacheAside(&code.Json{}, mcache)

	caf := ca.Fetch(func(ctx context.Context, keys []string, extra ...interface{}) ([]interface{}, error) {
		var res []interface{}
		for _, key := range keys {
			if key == "nil" {
				continue
			}
			res = append(res, genUser(key))
		}
		return res, nil
	}, func(ctx context.Context, v interface{}, extra ...interface{}) (string, error) {
		return v.(*User).Id, nil
	}, time.Hour)

	var u0, u1, u2, u_nil User
	ok, err := caf.Get(context.Background(), "0", &u0)
	if err != nil {
		t.Fatal(err)
	}
	if ok != false {
		t.Fatal("uo not equal nil")
	}
	ok, err = caf.Get(context.Background(), "1", &u1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&u1, genUser("1")) {
		t.Fatal("u1, not equal")
	}

	ok, err = caf.Get(context.Background(), "2", &u2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&u2, genUser("2")) {
		t.Fatal("u2, not equal")
	}

	ok, err = caf.Get(context.Background(), "nil", &u_nil)
	if err != nil {
		t.Fatal(err)
	}
	if ok != false {
		t.Fatal("u_nil not equal nil")
	}
}

func TestMGetAndMDel(t *testing.T) {

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

	ctrl := gomock.NewController(t)
	// Assert that Bar() is invoked.
	defer ctrl.Finish()

	mcache := cache.NewMockCacher(ctrl)
	mcache.EXPECT().MGet(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, keys ...string) (map[string][]byte, error) {
		key2bs := make(map[string][]byte)
		for _, key := range keys {
			if u, ok := id2User[key]; ok {
				if u == nil {
					key2bs[key] = nil
					continue
				}
				bs, err := json.Marshal(u)
				if err != nil {
					return nil, err
				}
				key2bs[key] = bs
			}
		}
		return key2bs, nil
	})
	mcache.EXPECT().MDel(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, keys ...string) error {
		if len(keys) != 2 || keys[0] != "0" || keys[1] != "1" {
			ctrl.T.Fatalf("%v", "not equal")
		}
		return nil
	})
	mcache.EXPECT().MSet(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, ttl time.Duration, kvs ...*cache.KV) error {
			fmt.Println(ttl)
			for _, kv := range kvs {
				fmt.Printf("Mset: kv:%v-%v-%v\n\n", kv.Key, string(kv.Data), kv.Val)
				if kv.Key != "nil" {
					bs, err := json.Marshal(genUser(kv.Key))
					if err != nil {
						ctrl.T.Fatalf("%v", err)
					}
					if string(kv.Data) != string(bs) {
						ctrl.T.Fatalf("%v", "not equal")
					}
				} else if len(kv.Data) != 0 {
					ctrl.T.Fatalf("%v", "not equal")
				}
			}
			return nil
		})

	ca := NewCacheAside(&code.Json{}, mcache)

	caf := ca.Fetch(func(ctx context.Context, keys []string, extra ...interface{}) ([]interface{}, error) {
		var res []interface{}
		for _, key := range keys {
			if key == "nil" {
				continue
			}
			res = append(res, genUser(key))
		}
		return res, nil
	}, func(ctx context.Context, v interface{}, extra ...interface{}) (string, error) {
		return v.(*User).Id, nil
	}, time.Hour)

	var us []*User
	err := caf.MGet(context.Background(), []string{"0", "1", "2", "nil"}, &us)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(us)
	if len(us) != 4 || us[0] != nil || us[3] != nil || us[1].Id != "1" || us[2].Id != "2" {
		t.Fatal("uo not equal nil")
	}
	err = caf.MDel(context.Background(), "0", "1")
	if err != nil {
		t.Fatal(err)
	}
}
