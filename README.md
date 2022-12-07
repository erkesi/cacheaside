# cacheaside
> cache 代理

## 使用方式

### redis hash

```go

import (
    "context"
    "fmt"
    "github.com/erkesi/cacheaside/code"
    csredis "github.com/erkesi/cacheaside/redis"
    "github.com/go-redis/redis"
    "time"
)

type User struct {
    Extra map[string]string
}

genUser := func(key string, field string) *User {
    return &User{
        Extra: map[string]string{field: field},
        }
    }

ca := NewHCacheAside(&code.Json{}, &csredis.NewRedisWrap(&redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    })))

caf := ca.HFetch(
            // 回源查询
            func(ctx context.Context, key string, fields []string, extra ...interface{}) ([]interface{}, error) {
                var res []interface{}
                for _, field := range fields {
                    if field == "nil" {
                        continue
                    }
                    res = append(res, genUser(key, field))
                }
                return res, nil
            },
            // 回源查询到的结果项（v）生成 redis hash field
            func(ctx context.Context, v interface{}, extra ...interface{}) (string, error) {
                for k := range v.(*User).Extra {
                    return k, nil
                }
                return "", nil
            },
WithTTL(time.Hour))

// 查询 hash 多个field
var us []*User
err := caf.HMGet(context.Background(), "1", []string{"nil", "Name", "Age"}, &us)
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(us)

// 删除 hash 多个field
err = caf.HMDel(context.Background(),"1", "Name", "Age")
if err != nil {
    fmt.Println(err)
    return
}

// 删除 hash key
err = caf.HDel(context.Background(),"1")
if err != nil {
    fmt.Println(err)
    return
}

```

###  redis string


```go

import (
    "context"
    "fmt"
    "github.com/erkesi/cacheaside/code"
    csredis "github.com/erkesi/cacheaside/redis"
    "github.com/go-redis/redis"
    "time"
)

type User struct {
    
    Extra map[string]string
    }

genUser := func(key string, field string) *User {
    return &User{
        Extra: map[string]string{field: field},
        }
        }

ca := NewCacheAside(&code.Json{}, &csredis.NewRedisWrap(&redis.NewClient(&redis.Options{
    Addr: "127.0.0.1:6379",
    })))
caf := ca.Fetch(
    // 回源查询
    func(ctx context.Context, keys []string, extra ...interface{}) ([]interface{}, error) {
        var res []interface{}
        for _, key := range keys {
            if key == "nil" {
                continue
            }
            res = append(res, genUser(key))
        }
        return res, nil
    },
    // 回源查询到的结果项（v）生成 redis hash field
    func(ctx context.Context, v interface{}, extra ...interface{}) (string, error) {
        return v.(*User).Id, nil
    },
    WithTTL(time.Hour))

// 查询 多个key
var us []*User
err := caf.MGet(context.Background(), []string{"0", "1", "2", "nil"}, &us)
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(us)

// 删除 key
err = caf.MDel(context.Background(), "0", "1")
if err != nil {
    fmt.Println(err)
    return
}

```

