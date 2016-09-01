// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package redis for cache provider
//
// depend on github.com/garyburd/redigo/redis
//
// go install github.com/garyburd/redigo/redis
//
// Usage:
// import(
//   _ "github.com/astaxie/beego/cache/redis_sentinel"
//   "github.com/astaxie/beego/cache"
// )
//
//  bm, err := cache.NewCache("redis", `{"conn":"127.0.0.1:11211"}`)
//
//  more docs http://beego.me/docs/module/cache.md
package redis_sentinel

import (
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/astaxie/beego/cache"
	"github.com/keimoon/gore"
)

var (
	// DefaultKey the collection name of redis for cache adapter.
	DefaultKey = "beecacheRedisSentinel"
)

// Cache is Redis cache adapter.
type Cache struct {
	sentinel *gore.Sentinel
	p        *gore.Pool // redis connection pool
	//	conninfo string
	master   string
	addrs    []string
	dbNum    int
	key      string
	password string
}

// NewRedisCache create new redis cache with default collection name.
func NewRedisCache() cache.Cache {
	return &Cache{key: DefaultKey}
}

// actually do the redis cmds
func (rc *Cache) do(commandName string, args ...interface{}) (reply interface{}, err error) {
	c, err := rc.p.Acquire()
	if err != nil {
		return nil, err
	}
	defer rc.p.Release(c)

	return gore.NewCommand(commandName, args...).Run(c)
}

// Get cache from redis.
func (rc *Cache) Get(key string) interface{} {
	if v, err := rc.do("GET", key); err == nil {
		return v
	}
	return nil
}

// GetMulti get cache from redis.
func (rc *Cache) GetMulti(keys []string) []interface{} {
	size := len(keys)
	var rv []interface{}
	c, err := rc.p.Acquire()
	if err != nil {
		goto ERROR
	}
	defer rc.p.Release(c)
	for _, key := range keys {
		err = gore.NewCommand("GET", key).Send(c)
		if err != nil {
			goto ERROR
		}
	}
	for i := 0; i < size; i++ {
		if v, err := gore.Receive(c); err == nil {
			b, _ := v.Bytes()
			rv = append(rv, b)
		} else {
			rv = append(rv, err)
		}
	}
	return rv
ERROR:
	rv = rv[0:0]
	for i := 0; i < size; i++ {
		rv = append(rv, nil)
	}

	return rv
}

// Put put cache to redis.
func (rc *Cache) Put(key string, val interface{}, timeout time.Duration) error {
	var err error
	if _, err = rc.do("SETEX", key, int64(timeout/time.Second), val); err != nil {
		return err
	}

	if _, err = rc.do("HSET", rc.key, key, true); err != nil {
		return err
	}
	return err
}

// Delete delete cache in redis.
func (rc *Cache) Delete(key string) error {
	var err error
	if _, err = rc.do("DEL", key); err != nil {
		return err
	}
	_, err = rc.do("HDEL", rc.key, key)
	return err
}

// IsExist check cache's existence in redis.
func (rc *Cache) IsExist(key string) bool {
	v, err := redis.Bool(rc.do("EXISTS", key))
	if err != nil {
		return false
	}
	if v == false {
		if _, err = rc.do("HDEL", rc.key, key); err != nil {
			return false
		}
	}
	return v
}

// Incr increase counter in redis.
func (rc *Cache) Incr(key string) error {
	_, err := redis.Bool(rc.do("INCRBY", key, 1))
	return err
}

// Decr decrease counter in redis.
func (rc *Cache) Decr(key string) error {
	_, err := redis.Bool(rc.do("INCRBY", key, -1))
	return err
}

// ClearAll clean all cache in redis. delete this redis collection.
func (rc *Cache) ClearAll() error {
	cachedKeys, err := redis.Strings(rc.do("HKEYS", rc.key))
	if err != nil {
		return err
	}
	for _, str := range cachedKeys {
		if _, err = rc.do("DEL", str); err != nil {
			return err
		}
	}
	_, err = rc.do("DEL", rc.key)
	return err
}

// StartAndGC start redis cache adapter.
// config is like {"key":"collection key","conn":"connection info","dbNum":"0"}
// the cache item in redis are stored forever,
// so no gc operation.
func (rc *Cache) StartAndGC(config string) error {
	var cf map[string]interface{}
	json.Unmarshal([]byte(config), &cf)

	if _, ok := cf["key"]; !ok {
		cf["key"] = DefaultKey
	}
	if _, ok := cf["master"]; !ok {
		return errors.New("config has no master")
	}
	if _, ok := cf["addrs"]; !ok {
		return errors.New("config has no addrs")
	}
	if _, ok := cf["dbNum"]; !ok {
		cf["dbNum"] = "0"
	}
	if _, ok := cf["password"]; !ok {
		cf["password"] = ""
	}
	addrsI := cf["addrs"].([]interface{})
	addrs := []string{}
	for _, addr := range addrsI{
		addrs = append(addrs, addr.(string))
	}
	rc.key = cf["key"].(string)
	rc.master = cf["master"].(string)
	rc.dbNum, _ = strconv.Atoi(cf["dbNum"].(string))
	rc.password = cf["password"].(string)
	rc.addrs = addrs
	if rc.master == "" || len(rc.addrs) == 0 {
		return errors.New("master and addrs cannot be empty")
	}

	if err := rc.connectInit(); err != nil {
		return err
	}

	c, err := rc.p.Acquire()
	if err != nil {
		return err
	}
	defer rc.p.Release(c)
	if !c.IsConnected() {
		err = errors.New("not connected")
	}
	return err
}

// connect to redis.
func (rc *Cache) connectInit() error {
	var err error
	sentinel := gore.NewSentinel()
	sentinel.AddServer(rc.addrs...)
	if err = sentinel.Dial(); err != nil {
		return err
	}
	rc.p, err = sentinel.GetPoolWithPassword(rc.master, rc.password)
	if err != nil {
		return err
	}
	rc.sentinel = sentinel
	return nil
}

func init() {
	cache.Register("redis_sentinel", NewRedisCache)
}
