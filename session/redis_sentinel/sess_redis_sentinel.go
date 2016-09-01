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

// Package redis for session provider
//
// depend on github.com/garyburd/redigo/redis
//
// go install github.com/garyburd/redigo/redis
//
// Usage:
// import(
//   _ "github.com/astaxie/beego/session/redis_sentinel"
//   "github.com/astaxie/beego/session"
// )
//
//	func init() {
//		globalSessions, _ = session.NewManager("redis", ``{"cookieName":"gosessionid","gclifetime":3600,"ProviderConfig":"127.0.0.1:7070"}``)
//		go globalSessions.GC()
//	}
//
// more docs: http://beego.me/docs/module/session.md
package redis_sentinel

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/astaxie/beego/session"
	"github.com/keimoon/gore"
	"errors"
//	"github.com/garyburd/redigo/redis"
)

var redispder = &Provider{}

// MaxPoolSize redis max pool size
var MaxPoolSize = 100

// SessionStore redis session store
type SessionStore struct {
	p           *gore.Pool
	sid         string
	lock        sync.RWMutex
	values      map[interface{}]interface{}
	maxlifetime int64
}

// Set value in redis session
func (rs *SessionStore) Set(key, value interface{}) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.values[key] = value
	return nil
}

// Get value in redis session
func (rs *SessionStore) Get(key interface{}) interface{} {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	if v, ok := rs.values[key]; ok {
		return v
	}
	return nil
}

// Delete value in redis session
func (rs *SessionStore) Delete(key interface{}) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	delete(rs.values, key)
	return nil
}

// Flush clear all values in redis session
func (rs *SessionStore) Flush() error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	rs.values = make(map[interface{}]interface{})
	return nil
}

// SessionID get redis session id
func (rs *SessionStore) SessionID() string {
	return rs.sid
}

// SessionRelease save session values to redis
func (rs *SessionStore) SessionRelease(w http.ResponseWriter) {
	b, err := session.EncodeGob(rs.values)
	if err != nil {
		return
	}
	c, err := rs.p.Acquire()
	if err != nil {
		return
	}
	defer rs.p.Release(c)
	gore.NewCommand("SETEX", rs.sid, rs.maxlifetime, string(b)).Run(c)
}

// Provider redis session provider
type Provider struct {
	maxlifetime int64
	savePath    string
	poolsize    int
	password    string
	master      string
	addrs       []string
	dbNum       int
	poollist    *gore.Pool
	sentinel *gore.Sentinel
}

// SessionInit init redis session
// savepath like redis server addr,pool size,password,dbnum
// e.g. 127.0.0.1:6379,100,astaxie,0,master,ip:port;ip:port
func (rp *Provider) SessionInit(maxlifetime int64, savePath string) error {
	rp.maxlifetime = maxlifetime
	configs := strings.Split(savePath, ",")
	if len(configs) > 0 {
		rp.savePath = configs[0]
	}
	if len(configs) > 1 {
		poolsize, err := strconv.Atoi(configs[1])
		if err != nil || poolsize <= 0 {
			rp.poolsize = MaxPoolSize
		} else {
			rp.poolsize = poolsize
		}
	} else {
		rp.poolsize = MaxPoolSize
	}
	if len(configs) > 2 {
		rp.password = configs[2]
	}
	if len(configs) > 3 {
		dbnum, err := strconv.Atoi(configs[3])
		if err != nil || dbnum < 0 {
			rp.dbNum = 0
		} else {
			rp.dbNum = dbnum
		}
	} else {
		rp.dbNum = 0
	}
	if len(configs) > 4 {
		rp.master = configs[4]
		if rp.master == "" {
			return errors.New("master is empty")
		}
	}
	if len(configs) > 5 {
		temp := configs[5]
		if temp == "" {
			return errors.New("addrs is empty")
		}
		rp.addrs = strings.Split(temp, ";")
	}

	var err error
	sentinel := gore.NewSentinel()
	sentinel.AddServer(rp.addrs...)
	if err = sentinel.Dial(); err != nil {
		return err
	}
	rp.poollist, err = sentinel.GetPoolWithPassword(rp.master, rp.password)
	if err != nil {
		return err
	}
	rp.sentinel = sentinel

	return err
}

// SessionRead read redis session by sid
func (rp *Provider) SessionRead(sid string) (session.Store, error) {
	c, err := rp.poollist.Acquire()
	if err != nil {
		return nil, err
	}
	defer rp.poollist.Release(c)
	reply, err := gore.NewCommand("GET", sid).Run(c)
	if err != nil {
		return nil, err
	}
	kvs, err := reply.String()
	var kv map[interface{}]interface{}
	if len(kvs) == 0 {
		kv = make(map[interface{}]interface{})
	} else {
		kv, err = session.DecodeGob([]byte(kvs))
		if err != nil {
			return nil, err
		}
	}
	rs := &SessionStore{p: rp.poollist, sid: sid, values: kv, maxlifetime: rp.maxlifetime}
	return rs, nil
}

// SessionExist check redis session exist by sid
func (rp *Provider) SessionExist(sid string) bool {
	c, err := rp.poollist.Acquire()
	if err != nil {
		return false
	}
	defer rp.poollist.Release(c)
	reply, err := gore.NewCommand("EXISTS", sid).Run(c)
	if err != nil {
		return false
	}
	existed, err := reply.Int()
	if err != nil || existed == 0 {
		return false
	}
	return true
}

// SessionRegenerate generate new sid for redis session
func (rp *Provider) SessionRegenerate(oldsid, sid string) (session.Store, error) {
	c, err := rp.poollist.Acquire()
	if err != nil {
		return nil, err
	}
	defer rp.poollist.Release(c)
	reply, err := gore.NewCommand("EXISTS", oldsid).Run(c)
	if err != nil {
		return nil, err
	}

	if existed, _ := reply.Int(); existed == 0 {
		// oldsid doesn't exists, set the new sid directly
		// ignore error here, since if it return error
		// the existed value will be 0
		gore.NewCommand("SET", sid, "", "EX", rp.maxlifetime).Run(c)
	} else {
		gore.NewCommand("RENAME", oldsid, sid).Run(c)
		gore.NewCommand("EXPIRE", sid, rp.maxlifetime).Run(c)
	}
	reply, err = gore.NewCommand("GET", sid).Run(c)
	if err != nil {
		return nil, err
	}
	kvs, err := reply.String()
	var kv map[interface{}]interface{}
	if len(kvs) == 0 {
		kv = make(map[interface{}]interface{})
	} else {
		kv, err = session.DecodeGob([]byte(kvs))
		if err != nil {
			return nil, err
		}
	}

	rs := &SessionStore{p: rp.poollist, sid: sid, values: kv, maxlifetime: rp.maxlifetime}
	return rs, nil
}

// SessionDestroy delete redis session by id
func (rp *Provider) SessionDestroy(sid string) error {
	c, err := rp.poollist.Acquire()
	if err != nil {
		return err
	}
	defer rp.poollist.Release(c)

	_, err = gore.NewCommand("DEL", sid).Run(c)
	return err
}

// SessionGC Impelment method, no used.
func (rp *Provider) SessionGC() {
	return
}

// SessionAll return all activeSession
func (rp *Provider) SessionAll() int {
	return 0
}

func init() {
	session.Register("redis_sentinel", redispder)
}
