// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package redis

import (
	"context"
	"github.com/go-redis/redis/v9"
	"perf-storage-go/conf"
	"time"
)

type Cli struct {
	client redis.UniversalClient
}

func newCli() *Cli {
	cli := &Cli{}
	if conf.RedisCluster {
		cli.client = newClusterClient()
	} else {
		cli.client = newClient()
	}
	return cli
}

func newClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         conf.RedisAddr,
		Username:     conf.RedisUser,
		Password:     conf.RedisPassword,
		DB:           conf.RedisDatabase,
		DialTimeout:  time.Second * time.Duration(conf.RedisDialTimeout),
		ReadTimeout:  time.Second * time.Duration(conf.RedisReadTimeout),
		WriteTimeout: time.Second * time.Duration(conf.RedisWriteTimeout),
		PoolSize:     conf.RedisPoolSize,
		PoolTimeout:  time.Second * time.Duration(conf.RedisPoolTimeout),
		MinIdleConns: conf.RedisMinIdleConn,
		MaxIdleConns: conf.RedisMaxIdleConn,
	})
}

func newClusterClient() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{conf.RedisAddr},
		Username:     conf.RedisUser,
		Password:     conf.RedisPassword,
		DialTimeout:  time.Second * time.Duration(conf.RedisDialTimeout),
		ReadTimeout:  time.Second * time.Duration(conf.RedisReadTimeout),
		WriteTimeout: time.Second * time.Duration(conf.RedisWriteTimeout),
		PoolSize:     conf.RedisPoolSize,
		PoolTimeout:  time.Second * time.Duration(conf.RedisPoolTimeout),
		MinIdleConns: conf.RedisMinIdleConn,
		MaxIdleConns: conf.RedisMaxIdleConn,
	})
}

func (c *Cli) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *Cli) Set(ctx context.Context, key, val string) error {
	return c.client.Set(ctx, key, val, time.Second*time.Duration(conf.RedisExpirationSeconds)).Err()
}

func (c *Cli) Del(ctx context.Context, keys ...string) error {
	return c.client.Del(ctx, keys...).Err()
}

func (c *Cli) Scan(ctx context.Context, match string, limit int64) ([]string, error) {
	keys, _, err := c.client.Scan(ctx, 0, match, limit).Result()
	return keys, err
}

func (c *Cli) getLimitKeys(ctx context.Context, limit int64) ([]string, error) {
	return c.Scan(ctx, "*", limit)
}
