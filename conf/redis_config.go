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

package conf

import "perf-storage-go/util"

var (
	RedisDatabase     = util.GetEnvInt("REDIS_DATABASE", 0)
	RedisAddr         = util.GetEnvStr("REDIS_ADDR", "localhost:9000")
	RedisUser         = util.GetEnvStr("REDIS_USER", "")
	RedisPassword     = util.GetEnvStr("REDIS_PASSWORD", "")
	RedisCluster      = util.GetEnvBool("REDIS_CLUSTER", false)
	RedisDialTimeout  = util.GetEnvInt("REDIS_DIAL_SECONDS", 0)
	RedisReadTimeout  = util.GetEnvInt("REDIS_READ_TIMEOUT", 0)
	RedisWriteTimeout = util.GetEnvInt("REDIS_WRITE_TIMEOUT", 0)
	RedisPoolSize     = util.GetEnvInt("REDIS_POOL_SIZE", 10)
	RedisPoolTimeout  = util.GetEnvInt("REDIS_POOL_TIMEOUT", 0)
	RedisMinIdleConn  = util.GetEnvInt("REDIS_MIN_IDLE_CONN", 5)
	RedisMaxIdleConn  = util.GetEnvInt("REDIS_MAX_IDLE_CONN", 10)
)
