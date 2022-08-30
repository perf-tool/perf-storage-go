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

import (
	"os"
	"perf-storage-go/util"
)

var (
	StorageType      = os.Getenv("STORAGE_TYPE")
	PresetRoutineNum = util.GetEnvInt("PRESET_ROUTINE_NUM", 100)
	RoutineNum       = util.GetEnvInt("ROUTINE_NUM", 100)
	RoutineRateLimit = util.GetEnvInt("ROUTINE_RATE_LIMIT", 100)
	DataSetSize      = util.GetEnvInt("DATA_SET_SIZE", 100_000)
	ReadOpPercent    = util.GetEnvFloat64("READ_OP_PERCENT", 0.25)
	UpdateOpPercent  = util.GetEnvFloat64("UPDATE_OP_PERCENT", 0.75)
)

const (
	StorageTypeEtcd      = "ETCD"
	StorageTypeMinio     = "MINIO"
	StorageTypeMysql     = "MYSQL"
	StorageTypeRedis     = "REDIS"
	StorageTypeZooKeeper = "ZOOKEEPER"
	OperationTypeInsert  = "INSERT"
	OperationTypeDelete  = "DELETE"
	OperationTypeUpdate  = "UPDATE"
	OperationTypeREAD    = "READ"
)
