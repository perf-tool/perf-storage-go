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
	"perf-storage-go/util"
)

var (
	Endpoints          = util.GetEnvStr("ETCD_ENDPOINTS", "localhost:2379")
	Username           = util.GetEnvStr("ETCD_USERNAME", "")
	Password           = util.GetEnvStr("ETCD_PASSWORD", "")
	DialTimeoutSeconds = util.GetEnvInt("ETCD_DIAL_TIMEOUT_SECONDS", 5)
	EtcdPath           = util.GetEnvStr("ETCD_PATH", "/perf")
)
