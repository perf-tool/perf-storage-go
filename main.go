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

package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"perf-storage-go/conf"
	"perf-storage-go/etcd"
	"perf-storage-go/metrics"
	"perf-storage-go/minio"
	"perf-storage-go/mysql"
	"perf-storage-go/redis"
	"perf-storage-go/zookeeper"

	_ "net/http/pprof"
)

func main() {
	logrus.Info("perf storage start")
	var err error
	metrics.Init()
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(":20004", nil)
		if err != nil {
			logrus.Error("http listener start failed")
			panic(err)
		}
	}()
	switch conf.StorageType {
	case conf.StorageTypeEtcd:
		err = etcd.Start()
	case conf.StorageTypeMinio:
		err = minio.Start()
	case conf.StorageTypeMysql:
		err = mysql.Start()
	case conf.StorageTypeRedis:
		err = redis.Start()
	case conf.StorageTypeZooKeeper:
		err = zookeeper.Start()
	}
	if err != nil {
		panic(err)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
	}
}
