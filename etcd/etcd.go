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

package etcd

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"perf-storage-go/conf"
	"perf-storage-go/metrics"
	"perf-storage-go/util"
	"strings"
	"time"
)

func Start() error {
	endpoints := strings.Split(conf.Endpoints, ",")
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		Username:    conf.Username,
		Password:    conf.Password,
		DialTimeout: time.Duration(conf.DialTimeout),
	})
	if err != nil {
		return err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Duration(conf.DialTimeout))

	resp, err := client.Get(ctx, conf.ETCDPath)
	if err != nil {
		return err
	}
	keyList := util.GetIdList(conf.ETCDDataSize - int(resp.Count))
	for _, key := range keyList {
		start := time.Now()
		_, err := client.Put(ctx, fmt.Sprintf("%s/%s", conf.ETCDPath, key), util.RandStr(conf.ETCDDataLength))
		if err != nil {
			metrics.FailCount.WithLabelValues(conf.StorageTypeEtcd, conf.OperationTypeInsert).Inc()
			logrus.Error("put fail. ", err)
		} else {
			metrics.SuccessCount.WithLabelValues(conf.StorageTypeEtcd, conf.OperationTypeInsert).Inc()
			metrics.SuccessLatency.WithLabelValues(conf.StorageTypeEtcd, conf.OperationTypeInsert).Observe(float64(time.Since(start)))
		}
	}
	cancelFunc()
	defer client.Close()
	return nil
}
