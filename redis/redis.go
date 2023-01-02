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
	"github.com/sirupsen/logrus"
	"go.uber.org/ratelimit"
	"math/rand"
	"perf-storage-go/conf"
	"perf-storage-go/metrics"
	"perf-storage-go/util"
	"time"
)

type KeySet []string

func (ks *KeySet) RandElement() string {
	return (*ks)[util.RandNumber(0, int64(len(*ks)))]
}

func Start() error {
	logrus.Info("perf storage redis start")

	client := newCli()

	keys, err := presetData(client)
	if err != nil {
		logrus.Errorf("preset data failed: %v", err)
		return err
	}

	for i := 0; i < conf.RoutineNum; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logrus.Errorf("goroutine error: %v", err)
				}
			}()
			limiter := ratelimit.New(conf.RoutineRateLimit)

			for {
				startTime := time.Now()
				limiter.Take()
				randomF := rand.Float64()
				opKey := keys.RandElement()
				if randomF < conf.ReadOpPercent {
					if _, err := client.Get(context.Background(), opKey); err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeInsert).Inc()
						logrus.Errorf("get redis key: %s , error: %v", opKey, err)
					} else {
						metrics.SuccessCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeREAD).Inc()
						metrics.SuccessLatency.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeREAD).Observe(float64(time.Since(startTime).Milliseconds()))
					}
				}

				if randomF < conf.UpdateOpPercent {
					if err := client.Set(context.Background(), opKey, util.RandStr(conf.DataSize)); err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeUpdate).Inc()
						logrus.Errorf("set redis key: %s , error: %v", opKey, err)
					} else {
						metrics.SuccessCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeUpdate).Inc()
						metrics.SuccessLatency.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeUpdate).Observe(float64(time.Since(startTime).Milliseconds()))
					}
				}
			}
		}()
	}

	return nil
}

func presetData(cli *Cli) (KeySet, error) {
	nowKeys, err := cli.getLimitKeys(context.Background(), int64(conf.DataSetSize))
	if err != nil {
		logrus.Errorf("get preset data failed: %v", err)
		logrus.Infof("generate data size: %d", conf.DataSetSize)
	}

	generateSize := conf.DataSetSize - len(nowKeys)

	logrus.Infof("current key size: %d, need generate data size: %d", len(nowKeys), generateSize)

	if generateSize <= 0 {
		return nowKeys, nil
	}

	presetKeys := util.GetIdList(generateSize)

	for _, presetKey := range presetKeys {
		startTime := time.Now()
		if err := cli.Set(context.Background(), presetKey, util.RandStr(conf.DataSize)); err != nil {
			metrics.FailCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeInsert).Inc()
			logrus.Errorf("set redis key: %s , error: %v", presetKey, err)
		} else {
			metrics.SuccessCount.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeInsert).Inc()
			metrics.SuccessLatency.WithLabelValues(conf.StorageTypeRedis, conf.OperationTypeInsert).Observe(float64(time.Since(startTime).Milliseconds()))
		}
	}
	logrus.Infof("preset data success!")

	return append(nowKeys, presetKeys...), nil
}
