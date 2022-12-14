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

package minio

import (
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/sirupsen/logrus"
	"go.uber.org/ratelimit"
	"math/rand"
	"perf-storage-go/conf"
	"perf-storage-go/metrics"
	"perf-storage-go/util"
	"time"
)

var FixedBytesCache []byte

func Start() error {
	logrus.Info("perf storage minio start")
	client, err := newCli()
	if err != nil {
		logrus.Errorf("new client failed: %v", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	bucketExists, err := client.BucketExists(ctx, conf.MinioBucketName)
	if err != nil {
		logrus.Errorf("get bucket failed: %v", err)
		return err
	}
	if !bucketExists {
		logrus.Infof("bucket %s not exist, create it", conf.MinioBucketName)
		err = client.MakeBucket(context.TODO(), conf.MinioBucketName, minio.MakeBucketOptions{})
		if err != nil {
			logrus.Errorf("create bucket failed: %v", err)
			return err
		}
	}
	listObjects := client.ListObjects(context.TODO(), conf.MinioBucketName, minio.ListObjectsOptions{})
	nowKeys := make([]string, 0)
	for object := range listObjects {
		if object.Err != nil {
			logrus.Errorf("get object failed: %v", err)
			return object.Err
		}
		nowKeys = append(nowKeys, object.Key)
	}
	needDataSetSize := conf.DataSetSize - len(nowKeys)
	if needDataSetSize > 0 {
		keys := util.GetIdList(needDataSetSize)
		var gpool = newGPool(conf.PresetRoutineNum)
		for _, key := range keys {
			var newKey = key
			gpool.newTask(func() {
				logrus.Infof("start put dataset object, bucket: %s, key: %s, size: %d", conf.MinioBucketName, newKey, conf.DataSize)
				startTime := time.Now()
				_, err := client.PutObject(context.TODO(), conf.MinioBucketName, newKey, conf.DataSize)
				if err != nil {
					metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Inc()
					logrus.Errorf("put dataset object key: %s , error: %v", newKey, err)
				} else {
					metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Inc()
					metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Observe(float64(time.Since(startTime).Milliseconds()))
				}
				if conf.UpdateRateInterval != 0 {
					execTime := time.Since(startTime)
					intervalTime := time.Second * time.Duration(conf.UpdateRateInterval)
					if execTime < intervalTime {
						time.Sleep(intervalTime - execTime)
					}
				}
				logrus.Infof("put dataset object, bucket: %s, key: %s, success", conf.MinioBucketName, newKey)
			})
		}
		gpool.wait()
		nowKeys = append(nowKeys, keys...)
	}
	logrus.Info("preset data end")
	for i := 0; i < conf.RoutineNum; i++ {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logrus.Errorf("goroutine error: %v", err)
				}
			}()
			limiter := ratelimit.New(conf.RoutineRateLimit)
			client, err := newCli()
			if err != nil {
				logrus.Errorf("create minio client error: %v", err)
				return
			}
			for {
				startTime := time.Now()
				limiter.Take()
				randomF := rand.Float64()
				if randomF < conf.ReadOpPercent {
					key := nowKeys[rand.Intn(len(nowKeys))]
					logrus.Infof("start get object, bucket: %s, key: %s", conf.MinioBucketName, key)
					err := client.GetObject(context.TODO(), conf.MinioBucketName, key, minio.GetObjectOptions{})
					if err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Inc()
						logrus.Errorf("get object, bucket: %s, key: %s, error: %v", conf.MinioBucketName, key, err)
					} else {
						metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Inc()
						metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Observe(float64(time.Since(startTime)))
						logrus.Infof("get object, bucket: %s, key: %s, success", conf.MinioBucketName, key)
					}
				}
				if randomF < conf.UpdateOpPercent {
					key := nowKeys[rand.Intn(len(nowKeys))]
					logrus.Infof("start put object, bucket: %s, key: %s", conf.MinioBucketName, key)
					startTime := time.Now()
					_, err := client.PutObject(context.TODO(), conf.MinioBucketName, key, conf.DataSize)
					if err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Inc()
						logrus.Errorf("put object, bucket: %s, key: %s, error: %v", conf.MinioBucketName, key, err)
					} else {
						metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Inc()
						metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Observe(float64(time.Since(startTime)))
						logrus.Infof("put object, bucket: %s, key: %s, success", conf.MinioBucketName, key)
					}
				}
				if conf.ReadRateInterval != 0 {
					execTime := time.Since(startTime)
					intervalTime := time.Second * time.Duration(conf.ReadRateInterval)
					if execTime < intervalTime {
						time.Sleep(intervalTime - execTime)
					}
				}
			}
		}()
	}
	return nil
}
