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
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"go.uber.org/ratelimit"
	"io"
	"math/rand"
	"perf-storage-go/conf"
	"perf-storage-go/metrics"
	"perf-storage-go/util"
	"time"
)

func Start() error {
	logrus.Info("perf storage minio start")
	client, err := minio.New(conf.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.MinioUsername, conf.MinioPassword, ""),
		Secure: false,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	bucketExists, err := client.BucketExists(ctx, conf.MinioBucketName)
	if err != nil {
		return err
	}
	if !bucketExists {
		logrus.Infof("bucket %s not exist, create it", conf.MinioBucketName)
		err = client.MakeBucket(context.TODO(), conf.MinioBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}
	listObjects := client.ListObjects(context.TODO(), conf.MinioBucketName, minio.ListObjectsOptions{})
	nowKeys := make([]string, 0)
	for object := range listObjects {
		if object.Err != nil {
			return object.Err
		}
		nowKeys = append(nowKeys, object.Key)
	}
	needDataSetSize := conf.DataSetSize - len(nowKeys)
	if needDataSetSize > 0 {
		keys := util.GetIdList(needDataSetSize)

		var gpool = newGPool(conf.PresetRoutineNum)
		for idx, key := range keys {
			var newKey = key
			gpool.newTask(func() {
				startTime := time.Now()
				_, err := client.PutObject(context.TODO(), conf.MinioBucketName, newKey, bytes.NewReader(util.RandBytes(conf.DataSize)), int64(conf.DataSetSize), minio.PutObjectOptions{})
				if err != nil {
					metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Inc()
					logrus.Errorf("put object key %s error %v", newKey, err)
				} else {
					metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Inc()
					metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeInsert).Observe(float64(time.Since(startTime).Milliseconds()))
				}
			})
			// sleep
			if idx != 0 && idx%conf.PresetRoutineNum == 0 && conf.UpdateRateInterval != 0 {
				time.Sleep(time.Second * time.Duration(conf.UpdateRateInterval))
			}
		}
		gpool.wait()
		nowKeys = append(nowKeys, keys...)
	}
	logrus.Info("preset data end")
	for i := 0; i < conf.RoutineNum; i++ {
		go func() {
			limiter := ratelimit.New(conf.RoutineRateLimit)
			client, err := minio.New(conf.MinioEndpoint, &minio.Options{
				Creds:  credentials.NewStaticV4(conf.MinioUsername, conf.MinioPassword, ""),
				Secure: false,
			})
			if err != nil {
				logrus.Error("create minio client error", err)
			}
			for {
				limiter.Take()
				randomF := rand.Float64()
				if randomF < conf.ReadOpPercent {
					key := nowKeys[rand.Intn(len(nowKeys))]
					startTime := time.Now()
					object, err := client.GetObject(context.TODO(), conf.MinioBucketName, key, minio.GetObjectOptions{})
					if err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Inc()
						logrus.Error("get object error", err)
					} else {
						_, err = io.ReadAll(object)
						if err != nil {
							metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Inc()
							logrus.Errorf("read object key %s error %v", key, err)
						} else {
							metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Inc()
							metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeREAD).Observe(float64(time.Since(startTime)))
						}
					}
				}
				if randomF < conf.UpdateOpPercent {
					key := nowKeys[rand.Intn(len(nowKeys))]
					startTime := time.Now()
					_, err := client.PutObject(context.TODO(), conf.MinioBucketName, key, bytes.NewReader(util.RandBytes(conf.DataSize)), int64(conf.DataSetSize), minio.PutObjectOptions{})
					if err != nil {
						metrics.FailCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Inc()
						logrus.Errorf("put object key %s error %v", key, err)
					} else {
						metrics.SuccessCount.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Inc()
						metrics.SuccessLatency.WithLabelValues(conf.StorageTypeMinio, conf.OperationTypeUpdate).Observe(float64(time.Since(startTime)))
					}
				}

				if conf.ReadRateInterval != 0 {
					time.Sleep(time.Second * time.Duration(conf.ReadRateInterval))
				}
			}
		}()
	}
	return nil
}
