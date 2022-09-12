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

package zookeeper

import (
	"errors"
	"fmt"
	"github.com/protocol-laboratory/zookeeper-codec-go/codec"
	"github.com/sirupsen/logrus"
	"perf-storage-go/conf"
	"perf-storage-go/metrics"
	"perf-storage-go/util"
	"time"
)

func Start() error {
	logrus.Info("perf storage zk start")
	client, err := newZkClient(conf.ZkHost, conf.ZkPort)
	if err != nil {
		return err
	}

	if client.connect() != nil {
		logrus.Errorf("connect zk fail. %s", conf.ZkHost)
		return err
	}

	exists, err := client.exists(conf.ZkPath)

	if err != nil {
		return err
	}

	if exists.Error != codec.EC_OK {
		resp, err := client.create(conf.ZkPath, []byte(""), conf.ZkPermission)
		if err != nil {
			logrus.Errorf("create zk path %s error %v", conf.ZkPath, err)
			return err
		}
		if resp.Error != codec.EC_OK {
			str := fmt.Sprintf("create zk path %s error %d", conf.ZkPath, resp.Error)
			logrus.Errorf(str)
			return errors.New(str)
		}
	}

	childrenResp, err := client.getChildren(conf.ZkPath)
	if err != nil {
		logrus.Errorf("get children %s error %d", conf.ZkPath, err)
		return err
	}
	if childrenResp.Error != codec.EC_OK {
		str := fmt.Sprintf("get children %s error %d", conf.ZkPath, childrenResp.Error)
		logrus.Errorf(str)
		return errors.New(str)
	}
	folders := childrenResp.Children
	ids := conf.DataSetSize - len(folders)
	if ids > 0 {
		idList := util.GetIdList(ids)
		for _, val := range idList {
			start := time.Now()
			path := conf.ZkPath + "/" + val
			resp, err := client.create(path, util.RandBytes(conf.ZkDataSize), conf.ZkPermission)
			if err != nil {
				metrics.SuccessCount.WithLabelValues(conf.StorageTypeZooKeeper, conf.OperationTypeInsert).Inc()
				metrics.SuccessLatency.WithLabelValues(conf.StorageTypeZooKeeper, conf.OperationTypeInsert).Observe(float64(time.Since(start)))
				logrus.Errorf("create zk path %s error %v", path, err)
			}
			if resp.Error != codec.EC_OK {
				metrics.FailCount.WithLabelValues(conf.StorageTypeZooKeeper, conf.OperationTypeInsert).Inc()
				logrus.Errorf("create zk path %s error %d", path, resp.Error)
			}
		}
	}
	defer client.close()
	return nil
}
