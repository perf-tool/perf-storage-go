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
	"github.com/protocol-laboratory/zookeeper-codec-go/codec"
	"github.com/sirupsen/logrus"
	"perf-storage-go/conf"
	"perf-storage-go/util"
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

	exists, err := client.netClient.Exists(&codec.ExistsReq{
		TransactionId: client.transactionId,
		OpCode:        codec.OP_EXISTS,
		Path:          conf.ZkPath,
		Watch:         true,
	})
	client.transactionId += 1

	if err != nil {
		return err
	}

	if exists.Error != codec.EC_OK {
		err := create(client, conf.ZkPermissions, conf.ZkPath, "")
		if err != nil {
			return err
		}
	}

	children, err := client.netClient.GetChildren(&codec.GetChildrenReq{
		TransactionId: client.transactionId,
		OpCode:        codec.OP_GET_DATA,
		Path:          conf.ZkPath,
		Watch:         true,
	})
	client.transactionId += 1
	if err != nil {
		return err
	}
	folders := children.Children
	ids := conf.ZkNodeTotalNum - len(folders)
	if ids > 0 {
		idList := util.GetIdList(ids)
		for _, val := range idList {
			_ = create(client, conf.ZkPermissions, conf.ZkPath+"/"+val, val)
		}
	}
	return nil
}

func create(zkClient *zkClient, permissions int, path string, val string) error {
	_, err := zkClient.netClient.Create(&codec.CreateReq{
		TransactionId: zkClient.transactionId,
		OpCode:        codec.OP_CREATE,
		Path:          path,
		Data:          []byte(val),
		Permissions:   []int{permissions},
		Scheme:        "world",
		Credentials:   "anyone",
		Flags:         0,
	})
	zkClient.transactionId += 1
	if err != nil {
		logrus.Errorf("create data fail. id: %s %s", val, err)
	}
	return err
}
