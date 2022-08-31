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
	"github.com/protocol-laboratory/zookeeper-codec-go/zknet"
	"github.com/sirupsen/logrus"
	"perf-storage-go/util"
)

type zkConfig struct {
	host           string
	port           int
	path           string
	permissions    int
	zkNodeTotalNum int
}

func initConfig() *zkConfig {
	return &zkConfig{
		host:           util.GetEnvStr("ZK_HOST", "localhost"),
		port:           util.GetEnvInt("ZK_PORT", 2181),
		path:           util.GetEnvStr("ZK_PATH", "/perf"),
		permissions:    util.GetEnvInt("ZK_PERMISSIONS", 31),
		zkNodeTotalNum: util.GetEnvInt("ZK_NODE_TOTAL_NUM", 10),
	}
}

func Start() error {

	logrus.Info("init zk config.")
	config := initConfig()

	logrus.Info("perf zk start.")
	client, err := newZkClient(config.host, config.port)
	if err != nil {
		return err
	}

	if client.connect() != nil {
		logrus.Errorf("connect zk fail. %s", config.host)
		return err
	}

	netClient := client.netClient
	exists, err := netClient.Exists(&codec.ExistsReq{
		TransactionId: 0,
		OpCode:        codec.OP_EXISTS,
		Path:          config.path,
		Watch:         true,
	})

	if err != nil || exists == nil {
		err := create(netClient, config.permissions, config.path, "")
		if err != nil {
			return err
		}
	}
	children, err := netClient.GetChildren(&codec.GetChildrenReq{
		TransactionId: 1,
		OpCode:        codec.OP_GET_DATA,
		Path:          config.path,
		Watch:         true,
	})
	if err != nil {
		return err
	}
	folders := children.Children
	ids := config.zkNodeTotalNum - len(folders)
	if ids > 0 {
		idList := util.GetIdList(ids)
		for _, val := range idList {
			_ = create(netClient, config.permissions, config.path+"/"+val, val)
		}
	}
	return nil
}

func create(netClient *zknet.ZookeeperNetClient, permissions int, path string, val string) error {
	_, err := netClient.Create(&codec.CreateReq{
		TransactionId: 1,
		OpCode:        codec.OP_CREATE,
		Path:          path,
		Data:          []byte(val),
		Permissions:   []int{permissions},
		Scheme:        "world",
		Credentials:   "anyone",
		Flags:         0,
	})
	if err != nil {
		logrus.Errorf("create data fail. id: %s %s", val, err)
	}
	return err
}
