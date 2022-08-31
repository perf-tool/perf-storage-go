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
	"fmt"
	"github.com/protocol-laboratory/zookeeper-codec-go/codec"
	"github.com/protocol-laboratory/zookeeper-codec-go/zknet"
	"github.com/sirupsen/logrus"
)

const defaultTimeout = 30_000

type zkClient struct {
	netClient     *zknet.ZookeeperNetClient
	transactionId int
}

func (z *zkClient) connect() error {
	resp, err := z.netClient.Connect(&codec.ConnectReq{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         defaultTimeout,
		SessionId:       0,
		Password:        codec.PasswordEmpty,
		ReadOnly:        false,
	})
	if err != nil {
		return err
	}
	logrus.Info("session id is ", resp.SessionId)
	return nil
}

func (z *zkClient) close() error {
	closeResp, err := z.netClient.CloseSession(&codec.CloseReq{
		TransactionId: z.transactionId,
	})
	if err != nil {
		return err
	}
	if closeResp.Error != codec.EC_OK {
		return fmt.Errorf("close session failed, code is %d", closeResp.Error)
	}
	return nil
}

func newZkClient(host string, port int) (*zkClient, error) {
	zkNetClient, err := zknet.NewZkNetClient(zknet.ZookeeperNetClientConfig{
		Host: host,
		Port: port,
	})
	if err != nil {
		return nil, err
	}
	zkClient := &zkClient{
		transactionId: 0,
	}
	zkClient.netClient = zkNetClient
	return zkClient, nil
}
