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
	"io"
	"perf-storage-go/conf"
	"perf-storage-go/util"
)

type Cli struct {
	client   *minio.Client
	dataSize int64
}

func (c Cli) BucketExists(ctx context.Context, name string) (bool, error) {
	return c.client.BucketExists(ctx, name)
}

func (c Cli) MakeBucket(ctx context.Context, name string, opts minio.MakeBucketOptions) error {
	return c.client.MakeBucket(ctx, name, opts)
}

func (c Cli) ListObjects(ctx context.Context, name string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	return c.client.ListObjects(ctx, name, opts)
}

func (c Cli) PutObject(ctx context.Context, name string, key string, dataSize int64, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	var data []byte
	if conf.RandomDataEnable {
		data = util.RandBytes(dataSize)
	} else {
		data = FixedBytesCache
	}
	return c.client.PutObject(ctx, name, key, bytes.NewReader(data), objectSize, opts)
}

func (c Cli) GetObject(ctx context.Context, name string, key string, opts minio.GetObjectOptions) error {
	object, err := c.client.GetObject(ctx, name, key, opts)
	if err != nil {
		_, err = io.ReadAll(object)
		return err
	}
	return err
}

func newCli() (*Cli, error) {
	client, err := minio.New(conf.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.MinioUsername, conf.MinioPassword, ""),
		Secure: false,
	})
	return &Cli{
		client:   client,
		dataSize: conf.DataSize,
	}, err
}