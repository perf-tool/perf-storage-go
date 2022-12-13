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
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"perf-storage-go/conf"
	"perf-storage-go/util"
)

const FixedFileDir = "/opt/perf/testdata/"

type Cli struct {
	client     *minio.Client
	dataSize   int64
	bufferType string
	filename   string
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

func (c Cli) PutObject(ctx context.Context, name string, key string, dataSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	switch c.bufferType {
	case conf.ExchangeTypeFile:
		return c.client.FPutObject(ctx, name, key, c.filename, opts)
	default:
		var data []byte
		if conf.RandomDataEnable {
			data = util.RandBytes(dataSize)
		} else {
			data = FixedBytesCache
		}
		return c.client.PutObject(ctx, name, key, bytes.NewReader(data), dataSize, opts)
	}
}

func (c Cli) GetObject(ctx context.Context, name string, key string, opts minio.GetObjectOptions) error {
	var err error
	var object *minio.Object
	switch conf.ExchangeType {
	case conf.ExchangeTypeFile:
		err = c.client.FGetObject(ctx, name, key, fmt.Sprintf("%s_download", c.filename), opts)
		if err != nil {
			logrus.Errorf("get file object failed: %v", err)
			return err
		}
	default:
		object, err = c.client.GetObject(ctx, name, key, opts)
		if err != nil {
			logrus.Errorf("get memory object failed: %v", err)
			return err
		}
		_, err := object.Stat()
		if err != nil {
			logrus.Errorf("read object metadata failed: %v", err)
			return err
		}
	}
	return err
}

func newCli() (*Cli, error) {
	client, err := minio.New(conf.MinioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(conf.MinioUsername, conf.MinioPassword, ""),
		Secure: false,
	})

	// if read from file, filename is resource
	var filename = fmt.Sprintf("%s%s", FixedFileDir, util.RandStr(8))
	switch conf.ExchangeType {
	case conf.ExchangeTypeFile:
		if err := util.DDFile(filename, conf.DataSize/1024, util.SizeUnitKB); err != nil {
			logrus.Errorf("dd file failed: %v", err)
			return nil, err
		}
	default:

	}

	return &Cli{
		client:     client,
		dataSize:   conf.DataSize,
		bufferType: conf.ExchangeType,
		filename:   filename,
	}, err
}
