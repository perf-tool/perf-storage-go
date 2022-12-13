package minio

import (
	"bytes"
	"context"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"perf-storage-go/conf"
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

func (c Cli) PutObject(ctx context.Context, name string, key string, reader *bytes.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return c.client.PutObject(ctx, name, key, reader, objectSize, opts)
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
