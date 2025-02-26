/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package s3

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Exporter struct {
	minioClient *minio.Client
	rootDir     string

	accessKeyID     string
	secretAccessKey string
}

func init() {
	exporter.Register("s3", NewS3Exporter)
}

func connect(location *url.URL, accessKeyID, secretAccessKey string) (*minio.Client, error) {
	endpoint := location.Host
	useSSL := false

	// Initialize minio client object.
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
}

func NewS3Exporter(config map[string]string) (exporter.Exporter, error) {
	location := config["location"]
	var accessKey string
	if tmp, ok := config["access_key"]; !ok {
		return nil, fmt.Errorf("missing access_key")
	} else {
		accessKey = tmp
	}

	var secretAccessKey string
	if tmp, ok := config["secret_access_key"]; !ok {
		return nil, fmt.Errorf("missing secret_access_key")
	} else {
		secretAccessKey = tmp
	}

	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	conn, err := connect(parsed, accessKey, secretAccessKey)
	if err != nil {
		return nil, err
	}

	err = conn.MakeBucket(context.Background(), strings.TrimPrefix(parsed.Path, "/"), minio.MakeBucketOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
			return nil, err
		}
	}

	return &S3Exporter{
		rootDir:     parsed.Path,
		minioClient: conn,
	}, nil
}

func (p *S3Exporter) Root() string {
	return p.rootDir
}

func (p *S3Exporter) CreateDirectory(pathname string) error {
	return nil
}

func (p *S3Exporter) StoreFile(pathname string, fp io.Reader) error {
	_, err := p.minioClient.PutObject(context.Background(),
		strings.TrimPrefix(p.rootDir, "/"),
		strings.TrimPrefix(pathname, p.rootDir+"/"),
		fp, -1, minio.PutObjectOptions{})
	return err
}

func (p *S3Exporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (p *S3Exporter) Close() error {
	return nil
}
