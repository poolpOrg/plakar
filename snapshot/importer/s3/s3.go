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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/importer"
)

type S3Importer struct {
	minioClient *minio.Client
	bucket      string
	host        string
	scanDir     string

	ino uint64
}

func init() {
	importer.Register("s3", NewS3Importer)
}

func connect(location *url.URL, useSsl bool, accessKeyID, secretAccessKey string) (*minio.Client, error) {
	endpoint := location.Host

	// Initialize minio client object.
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSsl,
	})
}

func NewS3Importer(config map[string]string) (importer.Importer, error) {

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

	useSsl := true
	if value, ok := config["use_ssl"]; ok {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid use_ssl value")
		}
		useSsl = tmp
	}

	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	conn, err := connect(parsed, useSsl, accessKey, secretAccessKey)
	if err != nil {
		return nil, err
	}

	atoms := strings.Split(parsed.RequestURI()[1:], "/")
	bucket := atoms[0]
	scanDir := filepath.Clean("/" + strings.Join(atoms[1:], "/"))

	return &S3Importer{
		bucket:      bucket,
		scanDir:     scanDir,
		minioClient: conn,
		host:        parsed.Host,
	}, nil
}

func (p *S3Importer) scanRecursive(prefix string, result chan *importer.ScanResult) {
	for object := range p.minioClient.ListObjects(context.Background(), p.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: false}) {
		objectPath := "/" + object.Key
		if !strings.HasPrefix(objectPath, p.scanDir) && !strings.HasPrefix(p.scanDir, objectPath) {
			continue
		}

		if strings.HasSuffix(object.Key, "/") {
			p.scanRecursive(object.Key, result)
		} else {
			fi := objects.NewFileInfo(
				filepath.Base("/"+prefix+object.Key),
				object.Size,
				0700,
				object.LastModified,
				1,
				atomic.AddUint64(&p.ino, 1),
				0,
				0,
				0,
			)
			result <- importer.NewScanRecord("/"+object.Key, "", fi, nil)
		}
	}

	var currentName string
	if prefix == "" {
		currentName = "/"
	} else {
		currentName = filepath.Base(prefix)
	}

	fi := objects.NewFileInfo(
		currentName,
		0,
		0700|os.ModeDir,
		time.Now(),
		0,
		atomic.AddUint64(&p.ino, 1),
		0,
		0,
		0,
	)
	result <- importer.NewScanRecord("/"+prefix, "", fi, nil)
}

func (p *S3Importer) Scan() (<-chan *importer.ScanResult, error) {
	c := make(chan *importer.ScanResult)
	go func() {
		defer close(c)
		p.scanRecursive("", c)
	}()
	return c, nil
}

func (p *S3Importer) NewReader(pathname string) (io.ReadCloser, error) {
	if pathname == "/" {
		return nil, fmt.Errorf("cannot read root directory")
	}
	if strings.HasSuffix(pathname, "/") {
		return nil, fmt.Errorf("cannot read directory")
	}
	pathname = strings.TrimPrefix(pathname, "/")

	obj, err := p.minioClient.GetObject(context.Background(), p.bucket, pathname,
		minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (p *S3Importer) NewExtendedAttributeReader(pathname string, attribute string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("extended attributes are not supported on S3")
}

func (p *S3Importer) GetExtendedAttributes(pathname string) ([]importer.ExtendedAttributes, error) {
	return nil, fmt.Errorf("extended attributes are not supported on S3")
}

func (p *S3Importer) Close() error {
	return nil
}

func (p *S3Importer) Root() string {
	return p.scanDir
}

func (p *S3Importer) Origin() string {
	return p.host + "/" + p.bucket
}

func (p *S3Importer) Type() string {
	return "s3"
}
