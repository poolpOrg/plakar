/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Repository struct {
	location    string
	Repository  string
	minioClient *minio.Client
	bucketName  string

	useSsl          bool
	accessKey       string
	secretAccessKey string
}

func init() {
	storage.Register("s3", NewRepository)
}

func NewRepository(storeConfig map[string]string) (storage.Store, error) {
	var accessKey string
	if value, ok := storeConfig["access_key"]; !ok {
		return nil, fmt.Errorf("missing access_key")
	} else {
		accessKey = value
	}

	var secretAccessKey string
	if value, ok := storeConfig["secret_access_key"]; !ok {
		return nil, fmt.Errorf("missing secret_access_key")
	} else {
		secretAccessKey = value
	}

	useSsl := true
	if value, ok := storeConfig["use_ssl"]; ok {
		tmp, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("invalid use_ssl value")
		}
		useSsl = tmp
	}

	return &Repository{
		location:        storeConfig["location"],
		accessKey:       accessKey,
		secretAccessKey: secretAccessKey,
		useSsl:          useSsl,
	}, nil
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repository *Repository) connect(location *url.URL) error {
	endpoint := location.Host
	useSSL := repository.useSsl

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(repository.accessKey, repository.secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return err
	}

	repository.minioClient = minioClient
	return nil
}

func (repository *Repository) Create(config []byte) error {
	parsed, err := url.Parse(repository.location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}
	repository.bucketName = parsed.RequestURI()[1:]

	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return err
	}
	if !exists {
		err = repository.minioClient.MakeBucket(context.Background(), repository.bucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}

	_, err = repository.minioClient.StatObject(context.Background(), repository.bucketName, "CONFIG", minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			return err
		}
	} else {
		return fmt.Errorf("bucket already initialized")
	}

	_, err = repository.minioClient.PutObject(context.Background(), repository.bucketName, "CONFIG", bytes.NewReader(config), int64(len(config)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (repository *Repository) Open() ([]byte, error) {
	parsed, err := url.Parse(repository.location)
	if err != nil {
		return nil, err
	}

	err = repository.connect(parsed)
	if err != nil {
		return nil, err
	}

	repository.bucketName = parsed.RequestURI()[1:]

	exists, err := repository.minioClient.BucketExists(context.Background(), repository.bucketName)
	if err != nil {
		return nil, fmt.Errorf("error checking if bucket exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket does not exist")
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, "CONFIG", minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting object: %w", err)
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting object stat: %w", err)
	}

	data := make([]byte, stat.Size)
	_, err = object.Read(data)
	if err != nil {
		if err != io.EOF {
			return nil, fmt.Errorf("error reading object: %w", err)
		}
	}
	object.Close()

	return data, nil
}

func (repository *Repository) Close() error {
	return nil
}

// states
func (repository *Repository) GetStates() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "states/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "states/") && len(object.Key) >= 10 {
			t, err := hex.DecodeString(object.Key[10:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 objects.MAC
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutState(mac objects.MAC, rd io.Reader) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", mac[0], mac), rd, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetState(mac objects.MAC) (io.Reader, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", mac[0], mac), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (repository *Repository) DeleteState(mac objects.MAC) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", mac[0], mac), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "packfiles/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "packfiles/") && len(object.Key) >= 13 {
			t, err := hex.DecodeString(object.Key[13:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 objects.MAC
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutPackfile(mac objects.MAC, rd io.Reader) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac), rd, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetPackfile(mac objects.MAC) (io.Reader, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (repository *Repository) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	opts := minio.GetObjectOptions{}
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac), opts)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, length)
	if nbytes, err := object.ReadAt(buffer, int64(offset)); err != nil {
		return nil, err
	} else if nbytes != int(length) {
		return nil, fmt.Errorf("short read")
	}

	return bytes.NewBuffer(buffer), nil
}

func (repository *Repository) DeletePackfile(mac objects.MAC) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", mac[0], mac), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetLocks() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "locks/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "locks/") && len(object.Key) >= 6 {
			t, err := hex.DecodeString(object.Key[6:])
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			ret = append(ret, objects.MAC(t))
		}
	}

	return ret, nil
}

func (repository *Repository) PutLock(lockID objects.MAC, rd io.Reader) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("locks/%016x", lockID), rd, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetLock(lockID objects.MAC) (io.Reader, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("locks/%016x", lockID), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (repository *Repository) DeleteLock(lockID objects.MAC) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("locks/%016x", lockID), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}
