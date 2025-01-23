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
	"log"
	"net/url"
	"strings"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/network"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Repository struct {
	config      storage.Configuration
	location    string
	Repository  string
	minioClient *minio.Client
	bucketName  string
}

func init() {
	network.ProtocolRegister()
	storage.Register("s3", NewRepository)
}

func NewRepository(location string) storage.Store {
	return &Repository{
		location: location,
	}
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repository *Repository) connect(location *url.URL) error {
	endpoint := location.Host
	accessKeyID := location.User.Username()
	secretAccessKey, _ := location.User.Password()
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	repository.minioClient = minioClient
	return nil
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return err
	}

	err = repository.connect(parsed)
	if err != nil {
		return err
	}
	repository.bucketName = parsed.RequestURI()[1:]

	err = repository.minioClient.MakeBucket(context.Background(), repository.bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	jconfig, err := msgpack.Marshal(config)
	if err != nil {
		return err
	}

	compressedConfig, err := compression.DeflateStream("GZIP", bytes.NewReader(jconfig))
	if err != nil {
		return err
	}

	data, err := io.ReadAll(compressedConfig)
	if err != nil {
		return err
	}

	_, err = repository.minioClient.PutObject(context.Background(), repository.bucketName, "CONFIG", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	repository.config = config
	return nil
}

func (repository *Repository) Open(location string) error {
	parsed, err := url.Parse(location)
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
		return fmt.Errorf("bucket does not exist")
	}

	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, "CONFIG", minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	stat, err := object.Stat()
	if err != nil {
		return err
	}

	compressed := make([]byte, stat.Size)
	_, err = object.Read(compressed)
	if err != nil {
		if err != io.EOF {
			return err
		}
	}
	object.Close()

	jconfig, err := compression.InflateStream("GZIP", bytes.NewReader(compressed))
	if err != nil {
		return err
	}

	data, err := io.ReadAll(jconfig)
	if err != nil {
		return err
	}

	var config storage.Configuration
	err = msgpack.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	repository.config = config

	return nil
}

func (repository *Repository) Close() error {
	return nil
}

func (repository *Repository) Configuration() storage.Configuration {
	return repository.config
}

// snapshots
func (repository *Repository) GetSnapshots() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)
	for object := range repository.minioClient.ListObjects(context.Background(), repository.bucketName, minio.ListObjectsOptions{
		Prefix:    "snapshots/",
		Recursive: true,
	}) {
		if strings.HasPrefix(object.Key, "snapshots/") && len(object.Key) == 13 {
			snapshotIDhex, err := hex.DecodeString(object.Key[13:])
			if err != nil {
				continue
			}
			if len(snapshotIDhex) != 64 {
				continue
			}
			var snapshotID objects.Checksum
			copy(snapshotID[:], snapshotIDhex)
			ret = append(ret, snapshotID)
		}
	}
	return ret, nil
}

func (repository *Repository) PutSnapshot(snapshotID objects.Checksum, data []byte) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetSnapshot(snapshotID objects.Checksum) ([]byte, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, stat.Size)
	_, err = object.Read(dataBytes)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}
	object.Close()

	return dataBytes, nil
}

func (repository *Repository) DeleteSnapshot(snapshotID objects.Checksum) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("snapshots/%x/%s", snapshotID[0], hex.EncodeToString(snapshotID[:])), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// states
func (repository *Repository) GetStates() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)
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
			var t32 objects.Checksum
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), rd, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (repository *Repository) DeleteState(checksum objects.Checksum) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("states/%02x/%016x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)
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
			var t32 objects.Checksum
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (repository *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	_, err := repository.minioClient.PutObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), rd, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (repository *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return object, nil
}

func (repository *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	opts := minio.GetObjectOptions{}
	opts.SetRange(int64(offset), int64(offset+length))
	object, err := repository.minioClient.GetObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), opts)
	if err != nil {
		return nil, err
	}
	stat, err := object.Stat()
	if err != nil {
		return nil, err
	}

	if stat.Size < int64(offset+length) {
		return nil, fmt.Errorf("invalid range")
	}

	if _, err := object.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}

	buffer := make([]byte, length)
	if nbytes, err := object.Read(buffer); err != nil {
		return nil, err
	} else if nbytes != int(length) {
		return nil, fmt.Errorf("short read")
	}

	return bytes.NewBuffer(buffer), nil
}

func (repository *Repository) DeletePackfile(checksum objects.Checksum) error {
	err := repository.minioClient.RemoveObject(context.Background(), repository.bucketName, fmt.Sprintf("packfiles/%02x/%016x", checksum[0], checksum), minio.RemoveObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}
