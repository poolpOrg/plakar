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

package fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

type Repository struct {
	config    storage.Configuration
	location  string
	packfiles Buckets
	states    Buckets
}

func init() {
	storage.Register("fs", NewRepository)
}

func NewRepository(location string) storage.Store {
	return &Repository{
		location: location,
	}
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repo *Repository) Path(args ...string) string {
	root := repo.Location()
	if strings.HasPrefix(root, "fs://") {
		root = root[4:]
	}

	args = append(args, "")
	copy(args[1:], args)
	args[0] = root

	return filepath.Join(args...)
}

func (repo *Repository) Create(location string, config storage.Configuration) error {

	err := os.Mkdir(repo.Path(), 0700)
	if err != nil {
		return err
	}

	repo.packfiles = NewBuckets(repo.Path("packfiles"))
	if err := repo.packfiles.Create(); err != nil {
		return err
	}

	repo.states = NewBuckets(repo.Path("states"))
	if err := repo.states.Create(); err != nil {
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

	return WriteToFileAtomic(repo.Path("CONFIG"), compressedConfig)
}

func (repo *Repository) Open(location string) error {

	repo.packfiles = NewBuckets(repo.Path("packfiles"))
	repo.states = NewBuckets(repo.Path("states"))

	rd, err := os.Open(repo.Path("CONFIG"))
	if err != nil {
		return err
	}
	defer rd.Close() // do we care about err?

	jconfig, err := compression.InflateStream("GZIP", rd)
	if err != nil {
		return err
	}

	data, err := io.ReadAll(jconfig)
	if err != nil {
		return err
	}

	config := storage.Configuration{}
	err = msgpack.Unmarshal(data, &config)
	if err != nil {
		return err
	}

	repo.config = config

	return nil
}

func (repo *Repository) Configuration() storage.Configuration {
	return repo.config
}

func (repo *Repository) GetPackfiles() ([]objects.Checksum, error) {
	return repo.packfiles.List()
}

func (repo *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	fp, err := repo.packfiles.Get(checksum)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}

	return fp, nil
}

func (repo *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	res, err := repo.packfiles.GetBlob(checksum, offset, length)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}
	return res, nil
}

func (repo *Repository) DeletePackfile(checksum objects.Checksum) error {
	return repo.packfiles.Remove(checksum)
}

func (repo *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	return repo.packfiles.Put(checksum, rd)
}

func (repo *Repository) Close() error {
	return nil
}

/* Indexes */
func (repo *Repository) GetStates() ([]objects.Checksum, error) {
	return repo.states.List()
}

func (repo *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	return repo.states.Put(checksum, rd)
}

func (repo *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	return repo.states.Get(checksum)
}

func (repo *Repository) DeleteState(checksum objects.Checksum) error {
	return repo.states.Remove(checksum)
}
