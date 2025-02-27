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

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
)

type Repository struct {
	location  string
	packfiles Buckets
	states    Buckets
}

func init() {
	storage.Register("fs", NewRepository)
}

func NewRepository(storeConfig map[string]string) (storage.Store, error) {
	return &Repository{
		location: storeConfig["location"],
	}, nil
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

func (repo *Repository) Create(config []byte) error {

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

	return WriteToFileAtomic(repo.Path("CONFIG"), bytes.NewReader(config))
}

func (repo *Repository) Open() ([]byte, error) {

	repo.packfiles = NewBuckets(repo.Path("packfiles"))
	repo.states = NewBuckets(repo.Path("states"))

	rd, err := os.Open(repo.Path("CONFIG"))
	if err != nil {
		return nil, err
	}
	defer rd.Close() // do we care about err?

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (repo *Repository) GetPackfiles() ([]objects.MAC, error) {
	return repo.packfiles.List()
}

func (repo *Repository) GetPackfile(mac objects.MAC) (io.Reader, error) {
	fp, err := repo.packfiles.Get(mac)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}

	return fp, nil
}

func (repo *Repository) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	res, err := repo.packfiles.GetBlob(mac, offset, length)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = repository.ErrPackfileNotFound
		}
		return nil, err
	}
	return res, nil
}

func (repo *Repository) DeletePackfile(mac objects.MAC) error {
	return repo.packfiles.Remove(mac)
}

func (repo *Repository) PutPackfile(mac objects.MAC, rd io.Reader) error {
	return repo.packfiles.Put(mac, rd)
}

func (repo *Repository) Close() error {
	return nil
}

/* Indexes */
func (repo *Repository) GetStates() ([]objects.MAC, error) {
	return repo.states.List()
}

func (repo *Repository) PutState(mac objects.MAC, rd io.Reader) error {
	return repo.states.Put(mac, rd)
}

func (repo *Repository) GetState(mac objects.MAC) (io.Reader, error) {
	return repo.states.Get(mac)
}

func (repo *Repository) DeleteState(mac objects.MAC) error {
	return repo.states.Remove(mac)
}
