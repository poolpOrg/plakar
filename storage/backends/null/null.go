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

package null

import (
	"bytes"
	"io"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/storage"
)

type Repository struct {
	config     []byte
	Repository string
	location   string
}

func init() {
	storage.Register("null", NewRepository)
}

func NewRepository(storeConfig map[string]string) (storage.Store, error) {
	return &Repository{
		location: storeConfig["location"],
	}, nil
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repository *Repository) Create(config []byte) error {
	repository.config = config
	return nil
}

func (repository *Repository) Open() ([]byte, error) {
	return repository.config, nil
}

func (repository *Repository) Close() error {
	return nil
}

// snapshots
func (repository *Repository) GetSnapshots() ([]objects.MAC, error) {
	return []objects.MAC{}, nil
}

func (repository *Repository) PutSnapshot(snapshotID objects.MAC, data []byte) error {
	return nil
}

func (repository *Repository) GetSnapshot(snapshotID objects.MAC) ([]byte, error) {
	return []byte{}, nil
}

func (repository *Repository) DeleteSnapshot(snapshotID objects.MAC) error {
	return nil
}

// states
func (repository *Repository) GetStates() ([]objects.MAC, error) {
	return []objects.MAC{}, nil
}

func (repository *Repository) PutState(mac objects.MAC, rd io.Reader) error {
	return nil
}

func (repository *Repository) GetState(mac objects.MAC) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) DeleteState(mac objects.MAC) error {
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([]objects.MAC, error) {
	return []objects.MAC{}, nil
}

func (repository *Repository) PutPackfile(mac objects.MAC, rd io.Reader) error {
	return nil
}

func (repository *Repository) GetPackfile(mac objects.MAC) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) DeletePackfile(mac objects.MAC) error {
	return nil
}
