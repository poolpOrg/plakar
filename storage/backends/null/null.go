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
	config     storage.Configuration
	Repository string
	location   string
}

func init() {
	storage.Register("null", NewRepository)
}

func NewRepository(location string) storage.Store {
	return &Repository{
		location: location,
	}
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repository *Repository) Create(location string, config storage.Configuration) error {
	return nil
}

func (repository *Repository) Open(location string) error {
	repositoryConfig := storage.NewConfiguration()
	repository.config = *repositoryConfig

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
	return []objects.Checksum{}, nil
}

func (repository *Repository) PutSnapshot(snapshotID objects.Checksum, data []byte) error {
	return nil
}

func (repository *Repository) GetSnapshot(snapshotID objects.Checksum) ([]byte, error) {
	return []byte{}, nil
}

func (repository *Repository) DeleteSnapshot(snapshotID objects.Checksum) error {
	return nil
}

// states
func (repository *Repository) GetStates() ([]objects.Checksum, error) {
	return []objects.Checksum{}, nil
}

func (repository *Repository) PutState(checksum objects.Checksum, rd io.Reader) error {
	return nil
}

func (repository *Repository) GetState(checksum objects.Checksum) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) DeleteState(checksum objects.Checksum) error {
	return nil
}

// packfiles
func (repository *Repository) GetPackfiles() ([]objects.Checksum, error) {
	return []objects.Checksum{}, nil
}

func (repository *Repository) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	return nil
}

func (repository *Repository) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) GetPackfileBlob(checksum objects.Checksum, offset uint32, length uint32) (io.Reader, error) {
	return bytes.NewBuffer([]byte{}), nil
}

func (repository *Repository) DeletePackfile(checksum objects.Checksum) error {
	return nil
}
