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

package database

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

type Repository struct {
	backend string

	conn    *sql.DB
	wrMutex sync.Mutex

	Repository string
	location   string
}

func init() {
	storage.Register("database", NewRepository)
}

func NewRepository(storeConfig map[string]string) (storage.Store, error) {
	return &Repository{
		location: storeConfig["location"],
	}, nil
}

func (repo *Repository) Location() string {
	return repo.location
}

func (repo *Repository) connect(addr string) error {
	var connectionString string
	if strings.HasPrefix(addr, "sqlite://") {
		repo.backend = "sqlite"
		connectionString = addr[9:]
	} else {
		return fmt.Errorf("unsupported database backend: %s", addr)
	}

	conn, err := sql.Open(repo.backend, connectionString)
	if err != nil {
		return err
	}
	repo.conn = conn

	if repo.backend == "sqlite" {
		_, err = repo.conn.Exec("PRAGMA journal_mode=WAL;")
		if err != nil {
			return nil
		}
		_, err = repo.conn.Exec("PRAGMA busy_timeout=2000;")
		if err != nil {
			return nil
		}

	}

	return nil
}

func (repo *Repository) Create(config []byte) error {
	err := repo.connect(repo.location)
	if err != nil {
		return err
	}

	statement, err := repo.conn.Prepare(`CREATE TABLE IF NOT EXISTS configuration (
		value	BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repo.conn.Prepare(`CREATE TABLE IF NOT EXISTS states (
		mac	VARCHAR(64) NOT NULL PRIMARY KEY,
		data		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repo.conn.Prepare(`CREATE TABLE IF NOT EXISTS packfiles (
		mac	VARCHAR(64) NOT NULL PRIMARY KEY,
		data		BLOB
	);`)
	if err != nil {
		return err
	}
	defer statement.Close()
	statement.Exec()

	statement, err = repo.conn.Prepare(`INSERT INTO configuration(value) VALUES(?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(config)
	if err != nil {
		return err
	}

	return nil
}

func (repo *Repository) Open() ([]byte, error) {
	err := repo.connect(repo.location)
	if err != nil {
		return nil, err
	}

	var buffer []byte

	err = repo.conn.QueryRow(`SELECT value FROM configuration`).Scan(&buffer)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func (repo *Repository) Close() error {
	return nil
}

// states
func (repo *Repository) GetStates() ([]objects.MAC, error) {
	rows, err := repo.conn.Query("SELECT mac FROM states")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	macs := make([]objects.MAC, 0)
	for rows.Next() {
		var mac []byte
		err = rows.Scan(&mac)
		if err != nil {
			return nil, err
		}
		var mac32 objects.MAC
		copy(mac32[:], mac)
		macs = append(macs, mac32)
	}
	return macs, nil
}

func (repo *Repository) PutState(mac objects.MAC, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}

	statement, err := repo.conn.Prepare(`INSERT INTO states (mac, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repo.wrMutex.Lock()
	_, err = statement.Exec(mac[:], data)
	repo.wrMutex.Unlock()
	if err != nil {
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) {
			return err
		}
		if sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT {
			return err
		}
	}

	return nil
}

func (repo *Repository) GetState(mac objects.MAC) (io.Reader, error) {
	var data []byte
	err := repo.conn.QueryRow(`SELECT data FROM states WHERE mac=?`, mac[:]).Scan(&data)
	if err != nil {
		return nil, err
	}
	return bytes.NewBuffer(data), nil
}

func (repo *Repository) DeleteState(mac objects.MAC) error {
	statement, err := repo.conn.Prepare(`DELETE FROM states WHERE mac=?`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repo.wrMutex.Lock()
	_, err = statement.Exec(mac[:])
	repo.wrMutex.Unlock()
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}
	return nil
}

// packfiles
func (repo *Repository) GetPackfiles() ([]objects.MAC, error) {
	rows, err := repo.conn.Query("SELECT mac FROM packfiles")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	macs := make([]objects.MAC, 0)
	for rows.Next() {
		var mac []byte
		err = rows.Scan(&mac)
		if err != nil {
			return nil, err
		}
		var mac32 objects.MAC
		copy(mac32[:], mac)
		macs = append(macs, mac32)
	}
	return macs, nil
}

func (repo *Repository) PutPackfile(mac objects.MAC, rd io.Reader) error {
	data, err := io.ReadAll(rd)
	if err != nil {
		return err
	}

	statement, err := repo.conn.Prepare(`INSERT INTO packfiles (mac, data) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repo.wrMutex.Lock()
	_, err = statement.Exec(mac[:], data)
	repo.wrMutex.Unlock()
	if err != nil {
		var sqliteErr *sqlite.Error
		if !errors.As(err, &sqliteErr) {
			return err
		}
		if sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT {
			return err
		}
	}

	return nil
}

func (repo *Repository) GetPackfile(mac objects.MAC) (io.Reader, error) {
	var data []byte
	err := repo.conn.QueryRow(`SELECT data FROM packfiles WHERE mac=?`, mac[:]).Scan(&data)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(data), nil
}

func (repo *Repository) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	var data []byte
	err := repo.conn.QueryRow(`SELECT substr(data, ?, ?) FROM packfiles WHERE mac=?`, offset+1, length, mac[:]).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			err = repository.ErrBlobNotFound
		}
		return nil, err
	}
	return bytes.NewBuffer(data), nil
}

func (repo *Repository) DeletePackfile(mac objects.MAC) error {
	statement, err := repo.conn.Prepare(`DELETE FROM packfiles WHERE mac=?`)
	if err != nil {
		return err
	}
	defer statement.Close()

	repo.wrMutex.Lock()
	_, err = statement.Exec(mac[:])
	repo.wrMutex.Unlock()
	if err != nil {
		// if err is that it's already present, we should discard err and assume a concurrent write
		return err
	}
	return nil
}
