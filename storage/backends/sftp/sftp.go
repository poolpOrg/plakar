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

package sftp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"os/user"
	"path"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

type Repository struct {
	location  string
	packfiles Buckets
	states    Buckets
	client    *sftp.Client
}

func init() {
	storage.Register("sftp", NewRepository)
}

func defaultSigners() ([]ssh.Signer, error) {
	var signers []ssh.Signer

	// Try the SSH agent first.
	if sock := os.Getenv("SSH_AUTH_SOCK"); sock != "" {
		conn, err := net.Dial("unix", sock)
		if err == nil {
			ag := agent.NewClient(conn)
			if s, err := ag.Signers(); err == nil {
				signers = append(signers, s...)
			}
		}
	}

	// Fallback: load from default key files.
	home, err := os.UserHomeDir()
	if err != nil {
		return signers, err
	}

	// List of default private key paths.
	keyFiles := []string{
		path.Join(home, ".ssh", "id_rsa"),
		path.Join(home, ".ssh", "id_dsa"),
		path.Join(home, ".ssh", "id_ecdsa"),
		path.Join(home, ".ssh", "id_ed25519"),
	}

	for _, file := range keyFiles {
		data, err := os.ReadFile(file)
		if err != nil {
			continue // Skip files that don't exist.
		}
		signer, err := ssh.ParsePrivateKey(data)
		if err != nil {
			continue // Skip unparseable keys.
		}
		signers = append(signers, signer)
	}

	return signers, nil
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
	if strings.HasPrefix(root, "sftp://") {
		root = root[7:]
	}
	atoms := strings.Split(root, "/")
	if len(atoms) == 0 {
		return "/"
	} else {
		root = "/" + strings.Join(atoms[1:], "/")
	}

	args = append(args, "")
	copy(args[1:], args)
	args[0] = root

	return path.Join(args...)
}

func connect(location string) (*sftp.Client, error) {
	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	var sshHost string
	if parsed.Port() == "" {
		sshHost = parsed.Host + ":22"
	} else {
		sshHost = parsed.Host
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %v", err)
	}
	knownHostsPath := path.Join(homeDir, ".ssh", "known_hosts")

	// Create the HostKeyCallback from the known_hosts file.
	hostKeyCallback, err := knownhosts.New(knownHostsPath)
	if err != nil {
		return nil, fmt.Errorf("could not create hostkeycallback function: %v", err)
	}

	signers, err := defaultSigners()
	if err != nil {
		return nil, err
	}

	username := parsed.User.Username()
	if username == "" {
		u, err := user.Current()
		if err != nil {
			return nil, err
		}
		username = u.Username
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signers...),
		},
		HostKeyCallback: hostKeyCallback,
	}

	client, err := ssh.Dial("tcp", sshHost, config)
	if err != nil {
		return nil, err
	}
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		client.Close()
		return nil, err
	}
	return sftpClient, nil
}

func (repo *Repository) Create(config []byte) error {
	client, err := connect(repo.location)
	if err != nil {
		return err
	}
	repo.client = client

	err = client.Mkdir(repo.Path())
	if err != nil {
		return err
	}
	err = client.Chmod(repo.Path(), 0700)
	if err != nil {
		return err
	}

	repo.packfiles = NewBuckets(client, repo.Path("packfiles"))
	if err := repo.packfiles.Create(); err != nil {
		return err
	}

	repo.states = NewBuckets(client, repo.Path("states"))
	if err := repo.states.Create(); err != nil {
		return err
	}

	return WriteToFileAtomic(client, repo.Path("CONFIG"), bytes.NewReader(config))
}

func (repo *Repository) Open() ([]byte, error) {
	client, err := connect(repo.location)
	if err != nil {
		return nil, err
	}
	repo.client = client

	rd, err := client.Open(repo.Path("CONFIG"))
	if err != nil {
		return nil, err
	}
	defer rd.Close() // do we care about err?

	data, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	repo.packfiles = NewBuckets(client, repo.Path("packfiles"))

	repo.states = NewBuckets(client, repo.Path("states"))

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
