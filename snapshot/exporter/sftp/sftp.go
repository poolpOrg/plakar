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

package sftp

import (
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/user"
	"path"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

type SFTPExporter struct {
	location string
	client   *sftp.Client
}

func init() {
	exporter.Register("sftp", NewSFTPExporter)
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

func connect(endpoint *url.URL) (*sftp.Client, error) {

	var sshHost string
	if endpoint.Port() == "" {
		sshHost = endpoint.Host + ":22"
	} else {
		sshHost = endpoint.Host
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

	username := endpoint.User.Username()
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

func NewSFTPExporter(config map[string]string) (exporter.Exporter, error) {
	location := config["location"]

	parsed, err := url.Parse(location)
	if err != nil {
		return nil, err
	}

	client, err := connect(parsed)
	if err != nil {
		return nil, err
	}
	return &SFTPExporter{
		location: parsed.Path,
		client:   client,
	}, nil
}

func (p *SFTPExporter) Root() string {

	return p.location
}

func (p *SFTPExporter) CreateDirectory(pathname string) error {
	return p.client.MkdirAll(pathname)
}

func (p *SFTPExporter) StoreFile(pathname string, fp io.Reader) error {
	f, err := p.client.Create(pathname)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, fp); err != nil {
		//logging.Warn("copy failure: %s: %s", pathname, err)
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		//logging.Warn("sync failure: %s: %s", pathname, err)
	}
	if err := f.Close(); err != nil {
		//logging.Warn("close failure: %s: %s", pathname, err)
	}
	return nil
}

func (p *SFTPExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	if err := p.client.Chmod(pathname, fileinfo.Mode()); err != nil {
		return err
	}
	if os.Getuid() == 0 {
		if err := p.client.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
			return err
		}
	}
	return nil
}

func (p *SFTPExporter) Close() error {
	return p.client.Close()
}
