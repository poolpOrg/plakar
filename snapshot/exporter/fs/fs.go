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

package fs

import (
	"io"
	"os"
	"strings"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/exporter"
)

type FSExporter struct {
	rootDir string
}

func init() {
	exporter.Register("fs", NewFSExporter)
}

func NewFSExporter(config map[string]string) (exporter.Exporter, error) {
	location := config["location"]

	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	return &FSExporter{
		rootDir: location,
	}, nil
}

func (p *FSExporter) Begin(config string) error {
	if strings.HasPrefix(config, "fs://") {
		config = config[4:]
	}
	p.rootDir = config // duplicate for now, config might change later
	return nil
}

func (p *FSExporter) Root() string {
	return p.rootDir
}

func (p *FSExporter) CreateDirectory(pathname string) error {
	return os.MkdirAll(pathname, 0700)
}

func (p *FSExporter) StoreFile(pathname string, fp io.Reader) error {
	f, err := os.Create(pathname)
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

func (p *FSExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	if err := os.Chmod(pathname, fileinfo.Mode()); err != nil {
		return err
	}
	if os.Getuid() == 0 {
		if err := os.Chown(pathname, int(fileinfo.Uid()), int(fileinfo.Gid())); err != nil {
			return err
		}
	}
	return nil
}

func (p *FSExporter) Close() error {
	return nil
}
