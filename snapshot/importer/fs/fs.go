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
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/PlakarKorp/plakar/snapshot/importer"
	"github.com/pkg/xattr"
)

type FSImporter struct {
	rootDir string
}

func init() {
	importer.Register("fs", NewFSImporter)
}

func NewFSImporter(config map[string]string) (importer.Importer, error) {
	var err error

	location := config["location"]

	if strings.HasPrefix(location, "fs://") {
		location = location[4:]
	}

	location, err = filepath.Abs(location)
	if err != nil {
		return nil, err
	}

	return &FSImporter{
		rootDir: location,
	}, nil
}

func (p *FSImporter) Origin() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	return hostname
}

func (p *FSImporter) Type() string {
	return "fs"
}

func (p *FSImporter) Scan() (<-chan *importer.ScanResult, error) {
	return walkDir_walker(p.rootDir, 256)
}

func (p *FSImporter) NewReader(pathname string) (io.ReadCloser, error) {
	if pathname[0] == '/' && runtime.GOOS == "windows" {
		pathname = pathname[1:]
	}
	return os.Open(pathname)
}

func (p *FSImporter) NewExtendedAttributeReader(pathname string, attribute string) (io.ReadCloser, error) {
	if pathname[0] == '/' && runtime.GOOS == "windows" {
		pathname = pathname[1:]
	}

	data, err := xattr.Get(pathname, attribute)
	if err != nil {
		return nil, err
	}
	return ioutil.NopCloser(bytes.NewReader(data)), nil
}

func (p *FSImporter) GetExtendedAttributes(pathname string) ([]importer.ExtendedAttributes, error) {
	if pathname[0] == '/' && runtime.GOOS == "windows" {
		pathname = pathname[1:]
	}

	return getExtendedAttributes(pathname)
}

func (p *FSImporter) Close() error {
	return nil
}

func (p *FSImporter) Root() string {
	return p.rootDir
}
