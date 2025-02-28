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

package importer

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/PlakarKorp/plakar/objects"
)

type ScanResult struct {
	Record *ScanRecord
	Error  *ScanError
}

type ExtendedAttributes struct {
	Name  string
	Value []byte
}

type ScanRecord struct {
	Pathname           string
	Target             string
	FileInfo           objects.FileInfo
	ExtendedAttributes []string
	FileAttributes     uint32
	IsXattr            bool
	XattrName          string
	XattrType          objects.Attribute
}

type ScanError struct {
	Pathname string
	Err      error
}

type Importer interface {
	Origin() string
	Type() string
	Root() string
	Scan() (<-chan *ScanResult, error)
	NewReader(string) (io.ReadCloser, error)
	NewExtendedAttributeReader(string, string) (io.ReadCloser, error)
	GetExtendedAttributes(string) ([]ExtendedAttributes, error)
	Close() error
}

var muBackends sync.Mutex
var backends map[string]func(config map[string]string) (Importer, error) = make(map[string]func(config map[string]string) (Importer, error))

func Register(name string, backend func(map[string]string) (Importer, error)) {
	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; ok {
		log.Fatalf("backend '%s' registered twice", name)
	}
	backends[name] = backend
}

func Backends() []string {
	muBackends.Lock()
	defer muBackends.Unlock()

	ret := make([]string, 0)
	for backendName := range backends {
		ret = append(ret, backendName)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func NewImporter(config map[string]string) (Importer, error) {
	location, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	muBackends.Lock()
	defer muBackends.Unlock()

	var backendName string
	if !strings.HasPrefix(location, "/") {
		if strings.HasPrefix(location, "s3://") {
			backendName = "s3"
		} else if strings.HasPrefix(location, "fs://") {
			backendName = "fs"
		} else if strings.HasPrefix(location, "ftp://") {
			backendName = "ftp"
		} else if strings.HasPrefix(location, "sftp://") {
			backendName = "sftp"
		} else {
			if strings.Contains(location, "://") {
				return nil, fmt.Errorf("unsupported importer protocol")
			} else {
				backendName = "fs"
			}
		}
	} else {
		backendName = "fs"
	}

	if backend, exists := backends[backendName]; !exists {
		return nil, fmt.Errorf("backend '%s' does not exist", backendName)
	} else {
		backendInstance, err := backend(config)
		if err != nil {
			return nil, err
		}
		return backendInstance, nil
	}
}

func NewScanRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:           pathname,
			Target:             target,
			FileInfo:           fileinfo,
			ExtendedAttributes: xattr,
		},
	}
}

func NewScanXattr(pathname, xattr string, kind objects.Attribute) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:  pathname,
			IsXattr:   true,
			XattrName: xattr,
			XattrType: kind,
		},
	}
}

func NewScanError(pathname string, err error) *ScanResult {
	return &ScanResult{
		Error: &ScanError{
			Pathname: pathname,
			Err:      err,
		},
	}
}
