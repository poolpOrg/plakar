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
	"github.com/vmihailenco/msgpack/v5"
)

type ScanResult interface {
	scanResult()
}

type RecordType int8

const (
	RecordTypeFile      RecordType = 0
	RecordTypeDirectory RecordType = 1
	RecordTypeSymlink   RecordType = 2
	RecordTypeDevice    RecordType = 3
	RecordTypePipe      RecordType = 4
	RecordTypeSocket    RecordType = 5
)

type ScanRecord struct {
	Type               RecordType
	Pathname           string
	Target             string
	FileInfo           objects.FileInfo
	ExtendedAttributes map[string][]byte
	FileAttributes     uint32
}

func (r ScanRecord) scanResult() {}
func (r ScanRecord) ToBytes() ([]byte, error) {
	return msgpack.Marshal(r)
}

type ScanError struct {
	Pathname string
	Err      error
}

func (r ScanError) scanResult() {}

type Importer interface {
	Origin() string
	Type() string
	Root() string
	Scan() (<-chan ScanResult, error)
	NewReader(string) (io.ReadCloser, error)
	Close() error
}

var muBackends sync.Mutex
var backends map[string]func(config string) (Importer, error) = make(map[string]func(config string) (Importer, error))

func Register(name string, backend func(string) (Importer, error)) {
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

func NewImporter(location string) (Importer, error) {
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
		} else if strings.HasPrefix(location, "rclone://") {
			backendName = "rclone"
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
		backendInstance, err := backend(location)
		if err != nil {
			return nil, err
		}
		return backendInstance, nil
	}
}
