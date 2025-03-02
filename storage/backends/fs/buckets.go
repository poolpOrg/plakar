/*
 * Copyright (c) 2025 Eric Faurot <eric@faurot.net>
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
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/PlakarKorp/plakar/objects"
)

type Buckets struct {
	path string
}

func NewBuckets(path string) Buckets {
	return Buckets{
		path: path,
	}
}

func (buckets *Buckets) Create() error {

	for i := 0; i < 256; i++ {
		err := os.MkdirAll(filepath.Join(buckets.path, fmt.Sprintf("%02x", i)), 0700)
		if err != nil {
			return err
		}
	}

	return nil
}

func (buckets *Buckets) List() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)

	bucketsDir, err := os.ReadDir(buckets.path)
	if err != nil {
		return ret, err
	}

	for _, bucket := range bucketsDir {
		if bucket.Name() == "." || bucket.Name() == ".." {
			continue
		}
		if !bucket.IsDir() {
			continue
		}
		path := filepath.Join(buckets.path, bucket.Name())
		entries, err := os.ReadDir(path)
		if err != nil {
			return ret, err
		}
		for _, entry := range entries {
			if entry.Name() == "." || entry.Name() == ".." {
				continue
			}
			if entry.IsDir() {
				continue
			}
			t, err := hex.DecodeString(entry.Name())
			if err != nil {
				return nil, err
			}
			if len(t) != 32 {
				continue
			}
			var t32 objects.MAC
			copy(t32[:], t)
			ret = append(ret, t32)
		}
	}
	return ret, nil
}

func (buckets *Buckets) Path(mac objects.MAC) string {
	return filepath.Join(buckets.path,
		fmt.Sprintf("%02x", mac[0]),
		fmt.Sprintf("%064x", mac))
}

func (buckets *Buckets) Get(mac objects.MAC) (io.Reader, error) {
	return os.Open(buckets.Path(mac))
}

func (buckets *Buckets) GetBlob(mac objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	fp, err := os.Open(buckets.Path(mac))
	if err != nil {
		return nil, err
	}
	return ClosingLimitedReaderFromOffset(fp, int64(offset), int64(length))
}

func (buckets *Buckets) Remove(mac objects.MAC) error {
	return os.Remove(buckets.Path(mac))
}

func (buckets *Buckets) Put(mac objects.MAC, rd io.Reader) error {
	return WriteToFileAtomicTempDir(buckets.Path(mac), rd, buckets.path)
}
