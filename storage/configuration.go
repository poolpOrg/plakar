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

package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/PlakarKorp/plakar/compression"
)

func newConfigurationFromBytes(buffer []byte, format string) (*Configuration, error) {

	if format == "auto" {
		// autodetect
		formats := []string{"msgpack", "json", "json+gz"}
		for _, format := range formats {
			config, err := newConfigurationFromBytes(buffer, format)
			if err == nil {
				return config, nil
			}
		}
		return nil, fmt.Errorf("Could not guess configuration format")
	}

	var err error
	var unmarshal func(data []byte, v interface{}) error
	gzip := false

	switch format {
	case "msgpack":
		gzip = true
		unmarshal = msgpack.Unmarshal
	case "json":
		unmarshal = json.Unmarshal
	case "json+gz":
		gzip = true
		unmarshal = json.Unmarshal
	default:
		return nil, fmt.Errorf("Unknown format '%s'", format)
	}

	if gzip {
		rd, err := compression.InflateStream("GZIP", bytes.NewReader(buffer))
		if err != nil {
			return nil, err
		}
		buffer, err = io.ReadAll(rd)
		if err != nil {
			return nil, err
		}
	}

	config := &Configuration{}
	err = unmarshal(buffer, &config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (config *Configuration) Formatter(format string) (io.Reader, error) {
	var rd io.Reader
	var data []byte
	var err error
	gzip := false

	switch format {
	case "msgpack":
		data, err = msgpack.Marshal(config)
		gzip = true
	case "json":
		data, err = json.Marshal(config)
	case "json+gz":
		data, err = json.Marshal(config)
		gzip = true
	default:
		return nil, fmt.Errorf("unknown format '%s'", format)
	}
	if err != nil {
		return nil, err
	}

	rd = bytes.NewReader(data)
	if gzip {
		rd, err = compression.DeflateStream("GZIP", rd)
		if err != nil {
			return nil, err
		}
	}

	return rd, nil
}

func (config *Configuration) Format(format string) ([]byte, error) {
	rd, err := config.Formatter(format)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func (config *Configuration) InitFromReader(rd io.Reader, format string) error {

	buffer, err := io.ReadAll(rd)
	if err != nil {
		return err
	}

	return config.InitFromBytes(buffer, format)
}

func (config *Configuration) InitFromBytes(buffer []byte, format string) error {

	newConfig, err := newConfigurationFromBytes(buffer, format)
	if err != nil {
		return err
	}

	*config = *newConfig

	return nil
}
