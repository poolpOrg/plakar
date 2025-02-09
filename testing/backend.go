package testing

import (
	"bytes"
	"errors"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/compression"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	storage.Register("fs", func(location string) storage.Store { return &MockBackend{location: location} })
}

type mockedBackendBehavior struct {
	statesChecksums    []objects.Checksum
	header             any
	packfilesChecksums []objects.Checksum
	packfile           string
}

var behaviors = map[string]mockedBackendBehavior{
	"default": {
		statesChecksums:    nil,
		header:             "blob data",
		packfilesChecksums: nil,
		packfile:           `{"test": "data"}`,
	},
	"oneState": {
		statesChecksums:    []objects.Checksum{{0x01}, {0x02}, {0x03}, {0x04}},
		header:             header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}, Sources: []header.Source{{VFS: objects.Checksum{0x01}}}},
		packfilesChecksums: []objects.Checksum{{0x04}, {0x05}, {0x06}},
	},
	"oneSnapshot": {
		statesChecksums:    []objects.Checksum{{0x01}, {0x02}, {0x03}},
		header:             header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}},
		packfilesChecksums: []objects.Checksum{{0x01}, {0x04}, {0x05}, {0x06}},
	},
	"brokenState": {
		statesChecksums:    nil,
		header:             nil,
		packfilesChecksums: nil,
	},
	"brokenGetState": {
		statesChecksums:    nil,
		header:             nil,
		packfilesChecksums: nil,
	},
	"nopackfile": {
		statesChecksums:    []objects.Checksum{{0x01}, {0x02}, {0x03}},
		header:             nil,
		packfilesChecksums: nil,
	},
}

// MockBackend implements the Backend interface for testing purposes
type MockBackend struct {
	configuration storage.Configuration
	location      string

	// used to trigger different behaviors during tests
	behavior string
}

func NewMockBackend(location string) *MockBackend {
	return &MockBackend{location: location}
}

func (mb *MockBackend) Create(repository string, configuration storage.Configuration) error {
	if strings.Contains(repository, "musterror") {
		return errors.New("creating error")
	}
	mb.configuration = configuration

	mb.behavior = "default"

	u, err := url.Parse(repository)
	if err != nil {
		return err
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	if m.Get("behavior") != "" {
		mb.behavior = m.Get("behavior")
	}
	return nil
}

func (mb *MockBackend) Open(repository string) error {
	if strings.Contains(repository, "musterror") {
		return errors.New("opening error")
	}
	return nil
}

func (mb *MockBackend) Configuration() storage.Configuration {
	return mb.configuration
}

func (mb *MockBackend) Location() string {
	return mb.location
}

func (mb *MockBackend) GetStates() ([]objects.Checksum, error) {
	ret := make([]objects.Checksum, 0)
	if mb.behavior == "brokenState" {
		return ret, errors.New("broken state")
	}
	return behaviors[mb.behavior].statesChecksums, nil
}

func (mb *MockBackend) PutState(checksum objects.Checksum, rd io.Reader) error {
	return nil
}

func (mb *MockBackend) GetState(checksum objects.Checksum) (io.Reader, error) {
	if mb.behavior == "brokenGetState" {
		return nil, errors.New("broken get state")
	}

	var buffer bytes.Buffer
	if mb.configuration.Compression != nil {
		return compression.DeflateStream(mb.configuration.Compression.Algorithm, &buffer)
	}
	return &buffer, nil
}

func (mb *MockBackend) DeleteState(checksum objects.Checksum) error {
	return nil
}

func (mb *MockBackend) GetPackfiles() ([]objects.Checksum, error) {
	if mb.behavior == "brokenGetPackfiles" {
		return nil, errors.New("broken get packfiles")
	}

	packfiles := behaviors[mb.behavior].packfilesChecksums
	return packfiles, nil
}

func (mb *MockBackend) PutPackfile(checksum objects.Checksum, rd io.Reader) error {
	return nil
}

func (mb *MockBackend) GetPackfile(checksum objects.Checksum) (io.Reader, error) {
	if mb.behavior == "brokenGetPackfile" {
		return nil, errors.New("broken get packfile")
	}

	packfile := behaviors[mb.behavior].packfile
	if packfile == "" {
		return bytes.NewReader([]byte("packfile data")), nil
	}

	if mb.configuration.Compression != nil {
		return compression.DeflateStream(mb.configuration.Compression.Algorithm, bytes.NewReader([]byte(packfile)))
	} else {
		return bytes.NewReader([]byte(packfile)), nil
	}
}

func (mb *MockBackend) GetPackfileBlob(checksum objects.Checksum, offset uint64, length uint32) (io.Reader, error) {
	if mb.behavior == "brokenGetPackfileBlob" {
		return nil, errors.New("broken get packfile blob")
	}

	header := behaviors[mb.behavior].header
	if header == nil {
		return bytes.NewReader([]byte("blob data")), nil
	}
	data, err := msgpack.Marshal(header)
	if err != nil {
		panic(err)
	}
	if mb.configuration.Compression != nil {
		return compression.DeflateStream(mb.configuration.Compression.Algorithm, bytes.NewReader(data))
	} else {
		return bytes.NewReader(data), nil
	}
}

func (mb *MockBackend) DeletePackfile(checksum objects.Checksum) error {
	return nil
}

func (mb *MockBackend) Close() error {
	return nil
}
