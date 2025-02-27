package testing

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/snapshot/header"
	"github.com/PlakarKorp/plakar/storage"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	storage.Register("fs", func(storeConfig map[string]string) (storage.Store, error) {
		return &MockBackend{location: storeConfig["location"]}, nil
	})
}

type mockedBackendBehavior struct {
	statesMACs    []objects.MAC
	header        any
	packfilesMACs []objects.MAC
	packfile      string
}

var behaviors = map[string]mockedBackendBehavior{
	"default": {
		statesMACs:    nil,
		header:        "blob data",
		packfilesMACs: nil,
		packfile:      `{"test": "data"}`,
	},
	"oneState": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}, {0x04}},
		header:        header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}, Sources: []header.Source{{VFS: header.VFS{Root: objects.MAC{0x01}}}}},
		packfilesMACs: []objects.MAC{{0x04}, {0x05}, {0x06}},
	},
	"oneSnapshot": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}},
		header:        header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}},
		packfilesMACs: []objects.MAC{{0x01}, {0x04}, {0x05}, {0x06}},
	},
	"brokenState": {
		statesMACs:    nil,
		header:        nil,
		packfilesMACs: nil,
	},
	"brokenGetState": {
		statesMACs:    nil,
		header:        nil,
		packfilesMACs: nil,
	},
	"nopackfile": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}},
		header:        nil,
		packfilesMACs: nil,
	},
}

// MockBackend implements the Backend interface for testing purposes
type MockBackend struct {
	configuration []byte
	location      string

	// used to trigger different behaviors during tests
	behavior string
}

func NewMockBackend(storeConfig map[string]string) *MockBackend {
	return &MockBackend{location: storeConfig["location"]}
}

func (mb *MockBackend) Create(configuration []byte) error {
	if strings.Contains(mb.location, "musterror") {
		return errors.New("creating error")
	}
	mb.configuration = configuration
	fmt.Println("CONFIG", mb.configuration)

	mb.behavior = "default"

	u, err := url.Parse(mb.location)
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

func (mb *MockBackend) Open() ([]byte, error) {
	if strings.Contains(mb.location, "musterror") {
		return nil, errors.New("opening error")
	}
	fmt.Println("CONFIG", mb.configuration)
	return mb.configuration, nil
}

func (mb *MockBackend) Location() string {
	return mb.location
}

func (mb *MockBackend) GetStates() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	if mb.behavior == "brokenState" {
		return ret, errors.New("broken state")
	}
	return behaviors[mb.behavior].statesMACs, nil
}

func (mb *MockBackend) PutState(MAC objects.MAC, rd io.Reader) error {
	return nil
}

func (mb *MockBackend) GetState(MAC objects.MAC) (io.Reader, error) {
	if mb.behavior == "brokenGetState" {
		return nil, errors.New("broken get state")
	}

	var buffer bytes.Buffer
	return &buffer, nil
}

func (mb *MockBackend) DeleteState(MAC objects.MAC) error {
	return nil
}

func (mb *MockBackend) GetPackfiles() ([]objects.MAC, error) {
	if mb.behavior == "brokenGetPackfiles" {
		return nil, errors.New("broken get packfiles")
	}

	packfiles := behaviors[mb.behavior].packfilesMACs
	return packfiles, nil
}

func (mb *MockBackend) PutPackfile(MAC objects.MAC, rd io.Reader) error {
	return nil
}

func (mb *MockBackend) GetPackfile(MAC objects.MAC) (io.Reader, error) {
	if mb.behavior == "brokenGetPackfile" {
		return nil, errors.New("broken get packfile")
	}

	packfile := behaviors[mb.behavior].packfile
	if packfile == "" {
		return bytes.NewReader([]byte("packfile data")), nil
	}

	return bytes.NewReader([]byte(packfile)), nil
}

func (mb *MockBackend) GetPackfileBlob(MAC objects.MAC, offset uint64, length uint32) (io.Reader, error) {
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
	return bytes.NewReader(data), nil
}

func (mb *MockBackend) DeletePackfile(MAC objects.MAC) error {
	return nil
}

func (mb *MockBackend) Close() error {
	return nil
}
