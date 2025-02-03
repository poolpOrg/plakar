package objects

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

type Checksum [32]byte

func (m Checksum) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%0x", m[:]))
}

func (m *Checksum) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	decoded, err := hex.DecodeString(s)
	if err != nil {
		return err
	}

	if len(decoded) != 32 {
		return fmt.Errorf("invalid checksum length: %d", len(decoded))
	}

	copy(m[:], decoded)
	return nil
}

type Classification struct {
	Analyzer string   `msgpack:"analyzer" json:"analyzer"`
	Classes  []string `msgpack:"classes" json:"classes"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key" json:"key"`
	Value []byte `msgpack:"value" json:"value"`
}

const OBJECT_VERSION = "1.0.0"

type Object struct {
	Version     versioning.Version `msgpack:"version" json:"version"`
	Checksum    Checksum           `msgpack:"checksum" json:"checksum"`
	Chunks      []Chunk            `msgpack:"chunks" json:"chunks"`
	ContentType string             `msgpack:"content_type,omitempty" json:"content_type"`
	Entropy     float64            `msgpack:"entropy,omitempty" json:"entropy"`
	Flags       uint64             `msgpack:"flags" json:"flags"`
}

// Return empty lists for nil slices.
func (o *Object) MarshalJSON() ([]byte, error) {
	// Create an alias to avoid recursive MarshalJSON calls
	type Alias Object

	ret := (*Alias)(o)

	if ret.Chunks == nil {
		ret.Chunks = []Chunk{}
	}
	return json.Marshal(ret)
}

func NewObject() *Object {
	return &Object{
		Version: versioning.FromString(OBJECT_VERSION),
	}
}

func NewObjectFromBytes(serialized []byte) (*Object, error) {
	var o Object
	if err := msgpack.Unmarshal(serialized, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (o *Object) Serialize() ([]byte, error) {
	serialized, err := msgpack.Marshal(o)
	if err != nil {
		return nil, err
	}
	return serialized, nil
}

const CHUNK_VERSION = "1.0.0"

type Chunk struct {
	Version  versioning.Version `msgpack:"version" json:"version"`
	Checksum Checksum           `msgpack:"checksum" json:"checksum"`
	Length   uint32             `msgpack:"length" json:"length"`
	Entropy  float64            `msgpack:"entropy" json:"entropy"`
	Flags    uint64             `msgpack:"flags" json:"flags"`
}
