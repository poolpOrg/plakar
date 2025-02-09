package objects

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const OBJECT_VERSION = "1.0.0"
const CHUNK_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_OBJECT, versioning.FromString(OBJECT_VERSION))
	versioning.Register(resources.RT_CHUNK, versioning.FromString(CHUNK_VERSION))
}

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
	return msgpack.Marshal(o)
}

type Chunk struct {
	Version  versioning.Version `msgpack:"version" json:"version"`
	Checksum Checksum           `msgpack:"checksum" json:"checksum"`
	Length   uint32             `msgpack:"length" json:"length"`
	Entropy  float64            `msgpack:"entropy" json:"entropy"`
	Flags    uint64             `msgpack:"flags" json:"flags"`
}

func NewChunk() *Chunk {
	return &Chunk{
		Version: versioning.FromString(CHUNK_VERSION),
	}
}

func NewChunkFromBytes(serialized []byte) (*Chunk, error) {
	var c Chunk
	if err := msgpack.Unmarshal(serialized, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Chunk) Serialize() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *Chunk) MarshalJSON() ([]byte, error) {
	// Create an alias to avoid recursive MarshalJSON calls
	type Alias Chunk
	return json.Marshal((*Alias)(c))
}
