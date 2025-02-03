package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const VERSION = 100

type Type uint8

const (
	TYPE_SNAPSHOT  Type = 1
	TYPE_CHUNK     Type = 2
	TYPE_OBJECT    Type = 3
	TYPE_VFS       Type = 4
	TYPE_VFS_ENTRY Type = 5
	TYPE_CHILD     Type = 6
	TYPE_SIGNATURE Type = 7
	TYPE_ERROR     Type = 8
)

type Blob struct {
	Type     Type
	Checksum [32]byte
	Offset   uint32
	Length   uint32
}

func Types() []Type {
	return []Type{TYPE_SNAPSHOT, TYPE_CHUNK, TYPE_OBJECT, TYPE_VFS, TYPE_VFS_ENTRY, TYPE_CHILD, TYPE_SIGNATURE, TYPE_ERROR}
}

func (b Blob) TypeName() string {
	switch b.Type {
	case TYPE_SNAPSHOT:
		return "snapshot"
	case TYPE_CHUNK:
		return "chunk"
	case TYPE_OBJECT:
		return "object"
	case TYPE_VFS:
		return "vfs"
	case TYPE_VFS_ENTRY:
		return "vfs_entry"
	case TYPE_CHILD:
		return "directory"
	case TYPE_SIGNATURE:
		return "signature"
	case TYPE_ERROR:
		return "error"
	default:
		return "unknown"
	}
}

type PackFile struct {
	Blobs  []byte
	Index  []Blob
	Footer PackFileFooter
}

type PackFileFooter struct {
	Version       uint32
	Timestamp     int64
	Count         uint32
	IndexOffset   uint32
	IndexChecksum [32]byte
}

type Configuration struct {
	MaxSize uint32
}

func DefaultConfiguration() *Configuration {
	return &Configuration{
		MaxSize: (20 << 10) << 10,
	}
}

func NewFooterFromBytes(serialized []byte) (PackFileFooter, error) {

	reader := bytes.NewReader(serialized)
	var footer PackFileFooter
	if err := binary.Read(reader, binary.LittleEndian, &footer.Version); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexChecksum); err != nil {
		return footer, err
	}
	return footer, nil
}

func NewIndexFromBytes(serialized []byte) ([]Blob, error) {
	reader := bytes.NewReader(serialized)
	index := make([]Blob, 0)
	for reader.Len() > 0 {
		var dataType uint8
		var checksum [32]byte
		var chunkOffset uint32
		var chunkLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &dataType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkLength); err != nil {
			return nil, err
		}
		index = append(index, Blob{
			Type:     Type(dataType),
			Checksum: checksum,
			Offset:   chunkOffset,
			Length:   chunkLength,
		})
	}
	return index, nil
}

func New() *PackFile {
	return &PackFile{
		Blobs: make([]byte, 0),
		Index: make([]Blob, 0),
		Footer: PackFileFooter{
			Version:   VERSION,
			Timestamp: time.Now().UnixNano(),
			Count:     0,
		},
	}
}

func NewFromBytes(serialized []byte) (*PackFile, error) {
	reader := bytes.NewReader(serialized)
	var footer PackFileFooter
	_, err := reader.Seek(-52, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexChecksum); err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, footer.IndexOffset)
	if err := binary.Read(reader, binary.LittleEndian, &data); err != nil {
		return nil, err
	}

	// we won't read the totalLength again
	remaining := reader.Len() - 52

	p := New()
	p.Footer = footer
	p.Blobs = data
	hasher := sha256.New()
	for remaining > 0 {
		var dataType uint8
		var checksum [32]byte
		var chunkOffset uint32
		var chunkLength uint32
		if err := binary.Read(reader, binary.LittleEndian, &dataType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &checksum); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &chunkLength); err != nil {
			return nil, err
		}

		if chunkOffset+chunkLength > p.Footer.IndexOffset {
			return nil, fmt.Errorf("chunk offset + chunk length exceeds total length of packfile")
		}

		if err := binary.Write(hasher, binary.LittleEndian, dataType); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunkOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunkLength); err != nil {
			return nil, err
		}
		p.Index = append(p.Index, Blob{
			Type:     Type(dataType),
			Checksum: checksum,
			Offset:   chunkOffset,
			Length:   chunkLength,
		})
		remaining -= (len(checksum) + 9)
	}
	checksum := [32]byte(hasher.Sum(nil))
	if checksum != p.Footer.IndexChecksum {
		return nil, fmt.Errorf("index checksum mismatch")
	}

	return p, nil
}

func (p *PackFile) Serialize() ([]byte, error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, p.Blobs); err != nil {
		return nil, err
	}

	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	p.Footer.IndexChecksum = [32]byte(hasher.Sum(nil))

	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexChecksum); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeData() ([]byte, error) {
	var buffer bytes.Buffer
	if err := binary.Write(&buffer, binary.LittleEndian, p.Blobs); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeIndex() ([]byte, error) {
	var buffer bytes.Buffer
	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (p *PackFile) SerializeFooter() ([]byte, error) {
	var buffer bytes.Buffer
	hasher := sha256.New()
	for _, chunk := range p.Index {
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(&buffer, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}

		if err := binary.Write(hasher, binary.LittleEndian, chunk.Type); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Checksum); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(hasher, binary.LittleEndian, chunk.Length); err != nil {
			return nil, err
		}
	}
	p.Footer.IndexChecksum = [32]byte(hasher.Sum(nil))

	buffer.Reset()
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(&buffer, binary.LittleEndian, p.Footer.IndexChecksum); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func (p *PackFile) AddBlob(dataType Type, checksum [32]byte, data []byte) {
	p.Index = append(p.Index, Blob{dataType, checksum, uint32(len(p.Blobs)), uint32(len(data))})
	p.Blobs = append(p.Blobs, data...)
	p.Footer.Count++
	p.Footer.IndexOffset = uint32(len(p.Blobs))
}

func (p *PackFile) GetBlob(checksum [32]byte) ([]byte, bool) {
	for _, chunk := range p.Index {
		if chunk.Checksum == checksum {
			return p.Blobs[chunk.Offset : chunk.Offset+chunk.Length], true
		}
	}
	return nil, false
}

func (p *PackFile) Size() uint32 {
	return uint32(len(p.Blobs))
}
