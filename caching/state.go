package caching

import (
	"iter"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
)

type StateCache interface {
	PutState(stateID objects.Checksum, data []byte) error
	HasState(stateID objects.Checksum) (bool, error)
	GetState(stateID objects.Checksum) ([]byte, error)
	DelState(stateID objects.Checksum) error
	GetStates() (map[objects.Checksum][]byte, error)

	PutDelta(blobType resources.Type, blobCsum objects.Checksum, data []byte) error
	GetDelta(blobType resources.Type, blobCsum objects.Checksum) ([]byte, error)
	HasDelta(blobType resources.Type, blobCsum objects.Checksum) (bool, error)
	GetDeltaByCsum(blobCsum objects.Checksum) ([]byte, error)
	GetDeltasByType(blobType resources.Type) iter.Seq2[objects.Checksum, []byte]
	GetDeltas() iter.Seq2[objects.Checksum, []byte]

	PutDeleted(blobType resources.Type, blobCsum objects.Checksum, data []byte) error
	HasDeleted(blobType resources.Type, blobCsum objects.Checksum) (bool, error)
	GetDeleteds() iter.Seq2[objects.Checksum, []byte]

	PutPackfile(stateID objects.Checksum, packfile objects.Checksum, data []byte) error
	GetPackfiles() iter.Seq2[objects.Checksum, []byte]
	GetPackfilesForState(stateID objects.Checksum) iter.Seq2[objects.Checksum, []byte]
}
