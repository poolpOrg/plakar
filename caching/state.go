package caching

import (
	"iter"

	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/packfile"
)

type StateCache interface {
	PutState(stateID objects.Checksum, data []byte) error
	HasState(stateID objects.Checksum) (bool, error)
	GetState(stateID objects.Checksum) ([]byte, error)
	DelState(stateID objects.Checksum) error
	GetStates() (map[objects.Checksum][]byte, error)

	PutDelta(blobType packfile.Type, blobCsum objects.Checksum, data []byte) error
	GetDelta(blobType packfile.Type, blobCsum objects.Checksum) ([]byte, error)
	HasDelta(blobType packfile.Type, blobCsum objects.Checksum) (bool, error)
	GetDeltaByCsum(blobCsum objects.Checksum) ([]byte, error)
	GetDeltasByType(blobType packfile.Type) iter.Seq2[objects.Checksum, []byte]
	GetDeltas() iter.Seq2[objects.Checksum, []byte]
}
