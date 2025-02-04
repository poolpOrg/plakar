package repository

import (
	"errors"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/vmihailenco/msgpack/v5"
)

var ErrStoreReadOnly = errors.New("read only store")

type RepositoryStore[K, V any] struct {
	repo     *Repository
	blobtype resources.Type
}

func NewRepositoryStore[K, V any](repo *Repository, blobtype resources.Type) *RepositoryStore[K, V] {
	return &RepositoryStore[K, V]{
		repo:     repo,
		blobtype: blobtype,
	}
}

func (rs *RepositoryStore[K, V]) Get(sum objects.Checksum) (*btree.Node[K, objects.Checksum, V], error) {
	rd, err := rs.repo.GetBlob(rs.blobtype, sum)
	if err != nil {
		return nil, err
	}
	node := &btree.Node[K, objects.Checksum, V]{}
	_ = msgpack.NewDecoder(rd).Decode(node)
	return node, nil
}

func (rs *RepositoryStore[K, V]) Update(sum objects.Checksum, node *btree.Node[K, objects.Checksum, V]) error {
	return ErrStoreReadOnly
}

func (rs *RepositoryStore[K, V]) Put(node *btree.Node[K, objects.Checksum, V]) (csum objects.Checksum, err error) {
	return csum, ErrStoreReadOnly
}
