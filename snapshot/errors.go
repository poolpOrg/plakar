package snapshot

import (
	"iter"
	"strings"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/PlakarKorp/plakar/objects"
	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const ERROR_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_ERROR, versioning.FromString(ERROR_VERSION))
}

type ErrorItem struct {
	Version versioning.Version `msgpack:"version" json:"version"`
	Name    string             `msgpack:"name" json:"name"`
	Error   string             `msgpack:"error" json:"error"`
}

func (e *ErrorItem) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func ErrorItemFromBytes(bytes []byte) (*ErrorItem, error) {
	e := &ErrorItem{}
	err := msgpack.Unmarshal(bytes, e)
	return e, err
}

func (snapshot *Snapshot) erroridx() (*btree.BTree[string, objects.Checksum, objects.Checksum], error) {
	rd, err := snapshot.repository.GetBlob(resources.RT_BTREE, snapshot.Header.GetSource(0).Errors)
	if err != nil {
		return nil, err
	}

	storage := SnapshotStore[string, objects.Checksum]{
		blobtype: resources.RT_BTREE,
		snap:     snapshot,
	}
	return btree.Deserialize(rd, &storage, strings.Compare)
}

func (snapshot *Snapshot) Errors(beneath string) (iter.Seq2[*ErrorItem, error], error) {
	if !strings.HasSuffix(beneath, "/") {
		beneath += "/"
	}

	tree, err := snapshot.erroridx()
	if err != nil {
		return nil, err
	}

	return func(yield func(*ErrorItem, error) bool) {
		iter, err := tree.ScanFrom(beneath)
		if err != nil {
			yield(&ErrorItem{}, err)
			return
		}

		for iter.Next() {
			_, csum := iter.Current()

			bytes, err := snapshot.GetBlob(resources.RT_ERROR, csum)
			if err != nil {
				yield(&ErrorItem{}, err)
				return
			}

			item, err := ErrorItemFromBytes(bytes)
			if err != nil {
				yield(&ErrorItem{}, err)
				return
			}

			if !strings.HasPrefix(item.Name, beneath) {
				break
			}
			if !yield(item, nil) {
				break
			}
		}
		if err := iter.Err(); err != nil {
			yield(&ErrorItem{}, err)
			return
		}
	}, nil
}

func (snapshot *Snapshot) IterErrorNodes() (btree.Iterator[objects.Checksum, *btree.Node[string, objects.Checksum, objects.Checksum]], error) {
	tree, err := snapshot.erroridx()
	if err != nil {
		return nil, err
	}

	return tree.IterDFS(), nil
}
