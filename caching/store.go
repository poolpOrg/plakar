package caching

import (
	"fmt"

	"github.com/PlakarKorp/plakar/btree"
	"github.com/vmihailenco/msgpack/v5"
)

// DBStore implements btree.Storer
type DBStore[K any, V any] struct {
	Prefix string
	idx    int
	Cache  *ScanCache
}

func (ds *DBStore[K, V]) Get(idx int) (*btree.Node[K, int, V], error) {
	bytes, err := ds.Cache.get(ds.Prefix, fmt.Sprint(idx))
	if err != nil {
		return nil, err
	}
	node := &btree.Node[K, int, V]{}
	err = msgpack.Unmarshal(bytes, node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (ds *DBStore[K, V]) Update(idx int, node *btree.Node[K, int, V]) error {
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return err
	}
	return ds.Cache.put(ds.Prefix, fmt.Sprint(idx), bytes)
}

func (ds *DBStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	ds.idx++
	idx := ds.idx
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}
	return idx, ds.Cache.put(ds.Prefix, fmt.Sprint(idx), bytes)
}
