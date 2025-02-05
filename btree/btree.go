package btree

import (
	"errors"
	"io"
	"slices"

	"github.com/PlakarKorp/plakar/resources"
	"github.com/PlakarKorp/plakar/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	ErrExists = errors.New("Item already exists")
)

const BTREE_VERSION = "1.0.0"
const BTREE_NODE_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_INDEX, versioning.FromString(BTREE_VERSION))
	versioning.Register(resources.RT_INDEX_ENTRY, versioning.FromString(BTREE_NODE_VERSION))
}

type Storer[K any, P any, V any] interface {
	// Get returns the node pointed by P.  The pointer is one
	// previously returned by the Put method.
	Get(P) (*Node[K, P, V], error)
	// Updates in-place the node pointed by P.
	Update(P, *Node[K, P, V]) error
	// Put saves a new node and returns its address, or an error.
	Put(*Node[K, P, V]) (P, error)
}

type Node[K any, P any, V any] struct {
	Version versioning.Version `msgpack:"version"`

	// An intermediate node has only Keys and Pointers, while
	// leaves have only keys and values and optionally a next
	// pointer.
	//
	// invariant: len(Pointers) == len(Keys) + 1 in intermediate nodes
	// invariant: len(Values)   == len(Keys)     in leaf nodes
	Keys     []K `msgpack:"keys"`
	Pointers []P `msgpack:"pointers"`
	Values   []V `msgpack:"values"`
	Prev     *P  `msgpack:"prev,omitempty"` // unused for now
	Next     *P  `msgpack:"next,omitempty"`
}

// BTree implements a B+tree.  K is the type for the key, V for the
// value stored, and P is a pointer type: it could be a disk sector,
// a checksum in a packfile, or a key in a leveldb cache.  or more.
type BTree[K any, P any, V any] struct {
	Version versioning.Version
	Order   int
	Count   int
	Root    P
	store   Storer[K, P, V]
	compare func(K, K) int
}

// New returns a new, empty tree.
func New[K any, P any, V any](store Storer[K, P, V], compare func(K, K) int, order int) (*BTree[K, P, V], error) {
	root := Node[K, P, V]{}
	ptr, err := store.Put(&root)
	if err != nil {
		return nil, err
	}

	return &BTree[K, P, V]{
		Order:   order,
		Root:    ptr,
		store:   store,
		compare: compare,
	}, nil
}

// FromStorage returns a btree from the given storage.  The root must
// exist, eventually empty, i.e. it should be a tree previously
// created via New().
func FromStorage[K any, P any, V any](root P, store Storer[K, P, V], compare func(K, K) int, order int) *BTree[K, P, V] {
	return &BTree[K, P, V]{
		Version: versioning.FromString(BTREE_VERSION),
		Order:   order,
		Root:    root,
		store:   store,
		compare: compare,
	}
}

func Deserialize[K, P, V any](rd io.Reader, store Storer[K, P, V], compare func(K, K) int) (*BTree[K, P, V], error) {
	var root BTree[K, P, V]
	if err := msgpack.NewDecoder(rd).Decode(&root); err != nil {
		return nil, err
	}
	return FromStorage(root.Root, store, compare, root.Order), nil
}

func newNodeFrom[K, P, V any](keys []K, pointers []P, values []V) *Node[K, P, V] {
	node := &Node[K, P, V]{
		Version:  versioning.FromString(BTREE_NODE_VERSION),
		Keys:     make([]K, len(keys)),
		Pointers: make([]P, len(pointers)),
		Values:   make([]V, len(values)),
	}
	copy(node.Keys, keys)
	copy(node.Pointers, pointers)
	copy(node.Values, values)
	return node
}

func (n *Node[K, P, V]) isleaf() bool {
	return len(n.Pointers) == 0
}

func (b *BTree[K, P, V]) findleaf(key K) (node *Node[K, P, V], path []P, err error) {
	ptr := b.Root

	for {
		path = append(path, ptr)
		node, err = b.store.Get(ptr)
		if err != nil {
			return
		}

		if node.isleaf() {
			return
		}

		idx, found := slices.BinarySearchFunc(node.Keys, key, b.compare)
		if found {
			idx++
		}
		if idx < len(node.Keys) {
			ptr = node.Pointers[idx]
		} else {
			ptr = node.Pointers[len(node.Keys)]
		}
	}
}

func (b *BTree[K, P, V]) Find(key K) (val V, found bool, err error) {
	leaf, _, err := b.findleaf(key)
	if err != nil {
		return
	}

	val, found = leaf.find(key, b.compare)
	return val, found, nil
}

func (n *Node[K, P, V]) find(key K, cmp func(K, K) int) (val V, found bool) {
	idx, found := slices.BinarySearchFunc(n.Keys, key, cmp)
	if found {
		return n.Values[idx], true
	}
	return val, false
}

func (n *Node[K, P, V]) insertAt(idx int, key K, val V) {
	n.Keys = slices.Insert(n.Keys, idx, key)
	n.Values = slices.Insert(n.Values, idx, val)
}

func (n *Node[K, P, V]) insertInternal(idx int, key K, ptr P) {
	// Pointers and Keys have different cardinalities, but to
	// decide whether to append or insert in Pointers we need
	// to consider the length of the keys.
	if idx >= len(n.Keys) {
		n.Keys = append(n.Keys, key)
		n.Pointers = append(n.Pointers, ptr)
		return
	}

	n.Keys = slices.Insert(n.Keys, idx, key)
	n.Pointers = slices.Insert(n.Pointers, idx+1, ptr)
}

func (b *BTree[K, P, V]) findsplit(key K, node *Node[K, P, V]) (int, bool) {
	return slices.BinarySearchFunc(node.Keys, key, b.compare)
}

func (n *Node[K, P, V]) split() (new *Node[K, P, V]) {
	cutoff := (len(n.Keys)+1)/2 - 1
	if cutoff == 0 {
		cutoff = 1
	}

	if n.isleaf() {
		new = newNodeFrom(n.Keys[cutoff:], []P{}, n.Values[cutoff:])
		n.Values = n.Values[:cutoff]
	} else {
		new = newNodeFrom(n.Keys[cutoff:], n.Pointers[cutoff+1:], []V{})
		n.Pointers = n.Pointers[:cutoff+1]
	}
	n.Keys = n.Keys[:cutoff]
	return new
}

func (b *BTree[K, P, V]) insert(key K, val V, overwrite bool) error {
	node, path, err := b.findleaf(key)
	if err != nil {
		return err
	}

	ptr := path[len(path)-1]
	path = path[:len(path)-1]

	idx, found := b.findsplit(key, node)
	if found {
		if overwrite {
			node.Values[idx] = val
			return b.store.Update(ptr, node)
		}
		return ErrExists
	}

	b.Count++

	node.insertAt(idx, key, val)
	if len(node.Keys) < b.Order {
		return b.store.Update(ptr, node)
	}

	new := node.split()
	new.Next = node.Next
	newptr, err := b.store.Put(new)
	if err != nil {
		return err
	}
	node.Next = &newptr
	if err := b.store.Update(ptr, node); err != nil {
		return err
	}

	key = new.Keys[0]
	return b.insertUpwards(key, newptr, path)
}

func (b *BTree[K, P, V]) Insert(key K, val V) error {
	return b.insert(key, val, false)
}

func (b *BTree[K, P, V]) Update(key K, val V) error {
	return b.insert(key, val, true)
}

func (b *BTree[K, P, V]) insertUpwards(key K, ptr P, path []P) error {
	for i := len(path) - 1; i >= 0; i-- {
		node, err := b.store.Get(path[i])
		if err != nil {
			return err
		}

		idx, found := b.findsplit(key, node)
		if found {
			panic("broken invariant: duplicate key in intermediate node")
		}

		node.insertInternal(idx, key, ptr)
		if len(node.Keys) < b.Order {
			return b.store.Update(path[i], node)
		}

		new := node.split()
		key = new.Keys[0]
		new.Keys = new.Keys[1:]
		ptr, err = b.store.Put(new)
		if err != nil {
			return err
		}
		if err := b.store.Update(path[i], node); err != nil {
			return err
		}
	}

	// reached the root, growing the tree
	newroot := &Node[K, P, V]{
		Version:  versioning.FromString(BTREE_NODE_VERSION),
		Keys:     []K{key},
		Pointers: []P{b.Root, ptr},
	}
	rootptr, err := b.store.Put(newroot)
	if err != nil {
		return err
	}
	b.Root = rootptr
	return nil
}
