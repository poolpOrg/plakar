package btree

import "errors"

var (
	notfound = errors.New("item not found")
)

type InMemoryStore[K any, V any] struct {
	store []Node[K, int, V]
}

func (s *InMemoryStore[K, V]) get(ptr int) (*Node[K, int, V], error) {
	if ptr >= len(s.store) {
		return nil, notfound
	}

	return &s.store[ptr], nil
}

func (s *InMemoryStore[K, V]) Get(ptr int) (n *Node[K, int, V], err error) {
	node, err := s.get(ptr)
	if err != nil {
		return
	}
	return node, nil
}

func (s *InMemoryStore[K, V]) Update(ptr int, n *Node[K, int, V]) error {
	_, err := s.get(ptr)
	if err != nil {
		return err
	}

	dup := newNodeFrom(n.Keys, n.Pointers, n.Values)
	dup.Next = n.Next
	s.store[ptr] = *dup
	return nil
}

func (s *InMemoryStore[K, V]) Put(n *Node[K, int, V]) (int, error) {
	dup := newNodeFrom(n.Keys, n.Pointers, n.Values)
	dup.Next = n.Next
	s.store = append(s.store, *dup)
	return len(s.store) - 1, nil
}

