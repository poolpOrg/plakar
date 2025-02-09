package btree

import (
	"fmt"
	"io"
)

func (b *BTree[K, P, V]) depth() int {
	ptr := b.Root
	depth := 0

	for {
		node, err := b.store.Get(ptr)
		if err != nil {
			return -1
		}

		if node.isleaf() {
			break
		}

		depth++
		ptr = node.Pointers[0]
	}

	return depth
}

type VerifyState struct {
	LeafDepth    int
	CurrDepth    int
	VisitedCount int64
}

func (b *BTree[K, P, V]) Verify() error {
	state := VerifyState{b.depth(), 0, 0}

	rootNode, err := b.store.Get(b.Root)
	if err != nil {
		return fmt.Errorf("failed to get root node: %w", err)
	}

	if rootNode.isleaf() {
		return nil
	}

	err = b.verifyNode(rootNode, nil, -1, &state)
	fmt.Printf("Verify ended, visited %d nodes\n", state.VisitedCount)

	return err
}

// ptrIdx is the pointer array index of the parent, aka the upper link to our cur node
func (b *BTree[K, P, V]) verifyNode(cur, parent *Node[K, P, V], ptrIdx int, state *VerifyState) error {
	state.VisitedCount++

	// First check for the occupancy invariants.
	if cur.isleaf() {
		if state.CurrDepth != state.LeafDepth {
			return fmt.Errorf("Leaf: broken invariant: Left-most leaf depth is different from current one's %v vs %v", state.LeafDepth, state.CurrDepth)
		}

		if len(cur.Keys) < (b.Order/2) || len(cur.Keys) >= b.Order {
			return fmt.Errorf("Leaf: broken invariant: Keys occupancy, should be between %d and %d but got %d", (b.Order / 2), b.Order, len(cur.Keys))
		}

		if len(cur.Values) < (b.Order/2) || len(cur.Values) >= b.Order {
			return fmt.Errorf("Leaf: broken invariant: Values occupancy, should be between %d and %d but got %d", (b.Order / 2), b.Order, len(cur.Values))
		}

		// This is impossible to reach, as isleaf uses this check but we keep it for documentation purpose
		if len(cur.Pointers) != 0 {
			return fmt.Errorf("Leaf: broken invariant: Pointers (child) is not empty")
		}
	} else {
		if len(cur.Keys) < (b.Order/2) || len(cur.Keys) >= b.Order {
			return fmt.Errorf("InternalNode: broken invariant: Keys occupancy, should be between %d and %d but got %d", (b.Order / 2), b.Order, len(cur.Keys))
		}

		if len(cur.Values) != 0 {
			return fmt.Errorf("InteralNode: broken invariant: Values is not empty")
		}

		if len(cur.Pointers) < (b.Order/2) || len(cur.Pointers) >= (b.Order+1) {
			return fmt.Errorf("InternalNode: broken invariant: Pointers occupancy, should be between %d and %d but got %d", (b.Order / 2), b.Order, len(cur.Pointers))
		}
	}

	// Now check the order of keys.
	for i := 1; i < len(cur.Keys); i++ {
		if b.compare(cur.Keys[i-1], cur.Keys[1]) >= 0 {
			return fmt.Errorf("Node: broken ordering of keys %v", cur.Keys)
		}
	}

	// Check ordering between parent and us
	if parent != nil {
		if ptrIdx == 0 {
			// left-most value we just check the upper bound. No need to check
			// the siblings, it's done by checking the bounds in parent.
			if b.compare(cur.Keys[len(cur.Keys)-1], parent.Keys[ptrIdx]) > 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('-inf' / '%v') -> Child('%v')", parent.Keys[ptrIdx], cur.Keys[len(cur.Keys)-1])
			}
		} else if ptrIdx == len(parent.Pointers)-1 {
			// right-most value. dito.
			if b.compare(cur.Keys[0], parent.Keys[ptrIdx-1]) < 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('%v' / '+inf') -> Child('%v')", parent.Keys[ptrIdx-1], cur.Keys[0])
			}
		} else {
			if b.compare(cur.Keys[0], parent.Keys[ptrIdx-1]) < 0 || b.compare(cur.Keys[len(cur.Keys)-1], parent.Keys[ptrIdx]) > 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('%v' / '%v') -> Child('%v')", parent.Keys[ptrIdx-1], parent.Keys[ptrIdx], cur.Keys[0])
			}
		}
	}

	// Now check the order of keys.
	for i := 1; i < len(cur.Keys); i++ {
		if b.compare(cur.Keys[i-1], cur.Keys[1]) >= 0 {
			return fmt.Errorf("Node: broken ordering of keys %v", cur.Keys)
		}
	}

	// Check ordering between parent and us
	if parent != nil {
		if ptrIdx == 0 {
			// left-most value we just check the upper bound. No need to check
			// the siblings, it's done by checking the bounds in parent.
			if b.compare(cur.Keys[len(cur.Keys)-1], parent.Keys[ptrIdx]) > 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('-inf' / '%v') -> Child('%v')", parent.Keys[ptrIdx], cur.Keys[len(cur.Keys)-1])
			}
		} else if ptrIdx == len(parent.Pointers)-1 {
			// right-most value. dito.
			if b.compare(cur.Keys[0], parent.Keys[ptrIdx-1]) < 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('%v' / '+inf') -> Child('%v')", parent.Keys[ptrIdx-1], cur.Keys[0])
			}
		} else {
			if b.compare(cur.Keys[0], parent.Keys[ptrIdx-1]) < 0 || b.compare(cur.Keys[len(cur.Keys)-1], parent.Keys[ptrIdx]) > 0 {
				return fmt.Errorf("InternalNode: broken invariant: Parent/Child ordering is wrong Parent ('%v' / '%v') -> Child('%v')", parent.Keys[ptrIdx-1], parent.Keys[ptrIdx], cur.Keys[0])
			}
		}
	}

	state.CurrDepth++
	for i, child := range cur.Pointers {
		childNode, err := b.store.Get(child)

		if err != nil {
			return fmt.Errorf("Failed to fetch node (%v)", child)
		}

		err = b.verifyNode(childNode, cur, i, state)
		if err != nil {
			return err
		}
	}
	state.CurrDepth--

	return nil
}

func (b *BTree[K, P, V]) Dot(w io.Writer, showNextPtrs bool) error {
	iter := b.IterDFS()
	for iter.Next() {
		ptr, n := iter.Current()

		fmt.Fprintf(w, "%v [label=%q]\n", ptr, fmt.Sprintf("%v %v", ptr, n.Keys))
		for _, cptr := range n.Pointers {
			fmt.Fprintf(w, "%v -> %v\n", ptr, cptr)
		}
		if showNextPtrs && n.Next != nil {
			fmt.Fprintf(w, "%v -> %v\n", ptr, *n.Next)
		}
	}

	return iter.Err()
}
