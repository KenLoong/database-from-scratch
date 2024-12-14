package core

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

type C struct {
	tree  BTree
	ref   map[string]string
	pages map[uint64]BNode
}

func TestBtree(t *testing.T) {
	_ = newC(t)
}

func newC(t *testing.T) *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				assert.True(t, ok)
				return node
			},
			new: func(node BNode) uint64 {
				assert.True(t, node.nbytes() <= BTREE_PAGE_SIZE)
				key := uint64(uintptr(unsafe.Pointer(&node.data[0])))
				assert.True(t, pages[key].data == nil)
				pages[key] = node
				return key
			},
			del: func(ptr uint64) {
				_, ok := pages[ptr]
				assert.True(t, ok)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func (c *C) add(key string, val string) {
	c.tree.Insert([]byte(key), []byte(val))
	c.ref[key] = val
}

func (c *C) del(key string) bool {
	delete(c.ref, key)
	return c.tree.Delete([]byte(key))
}
