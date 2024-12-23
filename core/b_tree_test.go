package core

import (
	"crypto/rand"
	"fmt"
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

func (c *C) get(key string) (string, bool) {
	val, found := c.tree.Get([]byte(key))
	if !found {
		return "", false
	}
	return string(val), true
}

func generateRandomString(byteCount int) (string, error) {
	// Create a byte slice of the specified size.
	randomBytes := make([]byte, byteCount)

	// Fill the slice with random bytes.
	_, err := rand.Read(randomBytes)
	if err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %v", err)
	}

	// Return the byte slice directly.
	return string(randomBytes), nil
}

func TestInsertKey(t *testing.T) {
	c := newC(t)
	c.add("a", "a1")
	val, exist := c.get("a")
	assert.True(t, exist)
	assert.Equal(t, "a1", val)
}

func TestLargeWrite(t *testing.T) {
	c := newC(t)

	k1, err := generateRandomString(BTREE_MAX_KEY_SIZE)
	assert.Nil(t, err)
	v1, err := generateRandomString(BTREE_MAX_VAL_SIZE)
	assert.Equal(t, BTREE_MAX_VAL_SIZE, len(v1))
	assert.Equal(t, BTREE_MAX_VAL_SIZE, len([]byte(v1)))
	assert.Nil(t, err)
	c.add(k1, v1)

	k2 := "a"
	v2 := "a1"
	_, exist := c.get(k2)
	assert.False(t, exist)

	treeV1, exist := c.get(k1)
	assert.True(t, exist)
	assert.Equal(t, v1, treeV1)

	c.add(k2, v2)
	treeV2, exist := c.get(k2)
	assert.True(t, exist)
	assert.Equal(t, v2, treeV2)
}

func TestMulWriteAndDel(t *testing.T) {
	c := newC(t)
	keyList := make([]string, 0)
	for i := 0; i < 6; i++ {
		key, err := generateRandomString(BTREE_MAX_KEY_SIZE)
		assert.Nil(t, err)
		keyList = append(keyList, key)
		val, err := generateRandomString(BTREE_MAX_VAL_SIZE)
		assert.Nil(t, err)
		c.add(key, val)
	}

	for i := 0; i < len(keyList); i++ {
		treeVal, exist := c.get(keyList[i])
		assert.True(t, exist)
		assert.Equal(t, c.ref[keyList[i]], treeVal)
	}

	for i := 0; i < len(keyList); i++ {
		if i%3 == 0 {
			fmt.Printf("i is %d\n", i)
			c.del(keyList[i])
		}
	}

	for i := 0; i < len(keyList); i++ {
		treeVal, exist := c.get(keyList[i])
		if i%3 == 0 {
			assert.False(t, exist)
			continue
		}
		assert.True(t, exist)
		assert.Equal(t, c.ref[keyList[i]], treeVal)
	}

}
