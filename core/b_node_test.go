package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLeafInsert(t *testing.T) {
	root := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	newNode := BNode{data: make([]byte, BTREE_MAX_VAL_SIZE)}
	root.setHeader(BNODE_LEAF, 0)
	leafInsert(newNode, root, 0, []byte("date"), []byte("2024-11-30"))
	key := newNode.getKey(0)
	val := newNode.getVal(0)
	assert.Equal(t, []byte("date"), key)
	assert.NotEqual(t, []byte("te"), key)
	assert.Equal(t, []byte("2024-11-30"), val)

	assert.Equal(t, uint16(0), root.nkeys())
	assert.Equal(t, uint16(1), newNode.nkeys())
}
