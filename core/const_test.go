package core

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeSizeFit(t *testing.T) {
	// | type | nkeys | pointers | offsets | key-values
	// | 2B | 2B | nkeys * 8B | nkeys * 2B | ...
	// 确保 a node with single KV pair always fits on a single page.
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	assert.True(t, node1max <= BTREE_PAGE_SIZE)
}

type A struct {
	data []byte
}

func TestAAA(t *testing.T) {
	a := A{data: []byte("hello")}

	modifyA(a)

	fmt.Printf("%s\n", a.data)

}

func modifyA(a A) {
	a.data = []byte("modify...")
}
