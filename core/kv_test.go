package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMmapSize(t *testing.T) {
	mmapSize := 64 << 20
	assert.True(t, mmapSize%BTREE_PAGE_SIZE == 0)
}
