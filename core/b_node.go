package core

import (
	"bytes"
	"encoding/binary"
)

const (
	BNODE_NODE = 1 // internal nodes without values
	BNODE_LEAF = 2 // leaf nodes with values
)

/*
a node's data formate:
| type | nkeys | pointers   | offsets    | key-values
| 2B   | 2B    | nkeys * 8B | nkeys * 2B | ...
This is the format of the KV pair. Lengths followed by data.
| klen | vlen | key | val |
| 2B   | 2B   | ... | ... |
*/
type BNode struct {
	data []byte // can be dumped to the disk
}

// header
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

// A fixed-sized header containing the type of the node
// (leaf node or internal node) and the number of keys.
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
func (node BNode) getPtr(idx uint16) uint64 {
	// 获取指向子节点的指针
	//assert(idx < node.nkeys()) todo:增加err处理
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	//assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// The offset is relative to the position of the first KV pair.
// The offset of the first KV pair is always zero, so it is not stored in the list.
// We store the offset to the end of the last KV pair in the offset list,
// which is used to determine the size of the node
// offsetPos 的目标是根据偏移量列表的起始位置和第 idx 个键值对的索引，计算出该偏移量在存储区域中的具体位置。
func offsetPos(node BNode, idx uint16) uint16 {
	//	assert(1 <= idx && idx <= node.nkeys())
	// 每个偏移量占用2个字节，所以乘以2
	// 这里的计算都是以Byte为单位的
	// 8*node.nkeys()就是pointers的位置，因为一个指针8Bytes
	// 这里是计算出offet字节的位置，然后再根据offet对应的值来算出对应的kvs的位置
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}

// 这个函数来获取offet字节数组存储的值
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
// 注意这些偏移量，可以非常快速地定位kv
func (node BNode) kvPos(idx uint16) uint16 {
	//assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	//assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	// 获取key的字节长度
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	// pos+4是跳过klen+vlen
	return node.data[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	//assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	// 为什么这个代表node的大小呢？偏移量不是返回idx对应的数据开始位置吗？
	// 这是因为idx是从0开始计算的，所以这里传入nkeys()相当于计算下一个要插入的kv的字节位置
	// 也就算出了当前存储数据的最后一个idx的字节数据结束位置
	return node.kvPos(node.nkeys())
}

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: bisect
// The lookup works for both leaf nodes and internal nodes.
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key.
	// Note that the first key is skipped
	// for comparison, since it has already been compared from the parent node
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// add a new key to a leaf node
func leafInsert(
	new BNode,
	old BNode,
	idx uint16,
	key []byte,
	val []byte,
) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// copy multiple KVs into the position
func nodeAppendRange(
	new BNode, old BNode,
	dstNew uint16, srcOld uint16, n uint16,
) {
	//assert(srcOld+n <= old.nkeys())
	//assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}
	// pointers
	// 注意是小于n
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// 复制offset
	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	// 因为offset不存储idx=0的偏移量
	for i := uint16(1); i <= n; i++ { // NOTE: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}
	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	// 计算当前键值对（键长度、值长度、键和值本身）的总字节数，作为下一个键值对的偏移量
	// 因为当前键值对的存储过程会直接影响下一个键值对的存储位置
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode{data: make([]byte, 2*BTREE_PAGE_SIZE)}
	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// 更新叶子节点的键值对数量（数量保持不变）
	new.setHeader(BNODE_LEAF, old.nkeys())

	// 1. 复制 `idx` 之前的键值对
	nodeAppendRange(new, old, 0, 0, idx)

	// 2. 更新目标键值对
	// 注意：即使键（key）没有变化，偏移量列表仍需要重新计算，因为值（val）的长度可能改变
	nodeAppendKV(new, idx, 0, key, val)

	// 3. 复制 `idx` 之后的键值对
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
func nodeSplit2(left BNode, right BNode, old BNode) {
	// [splitIdx,...)为右节点,[0,idx)是左节点，注意是左闭右开
	splitIdx := old.nkeys() - 1

	// 动态调整分裂点，确保右节点大小符合页面限制
	for {
		// 计算右节点的大小
		rightSize := old.nbytes() - old.kvPos(splitIdx)
		if rightSize <= BTREE_PAGE_SIZE {
			if splitIdx == 1 {
				break // 已经无法再向左调整，不然左节点就是空节点了
			}
			splitIdx--
			continue
		}
		// 如果右节点大小超出限制，向右调整分裂点
		if splitIdx == old.nkeys() {
			panic("Cannot split: no valid split point found")
		}
		splitIdx++
		break
	}

	// 设置左节点和右节点的头部
	left.setHeader(old.btype(), splitIdx)
	right.setHeader(old.btype(), old.nkeys()-splitIdx)

	// 将数据复制到左节点
	// 注意是左闭右开，splitIdx至少要等于1，不然左节点是空的
	nodeAppendRange(left, old, 0, 0, splitIdx)

	// 将数据复制到右节点
	nodeAppendRange(right, old, 0, splitIdx, old.nkeys()-splitIdx)
}

// split a node if it's too big. the results are 1~3 nodes.
// 检查子节点是否需要分裂。如果子节点的大小超出了限制，则将其分裂为 2 或 3 个新节点
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}
	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}
	// the left node is still too large
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)
	//assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right}
}

// replace a link with multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

// part of the treeDelete()
func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)
	new := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0:
		//assert(updated.nkeys() > 0)
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// merge 2 nodes into 1
func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}
	if idx > 0 {
		sibling := tree.get(node.getPtr(idx - 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling
		}
	}
	if idx+1 < node.nkeys() {
		sibling := tree.get(node.getPtr(idx + 1))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling
		}
	}
	return 0, BNode{}
}
