package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	// 因为idx=0的话，偏移量就是0，所以offset从idx=1开始存储
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
	// 获取key的字节长度(2 bytes for key)
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
	// Note that the first key is skipped for comparison,
	//  since it has already been compared from the parent node
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
	// idx右边的key右移
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// copy multiple KVs into the position
func nodeAppendRange(
	new BNode,
	old BNode,
	dstNew uint16, // 新旧节点开始复制的偏移量位置
	srcOld uint16,
	n uint16, // 代表要复制的kv对数
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
	//todo:这里报错了,既然能超出边界，那说明在merge的时候可能创建了一个超大节点
	fmt.Println("new.kvPos(dstNew):", new.kvPos(dstNew))
	fmt.Println("begin:", begin, "end:", end)
	fmt.Println("len(new.data):", len(new.data))
	fmt.Println("len(old.data[begin:end]):", len(old.data[begin:end]))
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

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	// 更新叶子节点的键值对数量（数量保持不变）
	new.setHeader(BNODE_LEAF, old.nkeys())

	// 1. 复制 `idx` 之前的键值对
	nodeAppendRange(new, old, 0, 0, idx)

	// 2. 更新目标键值对
	// 注意：即使键（key）没有变化，偏移量列表仍需要重新计算，因为值（val）的长度可能改变
	// 这里为什么ptr是0？
	// ptr 参数被设置为 0，这是因为在 叶子节点 中，ptr 实际上并不用于存储有意义的数据
	nodeAppendKV(new, idx, 0, key, val)

	// 3. 复制 `idx` 之后的键值对
	// 看到这里理解为什么在nodeAppendKV里面最后要计算下一个idx的偏移量了吧
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree,
	new BNode, node BNode,
	idx uint16,
	key []byte,
	val []byte,
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
	// 这里的new和node变量，都是分裂出来的字节点的父节点
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...)
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page.
// 这个函数是我自己实现的，一定要加单测
func nodeSplit2(left BNode, right BNode, old BNode) {
	// [splitIdx,...)为右节点,[0,idx)是左节点，注意是左闭右开
	splitIdx := old.nkeys()
	tryIdx := splitIdx - 1

	// 动态调整分裂点，确保右节点大小符合页面限制
	for {
		// 计算右节点的大小
		rightSize := old.nbytes() - old.kvPos(tryIdx)
		if rightSize <= BTREE_PAGE_SIZE {
			if tryIdx == 1 {
				splitIdx = tryIdx
				break // 已经无法再向左调整，不然左节点就是空节点了
			}
			if rightSize == BTREE_PAGE_SIZE {
				splitIdx = tryIdx
				break
			}
			// 还有空间可以放
			splitIdx = tryIdx
			tryIdx--
			continue
		}
		// 到达极限了
		splitIdx = tryIdx + 1
		if splitIdx == old.nkeys() { // 到达这一步，那就是右节点会是空节点
			panic("Cannot split: no valid split point found")
		}
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
	// 这里的逻辑是，确保right节点一定在page size范围内
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
	// 分裂出来的字节点数（如果是1，那就是没分裂）
	inc := uint16(len(kids))
	// 减去 1 是因为我们正在替换原来一个子节点
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	// 注意是左闭右开，所以这里的idx是不包括被复制的
	nodeAppendRange(new, old, 0, 0, idx)
	for i, kNode := range kids {
		// 从idx这里的位置开始，开始放置分裂后的字节点
		// 	node.getKey(0) 是为了获取 新子节点 的第一个键，这个键会用于更新父节点中指向该子节点的指针
		nodeAppendKV(new, idx+uint16(i), tree.new(kNode), kNode.getKey(0), nil)
	}
	// 从 old 节点中复制从 idx + 1 到最后的所有元素
	// 新节点从idx+inc开始填放，因为上面的遍历kids里面，最后一个放置的字节点的位置是idx+inc-1
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
	// 这里返回的updated就是已经更新过的叶子节点
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated.data) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)
	// 注意，这里的new是代替node，而node是中间节点
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
	case mergeDir == 0: // no need to merge
		//assert(updated.nkeys() > 0)
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}

// new代表父节点,node代表原来的父节点
// idx 代表 合并后子节点在父节点中的索引位置。
func nodeReplace2Kid(new, node BNode, idx uint16, u2 uint64, b []byte) {
	// 更新父节点的头部，新的子节点数量为原节点子节点数量 - 1（因为我们替换了一个原有的子节点）
	new.setHeader(BNODE_NODE, node.nkeys()-1) // 更新新的父节点的子节点数量

	// 2. 将原节点 `node` 中的 idx 之前的子节点复制到 `new` 中
	// `nodeAppendRange` 将原节点中的子节点指针从索引 0 到 idx（不包括 idx）复制到 `new` 中
	// 这样 `new` 中的前一部分子节点就和 `node` 保持一致
	nodeAppendRange(new, node, 0, 0, idx) // 将原节点中 0 到 idx 之前的子节点复制到新节点

	// 3. 插入新的子节点指针
	// `nodeAppendKV` 将新子节点的指针 `u2` 插入到新节点 `new` 中，并在父节点中更新相应的键 `b`
	// `u2` 是新的子节点的指针，`b` 是新子节点的第一个键
	nodeAppendKV(new, idx, u2, b, nil) // 插入新的子节点指针 `u2` 和相应的键 `b` 到父节点中

	// 4. 将 `node` 中 idx 之后的子节点复制到 `new` 中
	// dstNew := idx+1：目标节点 new 中，插入数据的起始位置是 idx+1。这个位置是用来接收 父节点中 idx+1 之后的所有子节点。即我们要从源节点 node 中复制的数据会插入到 new 的第 idx+1 位置开始。
	// srcOld := idx+1：源节点 node 中，复制的数据从 idx+1 开始，也就是从父节点 node 中的 第 idx+1 个子节点开始。这是因为我们刚刚删除了 idx 位置的子节点，因此需要将 idx+1 之后的所有子节点指针复制到新的父节点
	nodeAppendRange(new, node, idx+1, idx+1, node.nkeys()-(idx+1)) // 将原节点中 idx 之后的子节点复制到新节点
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
	//idx 是当前子节点在父节点中的索引。idx 表示当前子节点在父节点中的位置是 idx。
	//idx > 0 检查当前子节点是否是父节点的 第一个子节点。
	//如果 idx > 0，说明当前子节点左边有一个兄弟节点，即 有左邻居，可以考虑将当前子节点和左邻居子节点合并。
	//如果 idx == 0，说明当前子节点是父节点的 第一个子节点，没有左邻居节点，此时不能与左邻居合并，只能考虑与右邻居节点合并。
	if idx > 0 {
		leftSibling := tree.get(node.getPtr(idx - 1))
		merged := leftSibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, leftSibling
		}
	}
	if idx+1 < node.nkeys() {
		rightSibling := tree.get(node.getPtr(idx + 1))
		merged := rightSibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, rightSibling
		}
	}
	return 0, BNode{}
}
