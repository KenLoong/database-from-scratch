# note

## Why use an n-ary tree instead of a binary tree?

answer : p13

## Immutable Data Structures

很有意思的概念

# day3

看到 page 32

_nodeReplace2Kid,leafUpdate, nodeSplit2 函数要自己实现_

There are some easy improvements to our B-tree implementation:

1. Use different formats for leaf nodes and internal nodes. Leaf nodes do not need
   pointers and internal nodes do not need values. This saves some space.
2. One of the lengths of the key or value is redundant — the length of the KV pair can
   be inferred from the offset of the next key.
3. The first key of a node is not needed because it’s inherited from a link of its parent.
4. Add a checksum to detect data corruption

# day4

发现 node 都是值传递，这样子 split 是无效的！！！TODO：待改进

# day5

kv store

# day6

- 实现 tree 的 Get 函数
- 实现 freelist 的以下函数，开始上手了理解了
  //func flnSize(node BNode) int
  //func flnNext(node BNode) uint64
  //func flnPtr(node BNode, idx int)
  //func flnSetPtr(node BNode, idx int, ptr uint64)
  //func flnSetHeader(node BNode, size uint16, next uint64)
  //func flnSetTotal(node BNode, total uint64)

freeList 的节点只是复用了 Bnode 节点的结构体的定义，但是内部的格式数据不一样的，要理解这一点才行

freeList 是用来管理页面，而 header 后面的 pointers 才是指向真正的页面，要理解这一点
