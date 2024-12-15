# note

## Why use an n-ary tree instead of a binary tree?

answer : p13

## Immutable Data Structures

很有意思的概念

# day3

看到 page 32

nodeReplace2Kid,leafUpdate 函数要自己实现

There are some easy improvements to our B-tree implementation:

1. Use different formats for leaf nodes and internal nodes. Leaf nodes do not need
   pointers and internal nodes do not need values. This saves some space.
2. One of the lengths of the key or value is redundant — the length of the KV pair can
   be inferred from the offset of the next key.
3. The first key of a node is not needed because it’s inherited from a link of its parent.
4. Add a checksum to detect data corruption

# day4

发现 node 都是值传递，这样子 split 是无效的！！！
TODO：待改进
