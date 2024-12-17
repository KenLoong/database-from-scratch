package core

import (
	"encoding/binary"
)

const BNODE_FREE_LIST = 3
const FREE_LIST_HEADER = 4 + 8 + 8 // 定义了自由列表节点的头部大小，包括节点类型、大小和指向下一个节点的指针
// FREE_LIST_CAP表示在一个页面中可以存储的指针数量 (一个指针8B)
const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

// 函数声明，全部要自己实现
//func flnSize(node BNode) int
//func flnNext(node BNode) uint64
//func flnPtr(node BNode, idx int)
//func flnSetPtr(node BNode, idx int, ptr uint64)
//func flnSetHeader(node BNode, size uint16, next uint64)
//func flnSetTotal(node BNode, total uint64)

// 自由列表节点的格式，next pointer代表下一个自由列表节点，而pointers指向一个个空闲的页面
// size表示当前节点中存储的指针数量，一个指针指向一个空闲的页面
// The total number of items in the list. This only applies to the head node
// | type | size | total | next pointer | pointers... |
// | 2B   | 2B   | 	8B	 |	8B          | nkeys * 8B  |
type FreeList struct {
	head uint64
	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer
	// The new callback is only for appending new pages
	// since the free list must reuse pages from itself.
	new func(BNode) uint64  // append a new page
	use func(uint64, BNode) // reuse a page
}

func (fl *FreeList) Get(topn int) uint64 {
	// assert(0 <= topn && topn < fl.Total())
	node := fl.get(fl.head)
	for flnSize(node) <= topn {
		topn -= flnSize(node)
		next := flnNext(node)
		// assert(next != 0)
		node = fl.get(next)
	}
	return flnPtr(node, flnSize(node)-topn-1)
}

// remove `popn` pointers and add some new pointers
// popn: 表示请求的页面数量
// freed: 是一个无符号整型切片，用于存储被释放的页面指针
func (fl *FreeList) Update(popn int, freed []uint64) {
	// assert(popn <= fl.Total())
	if popn == 0 && len(freed) == 0 {
		return // nothing to do
	}
	// prepare to construct the new list
	total := fl.Total() // 获取当前自由列表中的总页面数量
	reuse := []uint64{}
	for fl.head != 0 && len(reuse)*FREE_LIST_CAP < len(freed) {
		node := fl.get(fl.head)
		freed = append(freed, fl.head) // recyle the node itself
		if popn >= flnSize(node) {
			// 如果popn大于或等于当前节点的大小，表示可以移除整个节点的所有指针
			// phase 1
			// remove all pointers in this node
			popn -= flnSize(node)
		} else {
			// 如果popn小于当前节点的大小，表示只需移除部分指针。计算剩余的指针数量remain
			// phase 2:
			// remove some pointers
			remain := flnSize(node) - popn
			popn = 0
			// reuse pointers from the free list itself
			for remain > 0 && len(reuse)*FREE_LIST_CAP < len(freed)+remain {
				remain--
				reuse = append(reuse, flnPtr(node, remain))
			}
			// move the node into the `freed` list
			for i := 0; i < remain; i++ {
				freed = append(freed, flnPtr(node, i))
			}
		}
		// discard the node and move to the next node
		total -= uint64(flnSize(node))
		fl.head = flnNext(node)
	}
	// assert(len(reuse)*FREE_LIST_CAP >= len(freed) || fl.head == 0)
	// phase 3: prepend new nodes
	flPush(fl, freed, reuse)
	// done
	flnSetTotal(fl.get(fl.head), uint64(total+uint64(len(freed))))
}

func flPush(fl *FreeList, freed []uint64, reuse []uint64) {
	for len(freed) > 0 {
		new := BNode{make([]byte, BTREE_PAGE_SIZE)}
		// construct a new node
		size := len(freed)
		// 指针数量太多，一个node存不完
		if size > FREE_LIST_CAP {
			size = FREE_LIST_CAP
		}
		flnSetHeader(new, uint16(size), fl.head)
		for i, ptr := range freed[:size] {
			flnSetPtr(new, i, ptr)
		}
		freed = freed[size:]
		if len(reuse) > 0 {
			// reuse a pointer from the list
			fl.head, reuse = reuse[0], reuse[1:]
			fl.use(fl.head, new)
		} else {
			// or append a page to house the new node
			fl.head = fl.new(new)
		}
	}
	// assert(len(reuse) == 0)
}

func flnSize(node BNode) int {
	return int(binary.LittleEndian.Uint16(node.data[2:4])) // 从节点数据中获取指针数量
}

// 只有head节点才需要记录
func flnSetTotal(node BNode, total uint64) {
	binary.LittleEndian.PutUint64(node.data[4:12], total) // 将总数存储在节点数据的第4到第12字节
}

func flnNext(node BNode) uint64 {
	return binary.LittleEndian.Uint64(node.data[12:16]) // 从节点数据中获取下一个节点的指针
}

func flnPtr(node BNode, idx int) uint64 {
	pos := FREE_LIST_HEADER + 8*idx                    // 计算指针在节点数据中的位置
	return binary.LittleEndian.Uint64(node.data[pos:]) // 返回该位置的指针
}

func flnSetHeader(node BNode, size uint16, next uint64) {
	binary.LittleEndian.PutUint16(node.data[2:4], size)   // 设置节点的大小
	binary.LittleEndian.PutUint64(node.data[12:16], next) // 设置下一个节点的指针
}

func flnSetPtr(node BNode, idx int, ptr uint64) {
	pos := FREE_LIST_HEADER + 8*idx                     // 计算指针在节点数据中的位置
	binary.LittleEndian.PutUint64(node.data[pos:], ptr) // 设置该位置的指针
}

func (fl *FreeList) Total() uint64 {
	total := uint64(0)      // 初始化总页面数量
	head := fl.get(fl.head) // 获取当前头节点

	listNodesTotal := binary.LittleEndian.Uint64(head.data[4:12])

	node := head
	// 遍历所有节点，累加每个节点的指针数量
	for i := 0; i < int(listNodesTotal); i++ {
		total += uint64(flnSize(node))
		next := flnNext(node)
		// assert(next != 0)
		node = fl.get(next)
	}

	return total // 返回总页面数量
}
