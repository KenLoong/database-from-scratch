package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"syscall"
)

type KV struct {
	Path string
	// internals
	fp   *os.File
	tree BTree
	mmap struct {
		file   int      // file size, can be larger than the database size
		total  int      // mmap size, can be larger than the file size
		chunks [][]byte // multiple mmaps, can be non-continuous
	}
	page struct {
		flushed uint64   // database size in number of pages
		temp    [][]byte // newly allocated pages
	}
}

func (db *KV) Open() error {
	// open or create the DB file
	fp, err := os.OpenFile(db.Path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("OpenFile: %w", err)
	}
	db.fp = fp
	// create the initial mmap
	sz, chunk, err := mmapInit(db.fp)
	if err != nil {
		goto fail
	}
	db.mmap.file = sz
	db.mmap.total = len(chunk)
	db.mmap.chunks = [][]byte{chunk}
	// btree callbacks
	db.tree.get = db.pageGet
	db.tree.new = db.pageNew
	db.tree.del = db.pageDel
	// read the master page
	err = masterLoad(db)
	if err != nil {
		goto fail
	}
	// done
	return nil

fail:
	db.Close()
	return fmt.Errorf("KV.Open: %w", err)
}

// cleanups
func (db *KV) Close() {
	for _, chunk := range db.mmap.chunks {
		err := syscall.Munmap(chunk)
		if err != nil {
			panic(fmt.Sprintf("db close failed,err %+v", err))
		}
	}
	_ = db.fp.Close()
}

// read the db
func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key []byte, val []byte) error {
	db.tree.Insert(key, val)
	return flushPages(db)
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted := db.tree.Delete(key)
	return deleted, flushPages(db)
}

// persist the newly allocated pages after updates
func flushPages(db *KV) error {
	if err := writePages(db); err != nil {
		return err
	}
	return syncPages(db)
}

// create the initial mmap that covers the whole file.
func mmapInit(fp *os.File) (int, []byte, error) {
	fi, err := fp.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat: %w", err)
	}
	if fi.Size()%BTREE_PAGE_SIZE != 0 {
		return 0, nil, errors.New("File size is not a multiple of page size.")
	}
	mmapSize := 64 << 20
	for mmapSize < int(fi.Size()) {
		mmapSize *= 2
	}

	chunk, err := syscall.Mmap(
		int(fp.Fd()),                         // 文件描述符
		0,                                    // 偏移量
		mmapSize,                             // 映射大小
		syscall.PROT_READ|syscall.PROT_WRITE, // 读写权限
		syscall.MAP_SHARED,                   // 共享映射
	)
	if err != nil {
		return 0, nil, fmt.Errorf("mmap: %w", err)
	}
	return int(fi.Size()), chunk, nil
}

// extend the mmap by adding new mappings.
func extendMmap(db *KV, npages int) error {
	// 如果当前总映射空间已经足够，直接返回
	if db.mmap.total >= npages*BTREE_PAGE_SIZE {
		return nil
	}

	chunk, err := syscall.Mmap(
		int(db.fp.Fd()),                      // 文件描述符
		int64(db.mmap.total),                 // offset：从文件的哪个位置开始映射
		db.mmap.total,                        // length: 要映射的长度
		syscall.PROT_READ|syscall.PROT_WRITE, // 读写权限
		syscall.MAP_SHARED,                   // 共享映射
	)
	if err != nil {
		return fmt.Errorf("mmap: %w", err)
	}

	// 更新数据库的内存映射信息

	db.mmap.total += db.mmap.total                 // 总大小翻倍
	db.mmap.chunks = append(db.mmap.chunks, chunk) // 保存新的映射块
	return nil
}

/*
让我用一个具体的例子来解释：

假设：
- BTREE_PAGE_SIZE = 4096（每页4KB）
- 有两个内存映射块(chunks)：
  - chunk[0]: 16KB (可以存4页)
  - chunk[1]: 16KB (可以存4页)

那么：

chunk[0]对应的页面编号：0,1,2,3
chunk[1]对应的页面编号：4,5,6,7

当要获取第6页（ptr=6）时：
1. 第一次循环：
   - start = 0
   - end = 4（16KB/4KB = 4页）
   - ptr(6) >= end(4)，继续下一个chunk

2. 第二次循环：
   - start = 4（上一个chunk的end）
   - end = 8
   - ptr(6) < end(8)，找到了目标chunk
   - offset = 4096 * (6 - 4)
   - = 4096 * 2
   - = 8192

所以`offset = BTREE_PAGE_SIZE * (ptr - start)`就是在计算：
- 目标页面在当前chunk中是第几页(ptr - start)
- 乘以页面大小，得到字节偏移量

这样就能精确定位到目标页面在chunk中的具体位置。
*/
// callback for BTree, dereference a pointer.
func (db *KV) pageGet(ptr uint64) BNode {
	start := uint64(0) // 起始页面编号
	for _, chunk := range db.mmap.chunks {
		// 计算当前chunk结束的页面编号
		end := start + uint64(len(chunk))/BTREE_PAGE_SIZE

		// 如果目标页面在当前chunk中
		if ptr < end {
			// 计算页面在chunk中的字节偏移量
			offset := BTREE_PAGE_SIZE * (ptr - start)
			// 返回对应的页面数据
			return BNode{chunk[offset : offset+BTREE_PAGE_SIZE]}
		}
		// 更新下一个chunk的起始页面编号
		start = end
	}
	panic("bad ptr")
}

const DB_SIG = "BuildYourOwnDB05"

// the master page format.
// it contains the pointer to the root and other important bits.
// | sig | btree_root | page_used |
// | 16B | 8B         | 8B        |
func masterLoad(db *KV) error {
	if db.mmap.file == 0 {
		// empty file, the master page will be created on the first write.
		db.page.flushed = 1 // reserved for the master page
		return nil
	}
	data := db.mmap.chunks[0]
	root := binary.LittleEndian.Uint64(data[16:])
	used := binary.LittleEndian.Uint64(data[24:])
	// verify the page
	if !bytes.Equal([]byte(DB_SIG), data[:16]) {
		return errors.New("Bad signature.")
	}
	bad := !(1 <= used && used <= uint64(db.mmap.file/BTREE_PAGE_SIZE))
	bad = bad || !(0 <= root && root < used)
	if bad {
		return errors.New("Bad master page.")
	}
	db.tree.root = root
	db.page.flushed = used
	return nil
}

// update the master page. it must be atomic.
func masterStore(db *KV) error {
	var data [32]byte
	copy(data[:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(data[16:], db.tree.root)
	binary.LittleEndian.PutUint64(data[24:], db.page.flushed)
	// NOTE: Updating the page via mmap is not atomic.
	// Use the `pwrite()` syscall instead.
	_, err := db.fp.WriteAt(data[:], 0)
	if err != nil {
		return fmt.Errorf("write master page: %w", err)
	}
	return nil
}

// callback for BTree, allocate a new page.
func (db *KV) pageNew(node BNode) uint64 {
	// TODO: reuse deallocated pages
	// assert(len(node.data) <= BTREE_PAGE_SIZE)
	ptr := db.page.flushed + uint64(len(db.page.temp))
	db.page.temp = append(db.page.temp, node.data)
	return ptr
}

// callback for BTree, deallocate a page.
func (db *KV) pageDel(uint64) {
	// TODO: implement this
}

// extend the file to at least `npages`.
func extendFile(db *KV, npages int) error {
	filePages := db.mmap.file / BTREE_PAGE_SIZE
	if filePages >= npages {
		return nil
	}
	for filePages < npages {
		// the file size is increased exponentially,
		// so that we don't have to extend the file for every update.
		inc := filePages / 8
		if inc < 1 {
			inc = 1
		}
		filePages += inc
	}
	fileSize := filePages * BTREE_PAGE_SIZE
	// Fallocate 是 Linux 特有的系统调用
	//err := syscall.Fallocate(int(db.fp.Fd()), 0, 0, int64(fileSize))
	// 扩展文件大小
	err := db.fp.Truncate(int64(fileSize))

	if err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	db.mmap.file = fileSize
	return nil
}
