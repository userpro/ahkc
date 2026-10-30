package ahkc

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"unsafe"
)

// region 是创建缓存时一次性预留的连续内存区，缓存关闭并回收完成后整体释放。
//
// 64 位 linux 且非 -race 构建用匿名 mmap：只占虚拟地址，物理页在首次写入时才提交，
// 且不计入 Go 的 GOGC 触发线；其余平台与 -race 构建用 Go 堆上的 []uint64（保证 8 字节对齐，
// 并保留竞态与 checkptr 检查）。区内不存放 Go 指针，因此 GC 不会扫描它。
type region struct {
	base   unsafe.Pointer
	size   uintptr
	heap   []uint64 // 堆后端持有的底层数组
	mapped []byte   // mmap 后端的映射
}

var errNoMmap = errors.New("ahkc: mmap unavailable")

// newRegion 预留 size 字节（向上取 8 的倍数）。forceHeap 或平台不支持 mmap 时走堆后端；
// 平台支持 mmap 却失败（虚拟内存上限、地址空间耗尽）时返回错误，不回退到同样大小的堆分配：
// 那个分配在该规模下必然失败，而 Go 的内存不足是无法恢复的 fatal error，会把宿主进程带崩。
func newRegion(size uintptr, forceHeap, hugePages bool) (*region, error) {
	size = (size + 7) &^ 7
	if !forceHeap {
		b, err := mmapRegion(int(size), hugePages)
		switch {
		case err == nil:
			return &region{base: unsafe.Pointer(&b[0]), size: size, mapped: b}, nil
		case !errors.Is(err, errNoMmap):
			return nil, fmt.Errorf("ahkc: reserving %d bytes of address space: %w (lower MaxBytes/MaxEntries/Shards, raise the virtual-memory limit, or set Memory.Heap)", size, err)
		}
	}
	if size > uintptr(math.MaxInt)/2 {
		return nil, fmt.Errorf("ahkc: %d bytes exceeds what a heap region can hold on this platform", size)
	}
	h := make([]uint64, size/8)
	return &region{base: unsafe.Pointer(&h[0]), size: size, heap: h}, nil
}

// prefault 按页写触碰前 n 字节，使物理页在启动期而不是线上请求路径上提交。
func (r *region) prefault(n uintptr) {
	n = min(n, r.size)
	for off := uintptr(0); off+4 <= n; off += 4096 {
		atomic.AddUint32((*uint32)(unsafe.Add(r.base, off)), 0) // 写访问触发缺页，内容不变
	}
}

// release 释放区；之后任何对区内地址的访问都是错误（mmap 后端会触发段错误）。
func (r *region) release() {
	if r == nil {
		return
	}
	if r.mapped != nil {
		_ = munmapRegion(r.mapped)
	}
	r.base, r.heap, r.mapped = nil, nil, nil
}
