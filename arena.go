package ahkc

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// arena 是建立在 region 之上的 TLSF 变长分配器（边界标记加分级空闲表）。
//
//   - alloc / free 均为 O(1)；释放时与相邻空闲块合并，碎片只来自实际的存活块分布。
//   - 只由所属分片的维护者调用，无锁；统计用原子镜像，供 Stats 无锁读取。
//   - blockRef 是以 8 字节为单位的偏移（0 为空）。块首的 8 字节是边界字：
//     低 32 位为块大小（单元数，含边界字），bit32 表示本块空闲，bit33 表示前一物理块空闲。
//   - 空闲块在边界字之后存放 next/prev 空闲链接，并在最后一个单元存放自身大小（footer），
//     供后一物理块释放时向前合并。
//
// 区首的 1 个单元是占位（使 0 永远不是有效引用），区尾的 1 个单元是大小为 0 的已用哨兵。
const (
	arenaUnit     = 8
	slLog2        = 4
	slCount       = 1 << slLog2
	fliCount      = 29 // 单元数 < 2^32
	minBlockUnits = 3  // 边界字 + 空闲链接 + footer

	blockFree     = uint64(1) << 32
	blockPrevFree = uint64(1) << 33
)

type blockRef uint32

type arena struct {
	base  unsafe.Pointer
	units uint32
	flMap uint32
	slMap [fliCount]uint32
	heads [fliCount][slCount]blockRef

	usedBytes atomic.Int64 // 已分配字节（含边界字）
	freeBytes atomic.Int64
	// largestFree 是最大空闲块的字节数，用于判断碎片：空闲很多但它很小，说明分配会失败。
	// shortcut: 取最高非空分级的链头块，大小与真正的最大块至多差该分级的宽度（约 1/16）；
	// 需要精确值时遍历该分级的链。
	largestFree atomic.Int64
}

// newArena 在 [base, base+size) 上建立分配器；size 须为 8 的倍数且至少容纳一个最小块。
func newArena(base unsafe.Pointer, size uintptr) *arena {
	units := size / arenaUnit
	if units > 1<<32-1 {
		units = 1<<32 - 1
	}
	a := &arena{base: base, units: uint32(units)}
	first := blockRef(1)
	n := a.units - 2 // 去掉占位与哨兵
	*a.word(first) = uint64(n) | blockFree
	*a.footer(first, n) = uint64(n)
	*a.word(blockRef(a.units - 1)) = blockPrevFree // 哨兵：大小 0，已用，前块空闲
	a.insertFree(first, n)
	a.freeBytes.Store(int64(n) * arenaUnit)
	a.publishLargest()
	return a
}

func (a *arena) publishLargest() {
	var v int64
	if a.flMap != 0 {
		fl := bits.Len32(a.flMap) - 1
		sl := bits.Len32(a.slMap[fl]) - 1
		v = int64(uint32(*a.word(a.heads[fl][sl]))) * arenaUnit
	}
	if a.largestFree.Load() != v { // 多数分配/释放不改变它：只读不写
		a.largestFree.Store(v)
	}
}

func (a *arena) word(r blockRef) *uint64 {
	return (*uint64)(unsafe.Add(a.base, uintptr(r)*arenaUnit))
}

func (a *arena) footer(r blockRef, size uint32) *uint64 {
	return (*uint64)(unsafe.Add(a.base, (uintptr(r)+uintptr(size)-1)*arenaUnit))
}

// links 返回空闲块的链接字：低 32 位 next，高 32 位 prev。
func (a *arena) links(r blockRef) *uint64 {
	return (*uint64)(unsafe.Add(a.base, (uintptr(r)+1)*arenaUnit))
}

// at 返回块起始地址（8 字节对齐）。
func (a *arena) at(r blockRef) unsafe.Pointer {
	return unsafe.Add(a.base, uintptr(r)*arenaUnit)
}

// mapping 返回向下取整的 (fl, sl) 分级。
func mapping(s uint32) (int, int) {
	if s < slCount {
		return 0, int(s)
	}
	k := bits.Len32(s) - 1
	return k - slLog2 + 1, int((s >> (k - slLog2)) & (slCount - 1))
}

// mappingSearch 返回向上取整的分级：该分级及以上的任何空闲块都足以容纳 s。
func mappingSearch(s uint32) (int, int) {
	if s >= slCount {
		k := bits.Len32(s) - 1
		if r := uint64(s) + (1 << (k - slLog2)) - 1; r < 1<<32 {
			s = uint32(r)
		} else {
			s = 1<<32 - 1
		}
	}
	return mapping(s)
}

func (a *arena) insertFree(r blockRef, size uint32) {
	fl, sl := mapping(size)
	head := a.heads[fl][sl]
	*a.links(r) = uint64(head)
	if head != 0 {
		l := a.links(head)
		*l = *l&0xFFFFFFFF | uint64(r)<<32
	}
	a.heads[fl][sl] = r
	a.slMap[fl] |= 1 << sl
	a.flMap |= 1 << fl
}

func (a *arena) removeFree(r blockRef, size uint32) {
	fl, sl := mapping(size)
	l := *a.links(r)
	next, prev := blockRef(l), blockRef(l>>32)
	if prev != 0 {
		p := a.links(prev)
		*p = *p&^0xFFFFFFFF | uint64(next)
	} else {
		a.heads[fl][sl] = next
		if next == 0 {
			a.slMap[fl] &^= 1 << sl
			if a.slMap[fl] == 0 {
				a.flMap &^= 1 << fl
			}
		}
	}
	if next != 0 {
		n := a.links(next)
		*n = *n&0xFFFFFFFF | uint64(prev)<<32
	}
}

// alloc 分配至少 bytes 字节（含边界字）。区内没有足够大的空闲块时返回 false。
func (a *arena) alloc(bytes int) (blockRef, bool) {
	need := uint32((uint64(bytes) + arenaUnit - 1) / arenaUnit)
	if need < minBlockUnits {
		need = minBlockUnits
	}
	fl, sl := mappingSearch(need)
	m := a.slMap[fl] & (^uint32(0) << sl)
	if m == 0 {
		fm := a.flMap & (^uint32(0) << (fl + 1))
		if fm == 0 {
			return 0, false
		}
		fl = bits.TrailingZeros32(fm)
		m = a.slMap[fl]
	}
	sl = bits.TrailingZeros32(m)
	r := a.heads[fl][sl]
	size := uint32(*a.word(r))
	a.removeFree(r, size)
	a.freeBytes.Add(-int64(size) * arenaUnit)
	if rem := size - need; rem >= minBlockUnits {
		*a.word(r) = uint64(need)
		rest := r + blockRef(need)
		*a.word(rest) = uint64(rem) | blockFree
		*a.footer(rest, rem) = uint64(rem)
		a.insertFree(rest, rem)
		a.freeBytes.Add(int64(rem) * arenaUnit)
		size = need
	} else {
		*a.word(r) = uint64(size)
		*a.word(r + blockRef(size)) &^= blockPrevFree
	}
	a.usedBytes.Add(int64(size) * arenaUnit)
	a.publishLargest()
	return r, true
}

// free 释放块并与相邻空闲块合并。r 必须是 alloc 返回且尚未释放的引用。
func (a *arena) free(r blockRef) {
	w := *a.word(r)
	size := uint32(w)
	a.usedBytes.Add(-int64(size) * arenaUnit)
	merged := size
	if nw := *a.word(r + blockRef(size)); nw&blockFree != 0 {
		ns := uint32(nw)
		a.removeFree(r+blockRef(size), ns)
		merged += ns
	}
	if w&blockPrevFree != 0 {
		ps := uint32(*a.word(r - 1)) // 前块的 footer
		prev := r - blockRef(ps)
		a.removeFree(prev, ps)
		r = prev
		merged += ps
	}
	*a.word(r) = uint64(merged) | blockFree
	*a.footer(r, merged) = uint64(merged)
	*a.word(r + blockRef(merged)) |= blockPrevFree
	a.insertFree(r, merged)
	a.freeBytes.Add(int64(size) * arenaUnit)
	a.publishLargest()
}
