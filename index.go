package ahkc

import "sync/atomic"

// 固定探测范围的并发索引。
//
// 契约：
//   - 插入与查询使用同一固定 probeLimit 个位置；完整检查后可确认 ABSENT。
//   - EMPTY（nil）是从未写入的位置；删除只产生 TOMBSTONE，不能清回 EMPTY（I4）。
//   - 发布与摘除只由分片维护者执行；条目地址在生命周期内稳定（固定池）。
//   - 首版不做在线扩容或重分片；找不到合法插入位置时拒绝填充（INDEX_LIMIT）。
type index[V any] struct {
	slots      []atomic.Pointer[entry[V]]
	slotCount  int
	slotMask   int // slotCount-1（slotCount 恒为 2 的幂）
	probeLimit int
	tomb       *entry[V] // TOMBSTONE 哨兵
}

func newIndex[V any](slotCount, probeLimit int) *index[V] {
	return &index[V]{
		slots:      make([]atomic.Pointer[entry[V]], slotCount),
		slotCount:  slotCount,
		slotMask:   slotCount - 1,
		probeLimit: probeLimit,
		tomb:       new(entry[V]),
	}
}

// probeStart 使用混合后的高位比特：低位已用于分片选择（同分片 key 低位相同），
// 直接使用会造成同分片探测起点聚簇。
func (ix *index[V]) probeStart(keyHash uint64) int {
	return int(mix64(keyHash)) & ix.slotMask
}

type findResult[V any] struct {
	e     *entry[V]
	slot  int
	found bool
}

// find 完整检查 key 的固定探测域；返回 ABSENT 是确定结论（非预算失败推断）。
func (ix *index[V]) find(key string, keyHash uint64, nsID uint32) findResult[V] {
	start := ix.probeStart(keyHash)
	for j := 0; j < ix.probeLimit; j++ {
		slot := (start + j) & ix.slotMask
		p := ix.slots[slot].Load()
		if p == nil {
			return findResult[V]{} // EMPTY：探测链终止
		}
		if p == ix.tomb {
			continue
		}
		if p.keyHash == keyHash && p.nsID == nsID && p.key == key {
			return findResult[V]{e: p, slot: slot, found: true}
		}
	}
	return findResult[V]{}
}

// slotEntry 返回槽位当前条目（供冷却/重建扫描；nil 或 tomb 表示空）。
func (ix *index[V]) slotEntry(slot int) *entry[V] {
	p := ix.slots[slot].Load()
	if p == nil || p == ix.tomb {
		return nil
	}
	return p
}

// installSlot 为维护者寻找安装位置：EMPTY / TOMBSTONE / 属于本次移除集合的槽位。
// 不实际删除；不以“未来可能回收的内存”作为插入资源。
func (ix *index[V]) installSlot(keyHash uint64, inRemoval func(*entry[V]) bool) (int, bool) {
	start := ix.probeStart(keyHash)
	for j := 0; j < ix.probeLimit; j++ {
		slot := (start + j) & ix.slotMask
		p := ix.slots[slot].Load()
		if p == nil || p == ix.tomb || (inRemoval != nil && inRemoval(p)) {
			return slot, true
		}
	}
	return -1, false
}

// remove 摘除条目（唯一维护者）：先标记不可读，再落 TOMBSTONE。
func (ix *index[V]) remove(e *entry[V]) {
	ix.slots[e.indexSlot].Store(ix.tomb)
}

// publish 在预留槽位发布条目：先完整初始化并置 readable，再对读取可见（I3）。
func (ix *index[V]) publish(e *entry[V], slot int) {
	e.indexSlot = slot
	e.readable.Store(true)
	ix.slots[slot].Store(e)
}
