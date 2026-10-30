package ahkc

import (
	"math"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// F14 式组表并发索引（无墓碑）。
//
// 布局：每组 64 字节 = 8 个 uint64。第 0 字是控制字，低 32 位为 overflow：
// 探测越过本组、落在后续组的 key 数；第 1 到 7 字是槽，高 32 位为 keyHash 标签，
// 低 32 位为块引用，0 表示空。组放在预留的内存区里，不含指针，对 GC 不可见。
//
// 契约：
//   - key 的探测序列是从起始组开始的至多 probes 个相邻组（回绕）。
//   - 不变量：落在序列第 k 组的 key，会使序列前 k 个组的 overflow 各加一。
//     查询某组后若 overflow 为 0，key 一定不在后续组，因此未命中通常只访问一条缓存行。
//   - 插入（仅维护者）：先给途经组 overflow 加一，再发布槽；删除：先清槽，再把途经组减回。
//     读者并发读是可线性化的：在途插入不可见即视为不存在，在途删除视为已不存在。
//   - 没有墓碑，删除腾出的槽直接复用；长期 churn 后未命中的探测组数进入稳态而不会持续增长。
//   - 找不到合法插入位置时拒绝填充（INDEX_LIMIT）。
//
// shortcut: 探测上限固定为 maxGroups 个组（56 个槽）。默认装载率 0.85 满载时约 0.4% 的填充会超出上限，
// 超出只会拒绝这一次填充并计数；调低 Memory.LoadFactor 可降低。条目不会在删除时回迁：缓存满载后长期随机退场与填充会让约 19% 的
// key 落在后继组（未命中平均约 3 个组）；要回迁需要给读者加版本重试，代价大于收益。
const (
	groupWords = 8
	groupSlots = groupWords - 1
	maxGroups  = 8
)

type index struct {
	words  []atomic.Uint64 // groups × groupWords
	groups uint64
	probes int // min(maxGroups, groups)：序列内的组互不重复
	a      *arena
}

// indexGroups 是容纳 countMax 个条目所需的组数（装载率 loadFactor）。
func indexGroups(countMax int64, loadFactor float64) int {
	return max(1, int(math.Ceil(float64(countMax)/(groupSlots*loadFactor))))
}

// indexRegionBytes 是组表需要的内存区大小。
func indexRegionBytes(groups int) uintptr { return uintptr(groups) * groupWords * 8 }

// newIndex 把组表建立在 r 上（调用方保证 r 至少 indexRegionBytes 字节且已清零）。
func newIndex(r *region, a *arena, groups int) *index {
	return &index{
		words:  unsafe.Slice((*atomic.Uint64)(r.base), groups*groupWords),
		groups: uint64(groups),
		probes: min(maxGroups, groups),
		a:      a,
	}
}

func tagOf(keyHash uint64) uint64 { return keyHash >> 32 << 32 }

// home 是 key 的起始组。使用混合后的哈希：低位已用于分片选择（同分片 key 低位相同），
// 直接使用会造成同分片起点聚簇；fastrange 用高位，组数不必是 2 的幂。
func (ix *index) home(keyHash uint64) uint64 {
	hi, _ := bits.Mul64(mix64(keyHash), ix.groups)
	return hi
}

func (ix *index) next(g uint64) uint64 {
	if g++; g == ix.groups {
		g = 0
	}
	return g
}

type findResult struct {
	e     *entry
	slot  int
	found bool
}

// find 沿探测序列查找；返回 ABSENT 是确定结论（overflow 为 0 即终止，或序列走完）。
func (ix *index) find(key string, keyHash uint64, nsID uint32) findResult {
	g := ix.home(keyHash)
	tag := keyHash >> 32
	for j := 0; j < ix.probes; j++ {
		base := int(g) * groupWords
		for i := 1; i < groupWords; i++ {
			v := ix.words[base+i].Load()
			if v>>32 != tag || uint32(v) == 0 {
				continue
			}
			p := ix.a.entry(blockRef(v))
			if p.keyHash == keyHash && p.nsID == nsID && p.keyString() == key {
				return findResult{e: p, slot: base + i, found: true}
			}
		}
		if uint32(ix.words[base].Load()) == 0 {
			break
		}
		g = ix.next(g)
	}
	return findResult{}
}

// slotEntry 返回槽位当前条目（供慢扫描与提示复核；控制字与空槽返回 nil）。
func (ix *index) slotEntry(slot int) *entry {
	if slot < 0 || slot >= len(ix.words) || slot%groupWords == 0 {
		return nil
	}
	v := ix.words[slot].Load()
	if uint32(v) == 0 {
		return nil
	}
	return ix.a.entry(blockRef(v))
}

// installSlot 为维护者寻找安装位置：序列内第一个空槽，或属于本次移除集合的槽。
// 不实际删除；不以“未来可能回收的内存”作为插入资源。
func (ix *index) installSlot(keyHash uint64, inRemoval func(*entry) bool) (int, bool) {
	g := ix.home(keyHash)
	for j := 0; j < ix.probes; j++ {
		base := int(g) * groupWords
		for i := 1; i < groupWords; i++ {
			v := ix.words[base+i].Load()
			if uint32(v) == 0 || (inRemoval != nil && inRemoval(ix.a.entry(blockRef(v)))) {
				return base + i, true
			}
		}
		g = ix.next(g)
	}
	return -1, false
}

// inProbeDomain 报告槽位是否落在 keyHash 的探测序列内（供求解器判断移除该槽的条目能否腾出安装位）。
func (ix *index) inProbeDomain(keyHash uint64, slot int) bool {
	d := (uint64(slot/groupWords) + ix.groups - ix.home(keyHash)) % ix.groups
	return d < uint64(ix.probes)
}

// remove 摘除条目（唯一维护者）：先清槽，再把途经组的 overflow 减回。
func (ix *index) remove(e *entry) {
	slot := int(e.indexSlot)
	ix.words[slot].Store(0)
	target := uint64(slot / groupWords)
	for g := ix.home(e.keyHash); g != target; g = ix.next(g) {
		ix.words[int(g)*groupWords].Add(^uint64(0))
	}
}

// publish 在预留槽位发布条目：先给途经组 overflow 加一，再完整初始化并置 readable，
// 最后对读取可见（I3）。
func (ix *index) publish(e *entry, slot int) {
	target := uint64(slot / groupWords)
	for g := ix.home(e.keyHash); g != target; g = ix.next(g) {
		ix.words[int(g)*groupWords].Add(1)
	}
	e.indexSlot = uint32(slot)
	e.setReadable(true)
	ix.words[slot].Store(tagOf(e.keyHash) | uint64(ix.a.refOf(e)))
}
