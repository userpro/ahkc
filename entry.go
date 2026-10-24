package ahkc

import (
	"sync"
	"sync/atomic"
)

// entry 是缓存条目。字段分三区：
//   - 身份区：发布后不可变（key/keyHash/entryID/nsID/nsGen/value/hardDeadline/billedBytes）。
//   - 请求区：请求线程可更新（readable 只由有效变无效、heat、visited、ownedPins）。
//   - 维护区：仅分片维护者修改（graceUntil/lowSince/indexSlot/policy/timer 链接）。
//
// 固定池会主动复用条目；epoch/ownedPins 保护读者访问，并决定何时归还账目。
type entry[V any] struct {
	// 身份区
	key          string
	keyHash      uint64
	entryID      uint64
	nsID         uint32
	nsGen        uint64
	value        V
	hardDeadline int64
	billedBytes  int64
	costClass    uint32

	// 请求区
	readable  atomic.Bool
	heat      heatCell
	visited   atomic.Bool
	ownedPins atomic.Int64

	// 维护区（仅 shard owner 访问）
	graceUntil int64
	lowSince   int64 // 0 表示 NONE
	indexSlot  int

	inPolicy               bool
	policyPrev, policyNext *entry[V]

	timerMember           bool
	timerEpoch            uint64
	timerLevel, timerSlot int
	timerPrev, timerNext  *entry[V]

	// poolIdx 是固定条目池槽位，初始化时写入。
	poolIdx int32
	inPool  bool // 已由回收路径归还

	// retirement is prepared before an irreversible removal, including its
	// record slot and cleanup job metadata.
	retirement *retiredRecord[V]
}

const pinRetries = 2 // PIN_RETRIES

// tryPin 增加拥有型引用计数，不超过 max。无等待：先加后检，超限即回滚。
// 回滚前的瞬时超额只会让回收判定（ownedPins==0）更保守，不会提前释放；
// 相比有限次 CAS，热 key 并发下不会因 CAS 竞争而产生伪 BUSY。
func (e *entry[V]) tryPin(max int64) bool {
	if e.ownedPins.Add(1) > max {
		e.ownedPins.Add(-1)
		return false
	}
	return true
}

func (e *entry[V]) unpin() { e.ownedPins.Add(-1) }

// entryPool 是每分片固定大小的条目池：存储在初始化时一次分配（计入 M_fixed），
// 池本身不再虚构物理内存；value/key 的动态保留量按责任记账。
// 池的 pop/push 可能发生在填充准备（任意 goroutine）与回收执行器上，
// 使用短临界区互斥锁；池槽仅在退休记录完成回收后才可复用（A.8）。
type entryPool[V any] struct {
	mu    sync.Mutex // 仅用 TryLock / 短临界区 Lock；零值可用
	items []entry[V]
	free  []int32
}

func newEntryPool[V any](size int) *entryPool[V] {
	p := &entryPool[V]{
		items: make([]entry[V], size),
		free:  make([]int32, size),
	}
	for i := range p.items {
		p.items[i].poolIdx = int32(i)
		p.items[i].inPool = true
		p.free[i] = int32(size - 1 - i)
	}
	return p
}

// tryGet 取一个槽位；耗尽或竞争失败返回 false（不阻塞请求路径）。
func (p *entryPool[V]) tryGet() (*entry[V], bool) {
	if !p.mu.TryLock() {
		return nil, false
	}
	if len(p.free) == 0 {
		p.mu.Unlock()
		return nil, false
	}
	i := p.free[len(p.free)-1]
	p.free = p.free[:len(p.free)-1]
	e := &p.items[i]
	e.inPool = false
	p.mu.Unlock()
	return e, true
}

// put 归还槽位。仅在条目已不可达（epoch 安全且 ownedPins==0）后调用。
func (p *entryPool[V]) put(e *entry[V]) {
	p.mu.Lock()
	e.inPool = true
	p.free = append(p.free, e.poolIdx)
	p.mu.Unlock()
}
