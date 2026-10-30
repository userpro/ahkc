package ahkc

import (
	"math"
	"sync/atomic"
	"unsafe"
)

// 分片 epoch 回收域。
//
// 读者在访问索引前进入受限保护槽（PIN），读取期间保持保护，结束后退出（UNPIN）。
// 维护者先阻止新读者取得已摘链对象，记录退休 epoch，
// 再等 epoch 至少推进两代且 ownedPins==0 后才允许回收。
// 槽位数组固定，PIN 失败返回 BUSY，不临时无界分配。

// epochSlotInactive 表示保护槽空闲。活跃槽存储 epoch+1（0 保留给 INACTIVE）。
const epochSlotInactive = 0

// cacheLineSize 是常见 x86-64 / arm64 的缓存行大小，用于避免伪共享。
const cacheLineSize = 64

// epochSlot 独占一条缓存行：相邻读者 pin/unpin 不再互相使对方的缓存行失效。
type epochSlot struct {
	v atomic.Uint64
	_ [cacheLineSize - 8]byte
}

// epochDomain 的 epoch 字段（所有读者每次 pin 都读）与维护者侧的扫描状态之间用填充隔开；
// 起始槽位由每线程本地随机数选取，命中路径上没有共享的计数器写入。
type epochDomain struct {
	epoch atomic.Uint64
	_     [cacheLineSize - 8]byte
	slots []epochSlot

	// 维护者侧推进扫描状态（仅 owner 访问）。
	scanActive  bool
	scanEpoch   uint64
	scanCursor  int
	scanBlocked bool

	// 仅 pin 失败路径写入（独占缓存行，不影响命中路径）：槽耗尽次数 / epoch 连续推进致重试耗尽次数。
	_                   [cacheLineSize]byte
	slotFull, retryFail atomic.Uint64
}

func newEpochDomain(readers int) *epochDomain {
	return &epochDomain{slots: make([]epochSlot, readers)}
}

// guard 是一次活跃的读者保护。
type guard struct {
	dom  *epochDomain
	slot int
}

// pinStart 为当前 goroutine 选取起始槽位：用局部变量的栈地址做乘法散列。
// 不同 goroutine 的栈互不重叠，因此自然散开到不同槽位（热 key 并发时不会都从同一槽开始竞争），
// 且没有共享计数器或随机数状态的读写。栈被拷贝只会改变起点，不影响正确性。
func pinStart(n int) int {
	var anchor byte
	h := uint64(uintptr(unsafe.Pointer(&anchor))) * 0x9E3779B97F4A7C15
	return int((h >> 32) * uint64(n) >> 32)
}

// pin 先宣布保护，再由调用方读取索引指针。成功返回 guard。
// 失败（无空闲槽或 epoch 竞争重试耗尽）返回 ok=false，对应 BUSY。
func (d *epochDomain) pin() (guard, bool) {
	n := len(d.slots)
	start := pinStart(n)
	for attempt := 0; attempt < pinRetries; attempt++ {
		g := d.epoch.Load()
		if g == math.MaxUint64 {
			return guard{}, false
		}
		gotSlot := false
		for i := 0; i < n; i++ {
			idx := (start + i) % n
			if d.slots[idx].v.CompareAndSwap(epochSlotInactive, g+1) {
				gotSlot = true
				if d.epoch.Load() == g {
					return guard{dom: d, slot: idx}, true
				}
				// epoch 在宣布期间推进：撤销并重试。
				d.slots[idx].v.Store(epochSlotInactive)
				break
			}
		}
		if !gotSlot {
			d.slotFull.Add(1)
			return guard{}, false // 无保护槽：BUSY，不无界分配
		}
	}
	d.retryFail.Add(1)
	return guard{}, false
}

// unpin 退出保护；不运行回收扫描或析构。
func (g guard) unpin() {
	g.dom.slots[g.slot].v.Store(epochSlotInactive)
}

// advanceStep 按配额推进 epoch：每次完整扫描最多推进一个 epoch（A.8）。
func (d *epochDomain) advanceStep(quota int) int {
	work := 0
	if !d.scanActive {
		d.scanActive = true
		d.scanEpoch = d.epoch.Load()
		d.scanCursor = 0
		d.scanBlocked = false
	}
	for quota > 0 && d.scanCursor < len(d.slots) {
		v := d.slots[d.scanCursor].v.Load()
		d.scanCursor++
		quota--
		work++
		if v != epochSlotInactive && v-1 < d.scanEpoch {
			d.scanBlocked = true
		}
	}
	if d.scanCursor >= len(d.slots) {
		if !d.scanBlocked && d.scanEpoch < math.MaxUint64 && d.epoch.Load() == d.scanEpoch {
			d.epoch.Store(d.scanEpoch + 1)
		}
		d.scanActive = false
	}
	return work
}

// safeForReclaim 判断 retireEpoch 退休的对象是否满足两代 epoch 条件。
//
// retireEpoch 只存 epoch 的低 32 位，用有符号差比较：差在 [2, 2^31) 时才安全，早于 2 代或回绕后的
// 差值落在 [2^31, 2^32) 都判为不安全（只会推迟回收，不会提前）。
// shortcut: 一个条目在退休链上停留超过 2^31 个 epoch（每个 epoch 至少一个维护周期）才会被多挡一圈，
// 实际不可能；需要更强保证时把 retireEpoch 改回 64 位。
func (d *epochDomain) safeForReclaim(retireEpoch uint32) bool {
	cur := d.epoch.Load()
	if cur > math.MaxUint64-2 {
		return d.noReaders() // admission has stopped; the final domain cannot advance
	}
	return int32(uint32(cur)-retireEpoch) >= 2
}

func (d *epochDomain) noReaders() bool {
	for i := range d.slots {
		if d.slots[i].v.Load() != epochSlotInactive {
			return false
		}
	}
	return true
}
