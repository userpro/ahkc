package ahkc

import "sync/atomic"

// 预算凭证与有界预留。
//
// 不变式 I2：available ∈ [0, limit]；used+reserved+available == limit；
// Receipt 最多归还一次（RETURN_ONCE）。

// failReason 预留失败原因。
type failReason uint8

const (
	failOK failReason = iota
	failFull
	failBusy
)

const reserveRetries = 2 // RESERVE_RETRIES

// budget 是单一度量类别的原子预算。limit 初始化后不变。
type budget struct {
	limit int64
	avail atomic.Int64
}

// init 就地初始化（budget 含原子量，不可复制）。
func (b *budget) init(limit int64) {
	b.limit = limit
	b.avail.Store(limit)
}

// tryReserveInto 最多尝试 RESERVE_RETRIES 次，成功时把凭证写入调用方提供的存储（无堆分配）。
func (b *budget) tryReserveInto(r *receipt, amount int64) failReason {
	if amount <= 0 {
		r.budget, r.amount = b, 0
		r.settled.Store(false)
		return failOK
	}
	for i := 0; i < reserveRetries; i++ {
		old := b.avail.Load()
		if old < amount {
			return failFull
		}
		if b.avail.CompareAndSwap(old, old-amount) {
			r.budget, r.amount = b, amount
			r.settled.Store(false)
			return failOK
		}
	}
	return failBusy
}

// reserveUnitInto 无等待地预留 1 个单位并把凭证写入调用方提供的存储（无堆分配）。
// 先减后检、不足即回滚：单位预留不会产生伪失败（失败当且仅当额度确已耗尽）。
// 回滚前的瞬时负值只对并发的同类单位预留可见，available() 对其做下限截断。
func (b *budget) reserveUnitInto(r *receipt) bool {
	if b.avail.Add(-1) < 0 {
		b.avail.Add(1)
		return false
	}
	r.budget, r.amount = b, 1
	r.settled.Store(false)
	return true
}

// available 返回当前可用额度（观测用，不构成事务视图）。
func (b *budget) available() int64 { return max(0, b.avail.Load()) }

// receipt 是一次成功预留的凭证。终态只有一次：归还或转移。
type receipt struct {
	budget  *budget
	amount  int64
	settled atomic.Bool
}

// returnOnce 归还额度；重复调用安全但只生效一次。
func (r *receipt) returnOnce() {
	if r == nil || r.amount == 0 || r.budget == nil {
		return
	}
	if r.settled.CompareAndSwap(false, true) {
		r.budget.avail.Add(r.amount)
	}
}

// shrinkTo 把尚未归还的预留缩小到 n，多出的额度立即回到预算。调用方须是该凭证当前唯一的使用者
// （例如 loader 返回后、结果发布前）。
func (r *receipt) shrinkTo(n int64) {
	if r == nil || r.budget == nil || n >= r.amount || r.settled.Load() {
		return
	}
	r.budget.avail.Add(r.amount - n)
	r.amount = n
}

// moveTo 把尚未结算也尚未共享的凭证移入长期存储（如内嵌在 Flight / Future 中），
// 源凭证随即失效。仅用于“先在栈上取得额度、确认受理后再落到堆对象”的两段式预留。
func (r *receipt) moveTo(dst *receipt) {
	dst.budget, dst.amount = r.budget, r.amount
	r.budget, r.amount = nil, 0
}
