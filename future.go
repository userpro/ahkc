package ahkc

import (
	"context"
	"runtime"
	"sync/atomic"
)

// Future 与 delivery：等待者的异步结果句柄及不可变业务结果的交付容器。
// 回源合并状态机本身见 flight.go。

// waiter 状态：WAITING → READY/READY_INLINE → TAKEN，或 CANCELLED（I8）。
const (
	waiterWaiting int32 = iota
	waiterReady
	waiterReadyInline
	waiterReadyCached
	waiterTaken
	waiterCancelled
	waiterBinding // publisher owns the payload until it publishes READY or observes cancellation
)

// Future 是等待者的异步结果句柄。Wait/Take/Cancel 只会结算一次。
type Future[V any] struct {
	state    atomic.Int32
	ready    chan struct{} // MARK_READY：仅通知就绪
	notified atomic.Bool
	delivery *delivery[V] // READY 时持有
	cached   *Handle[V]   // READY_CACHED：直接持有缓存句柄，不经共享 delivery
	err      error        // READY_INLINE 时的有界错误
	deadline int64
	rcSlot   receipt     // 内嵌凭证存储：waiter 槽位凭证随 Future 一次分配
	guarded  atomic.Bool // 已挂终结器安全网（仅交给调用方持有的 Future）
}

// 已就绪的缓存结果共享关闭信号，避免每次分配并关闭一个 channel。
var completedFutureSignal = func() chan struct{} {
	ready := make(chan struct{})
	close(ready)
	return ready
}()

// guard 为交给调用方持有的 Future 挂安全网：若调用方既不取走结果也不取消就丢弃了它，
// GC 回收时自动 Cancel，归还 waiter 槽位与响应预留，避免预算被永久占用。
// 这只是兜底（回收时机不确定），正常使用仍应 Wait / Take / Cancel 终结。
func (w *Future[V]) guard() {
	if w.guarded.CompareAndSwap(false, true) {
		runtime.SetFinalizer(w, cancelAbandonedFuture)
	}
}

// 非泛型回调避免每次注册都分配捕获类型字典的闭包。
func cancelAbandonedFuture(f interface{ Cancel() }) { f.Cancel() }

// unguard 在 Future 已终结后摘除安全网，避免终结器拖慢回收。
func (w *Future[V]) unguard() {
	if w.guarded.CompareAndSwap(true, false) {
		runtime.SetFinalizer(w, nil)
	}
}

// newFuture 构造 Future 并接管已取得的 waiter 槽位凭证（src 在调用后失效）。
func newFuture[V any](deadline int64, src *receipt) *Future[V] {
	w := &Future[V]{ready: make(chan struct{}), deadline: deadline}
	if src != nil {
		src.moveTo(&w.rcSlot)
	}
	return w
}

// newFutureReserving 先检查额度再分配：过载时拒绝路径不产生堆分配。
func newFutureReserving[V any](b *budget, deadline int64) (*Future[V], failReason) {
	var rc receipt
	if fr := b.tryReserveInto(&rc, 1); fr != failOK {
		return nil, fr
	}
	return newFuture[V](deadline, &rc), failOK
}

// 终态 CAS 保证只有一个结算者；归还后断开凭证对整个缓存的引用。
func (w *Future[V]) releaseSlot() {
	w.rcSlot.returnOnce()
	w.rcSlot.budget = nil
}

// bindReady 绑定结果并通知（BIND_READY）。只有 CAS 认领成功者交付：
// 认领 → 写载荷 → 置发布位 → 通知。失败方不触碰载荷字段（可能已有胜者）。
func (w *Future[V]) bindReady(d *delivery[V]) {
	d.internalRefs.Add(1)
	if !w.state.CompareAndSwap(waiterWaiting, waiterBinding) {
		d.dropInternalRef()
		return
	}
	w.delivery = d
	if !w.state.CompareAndSwap(waiterBinding, waiterReady) {
		w.delivery = nil
		d.dropInternalRef()
	}
	w.notify()
}

// readyInline 以有界错误码终结（无 payload）。
func (w *Future[V]) readyInline(err error) {
	if !w.state.CompareAndSwap(waiterWaiting, waiterBinding) {
		return
	}
	w.err = err
	if !w.state.CompareAndSwap(waiterBinding, waiterReadyInline) {
		w.err = nil
	}
	w.notify()
}

func (w *Future[V]) notify() {
	if w.notified.CompareAndSwap(false, true) {
		close(w.ready)
	}
}

// Wait 等待结果；ctx 取消只解除自己的等待，不影响其他等待者与底层 I/O。
func (w *Future[V]) Wait(ctx context.Context) (V, error) {
	if ctx == nil {
		var zero V
		return zero, ErrNilContext
	}
	// 快速路径
	if v, ok, err := w.Take(); ok {
		return v, err
	}
	select {
	case <-w.ready:
		if v, ok, err := w.Take(); ok {
			return v, err
		}
		var zero V
		return zero, ErrConsumed
	case <-ctx.Done():
		w.Cancel()
		var zero V
		return zero, contextError(ctx.Err())
	}
}

// Take 非阻塞取结果。ok=false 仅表示尚未就绪；终态返回 ok=true，
// 重复取走返回 ErrConsumed，取消返回 ErrCanceled。
func (w *Future[V]) Take() (v V, ok bool, err error) {
	var zero V
	if w == nil || w.ready == nil {
		return zero, true, ErrUninitialized
	}
	s := w.state.Load()
	if s == waiterTaken {
		return zero, true, ErrConsumed
	}
	if s == waiterCancelled {
		w.unguard()
		return zero, true, ErrCanceled
	}
	if s != waiterReady && s != waiterReadyInline && s != waiterReadyCached {
		return zero, false, nil
	}
	if !w.state.CompareAndSwap(s, waiterTaken) {
		if w.state.Load() == waiterCancelled {
			return zero, true, ErrCanceled
		}
		return zero, true, ErrConsumed
	}
	w.unguard()
	if s == waiterReadyCached {
		h := w.cached
		w.cached = nil
		v = h.Value()
		h.Release()
		w.releaseSlot()
		return v, true, nil
	}
	if s == waiterReadyInline {
		err := w.err
		w.err = nil
		w.releaseSlot()
		return zero, true, err
	}
	d := w.delivery
	w.delivery = nil
	v, err = d.outcome.value, d.outcome.err
	d.dropInternalRef()
	w.releaseSlot()
	return v, true, err
}

// Cancel 取消等待（CANCEL_WAITER）：只解除自己的等待，不取消底层 I/O。
func (w *Future[V]) Cancel() {
	if w == nil || w.ready == nil {
		return
	}
	// All competing transitions advance the state; three attempts cover
	// WAITING -> BINDING -> READY -> TAKEN without waiting for a publisher.
	for attempt := 0; attempt < 3; attempt++ {
		s := w.state.Load()
		switch s {
		case waiterWaiting, waiterBinding, waiterReady, waiterReadyInline, waiterReadyCached:
			if !w.state.CompareAndSwap(s, waiterCancelled) {
				continue
			}
			if s == waiterReady {
				d := w.delivery
				w.delivery = nil
				d.dropInternalRef()
			} else if s == waiterReadyCached {
				w.cached.Release()
				w.cached = nil
			} else if s == waiterReadyInline {
				w.err = nil
			}
			w.releaseSlot()
			w.notify()
			w.unguard()
			return
		default:
			return // TAKEN/CANCELLED：不再归还
		}
	}
}

// Deadline 返回等待期限。
func (w *Future[V]) Deadline() int64 {
	if w == nil {
		return 0
	}
	return w.deadline
}

// delivery 是不可变业务结果的交付容器；响应保留与缓存保留分别记账。
type delivery[V any] struct {
	outcome      sourceOutcome[V]
	internalRefs atomic.Int32 // Flight bridge + 未移交 waiter
	respReceipt  *receipt
}

func (d *delivery[V]) dropInternalRef() {
	if d.internalRefs.Add(-1) == 0 {
		// End actual library retention before returning the response reservation.
		if d.outcome.cached != nil {
			d.outcome.cached.Release()
		}
		d.outcome = sourceOutcome[V]{}
		d.respReceipt.returnOnce()
	}
}
