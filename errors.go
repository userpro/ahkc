package ahkc

import (
	"context"
	"errors"
	"fmt"
)

// 对外结果错误。BUSY / MISS / CLOSED 语义互不相同，不可混用：
//   - ErrBusy：暂时无法完成判定（保护槽、gate、令牌竞争等），不是确定的 miss，可稍后重试。
//   - miss：Get 返回 ok=false 且 err==nil，表示按固定探测域完整检查后的确定不存在/过期/代际不符。
//   - ErrClosed：实例已停止受理。
var (
	ErrClosed     = errors.New("ahkc: cache closed")
	ErrBusy       = errors.New("ahkc: busy, retry later")
	ErrOverloaded = errors.New("ahkc: overloaded, resource budget exhausted")
	// ErrTimeout 表示等待期限已过。由调用方 ctx 超时引起时同时满足
	// errors.Is(err, context.DeadlineExceeded)；库内部期限（loader / waiter 超时）则不满足，可据此区分。
	ErrTimeout = errors.New("ahkc: wait deadline exceeded")
	// ErrCanceled 表示等待被取消。由调用方 ctx 取消引起时同时满足 errors.Is(err, context.Canceled)。
	ErrCanceled = errors.New("ahkc: request canceled")
	// ErrCloseIncomplete 表示 Close 的 ctx 先到期：停止受理已生效，工作终结与安全回收由后台继续，
	// 可用 WaitDrained / WaitReclaimed / CloseProgress 继续观察。同时满足 errors.Is(err, ctx 错误)。
	ErrCloseIncomplete = errors.New("ahkc: close incomplete, still draining")
	ErrInvalidRequest  = errors.New("ahkc: request belongs to a different namespace or is uninitialized")
	ErrUninitialized   = errors.New("ahkc: uninitialized cache, namespace or future")
	ErrNilContext      = errors.New("ahkc: nil context")
	ErrNilLoader       = errors.New("ahkc: nil loader on cache miss")
	ErrResultTooLarge  = errors.New("ahkc: loader result exceeds MaxResultBytes")
	// ErrInvalidConfig 表示 Config / NamespaceConfig 校验未通过；详细原因在被包装的错误中。
	ErrInvalidConfig = errors.New("ahkc: invalid config")
	// ErrConsumed 表示 Future 的结果已被取走，不会再产生结果。
	ErrConsumed = errors.New("ahkc: future result already consumed")
	// ErrInvalidWeight 表示 weigher 对 loader 结果返回了非正权重。
	ErrInvalidWeight = errors.New("ahkc: weigher returned a non-positive weight")
	// ErrNoWeigher 表示值类型无法默认计费（含指针/引用的类型），New 时必须提供 WithWeigher。
	ErrNoWeigher = errors.New("ahkc: value type has no default weigher; supply WithWeigher")
	ErrExhausted = errors.New("ahkc: identity or clock range exhausted; cache stopped")

	// ErrKeyTooLarge 表示 key 超过 maxKeyBytes。
	ErrKeyTooLarge = errors.New("ahkc: key exceeds maxKeyBytes")
	// ErrNoSuchNamespace 表示命名空间未注册。
	ErrNoSuchNamespace = errors.New("ahkc: no such namespace")
	ErrNamespaceExists = errors.New("ahkc: namespace already registered")
)

var (
	errContextCanceled = fmt.Errorf("%w: %w", ErrCanceled, context.Canceled)
	errContextTimeout  = fmt.Errorf("%w: %w", ErrTimeout, context.DeadlineExceeded)
)

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

// 已完成的确认优先于取消，重复 Wait 的结果不受 select 随机选择影响。
func waitContext(ctx context.Context, ready <-chan struct{}) error {
	if ctx == nil {
		return ErrNilContext
	}
	select {
	case <-ready:
		return nil
	default:
	}
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return contextError(ctx.Err())
	}
}

// contextError 把调用方 ctx 的错误并入库的错误体系：既能 errors.Is 到 ErrTimeout / ErrCanceled，
// 也保留 context.DeadlineExceeded / context.Canceled，调用方无需二选一。
func contextError(err error) error {
	switch err {
	case nil:
		return nil
	case context.Canceled:
		return errContextCanceled
	case context.DeadlineExceeded:
		return errContextTimeout
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %w", ErrTimeout, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %w", ErrCanceled, err)
	}
	return err
}

// PanicError 包装 loader 的 panic：按加载契约错误终结该次加载（额度正常归还），
// 业务侧可 errors.As 取出。库本身不因宿主代码 panic 而崩溃或泄漏预算。
type PanicError struct{ Value any }

func (e *PanicError) Error() string { return fmt.Sprintf("ahkc: loader panicked: %v", e.Value) }

// fillReason 是填充候选的终态原因（含成功）。
type fillReason uint8

const (
	fillNone fillReason = iota
	fillPublished
	fillRejectedStale         // STALE：令牌 / expectedEntryId / 代际不再匹配
	fillRejectedExpired       // EXPIRED：超过 hardDeadline / handoffDeadline
	fillRejectedNotHot        // 热度未达 T_in
	fillRejectedNoEstimate    // 估值 UNAVAILABLE
	fillRejectedNonPositive   // 净收益 <= 0
	fillRejectedInfeasible    // F(Q) 为空 / 索引无安装位 / 资源不可行
	fillRejectedResourceLimit // 退休 / pending / 预算不足
	fillRejectedQueueFull     // 填充队列满，未入队
	fillRejectedWorkLimit     // 求解工作量超限
	fillRejectedClosed        // 关闭
	fillRejectedTooLarge      // 超 maxEntryBytes
	fillRejectedAllocation    // 池分配失败
	fillRejectedAlreadyTerm   // 已终结
	fillRejectedObserveOnly   // 仅统计模式：观察模式下不缓存
	fillRejectedNoCache       // loader metadata explicitly refuses caching
)

func (r fillReason) String() string {
	switch r {
	case fillPublished:
		return "PUBLISHED"
	case fillRejectedStale:
		return "STALE"
	case fillRejectedExpired:
		return "EXPIRED"
	case fillRejectedNotHot:
		return "NOT_HOT"
	case fillRejectedNoEstimate:
		return "NO_ESTIMATE"
	case fillRejectedNonPositive:
		return "NON_POSITIVE_GAIN"
	case fillRejectedInfeasible:
		return "INFEASIBLE"
	case fillRejectedResourceLimit:
		return "RESOURCE_LIMIT"
	case fillRejectedQueueFull:
		return "QUEUE_FULL"
	case fillRejectedWorkLimit:
		return "WORK_LIMIT"
	case fillRejectedClosed:
		return "CLOSED"
	case fillRejectedTooLarge:
		return "TOO_LARGE"
	case fillRejectedAllocation:
		return "ALLOCATION_FAILED"
	case fillRejectedAlreadyTerm:
		return "ALREADY_TERMINAL"
	case fillRejectedObserveOnly:
		return "OBSERVE_ONLY"
	case fillRejectedNoCache:
		return "NOT_CACHEABLE"
	default:
		return "NONE"
	}
}
