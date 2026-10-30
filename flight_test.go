package ahkc

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestFutureGuardZeroAlloc(t *testing.T) {
	f := newFuture[string](0, nil)
	defer f.Cancel()
	if a := testing.AllocsPerRun(1000, func() { f.guard(); f.unguard() }); a != 0 {
		t.Fatalf("finalizer registration allocated %.0f objects", a)
	}
}

// drainUntil 逐个运行排队任务直到条件满足（用于停在“加载完成、填充未处理”的中间态）。
func (e *testEnv) drainUntil(cond func() bool) {
	e.t.Helper()
	for i := 0; i < 1000 && !cond(); i++ {
		e.ex.Drain(1)
		runtime.Gosched()
	}
	if !cond() {
		e.t.Fatal("drainUntil: condition not met")
	}
}

// holdMaintenance 阻止分片维护被调度，使已入队的填充候选停在队列中；
// 返回的 release 恢复调度并触发一轮维护。
func (e *testEnv) holdMaintenance(s *shard[string]) (release func()) {
	s.ownerState.Store(ownerRunning)
	return func() {
		s.ownerState.Store(ownerIdle)
		e.c.c.signalMaintenance(s)
	}
}

// waitReal 轮询等待真实时间条件（并发 drain goroutine 场景）。
func waitReal(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting: %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFlightTimeoutThenLateSourceCleanup 覆盖 A.6 TIMED_OUT_IO_RUNNING：
// 等待者按时限终结（ErrTimeout）；底层 I/O 不取消，迟到结果真实结束后清理孤儿记录并归还全部额度。
func TestFlightTimeoutThenLateSourceCleanup(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "late", nil
	}
	fut, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	// 后台驱动执行器：runLoader 阻塞在 loader 内，模拟慢源。
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })

	// 推进超过 LoadTimeout(10s)：等待者终结，flight 进入孤儿态，I/O 额度仍被占用。
	env.clk.Advance(11 * time.Second)
	env.drive()
	if _, err := fut.Wait(context.Background()); !errors.Is(err, ErrTimeout) {
		t.Fatalf("waiter must get ErrTimeout, got %v", err)
	}
	st := env.c.Stats()
	if st.LoadTimeout != 1 || st.OrphanLoads != 1 {
		t.Fatalf("timeout/orphan metrics: %+v", st)
	}
	if st.IOInFlight != 1 {
		t.Fatalf("underlying I/O must still be accounted, in-flight=%d", st.IOInFlight)
	}

	// 迟到完成：结果不再填充，但额度必须归还、注册槽必须摘除（防泄漏回归）。
	close(release)
	waitReal(t, "loader finished", func() bool { return env.c.Stats().IOInFlight == 0 })
	env.drive()
	st = env.c.Stats()
	if st.IOInFlight != 0 {
		t.Fatalf("I/O permit leaked: %d", st.IOInFlight)
	}
	if av := env.c.c.responseBytes.available(); av != env.c.c.responseBytes.limit {
		t.Fatalf("response budget leaked: avail=%d limit=%d", av, env.c.c.responseBytes.limit)
	}
	if av := env.c.c.waiterBudget.available(); av != int64(env.c.c.cfg.Limits.MaxWaitersTotal) {
		t.Fatalf("waiter budget leaked: avail=%d", av)
	}
	// 同身份新加载可立即注册（槽位已释放）。
	v, err := env.fetch("k", okLoader("v2"))
	if err != nil || v != "v2" {
		t.Fatalf("reload after orphan cleanup: %v %v", v, err)
	}
	env.drive()
}

// TestWaiterSettleOnce 覆盖 I8：waiter 只结算一次；Cancel 幂等且只归还一次额度。
func TestWaiterSettleOnce(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "v", nil
	}
	f1, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })
	f2, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	f3, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	total := int64(env.c.c.cfg.Limits.MaxWaitersTotal)
	if av := env.c.c.waiterBudget.available(); av != total-3 {
		t.Fatalf("waiter budget after 3 waiters: %d", av)
	}
	// 取消 f2（两次，幂等）；再 Take 得 CANCELLED；不重复归还。
	f2.Cancel()
	f2.Cancel()
	if av := env.c.c.waiterBudget.available(); av != total-2 {
		t.Fatalf("cancel must return receipt once: %d", av)
	}
	if _, ok, err := f2.Take(); !ok || !errors.Is(err, ErrCanceled) {
		t.Fatalf("cancelled waiter Take: %v %v", err, ok)
	}
	close(release)
	env.drive()
	for _, f := range []*Future[string]{f1, f3} {
		v, err := f.Wait(context.Background())
		if err != nil || v != "v" {
			t.Fatalf("waiter: %v %v", v, err)
		}
	}
	// Take 只成功一次。
	if _, ready, err := f1.Take(); !ready || !errors.Is(err, ErrConsumed) {
		t.Fatal("second Take must report its terminal state", err)
	}
	if av := env.c.c.waiterBudget.available(); av != total {
		t.Fatalf("waiter budget not fully returned: %d", av)
	}
	env.drive()
}

// fillQueueLen 返回全部分片填充队列积压（观测用）。
func (e *testEnv) fillQueueLen() int {
	n := 0
	for _, s := range e.c.c.shards {
		n += len(s.fillQueue)
	}
	return n
}

// flightPhaseOf 返回 key 对应 flight 的相位；-1 表示无注册 flight。
func (e *testEnv) flightPhaseOf(key string) int32 {
	kh := e.ns.ns.hashKey(key)
	s := e.c.c.shardOf(kh)
	stripe := &s.flightStripes[int(mix64(kh)%uint64(len(s.flightStripes)))]
	for _, f := range stripe.slots {
		if f != nil && f.key == key {
			return f.getPhase()
		}
	}
	return -1
}

// sameShardKeys 生成 n 个落在同一分片的 key（分片相关配额测试用）。
func (e *testEnv) sameShardKeys(n int) []string {
	var keys []string
	target := e.c.c.shards[0]
	for i := 0; len(keys) < n; i++ {
		k := fmt.Sprintf("sk%d", i)
		if e.c.c.shardOf(e.ns.ns.hashKey(k)) == target {
			keys = append(keys, k)
		}
	}
	return keys
}

// TestCompletedPendingReuse 覆盖 COMPLETED_PENDING 复用：填充交接期间，
// 同身份请求直接复用已完成结果，不发起新加载。
func TestCompletedPendingReuse(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
	var loads atomic.Int32
	loader := func(ctx context.Context, key string) (string, error) {
		loads.Add(1)
		return "v", nil
	}
	if _, err := env.fetch("k", loader); err != nil { // 热度 1，候选 NOT_HOT 拒绝
		t.Fatal(err)
	}
	fut2, err := env.ns.FetchAsync("k", loader) // 热度 2，候选被接受但暂不跑维护
	if err != nil {
		t.Fatal(err)
	}
	// 推进到候选入队且 flight 进入 COMPLETED_PENDING（泵分多轮，按相位判定）。
	env.drainUntil(func() bool {
		return loads.Load() == 2 && env.fillQueueLen() == 1 && env.flightPhaseOf("k") == phaseCompletedPending
	})
	fut3, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := fut3.Wait(context.Background()); err != nil || v != "v" {
		t.Fatalf("reuse waiter: %v %v", v, err)
	}
	if loads.Load() != 2 {
		t.Fatalf("reuse must not start a new load: %d", loads.Load())
	}
	if env.c.Stats().LoadMerged != 1 {
		t.Fatalf("merged count: %d", env.c.Stats().LoadMerged)
	}
	if v, err := fut2.Wait(context.Background()); err != nil || v != "v" {
		t.Fatalf("leader waiter: %v %v", v, err)
	}
	env.drive()
	if v, ok, _ := env.ns.Get("k"); !ok || v != "v" {
		t.Fatalf("published: %v %v", v, ok)
	}
}

// TestOverloadIOPermits 覆盖过载行为：真实 I/O 并发额度耗尽时新加载报 ErrOverloaded，
// 不排队、不阻塞，业务侧可退避。
func TestOverloadIOPermits(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000},
		func(c *Config) { c.Limits.MaxConcurrentLoads = 1 })
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "v", nil
	}
	f1, err := env.ns.FetchAsync("k1", loader)
	if err != nil {
		t.Fatal(err)
	}
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })
	if _, err := env.ns.FetchAsync("k2", loader); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("expected ErrOverloaded, got %v", err)
	}
	if env.c.Stats().Overloaded != 1 {
		t.Fatalf("overloaded metric: %d", env.c.Stats().Overloaded)
	}
	close(release)
	env.drive()
	if _, err := f1.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := env.c.Stats(); st.IOInFlight != 0 {
		t.Fatalf("I/O permit leaked: %d", st.IOInFlight)
	}
}

// TestOverloadWaiterBudget：全局 waiter 额度耗尽时合并加入失败，报 ErrOverloaded。
func TestOverloadWaiterBudget(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000},
		func(c *Config) { c.Limits.MaxWaitersTotal = 2; c.Limits.MaxWaitersPerKey = 8 })
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "v", nil
	}
	f1, err := env.ns.FetchAsync("k", loader) // leader 占 1
	if err != nil {
		t.Fatal(err)
	}
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })
	if _, err := env.ns.FetchAsync("k", loader); err != nil { // 合并占第 2 个
		t.Fatal(err)
	}
	if _, err := env.ns.FetchAsync("k", loader); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("waiter budget exhausted must yield ErrOverloaded, got %v", err)
	}
	close(release)
	env.drive()
	if _, err := f1.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestFillQueueFullStillDelivers 覆盖 A.6 契约：填充队列满只影响“是否缓存”，
// 业务结果照常交付；队列腾出后候选仍可发布。
func TestFillQueueFullStillDelivers(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		func(c *Config) { c.Limits.FillQueueSize = 1 })
	var loads atomic.Int32
	loader := func(ctx context.Context, key string) (string, error) {
		loads.Add(1)
		return "v-" + key, nil
	}
	// 两个 key 落在同一分片：共享填充队列；阻止该分片维护，让候选滞留队列。
	keys := env.sameShardKeys(2)
	b, c := keys[0], keys[1]
	release := env.holdMaintenance(env.c.c.shards[0])
	env.fetch(b, loader) // 热度 1，NOT_HOT 拒绝，不入队
	fb, err := env.ns.FetchAsync(b, loader)
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool {
		return env.fillQueueLen() == 1 && env.flightPhaseOf(b) == phaseCompletedPending
	})
	if v, err := fb.Wait(context.Background()); err != nil || v != "v-"+b {
		t.Fatalf("b waiter: %v %v", v, err)
	}
	if _, ok, _ := env.ns.Get(b); ok {
		t.Fatal("b must still be queued (maintenance held)")
	}
	// c：热度达标后候选入队失败（QUEUE_FULL），但业务结果照常交付。
	fc1, err := env.ns.FetchAsync(c, loader) // 热度 1
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool { return env.flightPhaseOf(c) == -1 }) // NOT_HOT 终结并摘除
	if _, err := fc1.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	fc2, err := env.ns.FetchAsync(c, loader) // 热度 2，候选入队失败
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool { return env.flightPhaseOf(c) == -1 })
	v, err := fc2.Wait(context.Background())
	if err != nil || v != "v-"+c {
		t.Fatalf("business result must be delivered despite full fill queue: %v %v", v, err)
	}
	st := env.c.Stats()
	if st.FillRejected["QUEUE_FULL"] == 0 {
		t.Fatalf("expected QUEUE_FULL rejection: %v", st.FillRejected)
	}
	if _, ok, _ := env.ns.Get(c); ok {
		t.Fatal("c must not be cached when its candidate was rejected")
	}
	// 恢复维护后 b 发布（未超 HandoffTimeout=2s）。
	release()
	env.drive()
	if _, ok, _ := env.ns.Get(b); !ok {
		t.Fatal("b must publish after maintenance resumes")
	}
}

// TestLoaderPanicRecovery 覆盖加载契约：loader panic 按错误终结（PanicError），
// 额度不泄漏，实例继续可用。
func TestLoaderPanicRecovery(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	panicLoader := func(ctx context.Context, key string) (string, error) { panic("boom") }
	_, err := env.fetch("p", panicLoader)
	if err == nil {
		t.Fatal("expected error from panicking loader")
	}
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Value != "boom" {
		t.Fatalf("expected PanicError{boom}, got %T %v", err, err)
	}
	if pe.Error() == "" {
		t.Fatal("PanicError must render a message")
	}
	env.drive()
	st := env.c.Stats()
	if st.LoadErrors != 1 {
		t.Fatalf("load errors: %d", st.LoadErrors)
	}
	if st.IOInFlight != 0 {
		t.Fatalf("I/O permit leaked by panic: %d", st.IOInFlight)
	}
	if av := env.c.c.responseBytes.available(); av != env.c.c.responseBytes.limit {
		t.Fatalf("response budget leaked by panic: %d", av)
	}
	// 实例继续可用。
	if v, err := env.fetch("ok", okLoader("v")); err != nil || v != "v" {
		t.Fatalf("cache must remain usable: %v %v", v, err)
	}
	env.drive()
}

// TestCloseBlockedByOrphanIO 覆盖 A.9 三阶段：停止受理 ≠ 工作终结 ≠ 回收完成；
// 未结束的真实 I/O 阻塞 RECLAIMED，Close 报告 ctx 错误但不强制结束 I/O。
func TestCloseBlockedByOrphanIO(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "v", nil
	}
	fut, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })

	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		closeDone <- env.c.Close(ctx)
	}()
	waitReal(t, "close stopped admission", func() bool { return env.c.c.state.Load() != cacheOpen })
	// 驱动关闭控制（停止受理），但 I/O 未结束 → 不达 RECLAIMED。
	waitReal(t, "close waiter ready", func() bool {
		env.ex.Drain(100)
		env.c.MaintenanceStep()
		return fut.state.Load() == waiterReadyInline
	})
	if err := <-closeDone; err == nil {
		t.Fatal("Close must report ctx error while I/O outstanding")
	}
	progress := env.c.CloseProgress()
	stopped, drained, reclaimed := progress.Stopped, progress.Drained, progress.Reclaimed
	if !stopped || drained || reclaimed {
		t.Fatalf("CloseState during orphan I/O: %v %v %v", stopped, drained, reclaimed)
	}
	// 等待者以 ErrClosed 终结（关闭不等待慢源）。
	if _, err := fut.Wait(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("waiter must get ErrClosed, got %v", err)
	}
	// 迟到完成后回收推进至 RECLAIMED。
	close(release)
	waitReal(t, "reclaimed", func() bool {
		env.ex.Drain(100)
		env.c.MaintenanceStep()
		r := env.c.CloseProgress().Reclaimed
		return r
	})
	if err := env.c.WaitReclaimed(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := env.c.Stats()
	if st.IOInFlight != 0 || st.ResponseBytesAvail != env.c.c.responseBytes.limit {
		t.Fatalf("budgets not conserved at RECLAIMED: %+v", st)
	}
}

// TestPumpGateFailureDoesNotLoseSignal（丢失唤醒回归）：泵在持有 completionOwner
// 后若无法取得条带 gate，不得清除 pumpNeeded 信号——否则 loader 完成的通知
// 永久丢失（等待者死锁 / flight 槽泄漏）。确定性构造该交错：持有 gate 后
// 运行 loader，释放 gate 后泵必须完成通知。
func TestPumpGateFailureDoesNotLoseSignal(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	fut, err := env.ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	// runLoader 已排队未运行；测试持有目标条带 gate，模拟泵竞争窗口。
	kh := env.ns.ns.hashKey("k")
	s := env.c.c.shardOf(kh)
	stripe := &s.flightStripes[int(mix64(kh)%uint64(len(s.flightStripes)))]
	if !stripe.tryLock() {
		t.Fatal("hold gate failed")
	}
	// 运行 loader：finishSource 的内联泵与重试泵均因 gate 被占用而无法进入临界区。
	for i := 0; i < 50; i++ {
		env.ex.Drain(10)
	}
	stripe.unlock()
	// Unlock must schedule the waiter without a periodic scan or clock advance.
	for i := 0; i < 20; i++ {
		env.ex.Drain(10)
		if v, ok, err := fut.Take(); ok {
			if err != nil || v != "v" {
				t.Fatalf("completion: %q %v", v, err)
			}
			return
		}
	}
	t.Fatal("gate unlock lost the completion wakeup")
}

// TestFutureWaitContextCancel 覆盖 Wait 的 ctx 取消分支：取消只解除自己的
// 等待（底层 I/O 不取消），waiter 槽位立即归还；后续 Wait 得 ErrCanceled。
func TestFutureWaitContextCancel(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	release := make(chan struct{})
	loader := func(ctx context.Context, key string) (string, error) {
		<-release
		return "v", nil
	}
	fut, err := env.ns.FetchAsync("k", loader)
	if err != nil {
		t.Fatal(err)
	}
	go env.ex.Drain(100)
	waitReal(t, "loader started", func() bool { return env.c.Stats().Loads == 1 })
	if d := fut.Deadline(); d <= 0 {
		t.Fatalf("deadline must be positive: %d", d)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := fut.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled Wait: %v", err)
	}
	// ctx 取消不取消底层 I/O。
	if st := env.c.Stats(); st.IOInFlight != 1 {
		t.Fatalf("underlying I/O must continue: %d", st.IOInFlight)
	}
	// 取消后的 waiter：终态 ErrCanceled。
	if _, err := fut.Wait(context.Background()); !errors.Is(err, ErrCanceled) {
		t.Fatalf("settled-after-cancel Wait: %v", err)
	}
	close(release)
	env.drive()
	total := int64(env.c.c.cfg.Limits.MaxWaitersTotal)
	if av := env.c.c.waiterBudget.available(); av != total {
		t.Fatalf("waiter budget not returned after cancel+load: %d", av)
	}
	if av := env.c.c.responseBytes.available(); av != env.c.c.responseBytes.limit {
		t.Fatalf("response budget leaked: %d", av)
	}
}

// TestFutureCancelAfterReady 覆盖 Cancel 的就绪态分支：
// waiterReady（有载荷）与 waiterReadyInline（有界错误）下取消，
// 引用与槽位只归还一次，Take 得 ErrCanceled。
func TestFutureCancelAfterReady(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
	// 成功结果就绪后取消：delivery 引用释放、响应预算归还。
	fut, err := env.ns.FetchAsync("ok", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	fut.Cancel()
	if _, ok, err := fut.Take(); !ok || !errors.Is(err, ErrCanceled) {
		t.Fatalf("Take after cancel-ready: %v %v", err, ok)
	}
	// 错误结果（readyInline）就绪后取消。
	boom := fmt.Errorf("boom")
	fut2, err := env.ns.FetchAsync("err", func(ctx context.Context, key string) (string, error) {
		return "", boom
	})
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	fut2.Cancel()
	if _, ok, err := fut2.Take(); !ok || !errors.Is(err, ErrCanceled) {
		t.Fatalf("Take after cancel-ready-inline: %v %v", err, ok)
	}
	env.drive()
	if av := env.c.c.responseBytes.available(); av != env.c.c.responseBytes.limit {
		t.Fatalf("response budget leaked by canceled waiters: %d", av)
	}
	total := int64(env.c.c.cfg.Limits.MaxWaitersTotal)
	if av := env.c.c.waiterBudget.available(); av != total {
		t.Fatalf("waiter budget leaked by canceled waiters: %d", av)
	}
}
