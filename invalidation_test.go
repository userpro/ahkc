package ahkc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNamespacesIsolateSameKey(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	other, err := env.c.RegisterNamespace("other", NamespaceConfig{TTL: time.Minute, Tin: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.fetch("shared", okLoader("first")); err != nil {
		t.Fatal(err)
	}
	if value, hit, err := other.Get("shared"); err != nil || hit {
		t.Fatalf("namespace leaked another value: %q, hit=%v, err=%v", value, hit, err)
	}
	f, err := other.FetchAsync("shared", okLoader("second"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if value, err := f.Wait(context.Background()); err != nil || value != "second" {
		t.Fatalf("load in other namespace: %q, %v", value, err)
	}
	h, err := other.InvalidateAsync("shared")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if value, hit, err := env.ns.Get("shared"); err != nil || !hit || value != "first" {
		t.Fatalf("invalidation crossed namespace: %q, hit=%v, err=%v", value, hit, err)
	}
}

// TestInvalidateKeyKillsInFlightFill 覆盖 I4/防复活：填充交接途中单 key 失效，
// fillFence 递增使在途候选 STALE 终结；已交付的业务结果不受影响；旧值不得复活。
// 用每分片 1 个填充令牌定住在途候选（先由同分片 key 消耗令牌），保证失效严格先于候选处理。
func TestInvalidateKeyKillsInFlightFill(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		nil)
	keys := env.sameShardKeys(2)
	a, k := keys[0], keys[1]
	// a 发布，耗尽分片唯一填充令牌。
	env.fetch(a, okLoader("va"))
	env.fetch(a, okLoader("va"))
	env.drive()
	if _, ok, _ := env.ns.Get(a); !ok {
		t.Fatal("a must be published")
	}
	if _, err := env.fetch(k, okLoader("v0")); err != nil { // 热度 1
		t.Fatal(err)
	}
	fut, err := env.ns.FetchAsync(k, okLoader("v1")) // 热度 2，候选入队但被令牌定住
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool {
		return env.fillQueueLen() == 1 && env.flightPhaseOf(k) == phaseCompletedPending
	})
	// 失效在候选提交前生效：fence 递增。
	h, err := env.ns.InvalidateAsync(k)
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool { return env.c.Stats().KeyInvalidations == 1 })
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 业务结果照常交付（响应成功不代表已缓存）。
	if v, err := fut.Wait(context.Background()); err != nil || v != "v1" {
		t.Fatalf("in-flight business result must still be delivered: %v %v", v, err)
	}
	if _, ok, _ := env.ns.Get(k); ok {
		t.Fatal("stale candidate must not resurrect the key")
	}
	// 推进 1s 补足令牌：候选以 STALE 终结（泵取消或 fillStep 复核，φ=0 热度仍达标）。
	env.clk.Advance(time.Second)
	for i := 0; i < 100 && env.c.Stats().FillRejected["STALE"] == 0; i++ {
		env.c.MaintenanceStep()
		env.ex.Drain(10)
	}
	if env.c.Stats().FillRejected["STALE"] == 0 {
		t.Fatalf("expected STALE rejection: %v", env.c.Stats().FillRejected)
	}
	if _, ok, _ := env.ns.Get(k); ok {
		t.Fatal("stale candidate must not publish")
	}
	// 失效确认后新加载重新进入：t=2s 两次请求（同桶热度 2），候选被令牌定住；
	// t=3s 令牌补足且 H=前桶 2 达标 → 发布。
	f1, err := env.ns.FetchAsync(k, okLoader("v2"))
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool {
		return env.fillQueueLen() == 1 && env.flightPhaseOf(k) == phaseCompletedPending
	})
	f2, err := env.ns.FetchAsync(k, okLoader("v2")) // 复用 COMPLETED_PENDING（记录热度）
	if err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(time.Second)
	env.drive()
	for _, f := range []*Future[string]{f1, f2} {
		if v, err := f.Wait(context.Background()); err != nil || v != "v2" {
			t.Fatalf("reload waiter: %v %v", v, err)
		}
	}
	if v, ok, _ := env.ns.Get(k); !ok || v != "v2" {
		t.Fatalf("re-cache after invalidation: %v %v", v, ok)
	}
}

// TestNamespaceInvalidateMidFillStale 覆盖命名空间失效与填充交错：
// 代际推进使在途候选 STALE；新代际下的加载正常发布。
// 用每分片 1 个填充令牌定住在途候选（先由同分片 key 消耗令牌），保证失效严格先于候选处理。
func TestNamespaceInvalidateMidFillStale(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		nil)
	k := env.sameShardKeys(1)[0]
	release := env.holdMaintenance(env.c.c.shards[0]) // 候选停在队列，保证失效先于候选处理
	// k：热度 1；第二次加载候选入队但被定住（COMPLETED_PENDING）。
	if _, err := env.fetch(k, okLoader("v0")); err != nil {
		t.Fatal(err)
	}
	fut, err := env.ns.FetchAsync(k, okLoader("v1"))
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool {
		return env.fillQueueLen() == 1 && env.flightPhaseOf(k) == phaseCompletedPending
	})
	// 命名空间失效：逐任务驱动至代际推进（维护被阻止，候选不会被抢先处理）。
	errCh := make(chan error, 1)
	go func() { errCh <- env.c.InvalidateNamespace(context.Background(), "t") }()
	env.drainUntil(func() bool { return env.c.c.nsByName["t"].gen.Load() == 2 })
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if v, err := fut.Wait(context.Background()); err != nil || v != "v1" {
		t.Fatalf("in-flight business result must still be delivered: %v %v", v, err)
	}
	if _, ok, _ := env.ns.Get(k); ok {
		t.Fatal("old-generation candidate must not be visible")
	}
	// 恢复维护，fillStep 处理候选 → 代际不符 STALE。
	env.clk.Advance(time.Second)
	release()
	for i := 0; i < 100 && env.c.Stats().FillRejected["STALE"] == 0; i++ {
		env.c.MaintenanceStep()
		env.ex.Drain(10)
	}
	if env.c.Stats().FillRejected["STALE"] == 0 {
		t.Fatalf("expected STALE rejection: %v", env.c.Stats().FillRejected)
	}
	if _, ok, _ := env.ns.Get(k); ok {
		t.Fatal("stale candidate must not publish")
	}
	// 新代际下重新加载并发布（t=3s：当前桶 1 + 前桶 0；两次 fetch 后热度达标；令牌已补足）。
	env.clk.Advance(time.Second)
	if _, err := env.fetch(k, okLoader("v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := env.fetch(k, okLoader("v2")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if v, ok, _ := env.ns.Get(k); !ok || v != "v2" {
		t.Fatalf("new generation must publish: %v %v", v, ok)
	}
}

// TestInvalidateWaitContextCancel 覆盖完成凭证 Wait 的 ctx 取消分支：
// 超时返回 ctx 错误；凭证不失效，维护处理后同一凭证可得确认。
func TestInvalidateWaitContextCancel(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
	h, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	// 不驱动维护：确认尚未发生。
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait before confirmation: %v", err)
	}
	// 驱动后同一凭证完成确认。
	env.drive()
	if err := h.Wait(context.Background()); err != nil {
		t.Fatalf("confirmed: %v", err)
	}
	if env.c.Stats().KeyInvalidations != 1 {
		t.Fatalf("key invalidations: %d", env.c.Stats().KeyInvalidations)
	}
}

// TestInvalidateConfirmSemantics 覆盖失效确认语义：失效确认表示逻辑失效已完成——
// 确认后新查询不得取得旧可读版本，即便物理回收尚未完成。
func TestInvalidateConfirmSemantics(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
	for i := 0; i < 2; i++ {
		env.fetch("k", okLoader("v1"))
	}
	env.drive()
	if _, ok, _ := env.ns.Get("k"); !ok {
		t.Fatal("expected cached")
	}
	h, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive() // 手动执行器：驱动维护处理控制操作
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 确认后逻辑上已不可见。
	if _, ok, _ := env.ns.Get("k"); ok {
		t.Fatal("confirmed invalidation must hide the entry")
	}
	if env.c.Stats().KeyInvalidations != 1 {
		t.Fatalf("key invalidations: %d", env.c.Stats().KeyInvalidations)
	}
}
