package ahkc

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type advancingReadClock struct {
	before, after    int64
	advanceAt, reads int
}

func (c *advancingReadClock) Now() int64 {
	c.reads++
	if c.reads >= c.advanceAt {
		return c.after
	}
	return c.before
}

// takeCandidate stops the manual executor at the fill handoff, before publication.
func takeCandidate(t testing.TB, env *testEnv, ns *Namespace[string], key string) (*shard[string], *fillCandidate[string], *Future[string]) {
	t.Helper()
	f, err := ns.FetchAsync(key, okLoader("new"))
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool { return env.fillQueueLen() == 1 })
	for _, s := range env.c.c.shards {
		select {
		case a := <-s.fillQueue:
			return s, a, f
		default:
		}
	}
	t.Fatal("candidate not queued")
	return nil, nil, nil
}

func TestFillExpiryAfterPublishKeepsIndexedEntry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	s, a, f := takeCandidate(t, env, env.ns, "late")
	plan, reason := env.c.c.planFill(s, a)
	if plan == nil {
		t.Fatalf("plan: %v", reason)
	}
	p, reason := env.c.c.prepareCommit(s, a, plan)
	if p == nil {
		t.Fatalf("prepare: %v", reason)
	}
	// TTL expires after the index publication, at the final validation.
	env.c.c.cfg.Runtime.Clock = &advancingReadClock{before: env.clk.Now(), after: a.e.hardDeadline, advanceAt: 3}
	env.c.c.commitFill(s, a, p)
	if p.e.indexSlot == noSlot {
		t.Fatal("expired publication must stay indexed until a sweep retires it")
	}
	if p.e.isReadable() {
		t.Fatal("expired publication is still readable")
	}
	if s.residentCount.Load() != 1 || s.residentBytes.Load() != int64(p.e.billedBytes) {
		t.Fatal("invalid indexed entry must retain resident accounting")
	}
	env.c.c.cfg.Runtime.Clock = env.clk
	// 慢扫描按流逝时间扫描：再走过一个完整周期，保证覆盖到这个条目所在的槽。
	env.clk.Set(a.e.hardDeadline + int64(env.c.c.cfg.Runtime.SweepPeriod))
	env.drive()
	if _, err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if s.residentCount.Load() != 0 || s.retiredCount.Load() != 0 {
		t.Fatal("interrupted publication did not reclaim its budgets")
	}
}

func TestFillVictimExpiresWhileFreezing(t *testing.T) {
	low := NamespaceConfig{TTL: time.Minute, Tin: 1}
	env := newTestEnv(t, low, func(c *Config) {
		c.Shards = 1
		c.MaxEntries = 2
		c.Namespaces = map[string]NamespaceConfig{
			"t":     low,
			"short": {TTL: 500 * time.Millisecond, Tin: 1},
			"high":  {TTL: time.Minute, Tin: 1},
		}
	})
	short, _ := env.c.Namespace("short")
	high, _ := env.c.Namespace("high")
	f, err := short.FetchAsync("expiring", okLoader("old"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := env.fetch("live", okLoader("live")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // 候选比受害者更热，净收益才为正
		high.Get("candidate")
	}
	s, a, future := takeCandidate(t, env, high, "candidate")
	env.c.c.cfg.Runtime.Clock = &advancingReadClock{before: env.clk.Now(), after: env.clk.Now() + int64(500*time.Millisecond), advanceAt: 2}
	plan, reason := env.c.c.planFill(s, a)
	if plan == nil {
		t.Fatalf("plan: %v", reason)
	}
	if len(plan.removals) != 1 || plan.removals[0].keyString() != "live" {
		t.Fatal("selected identities no longer match the frozen victim set")
	}
	env.c.c.cfg.Runtime.Clock = env.clk
	env.c.c.rejectCandidate(a, fillRejectedStale)
	env.drive()
	if _, err := future.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 填充决策路径（fill.go / solver.go）的拒绝原因覆盖。
// 与 cache_test.go 的正/负路径测试正交：本文件聚焦 PLAN_FILL 的
// 六类确定性拒绝（INFEASIBLE / RESOURCE_LIMIT / EXPIRED / CLOSED /
// NON_POSITIVE_GAIN 见 evict_test.go）。

// TestOverloadPendingBytesBudget 覆盖候选准备阶段的交接预算耗尽：
// 第二个候选 pending 预留失败 → RESOURCE_LIMIT，不缓存但业务交付；
// 第一个候选发布后预算归还（I2）。
func TestOverloadPendingBytesBudget(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1},
		func(c *Config) {
			c.Limits.PendingBytes = 2
			c.MaxEntryBytes = 2
		})
	env.c.c.weigh = func(key string, value string) int64 { return 2 }
	f1, err := env.ns.FetchAsync("k1", okLoader("v1"))
	if err != nil {
		t.Fatal(err)
	}
	f2, err := env.ns.FetchAsync("k2", okLoader("v2"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if v, err := f1.Wait(context.Background()); err != nil || v != "v1" {
		t.Fatalf("k1 must deliver: %v %v", v, err)
	}
	if v, err := f2.Wait(context.Background()); err != nil || v != "v2" {
		t.Fatalf("k2 must deliver despite budget exhaustion: %v %v", v, err)
	}
	st := env.c.Stats()
	if st.FillRejected["RESOURCE_LIMIT"] == 0 {
		t.Fatalf("expected RESOURCE_LIMIT rejection: %v", st.FillRejected)
	}
	if _, ok, _ := env.ns.Get("k1"); !ok {
		t.Fatal("k1 must be published (first candidate held the budget)")
	}
	if _, ok, _ := env.ns.Get("k2"); ok {
		t.Fatal("k2 must not be cached")
	}
	if av := env.c.c.pendingBytes.available(); av != env.c.c.pendingBytes.limit {
		t.Fatalf("pending budget leaked: avail=%d limit=%d", av, env.c.c.pendingBytes.limit)
	}
}

// TestFillCandidateHandoffExpiry 覆盖交接期限：候选滞留队列期间超过
// handoffDeadline → EXPIRED 终结，不得发布。
func TestFillCandidateHandoffExpiry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		func(c *Config) { c.Limits.HandoffTimeout = 2 * time.Second })
	keys := env.sameShardKeys(2)
	a, k := keys[0], keys[1]
	// a 发布，耗尽分片唯一填充令牌。
	env.fetch(a, okLoader("va"))
	env.fetch(a, okLoader("va"))
	env.drive()
	if _, ok, _ := env.ns.Get(a); !ok {
		t.Fatal("a must be published")
	}
	// k 热度 2 达标，候选入队但被令牌定住（COMPLETED_PENDING）。
	if _, err := env.fetch(k, okLoader("v0")); err != nil { // 热度 1：NOT_HOT
		t.Fatal(err)
	}
	fut, err := env.ns.FetchAsync(k, okLoader("v1"))
	if err != nil {
		t.Fatal(err)
	}
	env.drainUntil(func() bool {
		return env.fillQueueLen() == 1 && env.flightPhaseOf(k) == phaseCompletedPending
	})
	// 推进超过 HandoffTimeout(2s)：候选过期（φ=0 前桶热度仍达标，排除 NOT_HOT 干扰）。
	env.clk.Advance(3 * time.Second)
	env.drive()
	if v, err := fut.Wait(context.Background()); err != nil || v != "v1" {
		t.Fatalf("business result must deliver: %v %v", v, err)
	}
	st := env.c.Stats()
	if st.FillRejected["EXPIRED"] == 0 {
		t.Fatalf("expected EXPIRED rejection: %v", st.FillRejected)
	}
	if _, ok, _ := env.ns.Get(k); ok {
		t.Fatal("expired candidate must not publish")
	}
	// 候选资源全部归还。
	if av := env.c.c.pendingBytes.available(); av != env.c.c.pendingBytes.limit {
		t.Fatalf("pending budget leaked: avail=%d limit=%d", av, env.c.c.pendingBytes.limit)
	}
}

// TestCloseCancelsQueuedCandidates 覆盖 closeSweepStep 的排队候选终结：
// 关闭时滞留队列的候选以 CLOSED 终结；关闭仍可推进到 RECLAIMED。
func TestCloseCancelsQueuedCandidates(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		nil)
	keys := env.sameShardKeys(2)
	a, k := keys[0], keys[1]
	env.fetch(a, okLoader("va"))
	env.fetch(a, okLoader("va"))
	env.drive()
	if _, ok, _ := env.ns.Get(a); !ok {
		t.Fatal("a must be published")
	}
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
	// 结算等待者（I8：waiter 必须结算，否则其响应引用持有阻塞 RECLAIMED）。
	if v, err := fut.Wait(context.Background()); err != nil || v != "v1" {
		t.Fatalf("business result must deliver: %v %v", v, err)
	}
	// 关闭：候选未处理即被取消。
	closeManual(t, env.c, env.ex)
	st := env.c.Stats()
	if st.FillRejected["CLOSED"] == 0 {
		t.Fatalf("expected CLOSED rejection: %v", st.FillRejected)
	}
	if av := env.c.c.pendingBytes.available(); av != env.c.c.pendingBytes.limit {
		t.Fatalf("pending budget leaked: %d", av)
	}
}

// TTL 比 HandoffTimeout 短的命名空间照常工作：填充截止取 min(硬截止, 完成时刻 + HandoffTimeout)，
// 条目在自己的 TTL 到期时不可读，不会因为交接期限更长而多活。
func TestShortTTLBelowHandoffTimeoutStillCaches(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: 500 * time.Millisecond, Tin: 1},
		func(c *Config) { c.Limits.HandoffTimeout = 2 * time.Second })
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := env.ns.Get("k"); !ok || v != "v" {
		t.Fatalf("a short-TTL value must be cached: %q %v", v, ok)
	}
	env.clk.Advance(600 * time.Millisecond)
	if _, ok, _ := env.ns.Get("k"); ok {
		t.Fatal("value delivered after its TTL")
	}
}

// 响应预算按实际结果大小占用：默认配置（MaxResultBytes 4MiB、ResponseBytes 1GiB）下，
// 小结果的纯填充突发不得因为按最坏情况预留而被拒绝为 ErrOverloaded。
func TestFillBurstWithSmallValuesIsNotOverloaded(t *testing.T) {
	const n, callers = 40_000, 64
	c, err := NewBytes[string](Config{
		Shards: 16, MaxEntries: n, MaxBytes: n * 288, MaxEntryBytes: 512,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Hour, Tin: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	ns, _ := c.Namespace("t")
	var next, overloaded atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < callers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				_, err := ns.Fetch(context.Background(), "key-"+strconv.Itoa(i), func(context.Context, string) (string, error) { return "value", nil })
				if errors.Is(err, ErrOverloaded) {
					overloaded.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := float64(overloaded.Load()) / n; got > 0.05 {
		t.Fatalf("%.1f%% of small-value fills were rejected as overloaded (response budget reserved at the worst case?)", 100*got)
	}
	for c.Stats().PendingFills > 0 {
		time.Sleep(time.Millisecond)
	}
	if c.c.responseBytes.available() != c.c.responseBytes.limit {
		t.Fatalf("response budget not fully returned: %d of %d", c.c.responseBytes.available(), c.c.responseBytes.limit)
	}
}
