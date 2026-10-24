package ahkc

import (
	"context"
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

func TestModelFreezeReusesStorageWithoutRetainingModels(t *testing.T) {
	e := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) { c.SampleP = 1e-12 })
	defer closeManual(t, e.c, e.ex)
	m := newCostTable(t, CostTableConfig{Clock: e.clk, Classes: 4, MergeFactor: 1, FallbackCost: 1, DisableAutoObserve: true})
	if err := e.ns.SetModel(m); err != nil {
		t.Fatal(err)
	}
	s, a, f := takeCandidate(t, e, e.ns, "key")
	defer e.c.c.rejectCandidate(a, fillRejectedNoCache)
	if _, err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, old, _, reason := e.c.c.freezeInput(s, a, nil, nil)
	if reason != fillNone {
		t.Fatal(reason)
	}
	m.Observe(0, 2)
	if _, next, _, reason := e.c.c.freezeInput(s, a, nil, nil); reason != fillNone || next.G <= old.G {
		t.Fatal("reused snapshot missed new observations", reason)
	}
	if s.fs.tables[0].cfg != nil {
		t.Fatal("scratch retained the model")
	}
	if a := testing.AllocsPerRun(1000, func() {
		if snap, _, _, why := e.c.c.freezeInput(s, a, nil, nil); snap == nil {
			t.Fatal(why)
		}
	}); a != 0 {
		t.Fatalf("steady model freeze allocated %.0f objects", a)
	}
}

func TestFillExpiryAfterPublishKeepsIndexedEntry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
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
	env.c.c.cfg.Clock = &advancingReadClock{before: env.clk.Now(), after: a.e.hardDeadline, advanceAt: 4}
	env.c.c.commitFill(s, a, p)
	if a.e.inPool {
		t.Fatal("indexed entry was returned to the pool before retirement")
	}
	if a.e.readable.Load() {
		t.Fatal("expired publication is still readable")
	}
	if s.residentCount.Load() != 1 || s.residentBytes.Load() != a.e.billedBytes {
		t.Fatal("invalid indexed entry must retain resident accounting")
	}
	env.c.c.cfg.Clock = env.clk
	env.clk.Set(a.e.hardDeadline)
	env.drive()
	if _, err := f.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if s.residentCount.Load() != 0 || env.c.c.retiredBytes.available() != env.c.c.retiredBytes.limit {
		t.Fatal("interrupted publication did not reclaim its budgets")
	}
}

func TestFillVictimExpiresWhileFreezing(t *testing.T) {
	low := NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: &Prior{Lambda: 1}}
	env := newTestEnv(t, low, func(c *Config) {
		c.ShardCount = 1
		c.MaxEntries = 2
		c.Namespaces = map[string]NamespaceConfig{
			"t":     low,
			"short": {TTL: 500 * time.Millisecond, Tin: 1, Prior: &Prior{Lambda: 1}},
			"high":  {TTL: time.Minute, Tin: 1, Prior: &Prior{Lambda: 1000}},
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
	s, a, future := takeCandidate(t, env, high, "candidate")
	env.c.c.cfg.Clock = &advancingReadClock{before: env.clk.Now(), after: env.clk.Now() + int64(500*time.Millisecond), advanceAt: 2}
	plan, reason := env.c.c.planFill(s, a)
	if plan == nil {
		t.Fatalf("plan: %v", reason)
	}
	if len(plan.removals) != 1 || plan.removals[0].key != "live" {
		t.Fatal("selected identities no longer match the frozen victim set")
	}
	env.c.c.cfg.Clock = env.clk
	env.c.c.rejectCandidate(a, fillRejectedStale)
	env.drive()
	if _, err := future.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 填充决策路径（fill.go / solver.go）的拒绝原因覆盖。
// 与 cache_test.go 的正/负路径测试正交：本文件聚焦 PLAN_FILL 的
// 六类确定性拒绝（INFEASIBLE / RESOURCE_LIMIT / EXPIRED / CLOSED /
// NO_ESTIMATE 见 estimate_test.go / NON_POSITIVE_GAIN）。

// constantModel 是测试用固定输出成本模型：Lookup 恒定返回 (r,c,h,k,K)。
type constantModel struct{ r, c, h, k, K float64 }

func (constantModel) Version() uint64 { return 1 }
func (m constantModel) Lookup(float64, uint32) (float64, float64, float64, float64, float64, bool) {
	return m.r, m.c, m.h, m.k, m.K, true
}

// TestReplacementInfeasibleWhenRetiredBudgetShort 覆盖求解器可行性核算（A.5 FEASIBLE）：
// 容量缺口需要移除两个受害者，但退休记录槽只有一个 → INFEASIBLE 整次拒绝，
// 受害者保留、业务结果照常交付、退休预算无泄漏。
func TestReplacementInfeasibleWhenRetiredBudgetShort(t *testing.T) {
	low := NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: &Prior{Lambda: 1}}
	env := newTestEnv(t, low, func(c *Config) {
		c.ShardCount = 1
		c.MaxEntries = 2       // 条数额度 2：第三个候选必须移除 2 条
		c.MaxResidentBytes = 2 // 字节额度 2
		c.MaxEntryBytes = 2
		c.RetiredBytes = 2
		c.RetiredRecords = 1 // 只能登记一条退休 → 双受害者不可行
		c.Namespaces = map[string]NamespaceConfig{
			"t":    low,                                                     // env.ns：k1/k2 所在命名空间
			"high": {TTL: time.Minute, Tin: 1, Prior: &Prior{Lambda: 1000}}, // 高收益候选
		}
	})
	high, err := env.c.Namespace("high")
	if err != nil {
		t.Fatal(err)
	}
	// 计费：k1/k2 各 1 字节；k3 需 2 字节 → 求解必须同时移除两者。
	env.c.c.weigh = func(key string, value string) int64 {
		if key == "k3" {
			return 2
		}
		return 1
	}
	for _, k := range []string{"k1", "k2"} {
		if _, err := env.fetch(k, okLoader("v-"+k)); err != nil {
			t.Fatal(err)
		}
		env.drive()
	}
	st := env.c.Stats()
	if st.ResidentEntries != 2 || st.ResidentBytes != 2 {
		t.Fatalf("setup: entries=%d bytes=%d", st.ResidentEntries, st.ResidentBytes)
	}
	fut, err := high.FetchAsync("k3", okLoader("v3"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if v, err := fut.Wait(context.Background()); err != nil || v != "v3" {
		t.Fatalf("business result must deliver: %v %v", v, err)
	}
	st = env.c.Stats()
	if st.FillRejected["INFEASIBLE"] == 0 {
		t.Fatalf("expected INFEASIBLE rejection: %v", st.FillRejected)
	}
	if st.ResidentEntries != 2 || st.ResidentBytes != 2 {
		t.Fatalf("victims must survive infeasible fill: entries=%d bytes=%d", st.ResidentEntries, st.ResidentBytes)
	}
	if _, ok, _ := env.ns.Get("k1"); !ok {
		t.Fatal("k1 must remain cached")
	}
	if _, ok, _ := env.ns.Get("k2"); !ok {
		t.Fatal("k2 must remain cached")
	}
	if _, ok, _ := high.Get("k3"); ok {
		t.Fatal("k3 must not be cached")
	}
	if st.RetiredBytesAvail != 2 || st.RetiredRecordsAvail != 1 {
		t.Fatalf("retired budgets must be conserved: bytes=%d records=%d", st.RetiredBytesAvail, st.RetiredRecordsAvail)
	}
}

// TestOverloadPendingBytesBudget 覆盖候选准备阶段的交接预算耗尽：
// 第二个候选 pending 预留失败 → RESOURCE_LIMIT，不缓存但业务交付；
// 第一个候选发布后预算归还（I2）。
func TestOverloadPendingBytesBudget(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) {
			c.PendingBytes = 2
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.FillRatePerSec = 1; c.HandoffTimeout = 2 * time.Second })
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.FillRatePerSec = 1 })
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

// TestFillNonPositiveGainRejected 覆盖净收益核算：持续服务成本高于
// 可避免回源收益时（G = (r·c − h)×τ < 0），候选以 NON_POSITIVE_GAIN 拒绝。
func TestFillNonPositiveGainRejected(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{
		TTL: time.Minute, Tin: 1, Prior: &Prior{Lambda: 100},
		Model: constantModel{r: 1, c: 1, h: 1000}, // r·c−h = −999
	}, nil)
	v, err := env.fetch("k", okLoader("v"))
	if err != nil || v != "v" {
		t.Fatalf("business result must deliver: %v %v", v, err)
	}
	env.drive()
	st := env.c.Stats()
	if st.FillRejected["NON_POSITIVE_GAIN"] == 0 {
		t.Fatalf("expected NON_POSITIVE_GAIN rejection: %v", st.FillRejected)
	}
	if _, ok, _ := env.ns.Get("k"); ok {
		t.Fatal("non-positive gain candidate must not be cached")
	}
}

// TestRefillIgnoresClockRollback 覆盖时钟回拨：回拨产生的负时长不得倒扣填充令牌，
// 否则令牌被扣成大负数，填充停滞约回拨时长。
func TestRefillIgnoresClockRollback(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) { c.FillRatePerSec = 1000; c.FillBytesPerSec = 1 << 20 })
	s := env.c.c.shards[0]
	env.clk.Advance(5 * time.Second)
	env.c.c.refillFillTokens(s)
	tokens, byteTokens := s.fillTokens, s.fillByteTokens
	env.clk.Set(int64(2 * time.Second)) // 回拨 4s（ManualClock.Set 允许向后）
	env.c.c.refillFillTokens(s)
	if s.fillTokens < tokens || s.fillByteTokens < byteTokens {
		t.Fatalf("clock rollback debited tokens: count %v -> %v, bytes %v -> %v", tokens, s.fillTokens, byteTokens, s.fillByteTokens)
	}
	if s.lastRefill != int64(2*time.Second) {
		t.Fatalf("lastRefill must follow the clock after a rollback: %d", s.lastRefill)
	}
	// 回拨之后时间继续前进，令牌照常补充。
	s.fillTokens = 0
	env.clk.Advance(time.Millisecond * 500)
	env.c.c.refillFillTokens(s)
	if s.fillTokens <= 0 {
		t.Fatalf("tokens must refill after the clock moves forward again: %v", s.fillTokens)
	}
}
