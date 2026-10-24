package ahkc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// testEnv 确定性测试环境：手动时钟 + 手动执行器 + 关闭内置 ticker。
type testEnv struct {
	t   testing.TB
	clk *ManualClock
	ex  *ManualExecutor
	c   *Cache[string]
	ns  *Namespace[string]
}

func newTestEnv(t testing.TB, nsCfg NamespaceConfig, tweak func(*Config)) *testEnv {
	t.Helper()
	clk := NewManualClock(1_000_000_000) // t=1s
	ex := NewManualExecutor()
	cfg := Config{
		ShardCount:       4,
		MaxEntries:       64,
		MaxResidentBytes: 1 << 20,
		HeatDelta:        time.Second,
		ValueDelta:       10 * time.Second,
		SampleP:          1, // 测试全采样
		NMin:             1,
		PeriodicInterval: 50 * time.Millisecond,
		DisableTicker:    true,
		Executor:         ex,
		Clock:            clk,
		FillRatePerSec:   1e6,
		HandoffTimeout:   2 * time.Second,
		LoadTimeout:      10 * time.Second, Advanced: Advanced{ProbeLimit: 8, WheelTick: 100 * time.Millisecond, WheelSlots: 16, WheelLevels: 4},
	}
	if tweak != nil {
		tweak(&cfg)
	}
	if cfg.Namespaces == nil {
		cfg.Namespaces = map[string]NamespaceConfig{"t": nsCfg}
	}
	c, err := New[string](cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ns, err := c.Namespace("t")
	if err != nil {
		t.Fatalf("Namespace: %v", err)
	}
	return &testEnv{t: t, clk: clk, ex: ex, c: c, ns: ns}
}

// drive 运行执行器并触发维护，直到无待办（确定性推进）。
func (e *testEnv) drive() {
	e.t.Helper()
	for i := 0; i < 200; i++ {
		n := e.ex.Drain(1000)
		e.c.MaintenanceStep()
		if e.ex.Drain(1000)+n == 0 {
			return
		}
	}
}

// fetch 同步加载（异步入口 + 手动驱动）。
func (e *testEnv) fetch(key string, loader Loader[string]) (string, error) {
	e.t.Helper()
	fut, err := e.ns.FetchAsync(key, loader)
	if err != nil {
		return "", err
	}
	e.drive()
	return fut.Wait(context.Background())
}

func okLoader(v string) Loader[string] {
	return func(ctx context.Context, key string) (string, error) { return v, nil }
}

// closeManual first stops admission synchronously, then drives bounded cleanup.
// It does not depend on a Close goroutine being scheduled in a fixed iteration count.
func closeManual[V any](t testing.TB, c *Cache[V], ex *ManualExecutor) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Close(ctx)
	waitReal(t, "safe cache reclamation", func() bool {
		c.MaintenanceStep()
		ex.Drain(1000)
		reclaimed := c.CloseProgress().Reclaimed
		return reclaimed
	})
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

var testPrior = &Prior{Lambda: 100}

func TestFetchMissThenPublish(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3, Prior: testPrior}, nil)
	loads := 0
	loader := func(ctx context.Context, key string) (string, error) {
		loads++
		return "v-" + key, nil
	}
	// 前 3 次 miss 累积门卫热度，不缓存（未达 Tin 前候选被拒绝）。
	for i := 0; i < 3; i++ {
		v, err := env.fetch("k1", loader)
		if err != nil || v != "v-k1" {
			t.Fatalf("fetch %d: %v %v", i, v, err)
		}
		env.drive()
	}
	// 第 4 次：热度达标 → 候选发布。
	if _, err := env.fetch("k1", loader); err != nil {
		t.Fatal(err)
	}
	env.drive()
	v, ok, err := env.ns.Get("k1")
	if err != nil || !ok || v != "v-k1" {
		t.Fatalf("get after publish: %v %v %v", v, ok, err)
	}
	stats := env.c.Stats()
	if stats.FillPublished == 0 {
		t.Fatal("no fill published")
	}
	if stats.Misses == 0 {
		t.Fatal("misses not counted")
	}
	env.ex.Drain(1000)
}

func TestTTLExpiry(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: 30 * time.Second, Tin: 2, Prior: testPrior}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("ttl-key", okLoader("v"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("ttl-key"); !ok {
		t.Fatal("expected cached")
	}
	// 推进超过 TTL：读取严格 miss（不依赖维护）。
	env.clk.Advance(31 * time.Second)
	if _, ok, _ := env.ns.Get("ttl-key"); ok {
		t.Fatal("expired entry must not hit")
	}
	// 后台时间轮/冷却按预算摘除（验证维护进展）。
	env.drive()
	env.drive()
}

func TestInvalidateKey(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("ik", okLoader("v1"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("ik"); !ok {
		t.Fatal("expected cached")
	}
	h, err := env.ns.InvalidateAsync("ik")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := h.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := env.ns.Get("ik"); ok {
		t.Fatal("invalidated key must miss")
	}
	// 失效确认后的新加载可重新进入。
	for i := 0; i < 3; i++ {
		env.fetch("ik", okLoader("v2"))
		env.drive()
	}
	if v, ok, _ := env.ns.Get("ik"); !ok || v != "v2" {
		t.Fatalf("re-cache after invalidate: %v %v", v, ok)
	}
}

func TestInvalidateNamespace(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("a", okLoader("va"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("a"); !ok {
		t.Fatal("expected cached")
	}
	// InvalidateNamespace 同步等待处理完成；DisableTicker 下需并发驱动。
	errCh := make(chan error, 1)
	go func() { errCh <- env.c.InvalidateNamespace(context.Background(), "t") }()
	env.drive()
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := env.ns.Get("a"); ok {
		t.Fatal("namespace invalidation must hide old generation")
	}
}

func TestSingleflightMerge(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000, Prior: testPrior}, nil)
	var calls atomic.Int32
	loader := func(ctx context.Context, key string) (string, error) {
		calls.Add(1)
		return "merged", nil
	}
	const N = 8
	futs := make([]*Future[string], N)
	for i := 0; i < N; i++ {
		f, err := env.ns.FetchAsync("hot", loader)
		if err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		futs[i] = f
	}
	env.drive()
	for i, f := range futs {
		v, err := f.Wait(context.Background())
		if err != nil || v != "merged" {
			t.Fatalf("waiter %d: %v %v", i, v, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("I7 violated: %d loads", calls.Load())
	}
}

func TestLoadErrorNotCached(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	boom := fmt.Errorf("boom")
	loader := func(ctx context.Context, key string) (string, error) { return "", boom }
	if _, err := env.fetch("err", loader); err == nil {
		t.Fatal("expected loader error")
	}
	env.drive()
	if _, ok, _ := env.ns.Get("err"); ok {
		t.Fatal("errors must not be cached (首版不缓存错误)")
	}
}

func TestCooldownEviction(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: 10 * time.Minute, Tin: 2, Tout: 1, Prior: testPrior},
		func(c *Config) {
			c.Grace = 2 * time.Second
			c.ColdFor = time.Second
		})
	for i := 0; i < 3; i++ {
		env.fetch("cool", okLoader("v"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("cool"); !ok {
		t.Fatal("expected cached")
	}
	// 停止访问，推进时间：grace(2s) 后首次扫描记 lowSince，再持续 coldFor(1s) 后摘除。
	for i := 0; i < 6; i++ {
		env.clk.Advance(time.Second)
		env.drive()
	}
	if _, ok, _ := env.ns.Get("cool"); ok {
		t.Fatal("cold entry must be evicted after sustained low heat")
	}
}

func TestGracePeriodProtectsNewEntries(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: 10 * time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.Grace = 2 * time.Second; c.ColdFor = time.Second })
	for i := 0; i < 3; i++ {
		env.fetch("g", okLoader("v"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("g"); !ok {
		t.Fatal("expected cached")
	}
	// grace 期内即使无访问也不冷却。
	env.clk.Advance(time.Second)
	env.drive()
	if _, ok, _ := env.ns.Get("g"); !ok {
		t.Fatal("grace period must protect new entries")
	}
}

func TestCloseSemantics(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("c", okLoader("v"))
		env.drive()
	}
	closeManual(t, env.c, env.ex)
	if _, _, err := env.ns.Get("c"); err != ErrClosed {
		t.Fatalf("after close: %v", err)
	}
	if _, err := env.fetch("c", okLoader("v")); err != ErrClosed {
		t.Fatalf("fetch after close: %v", err)
	}
}

func TestReplacementWhenFull(t *testing.T) {
	// 每分片 countMax=64/4=16；制造分片 0 的容量压力需要构造同分片 key。
	// 简化：全库 64 条容量，填 80 个热 key，验证总驻留不超容量且持续准入。
	// key 零填充为等长：计费等大 → 求解器等大小快速路径（Top-K），
	// 避免每次替换 2^m 全枚举（一般枚举路径由 solver_test.go 性质测试覆盖）。
	env := newTestEnv(t, NamespaceConfig{TTL: 10 * time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) {
			c.MaxEntries = 32
			c.Grace = time.Hour   // grace 拉长防止冷却干扰
			c.RoundWorkLimit = 64 // 收窄每轮配额：240 次 fetch 的驱动开销在 -race 下可控
			c.RoundSoftTimeBudget = time.Hour
		})
	for i := 0; i < 80; i++ {
		key := fmt.Sprintf("k%02d", i)
		for j := 0; j < 3; j++ {
			env.fetch(key, okLoader("v"+key))
			env.drive()
		}
	}
	env.drive()
	stats := env.c.Stats()
	if stats.ResidentEntries > 32 {
		t.Fatalf("I1 violated: resident=%d > C=32", stats.ResidentEntries)
	}
	if stats.FillPublished == 0 {
		t.Fatal("no fills published")
	}
}

func TestHeatGateRejectsColdFill(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 100, Prior: testPrior}, nil)
	env.fetch("cold", okLoader("v"))
	env.drive()
	if _, ok, _ := env.ns.Get("cold"); ok {
		t.Fatal("cold key must not be cached")
	}
	stats := env.c.Stats()
	if stats.FillRejected["NOT_HOT"] == 0 {
		t.Fatalf("expected NOT_HOT rejection, got %v", stats.FillRejected)
	}
}

// ---- 修复回归测试 ----

// TestEntryHeatAndVisitedOnHit（C1 回归）：有效命中必须在 guard 临界区内完成
// 驻留热度与 visited 更新（A.3：guard 随视图存活）。
func TestEntryHeatAndVisitedOnHit(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("h", okLoader("v"))
		env.drive()
	}
	e, ok := findEntry(env, "h")
	if !ok {
		t.Fatal("entry not published")
	}
	if !e.visited.Load() {
		t.Fatal("hit must set visited")
	}
	for i := 0; i < 3; i++ {
		if _, ok, _ := env.ns.Get("h"); !ok {
			t.Fatal("hit expected")
		}
	}
	delta := int64(env.c.c.cfg.HeatDelta)
	now := env.clk.Now()
	h, okH := e.heat.read(bucketIndex(now, delta), bucketPhi(now, delta))
	if !okH || h < 3 {
		t.Fatalf("entry heat must record hits, h=%v ok=%v", h, okH)
	}
}

// findEntry 直取分片索引中的条目（测试辅助）。
func findEntry(env *testEnv, key string) (*entry[string], bool) {
	kh := env.ns.ns.hashKey(key)
	s := env.c.c.shardOf(kh)
	fr := s.idx.find(key, kh, env.ns.ns.id)
	if !fr.found {
		return nil, false
	}
	return fr.e, true
}

// TestResultTooLargeMetric（C2 回归）：默认配置（MaxEntryBytes < MaxResultBytes）
// 下，超过结果上限的加载必须计入 ResultTooLarge 并拒绝缓存。
func TestResultTooLargeMetric(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	// 用计费 weigher 模拟超大结果（避免实际分配）。
	env.c.c.weigh = func(key string, value string) int64 { return 5 << 20 } // 5MiB > 4MiB
	v, err := env.fetch("big", okLoader("big-value"))
	if !errors.Is(err, ErrResultTooLarge) || v != "" {
		t.Fatalf("oversized result must fail its load contract: %v %v", v, err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("big"); ok {
		t.Fatal("oversized result must not be cached")
	}
	st := env.c.Stats()
	if st.ResultTooLarge != 1 {
		t.Fatalf("ResultTooLarge metric: %d", st.ResultTooLarge)
	}
	if st.ResponseBytesAvail != env.c.c.responseBytes.limit {
		t.Fatal("oversized result retained response credits")
	}
}

// TestDrainSamplesRoundRobin（C4 回归）：单个空条带不得中断本轮样本汇总；
// 只有全部条带连续为空才结束。
func TestDrainSamplesRoundRobin(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	s := env.c.c.shards[0]
	// 只向条带 1 推样本；条带 0（轮转起点）保持空。
	const n = 3
	for i := 0; i < n; i++ {
		if !s.stripes[1].tryPush(sample{
			nsID: env.ns.ns.id, nsGen: env.ns.ns.gen.Load(),
			policyHash: 7, eventTime: env.clk.Now(), invP: 1,
		}) {
			t.Fatal("push failed")
		}
	}
	s.stripeDrain = 0
	if work := env.c.c.drainSamples(s, 16); work > 16 {
		t.Fatalf("stripe visits exceeded their work quota: %d", work)
	}
	if weighted, _ := s.stats.query(7, env.clk.Now(), int64(env.c.c.cfg.ValueDelta)); weighted != n {
		t.Fatalf("round-robin starved a nonempty stripe: got=%v want=%d", weighted, n)
	}
}

// toggleExecutor 可切换失败的手动执行器（C3 回归用）。
type toggleExecutor struct {
	fail atomic.Bool
	ManualExecutor
}

func (t *toggleExecutor) TrySubmit(task func()) bool {
	if t.fail.Load() {
		return false
	}
	return t.ManualExecutor.TrySubmit(task)
}

// TestNsDrainFlagNotLeakedOnSubmitFailure（C3 回归）：命名空间控制排水的
// TrySubmit 失败不得永久泄漏 nsCtlDraining 标志。
func TestNsDrainFlagNotLeakedOnSubmitFailure(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	ex := &toggleExecutor{}
	ex.fail.Store(true)
	cfg := Config{
		ShardCount: 2, MaxEntries: 16, MaxResidentBytes: 1 << 20,
		DisableTicker: true, Executor: ex, Clock: clk,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 2}},
	}
	c, err := New[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 第一次失效：ensureNsDrain 提交失败（标志必须复位而非泄漏）。
	errCh := make(chan error, 2)
	go func() { errCh <- c.InvalidateNamespace(context.Background(), "t") }()
	waitReal(t, "op1 enqueued", func() bool { return len(c.c.nsControlQueue) == 1 })
	// 恢复执行器：第二次失效必须能重新提交排水任务（标志未被泄漏）。
	ex.fail.Store(false)
	go func() { errCh <- c.InvalidateNamespace(context.Background(), "t") }()
	waitReal(t, "op2 enqueued", func() bool { return len(c.c.nsControlQueue) == 2 })
	completed := 0
	for i := 0; i < 500 && completed < 2; i++ {
		ex.Drain(10)
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("invalidate: %v", err)
			}
			completed++
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if completed != 2 {
		t.Fatal("both invalidations must complete via re-submitted drain task")
	}
}

// TestCloseReturnsWhenControlQueueStalled（D2 回归）：控制队列满且维护不推进时，
// Close 不得忙等占用调用线程，必须及时响应 ctx。
func TestCloseReturnsWhenControlQueueStalled(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.ControlQueueSize = 1 })
	// 填满分片控制队列（容量 1）且不驱动维护。
	if _, err := env.ns.InvalidateAsync("a"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		done <- env.c.Close(ctx)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled control queue must block RECLAIMED")
		}
		// ctx 错误：符合“不忙等、不无期限占用调用线程”。
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("want DeadlineExceeded, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close busy-waited instead of honoring ctx")
	}
}

// TestDefaultsResponseBudgetAndSoftTime（D1/D3 回归）：
// 响应预算默认与加载并发自洽；维护软时间预算有合法默认。
func TestDefaultsResponseBudgetAndSoftTime(t *testing.T) {
	cfg := Config{MaxEntries: 10, MaxResidentBytes: 1 << 20,
		MaxConcurrentLoads: 7, MaxResultBytes: 1000}
	cfg.applyDefaults()
	if cfg.RoundSoftTimeBudget <= 0 {
		t.Fatalf("RoundSoftTimeBudget default: %v", cfg.RoundSoftTimeBudget)
	}
	if cfg.ResponseBytes != 7000 {
		t.Fatalf("ResponseBytes default must be MaxConcurrentLoads*MaxResultBytes, got %d", cfg.ResponseBytes)
	}
	// 全默认：256×4MiB = 1GiB。
	cfg2 := Config{MaxEntries: 10, MaxResidentBytes: 1 << 20}
	cfg2.applyDefaults()
	if cfg2.ResponseBytes != int64(cfg2.MaxConcurrentLoads)*cfg2.MaxResultBytes {
		t.Fatalf("full default ResponseBytes: %d", cfg2.ResponseBytes)
	}
}

func TestSamplingUsesActualProbability(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, SampleP: 1e-12, Prior: testPrior}, nil)
	if env.ns.ns.sampleThreshold == 0 || math.Abs(env.ns.ns.sampleInvP*1e-12-1) > 1e-6 {
		t.Fatal("tiny but valid sampling probability was rounded to zero")
	}
	ns, err := env.c.RegisterNamespace("sampled", NamespaceConfig{TTL: time.Minute, SampleP: 1.0 / 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20000; i++ {
		ns.Get("k")
	}
	if env.c.Stats().Samples == 0 {
		t.Fatal("p=1/1024 must eventually sample")
	}
}

// TestNsByIDSnapshot（P2 回归）：命名空间快照按 id 无锁读取，
// 越界返回 nil 而不 panic。
func TestNsByIDSnapshot(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	c := env.c.c
	if ns := c.nsByID(env.ns.ns.id); ns == nil || ns.name != "t" {
		t.Fatalf("nsByID(%d): %+v", env.ns.ns.id, ns)
	}
	if ns := c.nsByID(99); ns != nil {
		t.Fatalf("out-of-range must be nil, got %+v", ns)
	}
	// 注册后新 id 可见。
	ns2, err := env.c.RegisterNamespace("t2", NamespaceConfig{TTL: time.Minute, Tin: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.nsByID(ns2.ns.id); got == nil || got.name != "t2" {
		t.Fatalf("nsByID(%d) after register: %+v", ns2.ns.id, got)
	}
}

// TestPoolCoversPendingAndRetiredDemand（D4 回归）：分片条目池容量必须覆盖
// 驻留 + 排队候选 + 退休未结算份额。
func TestPoolCoversPendingAndRetiredDemand(t *testing.T) {
	cfg := Config{ShardCount: 4, MaxEntries: 64, MaxResidentBytes: 1 << 20}
	cfg.applyDefaults()
	perCount := cfg.MaxEntries / int64(cfg.ShardCount)
	need := int(perCount) + cfg.FillQueueSize + cfg.RetiredRecords/cfg.ShardCount
	for _, s := range envOfShards(t, cfg) {
		if len(s.pool.items) < need {
			t.Fatalf("pool size %d < required %d", len(s.pool.items), need)
		}
	}
}

func envOfShards(t *testing.T, cfg Config) []*shard[string] {
	t.Helper()
	cfg.Namespaces = map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 2}}
	cfg.DisableTicker = true
	cfg.Executor = NewManualExecutor()
	cfg.Clock = NewManualClock(1_000_000_000)
	c, err := New[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c.c.shards
}

// ---- 拥有型句柄（I9 / GetOwned）----

// TestGetOwnedBasics 覆盖 GetOwned 的 hit / miss / closed 三态与
// Handle.Value / Release 幂等（README：释放前该条目不会被回收）。
func TestGetOwnedBasics(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	// 确定 miss：ok=false, err=nil。
	if h, ok, err := env.ns.GetOwned("k"); ok || err != nil || h != nil {
		t.Fatalf("miss: handle=%v ok=%v err=%v", h, ok, err)
	}
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	h, ok, err := env.ns.GetOwned("k")
	if err != nil || !ok || h == nil {
		t.Fatalf("hit: handle=%v ok=%v err=%v", h, ok, err)
	}
	if h.Value() != "v" {
		t.Fatalf("handle value: %v", h.Value())
	}
	// Release 幂等：重复释放只减一次计数。
	h.Release()
	h.Release()
	if v, ok, _ := env.ns.Get("k"); !ok || v != "v" {
		t.Fatalf("entry must remain readable after release: %v %v", v, ok)
	}
	// 关闭后：ErrClosed。
	closeManual(t, env.c, env.ex)
	if _, ok, err := env.ns.GetOwned("k"); !errors.Is(err, ErrClosed) || ok {
		t.Fatalf("GetOwned after close: ok=%v err=%v", ok, err)
	}
}

func TestOwnedRejectionZeroAlloc(t *testing.T) {
	for _, limit := range []string{"handles", "pins"} {
		t.Run(limit, func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) {
				c.SampleP = 1e-12
				if limit == "handles" {
					c.MaxOwnedHandles = 1
				} else {
					c.MaxOwnedPinsPerEntry = 1
				}
			})
			defer closeManual(t, env.c, env.ex)
			if _, err := env.fetch("key", okLoader("v")); err != nil {
				t.Fatal(err)
			}
			h, ok, err := env.ns.GetOwned("key")
			if err != nil || !ok {
				t.Fatal("expected owned hit", err)
			}
			defer h.Release()
			view, ok, err := env.ns.GetBorrowed("key")
			if err != nil || !ok {
				t.Fatal("expected borrowed hit", err)
			}
			defer view.Release()
			for name, get := range map[string]func() (*Handle[string], error){
				"get":            func() (*Handle[string], error) { h, _, err := env.ns.GetOwned("key"); return h, err },
				"owned clone":    h.Clone,
				"borrowed clone": view.Clone,
			} {
				a := testing.AllocsPerRun(1000, func() {
					if h, err := get(); h != nil || !errors.Is(err, ErrBusy) {
						t.Fatal("expected budget rejection", err)
					}
				})
				if a != 0 {
					t.Errorf("%s: rejected handle allocated %.0f objects", name, a)
				}
			}
		})
	}
}

// TestGetOwnedPinLimit 覆盖 MaxOwnedPinsPerEntry 上界：超出后 BUSY，
// 释放后可重新取得。
func TestGetOwnedPinLimit(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) { c.MaxOwnedPinsPerEntry = 2 })
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	h1, ok, err := env.ns.GetOwned("k")
	if !ok || err != nil || h1 == nil {
		t.Fatalf("first handle: %v %v %v", h1, ok, err)
	}
	h2, ok, err := env.ns.GetOwned("k")
	if !ok || err != nil || h2 == nil {
		t.Fatalf("second handle: %v %v %v", h2, ok, err)
	}
	// 达到条目持有上限：BUSY（不是确定 miss）。
	if _, ok, err := env.ns.GetOwned("k"); !errors.Is(err, ErrBusy) || ok {
		t.Fatalf("pin limit must yield ErrBusy: ok=%v err=%v", ok, err)
	}
	h1.Release()
	h3, ok, err := env.ns.GetOwned("k")
	if !ok || err != nil || h3 == nil {
		t.Fatalf("handle after release: %v %v %v", h3, ok, err)
	}
	h2.Release()
	h3.Release()
}

// TestGetOwnedDefersRetirement 覆盖 I9 与 A.9 交互：持有句柄期间条目
// 逻辑失效但不物理回收（Close 达 DRAINED 而非 RECLAIMED）；Release 后
// 退休结算、全部预算归还。
func TestGetOwnedDefersRetirement(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	h, ok, err := env.ns.GetOwned("k")
	if !ok || err != nil {
		t.Fatalf("GetOwned: %v %v", ok, err)
	}
	// 持有句柄时失效：逻辑隐藏，物理退休登记但未结算。
	ih, err := env.ns.InvalidateAsync("k")
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if err := ih.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := env.ns.Get("k"); ok {
		t.Fatal("invalidated key must be logically hidden")
	}
	// Close：I/O 与交接均已终结 → DRAINED；句柄仍持有 → 退休未结算 → 非 RECLAIMED。
	closeDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		closeDone <- env.c.Close(ctx)
	}()
	for i := 0; i < 50; i++ {
		env.ex.Drain(1000)
		env.c.MaintenanceStep()
	}
	if err := <-closeDone; err == nil {
		t.Fatal("Close must report ctx error while handle held")
	}
	progress := env.c.CloseProgress()
	stopped, drained, reclaimed := progress.Stopped, progress.Drained, progress.Reclaimed
	if !stopped || !drained || reclaimed {
		t.Fatalf("CloseState with held handle: %v %v %v", stopped, drained, reclaimed)
	}
	if v := h.Value(); v != "v" {
		t.Fatalf("held handle value: %v", v)
	}
	// Release 后：安全回收推进至 RECLAIMED，退休账目归还（I2）。
	h.Release()
	for i := 0; i < 50 && !reclaimed; i++ {
		env.ex.Drain(1000)
		env.c.MaintenanceStep()
		reclaimed = env.c.CloseProgress().Reclaimed
	}
	if !reclaimed {
		t.Fatal("RECLAIMED not reached after Release")
	}
	if err := env.c.WaitReclaimed(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := env.c.Stats()
	if st.RetiredBytesAvail != env.c.c.retiredBytes.limit || st.RetiredRecordsAvail != env.c.c.retiredRecords.limit {
		t.Fatalf("retired budgets not conserved: %+v", st)
	}
	if st.ResidentEntries != 0 {
		t.Fatalf("residual residents: %d", st.ResidentEntries)
	}
}

// ---- key 上限 ----

// TestKeyTooLarge 覆盖全部入口的 ErrKeyTooLarge 契约。
func TestKeyTooLarge(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.MaxKeyBytes = 8 })
	key := strings.Repeat("x", 9)
	if _, ok, err := env.ns.Get(key); !errors.Is(err, ErrKeyTooLarge) || ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if _, err := env.ns.Fetch(context.Background(), key, okLoader("v")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("Fetch: %v", err)
	}
	if _, err := env.ns.FetchAsync(key, okLoader("v")); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("FetchAsync: %v", err)
	}
	if _, ok, err := env.ns.GetOwned(key); !errors.Is(err, ErrKeyTooLarge) || ok {
		t.Fatalf("GetOwned: ok=%v err=%v", ok, err)
	}
	if _, err := env.ns.InvalidateAsync(key); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("InvalidateAsync: %v", err)
	}
	if _, err := env.ns.BeginRequest(key); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("BeginRequest: %v", err)
	}
	if _, ok := env.ns.Heat(key); ok {
		t.Fatal("Heat must reject oversized key")
	}
}

// ---- 计费（Weigher）----

// TestDefaultWeigher 覆盖默认计费的三种值类型口径。
func TestDefaultWeigher(t *testing.T) {
	if got := DefaultWeigher[string]("ab", "cde"); got != 2+3+int64(unsafe.Sizeof(entry[string]{})) {
		t.Fatalf("string weigh: %d", got)
	}
	if got := DefaultWeigher[[]byte]("ab", make([]byte, 0, 5)); got != 2+5+int64(unsafe.Sizeof(entry[[]byte]{})) {
		t.Fatalf("bytes weigh (cap): %d", got)
	}
	if got := DefaultWeigher[int]("ab", 42); got != 2+int64(unsafe.Sizeof(entry[int]{})) {
		t.Fatalf("flat weigh: %d", got)
	}
}

// TestWithWeigher 覆盖公共计费选项：驻留字节按自定义口径计费。
func TestWithWeigher(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	ex := NewManualExecutor()
	cfg := Config{
		ShardCount: 2, MaxEntries: 16, MaxResidentBytes: 1 << 20,
		HeatDelta: time.Second, ValueDelta: 10 * time.Second,
		SampleP: 1, NMin: 1, DisableTicker: true,
		Executor: ex, Clock: clk, FillRatePerSec: 1e6,
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 1, Prior: testPrior},
		}, Advanced: Advanced{WheelTick: 100 * time.Millisecond, WheelSlots: 16, WheelLevels: 4},
	}
	const fixed = 7
	c, err := New[string](cfg, WithWeigher[string](func(key, value string) int64 { return fixed }))
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	fut, err := ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		ex.Drain(1000)
		c.MaintenanceStep()
	}
	if _, err := fut.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := c.Stats(); st.ResidentEntries != 1 || st.ResidentBytes != fixed {
		t.Fatalf("custom weigher: entries=%d bytes=%d, want 1/%d", st.ResidentEntries, st.ResidentBytes, fixed)
	}
	// 关闭（手动执行器需并发驱动）。
	closeManual(t, c, ex)
}

// weigherTestConfig 返回手动时钟 / 执行器下的最小配置。
func weigherTestConfig() Config {
	return Config{
		ShardCount: 2, MaxEntries: 16, MaxResidentBytes: 1 << 20,
		HeatDelta: time.Second, ValueDelta: 10 * time.Second,
		SampleP: 1, NMin: 1, DisableTicker: true,
		Executor: NewManualExecutor(), Clock: NewManualClock(1_000_000_000), FillRatePerSec: 1e6,
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 1, Prior: testPrior},
		}, Advanced: Advanced{WheelTick: 100 * time.Millisecond, WheelSlots: 16, WheelLevels: 4},
	}
}

// TestNewRequiresWeigherForReferenceTypes：含指针的值类型无默认计费，New 必须明确拒绝，
// 而不是等到每次 Fetch 才以含义不符的错误失败。
func TestNewRequiresWeigherForReferenceTypes(t *testing.T) {
	type obj struct{ p *int }
	if _, err := New[*obj](weigherTestConfig()); !errors.Is(err, ErrNoWeigher) {
		t.Fatalf("New[*obj] without weigher: err=%v, want ErrNoWeigher", err)
	}
	if _, err := New[obj](weigherTestConfig()); !errors.Is(err, ErrNoWeigher) {
		t.Fatalf("New[obj] without weigher: err=%v, want ErrNoWeigher", err)
	}
	if _, err := New[*obj](weigherTestConfig(), WithWeigher[*obj](func(string, *obj) int64 { return 64 })); err != nil {
		t.Fatalf("New[*obj] with weigher: %v", err)
	}
	for _, ok := range []bool{
		func() bool { _, err := New[string](weigherTestConfig()); return err == nil }(),
		func() bool { _, err := New[[]byte](weigherTestConfig()); return err == nil }(),
		func() bool { _, err := New[int](weigherTestConfig()); return err == nil }(),
		func() bool { _, err := New[any](weigherTestConfig()); return err == nil }(),
	} {
		if !ok {
			t.Fatal("supported value type rejected without a weigher")
		}
	}
}

// TestNonPositiveWeightIsInvalidWeight：weigher 返回 <=0 与“超过 MaxResultBytes”是两类契约错误。
func TestNonPositiveWeightIsInvalidWeight(t *testing.T) {
	cfg := weigherTestConfig()
	ex := cfg.Executor.(*ManualExecutor)
	c, err := New[string](cfg, WithWeigher[string](func(string, string) int64 { return 0 }))
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	fut, err := ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		ex.Drain(1000)
		c.MaintenanceStep()
	}
	if _, err := fut.Wait(context.Background()); !errors.Is(err, ErrInvalidWeight) || errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("zero weight: err=%v, want ErrInvalidWeight", err)
	}
	closeManual(t, c, ex)
}

// TestFutureConsumedIsDistinctFromBusy：结果取走后再次 Wait 不可重试，必须返回 ErrConsumed。
func TestFutureConsumedIsDistinctFromBusy(t *testing.T) {
	cfg := weigherTestConfig()
	ex := cfg.Executor.(*ManualExecutor)
	c, err := New[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	fut, err := ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		ex.Drain(1000)
		c.MaintenanceStep()
	}
	if v, err := fut.Wait(context.Background()); err != nil || v != "v" {
		t.Fatalf("first Wait: %q %v", v, err)
	}
	if _, err := fut.Wait(context.Background()); !errors.Is(err, ErrConsumed) || errors.Is(err, ErrBusy) {
		t.Fatalf("second Wait: err=%v, want ErrConsumed", err)
	}
	closeManual(t, c, ex)
}

// ---- 退休预算耗尽的优雅降级 ----

// TestRetireBudgetExhaustedThenRecovers 覆盖 TRY_RETIRE 无预算分支：
// 逻辑失效立即生效，账目保留（不丢账）；预算恢复后后台自动摘除并守恒。
func TestRetireBudgetExhaustedThenRecovers(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) { c.RetiredRecords = 1 })
	for _, k := range []string{"k1", "k2"} {
		if _, err := env.fetch(k, okLoader("v-"+k)); err != nil {
			t.Fatal(err)
		}
		env.drive()
	}
	// 两个失效同轮处理：k1 占用唯一退休记录，k2 无预算 → 账目保留。
	h1, err := env.ns.InvalidateAsync("k1")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := env.ns.InvalidateAsync("k2")
	if err != nil {
		t.Fatal(err)
	}
	// 单步驱动：只处理控制操作一轮（完整 drive 会把恢复也跑完）。
	env.ex.Drain(1000)
	if err := h1.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := h2.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := env.c.Stats()
	if st.ResidentEntries != 1 {
		// k1 已退休；k2 无预算 → 账目必须保留（不因预算不足丢账）。
		t.Fatalf("exhausted retire budget must keep accounts: residents=%d", st.ResidentEntries)
	}
	if st.RetiredRecordsAvail != 0 {
		t.Fatalf("retired records must be exhausted: %d", st.RetiredRecordsAvail)
	}
	// 回收推进：k1 结算释放预算 → 冷却扫描摘除 k2 → 全部守恒。
	for i := 0; i < 200; i++ {
		env.ex.Drain(1000)
		env.c.MaintenanceStep()
	}
	st = env.c.Stats()
	if st.ResidentEntries != 0 {
		t.Fatalf("residents must drain after budget recovery: %d", st.ResidentEntries)
	}
	if st.RetiredRecordsAvail != 1 || st.RetiredBytesAvail != env.c.c.retiredBytes.limit {
		t.Fatalf("retired budgets not conserved: records=%d bytes=%d/%d",
			st.RetiredRecordsAvail, st.RetiredBytesAvail, env.c.c.retiredBytes.limit)
	}
}

// TestPushSampleSpillsAcrossStripes：起始条带 gate 被占用时样本应落入其他条带而不置丢样标记；
// 只有全部条带都不可用才放弃并置标记（丢样不得因热 key 集中在同一条带而放大）。
func TestPushSampleSpillsAcrossStripes(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	s := env.c.c.shards[0]
	if len(s.stripes) < 2 {
		t.Skip("needs multiple stripes")
	}
	const kh = 12345
	start := int(mix64(kh) % uint64(len(s.stripes)))
	sm := sample{nsID: env.ns.ns.id, nsGen: env.ns.ns.gen.Load(), policyHash: 7, eventTime: env.clk.Now(), invP: 1}

	s.stripes[start].mu.Lock() // 模拟起始条带被并发请求占用
	if !s.pushSample(kh, sm) {
		t.Fatal("sample dropped although other stripes were free")
	}
	for i, st := range s.stripes {
		if st.faultFlag.Load() {
			t.Fatalf("stripe %d marked fault despite successful spill", i)
		}
	}
	if s.stripes[start].queued.Load() != 0 {
		t.Fatal("sample landed in the occupied stripe")
	}

	for i, st := range s.stripes {
		if i != start {
			st.mu.Lock()
		}
	}
	if s.pushSample(kh, sm) {
		t.Fatal("sample accepted with every stripe occupied")
	}
	if !s.stripes[start].faultFlag.Load() {
		t.Fatal("total drop must set the fault flag")
	}
	for _, st := range s.stripes {
		st.mu.Unlock()
	}
}

// TestOwnedHitNoSpuriousBusyUnderContention：拥有型命中的句柄预留与条目持有计数都是无等待的，
// 只要配额充足，同一热 key 上的并发 GetOwned 不得因 CAS 竞争返回伪 BUSY；
// 全部释放后句柄预算与 ownedPins 精确归零。
func TestOwnedHitNoSpuriousBusyUnderContention(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, func(c *Config) {
		c.MaxOwnedHandles = 1024
		c.MaxOwnedPinsPerEntry = 1024
		c.ReaderSlotsPerShard = 64
	})
	if _, err := env.fetch("hot", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	limit := env.c.c.handleBudget.limit
	const workers, iters = 16, 2000
	var busy atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				h, hit, err := env.ns.GetOwned("hot")
				if err != nil || !hit {
					busy.Add(1)
					continue
				}
				h.Release()
			}
		}()
	}
	wg.Wait()
	if n := busy.Load(); n != 0 {
		t.Fatalf("%d spurious BUSY/miss of %d owned hits", n, workers*iters)
	}
	if got := env.c.c.handleBudget.available(); got != limit {
		t.Fatalf("handle budget leaked: available=%d limit=%d", got, limit)
	}
	h, _, _ := env.ns.GetOwned("hot")
	h.Release()
	closeManual(t, env.c, env.ex)
}

// TestContextErrorsJoinLibraryErrors：调用方 ctx 的取消 / 超时同时满足库错误与 context 错误，
// 库内部期限（无 ctx 关联）则不满足 context 错误，调用方可据此区分来源。
func TestContextErrorsJoinLibraryErrors(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	fut, err := env.ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fut.Wait(canceled); !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: err=%v, want ErrCanceled and context.Canceled", err)
	}
	expired, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	fut2, err := env.ns.FetchAsync("k2", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fut2.Wait(expired); !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("expired ctx: err=%v, want ErrTimeout and DeadlineExceeded", err)
	}
	if errors.Is(ErrTimeout, context.DeadlineExceeded) {
		t.Fatal("internal ErrTimeout must stay distinguishable from caller ctx expiry")
	}
	env.drive()
	closeManual(t, env.c, env.ex)
}

// TestCloseStagesAreIndependentlyAwaitable：被持有的句柄只阻止 RECLAIMED，不阻止 DRAINED；
// Close 超时返回 ErrCloseIncomplete（可与 ctx 错误一同判断），释放后可继续完成。
func TestCloseStagesAreIndependentlyAwaitable(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	h, hit, err := env.ns.GetOwned("k")
	if err != nil || !hit {
		t.Fatalf("GetOwned: hit=%v err=%v", hit, err)
	}
	env.c.Shutdown()
	if p := env.c.CloseProgress(); !p.Stopped {
		t.Fatalf("Shutdown must stop admission immediately: %+v", p)
	}
	if _, _, err := env.ns.Get("k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Shutdown: %v, want ErrClosed", err)
	}
	waitReal(t, "work drained", func() bool {
		env.c.MaintenanceStep()
		env.ex.Drain(1000)
		return env.c.CloseProgress().Drained
	})
	if err := env.c.WaitDrained(context.Background()); err != nil {
		t.Fatalf("WaitDrained: %v", err)
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := env.c.WaitReclaimed(expired); !errors.Is(err, ErrTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitReclaimed with live handle: %v", err)
	}
	if err := env.c.Close(expired); !errors.Is(err, ErrCloseIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close timeout: %v, want ErrCloseIncomplete wrapping the ctx error", err)
	}
	if env.c.CloseProgress().Reclaimed {
		t.Fatal("reclaimed while an owned handle is live")
	}
	h.Release()
	closeManual(t, env.c, env.ex)
}

// TestDroppedFutureReleasesBudgetOnGC：丢弃未终结的 FetchAsync 结果时，GC 终结器兜底 Cancel，
// 归还 waiter 槽位与拥有型句柄；正常 Wait 终结后终结器被摘除。
func TestDroppedFutureReleasesBudgetOnGC(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	c := env.c.c
	waiterLimit, handleLimit := c.waiterBudget.limit, c.handleBudget.limit
	func() {
		for i := 0; i < 8; i++ {
			if _, err := env.ns.FetchAsync("k", okLoader("v")); err != nil { // 命中：立即就绪，随即被丢弃
				t.Fatal(err)
			}
		}
	}()
	if c.waiterBudget.available() == waiterLimit {
		t.Fatal("setup: dropped futures should still hold waiter slots before GC")
	}
	waitReal(t, "finalizer cancels dropped futures", func() bool {
		runtime.GC()
		return c.waiterBudget.available() == waiterLimit && c.handleBudget.available() == handleLimit
	})

	fut, err := env.ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	if !fut.guarded.Load() {
		t.Fatal("public future must carry the safety net")
	}
	if _, err := fut.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fut.guarded.Load() {
		t.Fatal("safety net must be removed once the future is consumed")
	}
	closeManual(t, env.c, env.ex)
}
