package ahkc

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		Shards:     4,
		MaxEntries: 64,
		MaxBytes:   1 << 20,
		Heat: HeatConfig{
			Delta: time.Second,
		},
		Limits: LimitsConfig{
			HandoffTimeout: 2 * time.Second,
			LoadTimeout:    10 * time.Second,
		},
		Runtime: RuntimeConfig{
			PeriodicInterval: 50 * time.Millisecond,
			DisableTicker:    true,
			Executor:         ex,
			Clock:            clk,
		},
	}
	if tweak != nil {
		tweak(&cfg)
	}
	if cfg.Namespaces == nil {
		cfg.Namespaces = map[string]NamespaceConfig{"t": nsCfg}
	}
	c, err := NewBytes[string](cfg)
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

func TestFetchMissThenPublish(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: 30 * time.Second, Tin: 2}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("ttl-key", okLoader("v"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("ttl-key"); !ok {
		t.Fatal("expected cached")
	}
	// 推进超过 TTL：读取严格 miss（不依赖维护）；退场本身由 TestIdleExpiredEntryRetiredBySweep 守护。
	env.clk.Advance(31 * time.Second)
	if _, ok, _ := env.ns.Get("ttl-key"); ok {
		t.Fatal("expired entry must not hit")
	}
}

func TestInvalidateKey(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1000}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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

func TestExpiredEntryRetiredByAccessHint(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("h", okLoader("v"))
		env.drive()
	}
	if _, ok, _ := env.ns.Get("h"); !ok {
		t.Fatal("expected cached")
	}
	env.clk.Advance(2 * time.Minute)
	// 读时校验：过期值永不交付，并登记退场提示。
	if _, ok, _ := env.ns.Get("h"); ok {
		t.Fatal("expired value must not be delivered")
	}
	env.drive()
	st := env.c.Stats()
	if st.ResidentEntries != 0 || st.ExpiredHints == 0 {
		t.Fatalf("hint must retire the expired entry: resident=%d hints=%d", st.ResidentEntries, st.ExpiredHints)
	}
}

func TestIdleExpiredEntryRetiredBySweep(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("s", okLoader("v"))
		env.drive()
	}
	if env.c.Stats().ResidentEntries != 1 {
		t.Fatal("expected one resident entry")
	}
	// 无任何访问：慢扫描在一个 SweepPeriod 内覆盖整张索引。
	env.clk.Advance(10 * time.Minute)
	env.drive()
	if n := env.c.Stats().ResidentEntries; n != 0 {
		t.Fatalf("sweep must retire the idle expired entry, resident=%d", n)
	}
}

func TestCloseSemantics(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: 10 * time.Minute, Tin: 2},
		func(c *Config) {
			c.MaxEntries = 32
			c.Runtime.RoundWorkLimit = 64 // 收窄每轮配额：240 次 fetch 的驱动开销在 -race 下可控
			c.Runtime.RoundSoftTimeBudget = time.Hour
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 100}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
	for i := 0; i < 3; i++ {
		env.fetch("h", okLoader("v"))
		env.drive()
	}
	e, ok := findEntry(env, "h")
	if !ok {
		t.Fatal("entry not published")
	}
	if e.flags.Load()&flagVisited == 0 {
		t.Fatal("hit must set visited")
	}
	for i := 0; i < 3; i++ {
		if _, ok, _ := env.ns.Get("h"); !ok {
			t.Fatal("hit expected")
		}
	}
	delta := int64(env.c.c.cfg.Heat.Delta)
	now := env.clk.Now()
	h, okH := e.heat.read(bucketIndex(now, delta), bucketPhi(now, delta))
	if !okH || h < 3 {
		t.Fatalf("entry heat must record hits, h=%v ok=%v", h, okH)
	}
}

// findEntry 直取分片索引中的条目（测试辅助）。
func findEntry(env *testEnv, key string) (*entry, bool) {
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
		t.Fatal("oversized result retained its response reservation")
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
		Shards:     2,
		MaxEntries: 16,
		MaxBytes:   1 << 20,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 2}},
		Runtime: RuntimeConfig{
			DisableTicker: true,
			Executor:      ex,
			Clock:         clk,
		},
	}
	c, err := NewBytes[string](cfg)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		func(c *Config) { c.Limits.ControlQueueSize = 1 })
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
	cfg := Config{
		MaxEntries:     10,
		MaxBytes:       1 << 20,
		MaxResultBytes: 1000,
		Limits: LimitsConfig{
			MaxConcurrentLoads: 7,
		},
	}
	cfg.applyDefaults()
	if cfg.Runtime.RoundSoftTimeBudget <= 0 {
		t.Fatalf("RoundSoftTimeBudget default: %v", cfg.Runtime.RoundSoftTimeBudget)
	}
	if cfg.Limits.ResponseBytes != 7000 {
		t.Fatalf("ResponseBytes default must be MaxConcurrentLoads*MaxResultBytes, got %d", cfg.Limits.ResponseBytes)
	}
	// 全默认：256×4MiB = 1GiB。
	cfg2 := Config{
		MaxEntries: 10,
		MaxBytes:   1 << 20,
	}
	cfg2.applyDefaults()
	if cfg2.Limits.ResponseBytes != int64(cfg2.Limits.MaxConcurrentLoads)*cfg2.MaxResultBytes {
		t.Fatalf("full default ResponseBytes: %d", cfg2.Limits.ResponseBytes)
	}
}

// TestNsByIDSnapshot（P2 回归）：命名空间快照按 id 无锁读取，
// 越界返回 nil 而不 panic。
func TestNsByIDSnapshot(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
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

// TestEntryRegionCoversQuota：每分片条目区必须覆盖字节额度与最大条目，并带碎片/退休余量。
func TestEntryRegionCoversQuota(t *testing.T) {
	cfg := Config{
		Shards:     4,
		MaxEntries: 64,
		MaxBytes:   1 << 20,
	}
	cfg.applyDefaults()
	for _, s := range envOfShards(t, cfg) {
		need := uintptr(s.byteQuota.Load()) + uintptr(cfg.MaxEntryBytes)
		if s.reg.size < need {
			t.Fatalf("entry region %d < required %d", s.reg.size, need)
		}
	}
}

func envOfShards(t *testing.T, cfg Config) []*shard[string] {
	t.Helper()
	cfg.Namespaces = map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 2}}
	cfg.Runtime.DisableTicker = true
	cfg.Runtime.Executor = NewManualExecutor()
	cfg.Runtime.Clock = NewManualClock(1_000_000_000)
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c.c.shards
}

// ---- 拥有型句柄（I9 / GetOwned）----

// TestGetOwnedBasics 覆盖 GetOwned 的 hit / miss / closed 三态与
// Handle.Value / Release 幂等（README：释放前该条目不会被回收）。
func TestGetOwnedBasics(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
				if limit == "handles" {
					c.Limits.MaxOwnedHandles = 1
				} else {
					c.Limits.MaxOwnedPinsPerEntry = 1
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1},
		func(c *Config) { c.Limits.MaxOwnedPinsPerEntry = 2 })
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	for _, s := range env.c.c.shards {
		if s.retiredCount.Load() != 0 || s.retireHead != 0 {
			t.Fatalf("retired chain not drained: %d", s.retiredCount.Load())
		}
	}
	if st.ResidentEntries != 0 {
		t.Fatalf("residual residents: %d", st.ResidentEntries)
	}
}

// ---- key 上限 ----

// TestKeyTooLarge 覆盖全部入口的 ErrKeyTooLarge 契约。
func TestKeyTooLarge(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
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

// ---- 计费 ----

// TestBlockBillingPerLayout 覆盖字节串与定长两种布局的计费口径：头 + 8 字节对齐 key + 8 字节对齐 value。
func TestBlockBillingPerLayout(t *testing.T) {
	if got := bytesWeigh[string]("ab", "cde"); got != EntryHeaderBytes+8+8 {
		t.Fatalf("string weigh: %d", got)
	}
	if got := bytesWeigh[[]byte]("ab", make([]byte, 0, 5)); got != EntryHeaderBytes+8 {
		t.Fatalf("bytes weigh uses len, not cap: %d", got)
	}
	if got := flatWeigh[int]("ab", 42); got != EntryHeaderBytes+8+8 {
		t.Fatalf("flat weigh: %d", got)
	}
	if got := flatWeigh[[3]int32]("abcdefghi", [3]int32{}); got != EntryHeaderBytes+16+16 {
		t.Fatalf("flat array weigh: %d", got)
	}
}

// TestNewObjectWeigh 覆盖对象缓存的自定义计费：驻留字节按调用方口径。
func TestNewObjectWeigh(t *testing.T) {
	cfg := smallConfig(2, 16)
	ex := cfg.Runtime.Executor.(*ManualExecutor)
	const fixed = 7
	c, err := NewObject[string](cfg, func(key, value string) int64 { return fixed })
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

// TestConstructorsValidateValueType：NewFlat 拒绝含指针类型，NewObject 要求计费函数。
func TestConstructorsValidateValueType(t *testing.T) {
	type obj struct{ p *int }
	if _, err := NewFlat[*obj](smallConfig(2, 16)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewFlat[*obj]: err=%v, want ErrInvalidConfig", err)
	}
	if _, err := NewFlat[obj](smallConfig(2, 16)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewFlat[obj]: err=%v, want ErrInvalidConfig", err)
	}
	if _, err := NewFlat[string](smallConfig(2, 16)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewFlat[string] must be rejected (use NewBytes): %v", err)
	}
	if _, err := NewObject[*obj](smallConfig(2, 16), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewObject without weigh: %v", err)
	}
	if _, err := NewObject[*obj](smallConfig(2, 16), func(string, *obj) int64 { return 64 }); err != nil {
		t.Fatalf("NewObject[*obj]: %v", err)
	}
	type point struct{ X, Y int32 }
	for _, ok := range []bool{
		func() bool { _, err := NewBytes[string](smallConfig(2, 16)); return err == nil }(),
		func() bool { _, err := NewBytes[[]byte](smallConfig(2, 16)); return err == nil }(),
		func() bool { _, err := NewFlat[int](smallConfig(2, 16)); return err == nil }(),
		func() bool { _, err := NewFlat[point](smallConfig(2, 16)); return err == nil }(),
	} {
		if !ok {
			t.Fatal("supported value type rejected")
		}
	}
}

// TestNonPositiveWeightIsInvalidWeight：weigher 返回 <=0 与“超过 MaxResultBytes”是两类契约错误。
func TestNonPositiveWeightIsInvalidWeight(t *testing.T) {
	cfg := smallConfig(2, 16)
	ex := cfg.Runtime.Executor.(*ManualExecutor)
	c, err := NewObject[string](cfg, func(string, string) int64 { return 0 })
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
	cfg := smallConfig(2, 16)
	ex := cfg.Runtime.Executor.(*ManualExecutor)
	c, err := NewBytes[string](cfg)
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

// TestOwnedHitNoSpuriousBusyUnderContention：拥有型命中的句柄预留与条目持有计数都是无等待的，
// 只要配额充足，同一热 key 上的并发 GetOwned 不得因 CAS 竞争返回伪 BUSY；
// 全部释放后句柄预算与 ownedPins 精确归零。
func TestOwnedHitNoSpuriousBusyUnderContention(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
		c.Limits.MaxOwnedHandles = 1024
		c.Limits.MaxOwnedPinsPerEntry = 1024
		c.Limits.ReaderSlotsPerShard = 64
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, nil)
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

// modeOf 把布尔开关映射为运行模式，供表驱动用例复用。
func modeOf(observe bool) Mode {
	if observe {
		return ModeObserve
	}
	return ModeCache
}

// retiredPending 返回所有分片退休链上尚未回收的条目数。
func retiredPending(env *testEnv) int64 {
	var n int64
	for _, s := range env.c.c.shards {
		n += s.retiredCount.Load()
	}
	return n
}
