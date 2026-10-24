package ahkc

import (
	"context"
	"math"
	"testing"
	"time"
)

func newCostTable(t testing.TB, cfg CostTableConfig) *CostTableModel {
	t.Helper()
	m, err := NewCostTableModel(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// ---- 估计器三态与污染窗口（A.4）----

// TestEstimateModeString 覆盖三态的调试字符串（可观测性）。
func TestEstimateModeString(t *testing.T) {
	for m, want := range map[estimateMode]string{
		estMeasured: "MEASURED", estPrior: "PRIOR", estUnavailable: "UNAVAILABLE", estExpired: "EXPIRED",
	} {
		if got := m.String(); got != want {
			t.Fatalf("mode %d: %q, want %q", m, got, want)
		}
	}
}

// TestManualClockAndExecutor 覆盖测试支撑类型：时钟 Set 与执行器 Pending。
func TestManualClockAndExecutor(t *testing.T) {
	clk := NewManualClock(1000)
	clk.Set(2000)
	if clk.Now() != 2000 {
		t.Fatalf("Set: %d", clk.Now())
	}
	ex := NewManualExecutor()
	ran := false
	ex.TrySubmit(func() { ran = true })
	if ex.Pending() != 1 {
		t.Fatalf("pending: %d", ex.Pending())
	}
	if ex.Drain(10) != 1 || !ran {
		t.Fatal("drain must run the task")
	}
	if ex.Pending() != 0 {
		t.Fatalf("pending after drain: %d", ex.Pending())
	}
}

func estTestConfig() Config {
	cfg := Config{}
	cfg.applyDefaults()
	cfg.ValueDelta = 10 * time.Second
	cfg.NMin = 8
	return cfg
}

func TestEstimatePriorAndUnavailable(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	vs := newValueStats(4, 64, clk.Now())
	cfg := estTestConfig()
	item := estimateItem{policyHash: 12345, hardDeadline: clk.Now() + int64(2*time.Hour)}
	model := hitCountModel{version: 1}

	// 无样本、无先验 → UNAVAILABLE（延后新填充）。
	nsCfg := NamespaceConfig{TTL: time.Minute}
	if est := vs.estimate(item, clk.Now(), &nsCfg, model, &cfg); est.mode != estUnavailable {
		t.Fatalf("cold without prior must be UNAVAILABLE, got %v", est.mode)
	}
	// 有先验 → PRIOR，G = λ × min(W, 剩余 TTL)。
	nsCfg.Prior = &Prior{Lambda: 5, BuiltAt: clk.Now()}
	est := vs.estimate(item, clk.Now(), &nsCfg, model, &cfg)
	if est.mode != estPrior {
		t.Fatalf("expected PRIOR, got %v", est.mode)
	}
	if want := 5 * 60.0; math.Abs(est.G-want) > 1e-9 {
		t.Fatalf("G = %v, want %v", est.G, want)
	}
	// 先验超龄 → UNAVAILABLE。
	clk.Advance(11 * time.Minute)
	if est := vs.estimate(item, clk.Now(), &nsCfg, model, &cfg); est.mode != estUnavailable {
		t.Fatalf("aged prior must be UNAVAILABLE, got %v", est.mode)
	}
	// 已过 hardDeadline → EXPIRED。
	exp := estimateItem{policyHash: 1, hardDeadline: clk.Now() - 1}
	if est := vs.estimate(exp, clk.Now(), &nsCfg, model, &cfg); est.mode != estExpired {
		t.Fatalf("expired item must be EXPIRED, got %v", est.mode)
	}
}

func TestEstimateMeasured(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	vs := newValueStats(4, 64, clk.Now())
	cfg := estTestConfig()
	delta := int64(cfg.ValueDelta)
	// 暖机后（builtAt+2Δv）按事件时间写入 10 个样本（invP=1）。
	clk.Advance(time.Duration(2 * delta))
	ph := uint64(777)
	for i := 0; i < 10; i++ {
		if !vs.addSample(sample{policyHash: ph, eventTime: clk.Now(), invP: 1}, clk.Now(), delta) {
			t.Fatal("sample rejected")
		}
	}
	nsCfg := NamespaceConfig{TTL: time.Minute}
	item := estimateItem{policyHash: ph, hardDeadline: clk.Now() + int64(time.Minute)}
	est := vs.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg)
	if est.mode != estMeasured {
		t.Fatalf("expected MEASURED, got %v", est.mode)
	}
	// λ = 10/10s = 1/s；G = 1 × min(60s, 60s) = 60。
	if want := 60.0; math.Abs(est.G-want) > 1e-9 {
		t.Fatalf("G = %v, want %v", est.G, want)
	}
	// 样本不足 NMin 且无先验 → UNAVAILABLE。
	vs2 := newValueStats(4, 64, clk.Now())
	vs2.addSample(sample{policyHash: ph, eventTime: clk.Now(), invP: 1}, clk.Now(), delta)
	if est := vs2.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg); est.mode != estUnavailable {
		t.Fatalf("below NMin must be UNAVAILABLE, got %v", est.mode)
	}
}

func TestEstimatePollutionWindows(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	vs := newValueStats(4, 64, clk.Now())
	cfg := estTestConfig()
	delta := int64(cfg.ValueDelta)
	nsCfg := NamespaceConfig{TTL: time.Minute, Prior: &Prior{Lambda: 5, BuiltAt: clk.Now()}}
	item := estimateItem{policyHash: 9, hardDeadline: clk.Now() + int64(10*time.Minute)}

	// 丢样污染（faultFlag 由 drainSamples 记入 lastFaultAt）：2Δv 内 UNAVAILABLE。
	vs.lastFaultAt = clk.Now()
	if est := vs.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg); est.mode != estUnavailable {
		t.Fatal("fault window must be UNAVAILABLE")
	}
	clk.Advance(time.Duration(2*delta + 1))
	if est := vs.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg); est.mode != estPrior {
		t.Fatalf("after fault window must recover, got %v", est.mode)
	}
	// 滞后污染窗口同理。
	vs.lastLagViolation = clk.Now()
	if est := vs.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg); est.mode != estUnavailable {
		t.Fatal("lag window must be UNAVAILABLE")
	}
	// 来自未来的样本：addSample 记 fault 并拒绝。
	clk.Advance(time.Duration(2*delta + 1))
	if vs.addSample(sample{policyHash: 9, eventTime: clk.Now() + delta, invP: 1}, clk.Now(), delta) {
		t.Fatal("future sample must be rejected")
	}
	if est := vs.estimate(item, clk.Now(), &nsCfg, hitCountModel{1}, &cfg); est.mode != estUnavailable {
		t.Fatal("future sample must pollute window")
	}
	// 非有限 invP 同样污染。
	clk.Advance(time.Duration(2*delta + 1))
	if vs.addSample(sample{policyHash: 9, eventTime: clk.Now(), invP: math.NaN()}, clk.Now(), delta) {
		t.Fatal("NaN invP must be rejected")
	}
}

// TestEstimateModelFailure 模型返回 ok=false / 非有限值 → UNAVAILABLE（不在线推理）。
func TestEstimateModelFailure(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	vs := newValueStats(4, 64, clk.Now())
	cfg := estTestConfig()
	nsCfg := NamespaceConfig{TTL: time.Minute, Prior: &Prior{Lambda: 5, BuiltAt: clk.Now()}}
	item := estimateItem{policyHash: 3, hardDeadline: clk.Now() + int64(time.Minute)}
	bad := failModel{}
	if est := vs.estimate(item, clk.Now(), &nsCfg, bad, &cfg); est.mode != estUnavailable {
		t.Fatal("model failure must yield UNAVAILABLE")
	}
	if est := vs.estimate(item, clk.Now(), &nsCfg, nil, &cfg); est.mode != estUnavailable {
		t.Fatal("nil model must yield UNAVAILABLE")
	}
}

type failModel struct{}

func (failModel) Version() uint64 { return 1 }
func (failModel) Lookup(float64, uint32) (float64, float64, float64, float64, float64, bool) {
	return 0, 0, 0, 0, 0, false
}

// ---- CostTableModel ----

// TestHitCountModelBounds 覆盖内置默认模型的边界契约：
// 负请求率拒绝；正常输入 r=λ、c=1、其余为 0。
func TestHitCountModelBounds(t *testing.T) {
	m := hitCountModel{version: 7}
	if m.Version() != 7 {
		t.Fatalf("version: %d", m.Version())
	}
	if _, _, _, _, _, ok := m.Lookup(-1, 0); ok {
		t.Fatal("negative lambda must be rejected")
	}
	r, c, h, k, K, ok := m.Lookup(5, 3)
	if !ok || r != 5 || c != 1 || h != 0 || k != 0 || K != 0 {
		t.Fatalf("lookup: r=%v c=%v h=%v k=%v K=%v ok=%v", r, c, h, k, K, ok)
	}
}

// TestSetModelSwitchesFillDecisions 覆盖 SetModel 原子切换：
// 失败模型下新填充 NO_ESTIMATE；已驻留条目不受影响；切回后恢复发布。
func TestSetModelSwitchesFillDecisions(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior}, nil)
	// 默认模型：正常发布。
	if _, err := env.fetch("k1", okLoader("v1")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("k1"); !ok {
		t.Fatal("default model must publish")
	}
	// 切换为必然失败的模型：新填充统一 NO_ESTIMATE。
	env.ns.SetModel(failModel{})
	if _, err := env.fetch("k2", okLoader("v2")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("k2"); ok {
		t.Fatal("failed model must block new fills")
	}
	if env.c.Stats().FillRejected["NO_ESTIMATE"] == 0 {
		t.Fatalf("expected NO_ESTIMATE rejection: %v", env.c.Stats().FillRejected)
	}
	// 已驻留条目不受模型切换影响。
	if v, ok, _ := env.ns.Get("k1"); !ok || v != "v1" {
		t.Fatalf("resident entry must be unaffected: %v %v", v, ok)
	}
	// 切换回可用模型：恢复发布。
	env.ns.SetModel(hitCountModel{version: 2})
	if _, err := env.fetch("k3", okLoader("v3")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if v, ok, _ := env.ns.Get("k3"); !ok || v != "v3" {
		t.Fatalf("restored model must publish: %v %v", v, ok)
	}
}

func TestCostTableModelBasics(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	m := newCostTable(t, CostTableConfig{MergeFactor: 1, Classes: 4, Alpha: 0.25, Clock: clk})
	v0 := m.Version()
	m.Observe(1, 10)
	if m.Version() == v0 {
		t.Fatal("Observe must bump version")
	}
	_, c, _, _, _, ok := m.Lookup(3, 1)
	if !ok || c != 10 {
		t.Fatalf("first observation: c=%v ok=%v", c, ok)
	}
	m.Observe(1, 20) // EWMA: 10*0.75 + 20*0.25 = 12.5
	if _, c, _, _, _, _ = m.Lookup(3, 1); math.Abs(c-12.5) > 1e-9 {
		t.Fatalf("EWMA: c=%v want 12.5", c)
	}
	// r = λ × 显式校准的 MergeFactor=1。
	if r, _, _, _, _, _ := m.Lookup(3, 1); r != 3 {
		t.Fatalf("r=%v want 3", r)
	}
	// 越界类别与非法 λ → ok=false。
	if _, _, _, _, _, ok := m.Lookup(1, 99); ok {
		t.Fatal("class out of range must fail")
	}
	if _, _, _, _, _, ok := m.Lookup(math.NaN(), 1); ok {
		t.Fatal("NaN lambda must fail")
	}
	// 非法观测被忽略且不动版本。
	v := m.Version()
	m.Observe(1, math.NaN())
	m.Observe(1, -5)
	m.Observe(99, 1)
	if m.Version() != v {
		t.Fatal("invalid observations must not bump version")
	}
}

func TestCostTableModelAgingAndFallback(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	// 无先验：观测过期 → UNAVAILABLE。
	m := newCostTable(t, CostTableConfig{MergeFactor: 1, Classes: 2, MaxAge: time.Minute, FallbackCost: -1, Clock: clk})
	if _, _, _, _, _, ok := m.Lookup(1, 0); ok {
		t.Fatal("no observation, no fallback must fail")
	}
	m.Observe(0, 7)
	clk.Advance(2 * time.Minute)
	if _, _, _, _, _, ok := m.Lookup(1, 0); ok {
		t.Fatal("aged observation without fallback must fail")
	}
	// 有先验：过期回退先验。
	m2 := newCostTable(t, CostTableConfig{MergeFactor: 1, Classes: 2, MaxAge: time.Minute, FallbackCost: 3, Clock: clk})
	m2.Observe(0, 7)
	clk.Advance(2 * time.Minute)
	_, c, _, _, _, ok := m2.Lookup(1, 0)
	if !ok || c != 3 {
		t.Fatalf("fallback: c=%v ok=%v want 3", c, ok)
	}
}

// TestCostTableAutoObserve 库内自动观测：加载完成路径以墙钟时长更新模型。
func TestCostTableAutoObserve(t *testing.T) {
	clk := NewManualClock(1_000_000_000)
	model := newCostTable(t, CostTableConfig{MergeFactor: 1, Classes: 4, Clock: clk})
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior, Model: model, CostClass: 2}, nil)
	v0 := model.Version()
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if model.Version() == v0 {
		t.Fatal("auto-observe must record loader completion")
	}
	if _, c, _, _, _, ok := model.Lookup(1, 2); !ok || c != 0 {
		// ManualClock 下时长为 0：观测存在且 c=0。
		t.Fatalf("auto-observed cost: c=%v ok=%v", c, ok)
	}
	// 关闭自动观测后版本不变。
	model2 := newCostTable(t, CostTableConfig{MergeFactor: 1, Classes: 4, Clock: clk, DisableAutoObserve: true})
	env2 := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior, Model: model2}, nil)
	if _, err := env2.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env2.drive()
	if model2.Version() != 0 {
		t.Fatal("DisableAutoObserve must suppress auto observation")
	}
}

// ---- I11：一次逻辑请求只采样一次、只记一次热度 ----

func TestSampleOncePerRequest(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 100, Prior: testPrior}, nil) // SampleP=1 全采样
	before := env.c.Stats().Samples
	r, err := env.ns.BeginRequest("k")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := env.ns.GetOn(r); ok {
		t.Fatal("miss expected")
	}
	env.ns.GetOn(r) // 同请求第二次读
	// 同请求加载路径（手动执行器：先异步登记，再驱动，再等待）。
	fut, err := env.ns.FetchAsyncOn(r, okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, err := fut.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := env.c.Stats().Samples - before; got != 1 {
		t.Fatalf("one logical request must sample once, got %d", got)
	}
	// 热度只记一次（I11）：H==1。
	delta := int64(env.c.c.cfg.HeatDelta)
	now := env.clk.Now()
	h, ok := r.sh.gate.read(r.keyHash, bucketIndex(now, delta), bucketPhi(now, delta))
	if !ok || h != 1 {
		t.Fatalf("heat must be recorded once, h=%v ok=%v", h, ok)
	}
}
