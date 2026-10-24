package ahkc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// 初始化契约与命名空间管理错误分支。
// 正交说明：不重复 cache_test.go 的运行期语义，只验证"非法配置拒绝创建"
// 与命名空间注册/失效入口的确定性错误。

func validConfig() Config {
	return Config{
		ShardCount:       4,
		MaxEntries:       64,
		MaxResidentBytes: 1 << 20,
		HeatDelta:        time.Second,
		ValueDelta:       10 * time.Second,
		DisableTicker:    true,
		Executor:         NewManualExecutor(),
		Clock:            NewManualClock(1_000_000_000),
		FillRatePerSec:   1e6,
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 2, Tout: 1, Prior: testPrior},
		}, Advanced: Advanced{WheelTick: 100 * time.Millisecond, WheelSlots: 16, WheelLevels: 4},
	}
}

// TestConfigValidationRejects 覆盖 validate 的全部可达拒绝分支：
// 每个用例只改一项，验证该项约束独立生效（正交）。
func TestConfigValidationRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"MaxEntries<=0", func(c *Config) { c.MaxEntries = 0 }},
		{"MaxResidentBytes<=0", func(c *Config) { c.MaxResidentBytes = 0 }},
		{"MaxEntries<ShardCount", func(c *Config) { c.MaxEntries = 2 }},
		{"HeatCap超位宽", func(c *Config) { c.HeatCap = heatCountMax + 1 }},
		{"VictimLimit>8", func(c *Config) { c.VictimLimit = 9 }},
		{"MaxSubsetChecks>2^VictimLimit", func(c *Config) { c.VictimLimit = 3; c.MaxSubsetChecks = 16 }},
		{"Grace<2HeatDelta", func(c *Config) { c.Grace = time.Second }},
		{"ColdFor<HeatDelta", func(c *Config) { c.ColdFor = time.Millisecond }},
		{"SampleP>1", func(c *Config) { c.SampleP = 1.5 }},
		{"时间轮跨度不足", func(c *Config) { c.MaxTTL = 24 * time.Hour }},
		{"时间轮跨度恰差滞后边界一tick", func(c *Config) {
			// K^L*tick=1.6s；旧公式要求 1.1s+(4+1)*tick=1.6s 通过，但最大 delta 恰等于 K^L 不可调度。
			c.WheelSlots, c.WheelLevels, c.MaxWheelLag = 4, 2, 4
			c.MaxTTL, c.HandoffTimeout = 1100*time.Millisecond, 500*time.Millisecond
			c.Namespaces["t"] = NamespaceConfig{TTL: time.Second, Tin: 2, Tout: 1, Prior: testPrior}
		}},
		{"RetireBytesPerRound<MaxEntryBytes", func(c *Config) {
			c.RetireBytesPerRound, c.ReclaimBytesPerRound = 1, 1<<20
		}},
		{"HandoffTimeout>=MaxTTL", func(c *Config) { c.HandoffTimeout = time.Hour }},
		{"MaxEntryBytes>MaxResidentBytes", func(c *Config) { c.MaxEntryBytes = 2 << 20 }},
		{"PendingBytes<MaxEntryBytes", func(c *Config) { c.PendingBytes = 1; c.MaxEntryBytes = 1 << 10 }},
		{"RetiredBytes<MaxEntryBytes", func(c *Config) { c.RetiredBytes = 1; c.MaxEntryBytes = 1 << 10 }},
		{"ResponseBytes<MaxResultBytes", func(c *Config) { c.ResponseBytes = 1; c.MaxResultBytes = 4096 }},
		{"GateEpsilon未成对", func(c *Config) { c.GateEpsilon = 0.001 }},
		{"GateDelta越界", func(c *Config) { c.GateEpsilon = 0.001; c.GateDelta = 1.5 }},
		{"GateWidth超上界", func(c *Config) { c.GateWidth = 1<<22 + 1 }},
		{"HotHistoryWindows负值", func(c *Config) { c.HotHistoryWindows = -1 }},
		{"HotHistory缺tracker", func(c *Config) { c.HotHistoryWindows = 2 }},
		{"OnHotWindow缺tracker", func(c *Config) { c.OnHotWindow = func(HotSnapshot) {} }},
		{"命名空间缺TTL", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{Tin: 2}
		}},
		{"命名空间TTL>MaxTTL", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{TTL: 2 * time.Hour, Tin: 2}
		}},
		{"命名空间Tout>=Tin", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, Tin: 2, Tout: 2}
		}},
		{"命名空间SampleP非法", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, SampleP: -1}
		}},
	}
	for _, tc := range cases {
		cfg := validConfig()
		tc.mutate(&cfg)
		if _, err := New[string](cfg); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err=%v, want ErrInvalidConfig", tc.name, err)
		}
	}
}

// TestConfigValidateDefensiveBranches 覆盖被 applyDefaults 归一化、
// 无法经 New 触达的防御分支（白盒直调 validate）。
func TestConfigValidateDefensiveBranches(t *testing.T) {
	check := func(name string, mutate func(*Config)) {
		t.Helper()
		cfg := validConfig()
		cfg.applyDefaults()
		mutate(&cfg)
		if err := cfg.validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	check("HeatCap==0", func(c *Config) { c.HeatCap = 0 })
	check("ShardCount非2的幂", func(c *Config) { c.ShardCount = 3 })
	check("ProbeLimit<=0", func(c *Config) { c.ProbeLimit = 0 })
	// 合法基线必须通过（防御分支不误伤）。
	cfg := validConfig()
	cfg.applyDefaults()
	cfg.HeatCap = defaultHeatCap(cfg) // New 在 validate 前补默认
	if err := cfg.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// TestGateErrorBudgetDerivation 覆盖门卫误差预算推导：
// 有效 ε/δ 对覆盖默认尺寸（w=⌈e/ε⌉、d=⌈ln(1/δ)⌉），端到端可创建。
func TestGateErrorBudgetDerivation(t *testing.T) {
	cfg := validConfig()
	cfg.GateEpsilon = 0.001
	cfg.GateDelta = 0.001
	cfg.applyDefaults()
	if cfg.GateWidth != 2719 || cfg.GateRows != 7 {
		t.Fatalf("derived gate = %dx%d, want 7x2719", cfg.GateRows, cfg.GateWidth)
	}
	c, err := New[string](cfg)
	if err != nil {
		t.Fatalf("error-budget config rejected: %v", err)
	}
	if got := c.c.cfg; got.GateRows != 7 || got.GateWidth != 2719 {
		t.Fatalf("runtime gate = %dx%d, want 7x2719", got.GateRows, got.GateWidth)
	}
	// 未设置 ε/δ 时保持既有默认 4x4096。
	cfg = validConfig()
	cfg.applyDefaults()
	if cfg.GateRows != 4 || cfg.GateWidth != 4096 {
		t.Fatalf("default gate = %dx%d, want 4x4096", cfg.GateRows, cfg.GateWidth)
	}
}

// TestRegisterNamespaceErrors 覆盖命名空间注册的全部错误分支与上限。
func TestRegisterNamespaceErrors(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior},
		func(c *Config) { c.MaxNamespaces = 2 })
	nsCfg := NamespaceConfig{TTL: time.Minute, Tin: 2}
	if _, err := env.c.RegisterNamespace("", nsCfg); err == nil {
		t.Fatal("empty name must be rejected")
	}
	if _, err := env.c.RegisterNamespace("t", nsCfg); err == nil {
		t.Fatal("duplicate name must be rejected")
	}
	if _, err := env.c.RegisterNamespace("x", NamespaceConfig{Tin: 2}); err == nil {
		t.Fatal("missing TTL must be rejected")
	}
	if _, err := env.c.RegisterNamespace("x", NamespaceConfig{TTL: 2 * time.Hour}); err == nil {
		t.Fatal("TTL beyond MaxTTL must be rejected")
	}
	// 上限：初始 1 个 + 新注册 1 个 = MaxNamespaces(2)。
	ns2, err := env.c.RegisterNamespace("x", nsCfg)
	if err != nil {
		t.Fatalf("second namespace must register: %v", err)
	}
	if _, err := env.c.RegisterNamespace("y", nsCfg); err == nil {
		t.Fatal("namespace limit must be enforced")
	}
	// 新命名空间可正常工作（非仅是注册成功）。
	if _, err := ns2.FetchAsync("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := env.c.Namespace("nope"); !errors.Is(err, ErrNoSuchNamespace) {
		t.Fatalf("unknown namespace: %v", err)
	}
}

// TestInvalidateNamespaceErrors 覆盖命名空间失效入口的错误分支：
// 未知命名空间 / 队列满 BUSY / 关闭后 ErrClosed。
func TestInvalidateNamespaceErrors(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2, Prior: testPrior}, nil)
	if err := env.c.InvalidateNamespace(context.Background(), "nope"); !errors.Is(err, ErrNoSuchNamespace) {
		t.Fatalf("unknown namespace: %v", err)
	}
	// 关闭后：ErrClosed。
	closeManual(t, env.c, env.ex)
	if err := env.c.InvalidateNamespace(context.Background(), "t"); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close: %v", err)
	}
}

// TestInvalidateNamespaceQueueFull 覆盖全局命名空间控制队列满时的 BUSY：
// 不排队、不阻塞，未受理操作不产生效果（toggleExecutor 拒绝排水提交）。
func TestInvalidateNamespaceQueueFull(t *testing.T) {
	ex := &toggleExecutor{}
	ex.fail.Store(true)
	cfg := validConfig()
	cfg.ControlQueueSize = 1
	cfg.Executor = ex
	c, err := New[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		errCh <- c.InvalidateNamespace(ctx, "t")
	}()
	waitReal(t, "op enqueued", func() bool { return len(c.c.nsControlQueue) == 1 })
	// 队列已满：第二个操作必须立即 BUSY。
	if err := c.InvalidateNamespace(context.Background(), "t"); !errors.Is(err, ErrBusy) {
		t.Fatalf("queue full must yield ErrBusy, got %v", err)
	}
	// 恢复执行器并手动驱动：第一个操作完成（不因排水提交失败而丢失）。
	ex.fail.Store(false)
	c.MaintenanceStep()
	if err := <-errCh; err != nil {
		t.Fatalf("first op must complete: %v", err)
	}
	if c.c.nsByName["t"].gen.Load() != 2 {
		t.Fatalf("generation must advance: %d", c.c.nsByName["t"].gen.Load())
	}
}

// TestValidationErrorMessagesAreActionable 校验错误信息包含字段名
// （生产部署排障可用性）。
func TestValidationErrorMessagesAreActionable(t *testing.T) {
	cfg := validConfig()
	cfg.MaxEntries = 0
	_, err := New[string](cfg)
	if err == nil || !strings.Contains(err.Error(), "MaxEntries") {
		t.Fatalf("error must name the offending field: %v", err)
	}
}
