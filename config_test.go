package ahkc

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 初始化契约与命名空间管理错误分支。
// 正交说明：不重复 cache_test.go 的运行期语义，只验证"非法配置拒绝创建"
// 与命名空间注册/失效入口的确定性错误。

func validConfig() Config {
	return Config{
		Shards:     4,
		MaxEntries: 64,
		MaxBytes:   1 << 20,
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 2},
		},
		Heat: HeatConfig{
			Delta: time.Second,
		},
		Runtime: RuntimeConfig{
			DisableTicker: true,
			Executor:      NewManualExecutor(),
			Clock:         NewManualClock(1_000_000_000),
		},
	}
}

// smallConfig 是 validConfig 的变体：指定分片数与条目额度，命名空间 "t" 的准入阈值为 1（首次请求即有资格）。
func smallConfig(shards int, entries int64) Config {
	cfg := validConfig()
	cfg.Shards, cfg.MaxEntries = shards, entries
	cfg.Namespaces["t"] = NamespaceConfig{TTL: time.Minute, Tin: 1}
	return cfg
}

// TestConfigValidationRejects 覆盖 validate 的全部可达拒绝分支：
// 每个用例只改一项，验证该项约束独立生效（正交）。
func TestConfigValidationRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"MaxEntries<=0", func(c *Config) { c.MaxEntries = 0 }},
		{"MaxBytes<=0", func(c *Config) { c.MaxBytes = 0 }},
		{"MaxEntries<Shards", func(c *Config) { c.MaxEntries = 2 }},
		{"VictimLimit>8", func(c *Config) { c.Evict.VictimLimit = 9 }},
		{"MaxEntryBytes>MaxBytes", func(c *Config) { c.MaxEntryBytes = 2 << 20 }},
		{"PendingBytes<MaxEntryBytes", func(c *Config) { c.Limits.PendingBytes = 1; c.MaxEntryBytes = 1 << 10 }},
		{"ResponseBytes<MaxResultBytes", func(c *Config) { c.Limits.ResponseBytes = 1; c.MaxResultBytes = 4096 }},
		{"GateCells超上界", func(c *Config) { c.Heat.GateCells = 1<<26 + 1 }},
		{"MaxKeyBytes超16位长度", func(c *Config) { c.MaxKeyBytes = 1 << 16 }},
		{"MaxResultBytes超32位计费", func(c *Config) { c.MaxResultBytes = 1 << 32 }},
		{"MaxOwnedPinsPerEntry超32位计数", func(c *Config) { c.Limits.MaxOwnedPinsPerEntry = 1<<30 + 1 }},
		{"Hot.Windows负值", func(c *Config) { c.Hot.Windows = -1 }},
		{"HotHistory缺tracker", func(c *Config) { c.Hot.Windows = 2 }},
		{"Hot.OnWindow缺tracker", func(c *Config) { c.Hot.OnWindow = func(HotSnapshot) {} }},
		{"命名空间缺TTL", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{Tin: 2}
		}},
		{"命名空间TTL>1年", func(c *Config) {
			c.Namespaces["t"] = NamespaceConfig{TTL: 2 * maxTTL, Tin: 2}
		}},
	}
	for _, tc := range cases {
		cfg := validConfig()
		tc.mutate(&cfg)
		if _, err := NewBytes[string](cfg); !errors.Is(err, ErrInvalidConfig) {
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
	check("Shards非2的幂", func(c *Config) { c.Shards = 3 })
	check("Memory.LoadFactor>1", func(c *Config) { c.Memory.LoadFactor = 1.5 })
	// 合法基线必须通过（防御分支不误伤）。
	cfg := validConfig()
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}

// TestRegisterNamespaceErrors 覆盖命名空间注册的全部错误分支与上限。
func TestRegisterNamespaceErrors(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2},
		func(c *Config) { c.Limits.MaxNamespaces = 2 })
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
	if _, err := env.c.RegisterNamespace("x", NamespaceConfig{TTL: 2 * maxTTL}); err == nil {
		t.Fatal("TTL beyond the 1-year cap must be rejected")
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
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 2}, nil)
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
	cfg.Limits.ControlQueueSize = 1
	cfg.Runtime.Executor = ex
	c, err := NewBytes[string](cfg)
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
	_, err := NewBytes[string](cfg)
	if err == nil || !strings.Contains(err.Error(), "MaxEntries") {
		t.Fatalf("error must name the offending field: %v", err)
	}
}

// 32 位平台上区大小以 uintptr 保存：总预留超过地址空间必须被拒绝，而不是静默截断。
// 64 位平台的上限由单区 32GiB 约束，这里只在 32 位下断言。
func TestRegionBeyondAddressSpaceRejectedOn32Bit(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("only meaningful where int is 32 bits")
	}
	cfg := validConfig()
	cfg.Shards, cfg.MaxEntries, cfg.MaxBytes = 1, 1000, 4<<30
	if c, err := NewBytes[string](cfg); err == nil {
		_ = c.Close(context.Background())
		t.Fatal("a 4GiB region must be rejected on a 32-bit platform")
	} else if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("want ErrInvalidConfig, got %v", err)
	}
}
