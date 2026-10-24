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

// newBenchCache 构造基准环境：真实时钟、默认执行器、周期维护、近似不采样。
func newBenchCache(b *testing.B, entries int64, keys int) (*Cache[string], *Namespace[string]) {
	b.Helper()
	return newBenchCacheCfg(b, entries, keys, nil)
}

// newBenchCacheCfg 同 newBenchCache，并允许基准调整配置（例如启用请求热点追踪）。
func newBenchCacheCfg(b *testing.B, entries int64, keys int, tweak func(*Config)) (*Cache[string], *Namespace[string]) {
	b.Helper()
	cfg := Config{
		ShardCount:       64,
		MaxEntries:       entries,
		MaxResidentBytes: 1 << 30,
		SampleP:          1e-12,
		Namespaces: map[string]NamespaceConfig{
			"bench": {TTL: time.Hour, Tin: 1, Prior: &Prior{Lambda: 100}},
		}, Advanced: Advanced{ // 保留独立采样判断，近似隔离样本传输成本
			WheelTick: 100 * time.Millisecond, WheelSlots: 64, WheelLevels: 4},
	}
	if tweak != nil {
		tweak(&cfg)
	}
	c, err := New[string](cfg)
	if err != nil {
		b.Fatal(err)
	}
	ns, err := c.Namespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	loader := okLoader("v")
	workingSet := make([]string, keys)
	for i := range workingSet {
		workingSet[i] = fmt.Sprintf("key%d", i)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		ready := true
		for _, key := range workingSet {
			_, hit, err := ns.Get(key)
			if err != nil && !errors.Is(err, ErrBusy) {
				b.Fatal(err)
			}
			if err == nil && hit {
				continue
			}
			ready = false
			if _, err := ns.Fetch(context.Background(), key, loader); err != nil &&
				!errors.Is(err, ErrBusy) && !errors.Is(err, ErrOverloaded) {
				b.Fatal(err)
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			b.Fatalf("warmup did not publish working set: %+v", c.Stats())
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Close(ctx)
	})
	return c, ns
}

func BenchmarkGetHit(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := ns.Get("key255"); err != nil || !ok {
			b.Fatal("miss")
		}
	}
}

// Both paths hash the same key, pin a reader, check namespace/TTL, access the
// value and release protection. The index baseline omits heat/sampling/metrics.
func BenchmarkBorrowHit(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	var request Request[string]
	var borrowed Borrowed[string]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.BeginRequestOn(&request, "key255"); err != nil {
			b.Fatal(err)
		}
		if ok, err := ns.BorrowOn(&request, &borrowed); err != nil || !ok {
			b.Fatal("miss", err)
		}
		runtime.KeepAlive(borrowed.Value())
		borrowed.Release()
	}
}

func BenchmarkIndexHitProtected(b *testing.B) {
	c, ns := newBenchCache(b, 16384, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if c.c.state.Load() != cacheOpen {
			b.Fatal("closed")
		}
		hash := ns.ns.hashKey("key255")
		s := c.c.shardOf(hash)
		guard, ok := s.epoch.pin()
		if !ok {
			b.Fatal("reader busy")
		}
		found := s.idx.find("key255", hash, ns.ns.id)
		if !found.found || !found.e.readable.Load() || found.e.nsGen != ns.ns.gen.Load() || c.c.now() >= found.e.hardDeadline || c.c.state.Load() != cacheOpen {
			guard.unpin()
			b.Fatal("miss")
		}
		runtime.KeepAlive(found.e.value)
		guard.unpin()
	}
}

func BenchmarkGetHitParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 4096)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}
	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(ctr.Add(1)) % len(keys)
		for pb.Next() {
			if _, ok, err := ns.Get(keys[i]); err != nil || !ok {
				b.Fatal("miss")
			}
			i = (i + 1) % len(keys)
		}
	})
}

func BenchmarkGetMissParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 0)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("absent%d", i)
	}
	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(ctr.Add(1)) % len(keys)
		for pb.Next() {
			if _, ok, err := ns.Get(keys[i]); err != nil || ok {
				b.Fatal("unexpected hit")
			}
			i = (i + 1) % len(keys)
		}
	})
}

// BenchmarkFetchMissLoad 测量完整 miss→singleflight→加载→交付路径（Tin 极高，不缓存）。
func BenchmarkFetchMissLoad(b *testing.B) {
	cfg := Config{
		ShardCount: 64, MaxEntries: 16384, MaxResidentBytes: 1 << 30,
		SampleP: 1e-12,
		Namespaces: map[string]NamespaceConfig{
			"bench": {TTL: time.Hour, Tin: 1000}, // no prior or samples: no profitable fill can be estimated
		}, Advanced: Advanced{WheelTick: 100 * time.Millisecond, WheelSlots: 64, WheelLevels: 4},
	}
	c, err := New[string](cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Close(ctx)
	}()
	ns, _ := c.Namespace("bench")
	var ctr atomic.Uint64
	loader := okLoader("v")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := ctr.Add(1)
			for {
				_, err := ns.Fetch(context.Background(), fmt.Sprintf("load%d", n), loader)
				if err == nil {
					break
				}
				if !errors.Is(err, ErrBusy) && !errors.Is(err, ErrOverloaded) {
					b.Fatal(err)
				}
				runtime.Gosched()
			}
		}
	})
}

// TestGetHitZeroAlloc 普通值读取稳态 0 堆分配（栈上 Request）。
func TestGetHitZeroAlloc(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1, Prior: testPrior},
		func(c *Config) { c.SampleP = 1e-12 }) // 近似隔离样本传输分配
	if _, err := env.fetch("hit", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("hit"); !ok {
		t.Fatal("expected cached")
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, ok, err := env.ns.Get("hit"); err != nil || !ok {
			t.Fatal("miss")
		}
	})
	if allocs != 0 {
		t.Fatalf("hit path must be zero-alloc, got %.2f allocs/op", allocs)
	}
}

// BenchmarkGetOwnedHotKeyParallel 测量同一热 key 上并发拥有型命中：
// ownedPins 的写入不应使其他读者读取的只读字段所在缓存行失效。
func BenchmarkGetOwnedHotKeyParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h, ok, err := ns.GetOwned("key7")
			if err != nil || !ok {
				b.Fatal("miss")
			}
			h.Release()
		}
	})
}

// BenchmarkGetHotKeyParallel 同一热 key 上并发值读取（热度已饱和，读路径应只读共享该条目）。
func BenchmarkGetHotKeyParallel(b *testing.B) {
	_, ns := newBenchCache(b, 16384, 64)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, ok, err := ns.Get("key7"); err != nil || !ok {
				b.Fatal("miss")
			}
		}
	})
}

// BenchmarkFetchFillChurn 测量会真实触发填充、求解与替换的完整路径：
// 工作集（8192 key）远大于容量（1024 条），Tin=1、先验使收益为正，加载近乎瞬时。
func BenchmarkFetchFillChurn(b *testing.B) {
	cfg := Config{
		ShardCount: 16, MaxEntries: 1024, MaxResidentBytes: 1 << 28,
		SampleP: 1.0 / 64, FillRatePerSec: 1e9,
		Namespaces: map[string]NamespaceConfig{
			"bench": {TTL: time.Hour, Tin: 1, Prior: &Prior{Lambda: 100}},
		},
		Advanced: Advanced{WheelTick: 100 * time.Millisecond, WheelSlots: 64, WheelLevels: 4},
	}
	c, err := New[string](cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		c.Close(ctx)
	}()
	ns, _ := c.Namespace("bench")
	const keys = 8192
	names := make([]string, keys)
	for i := range names {
		names[i] = fmt.Sprintf("key%d", i)
	}
	loader := okLoader("value-payload")
	var ctr atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := ctr.Add(1) * 0x9E3779B97F4A7C15
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			for {
				_, err := ns.Fetch(context.Background(), names[x%keys], loader)
				if err == nil {
					break
				}
				if !errors.Is(err, ErrBusy) && !errors.Is(err, ErrOverloaded) {
					b.Fatal(err)
				}
				runtime.Gosched()
			}
		}
	})
	b.StopTimer()
	st := c.Stats()
	b.ReportMetric(float64(st.Hits)/float64(st.Hits+st.Misses+1)*100, "hit%")
}

// ---- 请求热点追踪基准（对比关闭 / q=1/1024 / q=1）----

func trackingTweak(q float64) func(*Config) {
	return func(c *Config) {
		c.ObserveRequestKeys, c.ObserveRequestSampleP = 64, q
		c.HotHistoryWindows = 60
		c.HotHistoryInterval = time.Minute
	}
}

func BenchmarkReadAllocations(b *testing.B) {
	for _, mode := range []string{"Get", "Fetch", "GetOwned", "FetchAsync", "OwnedBusy"} {
		b.Run(mode, func(b *testing.B) {
			_, ns := newBenchCacheCfg(b, 64, 1, func(c *Config) {
				c.ShardCount, c.MaxOwnedHandles = 1, 1
			})
			if mode == "OwnedBusy" {
				h, ok, err := ns.GetOwned("key0")
				if err != nil || !ok {
					b.Fatal("expected hit", err)
				}
				defer h.Release()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch mode {
				case "Get":
					if _, ok, err := ns.Get("key0"); err != nil || !ok {
						b.Fatal("expected hit", err)
					}
				case "Fetch":
					if _, err := ns.Fetch(context.Background(), "key0", nil); err != nil {
						b.Fatal(err)
					}
				case "GetOwned":
					h, ok, err := ns.GetOwned("key0")
					if err != nil || !ok {
						b.Fatal("expected hit", err)
					}
					h.Release()
				case "FetchAsync":
					f, err := ns.FetchAsync("key0", nil)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := f.Wait(context.Background()); err != nil {
						b.Fatal(err)
					}
				case "OwnedBusy":
					if h, ok, err := ns.GetOwned("key0"); h != nil || ok || !errors.Is(err, ErrBusy) {
						b.Fatal("expected busy", err)
					}
				}
			}
		})
	}
}

func BenchmarkInitialize(b *testing.B) {
	for _, only := range []bool{false, true} {
		b.Run(fmt.Sprintf("observeOnly=%v", only), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				c, err := New[string](Config{
					MaxEntries: 1024, MaxResidentBytes: 64 << 20,
					ObserveOnly: only, DisableTicker: true,
				})
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(c) // 无 ticker、任务或 I/O，构造对象由 GC 回收。
			}
		})
	}
}

func benchTracking(b *testing.B, keys int, hot bool, miss bool) {
	for _, mode := range []struct {
		name  string
		tweak func(*Config)
	}{
		{"off", nil},
		{"q1_1024", trackingTweak(1.0 / 1024)},
		{"q1", trackingTweak(1)},
	} {
		b.Run(mode.name, func(b *testing.B) {
			_, ns := newBenchCacheCfg(b, 16384, keys, func(c *Config) {
				c.SampleP = 1.0 / 1024 // 所有组使用相同的生产默认收益采样率。
				if mode.tweak != nil {
					mode.tweak(c)
				}
			})
			names := make([]string, 4096)
			for i := range names {
				if miss {
					names[i] = fmt.Sprintf("absent%d", i)
				} else {
					names[i] = fmt.Sprintf("key%d", i%max(keys, 1))
				}
			}
			var ctr atomic.Uint64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := int(ctr.Add(1)) % len(names)
				for pb.Next() {
					k := names[i]
					if hot {
						k = names[7%len(names)]
					}
					if _, ok, err := ns.Get(k); err != nil || ok == miss {
						b.Fatal("unexpected result")
					}
					i = (i + 1) % len(names)
				}
			})
		})
	}
}

// BenchmarkObserveGetHitParallel：常规命中读（M1，分散 key）。
func BenchmarkObserveGetHitParallel(b *testing.B) { benchTracking(b, 4096, false, false) }

// BenchmarkObserveGetHotKeyParallel：单热 key 命中读（M2，追踪锁集中在一个分片）。
func BenchmarkObserveGetHotKeyParallel(b *testing.B) { benchTracking(b, 64, true, false) }

// BenchmarkObserveGetLongTailParallel：长尾 miss（M3，持续入槽与 Space-Saving 逐出）。
func BenchmarkObserveGetLongTailParallel(b *testing.B) { benchTracking(b, 0, false, true) }

// BenchmarkHotWindowRoll 测量一次窗口滚动采集 + 发布（全部槽位已满）。
func BenchmarkHotWindowRoll(b *testing.B) {
	clk := NewManualClock(1_000_000_000)
	c, err := New[string](Config{
		ShardCount: 16, MaxEntries: 1024, MaxResidentBytes: 1 << 22, ObserveOnly: true,
		DisableTicker: true, Clock: clk, Executor: discardExecutor{},
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 3, Prior: &Prior{Lambda: 100}}},
		Advanced: Advanced{ObserveHotKeys: 128, ObserveRequestKeys: 64, ObserveRequestSampleP: 1,
			HotHistoryWindows: 60, HotHistoryTopN: 16},
	})
	if err != nil {
		b.Fatal(err)
	}
	cc := c.c
	ns, _ := c.Namespace("t")
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%04d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		now := cc.now()
		for i, k := range keys {
			kh := ns.ns.hashKey(k)
			s := cc.shardOf(kh)
			s.hot.touchLoad(ns.ns.id, kh, k, bucketIndex(now, int64(cc.cfg.HeatDelta)), now)
			s.req.record(ns.ns.id, kh, k, now+int64(i&1))
		}
		clk.Advance(1100 * time.Millisecond)
		b.StartTimer()
		for {
			cc.rollHotWindow()
			if !cc.hotHist.roll.active {
				break
			}
		}
	}
}

// BenchmarkHotKeys20 测量监控适配器常见的 HotKeys(20) 轮询。
func BenchmarkHotKeys20(b *testing.B) {
	c, err := New[string](Config{
		ShardCount: 16, MaxEntries: 1024, MaxResidentBytes: 1 << 22, ObserveOnly: true, DisableTicker: true,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 3, Prior: &Prior{Lambda: 100}}},
		Advanced:   Advanced{ObserveHotKeys: 128},
	})
	if err != nil {
		b.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	for i := 0; i < 4096; i++ {
		ns.Fetch(context.Background(), fmt.Sprintf("key-%04d", i), okLoader("v"))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_ = c.HotKeys(20)
	}
}
