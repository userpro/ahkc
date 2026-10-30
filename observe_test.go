package ahkc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHotKeysLimits(t *testing.T) {
	for _, slots := range []int{0, 4} {
		env := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) { c.Hot.LoadSlots = slots })
		if _, err := env.fetch("key", okLoader("v")); err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{0, -1, math.MinInt, 1, math.MaxInt} {
			got := env.c.HotKeys(n)
			want := 0
			if slots > 0 {
				want = 1
			}
			if len(got) != want || cap(got) > slots*len(env.c.c.shards) {
				t.Fatalf("slots=%d n=%d: len=%d cap=%d", slots, n, len(got), cap(got))
			}
		}
		closeManual(t, env.c, env.ex)
	}
}

func TestCacheAndObservationModes(t *testing.T) {
	for _, only := range []bool{false, true} {
		for _, tracking := range []bool{false, true} {
			t.Run(fmt.Sprintf("observeOnly=%v/tracking=%v", only, tracking), func(t *testing.T) {
				env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
					c.Mode, c.Hot.LoadSlots = modeOf(only), 0
					if tracking {
						c.Hot.LoadSlots, c.Hot.RequestSlots = 8, 8
						c.Hot.RequestSampleP, c.Hot.Windows = 1, 2
					}
				})
				defer closeManual(t, env.c, env.ex)
				for i := 0; i < 3; i++ {
					v, err := env.fetch("key", okLoader("value"))
					if err != nil || v != "value" {
						t.Fatalf("fetch: value=%q err=%v", v, err)
					}
				}
				for i := 0; i < 2; i++ {
					if v, ok, err := env.ns.Get("key"); err != nil || ok == only || (ok && v != "value") {
						t.Fatalf("get: value=%q ok=%v err=%v", v, ok, err)
					}
				}
				st := env.c.Stats()
				if only {
					if st.Loads != 3 || st.Hits != 0 || st.Misses != 5 || st.ResidentEntries != 0 || st.FillPublished != 0 {
						t.Fatalf("observe only: %+v", st)
					}
				} else if st.Loads != 1 || st.Hits != 4 || st.Misses != 1 || st.ResidentEntries != 1 || st.FillPublished != 1 {
					t.Fatalf("cache: %+v", st)
				}
				if heat, ok := env.ns.Heat("key"); !ok || heat != 5 {
					t.Fatalf("heat=%v ok=%v", heat, ok)
				}
				env.clk.Advance(time.Second)
				env.drive()
				hist := env.c.HotKeyHistory(0)
				if !tracking {
					if len(hist) != 0 || len(env.c.HotKeys(0)) != 0 {
						t.Fatal("disabled tracking produced output")
					}
				} else if len(hist) != 1 || hist[0].Totals.Requests != 5 || len(hist[0].Requests) != 1 || hist[0].Requests[0].WindowRequests != 5 || hist[0].Totals.Loads != st.Loads {
					t.Fatalf("history=%+v", hist)
				}
			})
		}
	}
}

func TestModeObserveMemoryDoesNotScaleWithEntryLimit(t *testing.T) {
	var fixed int64
	for _, entries := range []int64{64, 4096} {
		env := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) {
			c.Mode, c.MaxEntries = ModeObserve, entries
		})
		defer closeManual(t, env.c, env.ex)
		got := env.c.MemoryLimits().FixedBytes
		if fixed != 0 && got != fixed {
			t.Fatalf("unused resident capacity increased observation storage: %d -> %d", fixed, got)
		}
		fixed = got
	}
}

// ---- 仅统计不缓存（ModeObserve）----

func TestModeObserveNeverCaches(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve })
	loads := 0
	loader := func(ctx context.Context, key string) (string, error) {
		loads++
		return "v-" + key, nil
	}
	// 热度远超 Tin：观察模式下也必须每次真实回源、永不驻留。
	for i := 0; i < 6; i++ {
		if _, err := env.fetch("k1", loader); err != nil {
			t.Fatalf("fetch %d: %v", i, err)
		}
		env.drive()
	}
	if loads != 6 {
		t.Fatalf("loads = %d, want 6（观察模式不得缓存）", loads)
	}
	if v, ok, err := env.ns.Get("k1"); ok || err != nil || v != "" {
		t.Fatalf("Get in observe mode: %v %v %v", v, ok, err)
	}
	st := env.c.Stats()
	if st.Mode != ModeObserve {
		t.Fatal("Stats.Mode != ModeObserve")
	}
	if st.FillPublished != 0 {
		t.Fatalf("FillPublished = %d, want 0", st.FillPublished)
	}
	if st.ResidentEntries != 0 || st.ResidentBytes != 0 {
		t.Fatalf("resident = %d/%d, want 0/0", st.ResidentEntries, st.ResidentBytes)
	}
	if got := st.FillRejected["OBSERVE_ONLY"]; got != 6 {
		t.Fatalf("FillRejected[OBSERVE_ONLY] = %d, want 6", got)
	}
	// 6 次 Fetch + 末尾 1 次 Get：观察模式下全部是确定 miss。
	if st.Misses != 7 {
		t.Fatalf("Misses = %d, want 7", st.Misses)
	}
}

func TestModeObserveSingleflightStillMerges(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve; c.Hot.LoadSlots = 8 })
	const n = 8
	var mu sync.Mutex
	loads := 0
	loader := func(ctx context.Context, key string) (string, error) {
		mu.Lock()
		loads++
		mu.Unlock()
		return "v", nil
	}
	// 并发 miss：全部并入同一 leader，只发生一次真实回源。
	futs := make([]*Future[string], 0, n)
	for i := 0; i < n; i++ {
		fut, err := env.ns.FetchAsync("hot", loader)
		if err != nil {
			t.Fatalf("FetchAsync %d: %v", i, err)
		}
		futs = append(futs, fut)
	}
	env.drive()
	for i, fut := range futs {
		if v, err := fut.Wait(context.Background()); err != nil || v != "v" {
			t.Fatalf("wait %d: %v %v", i, v, err)
		}
	}
	if loads != 1 {
		t.Fatalf("loads = %d, want 1（合并照常工作）", loads)
	}
	st := env.c.Stats()
	if st.LoadMerged != n-1 {
		t.Fatalf("LoadMerged = %d, want %d", st.LoadMerged, n-1)
	}
	// 合并数必须体现在热点榜单上。
	hot := env.c.HotKeys(1)
	if len(hot) != 1 || hot[0].Key != "hot" {
		t.Fatalf("HotKeys = %+v", hot)
	}
	if hot[0].Merged != n-1 || hot[0].Loads != 1 || hot[0].LoadHeat != 1 {
		t.Fatalf("HotKeys[0] = %+v, want Loads=1 LoadHeat=1 Merged=%d", hot[0], n-1)
	}
}

func TestModeObserveHeatProbe(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve })
	for i := 0; i < 3; i++ {
		if _, err := env.fetch("k1", okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	// 同一热度桶内：3 次 miss 全部计入门卫。
	if h, ok := env.ns.Heat("k1"); !ok || h != 3 {
		t.Fatalf("Heat = %v %v, want 3 true", h, ok)
	}
	// 跨 1 桶：previous 参与加权（phi=0.5 → 0 + 0.5*3）。
	env.clk.Advance(1500 * time.Millisecond)
	if h, ok := env.ns.Heat("k1"); !ok || h != 1.5 {
		t.Fatalf("Heat after advance = %v %v, want 1.5 true", h, ok)
	}
	// 跨 >=2 桶：清零。
	env.clk.Advance(3 * time.Second)
	if h, ok := env.ns.Heat("k1"); !ok || h != 0 {
		t.Fatalf("Heat after clear = %v %v, want 0 true", h, ok)
	}
}

// TestHeatProbeNegativeCases 覆盖 Heat 探针的拒绝分支：
// key 超限与实例关闭时 ok=false（纯观测接口的确定性契约）。
func TestHeatProbeNegativeCases(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.MaxKeyBytes = 8 })
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	if h, ok := env.ns.Heat("k"); !ok || h < 1 {
		t.Fatalf("normal probe: %v %v", h, ok)
	}
	if _, ok := env.ns.Heat("012345678"); ok {
		t.Fatal("oversized key must be rejected")
	}
	// 关闭后 ok=false。
	closeManual(t, env.c, env.ex)
	if _, ok := env.ns.Heat("k"); ok {
		t.Fatal("Heat must reject after close")
	}
}

// ---- 热点 key 追踪（Hot.LoadSlots）----

func TestHotKeysRankSequentialDuplication(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve; c.Hot.LoadSlots = 4 })
	loader := okLoader("v")
	// hot：同一桶内 5 次顺序加载（高度重复的顺序回源）；cold：1 次。
	for i := 0; i < 5; i++ {
		if _, err := env.fetch("hot", loader); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := env.fetch("cold", loader); err != nil {
		t.Fatal(err)
	}
	all := env.c.HotKeys(0)
	if len(all) != 2 {
		t.Fatalf("HotKeys(0) = %+v", all)
	}
	if all[0].Key != "hot" || all[0].Namespace != "t" {
		t.Fatalf("top = %+v, want key hot", all[0])
	}
	if all[0].Loads != 5 || all[0].LoadHeat != 5 {
		t.Fatalf("top stats = %+v, want Loads=5 LoadHeat=5", all[0])
	}
	if all[1].Key != "cold" || all[1].Loads != 1 {
		t.Fatalf("second = %+v", all[1])
	}
	if top := env.c.HotKeys(1); len(top) != 1 || top[0].Key != "hot" {
		t.Fatalf("HotKeys(1) = %+v", top)
	}
}

func TestHotKeysEvictsColdest(t *testing.T) {
	// 单分片 + 单槽位：逐出行为确定可见。
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve; c.Hot.LoadSlots = 1; c.Shards = 1 })
	loader := okLoader("v")
	if _, err := env.fetch("stale", loader); err != nil {
		t.Fatal(err)
	}
	// 唯一槽位空闲（窗口热度归零）后被更热的新 key 逐出。
	env.clk.Advance(3 * time.Second)
	for i := 0; i < 3; i++ {
		if _, err := env.fetch("fresh", loader); err != nil {
			t.Fatal(err)
		}
	}
	all := env.c.HotKeys(0)
	if len(all) != 1 || all[0].Key != "fresh" || all[0].Loads != 3 {
		t.Fatalf("HotKeys = %+v, want only fresh Loads=3", all)
	}
}

func TestHotKeysWorksWithCachingEnabled(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Hot.LoadSlots = 8 })
	// 正常缓存模式下：追踪生效，且不影响发布。
	// Tin=3：前 3 次 miss 加载并在第 3 次完成时发布，后 2 次命中不再回源。
	for i := 0; i < 5; i++ {
		if _, err := env.fetch("k1", okLoader("v-k1")); err != nil {
			t.Fatal(err)
		}
	}
	if v, ok, err := env.ns.Get("k1"); !ok || err != nil || v != "v-k1" {
		t.Fatalf("get after publish: %v %v %v", v, ok, err)
	}
	// 命中计入条目热度：请求热度 = 门卫 miss 热度(3) + 条目 hit 热度(3)。
	if h, ok := env.ns.Heat("k1"); !ok || h != 6 {
		t.Fatalf("Heat = %v %v, want 6 true（3 miss + 3 hit）", h, ok)
	}
	hot := env.c.HotKeys(1)
	if len(hot) != 1 || hot[0].Key != "k1" || hot[0].Loads != 3 {
		t.Fatalf("HotKeys = %+v, want k1 Loads=3（后 2 次命中不回源）", hot)
	}
}

// 槽位数取 0 即关闭，所有模式一致：没有特例默认值，负值不再表示关闭而是非法。
func TestHotSlotsZeroDisablesInEveryMode(t *testing.T) {
	for _, mode := range []Mode{ModeCache, ModeShadow, ModeObserve} {
		cfg := Config{Mode: mode}
		cfg.applyDefaults()
		if cfg.Hot.LoadSlots != 0 || cfg.Hot.RequestSlots != 0 {
			t.Fatalf("mode %d: Hot slots = %d/%d, want 0/0", mode, cfg.Hot.LoadSlots, cfg.Hot.RequestSlots)
		}
	}
	bad := validConfig()
	bad.Hot.LoadSlots = -1
	if _, err := NewBytes[string](bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("negative LoadSlots: err=%v, want ErrInvalidConfig", err)
	}
	// 端到端：不配置槽位就没有榜单，热度探针不依赖榜单。
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) { c.Mode = ModeObserve })
	if _, err := env.fetch("k1", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	if all := env.c.HotKeys(0); len(all) != 0 {
		t.Fatalf("HotKeys = %+v, want empty", all)
	}
	if h, ok := env.ns.Heat("k1"); !ok || h != 1 {
		t.Fatalf("Heat = %v %v, want 1 true（探针不依赖榜单）", h, ok)
	}
}

// TestModeObserveConcurrentStress 并发混合压测：真实时钟 + 默认执行器，
// 观察模式下逐出/计数/榜单并发安全，关闭三阶段照常完成（race 覆盖）。
func TestModeObserveConcurrentStress(t *testing.T) {
	c, err := NewBytes[string](Config{
		Shards:     4,
		MaxEntries: 1024,
		MaxBytes:   1 << 22,
		Mode:       ModeObserve, // 槽位少：强制逐出路径
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 3},
		},
		Hot: HotConfig{
			LoadSlots: 16,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	keys := []string{"hot", "spike", "k1", "k2", "k3"}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// 高度重复：全部工作者都反复访问同一小集合。
				ns.Fetch(context.Background(), keys[i%len(keys)], okLoader("v"))
				if _, _, err := ns.Get(keys[(i+1)%len(keys)]); err != nil && err != ErrBusy {
					t.Errorf("Get: %v", err)
					return
				}
				i++
			}
		}(w)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close under load: %v", err)
	}
	progress := c.CloseProgress()
	stopped, drained, reclaimed := progress.Stopped, progress.Drained, progress.Reclaimed
	if !stopped || !drained || !reclaimed {
		t.Fatalf("CloseState = %v %v %v", stopped, drained, reclaimed)
	}
	st := c.Stats()
	if st.Mode != ModeObserve || st.FillPublished != 0 || st.ResidentEntries != 0 {
		t.Fatalf("observe invariants broken: %+v", st)
	}
	if st.FillRejected["OBSERVE_ONLY"] == 0 {
		t.Fatal("no OBSERVE_ONLY rejections counted")
	}
	if st.LoadMerged == 0 {
		t.Fatal("no merges observed under concurrent duplicates")
	}
	// 关闭后榜单仍可读（快照语义）。
	if len(c.HotKeys(5)) == 0 {
		t.Fatal("HotKeys empty after stress")
	}
}

// ---- 窗口摘要历史与滚动通知（Hot.Windows / Hot.OnWindow）----

func TestHotKeyHistoryRolls(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) {
			c.Mode = ModeObserve
			c.Hot.LoadSlots = 8
			c.Hot.Windows = 2
			c.Hot.TopN = 4
		})
	loader := okLoader("v")
	// 起始桶（t=1s，桶 1）内 hot 加载 3 次。
	for i := 0; i < 3; i++ {
		if _, err := env.fetch("hot", loader); err != nil {
			t.Fatal(err)
		}
	}
	// 跨 1 个窗口边界（t=2.1s，桶 2）：首个维护周期滚动快照，
	// 快照属于刚结束的窗口（桶 1），热度过零的 key 不进窗口快照。
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	if hist[0].Bucket != 1 {
		t.Fatalf("snapshot bucket = %d, want 1", hist[0].Bucket)
	}
	if len(hist[0].Top) != 1 || hist[0].Top[0].Key != "hot" || hist[0].Top[0].Loads != 3 {
		t.Fatalf("snapshot top = %+v, want hot Loads=3", hist[0].Top)
	}
	if hist[0].Top[0].LoadHeat <= 0 {
		t.Fatalf("snapshot heat = %v, want > 0", hist[0].Top[0].LoadHeat)
	}

	// 下一窗口无加载活动：快照为空，但窗口照常滚动（历史连续）。
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	hist = env.c.HotKeyHistory(0)
	if len(hist) != 2 {
		t.Fatalf("history len = %d, want 2", len(hist))
	}
	if hist[1].Bucket != 2 || len(hist[1].Top) != 0 {
		t.Fatalf("second snapshot = %+v, want bucket 2 with empty top", hist[1])
	}
	// HotKeys 口径不受快照过滤影响：仍返回全部被追踪 key。
	if all := env.c.HotKeys(0); len(all) != 1 || all[0].Key != "hot" {
		t.Fatalf("HotKeys = %+v, want hot retained", all)
	}

	// 环形容量 2：再滚两窗，只保留最近两份（时间升序）。
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	hist = env.c.HotKeyHistory(0)
	if len(hist) != 2 || hist[0].Bucket != 3 || hist[1].Bucket != 4 {
		t.Fatalf("history after wrap = %+v, want buckets 3,4", hist)
	}
	if latest := env.c.HotKeyHistory(1); len(latest) != 1 || latest[0].Bucket != 4 {
		t.Fatalf("history(1) = %+v, want latest bucket 4", latest)
	}
}

func TestOnHotWindowNotify(t *testing.T) {
	var mu sync.Mutex
	var got []HotSnapshot
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3},
		func(c *Config) {
			c.Mode = ModeObserve
			c.Hot.LoadSlots = 8
			c.Hot.OnWindow = func(s HotSnapshot) {
				mu.Lock()
				got = append(got, s)
				mu.Unlock()
			}
		})
	// 仅通知模式（Hot.Windows=0）：无历史、有通知。
	if _, err := env.fetch("hot", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(1100 * time.Millisecond)
	env.drive() // MaintenanceStep 滚动 + TrySubmit 投递
	mu.Lock()
	if len(got) != 1 {
		mu.Unlock()
		t.Fatalf("notifications = %d, want 1", len(got))
	}
	if got[0].Bucket != 1 || len(got[0].Top) != 1 || got[0].Top[0].Key != "hot" {
		mu.Unlock()
		t.Fatalf("notification = %+v, want bucket 1 with hot", got[0])
	}
	mu.Unlock()
	if hist := env.c.HotKeyHistory(0); hist != nil {
		t.Fatalf("history must be nil without Hot.Windows, got %+v", hist)
	}
	// 同窗口内的重复维护不重复通知（每窗口至多一次）。
	env.drive()
	if len(got) != 1 {
		t.Fatalf("notifications = %d, want 1", len(got))
	}
}

func TestOnHotWindowDroppedWhenExecutorRejects(t *testing.T) {
	ex := &toggleExecutor{}
	ex.fail.Store(true)
	clk := NewManualClock(1_000_000_000)
	cfg := validConfig()
	cfg.Mode = ModeObserve
	cfg.Hot.LoadSlots = 8
	cfg.Hot.OnWindow = func(HotSnapshot) {}
	cfg.Runtime.Executor = ex
	cfg.Runtime.Clock = clk
	c, err := NewBytes[string](cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 同桶：不滚动。
	c.MaintenanceStep()
	if st := c.Stats(); st.HotWindowDropped != 0 {
		t.Fatalf("dropped = %d before roll, want 0", st.HotWindowDropped)
	}
	// 跨窗口：投递被拒绝即丢弃并计数。
	clk.Advance(1100 * time.Millisecond)
	c.MaintenanceStep()
	if st := c.Stats(); st.HotWindowDropped != 1 {
		t.Fatalf("dropped = %d, want 1", st.HotWindowDropped)
	}
}

// TestHotWindowHistoryConcurrentStress 短窗口 + 并发：历史滚动、快照采集
// 与 HotKeyHistory/HotKeys 并发读取安全（race 覆盖）。
func TestHotWindowHistoryConcurrentStress(t *testing.T) {
	var notified atomic.Uint64
	c, err := NewBytes[string](Config{
		Shards:     4,
		MaxEntries: 1024,
		MaxBytes:   1 << 22,
		Mode:       ModeObserve,
		Namespaces: map[string]NamespaceConfig{
			"t": {TTL: time.Minute, Tin: 3},
		},
		Heat: HeatConfig{
			Delta: 10 * time.Millisecond,
		},
		Hot: HotConfig{
			LoadSlots: 16,
			Windows:   4, // 压测期内多次滚动
			OnWindow:  func(s HotSnapshot) { notified.Add(1) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	keys := []string{"hot", "spike", "k1", "k2", "k3"}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ns.Fetch(context.Background(), keys[i%len(keys)], okLoader("v"))
				if _, _, err := ns.Get(keys[(i+1)%len(keys)]); err != nil && err != ErrBusy {
					t.Errorf("Get: %v", err)
					return
				}
				// 并发读取历史与榜单（快照语义，安全持有）。
				_ = c.HotKeyHistory(2)
				_ = c.HotKeys(3)
				i++
			}
		}(w)
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatalf("close under load: %v", err)
	}
	if notified.Load() == 0 {
		t.Fatal("no window notifications fired")
	}
	if hist := c.HotKeyHistory(0); len(hist) == 0 || len(hist) > 4 {
		t.Fatalf("history = %d snapshots, want 1..4", len(hist))
	}
	// 快照桶号单调递增。
	hist := c.HotKeyHistory(0)
	for i := 1; i < len(hist); i++ {
		if hist[i].Bucket <= hist[i-1].Bucket {
			t.Fatalf("history buckets not increasing: %+v", hist)
		}
	}
}
