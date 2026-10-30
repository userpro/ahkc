package ahkc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- 追踪表单元：迟到事件、Partial、Space-Saving ----

func collectReq(t *testing.T, tr *reqTracker, w uint64) (h winHeap, samples, lost uint64, partial bool) {
	t.Helper()
	h = newWinHeap(8, tr.maxKey)
	samples, lost, partial = tr.collect(w, &h)
	return
}

func entryKey(h *winHeap, e winEntry) string {
	return string(h.keys[int(e.buf)*h.maxKey : int(e.buf)*h.maxKey+int(e.klen)])
}

// 迟到事件只写回仍完好且未采集的所属窗口，绝不回写已采集窗口。
func TestReqTrackerLateEventsDoNotLeak(t *testing.T) {
	tr, _ := newReqTracker(4, 8, 0, 10, true)
	tr.record(1, 11, "a", 15) // 窗口 1
	tr.record(1, 22, "b", 25) // 窗口 2
	tr.record(1, 11, "a", 12) // 迟到：窗口 1 缓冲仍完好 → 写回窗口 1
	h, samples, lost, partial := collectReq(t, tr, 1)
	if samples != 2 || lost != 0 || partial || len(h.es) != 1 || h.es[0].n != 2 || entryKey(&h, h.es[0]) != "a" {
		t.Fatalf("window 1: samples=%d lost=%d partial=%v entries=%+v", samples, lost, partial, h.es)
	}
	tr.record(1, 11, "a", 13) // 窗口 1 已采集：迟到事件计入当前窗口 2，不回写
	h, samples, _, _ = collectReq(t, tr, 2)
	if samples != 2 || len(h.es) != 2 {
		t.Fatalf("window 2: samples=%d entries=%+v", samples, h.es)
	}
	if _, samples, _, _ := collectReq(t, tr, 1); samples != 0 {
		t.Fatalf("window 1 re-collected %d samples", samples)
	}
}

// 追踪表见过的最新窗口恰好已被采集时，迟到事件应计入最新未采集窗口，而不是重新写入已采集窗口。
func TestReqTrackerLateEventForCollectedNewestWindow(t *testing.T) {
	tr, _ := newReqTracker(4, 8, 0, 10, true)
	tr.record(1, 11, "a", 15) // 窗口 1 是见过的最新窗口
	if _, samples, _, _ := collectReq(t, tr, 1); samples != 1 {
		t.Fatalf("window 1 samples = %d", samples)
	}
	tr.record(1, 11, "a", 16) // 窗口 1 的迟到事件
	if _, samples, _, _ := collectReq(t, tr, 1); samples != 0 {
		t.Fatalf("collected window 1 was written again: %d", samples)
	}
	if _, samples, lost, _ := collectReq(t, tr, 2); samples != 1 || lost != 0 {
		t.Fatalf("window 2 samples=%d lost=%d, want the late event", samples, lost)
	}
}

// 维护滞后超过一个窗口：被覆盖的未采集窗口计入 lost，并标记 Partial。
func TestReqTrackerPartialAndLost(t *testing.T) {
	tr, _ := newReqTracker(4, 8, 0, 10, true)
	tr.record(1, 11, "a", 15) // 窗口 1（奇偶 1）
	tr.record(1, 11, "a", 16)
	tr.record(1, 33, "c", 35) // 窗口 3，同奇偶：覆盖窗口 1 的未采集数据
	h, samples, lost, partial := collectReq(t, tr, 1)
	if samples != 0 || lost != 2 || !partial || len(h.es) != 0 {
		t.Fatalf("window 1: samples=%d lost=%d partial=%v", samples, lost, partial)
	}
	_, samples, lost, partial = collectReq(t, tr, 3)
	if samples != 1 || lost != 0 || partial {
		t.Fatalf("window 3: samples=%d lost=%d partial=%v", samples, lost, partial)
	}
}

// 窗口内无事件但缓冲滞留更旧数据：丢弃并计入 lost，不算 Partial。
func TestReqTrackerStaleBufferIsEmptyWindow(t *testing.T) {
	tr, _ := newReqTracker(4, 8, 0, 10, true)
	tr.record(1, 11, "a", 15) // 窗口 1，从未被采集
	_, samples, lost, partial := collectReq(t, tr, 3)
	if samples != 0 || lost != 1 || partial {
		t.Fatalf("stale: samples=%d lost=%d partial=%v", samples, lost, partial)
	}
}

// Space-Saving 保证：每个入榜条目 真实值 ∈ [观测值, 观测值+误差]，
// 且占比超过 1/槽位数 的 key 必然入榜。
func TestReqTrackerSpaceSavingBound(t *testing.T) {
	const slots = 8
	rng := rand.New(rand.NewSource(7))
	tr, _ := newReqTracker(slots, 8, 0, 1<<40, true)
	truth := map[string]uint64{}
	var total uint64
	zipf := rand.NewZipf(rng, 1.3, 2, 63)
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("k%02d", zipf.Uint64())
		truth[k]++
		total++
		tr.record(1, uint64(len(k))*0x9e3779b97f4a7c15+uint64(k[1])*131+uint64(k[2]), k, 5)
	}
	h, samples, _, _ := collectReq(t, tr, 0)
	if samples != total {
		t.Fatalf("samples=%d want %d", samples, total)
	}
	in := map[string]bool{}
	for _, e := range h.es {
		k := entryKey(&h, e)
		in[k] = true
		lo, hi := uint64(e.n), uint64(e.n)+uint64(e.err)
		if truth[k] < lo || truth[k] > hi {
			t.Fatalf("%s: true=%d not in [%d,%d]", k, truth[k], lo, hi)
		}
	}
	for k, c := range truth {
		if c > total/slots && !in[k] {
			t.Fatalf("heavy hitter %s (%d of %d) missing from top", k, c, total)
		}
	}
}

// 回源追踪表同样满足 Space-Saving 上界，且窗口总量包含被逐出 key 的事件。
func TestHotTrackerSpaceSavingBound(t *testing.T) {
	const slots = 8
	tr, _ := newHotTracker(slots, 8, 0, 1<<40, true)
	rng := rand.New(rand.NewSource(11))
	zipf := rand.NewZipf(rng, 1.3, 2, 63)
	truth := map[string]uint64{}
	var total uint64
	for i := 0; i < 20000; i++ {
		k := fmt.Sprintf("k%02d", zipf.Uint64())
		truth[k]++
		total++
		tr.touchLoad(1, uint64(k[1])*131+uint64(k[2]), k, 0, 5)
	}
	h := newWinHeap(slots, 8)
	tot, lost, partial := tr.collect(0, &h, 0, 0)
	if tot[0] != total || tot[1] != 0 || lost != 0 || partial {
		t.Fatalf("tot=%v lost=%d partial=%v, want loads=%d", tot, lost, partial, total)
	}
	for _, e := range h.es {
		lo, hi := uint64(e.n), uint64(e.n)+uint64(e.err)
		if k := entryKey(&h, e); truth[k] < lo || truth[k] > hi {
			t.Fatalf("%s: true=%d not in [%d,%d]", k, truth[k], lo, hi)
		}
	}
	if len(h.es) != slots {
		t.Fatalf("entries = %d, want %d", len(h.es), slots)
	}
}

// 计数饱和时窗口记录不回绕：加载数不超过总观测数，合并数 = 总观测 - 加载。
func TestHotWindowSaturatedCountsDoNotWrap(t *testing.T) {
	tr, _ := newHotTracker(1, 8, 0, 10, true)
	tr.touchLoad(1, 1, "hot", 0, 0)
	tr.rank[0][0], tr.aux[0][0] = math.MaxUint32, math.MaxUint32
	h := newWinHeap(1, 8)
	tr.collect(0, &h, 0, 0)
	if len(h.es) != 1 || h.es[0].n != math.MaxUint32 || h.es[0].m != 0 || h.es[0].score != math.MaxUint32 {
		t.Fatalf("saturated counts wrapped: %+v", h.es)
	}
}

// ---- 集成：窗口解耦、总量、缺窗、合并 ----

func histCfg(c *Config) {
	c.Mode = ModeObserve
	c.Hot.LoadSlots = 8
	c.Hot.RequestSlots = 8
	c.Hot.RequestSampleP = 1
	c.Hot.Windows = 4
}

func TestRequestHistoryPreservesLongKey(t *testing.T) {
	key := strings.Repeat("k", 1<<16-1) // MaxKeyBytes 上限
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) {
		histCfg(c)
		c.MaxKeyBytes = len(key)
		c.Hot.LoadSlots, c.Hot.RequestSlots = 0, 1
	})
	defer closeManual(t, env.c, env.ex)
	if _, _, err := env.ns.Get(key); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(time.Second)
	env.drive()
	hist := env.c.HotKeyHistory(1)
	if len(hist) != 1 || len(hist[0].Requests) != 1 || hist[0].Requests[0].Key != key {
		t.Fatal("request history truncated the key")
	}
}

func TestHotSnapshotResultsAreIndependent(t *testing.T) {
	for _, label := range []bool{false, true} {
		t.Run(fmt.Sprintf("label=%v", label), func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute}, func(c *Config) {
				histCfg(c)
				if label {
					c.Hot.Label = func(_, key string) (string, bool) { return key, true }
				}
				c.Hot.OnWindow = func(s HotSnapshot) {
					s.Top[0].Key, s.Requests[0].Key = "callback", "callback"
				}
			})
			defer closeManual(t, env.c, env.ex)
			if _, err := env.fetch("key", okLoader("v")); err != nil {
				t.Fatal(err)
			}
			env.clk.Advance(time.Second)
			env.drive()
			for _, read := range []func() []HotSnapshot{
				func() []HotSnapshot { return env.c.HotKeyHistory(0) },
				func() []HotSnapshot { s, _ := env.c.HotWindowsSince(0); return s },
			} {
				s := read()
				if len(s) != 1 || len(s[0].Top) != 1 || len(s[0].Requests) != 1 || s[0].Top[0].Key != "key" || s[0].Requests[0].Key != "key" {
					t.Fatalf("history changed by consumer: %+v", s)
				}
				s[0].Top[0].Key, s[0].Requests[0].Key = "reader", "reader"
			}
			if s := env.c.HotKeyHistory(1)[0]; s.Top[0].Key != "key" || s.Requests[0].Key != "key" {
				t.Fatal("reader mutated retained history")
			}
		})
	}
}

func TestHotHistoryWindowDecoupledFromHeat(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Hot.Windows, c.Hot.Interval = 3, 10*time.Second
	})
	for i := 0; i < 5; i++ {
		if _, err := env.fetch("hot", okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := env.fetch("cold", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	// 热度窗口（1s）已滚动多次，但历史窗口（10s）尚未结束：没有快照。
	env.clk.Advance(5 * time.Second)
	env.drive()
	if hist := env.c.HotKeyHistory(0); len(hist) != 0 {
		t.Fatalf("snapshot before the 10s boundary: %+v", hist)
	}
	env.clk.Advance(5500 * time.Millisecond) // t=11.5s
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 {
		t.Fatalf("history len = %d, want 1", len(hist))
	}
	s := hist[0]
	if s.Seq != 1 || s.Bucket != 0 || s.Interval != 10*time.Second || s.Skipped != 0 || s.Partial {
		t.Fatalf("snapshot header = %+v", s)
	}
	if !s.Start.Equal(time.Unix(0, 0)) || !s.End.Equal(time.Unix(10, 0)) {
		t.Fatalf("window range = [%v,%v)", s.Start, s.End)
	}
	if len(s.Top) != 2 || s.Top[0].Key != "hot" || s.Top[0].WindowLoads != 5 || s.Top[0].Loads != 5 ||
		s.Top[1].Key != "cold" || s.Top[1].WindowLoads != 1 {
		t.Fatalf("Top = %+v", s.Top)
	}
	if len(s.Requests) != 2 || s.Requests[0].Key != "hot" || s.Requests[0].WindowRequests != 5 ||
		s.Requests[1].Key != "cold" || s.Requests[1].WindowRequests != 1 {
		t.Fatalf("Requests = %+v", s.Requests)
	}
	if s.Totals.Loads != 6 || s.Totals.Merged != 0 || s.Totals.Requests != 6 || s.Totals.Evicted != 0 ||
		s.Totals.LoadEventsDropped != 0 || s.Totals.RequestSamplesDropped != 0 {
		t.Fatalf("Totals = %+v", s.Totals)
	}
}

// 请求热点包含命中：缓存命中的热 key 仍出现在请求榜上（回源榜里则没有）。
func TestRequestHotCountsHits(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		c.Hot.RequestSlots, c.Hot.RequestSampleP = 8, 1
		c.Hot.Windows = 2
	})
	for i := 0; i < 5; i++ { // 前 3 次回源并发布，后 2 次命中
		if _, err := env.fetch("k1", okLoader("v-k1")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		if _, ok, err := env.ns.Get("k1"); err != nil || !ok {
			t.Fatalf("get %d: ok=%v err=%v", i, ok, err)
		}
	}
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 || len(hist[0].Requests) != 1 || hist[0].Requests[0].Key != "k1" {
		t.Fatalf("history = %+v", hist)
	}
	if got := hist[0].Requests[0].WindowRequests; got != 15 {
		t.Fatalf("WindowRequests = %v, want 15（5 次 Fetch + 10 次 Get，含命中）", got)
	}
	if len(hist[0].Top) != 0 {
		t.Fatalf("Top must be empty without Hot.LoadSlots: %+v", hist[0].Top)
	}
}

func TestHotWindowSkippedWhenMaintenanceLags(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, histCfg)
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(1100 * time.Millisecond) // t=2.1s：发布窗口 1
	env.drive()
	env.clk.Advance(3 * time.Second) // t=5.1s：窗口 2、3 被跳过，发布窗口 4
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 2 || hist[0].Bucket != 1 || hist[0].Skipped != 0 || hist[1].Bucket != 4 || hist[1].Skipped != 2 {
		t.Fatalf("history = %+v", hist)
	}
	if hist[1].Seq != hist[0].Seq+1 {
		t.Fatalf("Seq not consecutive: %d %d", hist[0].Seq, hist[1].Seq)
	}
}

func TestHotWindowMergedTotals(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, histCfg)
	f1, err := env.ns.FetchAsync("k", okLoader("v"))
	if err != nil {
		t.Fatal(err)
	}
	f2, err := env.ns.FetchAsync("k", okLoader("v")) // loader 尚未运行：被合并
	if err != nil {
		t.Fatal(err)
	}
	env.drive()
	for _, f := range []*Future[string]{f1, f2} {
		if _, err := f.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 {
		t.Fatalf("history = %+v", hist)
	}
	s := hist[0]
	if s.Totals.Loads != 1 || s.Totals.Merged != 1 || len(s.Top) != 1 || s.Top[0].Key != "k" ||
		s.Top[0].WindowLoads != 1 || s.Top[0].WindowMerged != 1 || s.Top[0].Merged != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
}

// 分片很多时增量采集：每个维护周期只处理固定预算的槽位，采完才发布。
func TestHotWindowIncrementalCollection(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Shards, c.Hot.LoadSlots, c.Hot.RequestSlots = 64, 512, 0 // 32768 槽 > 16384 预算
	})
	if _, err := env.fetch("k", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.clk.Advance(1100 * time.Millisecond)
	env.c.MaintenanceStep()
	if hist := env.c.HotKeyHistory(0); len(hist) != 0 {
		t.Fatalf("published after the first partial pass: %+v", hist)
	}
	env.c.MaintenanceStep()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 || len(hist[0].Top) != 1 || hist[0].Top[0].Key != "k" || hist[0].Partial {
		t.Fatalf("history after the second pass = %+v", hist)
	}
}

// ---- 导出：游标、映射、回调 ----

func TestHotWindowsSinceCursor(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Hot.Windows = 2
	})
	for i := 0; i < 4; i++ {
		env.clk.Advance(1100 * time.Millisecond)
		env.drive()
	}
	seqs := func(s []HotSnapshot) []uint64 {
		out := make([]uint64, len(s))
		for i := range s {
			out[i] = s[i].Seq
		}
		return out
	}
	check := func(since uint64, wantSeqs []uint64, wantLost uint64) {
		t.Helper()
		got, lost := env.c.HotWindowsSince(since)
		if fmt.Sprint(seqs(got)) != fmt.Sprint(wantSeqs) || lost != wantLost {
			t.Fatalf("since %d: seqs=%v lost=%d, want %v lost=%d", since, seqs(got), lost, wantSeqs, wantLost)
		}
	}
	check(0, []uint64{3, 4}, 2) // 环容量 2：Seq 1、2 已被覆盖
	check(1, []uint64{3, 4}, 1)
	check(2, []uint64{3, 4}, 0)
	check(3, []uint64{4}, 0)
	check(4, nil, 0)
	check(99, nil, 0) // 游标超前（例如实例重启）
}

func TestHotKeyLabelAppliedEverywhere(t *testing.T) {
	var mu sync.Mutex
	var got []HotSnapshot
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Hot.Label = func(ns, key string) (string, bool) {
			switch key {
			case "drop":
				return "", false
			case "boom":
				panic("label bug")
			}
			return ns + ":" + key + "#h", true
		}
		c.Hot.OnWindow = func(s HotSnapshot) {
			mu.Lock()
			got = append(got, s)
			mu.Unlock()
		}
	})
	for _, k := range []string{"a", "drop", "boom"} {
		if _, err := env.fetch(k, okLoader("v")); err != nil {
			t.Fatal(err)
		}
	}
	env.clk.Advance(1100 * time.Millisecond)
	env.drive()
	only := func(name string, stats []HotKeyStat) {
		t.Helper()
		if len(stats) != 1 || stats[0].Key != "t:a#h" {
			t.Fatalf("%s = %+v, want only the mapped key", name, stats)
		}
	}
	only("HotKeys", env.c.HotKeys(0))
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 {
		t.Fatalf("history = %+v", hist)
	}
	only("history.Top", hist[0].Top)
	only("history.Requests", hist[0].Requests)
	since, _ := env.c.HotWindowsSince(0)
	only("since.Top", since[0].Top)
	mu.Lock()
	if len(got) != 1 {
		mu.Unlock()
		t.Fatalf("notifications = %d", len(got))
	}
	only("callback.Top", got[0].Top)
	only("callback.Requests", got[0].Requests)
	mu.Unlock()
	// 内部历史不被映射修改。
	ring := env.c.c.hotHist.ring[0]
	if len(ring.top) != 3 {
		t.Fatalf("internal history was modified: %+v", ring.top)
	}
	if st := env.c.Stats(); st.HotExportPanics == 0 {
		t.Fatalf("label panics were not counted: %+v", st)
	}
}

func TestOnHotWindowPanicIsolated(t *testing.T) {
	var calls atomic.Int32
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Hot.OnWindow = func(HotSnapshot) { calls.Add(1); panic("callback bug") }
	})
	for i := 0; i < 2; i++ {
		env.clk.Advance(1100 * time.Millisecond)
		env.drive()
	}
	if calls.Load() != 2 {
		t.Fatalf("callback calls = %d, want 2（panic 后下一窗口仍能推送）", calls.Load())
	}
	if st := env.c.Stats(); st.HotExportPanics != 2 || st.HotWindowDropped != 0 {
		t.Fatalf("stats = %+v", st)
	}
	closeManual(t, env.c, env.ex) // 在途标志已复位：Close 能到达 RECLAIMED
}

func TestOnHotWindowBusyCounted(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
		histCfg(c)
		c.Hot.OnWindow = func(HotSnapshot) {}
	})
	env.clk.Advance(1100 * time.Millisecond)
	env.c.MaintenanceStep() // 通知已入执行器队列但尚未执行
	env.clk.Advance(1100 * time.Millisecond)
	env.c.MaintenanceStep() // 上一个回调仍在途
	if st := env.c.Stats(); st.HotWindowBusy != 1 || st.HotWindowDropped != 1 {
		t.Fatalf("stats = %+v, want Busy=1 Dropped=1", st)
	}
	env.drive()
	if st := env.c.Stats(); st.HotWindowBusy != 1 {
		t.Fatalf("stats after drain = %+v", st)
	}
	if hist, _ := env.c.HotWindowsSince(0); len(hist) != 2 {
		t.Fatalf("both windows must still be in history: %+v", hist)
	}
}

// 请求热点追踪不能让既有路径出现堆分配（命中、持续换新 key 的 miss）。
func TestObserveRequestZeroAlloc(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 1}, func(c *Config) {
		c.Hot.RequestSlots, c.Hot.RequestSampleP = 8, 1
		c.Hot.Windows = 2
	})
	if _, err := env.fetch("hit", okLoader("v")); err != nil {
		t.Fatal(err)
	}
	env.drive()
	if _, ok, _ := env.ns.Get("hit"); !ok {
		t.Fatal("expected cached")
	}
	var b Borrowed[string]
	if a := testing.AllocsPerRun(1000, func() {
		var r Request[string]
		env.ns.BeginRequestOn(&r, "hit")
		env.ns.BorrowOn(&r, &b)
		b.Release()
	}); a != 0 {
		t.Fatalf("hit with request tracking: %.2f allocs/op, want 0", a)
	}
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("miss-%d", i)
	}
	i := 0
	if a := testing.AllocsPerRun(4000, func() { env.ns.Get(keys[i%len(keys)]); i++ }); a != 0 {
		t.Fatalf("unique-key miss with request tracking (continuous slot eviction): %.2f allocs/op, want 0", a)
	}
}

// 每个窗口的滚动最多 3 次堆分配（回源榜、请求榜、请求 key 拼接串），与榜单条数无关；
// HotKeys(topN) 只有结果切片 1 次分配。
func TestHotRollAndExportAllocs(t *testing.T) {
	for _, withCallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("callback=%v", withCallback), func(t *testing.T) {
			env := newTestEnv(t, NamespaceConfig{TTL: time.Minute, Tin: 3}, func(c *Config) {
				histCfg(c)
				c.Hot.LoadSlots, c.Hot.RequestSlots = 32, 32
				c.Hot.TopN = 16
				if withCallback {
					c.Hot.OnWindow = func(HotSnapshot) {}
					c.Runtime.Executor = discardExecutor{}
				}
			})
			c := env.c.c
			keys := make([]string, 40)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%02d", i)
			}
			round := func() {
				now := c.now()
				for i, k := range keys {
					kh := env.ns.ns.hashKey(k)
					s := c.shardOf(kh)
					for j := 0; j <= i%3; j++ {
						s.hot.touchLoad(env.ns.ns.id, kh, k, bucketIndex(now, int64(c.cfg.Heat.Delta)), now)
						s.req.record(env.ns.ns.id, kh, k, now)
					}
				}
				env.clk.Advance(1100 * time.Millisecond)
				c.hotCallbackPending.Store(false)
				c.rollHotWindow()
			}
			round()
			hist := env.c.HotKeyHistory(1)
			if len(hist) != 1 || len(hist[0].Top) != 16 || len(hist[0].Requests) != 16 {
				t.Fatalf("snapshot = %d/%d entries", len(hist[0].Top), len(hist[0].Requests))
			}
			if a := testing.AllocsPerRun(50, round); a > 3 {
				t.Fatalf("roll allocs = %.1f, want <= 3", a)
			}
			if a := testing.AllocsPerRun(50, func() { env.c.HotKeys(5) }); a > 2 {
				t.Fatalf("HotKeys(5) allocs = %.1f, want <= 2 (result slice + key buffer)", a)
			}
		})
	}
}

// discardExecutor 接受并丢弃任务（不入队），用于测量投递路径本身的分配。
type discardExecutor struct{}

func (discardExecutor) TrySubmit(func()) bool { return true }

// 并发压测：请求/回源追踪、滚动、游标拉取、映射与回调并发（race 覆盖）。
func TestHotWindowRequestTrackingConcurrentStress(t *testing.T) {
	var notified atomic.Uint64
	c, err := NewBytes[string](Config{
		Shards:     4,
		MaxEntries: 1024,
		MaxBytes:   1 << 22,
		Mode:       ModeObserve,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 3}},
		Heat: HeatConfig{
			Delta: 10 * time.Millisecond,
		},
		Hot: HotConfig{
			LoadSlots:      16,
			RequestSlots:   16,
			RequestSampleP: 0.25,
			Windows:        4,
			Interval:       100 * time.Millisecond,
			Label:          func(ns, key string) (string, bool) { return key + "#", key != "k3" },
			OnWindow:       func(HotSnapshot) { notified.Add(1) },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ns, _ := c.Namespace("t")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	keys := []string{"hot", "spike", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11", "k12", "k13", "k14", "k15", "k16", "k17", "k18"}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var cursor uint64
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ns.Fetch(context.Background(), keys[(i*7+id)%len(keys)], okLoader("v"))
				if _, _, err := ns.Get(keys[(i+id)%len(keys)]); err != nil && err != ErrBusy {
					t.Errorf("Get: %v", err)
					return
				}
				if i%16 == 0 {
					snaps, _ := c.HotWindowsSince(cursor)
					for _, s := range snaps {
						if s.Seq <= cursor {
							t.Errorf("cursor went backwards: %d after %d", s.Seq, cursor)
							return
						}
						cursor = s.Seq
					}
					_ = c.HotKeys(3)
					_ = c.HotKeyHistory(2)
				}
			}
		}(w)
	}
	time.Sleep(450 * time.Millisecond)
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
	hist := c.HotKeyHistory(0)
	if len(hist) == 0 || len(hist) > 4 {
		t.Fatalf("history = %d snapshots, want 1..4", len(hist))
	}
	for i := 1; i < len(hist); i++ {
		if hist[i].Seq != hist[i-1].Seq+1 || hist[i].Bucket <= hist[i-1].Bucket {
			t.Fatalf("history order broken: %+v", hist)
		}
	}
	for _, s := range hist {
		for _, st := range s.Requests {
			if st.Key == "k3#" || st.WindowRequests < 4 { // q=0.25：每个采样折算 4 次请求
				t.Fatalf("bad request entry %+v", st)
			}
		}
	}
	if st := c.Stats(); st.HotExportPanics != 0 {
		t.Fatalf("unexpected panics: %+v", st)
	}
}

// 内置时钟下历史窗口按 Unix 时间对齐：当前窗口的墙钟范围必须包含"现在"，
// 且边界落在窗口宽度的整数倍上（多实例同窗口数据可直接相加）。
func TestHotWindowAlignedToUnixTime(t *testing.T) {
	c, err := NewBytes[string](Config{
		MaxEntries: 64,
		MaxBytes:   1 << 20,
		Mode:       ModeObserve,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: time.Minute, Tin: 3}},
		Hot: HotConfig{
			LoadSlots: 4,
			Windows:   2,
			Interval:  time.Minute,
		},
		Runtime: RuntimeConfig{
			DisableTicker: true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	h := c.c.hotHist
	if h.align != uint64(processClockBase.UnixNano()) {
		t.Fatalf("align = %d, want process clock origin %d", h.align, processClockBase.UnixNano())
	}
	w := (uint64(c.c.now()) + h.align) / h.interval
	start, end := hotWindowTime(w, h.interval), hotWindowTime(w+1, h.interval)
	if now := time.Now(); now.Before(start) || !now.Before(end) {
		t.Fatalf("now %v not in [%v,%v)", now, start, end)
	}
	if start.Unix()%60 != 0 || start.Nanosecond() != 0 || end.Sub(start) != time.Minute {
		t.Fatalf("window not aligned to whole minutes: [%v,%v)", start, end)
	}
}

func TestHotWindowConfigValidation(t *testing.T) {
	base := func(c *Config) {
		c.Hot.LoadSlots, c.Hot.Windows = 4, 2
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		ok     bool
	}{
		{"valid", func(c *Config) { base(c) }, true},
		{"explicit interval ok", func(c *Config) { base(c); c.Hot.Interval = time.Minute }, true},
		{"interval below 1ms", func(c *Config) { base(c); c.Hot.Interval = time.Microsecond }, false},
		{"interval above 24h", func(c *Config) { base(c); c.Hot.Interval = 25 * time.Hour }, false},
		{"interval shorter than collection needs", func(c *Config) {
			base(c)
			c.Runtime.PeriodicInterval, c.Hot.Interval = 50*time.Millisecond, 60*time.Millisecond
		}, false},
		{"request keys without history", func(c *Config) { c.Hot.RequestSlots = 4 }, false},
		{"request keys negative", func(c *Config) { base(c); c.Hot.RequestSlots = -1 }, false},
		{"request keys over dimension cap", func(c *Config) { base(c); c.Hot.RequestSlots = 1<<24 + 1 }, false},
		{"request sampleP above 1", func(c *Config) { base(c); c.Hot.RequestSlots, c.Hot.RequestSampleP = 4, 1.5 }, false},
		{"request sampleP negative", func(c *Config) { base(c); c.Hot.RequestSlots, c.Hot.RequestSampleP = 4, -0.1 }, false},
		{"request-only history", func(c *Config) { c.Hot.RequestSlots, c.Hot.Windows = 4, 2 }, true},
		{"history without any tracker", func(c *Config) { c.Hot.Windows = 2 }, false},
	}
	for _, tc := range cases {
		cfg := validConfig()
		tc.mutate(&cfg)
		_, err := NewBytes[string](cfg)
		if tc.ok && err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
		}
		if !tc.ok && !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: err=%v, want ErrInvalidConfig", tc.name, err)
		}
	}
	// 默认值：采样率 1/1024，历史窗口 = 热度窗口。
	cfg := validConfig()
	cfg.Hot.RequestSlots, cfg.Hot.Windows = 4, 2
	cfg.applyDefaults()
	if cfg.Hot.RequestSampleP != 1.0/1024 || cfg.Hot.Interval != cfg.Heat.Delta {
		t.Fatalf("defaults: q=%v interval=%v", cfg.Hot.RequestSampleP, cfg.Hot.Interval)
	}
}

// BenchmarkReqTrackerRecord 测量请求热点追踪表在“大 key 空间长尾”流量下的单次记录成本：
// 绝大多数样本是未追踪的新 key，每次都要完整 find + claimSlot（线性扫描）。
func BenchmarkReqTrackerRecord(b *testing.B) {
	for _, slots := range []int{256, 1024, 4096, 16384} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			tr, _ := newReqTracker(slots, 64, 0, uint64(time.Minute), true)
			keys := make([]string, 1<<16)
			for i := range keys {
				keys[i] = fmt.Sprintf("key-%d", i)
			}
			now := int64(time.Second)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// 80% 落在 64 个热 key 上，20% 是长尾新 key。
				var k int
				if i%5 != 0 {
					k = i % 64
				} else {
					k = 64 + int(uint64(i)*2654435761%uint64(len(keys)-64))
				}
				tr.record(1, mix64(uint64(k)+1), keys[k], now)
			}
		})
	}
}
