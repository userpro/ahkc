package ahkc

import (
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
)

// checkTracker 校验追踪表核心的内部不变量：定位表与槽位一一对应，堆有序且位置表一致。
func checkTracker(t *testing.T, tr *keyTracker) {
	t.Helper()
	used := 0
	for _, v := range tr.tab {
		if v != 0 {
			used++
		}
	}
	if used != tr.filled {
		t.Fatalf("lookup table holds %d entries, want %d", used, tr.filled)
	}
	for s := 0; s < tr.filled; s++ {
		m := trackHash(tr.hash[s], tr.ns[s])
		if got := tr.find(tr.ns[s], tr.hash[s], m); got != s {
			t.Fatalf("slot %d is found as %d", s, got)
		}
	}
	if len(tr.heap) != tr.filled {
		t.Fatalf("heap has %d slots, want %d", len(tr.heap), tr.filled)
	}
	if tr.hw == 0 {
		return // 堆待重建：下一次记录前不要求有序
	}
	for i, s := range tr.heap {
		if tr.pos[s] != uint32(i) {
			t.Fatalf("pos[%d] = %d, want %d", s, tr.pos[s], i)
		}
		if i > 0 && tr.less(s, tr.heap[(i-1)/2]) {
			t.Fatalf("heap order violated at %d", i)
		}
	}
}

// 随机事件流（多窗口、多命名空间、迟到事件）下，定位表与堆始终自洽，
// 窗口内已追踪 key 的真实计数落在 [rank-err, rank]，未追踪 key 不超过堆根计数。
func TestTrackerInvariantsAndBounds(t *testing.T) {
	const slots, interval = 16, 1000
	rng := rand.New(rand.NewSource(3))
	zipf := rand.NewZipf(rng, 1.2, 1, 4000)
	tr, _ := newReqTracker(slots, 8, 0, interval, true)
	now := int64(0)
	for i := 0; i < 60000; i++ {
		now += int64(rng.Intn(3))
		at := now
		if rng.Intn(50) == 0 && at > 200 {
			at -= int64(rng.Intn(200)) // 迟到事件
		}
		k := zipf.Uint64()
		ns := uint32(k & 1)
		tr.record(ns, mix64(k), "k", at)
		if i%997 == 0 {
			checkTracker(t, &tr.keyTracker)
		}
	}
	checkTracker(t, &tr.keyTracker)

	// 单窗口精确界：重放一个窗口的事件，核对上下界。
	tr, _ = newReqTracker(slots, 8, 0, 1<<40, true)
	exact := map[[2]uint64]uint64{}
	for i := 0; i < 50000; i++ {
		k := zipf.Uint64()
		ns := uint32(k & 1)
		exact[[2]uint64{uint64(ns), k}]++
		tr.record(ns, mix64(k), "k", 0)
	}
	checkTracker(t, &tr.keyTracker)
	root := tr.rank[0][tr.heap[0]]
	tracked := map[[2]uint64]bool{}
	for s := 0; s < tr.filled; s++ {
		// 反查身份：用同一哈希函数枚举 key 空间。
		for k := uint64(0); k <= 4000; k++ {
			if mix64(k) == tr.hash[s] && uint32(k&1) == tr.ns[s] {
				id := [2]uint64{uint64(tr.ns[s]), k}
				tracked[id] = true
				lo, hi := uint64(tr.rank[0][s]-tr.err[0][s]), uint64(tr.rank[0][s])
				if c := exact[id]; c < lo || c > hi {
					t.Fatalf("key %v: true=%d not in [%d,%d]", id, c, lo, hi)
				}
			}
		}
	}
	if len(tracked) != slots {
		t.Fatalf("tracked %d keys, want %d", len(tracked), slots)
	}
	for id, c := range exact {
		if !tracked[id] && c > uint64(root) {
			t.Fatalf("untracked key %v has %d events > root %d", id, c, root)
		}
	}
}

// 长尾冲击：80% 的流量来自 100 万个互不重复的冷 key 时，真实热点不被挤出。
func TestTrackerLongTailKeepsHotKeys(t *testing.T) {
	const slots, hot, events = 1024, 200, 2_000_000
	rng := rand.New(rand.NewSource(5))
	zipf := rand.NewZipf(rng, 1.1, 8, hot-1)
	tr, _ := newReqTracker(slots, 8, 0, 1<<40, true)
	truth := make([]uint64, hot)
	var cold uint64 = 1 << 32
	for i := 0; i < events; i++ {
		if rng.Intn(5) == 0 {
			k := zipf.Uint64()
			truth[k]++
			tr.record(0, mix64(k), "h", 0)
		} else {
			cold++
			tr.record(0, mix64(cold), "c", 0)
		}
	}
	checkTracker(t, &tr.keyTracker)
	got := map[uint64]bool{}
	for s := 0; s < tr.filled; s++ {
		got[tr.hash[s]] = true
	}
	order := make([]int, hot)
	for i := range order {
		order[i] = i
	}
	slices.SortFunc(order, func(a, b int) int { return int(truth[b]) - int(truth[a]) })
	missing := 0
	for _, k := range order[:100] {
		if !got[mix64(uint64(k))] {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("%d of the top 100 hot keys were pushed out by the long tail", missing)
	}
}

// 窗口滚动后堆按新窗口重建，采集清零后继续记录仍自洽。
func TestTrackerWindowRotationAndCollect(t *testing.T) {
	tr, _ := newReqTracker(8, 8, 0, 10, true)
	for w := uint64(0); w < 6; w++ {
		for i := 0; i < 200; i++ {
			k := uint64(i % 23)
			if i%2 == 0 {
				k = uint64(i % 3) // 热 key
			}
			tr.record(1, mix64(k+w*100), "k", int64(w*10+uint64(i%10)))
		}
		checkTracker(t, &tr.keyTracker)
		if w > 0 {
			h := newWinHeap(4, 8)
			if samples, _, _ := tr.collect(w-1, &h); samples != 200 {
				t.Fatalf("window %d: samples=%d", w-1, samples)
			}
			checkTracker(t, &tr.keyTracker)
		}
	}
}

// 后移删除保持探测链完整：反复逐出重入后所有已追踪 key 都能定位。
func TestTrackerTableChurn(t *testing.T) {
	tr, _ := newReqTracker(64, 8, 0, 1<<40, true)
	rng := rand.New(rand.NewSource(9))
	for i := 0; i < 200000; i++ {
		k := uint64(rng.Intn(500))
		if rng.Intn(3) == 0 {
			k = uint64(rng.Intn(5)) // 热 key 保证草图放行逐出
		}
		tr.record(0, mix64(k), "k", 0)
		if i%4999 == 0 {
			checkTracker(t, &tr.keyTracker)
		}
	}
	checkTracker(t, &tr.keyTracker)
}

// 记录成本不随追踪槽位数增长（64 万 key 的长尾流量，80% 冷 key）。
func BenchmarkTrackerRecordBySlots(b *testing.B) {
	for _, slots := range []int{256, 4096, 65536} {
		b.Run(fmt.Sprintf("slots=%d", slots), func(b *testing.B) {
			rng := rand.New(rand.NewSource(1))
			zipf := rand.NewZipf(rng, 1.1, 8, 1<<20)
			tr, _ := newReqTracker(slots, 8, 0, 1<<40, true)
			keys := make([]uint64, 1<<16)
			for i := range keys {
				if rng.Intn(5) == 0 {
					keys[i] = zipf.Uint64() & 1023
				} else {
					keys[i] = rng.Uint64()
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				k := keys[i&(len(keys)-1)]
				tr.record(0, mix64(k), "key", 0)
			}
		})
	}
}

// 历史记录不含 Go 指针：长期保存的窗口历史对 GC 不可见（key 字节在无指针的字节块里）。
func TestHistoryRecordIsPointerFree(t *testing.T) {
	assertPointerFree(t, histRec{})
	assertPointerFree(t, winEntry{})
}

// 键池在大量逐出后仍保存每个槽位自己的 key，且逐出时归还旧块：泄漏会很快耗尽 25% 的余量，
// 表现为 dropped 增长。
func TestTrackerKeyPoolSurvivesChurn(t *testing.T) {
	const slots, maxKey = 64, 40
	keyOf := func(k uint64) string { return fmt.Sprintf("%s%d", strings.Repeat("x", int(k%20)), k) }
	tr, _ := newReqTracker(slots, maxKey, 0, 1<<40, true)
	want := map[uint64]string{}
	rng := rand.New(rand.NewSource(3))
	zipf := rand.NewZipf(rng, 1.05, 1, 1<<20)
	for i := 0; i < 300_000; i++ {
		k := zipf.Uint64()
		want[mix64(k)] = keyOf(k)
		tr.record(0, mix64(k), keyOf(k), 0)
	}
	checkTracker(t, &tr.keyTracker)
	if tr.dropped.Load() != 0 {
		t.Fatalf("key pool dropped %d events: blocks leaked or the reserve is too small", tr.dropped.Load())
	}
	for s := 0; s < tr.filled; s++ {
		if got := tr.keyString(s); got != want[tr.hash[s]] {
			t.Fatalf("slot %d holds key %q, want %q", s, got, want[tr.hash[s]])
		}
	}
	var live int64
	for s := 0; s < tr.filled; s++ {
		live += max(int64(minBlockUnits*arenaUnit), roundUp8(8+int64(tr.klen[s])))
	}
	if used := tr.kpool.usedBytes.Load(); used < live || used > live+int64(tr.filled)*2*arenaUnit {
		t.Fatalf("pool used %d bytes for %d bytes of live keys", used, live)
	}
}

// 同一窗口的回源榜与请求榜经列式存储往返后，榜单顺序与内容不变。
func TestHistoryColumnarRoundTripOrder(t *testing.T) {
	env := newTestEnv(t, NamespaceConfig{TTL: 60e9}, func(c *Config) {
		histCfg(c)
		c.Hot.TopN = 4
	})
	defer closeManual(t, env.c, env.ex)
	for i, n := range []int{5, 3, 3, 1} {
		for j := 0; j < n; j++ {
			if _, err := env.fetch(fmt.Sprintf("key%d", i), okLoader("v")); err != nil {
				t.Fatal(err)
			}
		}
	}
	env.clk.Advance(1100e6)
	env.drive()
	hist := env.c.HotKeyHistory(0)
	if len(hist) != 1 {
		t.Fatalf("history = %+v", hist)
	}
	var got []string
	for _, st := range hist[0].Top {
		got = append(got, st.Key)
	}
	if !slices.Equal(got, []string{"key0", "key1", "key2", "key3"}) {
		t.Fatalf("Top order = %v", got)
	}
	if len(hist[0].Requests) == 0 || hist[0].Requests[0].Key != "key0" {
		t.Fatalf("Requests = %+v", hist[0].Requests)
	}
}

// 热点记录零分配：入槽、逐出、草图过滤、合并三条路径都在预留的数组和键池里完成。
func TestTrackerRecordZeroAlloc(t *testing.T) {
	rt, _ := newReqTracker(128, 24, 0, 1<<40, true)
	ht, _ := newHotTracker(128, 24, 0, 1<<40, true)
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	zipf := rand.NewZipf(rand.New(rand.NewSource(9)), 1.1, 1, uint64(len(keys)-1))
	i := 0
	if a := testing.AllocsPerRun(20000, func() {
		k := zipf.Uint64()
		kh := mix64(k)
		rt.record(0, kh, keys[k], 0)
		ht.touchLoad(0, kh, keys[k], 0, 0)
		if i++; i%4 == 0 {
			ht.addMerged(0, kh, keys[k], 0, 0)
		}
	}); a != 0 {
		t.Fatalf("tracker record allocs = %v, want 0", a)
	}
	if rt.filled != 128 || ht.filled != 128 {
		t.Fatalf("tables not full: %d/%d, the eviction path was not exercised", rt.filled, ht.filled)
	}
}

// 关闭时键池内存区归还、存活 key 压实：榜单内容不变，表不再记录，且压实后的堆占用只与存活 key 成正比。
func TestTrackerReleaseCompactsAndKeepsKeys(t *testing.T) {
	const slots, maxKey = 64, 4096
	tr, _ := newHotTracker(slots, maxKey, 0, 1<<40, false)
	want := map[uint64]string{}
	for i := 0; i < 40; i++ {
		k := fmt.Sprintf("%s-%d", strings.Repeat("k", 1+i*7), i) // 长度各不相同，含非 8 倍数
		kh := mix64(uint64(i))
		want[kh] = k
		tr.touchLoad(1, kh, k, 0, 5)
	}
	before := map[uint64]string{}
	for s := 0; s < tr.filled; s++ {
		before[tr.hash[s]] = strings.Clone(tr.keyString(s)) // 视图在释放后失效，必须拷贝
	}
	tr.release()
	tr.release() // 幂等
	if len(before) != 40 {
		t.Fatalf("tracked %d keys, want 40", len(before))
	}
	for s := 0; s < tr.filled; s++ {
		if got := tr.keyString(s); got != want[tr.hash[s]] || got != before[tr.hash[s]] {
			t.Fatalf("slot %d: key %q after release, want %q", s, got, want[tr.hash[s]])
		}
	}
	if tr.tryLock() {
		t.Fatal("a released table must refuse new records")
	}
	h := newWinHeap(8, maxKey)
	tot, _, _ := tr.collect(0, &h, 0, 0)
	if tot[0] != 40 || len(h.es) != 8 {
		t.Fatalf("collect after release: loads=%d entries=%d", tot[0], len(h.es))
	}
	for _, e := range h.es {
		if entryKey(&h, e) != want[e.hash] {
			t.Fatalf("collected key %q, want %q", entryKey(&h, e), want[e.hash])
		}
	}
}
