package ahkc

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	rtmetrics "runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// 规模验证：默认跳过，显式开启：
//
//	AHKC_SCALE_ENTRIES=2000000 go test -run TestScaleResidentGC -v -timeout 30m .
//
// 填满 N 个变长值（64B~2KB）后测量：Go 堆存活量、GC 需要扫描的字节数、命中耗时与驻留偏差，
// 并与对象布局（值留在 Go 堆上）对照。字节布局的 GC 扫描量应与条目数无关。

// 值长度区间（字节）；对象布局对照可用 AHKC_SCALE_OBJECT=0 关闭（对象布局把全部值留在 Go 堆上，内存开销大）。
func scaleValueRange() (lo, hi int) {
	lo, hi = 64, 2047
	if v, err := strconv.Atoi(os.Getenv("AHKC_SCALE_VMIN")); err == nil && v > 0 {
		lo = v
	}
	if v, err := strconv.Atoi(os.Getenv("AHKC_SCALE_VMAX")); err == nil && v >= lo {
		hi = v
	}
	return lo, hi
}

func scaleEntries(t *testing.T) int {
	v := os.Getenv("AHKC_SCALE_ENTRIES")
	if v == "" {
		t.Skip("set AHKC_SCALE_ENTRIES to run the scale test")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("bad AHKC_SCALE_ENTRIES %q", v)
	}
	return n
}

func gcMetrics() (scanHeap, liveHeap uint64) {
	s := []rtmetrics.Sample{{Name: "/gc/scan/heap:bytes"}, {Name: "/gc/heap/live:bytes"}}
	rtmetrics.Read(s)
	if s[0].Value.Kind() == rtmetrics.KindUint64 {
		scanHeap = s[0].Value.Uint64()
	}
	if s[1].Value.Kind() == rtmetrics.KindUint64 {
		liveHeap = s[1].Value.Uint64()
	}
	return
}

type scaleResult struct {
	resident    int64
	residentB   int64
	scanHeap    uint64
	liveHeap    uint64
	gcPause     time.Duration
	hitNsPerOp  float64
	fillSeconds float64
	regionBytes int64
	newSeconds  float64 // 创建实例（含预留内存区与索引）耗时
	rssFilled   uint64  // 填满后的进程 RSS（字节）
}

// processRSS 读取当前进程的常驻集大小（linux）；其它平台返回 0。
func processRSS() uint64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	var size, rss uint64
	fmt.Sscanf(string(b), "%d %d", &size, &rss)
	return rss * uint64(os.Getpagesize())
}

// runScale 用 workers 个并发 Fetch 填充 n 个 key，等待驻留稳定后测量。
func runScale(t *testing.T, n int, build func(cfg Config) (fetch func(string) error, get func(string) bool, stats func() Stats, closeFn func(), mem MemoryLimits)) scaleResult {
	lo, hi := scaleValueRange()
	avgValue := (lo + hi) / 2
	cfg := Config{
		Shards:        16,
		MaxEntries:    int64(n),
		MaxBytes:      int64(n) * int64(avgValue+160),
		MaxEntryBytes: int64(hi) + 256,
		Namespaces:    map[string]NamespaceConfig{"t": {TTL: time.Hour, Tin: 1}},
		Heat: HeatConfig{
			Delta: time.Second,
		},
		Limits: LimitsConfig{
			FillQueueSize: 4096,
		},
	}
	newStart := time.Now()
	fetch, get, stats, closeFn, mem := build(cfg)
	newDur := time.Since(newStart)
	defer closeFn()

	runtime.GC()
	baseScan, _ := gcMetrics()
	_ = baseScan

	start := time.Now()
	var next atomic.Int64
	var wg sync.WaitGroup
	workers := runtime.GOMAXPROCS(0)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				for {
					err := fetch(fmt.Sprintf("key-%d", i))
					if err == nil {
						break
					}
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
	// 等待填充队列排空。
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		st := stats()
		if st.PendingFills == 0 && st.ResidentEntries >= int64(n)*95/100 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fillSec := time.Since(start).Seconds()

	runtime.GC()
	runtime.GC()
	rss := processRSS()
	scan, live := gcMetrics()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	st := stats()

	// 命中耗时：预先生成 key，避免把格式化成本计入读路径。
	keys := make([]string, min(n, 1<<20))
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
	}
	const probes = 2_000_000
	hits := 0
	t0 := time.Now()
	for i := 0; i < probes; i++ {
		if get(keys[(i*7919)%len(keys)]) {
			hits++
		}
	}
	elapsed := time.Since(t0)
	_ = hits
	return scaleResult{
		resident: st.ResidentEntries, residentB: st.ResidentBytes, scanHeap: scan, liveHeap: live,
		gcPause:    time.Duration(ms.PauseNs[(ms.NumGC+255)%256]),
		hitNsPerOp: float64(elapsed.Nanoseconds()) / probes, fillSeconds: fillSec,
		regionBytes: mem.RegionBytes, newSeconds: newDur.Seconds(), rssFilled: rss,
	}
}

func valueFor(i int) string {
	lo, hi := scaleValueRange()
	n := lo + int(uint64(i)*2654435761%uint64(hi-lo+1))
	b := make([]byte, n)
	for j := range b {
		b[j] = byte('a' + (i+j)%26)
	}
	return string(b)
}

func TestScaleResidentGC(t *testing.T) {
	n := scaleEntries(t)
	loader := func(_ context.Context, key string) (string, error) {
		i, _ := strconv.Atoi(key[4:])
		return valueFor(i), nil
	}
	report := func(name string, r scaleResult) {
		t.Logf("%-8s resident=%d (%.0f%%) residentBytes=%dMiB regionReserved=%dMiB rss=%dMiB liveHeap=%dMiB gcScanHeap=%dMiB new=%.3fs fill=%.1fs hit=%.0fns/op",
			name, r.resident, 100*float64(r.resident)/float64(n), r.residentB>>20, r.regionBytes>>20, r.rssFilled>>20,
			r.liveHeap>>20, r.scanHeap>>20, r.newSeconds, r.fillSeconds, r.hitNsPerOp)
	}

	bytesRes := runScale(t, n, func(cfg Config) (func(string) error, func(string) bool, func() Stats, func(), MemoryLimits) {
		c, err := NewBytes[string](cfg)
		if err != nil {
			t.Fatal(err)
		}
		ns, _ := c.Namespace("t")
		return func(k string) error { _, err := ns.Fetch(context.Background(), k, loader); return err },
			func(k string) bool {
				var r Request[string]
				var b Borrowed[string]
				ns.BeginRequestOn(&r, k)
				ok, _ := ns.BorrowOn(&r, &b)
				b.Release()
				return ok
			},
			c.Stats, func() { c.Close(context.Background()) }, c.MemoryLimits()
	})
	report("bytes", bytesRes)

	if bytesRes.resident < int64(n)*9/10 {
		t.Fatalf("bytes layout resident %d of %d", bytesRes.resident, n)
	}
	if os.Getenv("AHKC_SCALE_OBJECT") == "0" {
		return
	}
	objRes := runScale(t, n, func(cfg Config) (func(string) error, func(string) bool, func() Stats, func(), MemoryLimits) {
		c, err := NewObject[string](cfg, func(k string, v string) int64 { return blockBytes(len(k), int64(len(v))) })
		if err != nil {
			t.Fatal(err)
		}
		ns, _ := c.Namespace("t")
		return func(k string) error { _, err := ns.Fetch(context.Background(), k, loader); return err },
			func(k string) bool { _, ok, _ := ns.Get(k); return ok },
			c.Stats, func() { c.Close(context.Background()) }, c.MemoryLimits()
	})
	report("object", objRes)

	// 核心断言：字节布局的 GC 扫描量不随条目数增长；对象布局随条目数增长。
	if bytesRes.scanHeap > 256<<20 && bytesRes.scanHeap*4 > objRes.scanHeap*3 {
		t.Fatalf("bytes layout scans %dMiB, object layout %dMiB: no GC advantage", bytesRes.scanHeap>>20, objRes.scanHeap>>20)
	}
}

// 热点追踪规模验证：默认跳过，显式开启：
//
//	AHKC_SCALE_HOT=20000000 go test -run TestScaleHotTracker -v -timeout 30m .
//
// 16 个分片共 65536 个请求追踪槽，1 亿个 key 的 Zipf(1.1) 流量，取 N 个采样事件（单线程记录）。
// 测量：记录耗时、丢弃数、top-1 万召回率、窗口采集与发布延迟、历史占用与 GC 扫描量。
func TestScaleHotTracker(t *testing.T) {
	v := os.Getenv("AHKC_SCALE_HOT")
	if v == "" {
		t.Skip("set AHKC_SCALE_HOT to run the hot tracker scale test")
	}
	events, err := strconv.Atoi(v)
	if err != nil || events <= 0 {
		t.Fatalf("bad AHKC_SCALE_HOT %q", v)
	}
	const shards, slots, keySpace, topN = 16, 4096, 100_000_000, 10000
	rng := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rng, 1.1, 1, keySpace-1)
	trs := make([]*reqTracker, shards)
	for i := range trs {
		trs[i], _ = newReqTracker(slots, 16, 0, 1<<40, true)
	}
	const deep = 1 << 24 // 精确计数只覆盖前 2^24 名；更深的 key 频次远低于 top-1 万的门槛
	truth := make([]uint32, deep)
	ids := make([]uint64, 1<<20)
	t0 := time.Now()
	var recordNs time.Duration
	for done := 0; done < events; {
		batch := min(len(ids), events-done)
		for i := 0; i < batch; i++ {
			ids[i] = zipf.Uint64()
			if ids[i] < deep {
				truth[ids[i]]++
			}
		}
		start := time.Now()
		for _, id := range ids[:batch] {
			kh := mix64(id)
			trs[kh&(shards-1)].record(1, kh, "key-0000000000", 0)
		}
		recordNs += time.Since(start)
		done += batch
	}
	_ = t0

	collectStart := time.Now()
	h := newWinHeap(topN, 16)
	var samples, lost uint64
	for _, tr := range trs {
		s, l, _ := tr.collect(0, &h)
		samples, lost = samples+s, lost+l
	}
	collectDur := time.Since(collectStart)

	order := make([]uint32, 0, topN*2)
	for id := range truth {
		if truth[id] > 0 {
			order = append(order, uint32(id))
		}
	}
	slices.SortFunc(order, func(a, b uint32) int { return int(truth[b]) - int(truth[a]) })
	got := map[uint64]bool{}
	for _, e := range h.es {
		got[e.hash] = true
	}
	hit := 0
	for _, id := range order[:topN] {
		if got[mix64(uint64(id))] {
			hit++
		}
	}

	var dropped uint64
	for _, tr := range trs {
		dropped += tr.dropped.Load()
	}
	scanBefore, liveBefore := gcMetrics()
	packStart := time.Now()
	recs, keys := packWin[string](&h, nil, make([]byte, 0, topN*16))
	packDur := time.Since(packStart)
	ring := make([]histSnap, 0, 1440) // 一天的 1 分钟窗口
	for i := 0; i < 1440; i++ {
		ring = append(ring, histSnap{req: slices.Clone(recs), keys: slices.Clone(keys)})
	}
	runtime.GC()
	runtime.GC()
	scanAfter, liveAfter := gcMetrics()
	runtime.KeepAlive(ring)
	t.Logf("events=%d record=%.1fns/op samples=%d dropped=%d lost=%d recall@%d=%.1f%% collect=%v pack=%v",
		events, float64(recordNs.Nanoseconds())/float64(events), samples, dropped, lost, topN, 100*float64(hit)/topN, collectDur, packDur)
	t.Logf("history: %d windows × %d records = %d MiB; GC scan heap %d→%d MiB, live heap %d→%d MiB; tracker arrays %d MiB",
		len(ring), len(recs), len(ring)*(len(recs)*int(unsafe.Sizeof(histRec{}))+len(keys))>>20,
		scanBefore>>20, scanAfter>>20, liveBefore>>20, liveAfter>>20, int(trackBytes(slots*shards))>>20)
	if recall := float64(hit) / topN; recall < 0.9 {
		t.Fatalf("top-%d recall %.1f%% < 90%%", topN, 100*recall)
	}
}

// ---- 全周期验收（百万级）----

func mallocCount() uint64 {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Mallocs
}

// 满载的索引上，未命中查询在遇到 overflow 为 0 的组之前要访问的平均组数。
func (ix *index) missGroups(probes int) float64 {
	visited := 0
	for i := 0; i < probes; i++ {
		g := ix.home(mix64(uint64(1)<<50 + uint64(i)))
		visited++
		for j := 1; j < ix.probes && uint32(ix.words[int(g)*groupWords].Load()) != 0; j++ {
			g = ix.next(g)
			visited++
		}
	}
	return float64(visited) / float64(probes)
}

// 默认跳过；AHKC_SCALE_LIFECYCLE=1000000 开启。全程真实时钟与内置周期触发（TTL 与慢扫描周期按规模缩短），
// 填充、命中与回收都是真实并发。逐项对应验收：创建与常驻内存偏差、GC 扫描量、命中耗时与 P99、
// 填充吞吐、未命中探测组数、满载后热 key 替换冷 key 且受保护的热条目不被挤出、
// 过期条目在一个慢扫描周期内退场、退休与回收全程不分配。
//
//	AHKC_SCALE_LIFECYCLE=1000000 go test -run TestScaleLifecycle -v -timeout 10m .
func TestScaleLifecycle(t *testing.T) {
	v := os.Getenv("AHKC_SCALE_LIFECYCLE")
	if v == "" {
		t.Skip("set AHKC_SCALE_LIFECYCLE to run the lifecycle acceptance test")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 10000 {
		t.Fatalf("bad AHKC_SCALE_LIFECYCLE %q", v)
	}
	t.Setenv("AHKC_SCALE_VMIN", "16")
	t.Setenv("AHKC_SCALE_VMAX", "240")
	const shards, sweep = 16, 10 * time.Second
	ttl := 25*time.Second + time.Duration(n/50_000)*time.Second // 须长于全部填充与测量阶段
	rss0 := processRSS()
	tNew := time.Now()
	c, err := NewBytes[string](Config{
		Shards: shards, MaxEntries: int64(n), MaxBytes: int64(n) * 288, MaxEntryBytes: 512,
		Namespaces: map[string]NamespaceConfig{"t": {TTL: ttl, Tin: 1}},
		Heat:       HeatConfig{Delta: time.Second},
		Limits:     LimitsConfig{FillQueueSize: 4096},
		Runtime:    RuntimeConfig{SweepPeriod: sweep},
	})
	if err != nil {
		t.Fatal(err)
	}
	newDur := time.Since(tNew)
	ns, _ := c.Namespace("t")
	defer c.Close(context.Background())
	quiesce := func() {
		for i := 0; i < 2000; i++ {
			busy := false
			for _, s := range c.c.shards {
				if s.ownerState.Load() != ownerIdle || len(s.fillQueue) > 0 {
					busy = true
				}
			}
			if !busy {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	loader := func(_ context.Context, key string) (string, error) {
		i, _ := strconv.Atoi(key[strings.IndexByte(key, '-')+1:])
		return valueFor(i), nil
	}
	fetchAll := func(prefix string, from, to int) {
		var next atomic.Int64
		next.Store(int64(from))
		var wg sync.WaitGroup
		for w := 0; w < runtime.GOMAXPROCS(0); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= to {
						return
					}
					for {
						if _, err := ns.Fetch(context.Background(), prefix+strconv.Itoa(i), loader); err == nil {
							break
						}
						runtime.Gosched()
					}
				}
			}()
		}
		wg.Wait()
		for deadline := time.Now().Add(2 * time.Minute); c.Stats().PendingFills > 0 && time.Now().Before(deadline); {
			time.Sleep(10 * time.Millisecond)
		}
		quiesce()
	}

	// 1. 填充
	runtime.GC()
	tFill := time.Now()
	fetchAll("key-", 0, n)
	fillSec := time.Since(tFill).Seconds()
	runtime.GC()
	runtime.GC()
	st := c.Stats()
	mem := c.MemoryLimits()
	scan, live := gcMetrics()
	rss := processRSS()
	perEntry := float64(rss-rss0) / float64(st.ResidentEntries)
	t.Logf("create=%.3fs fill=%.1fs (%.0f fills/s) resident=%d (%.1f%%) residentBytes=%dMiB region(reserved)=%dMiB rss=+%dMiB (%.0f B/entry, billed %.0f B/entry) liveHeap=%dMiB gcScan=%dMiB",
		newDur.Seconds(), fillSec, float64(n)/fillSec, st.ResidentEntries, 100*float64(st.ResidentEntries)/float64(n),
		st.ResidentBytes>>20, mem.RegionBytes>>20, (rss-rss0)>>20, perEntry, float64(st.ResidentBytes)/float64(st.ResidentEntries), live>>20, scan>>20)
	if st.ResidentEntries < int64(n)*98/100 {
		t.Fatalf("only %d of %d entries resident", st.ResidentEntries, n)
	}
	if live > 16<<20 {
		t.Fatalf("Go live heap %d MiB depends on the entry count", live>>20)
	}
	// 常驻内存偏差：RSS 增量相对计费字节。计费已含块头与对齐；其余是索引、门卫与运行时。
	// 小规模时固定开销与 loader 垃圾占比大，只在百万级判定。
	if dev := float64(rss-rss0)/float64(st.ResidentBytes) - 1; n >= 500_000 && dev > 0.25 {
		t.Fatalf("RSS exceeds billed bytes by %.0f%% (budget 25%%)", 100*dev)
	}

	// 2. 未命中探测
	var missSum float64
	for _, s := range c.c.shards {
		missSum += s.idx.missGroups(20000)
	}
	t.Logf("miss probes on the full index: %.2f groups per lookup (limit %d)", missSum/shards, maxGroups)

	// 3. 命中：单线程逐次耗时（扣除计时本身）与并行吞吐，期间不分配
	keys := make([]string, min(n, 1<<20))
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	var r Request[string]
	var b Borrowed[string]
	hit := func(k string) bool {
		ns.BeginRequestOn(&r, k)
		ok, _ := ns.BorrowOn(&r, &b)
		b.Release()
		return ok
	}
	lat := make([]int64, 400_000)
	t0 := time.Now()
	for i := range lat { // 计时开销
		t1 := time.Now()
		lat[i] = int64(t1.Sub(t0))
		t0 = t1
	}
	slices.Sort(lat)
	overhead := lat[len(lat)/2]
	m0 := mallocCount()
	hits := 0
	for i := range lat {
		s := time.Now()
		if hit(keys[(i*7919)%len(keys)]) {
			hits++
		}
		lat[i] = int64(time.Since(s))
	}
	mallocs := mallocCount() - m0
	slices.Sort(lat)
	pct := func(p float64) int64 { return lat[int(p*float64(len(lat)-1))] - overhead }
	t.Logf("borrowed hit (single goroutine, hit %.1f%%): p50=%dns p99=%dns p99.9=%dns (timer overhead %dns removed), mallocs=%d over %d ops",
		100*float64(hits)/float64(len(lat)), pct(0.5), pct(0.99), pct(0.999), overhead, mallocs, len(lat))
	if mallocs > 50 {
		t.Fatalf("borrowed hits allocated %d times in %d ops", mallocs, len(lat))
	}
	var total atomic.Int64
	par := runtime.GOMAXPROCS(0)
	tPar := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < par; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var r Request[string]
			var b Borrowed[string]
			ops := 0
			for i := 0; i < 1_000_000; i++ {
				ns.BeginRequestOn(&r, keys[(i*7919+w*104729)%len(keys)])
				if ok, _ := ns.BorrowOn(&r, &b); ok {
					b.Release()
				}
				ops++
			}
			total.Add(int64(ops))
		}(w)
	}
	wg.Wait()
	t.Logf("borrowed hit (%d goroutines): %.1f M ops/s", par, float64(total.Load())/time.Since(tPar).Seconds()/1e6)

	// 4. 满载后：受保护的热条目不被新 key 挤出
	prot := make([]string, 0, n/50) // 取已驻留的：分片额度固定，填充时本来就有约 1% 的 key 因分片超额没有进来
	for i := 0; i < n; i += 50 {
		if _, ok, _ := ns.Get("key-" + strconv.Itoa(i)); ok {
			prot = append(prot, "key-"+strconv.Itoa(i))
		}
	}
	for rep := 0; rep < 20; rep++ {
		for _, k := range prot {
			ns.Get(k)
		}
	}
	pub0 := c.Stats().FillPublished
	fetchAll("new-", 0, n/4)
	kept := 0
	for _, k := range prot {
		if _, ok, _ := ns.Get(k); ok {
			kept++
		}
	}
	admitted := c.Stats().FillPublished - pub0
	t.Logf("after %d new keys on a full cache: %d replacements published, protected hot entries kept %d/%d (%.2f%%)",
		n/4, admitted, kept, len(prot), 100*float64(kept)/float64(len(prot)))
	if float64(kept) < 0.99*float64(len(prot)) {
		t.Fatalf("hot entries were evicted by cold newcomers: kept %d of %d", kept, len(prot))
	}

	// 5. 热度衰减后，新的热 key 稳定替换旧的冷 key
	time.Sleep(3 * time.Second)
	hot := n / 20
	fetchAll("hot-", 0, hot)
	fetchAll("hot-", 0, hot) // 第二遍：已驻留的应当命中
	lastLoad := time.Now()   // 之后不再有填充：这是全部条目硬截止的上界基准
	hotHit := 0
	for i := 0; i < hot; i++ {
		if _, ok, _ := ns.Get("hot-" + strconv.Itoa(i)); ok {
			hotHit++
		}
	}
	t.Logf("after heat decay: hot newcomers resident %d/%d (%.1f%%), resident total %d (cap %d)",
		hotHit, hot, 100*float64(hotHit)/float64(hot), c.Stats().ResidentEntries, n)
	if float64(hotHit) < 0.9*float64(hot) {
		t.Fatalf("hot keys did not replace cold ones: %d of %d resident", hotHit, hot)
	}
	if got := c.Stats().ResidentEntries; got > int64(n) {
		t.Fatalf("resident %d exceeds MaxEntries %d", got, n)
	}

	// 6. 过期：真实时间等到最后一批条目的硬截止，之后全部退场不得晚于一个慢扫描周期（含回收）。
	// 轮询只读原子计数，不产生分配。
	residentNow := func() (n int64) {
		for _, s := range c.c.shards {
			n += s.residentCount.Load()
		}
		return n
	}
	residentStart := residentNow()
	lastDeadline := lastLoad.Add(ttl)
	m0 = mallocCount()
	for time.Now().Before(lastDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if _, ok, _ := ns.Get("key-1"); ok {
		t.Fatal("expired value delivered")
	}
	var lag time.Duration
	for deadline := lastDeadline.Add(3 * sweep); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if residentNow() == 0 {
			break
		}
	}
	lag = max(0, time.Since(lastDeadline))
	if residentNow() != 0 {
		t.Fatalf("%d entries still resident 3 sweep periods after the last deadline", residentNow())
	}
	var retired int64
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		retired = 0
		for _, s := range c.c.shards {
			retired += s.retiredCount.Load() + s.arena.usedBytes.Load()
		}
		if retired == 0 {
			break
		}
	}
	mallocs = mallocCount() - m0
	st = c.Stats()
	t.Logf("expiry (ttl %v, sweep period %v): %d entries; last deadline passed %v ago when the last one left (reclaimed too: pending=%d); mallocs=%d over the whole wait (%.5f per entry); used=%dMiB largestFree=%dMiB free=%dMiB",
		ttl, sweep, residentStart, lag.Round(time.Millisecond), st.RetiredPending, mallocs, float64(mallocs)/float64(residentStart), st.ArenaUsedBytes>>20, st.ArenaLargestFree>>20, st.ArenaFreeBytes>>20)
	if lag > sweep+sweep/2 {
		t.Fatalf("entries took %v to leave, want within one sweep period %v", lag, sweep)
	}
	if st.RetiredPending != 0 || st.ArenaUsedBytes != 0 {
		t.Fatalf("not fully reclaimed: pending=%d used=%d", st.RetiredPending, st.ArenaUsedBytes)
	}
	if st.ArenaLargestFree*shards < st.ArenaFreeBytes*99/100 {
		t.Fatalf("free space fragmented after full reclaim: largest %d × %d shards < free %d", st.ArenaLargestFree, shards, st.ArenaFreeBytes)
	}
	if mallocs > uint64(residentStart)/100 {
		t.Fatalf("retire and reclaim allocated %d times for %d entries", mallocs, residentStart)
	}
}
