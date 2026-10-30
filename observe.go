package ahkc

import (
	"cmp"
	"slices"
	"sync"
	"time"
	"unsafe"
)

// 观测（仅统计不缓存）。
//
// ModeObserve 下值永不驻留，热度门卫 / 稀疏采样 / singleflight 合并
// 照常工作；本文件提供把"当前高度重复的数据"变成可查询状态的接口：
//
//   - Namespace.Heat(key)：单 key 请求热度探针；
//   - Cache.HotKeys(topN)：回源热点榜单（窗口加载热度 + 累计加载 + 被合并的等待请求）；
//   - Cache.HotKeyHistory / HotWindowsSince / Hot.OnWindow：与准入热度窗口解耦的
//     历史窗口快照（回源热点 + 请求热点），增量拉取与串行推送，对接外部监控。
//
// 所有结构固定槽位、初始化时分配，运行期零无界增长（A.1）。

// hotSlotBytes 是回源表每个槽位在追踪表核心之外的固定开销（key 在核心的键池里）。
const hotSlotBytes = 8 + 8 + 8 + 2*4

// hotTracker 是每分片的回源热点追踪表：固定槽位，触碰点只在真实加载（leader）与
// 等待者合并路径，频率受 MaxConcurrentLoads / MaxWaitersPerKey 天然约束，互斥锁不落在
// 普通读路径；观测写入都是 TryLock，失败只计数。淘汰与历史窗口共用追踪表核心（tracker.go）：
// 逐出窗口计数最小者，新 key 继承其计数作为误差上界；是否保留历史只决定是否采集。
// key 存在核心的键池里，整张表不含指针。
type hotTracker struct {
	keyTracker

	cells  []heatCell // 窗口加载热度（双桶热度，饱和上限 heatCountMax），决定即时榜顺序
	loads  []uint64   // 累计真实加载次数（被逐出即重置）
	merged []uint64   // 累计被合并的等待请求数（被逐出即重置）
	// 窗口内真实加载数在 aux 列；合并数 = rank - err - 加载数。
}

func newHotTracker(n, maxKey int, align, interval uint64, heap bool) (*hotTracker, error) {
	t := &hotTracker{}
	if err := t.init(n, maxKey, align, interval, heap); err != nil {
		return nil, err
	}
	t.initAux()
	t.cells, t.loads, t.merged = make([]heatCell, n), make([]uint64, n), make([]uint64, n)
	return t, nil
}

// reset 重置刚被占用的槽位的负载列。
func (t *hotTracker) reset(s int, e uint64) {
	t.loads[s], t.merged[s] = 0, 0
	t.cells[s].v.Store(packHeat(heatSnap{epoch: e}))
}

// touchLoad 记录一次真实加载：窗口加载热度 +1、累计加载 +1、窗口计数 +1。
// 全程持锁：计数与逐出串行化，避免被逐槽位错记到新 key 头上。
func (t *hotTracker) touchLoad(nsID uint32, kh uint64, key string, e uint64, now int64) {
	if !t.tryLock() {
		return
	}
	s, p, fresh := t.touch(nsID, kh, key, now)
	if s >= 0 {
		if fresh {
			t.reset(s, e)
		}
		t.loads[s]++
		t.cells[s].add(e, heatCountMax, 2)
		satInc(&t.aux[p][s])
	}
	t.win.tot[p][0]++
	t.mu.Unlock()
}

// addMerged 记录一次等待者合并（该 key 的重复请求被 singleflight 吸收）。
// 等待者可能先于 loader 启动到达：槽位不存在时先入驻（loads=0），加载发生时再补记。
func (t *hotTracker) addMerged(nsID uint32, kh uint64, key string, e uint64, now int64) {
	if !t.tryLock() {
		return
	}
	s, p, fresh := t.touch(nsID, kh, key, now)
	if s >= 0 {
		if fresh {
			t.reset(s, e)
		}
		t.merged[s]++
	}
	t.win.tot[p][1]++
	t.mu.Unlock()
}

func (t *hotTracker) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.filled
}

// eachStat 在表锁内逐槽回调当前榜单统计。Key 是键池里的视图，只在回调期间有效，要保留必须拷贝。
func (t *hotTracker) eachStat(e uint64, phi float64, nsName func(uint32) string, fn func(HotKeyStat)) {
	t.mu.Lock()
	for i := 0; i < t.filled; i++ {
		heat, _ := t.cells[i].read(e, phi) // UNKNOWN（时钟回退）读作 0，计数照常
		fn(HotKeyStat{Namespace: nsName(t.ns[i]), Key: t.keyString(i), LoadHeat: heat, Loads: t.loads[i], Merged: t.merged[i]})
	}
	t.mu.Unlock()
}

// collect 结束窗口 w 的采集：把候选压入有界堆并清零该窗口缓冲（短锁，不分配）。
// e、phi 用于读取候选的当前 LoadHeat。
func (t *hotTracker) collect(w uint64, h *winHeap, e uint64, phi float64) (tot [2]uint64, lost uint64, partial bool) {
	t.mu.Lock()
	tot, st, lost := t.win.take(w)
	if st == winData {
		p := w & 1
		rank, errs, loads := t.rank[p], t.err[p], t.aux[p]
		for i := 0; i < t.filled; i++ {
			seen := rank[i] - errs[i]
			if seen == 0 {
				continue
			}
			if score := uint64(rank[i]); h.wants(score, t.hash[i]) {
				n := min(loads[i], seen)
				heat, _ := t.cells[i].read(e, phi)
				h.add(winEntry{
					score: score, hash: t.hash[i], loads: t.loads[i], merged: t.merged[i],
					heat: heat, n: n, m: seen - n, err: errs[i], ns: t.ns[i],
				}, t.keyBytes(i))
			}
		}
	}
	t.finish(w, st)
	t.mu.Unlock()
	return tot, lost, st == winPartial
}

// ---- 完成路径挂钩 ----

// observeLoad 在真实 loader 调用时记录（与 metrics.loads 同点，leader 唯一）。
func (c *cache[V]) observeLoad(f *flight[V], now int64) {
	if h := c.shards[f.shardID].hot; h != nil {
		h.touchLoad(f.nsID, f.keyHash, f.key, bucketIndex(now, int64(c.cfg.Heat.Delta)), now)
	}
}

// observeMerge 在等待者合并进既有 flight 后记录（在 flight 条带锁之外调用）。
// key 必须是持锁时拷出的 flight.key（自有副本），不得使用调用方的 key。
func (c *cache[V]) observeMerge(s *shard[V], nsID uint32, keyHash uint64, key string, now int64) {
	if s.hot != nil {
		s.hot.addMerged(nsID, keyHash, key, bucketIndex(now, int64(c.cfg.Heat.Delta)), now)
	}
}

// ---- 对外观测接口 ----

// Heat 返回 key 当前窗口的请求热度估计 H(t) = current + (1-φ)·previous
// （窗口宽 Heat.Delta）。未命中计入门卫（碰撞可高估，饱和与丢更新可低估）、
// 命中计入驻留条目热度，二者之和即请求热度；ModeObserve 下无驻留
// 条目，返回值即门卫热度。纯观测：不记录热度、不产生副作用。
// ok=false 表示 key 超限、实例已关闭或热度未知（时钟回退等）。
// 保护槽竞争（ErrBusy 档位）时只返回门卫部分，属有界近似。
func (n *Namespace[V]) Heat(key string) (float64, bool) {
	if n == nil || n.ns == nil {
		return 0, false
	}
	c := n.ns.c
	if len(key) > c.cfg.MaxKeyBytes || c.state.Load() != cacheOpen {
		return 0, false
	}
	kh := n.ns.hashKey(key)
	s := c.shardOf(kh)
	now := c.now()
	delta := int64(c.cfg.Heat.Delta)
	e := bucketIndex(now, delta)
	phi := bucketPhi(now, delta)
	h, ok := s.gate.read(kh, e, phi)
	if !ok {
		return 0, false
	}
	if g, pinOK := s.epoch.pin(); pinOK {
		if c.state.Load() != cacheOpen {
			g.unpin()
			return 0, false
		}
		fr := s.idx.find(key, kh, n.ns.id)
		if fr.found {
			eh, eok := fr.e.heat.read(e, phi)
			valid := fr.e.isReadable() && fr.e.genIs(n.ns.gen.Load()) && now < fr.e.hardDeadline
			g.unpin()
			if !eok {
				return 0, false
			}
			if valid {
				h += eh // 驻留条目热度只属于当前逻辑（代际有效）key
			}
		} else {
			g.unpin()
		}
	}
	return h, true
}

// HotKeyStat 是一条热点 key 观测记录。
type HotKeyStat struct {
	// Namespace / Key 是被追踪身份（命名空间 + 完整 key；设置 Hot.Label 后 Key 为映射结果）。
	Namespace string
	Key       string
	// LoadHeat 当前窗口的加载热度估计（双桶加权，窗口宽 Heat.Delta）。
	// 高度重复的顺序回源会在这里直接体现；并发合并场景看 Merged。
	LoadHeat float64
	// Loads 累计真实加载次数（自该 key 被追踪起，逐出后重置）。
	Loads uint64
	// Merged 累计被 singleflight 合并的等待请求数：高并发重复回源的直接证据。
	Merged uint64
	// WindowLoads / WindowMerged（回源热点榜）：key 入槽以来在本历史窗口内的精确
	// 加载/合并次数（下界）。仅出现在历史窗口快照中。
	WindowLoads, WindowMerged uint64
	// WindowRequests（请求热点榜）：本历史窗口内的请求数估计 = 采样数 × 1/q
	// （含命中；q 为 Hot.RequestSampleP）。仅出现在历史窗口快照中。
	WindowRequests float64
	// WindowError 是入槽时继承的最小计数（Space-Saving 误差上界；请求榜已折算 1/q）：
	// 在无丢弃/饱和时约束记录到的事件或样本数；请求榜仍有采样误差。按"观测值 + 误差"降序。
	WindowError float64
}

// cmpHotKeyStat 是 HotKeys 榜单全序：LoadHeat 降序、Merged/Loads 依次破平，
// 最后以命名空间/key 字典序稳定化。
func cmpHotKeyStat(a, b HotKeyStat) int {
	if d := cmp.Compare(b.LoadHeat, a.LoadHeat); d != 0 {
		return d
	}
	if d := cmp.Compare(b.Merged, a.Merged); d != 0 {
		return d
	}
	if d := cmp.Compare(b.Loads, a.Loads); d != 0 {
		return d
	}
	if a.Namespace != b.Namespace {
		return cmp.Compare(a.Namespace, b.Namespace)
	}
	return cmp.Compare(a.Key, b.Key)
}

func betterHotKeyStat(a, b *HotKeyStat) bool { return cmpHotKeyStat(*a, *b) < 0 }

// collectHot 采集当前回源热点榜单（跨全部分片）。topN > 0 时用有界小顶堆选择
// （分配 O(topN)）；否则返回全部被追踪 key。榜单只覆盖加载路径触达的 key：
// Get-only 流量与缓存命中不进榜单，可用 Heat 探针或请求热点（Hot.RequestSlots）。
func (c *cache[V]) collectHot(topN int) []HotKeyStat {
	now := c.now()
	delta := int64(c.cfg.Heat.Delta)
	e, phi := bucketIndex(now, delta), bucketPhi(now, delta)
	list := c.nsList()
	nsName := func(id uint32) string { return nsNameOf(list, id) }
	capacity := 0
	for _, s := range c.shards {
		if s.hot != nil {
			capacity += s.hot.size()
		}
	}
	if topN > 0 {
		capacity = min(capacity, topN)
	}
	out := make([]HotKeyStat, 0, capacity)
	// st.Key 是键池视图：入选时才拷进共用缓冲。缓冲写满就换新的，已发出的 key 仍指向旧缓冲，内容不再改动。
	kb := make([]byte, 0, max(256, capacity*32))
	own := func(key string) string {
		if len(key) == 0 {
			return ""
		}
		if cap(kb)-len(kb) < len(key) {
			kb = make([]byte, 0, max(2*cap(kb), len(key)))
		}
		n := len(kb)
		kb = append(kb, key...)
		return unsafe.String(&kb[n], len(key))
	}
	visit := func(st HotKeyStat) {
		switch {
		case topN <= 0:
			st.Key = own(st.Key)
			out = append(out, st)
		case len(out) < topN:
			st.Key = own(st.Key)
			out = append(out, st)
			heapUp(out, len(out)-1, betterHotKeyStat)
		case cmpHotKeyStat(st, out[0]) < 0:
			st.Key = own(st.Key)
			out[0] = st
			heapDown(out, 0, betterHotKeyStat)
		}
	}
	for _, s := range c.shards {
		if s.hot != nil {
			s.hot.eachStat(e, phi, nsName, visit)
		}
	}
	slices.SortFunc(out, cmpHotKeyStat)
	return out
}

// HotKeys 返回回源热点 key 榜单快照（跨全部分片），按 LoadHeat 降序、
// Merged / Loads 依次破平。topN <= 0 返回全部被追踪 key。
// 追踪槽位有限（每分片 Hot.LoadSlots 个），未进入榜单的 key 仍可通过
// Namespace.Heat 探针查询；门卫碰撞可高估，饱和与丢更新可低估。
// 设置了 Hot.Label 时 Key 为映射结果；返回切片归调用方所有。
func (c *Cache[V]) HotKeys(topN int) []HotKeyStat {
	if c == nil || c.c == nil || c.c.cfg.Hot.LoadSlots == 0 {
		return nil
	}
	out := c.c.collectHot(topN)
	if c.c.cfg.Hot.Label != nil {
		out = c.c.mapStats(out[:0], out) // 原地压缩：写下标不超过读下标
	}
	return out
}

// ---- 窗口历史 ----

// HotWindowTotals 是一个历史窗口的总量与数据质量信号。
type HotWindowTotals struct {
	// Loads / Merged 窗口内已记录的回源加载/合并事件数（含未进榜单的 key）。
	Loads, Merged uint64
	// Requests 窗口内请求总数估计 =（已记录采样 + 因锁竞争丢弃的采样）× 1/q；
	// 未启用请求热点时为 0。
	Requests float64
	// LoadEventsDropped / RequestSamplesDropped 因追踪锁竞争丢弃的事件数
	// （按采集时刻归窗，有界近似）。
	LoadEventsDropped, RequestSamplesDropped uint64
	// Evicted 未被采集就被覆盖/逐出而丢失的事件数（维护滞后或追踪槽位不足；
	// 请求热点按采样数计）。
	Evicted uint64
}

// HotSnapshot 是一个历史窗口结束时的快照；导出的榜单归调用方所有。
type HotSnapshot struct {
	// Seq 单调递增的快照序号（从 1 起），用于 HotWindowsSince 游标与缺窗判断。
	Seq uint64
	// Bucket 窗口号 = (时钟 + 对齐偏移) / Interval。
	Bucket uint64
	// Interval 窗口宽度；Start / End 是窗口的 UTC 墙钟范围（内置时钟下按 Unix 时间对齐）。
	Interval   time.Duration
	Start, End time.Time
	// Skipped 本快照与上一份快照之间因维护滞后而未发布的窗口数。
	Skipped uint64
	// Partial 为 true 表示部分分片的该窗口数据在采集前已被更新的窗口覆盖。
	Partial bool
	// Top 回源热点榜（窗口内有加载/合并的 key）；Requests 请求热点榜（含命中，估计值）。
	// 各最多 Hot.TopN 条，按"观测 + 误差"降序。
	Top, Requests []HotKeyStat
	Totals        HotWindowTotals
}

// hotRoller 是增量采集状态，仅维护者（tickOwner）访问。
type hotRoller struct {
	nextW, nextStart       uint64 // 下一个待发布窗口及其结束的对齐时间
	active                 bool
	w, skipped             uint64 // 正在采集的窗口及其跳过的窗口数
	shard                  int
	partial                bool
	tot                    HotWindowTotals
	samples                uint64 // 窗口内已记录的请求采样数
	hotDropped, reqDropped uint64 // 上次发布时的累计丢弃计数

	hot, req winHeap
}

// hotHistory 是固定环形窗口历史：rollHotWindow（维护触发）写，
// HotKeyHistory / HotWindowsSince（任意线程）读。ring 可为空（仅通知模式）。
type hotHistory struct {
	mu      sync.Mutex
	ring    []histSnap
	head, n int
	seq     uint64

	interval, align uint64
	roll            hotRoller

	// pending 是待发给 Hot.OnWindow 的快照，由 cache.hotCallbackPending 保护；
	// task 是预建的投递任务（避免每窗口分配闭包）。
	pending histSnap
	task    func()
}

func newHotHistory(cfg *Config, firstWindow, align uint64) *hotHistory {
	interval := uint64(cfg.Hot.Interval)
	h := &hotHistory{ring: make([]histSnap, cfg.Hot.Windows), interval: interval, align: align}
	h.roll.nextW, h.roll.nextStart = firstWindow, (firstWindow+1)*interval
	if cfg.Hot.LoadSlots > 0 {
		h.roll.hot = newWinHeap(cfg.Hot.TopN, cfg.MaxKeyBytes)
	}
	if cfg.Hot.RequestSlots > 0 {
		h.roll.req = newWinHeap(cfg.Hot.TopN, cfg.MaxKeyBytes)
	}
	return h
}

// latest 返回最近 n 份快照（时间升序）；n 须在 [0, h.n]，调用方持有 mu。
func (h *hotHistory) latest(n int) []histSnap {
	out := make([]histSnap, n)
	start := (h.head - n + len(h.ring)) % len(h.ring)
	for i := range out {
		out[i] = h.ring[(start+i)%len(h.ring)]
	}
	return out
}

// rollHotWindow 滚动历史窗口（periodicTick / MaintenanceStep 驱动，仅 tickOwner 调用）。
// 未跨窗口时只有一次时钟读取与整数比较；跨窗口按分片采集，以 hotCollectBudget
// 为每周期目标预算，采完全部分片才发布。
func (c *cache[V]) rollHotWindow() {
	h := c.hotHist
	if h == nil || c.state.Load() != cacheOpen {
		return
	}
	r, now := &h.roll, c.now()
	if !r.active {
		ts := uint64(now) + h.align
		if ts < r.nextStart {
			return
		}
		e := ts / h.interval
		r.w, r.skipped = e-1, e-1-r.nextW
		r.nextW, r.nextStart = e, (e+1)*h.interval
		r.active, r.shard, r.partial, r.samples, r.tot = true, 0, false, 0, HotWindowTotals{}
	}
	delta := int64(c.cfg.Heat.Delta)
	e, phi := bucketIndex(now, delta), bucketPhi(now, delta)
	for budget := hotCollectBudget; budget > 0 && r.shard < len(c.shards); {
		s := c.shards[r.shard]
		r.shard++
		if s.hot != nil {
			tot, lost, partial := s.hot.collect(r.w, &r.hot, e, phi)
			r.tot.Loads, r.tot.Merged, r.tot.Evicted = r.tot.Loads+tot[0], r.tot.Merged+tot[1], r.tot.Evicted+lost
			r.partial = r.partial || partial
			budget -= s.hot.n
		}
		if s.req != nil {
			samples, lost, partial := s.req.collect(r.w, &r.req)
			r.samples, r.tot.Evicted = r.samples+samples, r.tot.Evicted+lost
			r.partial = r.partial || partial
			budget -= s.req.n
		}
	}
	if r.shard == len(c.shards) {
		r.active = false
		c.publishHotWindow()
	}
}

// publishHotWindow 由采集结果构造列式快照，写入历史环并提交窗口通知。
func (c *cache[V]) publishHotWindow() {
	h := c.hotHist
	r := &h.roll
	list := c.nsList()
	var hotDropped, reqDropped uint64
	for _, s := range c.shards {
		if s.hot != nil {
			hotDropped += s.hot.dropped.Load()
		}
		if s.req != nil {
			reqDropped += s.req.dropped.Load()
		}
	}
	snap := histSnap{bucket: r.w, skipped: r.skipped, partial: r.partial, totals: r.tot}
	keyBytes := 0
	for i := range r.hot.es {
		keyBytes += int(r.hot.es[i].klen)
	}
	for i := range r.req.es {
		keyBytes += int(r.req.es[i].klen)
	}
	keys := make([]byte, 0, keyBytes)
	snap.top, keys = packWin(&r.hot, list, keys)
	snap.req, keys = packWin(&r.req, list, keys)
	snap.keys = keys
	snap.totals.LoadEventsDropped = hotDropped - r.hotDropped
	snap.totals.RequestSamplesDropped = reqDropped - r.reqDropped
	if c.reqThreshold != 0 {
		snap.totals.Requests = float64(r.samples+snap.totals.RequestSamplesDropped) * c.reqInvQ
	}
	r.hotDropped, r.reqDropped = hotDropped, reqDropped
	r.hot.reset()
	r.req.reset()

	h.mu.Lock()
	h.seq++
	snap.seq = h.seq
	if len(h.ring) > 0 {
		h.ring[h.head] = snap
		h.head = (h.head + 1) % len(h.ring)
		h.n = min(h.n+1, len(h.ring))
	}
	h.mu.Unlock()

	if c.cfg.Hot.OnWindow == nil {
		return
	}
	// 回调串行：上一个仍在执行或提交失败即丢弃并计数，不在维护线程重试、排队或执行宿主代码。
	if !c.hotCallbackPending.CompareAndSwap(false, true) {
		c.metrics.hotWindowBusy.Add(1)
		c.metrics.hotWindowDropped.Add(1)
		return
	}
	h.pending = snap
	if !c.trySubmit(h.task) {
		h.pending = histSnap{}
		c.hotCallbackPending.Store(false)
		c.metrics.hotWindowDropped.Add(1)
	}
}

// hotNotify 是预建的窗口通知任务：在执行器上物化快照、映射 key 并调用宿主回调，
// panic 被隔离；在途标志总会复位（保证 Close 能到达 RECLAIMED）。
func (c *cache[V]) hotNotify() {
	h := c.hotHist
	snap := h.pending
	h.pending = histSnap{}
	defer func() {
		if recover() != nil {
			c.metrics.hotExportPanics.Add(1)
		}
		c.hotCallbackPending.Store(false)
	}()
	one := [1]histSnap{snap}
	c.cfg.Hot.OnWindow(c.exportHist(one[:])[0])
}

// HotKeyHistory 返回最近 n 份窗口快照（时间升序，最旧在前；n <= 0 返回全部保留的历史）。
// 未启用 Hot.Windows 或尚无快照时返回 nil。返回值与其中的榜单可安全持有
// 并修改，不影响内部历史或其他消费者。
func (c *Cache[V]) HotKeyHistory(n int) []HotSnapshot {
	if c == nil || c.c == nil {
		return nil
	}
	h := c.c.hotHist
	if h == nil || len(h.ring) == 0 {
		return nil
	}
	h.mu.Lock()
	if n <= 0 || n > h.n {
		n = h.n
	}
	if n == 0 {
		h.mu.Unlock()
		return nil
	}
	out := h.latest(n)
	h.mu.Unlock()
	return c.c.exportHist(out)
}

// HotWindowsSince 增量拉取：返回 Seq > seq 的全部保留快照（时间升序）。
// lost 是介于 seq 与最旧保留快照之间、已被环形缓冲覆盖而无法再读到的快照数。
// 宿主保存最后读到的 Seq 作为游标；推荐"Hot.OnWindow 作信号 + 本接口补齐"。
// 未启用 Hot.Windows 或没有新快照时返回 nil, 0。
func (c *Cache[V]) HotWindowsSince(seq uint64) (snaps []HotSnapshot, lost uint64) {
	if c == nil || c.c == nil {
		return nil, 0
	}
	h := c.c.hotHist
	if h == nil || len(h.ring) == 0 {
		return nil, 0
	}
	h.mu.Lock()
	if seq >= h.seq {
		h.mu.Unlock()
		return nil, 0
	}
	cnt := min(h.seq-seq, uint64(h.n))
	out := h.latest(int(cnt))
	lost = h.seq - seq - cnt
	h.mu.Unlock()
	return c.c.exportHist(out), lost
}
