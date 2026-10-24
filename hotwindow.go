package ahkc

import (
	"cmp"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 热点窗口历史的内部机件：窗口状态、请求热点追踪表、采集用有界堆、导出映射。
// 请求路径只有 TryLock + 连续哈希数组扫描 + 整数累加，无堆分配；竞争失败时增加原子丢弃计数；
// 滚动采集在表锁内只做比较与堆插入，排序与输出构造在锁外。

// hotCollectBudget 每个维护周期的槽位目标预算（按分片整体采集，单个大分片可超出）。
const hotCollectBudget = 4096

// ---- 窗口状态 ----

// winState 是追踪表的窗口状态（在表锁内维护）。窗口按奇偶分到两块缓冲，
// 事件按自己的时间戳归入所属窗口；采集只读已结束窗口的缓冲，采后清零。
// 时间一律是对齐时间 ts = uint64(时钟读数) + align。
type winState struct {
	curB, curStart, curEnd uint64       // 见过的最新窗口及其区间缓存
	pb                     [2]uint64    // 缓冲承载的窗口号+1，0 为空闲
	done                   uint64       // 窗口号 < done 的窗口都已采集（或已放弃）
	tot                    [2][2]uint64 // 缓冲的窗口事件总量（回源：加载/合并；请求：[0] 为采样数）
	lost                   uint64       // 未采集即被覆盖的事件数
}

// at 返回时间 ts 的事件应计入的窗口。迟到事件只在所属窗口缓冲完好且尚未采集时
// 写回原窗口，否则计入最新的未采集窗口——绝不回写已采集的窗口。
func (w *winState) at(ts, interval uint64) uint64 {
	if ts >= w.curEnd {
		b := ts / interval
		w.curB, w.curStart, w.curEnd = b, b*interval, (b+1)*interval
	} else if ts < w.curStart {
		if b := ts / interval; b >= w.done && w.pb[b&1] == b+1 {
			return b
		}
	}
	return max(w.curB, w.done)
}

// claim 使缓冲 b&1 承载窗口 b。返回 true 表示其中有未采集的外来数据（维护滞后，
// 已计入 lost），调用方须清零该缓冲的计数数组。
func (w *winState) claim(b uint64) (dirty bool) {
	p := b & 1
	if w.pb[p] == b+1 {
		return false
	}
	if w.pb[p] != 0 {
		w.lost += w.tot[p][0] + w.tot[p][1]
		dirty = true
	}
	w.pb[p], w.tot[p] = b+1, [2]uint64{}
	return dirty
}

type winStatus uint8

const (
	winEmpty   winStatus = iota // 窗口内无事件
	winData                     // 缓冲承载该窗口：扫描后清零
	winStale                    // 缓冲滞留更旧窗口的数据（窗口内无事件）：清零即可
	winPartial                  // 缓冲已被更新窗口覆盖，窗口数据已丢失
)

// take 结束窗口 wnd 的采集：释放缓冲标签，返回窗口事件总量与状态，并取走累计的
// 丢失事件数。缓冲中的计数数组由调用方按状态扫描并清零。
func (w *winState) take(wnd uint64) (tot [2]uint64, st winStatus, lost uint64) {
	p := wnd & 1
	switch pb := w.pb[p]; {
	case pb == wnd+1:
		tot, st = w.tot[p], winData
	case pb == 0:
	case pb < wnd+1:
		w.lost += w.tot[p][0] + w.tot[p][1]
		st = winStale
	default:
		st = winPartial
	}
	if st == winData || st == winStale {
		w.pb[p], w.tot[p] = 0, [2]uint64{}
	}
	w.done = max(w.done, wnd+1)
	lost, w.lost = w.lost, 0
	return tot, st, lost
}

func satInc(p *uint32) {
	if *p != math.MaxUint32 {
		*p++
	}
}

// trackerCore 是两张追踪表共有的状态。锁与丢弃计数各占一条缓存行：
// 竞争失败只写 dropped，不会让持锁者的缓存行失效。
type trackerCore struct {
	_       [cacheLineSize]byte
	mu      sync.Mutex
	_       [cacheLineSize - 8]byte
	dropped atomic.Uint64 // TryLock 失败而丢弃的事件数
	_       [cacheLineSize - 8]byte

	win             winState
	n, filled       int
	align, interval uint64
}

// tryLock 非阻塞加锁；失败只计数。
func (t *trackerCore) tryLock() bool {
	if t.mu.TryLock() {
		return true
	}
	t.dropped.Add(1)
	return false
}

// bucket 返回 now 时刻事件应计入的窗口，并保证其缓冲可用。
func (t *trackerCore) bucket(now int64) (b uint64, dirty bool) {
	b = t.win.at(uint64(now)+t.align, t.interval)
	return b, t.win.claim(b)
}

// ---- 请求热点追踪表 ----

// reqSlotBytes 是请求表每个槽位的固定数组开销（另加 MaxKeyBytes 的 key 字节区）。
const reqSlotBytes = 8 + 4 + 4 + 4*4

// reqTracker 是每分片的请求热点追踪表（含命中，采样）。固定槽位，Space-Saving：
// 满时逐出当前窗口计数最小者，新 key 继承其计数作为误差上界，
// 真实采样数 ∈ [rank-err, rank]。身份为 (nsID, 64 位种子哈希)，与门卫同口径；
// key 字节只在入槽时拷贝，供导出使用。全部数组不含指针（GC 不扫描）。
type reqTracker struct {
	trackerCore
	mru    int // 上次命中的槽位：单热 key 近似 O(1)
	maxKey int

	hash []uint64
	ns   []uint32
	klen []uint32
	rank [2][]uint32 // 含继承的误差
	err  [2][]uint32
	keys []byte // n × maxKey
}

func newReqTracker(n, maxKey int, align, interval uint64) *reqTracker {
	t := &reqTracker{maxKey: maxKey}
	t.n, t.align, t.interval = n, align, interval
	t.hash, t.ns, t.klen = make([]uint64, n), make([]uint32, n), make([]uint32, n)
	for p := range t.rank {
		t.rank[p], t.err[p] = make([]uint32, n), make([]uint32, n)
	}
	t.keys = make([]byte, n*maxKey)
	return t
}

func (t *reqTracker) clearParity(p uint64) {
	clear(t.rank[p][:t.filled])
	clear(t.err[p][:t.filled])
}

// record 记录一次采样请求。0 分配，TryLock 失败只计数。
func (t *reqTracker) record(nsID uint32, kh uint64, key string, now int64) {
	if !t.tryLock() {
		return
	}
	b, dirty := t.bucket(now)
	p := b & 1
	if dirty {
		t.clearParity(p)
	}
	i := t.find(nsID, kh)
	if i < 0 {
		i = t.claimSlot(nsID, kh, key, p)
	}
	satInc(&t.rank[p][i])
	t.win.tot[p][0]++
	t.mu.Unlock()
}

func (t *reqTracker) find(nsID uint32, kh uint64) int {
	if m := t.mru; m < t.filled && t.hash[m] == kh && t.ns[m] == nsID {
		return m
	}
	for i := 0; i < t.filled; i++ {
		if t.hash[i] == kh && t.ns[i] == nsID {
			t.mru = i
			return i
		}
	}
	return -1
}

// claimSlot 入驻空槽，或逐出当前窗口计数最小者（平局取另一缓冲真实计数更少者）。
func (t *reqTracker) claimSlot(nsID uint32, kh uint64, key string, p uint64) int {
	var i int
	if t.filled < t.n {
		i = t.filled
		t.filled++
	} else {
		o := 1 - p
		rk, rkO, erO := t.rank[p], t.rank[o], t.err[o]
		best, bestO := rk[0], rkO[0]-erO[0]
		for j := 1; j < t.filled; j++ {
			if r := rk[j]; r <= best {
				if ov := rkO[j] - erO[j]; r < best || ov < bestO {
					i, best, bestO = j, r, ov
				}
			}
		}
		if t.win.pb[o] != 0 { // 另一缓冲是尚未采集的相邻窗口：其中受害者的真实采样随之丢失
			t.win.lost += uint64(bestO)
		}
		rkO[i], erO[i] = 0, 0
		t.rank[p][i], t.err[p][i] = best, best // 继承最小计数作为误差上界
	}
	t.hash[i], t.ns[i], t.klen[i] = kh, nsID, uint32(len(key))
	copy(t.keys[i*t.maxKey:], key)
	t.mru = i
	return i
}

// collect 结束窗口 w 的采集：把候选压入有界堆并清零该窗口缓冲（短锁，不分配）。
func (t *reqTracker) collect(w uint64, h *winHeap) (samples, lost uint64, partial bool) {
	t.mu.Lock()
	tot, st, lost := t.win.take(w)
	p := w & 1
	if st == winData {
		rank, errs := t.rank[p], t.err[p]
		for i := 0; i < t.filled; i++ {
			if rk, er := rank[i], errs[i]; rk != er && h.wants(uint64(rk), t.hash[i]) {
				h.add(winEntry{score: uint64(rk), hash: t.hash[i], n: rk - er, err: er, ns: t.ns[i]},
					t.keys[i*t.maxKey:i*t.maxKey+int(t.klen[i])])
			}
		}
	}
	if st == winData || st == winStale {
		t.clearParity(p)
	}
	t.mu.Unlock()
	return tot[0], lost, st == winPartial
}

// ---- 采集用有界堆 ----

// winEntry 是采集候选。回源表用 n/m/err 与 loads/merged/heat/key；请求表用 n/err 与堆内 key 字节槽。
type winEntry struct {
	score         uint64 // 观测 + 误差（排序依据）
	hash          uint64
	loads, merged uint64
	heat          float64
	key           string
	n, m, err     uint32
	ns            uint32
	buf           int32 // 堆内 key 字节槽序号
	klen          uint32
}

// winBetter 是候选的全序：score 降序，其次 hash 升序。
func winBetter(a, b *winEntry) bool {
	return a.score > b.score || (a.score == b.score && a.hash < b.hash)
}

// winHeap 是固定容量的小顶堆（根为当前最差候选）。maxKey > 0 时每个候选独占一个
// key 字节槽，使入堆不产生分配；容量与字节槽在初始化时一次分配。
type winHeap struct {
	es     []winEntry
	keys   []byte
	maxKey int
}

func newWinHeap(n, maxKey int) winHeap {
	return winHeap{es: make([]winEntry, 0, n), keys: make([]byte, n*maxKey), maxKey: maxKey}
}

func (h *winHeap) reset() {
	clear(h.es) // 释放回源 key 引用
	h.es = h.es[:0]
}

// wants 判定候选能否入榜（先于构造候选，避免无谓拷贝）。
func (h *winHeap) wants(score, hash uint64) bool {
	if len(h.es) < cap(h.es) {
		return true
	}
	r := &h.es[0]
	return score > r.score || (score == r.score && hash < r.hash)
}

// add 插入候选（调用方已用 wants 预检）；key 拷入该候选独占的字节槽。
func (h *winHeap) add(e winEntry, key []byte) {
	full := len(h.es) == cap(h.es)
	if full {
		e.buf = h.es[0].buf // 复用即将被替换的根的字节槽
	} else {
		e.buf = int32(len(h.es))
	}
	if h.maxKey > 0 {
		e.klen = uint32(copy(h.keys[int(e.buf)*h.maxKey:int(e.buf+1)*h.maxKey], key))
	}
	if full {
		h.es[0] = e
		heapDown(h.es, 0, winBetter)
		return
	}
	h.es = append(h.es, e)
	heapUp(h.es, len(h.es)-1, winBetter)
}

// heapUp / heapDown 维护"父不优于子"的堆（根为最差）。
func heapUp[T any](s []T, i int, better func(a, b *T) bool) {
	for i > 0 {
		p := (i - 1) / 2
		if !better(&s[p], &s[i]) {
			return
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func heapDown[T any](s []T, i int, better func(a, b *T) bool) {
	for {
		w := 2*i + 1
		if w >= len(s) {
			return
		}
		if r := w + 1; r < len(s) && better(&s[w], &s[r]) {
			w = r
		}
		if !better(&s[i], &s[w]) {
			return
		}
		s[i], s[w] = s[w], s[i]
		i = w
	}
}

// ---- 输出榜单 ----

// windowScore 是窗口榜单的排序分：观测 + 误差（Space-Saving 计数）。
func windowScore(a *HotKeyStat) float64 {
	return float64(a.WindowLoads+a.WindowMerged) + a.WindowRequests + a.WindowError
}

// cmpWindowStat 窗口榜单全序：窗口分降序，其次同 HotKeys 的全序。
func cmpWindowStat(a, b HotKeyStat) int {
	if d := cmp.Compare(windowScore(&b), windowScore(&a)); d != 0 {
		return d
	}
	return cmpHotKeyStat(a, b)
}

func nsNameOf[V any](list []*namespace[V], id uint32) string {
	if int(id) < len(list) {
		return list[id].name
	}
	return ""
}

// buildLoadList 由回源堆构造榜单：一个切片，key 引用追踪表持有的不可变副本。
func buildLoadList[V any](h *winHeap, list []*namespace[V]) []HotKeyStat {
	if len(h.es) == 0 {
		return nil
	}
	out := make([]HotKeyStat, len(h.es))
	for i := range h.es {
		e := &h.es[i]
		out[i] = HotKeyStat{
			Namespace: nsNameOf(list, e.ns), Key: e.key,
			LoadHeat: e.heat, Loads: e.loads, Merged: e.merged,
			WindowLoads: uint64(e.n), WindowMerged: uint64(e.m), WindowError: float64(e.err),
		}
	}
	slices.SortFunc(out, cmpWindowStat)
	return out
}

// buildRequestList 由请求堆构造榜单：一个切片 + 一个 key 拼接串（2 次分配）。
func buildRequestList[V any](h *winHeap, list []*namespace[V], invQ float64) []HotKeyStat {
	if len(h.es) == 0 {
		return nil
	}
	total := 0
	for i := range h.es {
		total += int(h.es[i].klen)
	}
	var sb strings.Builder
	sb.Grow(total)
	for i := range h.es {
		e := &h.es[i]
		sb.Write(h.keys[int(e.buf)*h.maxKey : int(e.buf)*h.maxKey+int(e.klen)])
	}
	all := sb.String()
	out := make([]HotKeyStat, len(h.es))
	off := 0
	for i := range h.es {
		e := &h.es[i]
		out[i] = HotKeyStat{
			Namespace: nsNameOf(list, e.ns), Key: all[off : off+int(e.klen)],
			WindowRequests: float64(e.n) * invQ, WindowError: float64(e.err) * invQ,
		}
		off += int(e.klen)
	}
	slices.SortFunc(out, cmpWindowStat)
	return out
}

// hotWindowTime 把窗口号换算为 UTC 墙钟时间：对齐时间本身就是 Unix 纳秒
// （内置时钟 align = 进程时钟原点的 Unix 时间；自定义时钟 align = 0）。
func hotWindowTime(w, interval uint64) time.Time {
	return time.Unix(0, int64(w*interval)).UTC()
}

// ---- 导出映射 ----

// labelKey 在 recover 保护下调用宿主映射钩子；panic 视为丢弃该条并计数。
func (c *cache[V]) labelKey(ns, key string) (label string, keep bool) {
	defer func() {
		if recover() != nil {
			c.metrics.hotExportPanics.Add(1)
			label, keep = "", false
		}
	}()
	return c.cfg.HotKeyLabel(ns, key)
}

// mapStats 把 src 经映射钩子逐条追加到 dst（keep=false 的条目剔除）。
// dst 与 src 可以是同一底层数组的 [:0] 与全量（原地压缩）。
func (c *cache[V]) mapStats(dst, src []HotKeyStat) []HotKeyStat {
	for i := range src {
		st := src[i]
		label, keep := c.labelKey(st.Namespace, st.Key)
		if keep {
			st.Key = label
			dst = append(dst, st)
		}
	}
	return dst
}

// exportSnapshots 用一次分配隔离所有榜单，防止读者或回调修改内部历史。
func (c *cache[V]) exportSnapshots(snaps []HotSnapshot) []HotSnapshot {
	total := 0
	for i := range snaps {
		total += len(snaps[i].Top) + len(snaps[i].Requests)
	}
	flat := make([]HotKeyStat, 0, total)
	copyStats := func(src []HotKeyStat) {
		if c.cfg.HotKeyLabel == nil {
			flat = append(flat, src...)
		} else {
			flat = c.mapStats(flat, src)
		}
	}
	for i := range snaps {
		a := len(flat)
		copyStats(snaps[i].Top)
		b := len(flat)
		copyStats(snaps[i].Requests)
		snaps[i].Top, snaps[i].Requests = orNil(flat[a:b:b]), orNil(flat[b:len(flat):len(flat)])
	}
	return snaps
}

func orNil(s []HotKeyStat) []HotKeyStat {
	if len(s) == 0 {
		return nil
	}
	return s
}
