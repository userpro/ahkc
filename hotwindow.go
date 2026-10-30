package ahkc

import (
	"cmp"
	"slices"
	"strings"
	"time"
	"unsafe"
)

// 热点窗口历史的内部机件：请求热点追踪表、采集用有界堆、导出映射（追踪表核心见 tracker.go）。
// 请求路径只有 TryLock + 一次定位 + 整数累加，无堆分配；竞争失败时增加原子丢弃计数；
// 滚动采集在表锁内只做比较与堆插入，排序与输出构造在锁外。

// hotCollectBudget 每个维护周期的槽位目标预算（按分片整体采集，单个大分片可超出）。
const hotCollectBudget = 16384

// ---- 请求热点追踪表 ----

// reqTracker 是每分片的请求热点追踪表（含命中，采样）。Space-Saving + 草图前置过滤，
// 真实采样数 ∈ [rank-err, rank]。身份为 (nsID, 64 位种子哈希)，与门卫同口径；
// key 字节只在入槽时拷进键池，供导出使用。全部数组不含指针（GC 不扫描）。
// shortcut: 身份不比较完整 key，64 位哈希碰撞（概率约 槽数×key 数/2^64）会合并两个 key 的计数。
type reqTracker struct {
	keyTracker
}

func newReqTracker(n, maxKey int, align, interval uint64, heap bool) (*reqTracker, error) {
	t := &reqTracker{}
	if err := t.init(n, maxKey, align, interval, heap); err != nil {
		return nil, err
	}
	return t, nil
}

// record 记录一次采样请求。0 分配，TryLock 失败只计数。
func (t *reqTracker) record(nsID uint32, kh uint64, key string, now int64) {
	if !t.tryLock() {
		return
	}
	_, p, _ := t.touch(nsID, kh, key, now)
	t.win.tot[p][0]++
	t.mu.Unlock()
}

// collect 结束窗口 w 的采集：把候选压入有界堆并清零该窗口缓冲（短锁，不分配）。
func (t *reqTracker) collect(w uint64, h *winHeap) (samples, lost uint64, partial bool) {
	t.mu.Lock()
	tot, st, lost := t.win.take(w)
	if st == winData {
		rank, errs := t.rank[w&1], t.err[w&1]
		for i := 0; i < t.filled; i++ {
			if rk, er := rank[i], errs[i]; rk != er && h.wants(uint64(rk), t.hash[i]) {
				h.add(winEntry{score: uint64(rk), hash: t.hash[i], n: rk - er, err: er, ns: t.ns[i]}, t.keyBytes(i))
			}
		}
	}
	t.finish(w, st)
	t.mu.Unlock()
	return tot[0], lost, st == winPartial
}

// ---- 采集用有界堆 ----

// winEntry 是采集候选，不含指针；key 字节在堆的字节槽里。回源表用 n/m/err 与 loads/merged/heat；
// 请求表用 n/err。
type winEntry struct {
	score         uint64 // 观测 + 误差（排序依据）
	hash          uint64
	loads, merged uint64
	heat          float64
	n, m, err     uint32
	ns            uint32
	buf           int32 // 堆内 key 字节槽序号
	klen          uint32
}

// winBetter 是候选的全序：score 降序，其次 hash 升序。
func winBetter(a, b *winEntry) bool {
	return a.score > b.score || (a.score == b.score && a.hash < b.hash)
}

// winHeap 是固定容量的小顶堆（根为当前最差候选）。每个候选独占一个 maxKey 字节的
// key 字节槽，使入堆不产生分配；容量与字节槽在初始化时一次分配。
type winHeap struct {
	es     []winEntry
	keys   []byte
	maxKey int
}

func newWinHeap(n, maxKey int) winHeap {
	return winHeap{es: make([]winEntry, 0, n), keys: make([]byte, n*maxKey), maxKey: maxKey}
}

func (h *winHeap) reset() { h.es = h.es[:0] }

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
	e.klen = uint32(copy(h.keys[int(e.buf)*h.maxKey:int(e.buf+1)*h.maxKey], key))
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

// ---- 列式历史 ----

// histRec 是历史榜单的一条记录，不含指针：key 字节在所属窗口的 keys 块里，
// 命名空间只存编号（命名空间列表只追加，编号稳定）。长期保存的历史因此不增加 GC 扫描量。
// 回源榜：n/m = 窗口内加载/合并数；请求榜：n = 窗口内采样数（导出时折算 1/q）。
type histRec struct {
	heat          float64
	loads, merged uint64
	n, m, err     uint32
	ns            uint32
	off, klen     uint32
}

// histSnap 是一个窗口的列式快照；发布后不再修改，读者与回调可共享其底层数组。
type histSnap struct {
	seq, bucket, skipped uint64
	partial              bool
	totals               HotWindowTotals
	top, req             []histRec // 回源榜、请求榜，均已按榜单顺序排好
	keys                 []byte    // 两张榜单全部 key 的拼接
}

// keyOf 返回候选的 key 视图（堆的字节槽）；仅用于排序比较与拷贝。
func (h *winHeap) keyOf(e *winEntry) string {
	if e.klen == 0 {
		return ""
	}
	return unsafe.String(&h.keys[int(e.buf)*h.maxKey], int(e.klen))
}

// packWin 把采集堆排序后写成列式记录，key 追加到 keys。排序全序与 HotKeys 一致：
// "观测 + 误差"降序，其次加载热度、合并数、加载数降序，最后以命名空间名与 key 字典序稳定化。
// 堆在发布后立即复位，故可原地排序。
func packWin[V any](h *winHeap, list []*namespace[V], keys []byte) ([]histRec, []byte) {
	if len(h.es) == 0 {
		return nil, keys
	}
	slices.SortFunc(h.es, func(a, b winEntry) int {
		if d := cmp.Compare(b.score, a.score); d != 0 {
			return d
		}
		if d := cmp.Compare(b.heat, a.heat); d != 0 {
			return d
		}
		if d := cmp.Compare(b.merged, a.merged); d != 0 {
			return d
		}
		if d := cmp.Compare(b.loads, a.loads); d != 0 {
			return d
		}
		if a.ns != b.ns {
			if d := cmp.Compare(nsNameOf(list, a.ns), nsNameOf(list, b.ns)); d != 0 {
				return d
			}
		}
		return cmp.Compare(h.keyOf(&a), h.keyOf(&b))
	})
	recs := make([]histRec, len(h.es))
	for i := range h.es {
		e := &h.es[i]
		k := h.keyOf(e)
		recs[i] = histRec{
			heat: e.heat, loads: e.loads, merged: e.merged, n: e.n, m: e.m, err: e.err,
			ns: e.ns, off: uint32(len(keys)), klen: uint32(len(k)),
		}
		keys = append(keys, k...)
	}
	return recs, keys
}

func nsNameOf[V any](list []*namespace[V], id uint32) string {
	if int(id) < len(list) {
		return list[id].name
	}
	return ""
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
	return c.cfg.Hot.Label(ns, key)
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

// exportHist 把列式快照物化为对外的 HotSnapshot：一次切片分配加一个 key 拼接串，
// 返回值归调用方所有，与内部历史完全隔离。
func (c *cache[V]) exportHist(snaps []histSnap) []HotSnapshot {
	list := c.nsList()
	total, keyBytes := 0, 0
	for i := range snaps {
		total += len(snaps[i].top) + len(snaps[i].req)
		keyBytes += len(snaps[i].keys)
	}
	var sb strings.Builder
	sb.Grow(keyBytes)
	for i := range snaps {
		sb.Write(snaps[i].keys)
	}
	all := sb.String()
	flat := make([]HotKeyStat, 0, total)
	emit := func(st HotKeyStat) {
		if c.cfg.Hot.Label == nil {
			flat = append(flat, st)
		} else if label, keep := c.labelKey(st.Namespace, st.Key); keep {
			st.Key = label
			flat = append(flat, st)
		}
	}
	out := make([]HotSnapshot, len(snaps))
	base, interval := 0, c.hotHist.interval
	for i := range snaps {
		s := &snaps[i]
		a := len(flat)
		for j := range s.top {
			r := &s.top[j]
			emit(HotKeyStat{
				Namespace: nsNameOf(list, r.ns), Key: all[base+int(r.off) : base+int(r.off+r.klen)],
				LoadHeat: r.heat, Loads: r.loads, Merged: r.merged,
				WindowLoads: uint64(r.n), WindowMerged: uint64(r.m), WindowError: float64(r.err),
			})
		}
		b := len(flat)
		for j := range s.req {
			r := &s.req[j]
			emit(HotKeyStat{
				Namespace: nsNameOf(list, r.ns), Key: all[base+int(r.off) : base+int(r.off+r.klen)],
				WindowRequests: float64(r.n) * c.reqInvQ, WindowError: float64(r.err) * c.reqInvQ,
			})
		}
		out[i] = HotSnapshot{
			Seq: s.seq, Bucket: s.bucket, Interval: time.Duration(interval),
			Start: hotWindowTime(s.bucket, interval), End: hotWindowTime(s.bucket+1, interval),
			Skipped: s.skipped, Partial: s.partial, Totals: s.totals,
			Top: orNil(flat[a:b:b]), Requests: orNil(flat[b:len(flat):len(flat)]),
		}
		base += len(s.keys)
	}
	return out
}

func orNil(s []HotKeyStat) []HotKeyStat {
	if len(s) == 0 {
		return nil
	}
	return s
}
