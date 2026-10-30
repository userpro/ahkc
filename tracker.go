package ahkc

import (
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

// 热点追踪表核心：回源榜（hotTracker）与请求榜（reqTracker）共用。
//
// 每次记录的成本与槽位数无关：
//   - 定位：开放寻址表（uint64 = 32 位标签 | 槽号+1），装载率不超过 1/2，线性探测、后移删除；
//   - 淘汰：无指针下标最小堆，排序键为（当前窗口计数，另一窗口真实计数），根即 Space-Saving 的最小计数者；
//   - 前置过滤：每个窗口奇偶一份保守更新 Count-Min（一条缓存行装下 4 行 × 4 列），
//     未追踪的 key 只有估计值超过堆根计数才入槽并继承该计数作为误差上界，
//     长尾 key 因此不会逐出真实热点；
//   - 键池：key 字节存放在专用内存区里的 TLSF 分配器（与条目区同一实现），表内只存块引用和长度，
//     因此整张表不含 Go 指针，GC 不扫描。
//
// 不变量（窗口内）：已追踪 key 的真实计数 ∈ [rank-err, rank]；未追踪 key 的真实计数 ≤ 堆根计数。
// 证明要点：草图对每个事件都计数，被逐出者的计数在逐出时抬入草图，故估计值恒不小于真实值；
// 堆根计数在窗口内单调不减。例外：键池分配失败（碎片；预留已含 25% 余量）时该事件不入槽并计入
// dropped，此时上述上界对该 key 不再成立。

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
	released        bool // 键池已归还（缓存已 RECLAIMED）：表按空表处理，不再记录
}

// tryLock 非阻塞加锁；失败只计数。已归还的表一律拒绝，保证键池地址不再被访问。
func (t *trackerCore) tryLock() bool {
	if !t.mu.TryLock() {
		t.dropped.Add(1)
		return false
	}
	if t.released {
		t.mu.Unlock()
		return false
	}
	return true
}

// bucket 返回 now 时刻事件应计入的窗口，并保证其缓冲可用。
func (t *trackerCore) bucket(now int64) (b uint64, dirty bool) {
	b = t.win.at(uint64(now)+t.align, t.interval)
	return b, t.win.claim(b)
}

// ---- 追踪表核心 ----

// skCells 是草图一行（一条缓存行）的单元数：4 行 × 每行 4 列。
// shortcut: 草图宽度固定为槽数的 4 倍（每行），长尾 key 数远大于它时估计值偏高，
// 只会让准入更保守；需要更严的召回时加大追踪槽位。
const skCells = 16

type keyTracker struct {
	trackerCore

	hash []uint64
	ns   []uint32
	kref []blockRef // key 在键池中的块
	klen []uint16
	rank [2][]uint32 // 窗口内计数，含入槽时继承的误差
	err  [2][]uint32 // 入槽时继承的误差上界
	aux  [2][]uint32 // 可选的窗口内附加计数列（回源榜：真实加载数），随缓冲一同清零

	tab   []uint64 // 定位表：高 32 位为标签，低 32 位为槽号+1，0 为空
	tmask uint32

	heap, pos []uint32 // 最小堆（槽号）与槽号 → 堆位置
	hw        uint64   // 堆所依据的窗口号+1，0 表示需要重建
	hp        uint64   // 堆所依据窗口的缓冲奇偶

	sk    [2][]uint32 // 按奇偶的 Count-Min 草图，每条缓存行对应一个 key 的 4×4 单元
	lines uint64

	maxKey int
	kreg   *region
	kpool  *arena
}

// keyBlockBytes 是长度为 maxKey 的 key 在键池里占的块大小：边界字加补齐到 8 字节的 key，不小于 arena 的最小块。
func keyBlockBytes(maxKey int) int64 { return max(8+roundUp8(int64(maxKey)), minBlockUnits*arenaUnit) }

// keyPoolBytes 是 n 个槽位的键池预留大小：全部槽位都是最长 key 的块，再加 25% 余量覆盖碎片。
// 在 mmap 后端上它只占虚拟地址，物理页按实际 key 长度提交，也不计入 Go 的 GC 触发线；
// 堆后端（非 linux 64 位、-race、Memory.Heap）上才是真实的堆分配，MaxKeyBytes 很大时要留意。
// 缓存到达 RECLAIMED 时按存活 key 压实成一小块堆内存（见 release）。
func keyPoolBytes(n, maxKey int) int64 {
	b := keyBlockBytes(maxKey)
	return int64(n)*b*5/4 + b + 4096
}

func (t *keyTracker) init(n, maxKey int, align, interval uint64, heap bool) error {
	t.n, t.align, t.interval, t.maxKey = n, align, interval, maxKey
	var err error
	if t.kreg, err = newRegion(uintptr(keyPoolBytes(n, maxKey)), heap, false); err != nil {
		return err
	}
	t.kpool = newArena(t.kreg.base, t.kreg.size)
	t.kref, t.klen = make([]blockRef, n), make([]uint16, n)
	t.hash, t.ns = make([]uint64, n), make([]uint32, n)
	for p := range t.rank {
		t.rank[p], t.err[p] = make([]uint32, n), make([]uint32, n)
		t.sk[p] = make([]uint32, n*skCells)
	}
	size := nextPow2(2 * n)
	t.tab, t.tmask = make([]uint64, size), uint32(size-1)
	t.heap, t.pos = make([]uint32, 0, n), make([]uint32, n)
	t.lines = uint64(n)
	return nil
}

// trackBytes 是 n 个槽位的固定数组开销（不含调用方的负载列与键池）。
// 构成：hash 8 + ns 4 + 键引用与长度 6 + rank/err 16 + 双草图 2×64B + 定位表（≤4 项/槽）32 + 堆与位置 8。
func trackBytes(n int) int64 {
	return int64(n) * (8 + 4 + 4 + 2 + 4*4 + 2*skCells*4 + 4*8 + 2*4)
}

// allocKey 把 key 拷进键池；键池没有足够大的空闲块时返回 false。
func (t *keyTracker) allocKey(key string) (blockRef, bool) {
	key = key[:min(len(key), t.maxKey)]
	ref, ok := t.kpool.alloc(8 + len(key))
	if ok {
		copy(unsafe.Slice((*byte)(unsafe.Add(t.kpool.at(ref), 8)), len(key)), key)
	}
	return ref, ok
}

// keyBytes 返回槽 s 的 key 在键池里的视图：只在持表锁期间有效，调用方要保留必须拷贝。
func (t *keyTracker) keyBytes(s int) []byte {
	return unsafe.Slice((*byte)(unsafe.Add(t.kpool.at(t.kref[s]), 8)), int(t.klen[s]))
}

func (t *keyTracker) keyString(s int) string {
	return unsafe.String((*byte)(unsafe.Add(t.kpool.at(t.kref[s]), 8)), int(t.klen[s]))
}

// release 归还键池内存区，并把存活的 key 压实进一块刚好够用的堆内存：
// 关闭之后榜单与历史仍可读（快照语义），表则不再记录。
// 与记录、采集、导出共用表锁，所以与它们并发是安全的；已发出的榜单和历史快照持有的是拷贝，不受影响。
func (t *keyTracker) release() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return
	}
	t.released = true
	words := 0
	for s := 0; s < t.filled; s++ {
		words += 1 + int(roundUp8(int64(t.klen[s])))/8 // 与池内块同形：key 前留一个边界字
	}
	buf := make([]uint64, words+1)
	base := unsafe.Pointer(&buf[0]) // 经 kpool.base 保持存活
	off := 0
	for s := 0; s < t.filled; s++ {
		k := t.keyBytes(s)
		copy(unsafe.Slice((*byte)(unsafe.Add(base, off*8+8)), len(k)), k)
		t.kref[s] = blockRef(off)
		off += 1 + int(roundUp8(int64(len(k))))/8
	}
	t.kreg.release()
	t.kreg, t.kpool = nil, &arena{base: base} // 只剩 at() 可用；released 阻止任何后续分配
}

// trackHash 把身份 (命名空间, 64 位 key 哈希) 混合成定位用的 64 位值：
// 低位定位表位置，高位作标签与草图行号，中间位选草图列。
func trackHash(kh uint64, ns uint32) uint64 { return mix64(kh ^ uint64(ns)*rowSeedStep) }

// initAux 为每个窗口缓冲增加一列附加计数（须在使用前调用）。
func (t *keyTracker) initAux() {
	for p := range t.aux {
		t.aux[p] = make([]uint32, t.n)
	}
}

func (t *keyTracker) clearParity(p uint64) {
	clear(t.rank[p][:t.filled])
	clear(t.err[p][:t.filled])
	clear(t.sk[p])
	if t.aux[p] != nil {
		clear(t.aux[p][:t.filled])
	}
}

// finish 在采集结束窗口 w 之后清零所属缓冲；清零改变了堆的排序键，标记堆待重建。
func (t *keyTracker) finish(w uint64, st winStatus) {
	if st == winData || st == winStale {
		t.clearParity(w & 1)
		t.hw = 0
	}
}

func (t *keyTracker) resetAux(s int) {
	if t.aux[0] != nil {
		t.aux[0][s], t.aux[1][s] = 0, 0
	}
}

// ---- 定位表 ----

func (t *keyTracker) find(ns uint32, kh, m uint64) int {
	tag := m >> 32
	for i := uint32(m) & t.tmask; ; i = (i + 1) & t.tmask {
		v := t.tab[i]
		if v == 0 {
			return -1
		}
		if v>>32 == tag {
			if s := int(uint32(v)) - 1; t.hash[s] == kh && t.ns[s] == ns {
				return s
			}
		}
	}
}

func (t *keyTracker) tabInsert(m uint64, s int) {
	i := uint32(m) & t.tmask
	for t.tab[i] != 0 {
		i = (i + 1) & t.tmask
	}
	t.tab[i] = m>>32<<32 | uint64(s+1)
}

// tabDelete 删除槽 s 的表项并后移后继，保持探测链完整（无墓碑）。
func (t *keyTracker) tabDelete(m uint64, s int) {
	i := uint32(m) & t.tmask
	for uint32(t.tab[i])-1 != uint32(s) {
		i = (i + 1) & t.tmask
	}
	for {
		j := i
		for {
			j = (j + 1) & t.tmask
			v := t.tab[j]
			if v == 0 {
				t.tab[i] = 0
				return
			}
			sj := int(uint32(v)) - 1
			k := uint32(trackHash(t.hash[sj], t.ns[sj])) & t.tmask
			// 起点 k 落在 (i, j] 内的表项不能前移到 i。
			if (i <= j && i < k && k <= j) || (i > j && (i < k || k <= j)) {
				continue
			}
			t.tab[i] = v
			i = j
			break
		}
	}
}

// ---- 最小堆 ----

func (t *keyTracker) less(a, b uint32) bool {
	p, o := t.hp, 1-t.hp
	if ra, rb := t.rank[p][a], t.rank[p][b]; ra != rb {
		return ra < rb
	}
	return t.rank[o][a]-t.err[o][a] < t.rank[o][b]-t.err[o][b]
}

func (t *keyTracker) siftDown(i int) {
	h := t.heap
	x := h[i]
	for {
		c := 2*i + 1
		if c >= len(h) {
			break
		}
		if r := c + 1; r < len(h) && t.less(h[r], h[c]) {
			c = r
		}
		if !t.less(h[c], x) {
			break
		}
		h[i] = h[c]
		t.pos[h[i]] = uint32(i)
		i = c
	}
	h[i] = x
	t.pos[x] = uint32(i)
}

func (t *keyTracker) siftUp(i int) {
	h := t.heap
	x := h[i]
	for i > 0 {
		p := (i - 1) / 2
		if !t.less(x, h[p]) {
			break
		}
		h[i] = h[p]
		t.pos[h[i]] = uint32(i)
		i = p
	}
	h[i] = x
	t.pos[x] = uint32(i)
}

// rebuild 以奇偶 p 为排序依据重建堆（窗口切换或采集清零后，O(n)）。
func (t *keyTracker) rebuild(p uint64) {
	t.hp = p
	t.heap = t.heap[:0]
	for s := 0; s < t.filled; s++ {
		t.heap = append(t.heap, uint32(s))
		t.pos[s] = uint32(s)
	}
	for i := len(t.heap)/2 - 1; i >= 0; i-- {
		t.siftDown(i)
	}
}

// ---- 草图 ----

func (t *keyTracker) cells(p, m uint64) *[skCells]uint32 {
	line, _ := bits.Mul64(m, t.lines)
	return (*[skCells]uint32)(t.sk[p][line*skCells:])
}

func cellIdx(m uint64, row int) int { return row*4 + int(m>>(16+2*uint(row)))&3 }

// sketchInc 保守更新并返回 key 的估计值（≥ 真实计数）。
func (t *keyTracker) sketchInc(p, m uint64) uint32 {
	c := t.cells(p, m)
	i0, i1, i2, i3 := cellIdx(m, 0), cellIdx(m, 1), cellIdx(m, 2), cellIdx(m, 3)
	lo := min(c[i0], c[i1], c[i2], c[i3])
	if lo == math.MaxUint32 {
		return lo
	}
	lo++
	for _, i := range [4]int{i0, i1, i2, i3} {
		if c[i] < lo {
			c[i] = lo
		}
	}
	return lo
}

// sketchRaise 把被逐出者的计数抬入草图，使其再次出现时估计值仍不小于真实值。
func (t *keyTracker) sketchRaise(p, m uint64, r uint32) {
	c := t.cells(p, m)
	for row := 0; row < 4; row++ {
		if i := cellIdx(m, row); c[i] < r {
			c[i] = r
		}
	}
}

// ---- 记录 ----

// touch 在表锁内记录 (ns, kh) 在 now 时刻的一次事件：rank 加一，必要时入槽（key 拷入键池）。
// 返回事件所属窗口的缓冲奇偶 p（无论是否入槽，调用方都应累计 win.tot[p]）；
// s < 0 表示该 key 未被追踪（被草图挡在外，或是迟到事件）；
// fresh 为 true 表示槽位刚被占用，调用方须重置自己的负载列。
func (t *keyTracker) touch(ns uint32, kh uint64, key string, now int64) (s int, p uint64, fresh bool) {
	b, dirty := t.bucket(now)
	p = b & 1
	if dirty {
		t.clearParity(p)
	}
	if b+1 > t.hw {
		t.hw = b + 1
		t.rebuild(p)
	}
	m := trackHash(kh, ns)
	if s = t.find(ns, kh, m); s >= 0 {
		satInc(&t.rank[p][s])
		t.siftDown(int(t.pos[s]))
		return s, p, false
	}
	if b+1 < t.hw { // 迟到事件只补记已追踪的 key，不入槽
		return -1, p, false
	}
	o := 1 - p
	if t.filled < t.n {
		ref, ok := t.allocKey(key)
		if !ok {
			t.dropped.Add(1)
			return -1, p, false
		}
		s = t.filled
		t.filled++
		t.hash[s], t.ns[s] = kh, ns
		t.kref[s], t.klen[s] = ref, uint16(min(len(key), t.maxKey))
		t.tabInsert(m, s)
		t.rank[p][s], t.err[p][s], t.rank[o][s], t.err[o][s] = 1, 0, 0, 0
		t.resetAux(s)
		t.heap = append(t.heap, uint32(s))
		t.pos[s] = uint32(len(t.heap) - 1)
		t.siftUp(len(t.heap) - 1)
		return s, p, true
	}
	est := t.sketchInc(p, m)
	v := int(t.heap[0])
	minR := t.rank[p][v]
	if est <= minR {
		return -1, p, false
	}
	ref, ok := t.allocKey(key) // 先取新块再动受害者：失败时受害者原样保留
	if !ok {
		t.dropped.Add(1)
		return -1, p, false
	}
	if t.win.pb[o] != 0 { // 另一缓冲是尚未采集的相邻窗口：受害者的真实事件随之丢失
		t.win.lost += uint64(t.rank[o][v] - t.err[o][v])
	}
	mv := trackHash(t.hash[v], t.ns[v])
	t.sketchRaise(p, mv, minR)
	t.tabDelete(mv, v)
	t.kpool.free(t.kref[v])
	t.hash[v], t.ns[v] = kh, ns
	t.kref[v], t.klen[v] = ref, uint16(min(len(key), t.maxKey))
	t.tabInsert(m, v)
	t.rank[p][v], t.err[p][v] = minR+1, minR // 继承最小计数作为误差上界
	t.rank[o][v], t.err[o][v] = 0, 0
	t.resetAux(v)
	t.siftDown(0)
	return v, p, true
}
