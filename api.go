package ahkc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
)

// 对外操作语义。
//
// 只读查询：HIT + 值 / MISS / BUSY / CLOSED；不发起加载；BUSY 不是确定不存在。
// 查询并加载：singleflight 合并；响应成功不代表已缓存。
// 失效：已受理后的完成确认表示逻辑失效已完成，不等待物理回收。

// Request 是一次逻辑请求的上下文（RequestContext）。
// 同一业务请求的“查询后再加载”必须传递同一个 Request，防止重复计数（I11）。
// Request 不可复制或并发使用；一次请求结束后可用 BeginRequestOn 重新初始化。
type Request[V any] struct {
	_            noCopy
	ns           *namespace[V]
	key          string
	keyHash      uint64
	sh           *shard[V]
	heatRecorded bool
	counted      bool
	shadowHit    bool // 影子缓存：本请求已虚拟命中（见 lookup）
}

// go vet 检查含生命周期的公开对象被按值复制；标记不占运行期存储。
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// BeginRequest 开始一次逻辑请求：确定完整 key、分片，并按命名空间概率独立采样一次。
func (n *Namespace[V]) BeginRequest(key string) (*Request[V], error) {
	r := &Request[V]{}
	if err := n.beginRequest(r, key); err != nil {
		return nil, err
	}
	return r, nil
}

// BeginRequestOn initializes caller-provided storage for one logical request.
func (n *Namespace[V]) BeginRequestOn(r *Request[V], key string) error {
	if r == nil {
		return ErrInvalidRequest
	}
	if err := n.beginRequest(r, key); err != nil {
		*r = Request[V]{}
		return err
	}
	return nil
}

// beginRequest 就地初始化 Request（允许调用方以栈分配复用，命中路径 0 堆分配）。
func (n *Namespace[V]) beginRequest(r *Request[V], key string) error {
	if n == nil || n.ns == nil {
		return ErrUninitialized
	}
	c := n.ns.c
	if c.state.Load() != cacheOpen {
		return ErrClosed
	}
	if len(key) > c.cfg.MaxKeyBytes {
		return ErrKeyTooLarge
	}
	kh := n.ns.hashKey(key)
	s := c.shardOf(kh)
	// 逐字段写入：整体复合字面量赋值在泛型 shape 下会经 typedmemmove 带写屏障检查。
	r.ns, r.key, r.keyHash, r.sh = n.ns, key, kh, s
	r.heatRecorded, r.counted, r.shadowHit = false, false, false
	// 请求热点追踪：关闭时 reqThreshold 为 0，命中路径不抽随机数、不读时钟。
	if ns := n.ns; ns.reqThreshold != 0 && c.state.Load() == cacheOpen && rand.Uint64() < ns.reqThreshold {
		s.req.record(ns.id, kh, key, c.now())
	}
	return nil
}

func (n *Namespace[V]) validRequest(r *Request[V]) bool {
	return n != nil && n.ns != nil && r != nil && r.ns == n.ns
}

type readStatus int

const (
	readHit readStatus = iota
	readMiss
	readBusy
	readClosed
)

type readResult[V any] struct {
	status readStatus
	handle *Handle[V]
}

// lookupResult 是受保护查询的紧凑结果（全部字段可寄存器传递，不含 value）。
// 命中时 guard 仍然有效，调用方必须读取所需字段后 unpin（或把 guard 交给 Borrowed）。
type lookupResult[V any] struct {
	status readStatus
	e      *entry
	g      guard
}

// lookup 受保护查询（READ_INTERNAL）：先 PIN 后读索引；TTL 用当前单调时间。
// 热度与 visited 更新在 guard 临界区内完成（A.3：guard 随视图存活），
// 确保迟到的读者不会把统计落在已被回收并复用的池条目上；
// 内部二次查询（singleflight 复核）由 recordHeatOnce 的 I11 门控自然去重。
func (c *cache[V]) lookup(r *Request[V]) lookupResult[V] {
	if c.state.Load() != cacheOpen {
		return lookupResult[V]{status: readClosed}
	}
	s := r.sh
	g, ok := s.epoch.pin()
	if !ok {
		return lookupResult[V]{status: readBusy}
	}
	// 先 pin 再复核状态：RECLAIMED 要求没有任何 pin，且状态早已离开 OPEN，
	// 所以通过复核的读者一定先于内存区释放。
	if c.state.Load() != cacheOpen {
		g.unpin()
		return lookupResult[V]{status: readClosed}
	}
	fr := s.idx.find(r.key, r.keyHash, r.ns.id)
	t := c.now()
	if !fr.found {
		g.unpin()
		c.recordHeatOnce(r, false, nil, t)
		return lookupResult[V]{status: readMiss}
	}
	e := fr.e
	if !e.isReadable() || !e.genIs(r.ns.gen.Load()) || t >= e.hardDeadline {
		g.unpin()
		// 提示维护者退休它：槽号加一，0 表示无提示；丢失只推迟退场，读时校验仍保证不交付。
		if h, v := &s.hints[r.keyHash>>58], uint32(fr.slot)+1; h.Load() != v { // 热 key 反复读到同一条过期条目时只读不写，免得争用缓存行
			h.Store(v)
		}
		c.recordHeatOnce(r, false, nil, t)
		return lookupResult[V]{status: readMiss}
	}
	// 有效命中：驻留热度与 visited 是条目状态，必须在释放 guard 前更新。
	c.recordHeatOnce(r, true, e, t)
	e.markVisited()
	if c.cfg.shadow() {
		// 影子命中：热度与 visited 已与真实缓存同样演化；只计一次命中，对调用方按 miss 回源。
		g.unpin()
		r.countLookup(true)
		r.shadowHit = true
		return lookupResult[V]{status: readMiss}
	}
	return lookupResult[V]{status: readHit, e: e, g: g}
}

// readOwned 在 lookup 之上交付拥有型命中：guard 内取得持有权，再退出 guard。
func (c *cache[V]) readOwned(r *Request[V]) readResult[V] {
	lr := c.lookup(r)
	if lr.status != readHit {
		return readResult[V]{status: lr.status}
	}
	h, err := c.acquireHandle(lr.e)
	lr.g.unpin()
	if err != nil {
		return readResult[V]{status: readBusy}
	}
	return readResult[V]{status: readHit, handle: h}
}

// acquireHandle 先取得额度和 pin，再分配；调用方须持有 guard 或已有句柄。
func (c *cache[V]) acquireHandle(e *entry) (*Handle[V], error) {
	var rc receipt
	if !c.handleBudget.reserveUnitInto(&rc) {
		return nil, ErrBusy
	}
	if !e.tryPin(int32(c.cfg.Limits.MaxOwnedPinsPerEntry)) {
		rc.returnOnce()
		return nil, ErrBusy
	}
	h := &Handle[V]{e: e, owner: c}
	rc.moveTo(&h.rc)
	return h, nil
}

// recordHeatOnce 一次逻辑请求至多记录一次 hit/miss 热度（I11）。
func (c *cache[V]) recordHeatOnce(r *Request[V], hit bool, e *entry, t int64) {
	if r.heatRecorded {
		return
	}
	r.heatRecorded = true
	delta := int64(c.cfg.Heat.Delta)
	be := bucketIndex(t, delta)
	if hit {
		e.heat.add(be, heatCountMax, heatRetries)
	} else {
		r.sh.gate.add(r.keyHash, be)
	}
}

func (r *Request[V]) countLookup(hit bool) {
	if r.counted {
		return
	}
	r.counted = true
	if hit {
		r.sh.cnt.hits.Add(1)
	} else {
		r.sh.cnt.misses.Add(1)
	}
}

// GetOn 只读查询（GET）：命中返回值；热度/visited 已在 lookup 内记录。不发起加载。
func (n *Namespace[V]) GetOn(r *Request[V]) (V, bool, error) {
	if !n.validRequest(r) {
		var zero V
		return zero, false, ErrInvalidRequest
	}
	return n.getOn(r)
}

// getOn 复用已校验的 Request，避免便捷入口再次校验。
func (n *Namespace[V]) getOn(r *Request[V]) (V, bool, error) {
	c := n.ns.c
	lr := c.lookup(r)
	var zero V
	switch lr.status {
	case readHit:
		// 返回给调用方的值归调用方独占：字节布局在 guard 内拷贝出一份再退出 guard。
		// 需要零拷贝时使用 BorrowOn。
		v := c.copyOut(lr.e)
		lr.g.unpin()
		r.countLookup(true)
		return v, true, nil
	case readMiss:
		r.countLookup(false)
		return zero, false, nil
	case readBusy:
		r.sh.cnt.busy.Add(1)
		return zero, false, ErrBusy
	default:
		return zero, false, ErrClosed
	}
}

// Get 只读查询的便捷形式（栈上 Request，稳态命中 0 堆分配）。
func (n *Namespace[V]) Get(key string) (V, bool, error) {
	var r Request[V]
	if err := n.beginRequest(&r, key); err != nil {
		var zero V
		return zero, false, err
	}
	return n.getOn(&r)
}

// Handle 是拥有型句柄：跨线程/异步持有 value。用完必须 Release。
type Handle[V any] struct {
	mu    sync.Mutex
	e     *entry
	owner *cache[V]
	rc    receipt // 句柄槽位凭证，内嵌以避免每次拥有型命中额外分配
}

// Value 返回持有值。
func (h *Handle[V]) Value() V {
	if h == nil {
		var zero V
		return zero
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.e == nil {
		var zero V
		return zero
	}
	return h.owner.view(h.e)
}

// Clone creates an independent owned handle, including after invalidation.
// Contention or the entry's pin limit returns ErrBusy; Release is safe to race.
func (h *Handle[V]) Clone() (*Handle[V], error) {
	if h == nil {
		return nil, ErrCanceled
	}
	if !h.mu.TryLock() {
		return nil, ErrBusy
	}
	defer h.mu.Unlock()
	if h.e == nil {
		return nil, ErrCanceled
	}
	return h.owner.acquireHandle(h.e)
}

// Release 释放持有（幂等）。
func (h *Handle[V]) Release() {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.e != nil {
		e := h.e
		h.e = nil // no retained value or entry reference after settlement
		e.unpin()
		h.rc.returnOnce()
		h.rc.budget = nil
		h.owner = nil
	}
}

// Borrowed is a scoped read view. It is confined to one goroutine and must be
// released before reuse. Do not copy it; Clone obtains independent ownership.
type Borrowed[V any] struct {
	_     noCopy
	e     *entry
	guard guard
	owner *cache[V]
}

func (b *Borrowed[V]) Value() V {
	if b == nil || b.e == nil {
		var zero V
		return zero
	}
	return b.owner.view(b.e)
}

func (b *Borrowed[V]) Release() {
	if b != nil && b.e != nil {
		b.e = nil
		b.guard.unpin()
		b.guard = guard{}
		b.owner = nil
	}
}

func (b *Borrowed[V]) Clone() (*Handle[V], error) {
	if b == nil || b.e == nil {
		return nil, ErrCanceled
	}
	return b.owner.acquireHandle(b.e)
}

// BorrowOn stores a protected view in caller-provided storage. Together with a
// reused Request this permits a read scope without allocating a handle.
func (n *Namespace[V]) BorrowOn(r *Request[V], b *Borrowed[V]) (bool, error) {
	if b == nil {
		return false, ErrInvalidRequest
	}
	_, ok, err := n.borrowOn(r, b)
	return ok, err
}

func (n *Namespace[V]) borrowOn(r *Request[V], b *Borrowed[V]) (*Borrowed[V], bool, error) {
	if !n.validRequest(r) || (b != nil && b.e != nil) {
		return nil, false, ErrInvalidRequest
	}
	lr := n.ns.c.lookup(r)
	switch lr.status {
	case readHit:
		if b == nil {
			b = &Borrowed[V]{}
		}
		*b = Borrowed[V]{e: lr.e, owner: n.ns.c, guard: lr.g}
		r.countLookup(true)
		return b, true, nil
	case readMiss:
		r.countLookup(false)
		return nil, false, nil
	case readBusy:
		r.sh.cnt.busy.Add(1)
		return nil, false, ErrBusy
	default:
		return nil, false, ErrClosed
	}
}

// GetBorrowed creates a protected read scope; release it when done.
func (n *Namespace[V]) GetBorrowed(key string) (*Borrowed[V], bool, error) {
	var r Request[V]
	if err := n.beginRequest(&r, key); err != nil {
		return nil, false, err
	}
	return n.borrowOn(&r, nil)
}

// GetOwned 拥有型查询：返回句柄，释放前该条目不会被回收（I9）。
func (n *Namespace[V]) GetOwned(key string) (*Handle[V], bool, error) {
	var r Request[V]
	if err := n.beginRequest(&r, key); err != nil {
		return nil, false, err
	}
	return n.GetOwnedOn(&r)
}

// GetOwnedOn reuses one logical request, including bounded retries on BUSY.
func (n *Namespace[V]) GetOwnedOn(r *Request[V]) (*Handle[V], bool, error) {
	if !n.validRequest(r) {
		return nil, false, ErrInvalidRequest
	}
	c := n.ns.c
	rr := c.readOwned(r)
	switch rr.status {
	case readHit:
		r.countLookup(true)
		return rr.handle, true, nil
	case readMiss:
		r.countLookup(false)
		return nil, false, nil
	case readBusy:
		r.sh.cnt.busy.Add(1)
		return nil, false, ErrBusy
	default:
		return nil, false, ErrClosed
	}
}

// Fetch 查询并加载（GET_OR_LOAD 同步形式）：
// 命中返回缓存值；miss 时按 singleflight 合并加载；加载错误与过载分别报告。
// 响应成功不代表已缓存；BUSY 表示暂时竞争，可重试。
func (n *Namespace[V]) Fetch(ctx context.Context, key string, loader Loader[V]) (V, error) {
	if err := checkContext(ctx); err != nil {
		var zero V
		return zero, contextError(err)
	}
	var r Request[V] // 栈分配：Request 只在本次调用内使用
	if err := n.beginRequest(&r, key); err != nil {
		var zero V
		return zero, err
	}
	return n.fetchOn(ctx, &r, loader)
}

// FetchOn 以已有 Request 执行查询并加载。
func (n *Namespace[V]) FetchOn(ctx context.Context, r *Request[V], loader Loader[V]) (V, error) {
	if err := checkContext(ctx); err != nil {
		var zero V
		return zero, contextError(err)
	}
	if !n.validRequest(r) {
		var zero V
		return zero, ErrInvalidRequest
	}
	return n.fetchOn(ctx, r, loader)
}

func (n *Namespace[V]) fetchOn(ctx context.Context, r *Request[V], loader Loader[V]) (V, error) {
	v, ok, err := n.getOn(r)
	if err != nil {
		var zero V
		return zero, err
	}
	if ok {
		return v, nil
	}
	fut, err := n.ns.c.getOrLoad(r, loader)
	if err != nil {
		var zero V
		return zero, err
	}
	return fut.Wait(ctx)
}

// FetchAsync 查询并加载的异步形式：返回可等待的 Future。
// 返回成功后必须用 Wait、Take（ok=true）或 Cancel 之一终结 Future：未终结的 Future
// 持续占用 MaxWaitersTotal 槽位（命中时还占用拥有型句柄）。被丢弃的 Future 会在 GC 时自动 Cancel
// 作为兜底，但回收时机不确定，期间预算仍被占用，不能依赖它。
func (n *Namespace[V]) FetchAsync(key string, loader Loader[V]) (*Future[V], error) {
	var r Request[V]
	if err := n.beginRequest(&r, key); err != nil {
		return nil, err
	}
	return n.FetchAsyncOn(&r, loader)
}

// FetchAsyncOn 以已有 Request 执行异步查询并加载；终结要求同 FetchAsync。
func (n *Namespace[V]) FetchAsyncOn(r *Request[V], loader Loader[V]) (*Future[V], error) {
	f, err := n.fetchAsyncOn(r, loader)
	if f != nil {
		f.guard()
	}
	return f, err
}

// fetchAsyncOn 先完成受理，公开入口再为返回的 Future 注册安全网。
func (n *Namespace[V]) fetchAsyncOn(r *Request[V], loader Loader[V]) (*Future[V], error) {
	if !n.validRequest(r) {
		return nil, ErrInvalidRequest
	}
	c := n.ns.c
	lr := c.lookup(r)
	switch lr.status {
	case readClosed:
		return nil, ErrClosed
	case readBusy:
		r.sh.cnt.busy.Add(1)
		return nil, ErrBusy
	case readHit:
		r.countLookup(true)
		var rc receipt
		if c.waiterBudget.tryReserveInto(&rc, 1) != failOK {
			lr.g.unpin()
			c.metrics.overloaded.Add(1)
			return nil, ErrOverloaded
		}
		h, err := c.acquireHandle(lr.e)
		lr.g.unpin()
		if err != nil {
			rc.returnOnce()
			r.sh.cnt.busy.Add(1)
			return nil, err
		}
		// 命中仅由此 Future 持有，无需创建支持多等待者的 delivery。
		f := &Future[V]{ready: completedFutureSignal, cached: h}
		rc.moveTo(&f.rcSlot)
		f.state.Store(waiterReadyCached)
		f.notified.Store(true)
		return f, nil
	}
	// 同一逻辑请求只计一次 miss（I11；GetOn/FetchOn 前置读已计数时跳过）。
	r.countLookup(false)
	return c.getOrLoad(r, loader)
}

// ---- 控制操作 ----

type controlKind uint8

const (
	ctlInvalidateKey controlKind = iota
	ctlInvalidateNS
)

// controlOp 是已受理的控制操作：必须有终态。
type controlOp[V any] struct {
	kind    controlKind
	key     string
	keyHash uint64
	ns      *namespace[V]
	result  controlResult // 内嵌完成单元：操作与确认凭证一次分配
	handle  ControlHandle
	rc      receipt // 控制预算凭证（内嵌）
	claimed atomic.Bool
}

// newControlOp 构造控制操作：先取得控制额度再分配，过载拒绝路径无堆分配。
func newControlOp[V any](c *cache[V], kind controlKind, ns *namespace[V], keyHash uint64) (*controlOp[V], bool) {
	var rc receipt
	if c.controlBudget.tryReserveInto(&rc, 1) != failOK {
		return nil, false
	}
	o := &controlOp[V]{kind: kind, ns: ns, keyHash: keyHash}
	rc.moveTo(&o.rc)
	return o, true
}

// initResult 绑定内嵌的完成单元（失效操作必须有可等待的终态）。
func (o *controlOp[V]) initResult() {
	o.result.ready = make(chan struct{})
	o.handle.result = &o.result
}

// 完成或拒绝后只保留确认结果，不让凭证继续保留 key 和整个缓存。
func (o *controlOp[V]) release() {
	o.key, o.ns = "", nil
	o.rc.returnOnce()
	o.rc.budget = nil
}

type controlResult struct {
	ready chan struct{}
	err   error
	done  atomic.Bool
}

func (r *controlResult) finish(err error) {
	if r != nil && r.ready != nil && r.done.CompareAndSwap(false, true) {
		r.err = err
		close(r.ready)
	}
}

// ControlHandle 是失效操作的完成凭证：确认表示逻辑效果已提交。
type ControlHandle struct {
	result *controlResult
}

// Wait 等待确认。
func (h *ControlHandle) Wait(ctx context.Context) error {
	if h == nil || h.result == nil {
		return ErrUninitialized
	}
	if err := waitContext(ctx, h.result.ready); err != nil {
		return err
	}
	return h.result.err
}

// InvalidateAsync 单 key 失效：未受理返回 ErrBusy；已受理返回完成凭证。
func (n *Namespace[V]) InvalidateAsync(key string) (*ControlHandle, error) {
	if n == nil || n.ns == nil {
		return nil, ErrUninitialized
	}
	c := n.ns.c
	c.admissions.Add(1)
	defer c.admissions.Add(-1)
	if c.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	if len(key) > c.cfg.MaxKeyBytes {
		return nil, ErrKeyTooLarge
	}
	kh := n.ns.hashKey(key)
	s := c.shardOf(kh)
	if len(s.controlQueue) == cap(s.controlQueue) {
		return nil, ErrBusy
	}
	op, ok := newControlOp(c, ctlInvalidateKey, n.ns, kh)
	if !ok {
		return nil, ErrBusy
	}
	op.key = strings.Clone(key)
	op.initResult()
	select {
	case s.controlQueue <- op:
		c.signalMaintenance(s)
		return &op.handle, nil
	default:
		op.release()
		return nil, ErrBusy // 未受理，不产生效果
	}
}

// Invalidate 单 key 失效（同步等待确认）。
// 成功确认后，新查询不得取得旧可读版本。
func (n *Namespace[V]) Invalidate(ctx context.Context, key string) error {
	if err := checkContext(ctx); err != nil {
		return contextError(err)
	}
	h, err := n.InvalidateAsync(key)
	if err != nil {
		return err
	}
	return h.Wait(ctx)
}

// InvalidateNamespace 命名空间失效：推进代际，旧代际立即逻辑隐藏，后台分批摘除。
func (c *Cache[V]) InvalidateNamespace(ctx context.Context, name string) error {
	if err := checkContext(ctx); err != nil {
		return contextError(err)
	}
	h, err := c.InvalidateNamespaceAsync(name)
	if err != nil {
		return err
	}
	return h.Wait(ctx)
}

// InvalidateNamespaceAsync returns a repeatable completion confirmation.
func (c *Cache[V]) InvalidateNamespaceAsync(name string) (*ControlHandle, error) {
	if c == nil || c.c == nil {
		return nil, ErrUninitialized
	}
	cc := c.c
	cc.admissions.Add(1)
	defer cc.admissions.Add(-1)
	if cc.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	cc.nsMu.RLock()
	ns, ok := cc.nsByName[name]
	cc.nsMu.RUnlock()
	if !ok {
		return nil, ErrNoSuchNamespace
	}
	if len(cc.nsControlQueue) == cap(cc.nsControlQueue) {
		return nil, ErrBusy
	}
	op, ok := newControlOp(cc, ctlInvalidateNS, ns, 0)
	if !ok {
		return nil, ErrBusy
	}
	op.initResult()
	select {
	case cc.nsControlQueue <- op:
		cc.ensureNsDrain()
	default:
		op.release()
		return nil, ErrBusy
	}
	return &op.handle, nil
}

// ---- 关闭（A.9 BEGIN_CLOSE / POLL_CLOSE）----

// Shutdown 停止受理新工作并立即返回：返回时「停止受理」已生效（新请求得到 ErrClosed），
// 已受理的控制操作、等待者与真实 I/O 继续推进。幂等。
func (c *Cache[V]) Shutdown() {
	if c == nil || c.c == nil {
		return
	}
	cc := c.c
	cc.closeOnce.Do(cc.stopAdmission)
}

// Close 等价于 Shutdown 后等待安全回收完成（RECLAIMED）。
// 返回 nil 表示已回收；ctx 先到期则返回 ErrCloseIncomplete（同时满足 errors.Is(err, ctx.Err())）：
// 停止受理已生效，其余由后台继续推进，不强制释放仍被持有的对象。
func (c *Cache[V]) Close(ctx context.Context) error {
	if c == nil || c.c == nil {
		return ErrUninitialized
	}
	if ctx == nil {
		return ErrNilContext
	}
	c.Shutdown()
	if err := c.WaitReclaimed(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrCloseIncomplete, err)
	}
	c.c.tickerWG.Wait()
	return nil
}

// WaitDrained 等待已受理工作全部终结：控制操作、填充候选、真实 I/O 与响应均已结束。
// 此时读者与被持有句柄仍可能存在，物理回收尚未完成（见 WaitReclaimed）。
// 未调用 Shutdown / Close 时不会自行返回。
func (c *Cache[V]) WaitDrained(ctx context.Context) error {
	if c == nil || c.c == nil {
		return ErrUninitialized
	}
	return waitContext(ctx, c.c.drainedCh)
}

// WaitReclaimed 等待安全回收完成：所有读者、拥有型句柄、退休记录与预算均已清空。
func (c *Cache[V]) WaitReclaimed(ctx context.Context) error {
	if c == nil || c.c == nil {
		return ErrUninitialized
	}
	return waitContext(ctx, c.c.closedCh)
}

// CloseProgress 是关闭三阶段的非阻塞快照。任何阶段的缺失都不等价于强制结束 I/O 或释放被持有的内存。
type CloseProgress struct {
	Stopped   bool // 已停止受理新工作
	Drained   bool // 已受理工作全部终结
	Reclaimed bool // 安全回收完成
}

// CloseProgress 报告关闭进展（A.9 POLL_CLOSE）。
func (c *Cache[V]) CloseProgress() CloseProgress {
	if c == nil || c.c == nil {
		return CloseProgress{}
	}
	stopped, drained, reclaimed := c.c.pollClose()
	return CloseProgress{Stopped: stopped, Drained: drained, Reclaimed: reclaimed}
}
