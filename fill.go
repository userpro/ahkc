package ahkc

import (
	"math"
	"sync/atomic"
)

// 填充候选与条件提交。
//
// 候选阶段：QUEUED → COMMITTING → 终态（I6）。
// 过期/取消方只能把 QUEUED 改成终态；维护者只有成功取得 COMMITTING 才能修改驻留集合。
// 提交顺序固定：准备 → 复核取得提交权 → 移除 → 发布 → 终结；
// 第一个不可逆动作（COMMITTING CAS 成功）之前完成所有可能失败的准备。

const (
	cellQueued int32 = iota
	cellCommitting
	cellTerminalBase = 100 // 终态 = base + fillReason
)

// fillCell 是候选的提交协调单元。
type fillCell[V any] struct {
	phase  atomic.Int32
	flight *flight[V] // 可持有身份记录，不隐式多持有 value
}

func (c *fillCell[V]) isTerminal() bool { return c.phase.Load() >= cellTerminalBase }

// cancel 由超时/关闭方调用：只改 QUEUED→终态，不触碰 payload。
func (c *fillCell[V]) cancel(reason fillReason) bool {
	return c.phase.CompareAndSwap(cellQueued, cellTerminalBase+int32(reason))
}

// candEntry 是未公开候选的身份与值：挂起期间留在 Go 堆上（计入 pendingBytes），
// 提交时才写入条目区。entryID 由维护者补齐。
type candEntry[V any] struct {
	key          string
	keyHash      uint64
	entryID      uint64
	nsID         uint32
	nsGen        uint64
	value        V
	hardDeadline int64
	billedBytes  int64
	cost         float32
}

// fillCandidate 是填充候选（FillCandidate）。
type fillCandidate[V any] struct {
	e               candEntry[V]
	cell            fillCell[V] // 内嵌：候选与完成单元一次分配，flight 持有 &cand.cell
	tok             token
	expected        uint64 // expectedEntryId；0 = NONE
	handoffDeadline int64
	pending         receipt // pending 预算凭证；发布 / 拒绝时归还一次
}

// fillScratch 是分片维护者独占的填充决策 scratch：一次 processFill 的全部中间对象都复用它，
// 稳态填充不再逐次分配，也避免在每个新 goroutine 栈上放置大数组而触发栈扩容。
// 仅在 processFill 期间有效；不得跨填充保留引用。
type fillScratch[V any] struct {
	sums     [256]subsetAgg
	q        []*entry
	vis      []victimInfo
	removals []*entry
	snap     fillSnapshot
	plan     fillPlan[V]
	prep     prepared[V]
}

func (f *fillScratch[V]) init(victimLimit int) {
	f.q = make([]*entry, 0, victimLimit)
	f.vis = make([]victimInfo, 0, victimLimit)
	f.removals = make([]*entry, 0, victimLimit+1)
}

// fillPlan 是求解结果。
type fillPlan[V any] struct {
	snap     *fillSnapshot
	removals []*entry // O ∪ S*
	gain     float64
	av       float64 // 候选的估值收益
}

// prepared 是提交准备结果。
type prepared[V any] struct {
	plan *fillPlan[V]
	slot int
	e    *entry // 已分配并初始化、尚未公开的块
}

// tryPrepareCandidate 由完成泵调用（A.6 TRY_PREPARE_CANDIDATE）。
// 成功返回候选；失败返回终态原因。只有全部预留成功才产生唯一候选。
func (c *cache[V]) tryPrepareCandidate(f *flight[V]) (*fillCandidate[V], fillReason) {
	if c.cfg.observeOnly() {
		// 仅统计不缓存：交付与合并照常，候选在此统一拒绝并计数（OBSERVE_ONLY），
		// 不触碰计费/池/预算——观察模式不产生任何驻留责任。
		return nil, fillRejectedObserveOnly
	}
	if f.shadowHit {
		return nil, fillRejectedShadowHit
	}
	s := c.shards[f.shardID]
	ns := c.nsByID(f.nsID)
	d := f.dlv
	if d == nil || d.outcome.err != nil {
		return nil, fillRejectedStale
	}
	if !d.outcome.cacheable {
		return nil, fillRejectedNoCache
	}
	now := c.now()
	if c.state.Load() != cacheOpen || f.tok != s.currentToken(ns) {
		return nil, fillRejectedStale
	}
	hardDeadline := d.outcome.hardDeadline
	deadline := hardDeadline
	if hd := f.work.reuseDeadline; hd > 0 && hd < deadline {
		deadline = hd // min(hardDeadline, completedAt+handoffTimeout)
	}
	if now >= deadline {
		return nil, fillRejectedExpired
	}
	// 热度准入：加载开始时不热、完成时已热的 key 可以取得资格。
	delta := int64(c.cfg.Heat.Delta)
	h, ok := s.gate.read(f.keyHash, bucketIndex(now, delta), bucketPhi(now, delta))
	if !ok || h < ns.cfg.Tin {
		return nil, fillRejectedNotHot
	}
	b := d.outcome.billedBytes
	if b > c.cfg.MaxEntryBytes {
		return nil, fillRejectedTooLarge
	}
	a := &fillCandidate[V]{}
	if c.pendingBytes.tryReserveInto(&a.pending, b) != failOK {
		return nil, fillRejectedResourceLimit
	}
	a.e = candEntry[V]{
		key: f.key, keyHash: f.keyHash, nsID: f.nsID, nsGen: f.tok.nsGen,
		hardDeadline: hardDeadline, billedBytes: b, cost: d.outcome.cost,
	}
	if !c.cfg.shadow() { // 影子条目只留元数据；影子命中从不交付值
		a.e.value = d.outcome.value
	}
	a.tok, a.expected, a.handoffDeadline = f.tok, f.expected, deadline
	a.cell.flight = f
	a.cell.phase.Store(cellQueued)
	return a, fillNone
}

// cancelFillCell 由交接超时/关闭方调用（CANCEL_FILL_CELL）。
func (c *cache[V]) cancelFillCell(cell *fillCell[V], reason fillReason) {
	if cell == nil {
		return
	}
	if cell.cancel(reason) {
		c.metrics.fillRejected[reason].Add(1)
	}
	c.signalPump(cell.flight)
}

// rejectCandidate 终结并处置候选（REJECT_*）：调用方持有候选所有权。
// 已被取消时保留原终态，不重复计数。
func (c *cache[V]) rejectCandidate(a *fillCandidate[V], reason fillReason) {
	if a.cell.cancel(reason) {
		c.metrics.fillRejected[reason].Add(1)
	}
	c.disposeCandidate(a)
	c.signalPump(a.cell.flight)
}

// disposeCandidate 释放候选持有责任（DISPOSE_PENDING 的 Go 形态：
// 无用户析构，丢弃 value 引用并归还有界凭证）。
func (c *cache[V]) disposeCandidate(a *fillCandidate[V]) {
	a.e.value, a.e.key = *new(V), ""
	a.pending.returnOnce()
}

// matchExpected 条件安装：expectedEntryId 为空只允许当前 key 不存在（A.5）。
func matchExpected[V any](a *fillCandidate[V], fr findResult) bool {
	if a.expected == 0 {
		return !fr.found
	}
	return fr.found && fr.e.entryID == a.expected
}

// processFill 处理一个候选（PROCESS_FILL）。
func (c *cache[V]) processFill(s *shard[V], a *fillCandidate[V]) {
	if a.cell.isTerminal() {
		c.disposeCandidate(a)
		return
	}
	plan, reason := c.planFill(s, a)
	if plan == nil {
		c.rejectCandidate(a, reason)
		return
	}
	prep, reason := c.prepareCommit(s, a, plan)
	if prep == nil {
		c.rejectCandidate(a, reason)
		return
	}
	c.commitFill(s, a, prep)
}

// planFill 形成计划（PLAN_FILL）。
func (c *cache[V]) planFill(s *shard[V], a *fillCandidate[V]) (*fillPlan[V], fillReason) {
	if a.cell.phase.Load() != cellQueued {
		return nil, fillRejectedAlreadyTerm
	}
	cur := s.idx.find(a.e.key, a.e.keyHash, a.e.nsID)
	if !matchExpected(a, cur) {
		return nil, fillRejectedStale
	}
	var o *entry
	if cur.found {
		o = cur.e
	}
	now := c.now()
	var q []*entry
	// 空闲快速路径：O-only 替换在条数/字节/退休/索引下都可行时明确选用空 Q。
	if c.fitsWithoutVictims(s, a, o) {
		q = nil
	} else {
		q = c.collectQ(s, o, now, sieveScanLimit)
	}
	// collectQ 可能执行生命周期清理，复核旧身份与占用。
	if !matchExpected(a, s.idx.find(a.e.key, a.e.keyHash, a.e.nsID)) {
		return nil, fillRejectedStale
	}
	snap, av, oldLoss, reason := c.freezeInput(s, a, o, q)
	if snap == nil {
		return nil, reason
	}
	q = q[:len(snap.q)] // freezeInput compacts expired victims in place.
	sel := snap.exactSelect()
	c.metrics.fillChecks.Add(uint64(snap.checks))
	if sel == selectWorkLimit {
		return nil, fillRejectedWorkLimit
	}
	if sel == selectInfeasible {
		return nil, fillRejectedInfeasible
	}
	selLoss := float64(0)
	removals := s.fs.removals[:0]
	if o != nil {
		removals = append(removals, o)
	}
	for i := range q {
		if sel&(1<<uint(i)) != 0 {
			selLoss += snap.q[i].loss
			removals = append(removals, q[i])
		}
	}
	gain := av - oldLoss - selLoss
	if !allFiniteNonNegative(gain) || gain == 0 {
		return nil, fillRejectedNonPositive
	}
	s.fs.removals = removals
	s.fs.plan = fillPlan[V]{snap: snap, removals: removals, gain: gain, av: av}
	return &s.fs.plan, fillNone
}

// fitsWithoutVictims 判断无受害者（仅 O）是否可行。
func (c *cache[V]) fitsWithoutVictims(s *shard[V], a *fillCandidate[V], o *entry) bool {
	var oBytes int64
	oCount := int64(0)
	if o != nil {
		oBytes = int64(o.billedBytes)
		oCount = 1
	}
	if s.residentCount.Load()+1-oCount > s.countQuota.Load() {
		return false
	}
	if s.residentBytes.Load()+a.e.billedBytes-oBytes > s.byteQuota.Load() {
		return false
	}
	_, ok := s.idx.installSlot(a.e.keyHash, func(x *entry) bool { return x == o })
	return ok
}

// collectQ SIEVE 有界扫描（COLLECT_Q）。hand 从尾向头推进、到头回绕；
// 命中置位者清位跳过；失效条目顺手退休。
func (c *cache[V]) collectQ(s *shard[V], oldEntry *entry, t int64, scanLimit int) []*entry {
	pl := &s.policy
	q := s.fs.q[:0]
	limit := scanLimit
	if pl.count < limit {
		limit = pl.count
	}
	var anchor blockRef
	for i := 0; i < limit; i++ {
		if pl.count == 0 {
			break
		}
		er := pl.hand
		if er == 0 {
			er = pl.tail
		}
		if er == 0 || er == anchor {
			break // 不因删除后队列变短而重复绕圈
		}
		e := s.arena.entry(er)
		pl.hand = pl.nextTowardHeadOrWrap(e)
		if !e.isReadable() || t >= e.hardDeadline || !e.genIs(c.nsByID(e.nsID).gen.Load()) {
			c.tryRetire(s, e)
			continue
		}
		if anchor == 0 {
			anchor = er
		}
		if e == oldEntry {
			continue
		}
		if e.takeVisited() {
			continue
		}
		q = append(q, e)
		if len(q) >= c.cfg.Evict.VictimLimit {
			break
		}
	}
	s.fs.q = q
	return q
}

// freezeInput 冻结决策输入（FREEZE_INPUT）。
func (c *cache[V]) freezeInput(s *shard[V], a *fillCandidate[V], o *entry, q []*entry) (*fillSnapshot, float64, float64, fillReason) {
	t := c.now()
	ns := c.nsByID(a.e.nsID)
	if c.state.Load() != cacheOpen || a.tok != s.currentToken(ns) {
		return nil, 0, 0, fillRejectedStale
	}
	if t >= a.e.hardDeadline || t >= a.handoffDeadline {
		return nil, 0, 0, fillRejectedExpired
	}
	delta := int64(c.cfg.Heat.Delta)
	idx, phi := bucketIndex(t, delta), bucketPhi(t, delta)
	h, ok := s.gate.read(a.e.keyHash, idx, phi)
	if !ok || h < ns.cfg.Tin {
		return nil, 0, 0, fillRejectedNotHot
	}
	// 收益统一为热度 × 单次成本 × 剩余有效期。候选取门卫热度（准入已保证 ≥ Tin）；
	// 驻留条目的请求热度 = 条目命中热度（精确）+ 门卫未命中热度，与 Namespace.Heat 同口径，
	// 因此刚发布的条目仍带着准入时的热度；失效或过期者未来收益为 0。
	av := c.heatValue(h, a.e.cost, a.e.hardDeadline, t)
	lossOf := func(e *entry) float64 {
		if !e.isReadable() || t >= e.hardDeadline || !e.genIs(c.nsByID(e.nsID).gen.Load()) {
			return 0
		}
		eh, _ := e.heat.read(idx, phi) // 未知（时钟落后）按 0 计
		gh, _ := s.gate.read(e.keyHash, idx, phi)
		return c.heatValue(eh+gh, e.cost, e.hardDeadline, t)
	}
	oldLoss := float64(0)
	if o != nil {
		oldLoss = lossOf(o)
	}
	// 冻结受害者视图；失效/到期者排除（不追加新搜索）。
	vis := s.fs.vis[:0]
	liveQ := q[:0]
	for _, v := range q {
		if !v.isReadable() || t >= v.hardDeadline || !v.genIs(c.nsByID(v.nsID).gen.Load()) {
			continue
		}
		liveQ = append(liveQ, v)
		vis = append(vis, victimInfo{id: v.entryID, billed: int64(v.billedBytes), loss: lossOf(v), indexSlot: int(v.indexSlot)})
	}
	q = liveQ
	var oBytes int64
	oCount := 0
	if o != nil {
		oBytes = int64(o.billedBytes)
		oCount = 1
	}
	s.fs.vis = vis
	snap := &s.fs.snap
	*snap = fillSnapshot{
		sums:   &s.fs.sums,
		aBytes: a.e.billedBytes,
		oBytes: oBytes, oCount: oCount,
		q:          vis,
		n:          s.residentCount.Load(),
		bytes:      s.residentBytes.Load(),
		countQuota: s.countQuota.Load(), byteQuota: s.byteQuota.Load(),
		maxChecks: 1 << uint(c.cfg.Evict.VictimLimit),
	}
	// 索引安装位置（候选键的探测序列）：无受害者也能安装，或哪些受害者的槽位在序列内。
	_, snap.installWithoutQ = s.idx.installSlot(a.e.keyHash, func(x *entry) bool { return x == o })
	var installMask uint64
	for i, v := range q {
		if s.idx.inProbeDomain(a.e.keyHash, int(v.indexSlot)) {
			installMask |= 1 << uint(i)
		}
	}
	snap.installMask = installMask
	return snap, av, oldLoss, fillNone
}

// prepareCommit 准备提交（PREPARE_COMMIT）：第一个不可逆动作前完成所有可能失败的准备。
func (c *cache[V]) prepareCommit(s *shard[V], a *fillCandidate[V], plan *fillPlan[V]) (*prepared[V], fillReason) {
	// 复核身份与资源快照未变化。
	if plan.snap.n != s.residentCount.Load() || plan.snap.bytes != s.residentBytes.Load() ||
		plan.snap.countQuota != s.countQuota.Load() || plan.snap.byteQuota != s.byteQuota.Load() {
		return nil, fillRejectedStale
	}
	slot, ok := s.idx.installSlot(a.e.keyHash, func(x *entry) bool {
		for _, e := range plan.removals {
			if x == e {
				return true
			}
		}
		return false
	})
	if !ok {
		return nil, fillRejectedInfeasible
	}
	// CHECKED_NEXT：单调生成不重复身份。
	if s.entrySeq >= 1<<48-1 {
		c.stopAdmission()
		return nil, fillRejectedResourceLimit
	}
	// 物理分配：在不可逆点之前完成。受害者的块在 epoch 确认安全前不可复用，所以只从现有空闲
	// 空间分配；失败时不移除任何条目。
	valLen := c.valueLen(a.e.value)
	ref, ok := s.arena.alloc(c.blockSize(len(a.e.key), valLen))
	if !ok {
		return nil, fillRejectedAllocation
	}
	s.entrySeq++
	a.e.entryID = uint64(s.id)<<48 | s.entrySeq
	e, ok := c.initBlock(s, ref, a, valLen)
	if !ok {
		s.arena.free(ref)
		return nil, fillRejectedAllocation
	}
	s.fs.prep = prepared[V]{plan: plan, slot: slot, e: e}
	return &s.fs.prep, fillNone
}

// commitFill 条件提交（COMMIT_FILL）。COMMITTING 之后由维护者唯一给出终态。
func (c *cache[V]) commitFill(s *shard[V], a *fillCandidate[V], p *prepared[V]) {
	ns := c.nsByID(a.e.nsID)
	t := c.now()
	reject := func(reason fillReason) {
		c.freeBlock(s, p.e) // 准备阶段分配的块从未公开
		c.rejectCandidate(a, reason)
	}
	if c.state.Load() != cacheOpen || a.tok != s.currentToken(ns) {
		reject(fillRejectedStale)
		return
	}
	if !matchExpected(a, s.idx.find(a.e.key, a.e.keyHash, a.e.nsID)) {
		reject(fillRejectedStale)
		return
	}
	if t >= a.e.hardDeadline || t >= a.handoffDeadline {
		reject(fillRejectedExpired)
		return
	}
	if p.plan.snap.n != s.residentCount.Load() || p.plan.snap.bytes != s.residentBytes.Load() {
		reject(fillRejectedStale)
		return
	}
	if !a.cell.phase.CompareAndSwap(cellQueued, cellCommitting) {
		c.freeBlock(s, p.e)
		c.disposeCandidate(a)
		c.signalPump(a.cell.flight)
		return // ALREADY_TERMINAL：未改变驻留集合
	}
	// ---- 不可逆点：此点后无用户回调、无常规失败分配 ----
	// 移除：标不可读 → 索引/策略摘除 → 登记退休。
	for _, e := range p.plan.removals {
		e.setReadable(false)
		s.idx.remove(e)
		s.policy.remove(e)
		s.residentCount.Add(-1)
		s.residentBytes.Add(-int64(e.billedBytes))
	}
	// 移除后复核外部状态：命名空间失效/TTL 过期不能发布旧代际。
	outcome := fillPublished
	if c.state.Load() != cacheOpen || a.tok != s.currentToken(ns) || c.now() >= a.e.hardDeadline {
		outcome = fillRejectedStale
	}
	installed := outcome == fillPublished
	if installed {
		e := p.e
		s.residentCount.Add(1)
		s.residentBytes.Add(int64(e.billedBytes))
		// pending → resident 账目转移：新账目（owner 计数）先成立，再归还 pending 凭证。
		a.pending.returnOnce()
		s.policy.pushHead(e)
		s.idx.publish(e, p.slot)
		// 发布与失效可能交错：读取代际/TTL 检查是最终保障。
		if !e.genIs(ns.gen.Load()) || c.now() >= e.hardDeadline || c.state.Load() != cacheOpen {
			e.setReadable(false)
			outcome = fillRejectedStale
		} else {
			c.metrics.fillPublished.Add(1)
			s.fillGain.Store(math.Float64bits(math.Min(math.MaxFloat64, math.Float64frombits(s.fillGain.Load())+p.plan.gain)))
		}
	}
	// 退休登记（挂入退休链）。
	for _, e := range p.plan.removals {
		c.registerRetired(s, e)
	}
	if !installed {
		// 提交中断：块从未公开，直接归还；未发布对象继续计 pending 直到真实释放。
		c.freeBlock(s, p.e)
		c.disposeCandidate(a)
	}
	a.cell.phase.Store(cellTerminalBase + int32(outcome))
	if outcome != fillPublished {
		c.metrics.fillRejected[outcome].Add(1)
	}
	c.signalPump(a.cell.flight)
}

// heatValue 是热度估值 G = λ × cost × min(Horizon, 剩余 TTL)，λ = H/Δ（次/秒）。
// 热度 ≤ 4095、cost 为有限 float32，乘积远小于 float64 上界，多项求和不会溢出。
func (c *cache[V]) heatValue(heat float64, cost float32, hardDeadline, t int64) float64 {
	rem := hardDeadline - t
	if rem <= 0 {
		return 0
	}
	dur := math.Min(deltaSeconds(int64(c.cfg.Evict.Horizon)), deltaSeconds(rem))
	return heat / deltaSeconds(int64(c.cfg.Heat.Delta)) * float64(cost) * dur
}

func deltaSeconds(ns int64) float64 { return float64(ns) / 1e9 }

func allFiniteNonNegative(vs ...float64) bool {
	for _, v := range vs {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	return true
}
