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

// fillCandidate 是填充候选（FillCandidate）。
type fillCandidate[V any] struct {
	e               *entry[V]   // 未公开；entryId 由维护者补齐
	cell            fillCell[V] // 内嵌：候选与完成单元一次分配，flight 持有 &cand.cell
	tok             token
	expected        uint64 // expectedEntryId；0 = NONE
	handoffDeadline int64
	pending         receipt // pending 预算凭证；发布 / 拒绝时归还一次
}

// frozenModel 是一次决策冻结的命名空间模型、持有者与时间点（FREEZE_INPUT）。
type frozenModel[V any] struct {
	ns      *namespace[V]
	holder  *modelHolder
	version uint64
	model   CostModel
	config  NamespaceConfig
}

// maxFrozenModels 是一次决策最多涉及的命名空间数（候选 + O + Q 成员各自的命名空间）。
const maxFrozenModels = 10

// fillScratch 是分片维护者独占的填充决策 scratch：一次 processFill 的全部中间对象都复用它，
// 稳态填充不再逐次分配，也避免在每个新 goroutine 栈上放置大数组而触发栈扩容。
// 仅在 processFill 期间有效；不得跨填充保留引用。
type fillScratch[V any] struct {
	models   [maxFrozenModels]frozenModel[V]
	tables   [maxFrozenModels]costTableSnapshot
	sums     [256]subsetAgg
	q        []*entry[V]
	vis      []victimInfo
	removals []*entry[V]
	snap     fillSnapshot
	plan     fillPlan[V]
	prep     prepared[V]
}

func (f *fillScratch[V]) init(victimLimit int) {
	f.q = make([]*entry[V], 0, victimLimit)
	f.vis = make([]victimInfo, 0, victimLimit)
	f.removals = make([]*entry[V], 0, victimLimit+1)
}

// fillPlan 是求解结果。
type fillPlan[V any] struct {
	snap     *fillSnapshot
	removals []*entry[V] // O ∪ S*
	gain     float64
	av       estimate
}

// prepared 是提交准备结果。
type prepared[V any] struct {
	plan *fillPlan[V]
	slot int
}

// tryPrepareCandidate 由完成泵调用（A.6 TRY_PREPARE_CANDIDATE）。
// 成功返回候选；失败返回终态原因。只有全部预留成功才产生唯一候选。
func (c *cache[V]) tryPrepareCandidate(f *flight[V]) (*fillCandidate[V], fillReason) {
	if c.cfg.ObserveOnly {
		// 仅统计不缓存：交付与合并照常，候选在此统一拒绝并计数（OBSERVE_ONLY），
		// 不触碰计费/池/预算——观察模式不产生任何驻留责任。
		return nil, fillRejectedObserveOnly
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
	delta := int64(c.cfg.HeatDelta)
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
	e, ok := s.pool.tryGet()
	if !ok {
		a.pending.returnOnce()
		return nil, fillRejectedAllocation
	}
	// 初始化未公开条目（身份/值/截止时间发布后不可变）。
	e.key, e.keyHash = f.key, f.keyHash
	e.entryID = 0 // 未分配，维护者补齐
	e.nsID = f.nsID
	e.nsGen = f.tok.nsGen
	e.value = d.outcome.value
	e.hardDeadline = hardDeadline
	e.billedBytes = b
	e.costClass = d.outcome.costClass
	e.readable.Store(false)
	e.visited.Store(false)
	e.ownedPins.Store(0)
	e.heat.v.Store(packHeat(heatSnap{epoch: bucketIndex(now, delta)}))
	e.graceUntil, e.lowSince, e.indexSlot = 0, 0, -1
	e.inPolicy = false
	e.policyPrev, e.policyNext = nil, nil
	e.timerMember = false
	e.timerPrev, e.timerNext = nil, nil
	e.retirement = nil
	a.e, a.tok, a.expected, a.handoffDeadline = e, f.tok, f.expected, deadline
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
// 无用户析构，归还有界凭证并归还固定池槽）。
func (c *cache[V]) disposeCandidate(a *fillCandidate[V]) {
	e := a.e
	var zero V
	e.value = zero
	e.key = ""
	s := c.shards[a.cell.flight.shardID]
	s.pool.put(e)
	a.e = nil
	a.pending.returnOnce()
}

// matchExpected 条件安装：expectedEntryId 为空只允许当前 key 不存在（A.5）。
func matchExpected[V any](a *fillCandidate[V], fr findResult[V]) bool {
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
	var o *entry[V]
	if cur.found {
		o = cur.e
	}
	now := c.now()
	var q []*entry[V]
	// 空闲快速路径：O-only 替换在条数/字节/退休/索引/时间轮下都可行时明确选用空 Q。
	if c.fitsWithoutVictims(s, a, o) {
		q = nil
	} else {
		q = c.collectQ(s, o, now, c.cfg.SieveScanLimit)
	}
	// collectQ 可能执行生命周期清理，复核旧身份与占用。
	if !matchExpected(a, s.idx.find(a.e.key, a.e.keyHash, a.e.nsID)) {
		return nil, fillRejectedStale
	}
	snap, av, oldLoss, reason := c.freezeInput(s, a, o, q)
	if snap == nil {
		if reason == fillRejectedNoEstimate {
			c.metrics.fillUnavailable.Add(1)
		}
		return nil, reason
	}
	if av.mode == estMeasured {
		c.metrics.fillMeasured.Add(1)
	} else {
		c.metrics.fillPrior.Add(1)
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
	gain := av.G - av.installCost - oldLoss - selLoss
	if !allFiniteNonNegative(gain) || gain == 0 {
		return nil, fillRejectedNonPositive
	}
	s.fs.removals = removals
	s.fs.plan = fillPlan[V]{snap: snap, removals: removals, gain: gain, av: av}
	return &s.fs.plan, fillNone
}

// fitsWithoutVictims 判断无受害者（仅 O）是否可行。
func (c *cache[V]) fitsWithoutVictims(s *shard[V], a *fillCandidate[V], o *entry[V]) bool {
	var oBytes int64
	oCount := int64(0)
	if o != nil {
		oBytes = o.billedBytes
		oCount = 1
	}
	if s.residentCount.Load()+1-oCount > s.countQuota.Load() {
		return false
	}
	if s.residentBytes.Load()+a.e.billedBytes-oBytes > s.byteQuota.Load() {
		return false
	}
	if c.retiredBytes.available() < oBytes || c.retiredRecords.available() < oCount {
		return false
	}
	if s.roundActive && (s.retireWorkLeft < oCount || s.retireBytesLeft < oBytes) {
		return false
	}
	if s.wheel.rebasing || !s.wheel.schedulable(a.e.hardDeadline) {
		return false
	}
	if o != nil {
		_, ok := s.idx.installSlot(a.e.keyHash, func(x *entry[V]) bool { return x == o })
		return ok
	}
	_, ok := s.idx.installSlot(a.e.keyHash, nil)
	return ok
}

// collectQ SIEVE 有界扫描（COLLECT_Q）。hand 从尾向头推进、到头回绕；
// 命中置位者清位跳过；失效条目在退休预算允许时清理。
func (c *cache[V]) collectQ(s *shard[V], oldEntry *entry[V], t int64, scanLimit int) []*entry[V] {
	pl := &s.policy
	q := s.fs.q[:0]
	limit := scanLimit
	if pl.count < limit {
		limit = pl.count
	}
	var anchor *entry[V]
	for i := 0; i < limit; i++ {
		if pl.count == 0 {
			break
		}
		e := pl.hand
		if e == nil {
			e = pl.tail
		}
		if e == nil || e == anchor {
			break // 不因删除后队列变短而重复绕圈
		}
		pl.hand = pl.nextTowardHeadOrWrap(e)
		if !e.readable.Load() || t >= e.hardDeadline || e.nsGen != c.nsByID(e.nsID).gen.Load() {
			e.readable.Store(false)
			if !c.tryRetire(s, e) && anchor == nil {
				anchor = e
			}
			continue
		}
		if anchor == nil {
			anchor = e
		}
		if e == oldEntry {
			continue
		}
		if e.visited.Swap(false) {
			continue
		}
		q = append(q, e)
		if len(q) >= c.cfg.VictimLimit {
			break
		}
	}
	s.fs.q = q
	return q
}

// freezeInput 冻结决策输入（FREEZE_INPUT）。
func (c *cache[V]) freezeInput(s *shard[V], a *fillCandidate[V], o *entry[V], q []*entry[V]) (*fillSnapshot, estimate, float64, fillReason) {
	t := c.now()
	ns := c.nsByID(a.e.nsID)
	if c.state.Load() != cacheOpen || a.tok != s.currentToken(ns) {
		return nil, estimate{}, 0, fillRejectedStale
	}
	if t >= a.e.hardDeadline || t >= a.handoffDeadline {
		return nil, estimate{}, 0, fillRejectedExpired
	}
	delta := int64(c.cfg.HeatDelta)
	h, ok := s.gate.read(a.e.keyHash, bucketIndex(t, delta), bucketPhi(t, delta))
	if !ok || h < ns.cfg.Tin {
		return nil, estimate{}, 0, fillRejectedNotHot
	}
	if !c.sampleQuality(s, t) {
		return nil, estimate{}, 0, fillRejectedNoEstimate
	}
	// Freeze each namespace's prior, holder, time and model version once.
	models := &s.fs.models
	modelCount, tablesUsed := 0, 0
	defer func() {
		clear(models[:modelCount])
		for i := 0; i < tablesUsed; i++ {
			s.fs.tables[i].cfg = nil // 只保留无指针的计数缓冲，不保留旧模型。
		}
	}()
	unit := ""
	modelFor := func(ns *namespace[V]) *frozenModel[V] {
		for i := 0; i < modelCount; i++ {
			if models[i].ns == ns {
				return &models[i]
			}
		}
		holder := ns.model.Load()
		if holder == nil || holder.m == nil {
			return nil
		}
		if _, static := holder.m.(hitCountModel); !static && (t < holder.since || t-holder.since > int64(c.cfg.ModelMaxAge)) {
			return nil
		}
		model := holder.m
		unitName := modelUnit(model)
		if unitName == "" || (unit != "" && unit != unitName) {
			return nil
		}
		unit = unitName
		version := model.Version()
		if table, ok := model.(*CostTableModel); ok {
			snapshot := &s.fs.tables[modelCount]
			tablesUsed = max(tablesUsed, modelCount+1)
			if !table.snapshotOn(t, snapshot) {
				return nil
			}
			model = snapshot
		} else if snapshots, ok := model.(SnapshotCostModel); ok {
			model = snapshots.SnapshotAt(t)
		}
		if model == nil || model.Version() != version || unitName != modelUnit(model) {
			return nil
		}
		config := ns.cfg
		config.Prior = ns.prior.Load()
		models[modelCount] = frozenModel[V]{ns: ns, holder: holder, version: version, model: model, config: config}
		modelCount++
		return &models[modelCount-1]
	}
	estimateFor := func(e *entry[V]) estimate {
		model := modelFor(c.nsByID(e.nsID))
		if model == nil {
			return estimate{mode: estUnavailable}
		}
		return s.stats.estimate(estimateItem{policyHash: policyHash(e.keyHash, e.nsGen), hardDeadline: e.hardDeadline, costClass: e.costClass}, t, &model.config, model.model, &c.cfg)
	}
	av := estimateFor(a.e)
	if av.mode == estUnavailable || av.mode == estExpired {
		return nil, estimate{}, 0, fillRejectedNoEstimate
	}
	oldLoss := float64(0)
	if o != nil {
		ons := c.nsByID(o.nsID)
		if !o.readable.Load() || t >= o.hardDeadline || o.nsGen != ons.gen.Load() {
			// 已过期/失效旧条目未来收益为 0，仍计移除成本。
			oldModel := modelFor(ons)
			if oldModel == nil || oldModel.model == nil {
				return nil, estimate{}, 0, fillRejectedNoEstimate
			}
			_, _, _, k, _, ok := oldModel.model.Lookup(0, o.costClass)
			if !ok || !allFiniteNonNegative(k) || k > maxEstimateTerm {
				return nil, estimate{}, 0, fillRejectedNoEstimate
			}
			oldLoss += k
		} else {
			ov := estimateFor(o)
			if ov.mode == estUnavailable || ov.mode == estExpired {
				return nil, estimate{}, 0, fillRejectedNoEstimate
			}
			oldLoss += ov.loss()
		}
	}
	// 冻结受害者视图；失效/到期者排除（不追加新搜索）。
	vis := s.fs.vis[:0]
	liveQ := q[:0]
	for _, v := range q {
		vns := c.nsByID(v.nsID)
		if !v.readable.Load() || t >= v.hardDeadline || v.nsGen != vns.gen.Load() {
			continue
		}
		vv := estimateFor(v)
		if vv.mode == estUnavailable || vv.mode == estExpired {
			return nil, estimate{}, 0, fillRejectedNoEstimate
		}
		liveQ = append(liveQ, v)
		vis = append(vis, victimInfo{id: v.entryID, billed: v.billedBytes, loss: vv.loss(), indexSlot: v.indexSlot})
	}
	for i := range models[:modelCount] {
		m := &models[i]
		if m.ns.model.Load() != m.holder || m.ns.prior.Load() != m.config.Prior || m.holder.m.Version() != m.version {
			return nil, estimate{}, 0, fillRejectedNoEstimate
		}
	}
	if !c.sampleQuality(s, t) {
		return nil, estimate{}, 0, fillRejectedNoEstimate
	}
	q = liveQ
	var oBytes int64
	oCount := 0
	if o != nil {
		oBytes = o.billedBytes
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
		countMax:           s.countMax,
		retiredBytesAvail:  c.retiredBytes.available(),
		retiredRecordSlots: c.retiredRecords.available(),
		removalWorkLeft:    int64(c.cfg.VictimLimit + 1),
		wheelOK:            !s.wheel.rebasing && s.wheel.schedulable(a.e.hardDeadline),
		maxChecks:          c.cfg.MaxSubsetChecks,
	}
	if s.roundActive {
		snap.removalWorkLeft = min(snap.removalWorkLeft, s.retireWorkLeft)
		snap.retiredBytesAvail = min(snap.retiredBytesAvail, s.retireBytesLeft)
	}
	// 索引安装位置（候选键的固定探测域）。
	start := s.idx.probeStart(a.e.keyHash)
	installWithoutQ := false
	for j := 0; j < s.idx.probeLimit; j++ {
		slot := (start + j) & s.idx.slotMask
		p := s.idx.slots[slot].Load()
		if p == nil || p == s.idx.tomb || (o != nil && p == o) {
			installWithoutQ = true
		}
	}
	snap.installWithoutQ = installWithoutQ
	var installMask uint64
	for i, v := range q {
		if (v.indexSlot-start)&s.idx.slotMask < s.idx.probeLimit {
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
	// 逐移除对象预留退休字节与记录槽位；任一失败归还全部。
	for _, e := range plan.removals {
		if !c.prepareRetirement(s, e) {
			c.rollbackLeases(plan.removals)
			return nil, fillRejectedResourceLimit
		}
	}
	slot, ok := s.idx.installSlot(a.e.keyHash, func(x *entry[V]) bool {
		for _, e := range plan.removals {
			if x == e {
				return true
			}
		}
		return false
	})
	if !ok {
		c.rollbackLeases(plan.removals)
		return nil, fillRejectedInfeasible
	}
	// CHECKED_NEXT：单调生成不重复身份。
	if s.entrySeq >= 1<<48-1 {
		c.rollbackLeases(plan.removals)
		c.stopAdmission()
		return nil, fillRejectedResourceLimit
	}
	s.entrySeq++
	a.e.entryID = uint64(s.id)<<48 | s.entrySeq
	s.fs.prep = prepared[V]{plan: plan, slot: slot}
	return &s.fs.prep, fillNone
}

// rollbackLeases 归还准备阶段预留的退休凭证。
func (c *cache[V]) rollbackLeases(removals []*entry[V]) {
	for _, e := range removals {
		if r := e.retirement; r != nil {
			r.releaseLease()
			e.retirement = nil
			c.retiredPool.put(r)
		}
	}
}

// commitFill 条件提交（COMMIT_FILL）。COMMITTING 之后由维护者唯一给出终态。
func (c *cache[V]) commitFill(s *shard[V], a *fillCandidate[V], p *prepared[V]) {
	ns := c.nsByID(a.e.nsID)
	t := c.now()
	reject := func(reason fillReason) {
		c.rollbackLeases(p.plan.removals)
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
		c.rollbackLeases(p.plan.removals)
		c.disposeCandidate(a)
		c.signalPump(a.cell.flight)
		return // ALREADY_TERMINAL：未改变驻留集合
	}
	// ---- 不可逆点：此点后无用户回调、无常规失败分配 ----
	// 移除：标不可读 → 索引/策略/时间轮摘除 → 登记退休（账目转移至已预留退休凭证）。
	for _, e := range p.plan.removals {
		e.readable.Store(false)
		s.idx.remove(e)
		s.policy.remove(e)
		s.wheel.remove(e)
		s.residentCount.Add(-1)
		s.residentBytes.Add(-e.billedBytes)
	}
	// 移除后复核外部状态：命名空间失效/TTL 过期不能发布旧代际。
	outcome := fillPublished
	if c.state.Load() != cacheOpen || a.tok != s.currentToken(ns) || c.now() >= a.e.hardDeadline {
		outcome = fillRejectedStale
	}
	installed := outcome == fillPublished
	if installed {
		e := a.e
		s.residentCount.Add(1)
		s.residentBytes.Add(e.billedBytes)
		// pending → resident 账目转移：新账目（owner 计数）先成立，再归还 pending 凭证。
		a.pending.returnOnce()
		now := c.now()
		e.graceUntil = now + int64(c.cfg.Grace)
		e.lowSince = 0
		s.policy.pushHead(e)
		s.wheel.add(e)
		s.idx.publish(e, p.slot)
		// 发布与失效可能交错：读取代际/TTL 检查是最终保障。
		if e.nsGen != ns.gen.Load() || c.now() >= e.hardDeadline || c.state.Load() != cacheOpen {
			e.readable.Store(false)
			outcome = fillRejectedStale
		} else {
			c.metrics.fillPublished.Add(1)
			s.fillGain.Store(math.Float64bits(math.Min(math.MaxFloat64, math.Float64frombits(s.fillGain.Load())+p.plan.gain)))
		}
	}
	// 退休登记（每个移除对象一条记录）。
	for _, e := range p.plan.removals {
		c.registerRetired(s, e)
	}
	if !installed {
		// 提交中断：未发布对象继续计 pending 直到真实释放。
		c.disposeCandidate(a)
	}
	a.cell.phase.Store(cellTerminalBase + int32(outcome))
	if outcome != fillPublished {
		c.metrics.fillRejected[outcome].Add(1)
	}
	c.signalPump(a.cell.flight)
}
