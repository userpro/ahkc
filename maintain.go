package ahkc

import (
	"math"
	"time"
)

// 维护调度。
//
// 单维护者保证结构更新有序；分类配额保证相互不饿死；
// 有限队列之外限制每轮实际工作量；到软时间预算从未完成类别续行。

const (
	ownerIdle int32 = iota
	ownerQueued
	ownerRunning
)

const (
	pauseWheelRebase      int32 = 1
	pauseExecutorRejected int32 = 2
)

// 维护类别。
const (
	classControl = iota
	classLifecycle
	classSamples
	classFill
	classReclaim
	classCount
)

// signalMaintenance 发布待处理标记并尝试调度（SIGNAL_MAINTENANCE）。
func (c *cache[V]) signalMaintenance(s *shard[V]) {
	s.pending.Store(true)
	c.ensureMaintenanceTask(s)
}

// ensureMaintenanceTask 取得调度权（ENSURE_MAINTENANCE_TASK）。
// 调度被拒绝时不递归重试、不 caller-runs：记录 EXECUTOR_REJECTED 与 pending，
// 由定时触发有界重试。
func (c *cache[V]) ensureMaintenanceTask(s *shard[V]) {
	if c.state.Load() == cacheReclaimed || (c.state.Load() == cacheOpen && c.now() < s.nextAllowedRun.Load()) {
		return
	}
	if !s.ownerState.CompareAndSwap(ownerIdle, ownerQueued) {
		return // 正在 QUEUED/RUNNING：只记录待处理工作
	}
	if c.state.Load() == cacheReclaimed {
		s.ownerState.Store(ownerIdle)
		return
	}
	if !c.trySubmit(s.runFn) {
		s.ownerState.Store(ownerIdle)
		s.pauseReasons.Or(pauseExecutorRejected)
		s.pending.Store(true)
	}
}

// runMaintenance 执行一轮有界维护（RUN_MAINTENANCE）。
// 事件配额之外叠加软时间预算：到预算即让出，未完成类别由
// pending/周期触发续行，不无限拉长单轮尾延迟。
func (c *cache[V]) runMaintenance(s *shard[V]) {
	if !s.ownerState.CompareAndSwap(ownerQueued, ownerRunning) {
		return // 旧任务不再写驻留结构
	}
	s.pauseReasons.And(^pauseExecutorRejected)
	s.pending.Store(false)
	// 填充速率令牌补充：账目字段仅维护者修改（owner 上下文）。
	c.refillFillTokens(s)
	quota := c.cfg.RoundWorkLimit
	deadline := c.now() + int64(c.cfg.RoundSoftTimeBudget)
	s.roundActive = true
	s.roundDeadline = deadline
	s.retireWorkLeft = int64(c.cfg.RetireLimitPerRound)
	s.retireBytesLeft = c.cfg.RetireBytesPerRound
	s.reclaimBytesLeft = c.cfg.ReclaimBytesPerRound
	softExpired := func() bool { return c.now() >= deadline }
	perClass := quota / classCount
	if perClass < 1 {
		perClass = 1
	}
	work := 0
	for visited := 0; visited < classCount && work < quota; visited++ {
		if work > 0 && softExpired() {
			break
		}
		cls := s.resumeClass
		work += c.stepClass(s, cls, min(perClass, quota-work))
		s.resumeClass = (cls + 1) % classCount
	}
	// Read owner-only continuation state before giving up structural ownership.
	immediate := c.immediateWorkRemains(s)
	s.roundActive = false
	// 配额 / 软预算用尽且仍有可立即推进的工作时直接续行（A.9：完成配额后有待续工作则安排下一轮）；
	// 只剩等待时间 / 读者的工作才按周期节流，避免输入信号把空转的维护者反复唤醒。
	if (work >= quota || softExpired()) && !immediate {
		s.nextAllowedRun.Store(c.now() + int64(c.cfg.PeriodicInterval))
	}
	s.ownerState.Store(ownerIdle)
	// 退出至 IDLE 后重查输入，覆盖 enqueue 与退出交叉（A.9）：
	// 有待续工作则安排下一轮；仅等待读者/时间推进的工作不立即重调度（由周期触发补偿）。
	if s.pending.Load() || immediate {
		c.ensureMaintenanceTask(s)
	}
	// DisableTicker 模式下宿主只驱动执行器时也能达成 RECLAIMED。
	c.checkReclaimed()
}

func (c *cache[V]) immediateWorkRemains(s *shard[V]) bool {
	fillReady := len(s.fillQueue) > 0 && s.fillTokens >= 1 && s.fillByteTokens >= float64(c.cfg.MaxEntryBytes)
	if head := s.fillHead.Load(); head != nil {
		fillReady = s.fillTokens >= 1 && s.fillByteTokens >= float64(head.e.billedBytes)
	}
	if fillReady || len(s.controlQueue) > 0 || len(s.creditQueue) > 0 || s.deferredControl.Load() != nil || s.wheel.rebasing {
		return true
	}
	for _, st := range s.stripes {
		if st.queued.Load() != 0 || st.faultFlag.Load() {
			return true
		}
	}
	return false
}

// stepClass 执行一个类别的有界工作，返回完成的工作单元数。
func (c *cache[V]) stepClass(s *shard[V], cls, allowance int) int {
	switch cls {
	case classControl:
		return c.controlStep(s, allowance)
	case classLifecycle:
		return c.lifecycleStep(s, allowance)
	case classSamples:
		return c.drainSamples(s, allowance)
	case classFill:
		return c.fillStep(s, allowance)
	case classReclaim:
		return c.reclaimStep(s, allowance)
	}
	return 0
}

// controlStep 处理控制操作：先服务失效确认，再服务额度转移。
// 两类操作使用独立队列：额度转移取不到全局 creditOwner 时只暂存自己，
// 不会让排在后面的失效确认等待一个周期（避免队头阻塞）。
func (c *cache[V]) controlStep(s *shard[V], allowance int) int {
	n := 0
	for n < allowance {
		select {
		case op := <-s.controlQueue:
			n++
			c.processControl(s, op) // 失效操作不会因竞争失败
			continue
		default:
		}
		break
	}
	for n < allowance {
		if op := s.deferredControl.Load(); op != nil {
			n++
			if !c.processControl(s, op) {
				s.nextAllowedRun.Store(c.now() + int64(c.cfg.PeriodicInterval))
				return n
			}
			s.deferredControl.Store(nil)
			continue
		}
		select {
		case op := <-s.creditQueue:
			n++
			if !c.processControl(s, op) {
				s.deferredControl.Store(op)
				s.nextAllowedRun.Store(c.now() + int64(c.cfg.PeriodicInterval))
				return n
			}
		default:
			return n
		}
	}
	return n
}

// lifecycleStep 关闭扫描 + 时间轮推进 + 固定槽冷却扫描。
func (c *cache[V]) lifecycleStep(s *shard[V], allowance int) int {
	now := c.now()
	if s.wheel.exhausted || s.epoch.epoch.Load() >= math.MaxUint64-2 {
		c.stopAdmission()
	}
	work := 0
	if s.closeRequested.Load() {
		work += c.closeSweepStep(s, allowance)
		if work >= allowance {
			return work
		}
	}
	valid := func(e *entry[V], now int64) bool {
		return e.readable.Load() && e.nsGen == c.nsByID(e.nsID).gen.Load() && now < e.hardDeadline
	}
	retire := func(e *entry[V]) bool { return c.tryRetire(s, e) }
	// Independent shares keep wheel catch-up from starving the cold cursor.
	left := allowance - work
	coldQuota := (left + 1) / 2
	wheelQuota := left - coldQuota
	if s.wheel.rebasing {
		if s.wheel.rebaseStep(s.idx, wheelQuota, now, valid, retire) {
			s.pauseReasons.And(^pauseWheelRebase)
		}
		work += wheelQuota
	} else {
		r := s.wheel.advance(now, wheelQuota, c.cfg.MaxWheelLag, valid, retire)
		if r < 0 {
			s.wheel.startRebase(now / int64(c.cfg.WheelTick))
			s.pauseReasons.Or(pauseWheelRebase)
		} else {
			work += r
		}
	}
	work += c.coldSweep(s, coldQuota)
	return work
}

// coldSweep 固定索引槽冷却扫描（COLD_SWEEP）。
func (c *cache[V]) coldSweep(s *shard[V], quota int) int {
	work := 0
	delta := int64(c.cfg.HeatDelta)
	for i := 0; i < quota; i++ {
		work++ // Empty slots are real scan work too.
		slot := s.coldCursor
		s.coldCursor = (s.coldCursor + 1) % s.idx.slotCount
		e := s.idx.slotEntry(slot)
		if e == nil {
			continue
		}
		t := c.now()
		ns := c.nsByID(e.nsID)
		switch {
		case !e.readable.Load() || e.nsGen != ns.gen.Load() || t >= e.hardDeadline:
			e.readable.Store(false)
			c.tryRetire(s, e) // 无预算仍留索引计量
		case t < e.graceUntil:
			// 宽限期内跳过
		default:
			h, ok := e.heat.read(bucketIndex(t, delta), bucketPhi(t, delta))
			if !ok {
				e.lowSince = 0
				continue
			}
			if h >= ns.cfg.Tout {
				e.lowSince = 0
				continue
			}
			if e.lowSince == 0 {
				e.lowSince = t
				continue
			}
			if t-e.lowSince >= int64(c.cfg.ColdFor) {
				t2 := c.now()
				h2, ok := e.heat.read(bucketIndex(t2, delta), bucketPhi(t2, delta))
				if !ok {
					e.lowSince = 0
					continue
				}
				if h2 < ns.cfg.Tout {
					e.readable.Store(false)
					c.tryRetire(s, e)
				} else {
					e.lowSince = 0
				}
			}
		}
	}
	return work
}

// drainSamples 汇总收益样本（DRAIN_SAMPLES）。
// 轮转经过全部条带：只有连续 len(stripes) 个条带都为空时才结束
// （TRY_POP_NEXT_SAMPLE_ROUND_ROBIN 语义），单个空条带不中断本轮。
func (c *cache[V]) drainSamples(s *shard[V], quota int) int {
	now := c.now()
	work := 0
	delta := int64(c.cfg.ValueDelta)
	emptyStreak := 0
	for work < quota {
		st := s.stripes[s.stripeDrain%len(s.stripes)]
		s.stripeDrain = (s.stripeDrain + 1) % len(s.stripes)
		work++ // empty/busy stripes and quality flags consume scan work too
		if st.faultFlag.Swap(false) {
			s.stats.lastFaultAt = now
			c.metrics.samplesDropped.Add(1)
		}
		sm, ok := st.pop()
		if !ok {
			emptyStreak++
			if emptyStreak >= len(s.stripes) {
				break // 本轮所有条带均已清空
			}
			continue
		}
		emptyStreak = 0
		// A sample can arrive after the class-start timestamp. Compare it to
		// current time after popping, retaining the same event-time bucket.
		now = c.now()
		ns := c.nsByID(sm.nsID)
		if sm.nsGen != ns.gen.Load() {
			continue // 代际不符：丢弃
		}
		if now-sm.eventTime > int64(c.cfg.MaxSampleLag) {
			s.stats.lastLagViolation = now
		}
		s.stats.addSample(sm, now, delta)
	}
	return work
}

// fillStep 处理填充候选（受填充速率令牌与暂停标记约束）。
func (c *cache[V]) fillStep(s *shard[V], allowance int) int {
	work := 0
	for work < allowance {
		if work > 0 && s.roundActive && c.now() >= s.roundDeadline {
			return work
		}
		a := s.fillHead.Load()
		if a == nil {
			if c.state.Load() == cacheOpen && s.pauseReasons.Load()&pauseWheelRebase == 0 && s.fillTokens < 1 {
				return work
			}
			select {
			case a = <-s.fillQueue:
				s.fillHead.Store(a)
			default:
				return work
			}
		}
		if c.state.Load() != cacheOpen {
			s.fillHead.Store(nil)
			c.rejectCandidate(a, fillRejectedClosed)
			work++
			continue
		}
		if s.pauseReasons.Load()&pauseWheelRebase != 0 {
			s.fillHead.Store(nil)
			c.rejectCandidate(a, fillRejectedResourceLimit)
			work++
			continue
		}
		if s.fillTokens < 1 || s.fillByteTokens < float64(a.e.billedBytes) {
			return work
		}
		// 额度不足且有积压：向协调者申请空闲额度（不驱逐其他分片热点）。
		// 条数已触及索引物理上限时额外条数额度无法使用，只有字节缺口才值得申请，避免字节额度囤积。
		countShort := s.residentCount.Load() >= s.countQuota.Load() && s.countQuota.Load() < s.countMax
		byteShort := s.residentBytes.Load()+a.e.billedBytes > s.byteQuota.Load()
		if countShort || byteShort {
			s.wantCredit.Store(true)
		}
		s.fillHead.Store(nil)
		s.fillTokens--
		s.fillByteTokens -= float64(a.e.billedBytes)
		c.processFill(s, a)
		work++
	}
	return work
}

// closeSweepStep 关闭时按配额摘除全部驻留条目并取消排队候选。
func (c *cache[V]) closeSweepStep(s *shard[V], allowance int) int {
	work := 0
	if a := s.fillHead.Swap(nil); a != nil {
		c.rejectCandidate(a, fillRejectedClosed)
		work++
	}
	for work < (allowance+1)/2 {
		select {
		case a := <-s.fillQueue:
			c.cancelFillCell(&a.cell, fillRejectedClosed)
			c.disposeCandidate(a)
			work++
		default:
			goto scan
		}
	}
scan:
	for work < allowance && s.closeCursor < s.idx.slotCount {
		e := s.idx.slotEntry(s.closeCursor)
		s.closeCursor++
		work++
		if e == nil {
			continue
		}
		e.readable.Store(false)
		c.tryRetire(s, e) // 退休预算不足留待下轮（coldSweep 覆盖不可读条目）
	}
	if s.closeCursor >= s.idx.slotCount {
		s.closeCursor = 0 // 预算不足者下轮重扫
	}
	return work
}

// processControl 处理控制操作。
func (c *cache[V]) processControl(s *shard[V], op *controlOp[V]) bool {
	if op.kind == ctlCreditTake || op.kind == ctlCreditApply {
		if !c.creditOwner.CompareAndSwap(false, true) {
			return false
		}
		defer c.creditOwner.Store(false)
	}
	if !op.claimed.CompareAndSwap(false, true) {
		return true
	}
	defer op.release()
	switch op.kind {
	case ctlInvalidateKey:
		// 单 key 失效：先递增 fillFence（旧 fence 填充结果不得恢复），
		// 再使目标不可读，最后确认。
		if _, ok := checkedNext(&s.fillFence); !ok {
			c.stopAdmission()
			op.result.finish(ErrExhausted)
			return true
		}
		fr := s.idx.find(op.key, op.keyHash, op.ns.id)
		if fr.found {
			fr.e.readable.Store(false)
			c.tryRetire(s, fr.e)
		}
		c.metrics.keyInvalidations.Add(1)
		op.result.finish(nil)
	case ctlCreditTake:
		// DONOR_TAKE_IDLE_IN_OWNER：只捐出空闲额度。
		keepN := s.countQuota.Load() / 8
		keepB := s.byteQuota.Load() / 8
		freeN := s.countQuota.Load() - s.residentCount.Load() - keepN
		freeB := s.byteQuota.Load() - s.residentBytes.Load() - keepB
		if freeN < 0 {
			freeN = 0
		}
		if freeB < 0 {
			freeB = 0
		}
		takeN := min(op.amountN, freeN)
		takeB := min(op.amountB, freeB)
		if takeN > 0 || takeB > 0 {
			s.countQuota.Add(-takeN)
			s.byteQuota.Add(-takeB)
			c.globalFreeCount.Add(takeN)
			c.globalFreeBytes.Add(takeB)
			c.metrics.creditMoved.Add(1)
		}
		op.result.finish(nil)
	case ctlCreditApply:
		// RECEIVER_APPLY_IN_OWNER：不超物理上限。
		if s.countQuota.Load()+op.amountN <= s.countMax {
			s.countQuota.Add(op.amountN)
			s.byteQuota.Add(op.amountB)
			c.metrics.creditMoved.Add(1)
		} else {
			c.globalFreeCount.Add(op.amountN)
			c.globalFreeBytes.Add(op.amountB)
		}
		s.creditPendingN -= op.amountN
		c.creditInTransitN.Add(-op.amountN)
		c.creditInTransitB.Add(-op.amountB)
		op.result.finish(nil)
	}
	return true
}

// ---- 定时触发与全局维护面（PERIODIC_TICK）----

func (c *cache[V]) tickerLoop() {
	defer c.tickerWG.Done()
	t := time.NewTicker(c.cfg.PeriodicInterval)
	defer t.Stop()
	for {
		select {
		case <-c.stopTick:
			return
		case <-t.C:
			c.periodicTick()
		}
	}
}

// refillFillTokens 补充填充速率令牌（仅维护者调用）。
func (c *cache[V]) refillFillTokens(s *shard[V]) {
	now := c.now()
	// 时钟回拨（ManualClock.Set 或宿主时钟校正）时 now < lastRefill：负时长不得倒扣令牌，
	// 否则令牌被扣成大负数，填充停滞约回拨时长。lastRefill 仍跟随时钟，之后照常补充。
	elapsed := float64(max(now-s.lastRefill, 0)) / 1e9
	s.fillTokens += elapsed * c.cfg.FillRatePerSec
	if cap := math.Max(1, c.cfg.FillRatePerSec); s.fillTokens > cap {
		s.fillTokens = cap
	}
	s.fillByteTokens += elapsed * c.cfg.FillBytesPerSec
	if cap := math.Max(float64(c.cfg.MaxEntryBytes), c.cfg.FillBytesPerSec); s.fillByteTokens > cap {
		s.fillByteTokens = cap
	}
	s.lastRefill = now
}

func (c *cache[V]) periodicTick() {
	if c.state.Load() == cacheReclaimed {
		return
	}
	if !c.tickOwner.CompareAndSwap(false, true) {
		return
	}
	defer func() { c.tickOwner.Store(false); c.checkReclaimed() }()
	if c.state.Load() == cacheReclaimed {
		return
	}
	c.rollHotWindow()
	quota := (c.cfg.PeriodicWorkLimit - 2*min(32, c.cfg.RoundWorkLimit/classCount)) / 33
	if quota > len(c.shards) {
		quota = len(c.shards)
	}
	for work := 0; work < quota; work++ {
		s := c.shards[c.periodicCursor]
		c.periodicCursor = (c.periodicCursor + 1) % len(c.shards)
		// 定时触发不依赖新请求：负责空闲时的 TTL/冷却/Flight 期限与回收进展。
		s.nextAllowedRun.Store(0) // an explicit periodic visit permits one bounded round
		c.signalMaintenance(s)
		c.expireWaitersStep(s, 32)
	}
	c.drainNsControls()
	c.ensureNsDrain() // 覆盖 drain 期间新入队的命名空间控制
	c.creditCoordinator()
	c.checkReclaimed()
}

// MaintenanceStep 供 DisableTicker 模式的宿主手动驱动一次全局维护。
func (c *Cache[V]) MaintenanceStep() {
	if c == nil || c.c == nil {
		return
	}
	c.c.periodicTick()
}

// drainNsControls 处理命名空间级控制（命名空间失效的线性化点在此）。
func (c *cache[V]) drainNsControls() {
	if !c.nsCtlOwner.CompareAndSwap(false, true) {
		return
	}
	defer c.nsCtlOwner.Store(false)
	for work := 0; work < min(32, c.cfg.RoundWorkLimit/classCount); work++ {
		select {
		case op := <-c.nsControlQueue:
			ns := op.ns
			if _, ok := checkedNext(&ns.gen); !ok {
				c.stopAdmission()
				op.result.finish(ErrExhausted)
				op.release()
				continue
			}
			c.metrics.nsInvalidations.Add(1)
			op.result.finish(nil)
			op.release()
		default:
			return
		}
	}
}

// ensureNsDrain 确保命名空间控制队列有活跃排水者（C3 修复）：
//   - TrySubmit 失败时立即复位 nsCtlDraining，避免标志永久泄漏；
//   - 排水任务收尾时重查队列，覆盖“drain 与复位之间”入队的操作；
//   - 仍然失败时由周期触发（periodicTick 直接 drainNsControls）兜底。
func (c *cache[V]) ensureNsDrain() {
	if len(c.nsControlQueue) == 0 {
		return
	}
	if !c.nsCtlDraining.CompareAndSwap(false, true) {
		return // 已有排水者；其收尾重查会覆盖本次入队
	}
	if !c.trySubmit(func() {
		c.drainNsControls()
		c.nsCtlDraining.Store(false)
		c.ensureNsDrain() // 覆盖 drain 与复位之间入队的操作
	}) {
		c.nsCtlDraining.Store(false)
	}
}

// creditCoordinator 空闲额度转移（A.9）：收票 → 捐出 → 授予。
// 不驱逐其他分片热点来满足填充；请求不等待跨分片调配。
func (c *cache[V]) creditCoordinator() {
	if c.state.Load() != cacheOpen {
		return
	}
	if !c.creditOwner.CompareAndSwap(false, true) {
		return
	}
	defer c.creditOwner.Store(false)
	// Visit a fixed number of receiver/donor records and save both cursors.
	quota := min(32, int64(c.cfg.RoundWorkLimit/classCount))
	for work := int64(0); work < quota; work++ {
		s := c.shards[c.creditCursor%len(c.shards)]
		c.creditCursor = (c.creditCursor + 1) % len(c.shards)
		if !s.wantCredit.CompareAndSwap(true, false) {
			continue
		}
		wantN := s.countMax/8 + 1
		wantB := s.byteQuota.Load()/8 + c.cfg.MaxEntryBytes
		// 先从全局空闲授予。
		gn := min(min(wantN, c.globalFreeCount.Load()), s.countMax-s.countQuota.Load()-s.creditPendingN)
		gb := min(wantB, c.globalFreeBytes.Load())
		if gn > 0 || gb > 0 {
			grantID, ok := checkedNext(&c.creditSeq)
			if !ok {
				c.stopAdmission()
				return
			}
			c.globalFreeCount.Add(-gn)
			c.globalFreeBytes.Add(-gb)
			c.creditInTransitN.Add(gn)
			c.creditInTransitB.Add(gb)
			s.creditPendingN += gn
			if !c.sendControl(s, &controlOp[V]{kind: ctlCreditApply, grantID: grantID, amountN: gn, amountB: gb}) {
				c.globalFreeCount.Add(gn)
				c.globalFreeBytes.Add(gb)
				c.creditInTransitN.Add(-gn)
				c.creditInTransitB.Add(-gb)
				s.creditPendingN -= gn
				s.wantCredit.Store(true)
			}
			continue
		}
		// 从空闲分片捐出（本轮每申请者至多一个捐出方）。
		s.wantCredit.Store(true)
		for donorWork := 0; donorWork < len(c.shards) && work+1 < quota; donorWork++ {
			work++
			d := c.shards[c.creditDonorCursor%len(c.shards)]
			c.creditDonorCursor = (c.creditDonorCursor + 1) % len(c.shards)
			if d == s {
				continue
			}
			idleN := d.countQuota.Load() - d.residentCount.Load()
			idleB := d.byteQuota.Load() - d.residentBytes.Load()
			if idleN <= d.countQuota.Load()/4 || idleB <= d.byteQuota.Load()/4 {
				continue
			}
			grantID, ok := checkedNext(&c.creditSeq)
			if !ok {
				c.stopAdmission()
				return
			}
			c.sendControl(d, &controlOp[V]{kind: ctlCreditTake, grantID: grantID, amountN: wantN, amountB: wantB})
			// 下一轮 tick 再从 globalFree 授予接收方。
			s.wantCredit.Store(true)
			break
		}
	}
}

// sendControl 投递分片控制；失败（队列满）则放弃本轮转移。
func (c *cache[V]) sendControl(s *shard[V], op *controlOp[V]) bool {
	if c.controlBudget.tryReserveInto(&op.rc, 1) != failOK {
		return false
	}
	select {
	case s.creditQueue <- op:
		c.signalMaintenance(s)
		return true
	default:
		op.release()
		return false
	}
}

// pollClose 报告关闭进展的三阶段状态（A.9 POLL_CLOSE）：
// stopped=已停止受理；workDrained=已受理控制/填充终结且真实 I/O 与响应均结束；
// reclaimed=无驻留/退休/交接残留且全部预算归还。
func (c *cache[V]) pollClose() (stopped, workDrained, reclaimed bool) {
	state := c.state.Load()
	if state == cacheReclaimed {
		return true, true, true
	}
	if state == cacheOpen {
		return false, false, false
	}
	stopped = true // 停止受理：state 离开 OPEN 即成立
	if c.admissions.Load() != 0 || len(c.nsControlQueue) != 0 || c.nsCtlDraining.Load() || c.nsCtlOwner.Load() || c.creditOwner.Load() {
		return stopped, false, false
	}
	for _, s := range c.shards {
		if len(s.fillQueue) != 0 || s.fillHead.Load() != nil || len(s.controlQueue) != 0 || len(s.creditQueue) != 0 || s.deferredControl.Load() != nil || s.flightCount.Load() != 0 {
			return stopped, false, false
		}
	}
	if c.ioBudget.available() != c.ioBudget.limit ||
		c.waiterBudget.available() != c.waiterBudget.limit ||
		c.pendingBytes.available() != c.pendingBytes.limit ||
		c.responseBytes.available() != c.responseBytes.limit {
		return stopped, false, false // 真实源未结束或响应/交接未移交
	}
	workDrained = true
	if c.controlBudget.available() != c.controlBudget.limit {
		return stopped, false, false
	}
	if c.creditInTransitN.Load() != 0 || c.creditInTransitB.Load() != 0 || c.hotCallbackPending.Load() {
		return stopped, false, false
	}
	if c.tasks.Load() != 0 || c.tickOwner.Load() {
		return stopped, true, false
	}
	for _, s := range c.shards {
		if s.residentCount.Load() != 0 || s.retiredCount.Load() != 0 || s.ownerState.Load() != ownerIdle || !s.epoch.noReaders() {
			return stopped, true, false
		}
	}
	if c.retiredBytes.available() != c.retiredBytes.limit || c.retiredRecords.available() != c.retiredRecords.limit {
		return stopped, true, false
	}
	if c.handleBudget.available() != c.handleBudget.limit {
		return stopped, true, false
	}
	return stopped, true, true
}

// checkReclaimed 达到 RECLAIMED 时关闭通道（ticker / MaintenanceStep 驱动）。
func (c *cache[V]) checkReclaimed() {
	if c.state.Load() != cacheClosing {
		return
	}
	_, drained, reclaimed := c.pollClose()
	if drained && c.drained.CompareAndSwap(false, true) {
		close(c.drainedCh)
	}
	if !reclaimed {
		return
	}
	if c.state.CompareAndSwap(cacheClosing, cacheReclaimed) {
		close(c.stopTick)
		close(c.closedCh)
	}
}
