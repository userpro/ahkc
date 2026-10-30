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
	ownerDead // 内存区已释放：永远不再接受维护任务
)

const pauseExecutorRejected int32 = 2

// 维护类别。
const (
	classControl = iota
	classLifecycle
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
	quota := c.cfg.Runtime.RoundWorkLimit
	deadline := c.now() + int64(c.cfg.Runtime.RoundSoftTimeBudget)
	s.roundActive = true
	s.roundDeadline = deadline
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
		s.nextAllowedRun.Store(c.now() + int64(c.cfg.Runtime.PeriodicInterval))
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
	return len(s.fillQueue) > 0 || len(s.controlQueue) > 0 || s.reclaimMore
}

// stepClass 执行一个类别的有界工作，返回完成的工作单元数。
func (c *cache[V]) stepClass(s *shard[V], cls, allowance int) int {
	switch cls {
	case classControl:
		return c.controlStep(s, allowance)
	case classLifecycle:
		return c.lifecycleStep(s, allowance)
	case classFill:
		return c.fillStep(s, allowance)
	case classReclaim:
		return c.reclaimStep(s, allowance)
	}
	return 0
}

// controlStep 处理控制操作（单 key 失效确认）。
func (c *cache[V]) controlStep(s *shard[V], allowance int) int {
	n := 0
	for n < allowance {
		select {
		case op := <-s.controlQueue:
			n++
			c.processControl(s, op)
		default:
			return n
		}
	}
	return n
}

// lifecycleStep 终结关闭时排队的候选、消费过期提示，并推进唯一的慢扫描游标。
func (c *cache[V]) lifecycleStep(s *shard[V], allowance int) int {
	if s.epoch.epoch.Load() >= math.MaxUint64-2 {
		c.stopAdmission()
	}
	work := 0
	closing := s.closeRequested.Load()
	if closing {
		work += c.cancelQueuedFills(s, allowance)
	}
	work += c.consumeHints(s)
	// 额外扫描量不计入类别配额，避免挤占其他维护类别。
	return min(work+c.sweepStep(s, max(allowance-work, 0), closing), allowance)
}

// retireStale 在条目不可读、代际失效或过期时退休它；仍有效则不动。
func (c *cache[V]) retireStale(s *shard[V], e *entry, now int64) {
	if !e.isReadable() || !e.genIs(c.nsByID(e.nsID).gen.Load()) || now >= e.hardDeadline {
		e.setReadable(false)
		c.tryRetire(s, e) // 无退休额度时仍留在索引，下次扫描重试
	}
}

// consumeHints 取出读者登记的失效提示并按语义复核后退休（提示只存索引槽号，
// 槽内当前条目是否失效以复核为准，因此条目被复用也不会误删）。
func (c *cache[V]) consumeHints(s *shard[V]) int {
	work := 0
	for i := range s.hints {
		if s.hints[i].Load() == 0 {
			continue
		}
		h := s.hints[i].Swap(0)
		if h == 0 {
			continue
		}
		work++
		c.metrics.expiredHints.Add(1)
		if e := s.idx.slotEntry(int(h - 1)); e != nil {
			c.retireStale(s, e, c.now())
		}
	}
	return work
}

// sweepStep 是唯一的慢扫描游标：处理过期、命名空间代际失效与关闭。
// 非关闭时扫描量严格按流逝时间折算，使整张索引约在 SweepPeriod 内扫完一遍：维护被填充等事件
// 频繁唤醒时不会多扫（allowance 只决定超出配额后是否检查软预算，不是扫描下限）；
// 折算不足一个槽的时间留在 sweepAt 里继续累计，不会丢。关闭时不受周期限制。
// 超出 allowance 的部分只在软时间预算内继续，每 256 槽检查一次。
//
// shortcut: 软预算耗尽时放弃本轮欠下的扫描量而不累计，覆盖周期在高负载下会变长；
// 需要严格覆盖保证时改为累计欠账。
func (c *cache[V]) sweepStep(s *shard[V], allowance int, closing bool) int {
	now := c.now()
	n := len(s.idx.words)
	scan := n
	if !closing {
		period := int64(c.cfg.Runtime.SweepPeriod)
		elapsed := min(now-s.sweepAt, period)
		scan = min(int(float64(elapsed)/float64(period)*float64(n)), n)
		if scan == 0 {
			return 0 // 欠账不足一个槽：不推进 sweepAt，让时间继续累计
		}
		// 只前进已折算的那部分时间，余数留给下一轮
		s.sweepAt += int64(float64(scan) / float64(n) * float64(period))
		if scan == n {
			s.sweepAt = now
		}
	} else {
		s.sweepAt = now
	}
	done := 0
	for done < scan {
		if done >= allowance && done&255 == 0 && s.roundActive && c.now() >= s.roundDeadline {
			break
		}
		e := s.idx.slotEntry(s.sweepCursor)
		if s.sweepCursor++; s.sweepCursor == n {
			s.sweepCursor = 0
		}
		done++
		if e == nil {
			continue
		}
		if closing {
			e.setReadable(false)
			c.tryRetire(s, e) // 退休额度不足者留待下一圈
		} else {
			c.retireStale(s, e, now)
		}
	}
	return done
}

// fillStep 处理填充候选；每轮工作量由类别配额与软时间预算限制。
func (c *cache[V]) fillStep(s *shard[V], allowance int) int {
	work := 0
	for work < allowance {
		if work > 0 && s.roundActive && c.now() >= s.roundDeadline {
			return work
		}
		var a *fillCandidate[V]
		select {
		case a = <-s.fillQueue:
		default:
			return work
		}
		if c.state.Load() != cacheOpen {
			c.rejectCandidate(a, fillRejectedClosed)
		} else {
			c.processFill(s, a)
		}
		work++
	}
	return work
}

// cancelQueuedFills 关闭时终结排队与队首候选。
func (c *cache[V]) cancelQueuedFills(s *shard[V], allowance int) int {
	work := 0
	for work < (allowance+1)/2 {
		select {
		case a := <-s.fillQueue:
			c.cancelFillCell(&a.cell, fillRejectedClosed)
			c.disposeCandidate(a)
			work++
		default:
			return work
		}
	}
	return work
}

// processControl 处理控制操作。
func (c *cache[V]) processControl(s *shard[V], op *controlOp[V]) {
	if !op.claimed.CompareAndSwap(false, true) {
		return
	}
	defer op.release()
	switch op.kind {
	case ctlInvalidateKey:
		// 单 key 失效：先递增 fillFence（旧 fence 填充结果不得恢复），
		// 再使目标不可读，最后确认。
		if _, ok := checkedNext(&s.fillFence); !ok {
			c.stopAdmission()
			op.result.finish(ErrExhausted)
			return
		}
		fr := s.idx.find(op.key, op.keyHash, op.ns.id)
		if fr.found {
			fr.e.setReadable(false)
			c.tryRetire(s, fr.e)
		}
		c.metrics.keyInvalidations.Add(1)
		op.result.finish(nil)
	}
}

// ---- 定时触发与全局维护面（PERIODIC_TICK）----

func (c *cache[V]) tickerLoop() {
	defer c.tickerWG.Done()
	t := time.NewTicker(c.cfg.Runtime.PeriodicInterval)
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
	quota := (c.cfg.Runtime.PeriodicWorkLimit - 2*min(32, c.cfg.Runtime.RoundWorkLimit/classCount)) / 33
	if quota > len(c.shards) {
		quota = len(c.shards)
	}
	for work := 0; work < quota; work++ {
		s := c.shards[c.periodicCursor]
		c.periodicCursor = (c.periodicCursor + 1) % len(c.shards)
		// 定时触发不依赖新请求：负责空闲时的慢扫描、Flight 期限与回收进展。
		s.nextAllowedRun.Store(0) // an explicit periodic visit permits one bounded round
		c.signalMaintenance(s)
		c.expireWaitersStep(s, 32)
	}
	c.drainNsControls()
	c.ensureNsDrain() // 覆盖 drain 期间新入队的命名空间控制
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
	for work := 0; work < min(32, c.cfg.Runtime.RoundWorkLimit/classCount); work++ {
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
	if c.admissions.Load() != 0 || len(c.nsControlQueue) != 0 || c.nsCtlDraining.Load() || c.nsCtlOwner.Load() {
		return stopped, false, false
	}
	for _, s := range c.shards {
		if len(s.fillQueue) != 0 || len(s.controlQueue) != 0 || s.flightCount.Load() != 0 {
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
	if c.hotCallbackPending.Load() {
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
	if c.handleBudget.available() != c.handleBudget.limit {
		return stopped, true, false
	}
	return stopped, true, true
}

// sealShards 把全部分片原子地从 IDLE 置为 DEAD：此后没有维护任务能再启动，释放内存区才安全
// （pollClose 的检查与释放之间，定时触发可能又排入一轮，它会读到已释放的内存）。
// 任一分片忙则回滚，留给下一次 checkReclaimed；回滚期间被拒的调度由周期触发补上。
func (c *cache[V]) sealShards() bool {
	for i, s := range c.shards {
		if !s.ownerState.CompareAndSwap(ownerIdle, ownerDead) {
			for _, p := range c.shards[:i] {
				p.ownerState.Store(ownerIdle)
			}
			return false
		}
	}
	return true
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
	if !reclaimed || !c.sealShards() {
		return
	}
	if c.state.CompareAndSwap(cacheClosing, cacheReclaimed) {
		for _, s := range c.shards {
			s.release()
		}
		close(c.stopTick)
		close(c.closedCh)
	}
}
