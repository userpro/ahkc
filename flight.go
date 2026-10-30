package ahkc

import (
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
)

// Flight：受限回源合并状态机。
//
// 同一 (命名空间, 完整 key, namespaceGen, fillFence) 最多一个实际活动 leader（I7）。
// Flight 使用条带短临界区管理注册/加入；真实完成通过一次性邮箱交接，
// 完成路径可自行结束，不依赖填充队列有空位。

const (
	phaseLoading int32 = iota
	phaseCompleting
	phaseCompletedPending
	phaseTimedOutIORunning
	phaseTerminal
)

type outcomeKind uint8

const (
	outcomeLoaded outcomeKind = iota
	outcomeCached
	outcomeError        // 无 payload
	outcomeErrorPayload // 带 payload 的错误
)

// sourceOutcome 是一次性完成邮箱的内容。
type sourceOutcome[V any] struct {
	kind            outcomeKind
	value           V
	err             error
	realCompletedAt int64
	cached          *Handle[V]
	billedBytes     int64
	hardDeadline    int64
	cost            float32
	cacheable       bool
}

// flight 是一条回源合并记录。
type flight[V any] struct {
	key          string
	keyHash      uint64
	nsID         uint32
	shadowHit    bool // 影子缓存：leader 已虚拟命中，真实缓存不会加载，故不填充。shortcut: follower 沿用 leader 的判定，条目恰在飞行中过期时少填一次，无害（下次请求重填）
	shardID      int
	stripeID     int
	registered   atomic.Bool
	flightID     uint64
	tok          token
	expected     uint64 // expectedEntryId；0 表示 NONE（只允许首次发布）
	loadDeadline int64
	validityBase int64

	// 以下由条带 gate 保护；phase 允许原子读。
	phase                 int32 // atomic 访问
	sourcePending         atomic.Bool
	sourceStarted         atomic.Bool
	sourceCallbackPending atomic.Bool
	dlv                   *delivery[V]
	result                delivery[V] // 结果本来就通过响应凭证保留 flight；内嵌避免单独分配。
	fillCell              *fillCell[V]
	reuseDeadline         int64
	waiters               []*Future[V]  // 上限 maxWaitersPerKey；起始借用 waiterBuf，超出才增长
	waiterBuf             [2]*Future[V] // 内嵌的首批等待者存储：常见的单 / 双等待者 flight 无需额外分配
	capturedBuf           [2]*Future[V] // 完成通知快照的内嵌存储
	waiterScan            int

	ioReceipt   *receipt // 指向 ioRc；真实 I/O 结束后归还
	respReceipt *receipt // 指向 respRc（责任移交 delivery 后置 nil）
	ioRc        receipt  // 内嵌凭证存储：随 flight 一次分配
	respRc      receipt

	outcome         atomic.Pointer[sourceOutcome[V]] // 一次性完成邮箱
	sourceCompleted atomic.Bool

	pumpNeeded      atomic.Bool
	pumpResignalled atomic.Bool
	pumpQueued      atomic.Bool
	pumpFn          func() // 惰性创建一次的泵任务；仅在持有 pumpQueued 时读写
	completionOwner atomic.Bool
	work            *flightWork[V] // 仅 completionOwner 修改；非 nil 时指向 workSlot
	workSlot        flightWork[V]  // 内嵌的工作槽：每条 flight 同一时刻至多一个工作记录，无需逐阶段分配
	orphan          bool           // completionOwner: gauge responsibility until late-source cleanup
}

func (f *flight[V]) getPhase() int32  { return atomic.LoadInt32(&f.phase) }
func (f *flight[V]) setPhase(p int32) { atomic.StoreInt32(&f.phase, p) }

// matches 判定同一身份：命名空间、完整 key、版本令牌缺一不可。
// 不同命名空间的 gen 都从 1 开始，仅比较 token 会把 nsA/gen1 与 nsB/gen1 错误合并。
func (f *flight[V]) matches(key string, nsID uint32, tok token) bool {
	return f.key == key && f.nsID == nsID && f.tok == tok
}

// flightWork 是固定工作槽：阶段只由 completionOwner 推进，每阶段一次性向前。
type flightWork[V any] struct {
	stage         int
	afterNotify   int
	captured      []*Future[V]
	notifyCursor  int
	inlineErr     error
	reuseDeadline int64
	candidate     *fillCandidate[V]
	lateSource    *sourceOutcome[V]
}

const (
	wkNotify = iota
	wkCheckLateSource
	wkPrepareFill
	wkAttachCell
	wkEnqueue
	wkSetTerminal
	wkSetPendingOrTerminal
	wkDisposeLate
)

const (
	pumpStepsPerRun = 4  // PUMP_STEPS_PER_RUN
	notifyBatch     = 32 // NOTIFY_BATCH
	flightRetries   = 2  // FLIGHT_RETRIES
	tokenRetries    = 2  // TOKEN_RETRIES
)

// flightStripe 是 Flight 条带：gate + 固定槽位。
type flightStripe[V any] struct {
	mu          sync.Mutex // 仅 TryLock
	slots       []*flight[V]
	wakeSlots   []atomic.Pointer[flight[V]]
	wakePending atomic.Bool
	c           *cache[V]
}

func (s *flightStripe[V]) init(size int, c *cache[V]) {
	s.slots = make([]*flight[V], size)
	s.wakeSlots = make([]atomic.Pointer[flight[V]], size)
	s.c = c
}

// tryLock 实现 TRY_GATE（有限次非阻塞尝试）。
func (s *flightStripe[V]) tryLock() bool {
	for i := 0; i < flightRetries; i++ {
		if s.mu.TryLock() {
			return true
		}
	}
	return false
}

func (s *flightStripe[V]) unlock() {
	s.mu.Unlock()
	if s.c == nil || !s.wakePending.Swap(false) {
		return
	}
	// Only bounded scheduling happens after releasing the gate. A completion
	// that lost a gate race gets a new signal from the actual unlocking owner.
	for i := range s.wakeSlots {
		if f := s.wakeSlots[i].Load(); f != nil && f.pumpNeeded.Load() {
			f.pumpResignalled.Store(true)
			s.c.ensurePumpTask(f)
		}
	}
}

// find 在条带内查找同 (命名空间, key, token) 的活动 flight。
func (s *flightStripe[V]) find(key string, nsID uint32, tok token) *flight[V] {
	for _, f := range s.slots {
		if f != nil && f.matches(key, nsID, tok) {
			return f
		}
	}
	return nil
}

// ---- 主流程（A.6 GET_OR_LOAD）----

// readStableToken 双重读取令牌与当前条目身份（READ_STABLE_TOKEN_AND_EXPECTED）。
func (c *cache[V]) readStableToken(r *Request[V]) (token, uint64, error) {
	for i := 0; i < tokenRetries; i++ {
		t1 := token{r.ns.gen.Load(), r.sh.fillFence.Load()}
		g, ok := r.sh.epoch.pin()
		if !ok {
			return token{}, 0, ErrBusy
		}
		fr := r.sh.idx.find(r.key, r.keyHash, r.ns.id)
		var expected uint64
		if fr.found {
			expected = fr.e.entryID
		}
		g.unpin()
		t2 := token{r.ns.gen.Load(), r.sh.fillFence.Load()}
		if t1 == t2 {
			return t1, expected, nil
		}
	}
	return token{}, 0, ErrBusy
}

// getOrLoad 是 miss 后的加载合并入口。返回首个 waiter 的 Future。
func (c *cache[V]) getOrLoad(r *Request[V], loader Loader[V]) (future *Future[V], err error) {
	if loader == nil {
		return nil, ErrNilLoader
	}
	c.admissions.Add(1)
	defer func() {
		c.admissions.Add(-1)
		if err == ErrOverloaded {
			c.metrics.overloaded.Add(1)
		}
	}()
	if c.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	now := c.now()
	tok, expected, err := c.readStableToken(r)
	if err != nil {
		return nil, err
	}
	stripeID := int(mix64(r.keyHash) % uint64(len(r.sh.flightStripes)))
	stripe := &r.sh.flightStripes[stripeID]
	if !stripe.tryLock() {
		return nil, ErrBusy
	}
	if c.state.Load() != cacheOpen {
		stripe.unlock()
		return nil, ErrClosed
	}
	if f := stripe.find(r.key, r.ns.id, tok); f != nil {
		// 已有同身份 flight：按阶段加入或复用。
		phase := f.getPhase()
		reusable := phase == phaseCompletedPending &&
			(f.fillCell == nil || !f.fillCell.isTerminal()) &&
			tok == r.sh.currentToken(r.ns) && now < f.reuseDeadline
		if phase != phaseLoading && !reusable {
			stripe.unlock()
			c.signalPump(f)
			return nil, ErrBusy
		}
		w := c.tryAttachWaiterLocked(r, f)
		if w == nil {
			stripe.unlock()
			return nil, ErrOverloaded
		}
		if reusable {
			w.bindReady(f.dlv)
		}
		mergedKey := f.key // 注销会清空 f.key；持锁保留自有 key 副本。
		stripe.unlock()
		c.metrics.loadMerged.Add(1)
		c.observeMerge(r.sh, r.ns.id, r.keyHash, mergedKey, now)
		return w, nil
	}
	// 创建 leader：先取得活动记录、真实 I/O、响应交付与首个 waiter 的额度。
	freeSlot := -1
	for i := range stripe.slots {
		if stripe.slots[i] == nil {
			freeSlot = i
			break
		}
	}
	if freeSlot < 0 {
		stripe.unlock()
		return nil, ErrOverloaded
	}
	// 两段式预留：先在栈上按固定顺序取得全部额度（任一失败归还已得者），受理后再落到堆对象。
	var ioRc, waitRc, respRc receipt
	fr := c.ioBudget.tryReserveInto(&ioRc, 1)
	if fr == failOK {
		if fr = c.waiterBudget.tryReserveInto(&waitRc, 1); fr == failOK {
			if fr = c.responseBytes.tryReserveInto(&respRc, c.cfg.MaxResultBytes); fr != failOK {
				waitRc.returnOnce()
				ioRc.returnOnce()
			}
		} else {
			ioRc.returnOnce()
		}
	}
	if fr != failOK {
		stripe.unlock()
		return nil, ErrOverloaded
	}
	flightID, ok := checkedNext(&c.flightSeq)
	if !ok {
		respRc.returnOnce()
		waitRc.returnOnce()
		ioRc.returnOnce()
		stripe.unlock()
		if c.flightSeq.Load() == ^uint64(0) {
			c.stopAdmission()
			return nil, ErrExhausted
		}
		return nil, ErrBusy
	}
	f := &flight[V]{
		key: strings.Clone(r.key), keyHash: r.keyHash, nsID: r.ns.id, shardID: r.sh.id, stripeID: stripeID,
		flightID: flightID,
		tok:      tok, expected: expected, shadowHit: r.shadowHit,
		loadDeadline:  now + int64(c.cfg.Limits.LoadTimeout),
		validityBase:  now, // 保守 TTL 基准：不晚于 loader 开始
		phase:         phaseLoading,
		reuseDeadline: 0,
	}
	ioRc.moveTo(&f.ioRc)
	respRc.moveTo(&f.respRc)
	f.ioReceipt, f.respReceipt = &f.ioRc, &f.respRc
	f.sourcePending.Store(true)
	w := newFuture[V](f.loadDeadline, &waitRc)
	f.waiters = append(f.waiterBuf[:0], w)
	stripe.slots[freeSlot] = f
	f.registered.Store(true)
	stripe.wakeSlots[freeSlot].Store(f)
	r.sh.flightCount.Add(1)
	stripe.unlock()

	// leader 注册后二次检查缓存与令牌（无额外外部计数）。
	rr := c.readOwned(r)
	if rr.status == readHit {
		// 已有有效值：交付缓存命中句柄，释放未使用的加载额度。
		// 字节布局的视图在句柄释放后失效：先拷出再立即释放，交付结果不持有指向块内的视图。
		v := c.copyOut(rr.handle.e)
		rr.handle.Release()
		oc := &sourceOutcome[V]{kind: outcomeCached, value: v, realCompletedAt: c.now()}
		c.finishSource(f, oc)
		return w, nil
	}
	if rr.status != readMiss || tok != r.sh.currentToken(r.ns) || c.now() >= f.loadDeadline {
		c.finishSource(f, &sourceOutcome[V]{kind: outcomeError, err: ErrBusy, realCompletedAt: c.now()})
		return w, nil
	}
	// 确认 miss：在宿主执行资源上调用 loader（不持有任何临界区）。
	f.sourceCallbackPending.Store(true)
	if !c.trySubmit(func() {
		defer func() {
			f.sourceCallbackPending.Store(false)
			c.signalPump(f)
			c.tryPumpFlight(f)
		}()
		c.runLoader(f, loader)
	}) {
		f.sourceCallbackPending.Store(false)
		c.finishSource(f, &sourceOutcome[V]{kind: outcomeError, err: ErrBusy, realCompletedAt: c.now()})
	}
	return w, nil
}

// runLoader 执行真实加载；完成写入一次性邮箱（完成回调不依赖填充队列空位）。
// loader panic 按加载契约错误处理：终结为错误结果并归还额度，不允许泄漏 I/O 凭证（I7/I12）。
func (c *cache[V]) runLoader(f *flight[V], loader Loader[V]) {
	oc := &f.result.outcome // 独占尚未发布的邮箱，避免临时结果扩大加载栈帧。
	now := c.now()
	if c.state.Load() != cacheOpen || now >= f.loadDeadline || f.getPhase() != phaseLoading {
		oc.kind, oc.err, oc.realCompletedAt = outcomeError, ErrTimeout, c.now()
		c.finishSource(f, oc)
		return // accepted but not dispatched before its cache-side deadline
	}
	f.sourceStarted.Store(true)
	c.metrics.loads.Add(1)
	c.observeLoad(f, now)
	ns := c.nsByID(f.nsID)
	completed := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				c.metrics.loadErrs.Add(1)
				oc.kind = outcomeErrorPayload
				var zero V
				oc.value = zero
				oc.err = &PanicError{Value: rec}
			}
		}()
		v, err := loader(context.Background(), f.key)
		oc.realCompletedAt, completed = c.now(), true
		if err != nil {
			c.metrics.loadErrs.Add(1)
			oc.kind = outcomeErrorPayload
			oc.err = err
			return
		}
		oc.kind = outcomeLoaded
		oc.value = v
		oc.billedBytes = c.weigh(f.key, v)
		if oc.billedBytes <= 0 || oc.billedBytes > c.cfg.MaxResultBytes {
			c.metrics.resultTooLarge.Add(1)
			c.metrics.loadErrs.Add(1)
			oc.kind, oc.err = outcomeError, ErrResultTooLarge
			if oc.billedBytes <= 0 {
				oc.err = ErrInvalidWeight
			}
			var zero V
			oc.value = zero
			return
		}
		f.respRc.shrinkTo(oc.billedBytes) // 创建时按 MaxResultBytes 预留；结果已知后只占实际大小
		oc.hardDeadline = f.validityBase + int64(ns.cfg.TTL)
		oc.cost = 1
		oc.cacheable = true
		if c.metadata != nil {
			metadata := c.metadata(f.key, v)
			oc.cacheable = !metadata.NoCache && metadata.HardDeadline >= 0
			if metadata.HardDeadline > 0 && metadata.HardDeadline < oc.hardDeadline {
				oc.hardDeadline = metadata.HardDeadline
			}
			if metadata.Cost != 0 {
				if c := metadata.Cost; !(c > 0) || math.IsInf(float64(c), 0) {
					oc.cacheable = false // 非法成本不入缓存，避免毒化收益比较
				} else {
					oc.cost = c
				}
			}
		}
	}()
	if !completed {
		oc.realCompletedAt = c.now()
	}
	c.finishSource(f, oc)
}

// finishSource 真实 I/O 完成（FINISH_SOURCE）：只结算一次（I7）。
func (c *cache[V]) finishSource(f *flight[V], oc *sourceOutcome[V]) {
	if !f.sourceCompleted.CompareAndSwap(false, true) {
		return // DUPLICATE_COMPLETION
	}
	if oc != &f.result.outcome {
		f.result.outcome = *oc
		*oc = sourceOutcome[V]{} // 发布前转移责任，不让源回调继续保留 payload。
	}
	f.outcome.Store(&f.result.outcome)
	// 只有真实结束（或确认未启动）的源操作才归还 I/O 额度；超时路径不归因。
	f.ioReceipt.returnOnce()
	f.sourcePending.Store(false)
	c.signalPump(f)
	c.tryPumpFlight(f)
}

// tryAttachWaiterLocked 在条带 gate 内加入等待者（TRY_ATTACH_WAITER）。
func (c *cache[V]) tryAttachWaiterLocked(r *Request[V], f *flight[V]) *Future[V] {
	// 清理已终结 waiter 的注册槽。
	dst := 0
	for _, w := range f.waiters {
		s := w.state.Load()
		if s == waiterTaken || s == waiterCancelled {
			continue
		}
		f.waiters[dst] = w
		dst++
	}
	f.waiters = f.waiters[:dst]
	if len(f.waiters) >= c.cfg.Limits.MaxWaitersPerKey {
		return nil
	}
	w, fr := newFutureReserving[V](&c.waiterBudget, f.loadDeadline)
	if fr != failOK {
		return nil
	}
	f.waiters = append(f.waiters, w)
	return w
}

// ---- 完成推进泵（TRY_PUMP_FLIGHT）----

func (c *cache[V]) signalPump(f *flight[V]) {
	if !f.registered.Load() {
		return
	}
	f.pumpNeeded.Store(true)
	f.pumpResignalled.Store(true)
	c.shards[f.shardID].flightStripes[f.stripeID].wakePending.Store(true)
	c.ensurePumpTask(f)
}

func (c *cache[V]) ensurePumpTask(f *flight[V]) {
	if !f.registered.Load() || !f.pumpQueued.CompareAndSwap(false, true) {
		return
	}
	// 泵任务闭包按 flight 惰性建一次：pumpQueued 的 CAS 保证同一时刻只有一个提交者在此读写 pumpFn。
	if f.pumpFn == nil {
		f.pumpFn = func() {
			f.pumpResignalled.Store(false)
			progressed := c.tryPumpFlight(f)
			f.pumpQueued.Store(false)
			// The registry slot also bounds accepted pump callbacks. Keep it until
			// this queued callback has run, then allow terminal metadata to leave.
			if f.getPhase() == phaseTerminal {
				c.tryPumpFlight(f)
			}
			if (progressed || f.pumpResignalled.Load()) && f.pumpNeeded.Load() {
				c.ensurePumpTask(f)
			}
		}
	}
	if !c.trySubmit(f.pumpFn) {
		f.pumpQueued.Store(false) // readiness remains set for the periodic retry
	}
}

func (c *cache[V]) tryPumpFlight(f *flight[V]) bool {
	if !f.completionOwner.CompareAndSwap(false, true) {
		return false
	}
	progressed := false
	s := c.shards[f.shardID]
	stripe := &s.flightStripes[f.stripeID]
	for step := 0; step < pumpStepsPerRun; step++ {
		if f.work == nil {
			stripe.wakePending.Store(true) // publish the waiter before trying the gate
			if !stripe.tryLock() {
				// 未能进入临界区：不领取信号（pumpNeeded 保持 true），
				// 尾部会重试；否则此处会丢失唤醒（等待者/清理永不被推进）。
				break
			}
			// 领取信号必须在成功进入临界区之后、观察状态之前：
			// 发送方先改状态再置位，顺序一致的原子操作保证
			// 早于本次清除的信号其状态已被下方 pick 观察，晚于者保持置位。
			f.pumpNeeded.Store(false)
			c.pickFlightWorkLocked(f, s)
			stripe.unlock()
			if f.work == nil {
				break
			}
			progressed = true
		}
		w := f.work
		switch w.stage {
		case wkNotify:
			end := w.notifyCursor + notifyBatch
			if end > len(w.captured) {
				end = len(w.captured)
			}
			for ; w.notifyCursor < end; w.notifyCursor++ {
				wt := w.captured[w.notifyCursor]
				if w.inlineErr != nil {
					wt.readyInline(w.inlineErr)
				} else {
					wt.bindReady(f.dlv)
				}
			}
			if w.notifyCursor >= len(w.captured) {
				w.stage = w.afterNotify
			}
		case wkCheckLateSource:
			c.workCheckLateSource(f, w)
		case wkPrepareFill:
			cand, reason := c.tryPrepareCandidate(f)
			w.candidate = cand
			if cand == nil {
				c.metrics.fillRejected[reason].Add(1)
				w.stage = wkSetTerminal
			} else {
				w.stage = wkAttachCell
			}
		case wkAttachCell:
			stripe.wakePending.Store(true)
			if !stripe.tryLock() {
				goto out
			}
			f.fillCell = &w.candidate.cell
			stripe.unlock()
			w.stage = wkEnqueue
		case wkEnqueue:
			select {
			case s.fillQueue <- w.candidate:
				c.signalMaintenance(s)
			default:
				c.rejectCandidate(w.candidate, fillRejectedQueueFull)
			}
			w.candidate = nil // handoff transfers payload ownership to the queue or cleanup
			w.stage = wkSetPendingOrTerminal
		case wkSetTerminal, wkSetPendingOrTerminal:
			stripe.wakePending.Store(true)
			if !stripe.tryLock() {
				goto out
			}
			terminal := w.stage == wkSetTerminal || f.fillCell == nil || f.fillCell.isTerminal()
			if terminal {
				f.setPhase(phaseTerminal)
				c.clearBridgeRefLocked(f)
			} else {
				f.setPhase(phaseCompletedPending)
				f.reuseDeadline = w.reuseDeadline
			}
			f.work = nil
			c.removeFlightIfSettledLocked(f, s)
			stripe.unlock()
		case wkDisposeLate:
			// 迟到源结果：释放 payload 引用与响应预留（有界状态更新）。
			if w.lateSource != nil && w.lateSource.cached != nil {
				w.lateSource.cached.Release()
			}
			if w.lateSource != nil {
				*w.lateSource = sourceOutcome[V]{}
			}
			w.lateSource = nil
			if f.respReceipt != nil {
				f.respReceipt.returnOnce()
				f.respReceipt = nil
			}
			w.stage = wkSetTerminal
		}
		if f.work == nil {
			progressed = true
			continue
		}
		progressed = true
	}
out:
	needRetry := f.work != nil || f.pumpNeeded.Load()
	if needRetry {
		f.pumpNeeded.Store(true)
	}
	f.completionOwner.Store(false)
	if progressed && f.pumpNeeded.Load() {
		c.ensurePumpTask(f)
	}
	return progressed
}

// pickFlightWorkLocked 在 gate 内建立下一工作记录（PICK_FLIGHT_WORK_UNDER_GATE）。
func (c *cache[V]) pickFlightWorkLocked(f *flight[V], s *shard[V]) {
	if f.work != nil {
		return
	}
	now := c.now()
	phase := f.getPhase()
	if phase == phaseLoading && (now >= f.loadDeadline || c.state.Load() != cacheOpen) {
		// 超时/关闭：已等待请求终结，同身份新请求 BUSY；底层 I/O 结束后自行清理孤儿。
		err := error(ErrTimeout)
		if c.state.Load() != cacheOpen {
			err = ErrClosed
		} else {
			c.metrics.loadTimeout.Add(1)
			if f.sourcePending.Load() && f.sourceStarted.Load() {
				f.orphan = true
				c.metrics.orphanLoads.Add(1)
			}
		}
		f.setPhase(phaseTimedOutIORunning)
		f.setWork(flightWork[V]{
			stage:       wkNotify,
			afterNotify: wkCheckLateSource,
			captured:    f.snapshotWaiters(),
			inlineErr:   err,
		})
		return
	}
	if phase == phaseTimedOutIORunning {
		// 孤儿加载：底层真正结束后清理记录，结果不再填充（A.6 TIMED_OUT_IO_RUNNING）。
		// sourcePending 或邮箱未就绪时保持孤儿状态，等待真实完成邮箱。
		if !f.sourcePending.Load() && f.outcome.Load() != nil {
			f.setWork(flightWork[V]{stage: wkCheckLateSource})
		}
		c.removeFlightIfSettledLocked(f, s)
		return
	}
	if phase == phaseLoading || phase == phaseCompleting {
		if !f.sourcePending.Load() {
			oc := f.outcome.Swap(nil)
			if oc == nil {
				return
			}
			if phase == phaseCompleting {
				return // 邮箱已消费过，工作记录应已存在
			}
			if oc.kind == outcomeError {
				err := oc.err
				*oc = sourceOutcome[V]{}
				// 无 payload 错误：释放空响应预留。
				if f.respReceipt != nil {
					f.respReceipt.returnOnce()
					f.respReceipt = nil
				}
				f.setPhase(phaseCompleting)
				f.setWork(flightWork[V]{
					stage:       wkNotify,
					afterNotify: wkSetTerminal,
					captured:    f.snapshotWaiters(),
					inlineErr:   err,
				})
				return
			}
			// 成功或带 payload：构建交付（响应预留在创建 flight 时已取得）。
			d := &f.result
			d.respReceipt = f.respReceipt
			d.internalRefs.Store(1) // Flight bridge 持有
			f.respReceipt = nil     // 责任移交 delivery
			f.dlv = d
			f.setPhase(phaseCompleting)
			if d.outcome.kind == outcomeCached || d.outcome.kind == outcomeErrorPayload {
				f.setWork(flightWork[V]{
					stage:       wkNotify,
					afterNotify: wkSetTerminal,
					captured:    f.snapshotWaiters(),
				})
				return
			}
			hardDeadline := d.outcome.hardDeadline
			deadline := hardDeadline
			if hd := d.outcome.realCompletedAt + int64(c.cfg.Limits.HandoffTimeout); hd < deadline {
				deadline = hd
			}
			f.setWork(flightWork[V]{
				stage:         wkNotify,
				afterNotify:   wkPrepareFill,
				captured:      f.snapshotWaiters(),
				reuseDeadline: deadline,
			})
			return
		}
	}
	if phase == phaseCompletedPending {
		if f.fillCell == nil || f.fillCell.isTerminal() {
			f.setPhase(phaseTerminal)
			c.clearBridgeRefLocked(f)
		} else if f.tok != s.currentToken(c.nsByID(f.nsID)) || c.state.Load() != cacheOpen {
			// 版本变化（失效/关闭）：取消仍 QUEUED 的候选；COMMITTING 不被终结。
			reason := fillRejectedStale
			if c.state.Load() != cacheOpen {
				reason = fillRejectedClosed
			}
			c.cancelFillCell(f.fillCell, reason)
			c.clearBridgeRefLocked(f)
			if f.fillCell.isTerminal() {
				f.setPhase(phaseTerminal)
			}
		} else if now >= f.reuseDeadline {
			// 交接超时：取消仍 QUEUED 的候选。
			c.cancelFillCell(f.fillCell, fillRejectedExpired)
			c.clearBridgeRefLocked(f)
			if f.fillCell.isTerminal() {
				f.setPhase(phaseTerminal)
			}
		}
	}
	c.removeFlightIfSettledLocked(f, s)
}

// setWork 把工作记录装入内嵌槽位。仅 completionOwner 调用；此时上一条记录必已结束（f.work==nil）。
func (f *flight[V]) setWork(w flightWork[V]) {
	f.workSlot = w
	f.work = &f.workSlot
}

// snapshotWaiters 冻结当前等待者列表，复用工作槽已有的缓冲容量（通知期间 waiters 仍可能变化）。
func (f *flight[V]) snapshotWaiters() []*Future[V] {
	buf := f.workSlot.captured[:0]
	if buf == nil {
		buf = f.capturedBuf[:0]
	}
	return append(buf, f.waiters...)
}

// workCheckLateSource 处理超时后源操作的真实结束（CHECK_LATE_SOURCE）。
func (c *cache[V]) workCheckLateSource(f *flight[V], w *flightWork[V]) {
	if f.sourcePending.Load() {
		f.work = nil // 等待真实完成邮箱；保留 I/O/响应责任
		return
	}
	if f.orphan {
		f.orphan = false
		c.metrics.orphanLoads.Add(^uint64(0))
	}
	oc := f.outcome.Swap(nil)
	if oc != nil && oc.kind != outcomeError {
		w.lateSource = oc
		w.stage = wkDisposeLate
		return
	}
	if oc != nil {
		*oc = sourceOutcome[V]{}
	}
	if f.respReceipt != nil {
		f.respReceipt.returnOnce()
		f.respReceipt = nil
	}
	w.stage = wkSetTerminal
}

// clearBridgeRefLocked 解除 Flight→Delivery 桥接引用（不析构 payload）。
func (c *cache[V]) clearBridgeRefLocked(f *flight[V]) {
	d := f.dlv
	f.dlv = nil
	if d != nil {
		d.dropInternalRef()
	}
}

// removeFlightIfSettledLocked 终态且无持有责任时摘除注册槽（REMOVE_FLIGHT_IF_SETTLED）。
func (c *cache[V]) removeFlightIfSettledLocked(f *flight[V], s *shard[V]) {
	if f.getPhase() != phaseTerminal || f.sourcePending.Load() || f.sourceCallbackPending.Load() || f.work != nil || f.pumpQueued.Load() {
		return
	}
	if f.fillCell != nil && !f.fillCell.isTerminal() {
		return
	}
	c.clearBridgeRefLocked(f)
	f.fillCell = nil // 拆除 Flight <-> FillCell 引用环
	stripe := &s.flightStripes[f.stripeID]
	for i := range stripe.slots {
		if stripe.slots[i] == f {
			f.registered.Store(false)
			stripe.wakeSlots[i].CompareAndSwap(f, nil)
			stripe.slots[i] = nil
			f.waiters = nil
			f.key = ""
			s.flightCount.Add(-1)
			return
		}
	}
}

// expireWaitersStep 周期扫描 waiter 期限（EXPIRE_WAITERS_STEP）。
func (c *cache[V]) expireWaitersStep(s *shard[V], quota int) {
	slots := len(s.flightStripes[0].slots)
	for step := 0; step < quota; step++ {
		cursor := s.flightScan.Add(1) - 1
		stripe := &s.flightStripes[int(cursor/uint64(slots)%uint64(len(s.flightStripes)))]
		if !stripe.tryLock() {
			continue
		}
		f := stripe.slots[int(cursor%uint64(slots))]
		if f == nil {
			stripe.unlock()
			continue
		}
		var ws [notifyBatch]*Future[V]
		n := len(f.waiters)
		if n > len(ws) {
			n = len(ws)
		}
		for j := 0; j < n; j++ {
			ws[j] = f.waiters[f.waiterScan%len(f.waiters)]
			f.waiterScan++
		}
		stripe.unlock()
		now := c.now()
		for _, w := range ws[:n] {
			if now >= w.deadline {
				w.readyInline(ErrTimeout)
			}
		}
		c.signalPump(f) // user weighers run on completion resources, outside the periodic tick
	}
}

// nsByID 按 id 取命名空间（内部）。命名空间表只追加，快照在写锁内发布，
// 因此引用到的 id 必然已在快照中；无锁读取避免维护/完成路径的 RLock 开销。
func (c *cache[V]) nsByID(id uint32) *namespace[V] {
	if s := c.nsSnap.Load(); s != nil && int(id) < len(*s) {
		return (*s)[id]
	}
	return nil
}
