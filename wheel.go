package ahkc

import "math"

// 有界层级时间轮。
//
// 每层固定 K 个槽，层 ℓ 步长 tick×K^ℓ；截止时间向上取整为基础 tick 编号 T，
// 按剩余 tick 数选择能覆盖的最低层，槽位 floor(T/K^ℓ) mod K。
// 处理条目先比较真实 hardDeadline：已到期则逻辑失效，未到期降层重挂；
// 禁止提前过期或挂回已处理完的槽。每条目仅一个有效 timer 成员（I10）。
// 滞后超过 maxWheelLag 触发受限重建（epoch 作废旧成员，按索引槽位配额追赶）。

type wheelBucket[V any] struct {
	epoch      uint64
	head, tail *entry[V]
}

type wheelTask struct {
	level int
	slot  int
}

type timerWheel[V any] struct {
	tick   int64
	levels int
	slots  int // K

	epoch      uint64
	cursorTick int64
	// tickInProgress：正在处理的 tick；-1 表示无。
	tickInProgress int64

	buckets [][]wheelBucket[V]

	// 当前 tick 的固定任务（层从高到低）。
	tasks      []wheelTask
	taskCursor int

	rebasing     bool
	rebaseCursor int
	exhausted    bool
}

// newTimerWheel 创建时间轮。游标从构造时刻 now 所在的 tick 开始：
// 时钟原点之后很久才创建的实例（例如进程运行数秒后）若从 0 起算，首轮维护就会因
// “滞后超过 MaxWheelLag”触发受限重建，重建完成前所有填充被拒为 RESOURCE_LIMIT。
func newTimerWheel[V any](tick int64, levels, slots int, now int64) *timerWheel[V] {
	w := &timerWheel[V]{
		tick:           tick,
		levels:         levels,
		slots:          slots,
		buckets:        make([][]wheelBucket[V], levels),
		tasks:          make([]wheelTask, 0, levels),
		cursorTick:     now / tick,
		tickInProgress: -1,
	}
	for i := range w.buckets {
		w.buckets[i] = make([]wheelBucket[V], slots)
	}
	return w
}

func (w *timerWheel[V]) levelSpan(level int) int64 {
	return powInt(int64(w.slots), level)
}

// schedulable 检查截止时间在当前基准下是否可调度（delta < K^L）。
func (w *timerWheel[V]) schedulable(hardDeadline int64) bool {
	base := w.cursorTick
	if w.tickInProgress >= 0 {
		base = w.tickInProgress
	}
	t := ceilDiv(hardDeadline, w.tick)
	delta := t - base
	if delta < 1 {
		delta = 1
	}
	return delta < powInt(int64(w.slots), w.levels)
}

// add 挂载条目 timer（WHEEL_ADD）。要求已通过 schedulable 检查。
func (w *timerWheel[V]) add(e *entry[V]) {
	if e.timerMember && e.timerEpoch == w.epoch {
		w.remove(e) // 防御：同 epoch 重复挂载先摘除
	}
	t := ceilDiv(e.hardDeadline, w.tick)
	base := w.cursorTick
	if w.tickInProgress >= 0 {
		base = w.tickInProgress
	}
	delta := t - base
	if delta < 1 {
		delta = 1
	}
	level := w.levels - 1
	for l := 0; l < w.levels; l++ {
		if delta < powInt(int64(w.slots), l+1) {
			level = l
			break
		}
	}
	slot := int((t / w.levelSpan(level)) % int64(w.slots))
	b := &w.buckets[level][slot]
	if b.epoch != w.epoch {
		b.head, b.tail = nil, nil // 旧 epoch 残留惰性清空
		b.epoch = w.epoch
	}
	e.timerMember = true
	e.timerEpoch = w.epoch
	e.timerLevel, e.timerSlot = level, slot
	e.timerPrev, e.timerNext = b.tail, nil
	if b.tail != nil {
		b.tail.timerNext = e
	} else {
		b.head = e
	}
	b.tail = e
}

// remove 撤销 timer 成员；旧 epoch 的残留指针不可用于改链（I10）。
func (w *timerWheel[V]) remove(e *entry[V]) {
	if e.timerMember && e.timerEpoch == w.epoch {
		b := &w.buckets[e.timerLevel][e.timerSlot]
		if b.epoch == w.epoch {
			if e.timerPrev != nil {
				e.timerPrev.timerNext = e.timerNext
			} else if b.head == e {
				b.head = e.timerNext
			}
			if e.timerNext != nil {
				e.timerNext.timerPrev = e.timerPrev
			} else if b.tail == e {
				b.tail = e.timerPrev
			}
		}
	}
	e.timerMember = false
	e.timerPrev, e.timerNext = nil, nil
}

// popHead 取出桶首（仅 advance 使用）。
func (w *timerWheel[V]) popHead(b *wheelBucket[V]) *entry[V] {
	e := b.head
	if e == nil {
		return nil
	}
	b.head = e.timerNext
	if b.head != nil {
		b.head.timerPrev = nil
	} else {
		b.tail = nil
	}
	e.timerMember = false
	e.timerPrev, e.timerNext = nil, nil
	return e
}

// startRebase 启动受限重建（START_WHEEL_REBASE）。
func (w *timerWheel[V]) startRebase(targetTick int64) {
	if w.epoch == math.MaxUint64 {
		w.exhausted = true
		return
	}
	w.epoch++
	w.cursorTick = targetTick
	w.tickInProgress = -1
	w.tasks = w.tasks[:0]
	w.taskCursor = 0
	w.rebasing = true
	w.rebaseCursor = 0
}

// rebaseStep 按配额扫描索引槽重建成员（WHEEL_REBASE_STEP）。
// 返回 true 表示重建完成。
func (w *timerWheel[V]) rebaseStep(ix *index[V], quota int, now int64,
	valid func(e *entry[V], now int64) bool, retire func(e *entry[V]) bool) bool {
	for quota > 0 && w.rebaseCursor < ix.slotCount {
		e := ix.slotEntry(w.rebaseCursor)
		w.rebaseCursor++
		quota--
		if e == nil {
			continue
		}
		w.remove(e) // 旧 epoch 只清本条目成员标记
		if !valid(e, now) {
			e.readable.Store(false)
			retire(e)
		} else if w.schedulable(e.hardDeadline) {
			w.add(e)
		}
	}
	if w.rebaseCursor >= ix.slotCount {
		w.rebasing = false
		return true
	}
	return false
}

// advance 推进时间轮（ADVANCE_WHEEL）。retire 返回 false 表示退休预算不足：
// 条目保持逻辑失效并留索引计量，由 coldCursor 重试。
func (w *timerWheel[V]) advance(now int64, quota int, maxLag int64,
	valid func(e *entry[V], now int64) bool, retire func(e *entry[V]) bool) int {
	work := 0
	target := now / w.tick
	if w.rebasing {
		return 0
	}
	if target-w.cursorTick > maxLag {
		return -1 // 调用方触发 startRebase
	}
	for work < quota {
		if w.taskCursor >= len(w.tasks) {
			if w.tickInProgress >= 0 {
				w.cursorTick = w.tickInProgress // 本 tick 全部任务完成才推进游标
				w.tickInProgress = -1
			}
			if w.cursorTick >= target {
				return work
			}
			k := w.cursorTick + 1
			w.tasks = w.tasks[:0]
			for l := w.levels - 1; l >= 0; l-- {
				if k%w.levelSpan(l) == 0 {
					w.tasks = append(w.tasks, wheelTask{level: l, slot: int((k / w.levelSpan(l)) % int64(w.slots))})
				}
			}
			w.tickInProgress = k
			w.taskCursor = 0
			work++ // Starting a tick consumes work even if every bucket is empty.
			continue
		}
		task := w.tasks[w.taskCursor]
		b := &w.buckets[task.level][task.slot]
		if b.epoch != w.epoch {
			w.taskCursor++
			work++
			continue
		}
		e := w.popHead(b)
		if e == nil {
			w.taskCursor++
			work++
			continue
		}
		work++
		// 精确复核真实 hardDeadline 与身份：禁止提前过期。
		if !valid(e, now) {
			e.readable.Store(false)
			retire(e)
		} else {
			w.add(e) // 降层重挂至未来处理点
		}
	}
	return work
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return a/b + int64Bool(a%b != 0)
}

func int64Bool(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
