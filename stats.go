package ahkc

import (
	"math"
	"sync"
	"sync/atomic"
)

// 收益统计与稀疏采样。
//
// 样本在外部入口以概率 p 独立抽取（不按 key 哈希永久决定），
// 携带事件时间与当次概率；维护者按事件时间写入对应桶，加权增量 1/p。
// 样本缓冲是固定条带的有界队列：入队失败放弃样本并置 faultFlag（不静默）。

type sample struct {
	nsID       uint32
	policyHash uint64
	nsGen      uint64
	eventTime  int64
	invP       float64 // 预计算 1/p
}

// sampleStripe 是固定容量的样本条带（TRY_GATE + 有界环形缓冲）。
type sampleStripe struct {
	mu        sync.Mutex // 仅 TryLock
	buf       []sample
	ages      []int // min-heap of active ring slots; positions permits bounded removal
	positions []int
	head      int // 写位置
	tail      int // 读位置（owner）
	// faultFlag：丢样标记；不能因标记竞争而静默消失。
	faultFlag atomic.Bool
	queued    atomic.Int64
	oldest    atomic.Int64 // minimum event time of all queued samples
}

func newSampleStripe(capacity int) *sampleStripe {
	s := &sampleStripe{buf: make([]sample, capacity), ages: make([]int, 0, capacity), positions: make([]int, capacity)}
	return s
}

// tryPush 有限次入队尝试；失败置 faultFlag 并返回 false。
func (s *sampleStripe) tryPush(v sample) bool {
	if s.tryPushQuiet(v) {
		return true
	}
	s.faultFlag.Store(true)
	return false
}

// tryPushQuiet 同 tryPush，但失败时不置丢样标记，由调用方决定是否换条带重试。
func (s *sampleStripe) tryPushQuiet(v sample) bool {
	if !s.mu.TryLock() {
		return false
	}
	if len(s.ages) == len(s.buf) {
		s.mu.Unlock()
		return false
	}
	s.buf[s.head] = v
	s.positions[s.head] = len(s.ages)
	s.ages = append(s.ages, s.head)
	s.ageUp(len(s.ages) - 1)
	s.oldest.Store(s.buf[s.ages[0]].eventTime)
	s.head = (s.head + 1) % len(s.buf)
	s.queued.Add(1)
	s.mu.Unlock()
	return true
}

// pop 由维护者调用。
func (s *sampleStripe) pop() (sample, bool) {
	if !s.mu.TryLock() {
		return sample{}, false
	}
	if len(s.ages) == 0 {
		s.mu.Unlock()
		return sample{}, false
	}
	v := s.buf[s.tail]
	s.removeAge(s.tail)
	s.buf[s.tail] = sample{}
	s.tail = (s.tail + 1) % len(s.buf)
	if len(s.ages) == 0 {
		s.oldest.Store(0)
	} else {
		s.oldest.Store(s.buf[s.ages[0]].eventTime)
	}
	s.queued.Add(-1)
	s.mu.Unlock()
	return v, true
}

// Every heap operation is bounded by log2(SampleBufferSize), at most 20 levels.
// FIFO ordering remains unchanged; the heap tracks out-of-order event times.
func (s *sampleStripe) ageSwap(a, b int) {
	s.ages[a], s.ages[b] = s.ages[b], s.ages[a]
	s.positions[s.ages[a]], s.positions[s.ages[b]] = a, b
}
func (s *sampleStripe) ageUp(at int) {
	for at > 0 {
		parent := (at - 1) / 2
		if s.buf[s.ages[parent]].eventTime <= s.buf[s.ages[at]].eventTime {
			return
		}
		s.ageSwap(parent, at)
		at = parent
	}
}
func (s *sampleStripe) removeAge(slot int) {
	at, last := s.positions[slot], len(s.ages)-1
	s.ages[at] = s.ages[last]
	s.positions[s.ages[at]] = at
	s.ages[last] = 0
	s.ages = s.ages[:last]
	s.positions[slot] = 0
	if at == last {
		return
	}
	if at > 0 && s.buf[s.ages[at]].eventTime < s.buf[s.ages[(at-1)/2]].eventTime {
		s.ageUp(at)
		return
	}
	for {
		child := at*2 + 1
		if child >= len(s.ages) {
			return
		}
		if child+1 < len(s.ages) && s.buf[s.ages[child+1]].eventTime < s.buf[s.ages[child]].eventTime {
			child++
		}
		if s.buf[s.ages[at]].eventTime <= s.buf[s.ages[child]].eventTime {
			return
		}
		s.ageSwap(at, child)
		at = child
	}
}

// sampleQuality checks input that has not reached the owner yet. A pending
// fault must invalidate a decision even when the samples class runs later.
func (c *cache[V]) sampleQuality(s *shard[V], now int64) bool {
	healthy := true
	for _, st := range s.stripes {
		if st.faultFlag.Load() {
			s.stats.lastFaultAt = now
			healthy = false
		}
		if st.queued.Load() > 0 {
			oldest := st.oldest.Load()
			// A producer may enqueue after the decision timestamp was captured.
			// Read time after the queue timestamp before declaring it future.
			observedAt := c.now()
			if oldest > observedAt || oldest < 0 {
				s.stats.lastFaultAt = observedAt
				healthy = false
			} else if observedAt-oldest > int64(c.cfg.MaxSampleLag) {
				s.stats.lastLagViolation = observedAt
				healthy = false
			}
		}
	}
	return healthy
}

// valueCell 是收益 sketch 单元：双桶加权计数与样本数，计数不饱和（owner 独占写）。
type valueCell struct {
	epoch       uint64
	wCur, wPrev float64
	sCur, sPrev float64
}

func (c *valueCell) normalize(e uint64) {
	switch {
	case c.epoch == e:
	case e == c.epoch+1:
		c.wPrev, c.sPrev = c.wCur, c.sCur
		c.wCur, c.sCur = 0, 0
		c.epoch = e
	case e > c.epoch+1:
		*c = valueCell{epoch: e}
	}
}

// valueStats 是独立于热度门卫的固定宽度频率 sketch（维护者独占写）。
type valueStats struct {
	rows, width int
	cells       []valueCell // rows*width
	builtAt     int64
	// lastFaultAt：已知丢样/溢出/滞后污染时间；污染窗口禁止收益填充。
	lastFaultAt      int64
	lastLagViolation int64
}

// noFaultAt 是 lastFaultAt/lastLagViolation 的“从未发生”哨兵（足够负，窗口判定恒假）。
const noFaultAt = math.MinInt64 / 2

func newValueStats(rows, width int, builtAt int64) *valueStats {
	return &valueStats{
		rows: rows, width: width,
		cells:            make([]valueCell, rows*width),
		builtAt:          builtAt,
		lastFaultAt:      noFaultAt,
		lastLagViolation: noFaultAt,
	}
}

func (vs *valueStats) cellAt(row int, ph uint64) *valueCell {
	h := mix64(ph + uint64(row)*rowSeedStep)
	return &vs.cells[row*vs.width+int(h%uint64(vs.width))]
}

// addSample 按事件时间写入对应桶；过旧/未来样本丢弃并标记 fault。
func (vs *valueStats) addSample(s sample, now, delta int64) bool {
	t := now
	epoch := uint64(t / delta)
	if s.eventTime < 0 || s.eventTime > t || uint64(s.eventTime/delta)+1 < epoch {
		vs.lastFaultAt = t // 迟到至窗口之外或来自未来
		return false
	}
	if s.invP <= 0 || math.IsNaN(s.invP) || math.IsInf(s.invP, 0) {
		vs.lastFaultAt = t
		return false
	}
	// 样本滞后（now-eventTime）超限的污染由 drainSamples 按 maxSampleLag 判定。
	se := uint64(s.eventTime / delta)
	for row := 0; row < vs.rows; row++ {
		c := vs.cellAt(row, s.policyHash)
		c.normalize(epoch)
		if se == epoch {
			c.wCur += s.invP
			c.sCur++
		} else {
			c.wPrev += s.invP
			c.sPrev++
		}
		if math.IsInf(c.wCur, 0) || math.IsInf(c.wPrev, 0) || math.IsNaN(c.wCur) || math.IsNaN(c.wPrev) {
			vs.lastFaultAt = t
			return false
		}
	}
	return true
}

// query 返回窗口加权请求数与样本数（各行最小值）。
func (vs *valueStats) query(ph uint64, now, delta int64) (weighted, samples float64) {
	epoch := uint64(now / delta)
	phi := bucketPhi(now, delta)
	for row := 0; row < vs.rows; row++ {
		c := *vs.cellAt(row, ph) // 快照副本
		c.normalize(epoch)
		w := c.wCur + (1-phi)*c.wPrev
		s := c.sCur + (1-phi)*c.sPrev
		if row == 0 || w < weighted {
			weighted = w
		}
		if row == 0 || s < samples {
			samples = s
		}
	}
	return weighted, samples
}

// estimateMode 估值三态（A.4）。
type estimateMode uint8

const (
	estMeasured estimateMode = iota
	estPrior
	estUnavailable
	estExpired
)

func (m estimateMode) String() string {
	switch m {
	case estMeasured:
		return "MEASURED"
	case estPrior:
		return "PRIOR"
	case estUnavailable:
		return "UNAVAILABLE"
	default:
		return "EXPIRED"
	}
}

// estimateItem 是估值输入（冻结身份与截止时间）。
type estimateItem struct {
	policyHash   uint64
	hardDeadline int64
	costClass    uint32
}

// estimate 是估值输出。G 为预计净收益，k 为移除成本，installCost 为安装成本 K。
type estimate struct {
	G, k        float64
	installCost float64
	mode        estimateMode
}

// loss 返回移除损失 L = G + k。
func (e estimate) loss() float64 { return e.G + e.k }

// At most ten entries participate in a decision. Leave room for their signed
// sums as well as installation cost, so every exact comparison stays finite.
const maxEstimateTerm = math.MaxFloat64 / 32

// estimate 计算估值（A.4 ESTIMATE）。model 为本次决策冻结的不可变模型；
// prior 可空；所有输入必须同口径有限非负，否则 UNAVAILABLE。
func (vs *valueStats) estimate(item estimateItem, t int64, nsCfg *NamespaceConfig, model CostModel, cfg *Config) estimate {
	if t >= item.hardDeadline {
		return estimate{mode: estExpired}
	}
	delta := int64(cfg.ValueDelta)
	// 已知丢样 / 滞后污染窗口：UNAVAILABLE，恢复完整健康双桶（2Δv）后重新启用。
	if t < vs.lastFaultAt+2*delta || t < vs.lastLagViolation+2*delta {
		return estimate{mode: estUnavailable}
	}
	if model == nil {
		return estimate{mode: estUnavailable}
	}
	weighted, samples := vs.query(item.policyHash, t, delta)
	warm := t >= vs.builtAt+2*delta
	var lambda float64
	var mode estimateMode
	switch {
	case warm && samples >= cfg.NMin:
		lambda = weighted / deltaSeconds(delta)
		mode = estMeasured
	case nsCfg.Prior != nil && t >= nsCfg.Prior.BuiltAt && t-nsCfg.Prior.BuiltAt <= int64(cfg.PriorMaxAge) && allFiniteNonNegative(nsCfg.Prior.Lambda):
		lambda = nsCfg.Prior.Lambda
		mode = estPrior
	default:
		return estimate{mode: estUnavailable}
	}
	r, c, h, k, K, ok := model.Lookup(lambda, item.costClass)
	if !ok || !allFiniteNonNegative(r, c, h, k, K) {
		return estimate{mode: estUnavailable}
	}
	duration := math.Min(deltaSeconds(int64(cfg.HorizonW)), deltaSeconds(item.hardDeadline-t))
	if duration <= 0 {
		return estimate{mode: estExpired}
	}
	gain := (r*c - h) * duration
	if math.IsNaN(gain) || math.IsInf(gain, 0) || math.Abs(gain) > maxEstimateTerm || k > maxEstimateTerm || K > maxEstimateTerm {
		return estimate{mode: estUnavailable}
	}
	return estimate{G: gain, k: k, installCost: K, mode: mode}
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
