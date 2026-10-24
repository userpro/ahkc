package ahkc

import (
	"math"
	"sync/atomic"
	"time"
)

// 观测指标：固定条带，不按 key 创建指标。
// 请求路径计数在分片条纹上（shard.cnt），此处保留低频路径的全局计数。
type metrics struct {
	overloaded atomic.Uint64

	loads       atomic.Uint64 // 真实 loader 调用
	loadErrs    atomic.Uint64
	loadMerged  atomic.Uint64 // 合并等待的 miss 数
	loadTimeout atomic.Uint64
	orphanLoads atomic.Uint64 // 超时但底层仍在运行的孤儿加载

	fillPublished   atomic.Uint64
	fillRejected    [24]atomic.Uint64
	fillChecks      atomic.Uint64 // 求解器子集检查总数
	fillMeasured    atomic.Uint64
	fillPrior       atomic.Uint64
	fillUnavailable atomic.Uint64

	samplesDropped atomic.Uint64

	hotWindowDropped atomic.Uint64 // OnHotWindow 通知因执行器拒绝/任务池耗尽被丢弃的次数
	hotWindowBusy    atomic.Uint64 // OnHotWindow 通知因上一个回调仍在执行被丢弃的次数
	hotExportPanics  atomic.Uint64 // 宿主回调/映射钩子 panic 次数（已隔离）

	retiredCompleted atomic.Uint64
	creditMoved      atomic.Uint64

	resultTooLarge atomic.Uint64 // 违反 maxResultBytes 契约的结果数

	nsInvalidations  atomic.Uint64
	keyInvalidations atomic.Uint64
}

// Stats 是指标与预算快照（非事务一致性视图）。
type Stats struct {
	Hits, Misses, Busy, Overloaded             uint64
	Loads, LoadErrors, LoadMerged, LoadTimeout uint64
	// OrphanLoads 超时但底层 I/O 仍在运行的加载数。
	OrphanLoads   uint64
	FillPublished uint64
	// FillRejected 按原因统计；没有拒绝时为 nil，非空 map 独立归调用方持有。
	FillRejected map[string]uint64
	// FillChecks 求解器子集检查总数；FillGain 为已发布填充的预测净收益。
	// 只有累计期间模型单位一致时才可相加，不能直接当实际净 CPU 节省。
	FillChecks                               uint64
	FillGain                                 float64
	Samples, SamplesDropped                  uint64
	FillMeasured, FillPrior, FillUnavailable uint64
	SamplesQueued                            int64
	OldestSampleAge                          time.Duration
	PendingFills, PendingControls            int
	OwnedHandles, PinnedReaders              int64
	// ReaderEpochLag is the largest per-shard difference from a pinned epoch.
	ReaderEpochLag uint64
	// HotWindowDropped OnHotWindow 通知因执行器拒绝（或任务池耗尽）被丢弃的次数；
	// HotWindowBusy 因上一个回调仍在执行被丢弃的次数；
	// HotExportPanics 宿主回调与 HotKeyLabel 映射钩子的 panic 次数（均已隔离）。
	HotWindowDropped, HotWindowBusy, HotExportPanics uint64
	// HotEventsDropped 回源热点追踪表因锁竞争丢弃的事件数（有界近似）；
	// HotRequestSamplesDropped 请求热点追踪表因锁竞争丢弃的采样数。
	HotEventsDropped, HotRequestSamplesDropped uint64
	RetiredCompleted, CreditMoved              uint64
	// ResultTooLarge 违反 maxResultBytes 契约的加载结果数。
	ResultTooLarge                           uint64
	NamespaceInvalidations, KeyInvalidations uint64
	// ResidentEntries/ResidentBytes 当前驻留量。
	ResidentEntries int64
	ResidentBytes   int64
	// PendingBytesAvail 等预算余量。
	PendingBytesAvail   int64
	ResponseBytesAvail  int64
	RetiredBytesAvail   int64
	RetiredRecordsAvail int64
	IOInFlight          int64
	// ObserveOnly 报告实例是否运行在仅统计不缓存模式。
	ObserveOnly bool
}

// Stats 返回指标快照。
func (c *Cache[V]) Stats() Stats {
	if c == nil || c.c == nil {
		return Stats{}
	}
	m := &c.c.metrics
	s := Stats{
		Overloaded: m.overloaded.Load(),
		Loads:      m.loads.Load(), LoadErrors: m.loadErrs.Load(), LoadMerged: m.loadMerged.Load(),
		LoadTimeout: m.loadTimeout.Load(), OrphanLoads: m.orphanLoads.Load(),
		FillPublished: m.fillPublished.Load(),
		FillChecks:    m.fillChecks.Load(),
		FillMeasured:  m.fillMeasured.Load(), FillPrior: m.fillPrior.Load(), FillUnavailable: m.fillUnavailable.Load(),
		SamplesDropped:   m.samplesDropped.Load(),
		HotWindowDropped: m.hotWindowDropped.Load(),
		HotWindowBusy:    m.hotWindowBusy.Load(),
		HotExportPanics:  m.hotExportPanics.Load(),
		RetiredCompleted: m.retiredCompleted.Load(), CreditMoved: m.creditMoved.Load(),
		ResultTooLarge:         m.resultTooLarge.Load(),
		NamespaceInvalidations: m.nsInvalidations.Load(), KeyInvalidations: m.keyInvalidations.Load(),
		PendingBytesAvail: c.c.pendingBytes.available(), ResponseBytesAvail: c.c.responseBytes.available(),
		RetiredBytesAvail: c.c.retiredBytes.available(), RetiredRecordsAvail: c.c.retiredRecords.available(),
		IOInFlight:  int64(c.c.cfg.MaxConcurrentLoads) - c.c.ioBudget.available(),
		ObserveOnly: c.c.cfg.ObserveOnly,
	}
	now := c.c.cfg.Clock.Now()
	s.OwnedHandles = c.c.handleBudget.limit - c.c.handleBudget.available()
	s.PendingControls = len(c.c.nsControlQueue)
	for i := fillReason(1); i < 24; i++ {
		if v := m.fillRejected[i].Load(); v > 0 {
			if s.FillRejected == nil {
				s.FillRejected = make(map[string]uint64)
			}
			s.FillRejected[i.String()] = v
		}
	}
	for _, sh := range c.c.shards {
		s.PendingFills += len(sh.fillQueue)
		if sh.fillHead.Load() != nil {
			s.PendingFills++
		}
		s.PendingControls += len(sh.controlQueue) + len(sh.creditQueue)
		if sh.deferredControl.Load() != nil {
			s.PendingControls++
		}
		for _, stripe := range sh.stripes {
			n := stripe.queued.Load()
			s.SamplesQueued += n
			if oldest := stripe.oldest.Load(); n > 0 && oldest >= 0 && now >= oldest {
				s.OldestSampleAge = max(s.OldestSampleAge, time.Duration(now-oldest))
			}
		}
		for i := range sh.epoch.slots {
			if epoch := sh.epoch.slots[i].v.Load(); epoch != epochSlotInactive {
				s.PinnedReaders++
				if current := sh.epoch.epoch.Load(); current >= epoch-1 {
					s.ReaderEpochLag = max(s.ReaderEpochLag, current-(epoch-1))
				}
			}
		}
		s.FillGain = math.Min(math.MaxFloat64, s.FillGain+math.Float64frombits(sh.fillGain.Load()))
		s.ResidentEntries += sh.residentCount.Load()
		s.ResidentBytes += sh.residentBytes.Load()
		s.Hits += sh.cnt.hits.Load()
		s.Misses += sh.cnt.misses.Load()
		s.Busy += sh.cnt.busy.Load()
		s.Samples += sh.cnt.samples.Load()
		if sh.hot != nil {
			s.HotEventsDropped += sh.hot.dropped.Load()
		}
		if sh.req != nil {
			s.HotRequestSamplesDropped += sh.req.dropped.Load()
		}
	}
	return s
}
