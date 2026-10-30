package ahkc

import (
	"math"
	"sync/atomic"
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

	fillPublished atomic.Uint64
	fillRejected  [24]atomic.Uint64
	fillChecks    atomic.Uint64 // 求解器子集检查总数

	hotWindowDropped atomic.Uint64 // Hot.OnWindow 通知因执行器拒绝/任务池耗尽被丢弃的次数
	hotWindowBusy    atomic.Uint64 // Hot.OnWindow 通知因上一个回调仍在执行被丢弃的次数
	hotExportPanics  atomic.Uint64 // 宿主回调/映射钩子 panic 次数（已隔离）

	retiredCompleted atomic.Uint64
	expiredHints     atomic.Uint64 // 读者登记、维护者消费的失效提示数

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
	FillChecks                    uint64
	FillGain                      float64
	PendingFills, PendingControls int
	OwnedHandles, PinnedReaders   int64
	// ReaderEpochLag is the largest per-shard difference from a pinned epoch.
	ReaderEpochLag uint64
	// PinSlotsExhausted 读者因保护槽耗尽得到 ErrBusy 的次数（调大 ReaderSlotsPerShard；热 key 只在一个分片，加分片无效，借用后阻塞的调用方应改用 Get）；
	// PinEpochRetriesExhausted 读者宣布期间 epoch 连续推进、重试耗尽的次数。
	PinSlotsExhausted, PinEpochRetriesExhausted uint64
	// HotWindowDropped Hot.OnWindow 通知因执行器拒绝（或任务池耗尽）被丢弃的次数；
	// HotWindowBusy 因上一个回调仍在执行被丢弃的次数；
	// HotExportPanics 宿主回调与 Hot.Label 映射钩子的 panic 次数（均已隔离）。
	HotWindowDropped, HotWindowBusy, HotExportPanics uint64
	// HotEventsDropped 回源热点追踪表因锁竞争丢弃的事件数（有界近似）；
	// HotRequestSamplesDropped 请求热点追踪表因锁竞争丢弃的采样数。
	HotEventsDropped, HotRequestSamplesDropped uint64
	RetiredCompleted                           uint64
	// ExpiredHints 读者发现失效条目后登记、被维护者消费的提示数。
	ExpiredHints uint64
	// ResultTooLarge 违反 maxResultBytes 契约的加载结果数。
	ResultTooLarge                           uint64
	NamespaceInvalidations, KeyInvalidations uint64
	// ResidentEntries/ResidentBytes 当前驻留量。
	ResidentEntries int64
	ResidentBytes   int64
	// ArenaUsedBytes/ArenaFreeBytes 是全部分片条目区的已分配与空闲字节（含块边界字，
	// 两者之和是条目区总大小）。已分配大于 ResidentBytes 说明有退休块尚未回收；
	// 空闲很多但仍出现 ALLOCATION_FAILED 说明碎片（见 ArenaLargestFree），调大 Memory.SlackPercent。
	// RetiredPending 是退休链上尚未回收的条目数。
	ArenaUsedBytes, ArenaFreeBytes, RetiredPending int64
	// ArenaLargestFree 是单个分片里最大空闲块的字节数（取各分片最大值）。ArenaFreeBytes 很大而它
	// 很小，说明碎片严重，填充会以 ALLOCATION_FAILED 被拒；此时调大 Memory.SlackPercent。
	// 它由最高非空分级的链头块给出，与真正的最大块至多差约 1/16。
	ArenaLargestFree int64
	// PendingBytesAvail 等预算余量。
	PendingBytesAvail  int64
	ResponseBytesAvail int64
	IOInFlight         int64
	// Mode 报告运行模式。ModeShadow 下 Hits / Misses 是虚拟命中 / 未命中，
	// 不代表真实缓存效果，不可与缓存模式的统计混读。
	Mode Mode
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
		FillPublished:          m.fillPublished.Load(),
		FillChecks:             m.fillChecks.Load(),
		HotWindowDropped:       m.hotWindowDropped.Load(),
		HotWindowBusy:          m.hotWindowBusy.Load(),
		HotExportPanics:        m.hotExportPanics.Load(),
		RetiredCompleted:       m.retiredCompleted.Load(),
		ExpiredHints:           m.expiredHints.Load(),
		ResultTooLarge:         m.resultTooLarge.Load(),
		NamespaceInvalidations: m.nsInvalidations.Load(), KeyInvalidations: m.keyInvalidations.Load(),
		PendingBytesAvail: c.c.pendingBytes.available(), ResponseBytesAvail: c.c.responseBytes.available(),
		IOInFlight: int64(c.c.cfg.Limits.MaxConcurrentLoads) - c.c.ioBudget.available(),
		Mode:       c.c.cfg.Mode,
	}
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
		s.PendingControls += len(sh.controlQueue)
		for i := range sh.epoch.slots {
			if epoch := sh.epoch.slots[i].v.Load(); epoch != epochSlotInactive {
				s.PinnedReaders++
				if current := sh.epoch.epoch.Load(); current >= epoch-1 {
					s.ReaderEpochLag = max(s.ReaderEpochLag, current-(epoch-1))
				}
			}
		}
		s.PinSlotsExhausted += sh.epoch.slotFull.Load()
		s.PinEpochRetriesExhausted += sh.epoch.retryFail.Load()
		s.FillGain = math.Min(math.MaxFloat64, s.FillGain+math.Float64frombits(sh.fillGain.Load()))
		s.ArenaUsedBytes += sh.arena.usedBytes.Load()
		s.ArenaFreeBytes += sh.arena.freeBytes.Load()
		s.ArenaLargestFree = max(s.ArenaLargestFree, sh.arena.largestFree.Load())
		s.RetiredPending += sh.retiredCount.Load()
		s.ResidentEntries += sh.residentCount.Load()
		s.ResidentBytes += sh.residentBytes.Load()
		s.Hits += sh.cnt.hits.Load()
		s.Misses += sh.cnt.misses.Load()
		s.Busy += sh.cnt.busy.Load()
		if sh.hot != nil {
			s.HotEventsDropped += sh.hot.dropped.Load()
		}
		if sh.req != nil {
			s.HotRequestSamplesDropped += sh.req.dropped.Load()
		}
	}
	return s
}
