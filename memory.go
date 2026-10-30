package ahkc

import (
	"fmt"
	"math"
	"unsafe"
)

// MemoryLimits is the initialization-time bound on retained, billed library
// memory. It excludes caller-owned results, executor storage and Go's GC/RSS.
// Values and shared backing storage must satisfy the NewObject weigh / Loader contract.
//
// 条目、索引组表与热度门卫放在预留的内存区里（RegionBytes）：它是常驻内存的上界，但只有被写过的页才真正占用；
// 字节串与定长布局的驻留字节就落在内存区内，因此不重复计入 TotalBytes。
type MemoryLimits struct {
	FixedBytes, AuxBytes                       int64
	ResidentBytes, PendingBytes, ResponseBytes int64
	// RegionBytes 是全部分片条目区与索引区的预留大小（含 Memory.SlackPercent 的碎片/退休余量）。
	RegionBytes int64
	TotalBytes  int64
}

func (c *Cache[V]) MemoryLimits() MemoryLimits {
	if c == nil || c.c == nil {
		return MemoryLimits{}
	}
	return c.c.memory
}

type cacheLayout struct {
	countMax    int64
	groups      int     // 每分片索引组数
	idxBytes    uintptr // 每分片索引组表 + 门卫的内存区大小
	regionBytes uintptr // 每分片条目区大小
	sideSlots   int     // 每分片对象侧表槽数（仅对象布局）
	memory      MemoryLimits
}

// entryRegionBytes 是每分片条目区大小。
//
// 字节串与定长布局的计费就是块大小，所以区大小由字节额度推出；对象布局与影子模式的块只含
// 头和 key，按条数额度推出。Memory.SlackPercent 余量同时覆盖碎片与尚未回收的退休块。
func entryRegionBytes(cfg Config, lay valueLayout, countMax int64) int64 {
	var logical, margin int64
	if lay == layoutObject || cfg.shadow() {
		margin = blockBytes(cfg.MaxKeyBytes, 0)
		logical = countMax * margin
	} else {
		margin = cfg.MaxEntryBytes
		logical = (cfg.MaxBytes + int64(cfg.Shards) - 1) / int64(cfg.Shards)
	}
	if cfg.observeOnly() {
		return 4096
	}
	return logical/100*int64(100+cfg.Memory.SlackPercent) + margin + 4096
}

// Conservative per-object alignment/header charges are intentional. Fixed
// pools are charged here once; occupying a pool slot does not charge it again.
type byteBound struct {
	bytes int64
	err   error
}

func (b *byteBound) add(n, size int64) {
	if b.err != nil {
		return
	}
	if n < 0 || size < 0 || size > math.MaxInt64-31 {
		b.err = fmt.Errorf("ahkc: memory layout overflow")
		return
	}
	size = (size + 31) &^ 15
	if n > 0 && (size > math.MaxInt64/n || b.bytes > math.MaxInt64-n*size) {
		b.err = fmt.Errorf("ahkc: memory layout overflow")
		return
	}
	b.bytes += n * size
}

// maxRegionBytes 是单个分片条目区的上限：块引用是 32 位、8 字节粒度，最多寻址 32GiB。
const maxRegionBytes = int64(1)<<35 - 64

func planLayout[V any](cfg Config, lay valueLayout) (cacheLayout, error) {
	countMax := (cfg.MaxEntries + int64(cfg.Shards) - 1) / int64(cfg.Shards) // 份额最大的分片
	groups := indexGroups(countMax, cfg.Memory.LoadFactor)
	if cfg.observeOnly() {
		groups = 1 // 保留空索引及统一读/失效路径。
	}
	if groups > math.MaxUint32/groupWords {
		return cacheLayout{}, fmt.Errorf("ahkc: index layout exceeds its supported size; raise Shards")
	}
	// 索引组表与热度门卫共用每分片的一块内存区：组表在前，门卫在后（均 64 字节对齐）。
	idxBytes := indexRegionBytes(groups) + gateRegionBytes(cfg.Heat.GateCells)
	region := entryRegionBytes(cfg, lay, countMax)
	if region < 0 || uint64(region) > uint64(maxRegionBytes) {
		return cacheLayout{}, fmt.Errorf("ahkc: entry region layout exceeds %d bytes per shard; raise Shards", maxRegionBytes)
	}
	sideSlots := 0
	if lay == layoutObject && !cfg.shadow() && !cfg.observeOnly() {
		sideSlots = int(countMax + countMax*int64(cfg.Memory.SlackPercent)/100 + 64) // 余量覆盖尚未回收的退休槽
	}
	s := int64(cfg.Shards)
	// 区大小在 32 位平台上以 uintptr 保存：超出地址空间会被静默截断，必须在这里拒绝。
	if s*(region+int64(idxBytes)) > math.MaxInt {
		return cacheLayout{}, fmt.Errorf("ahkc: reserved regions (%d bytes) exceed this platform's address space; lower MaxBytes or MaxEntries", s*(region+int64(idxBytes)))
	}
	flights := s * int64(cfg.Limits.FlightStripes) * int64(cfg.Limits.FlightsPerStripe)
	controlSlots := int64(cfg.Shards+1) * int64(cfg.Limits.ControlQueueSize+1)
	fixed := byteBound{}
	fixed.add(1, int64(unsafe.Sizeof(cache[V]{}))+1024)
	fixed.add(s, int64(unsafe.Sizeof(shard[V]{}))+2048)
	var zeroV V
	fixed.add(s*int64(sideSlots), int64(unsafe.Sizeof(zeroV))+4)
	fixed.add(s*int64(cfg.Limits.ReaderSlotsPerShard), int64(unsafe.Sizeof(epochSlot{})))
	fixed.add(s*int64(cfg.Limits.FlightStripes), int64(unsafe.Sizeof(flightStripe[V]{}))+256)
	fixed.add(flights, 16) // registry and completion wake pointers
	fixed.add(s*int64(cfg.Limits.FillQueueSize+cfg.Limits.ControlQueueSize), 8)
	fixed.add(int64(cfg.Limits.MaxExecutorTasks), int64(unsafe.Sizeof(executorTask{}))+64)
	fixed.add(s, 32*1024) // bounded exact-solver and maintenance scratch per owner
	fixed.add(int64(cfg.Limits.MaxNamespaces), int64(unsafe.Sizeof(namespace[V]{}))+int64(cfg.MaxKeyBytes)+4096)
	// 热点追踪表（固定槽位 SoA）与各自的键池（Go 堆上不含指针的字节区）。
	fixed.add(s*int64(cfg.Hot.LoadSlots), hotSlotBytes+trackBytes(1))
	fixed.add(s*int64(cfg.Hot.RequestSlots), trackBytes(1))
	if cfg.Hot.LoadSlots > 0 {
		fixed.add(s, keyPoolBytes(cfg.Hot.LoadSlots, cfg.MaxKeyBytes))
	}
	if cfg.Hot.RequestSlots > 0 {
		fixed.add(s, keyPoolBytes(cfg.Hot.RequestSlots, cfg.MaxKeyBytes))
	}
	fixed.add(s, int64(unsafe.Sizeof(hotTracker{}))+int64(unsafe.Sizeof(reqTracker{})))
	// 历史窗口：采集堆（请求榜另含 key 字节槽）与环中快照（每份最多两张榜单；+2 为待通知快照与在建快照）。
	if cfg.Hot.Windows > 0 || cfg.Hot.OnWindow != nil {
		topN, lists := int64(cfg.Hot.TopN), int64(min(cfg.Hot.LoadSlots, 1)+min(cfg.Hot.RequestSlots, 1))
		fixed.add(lists*topN, int64(unsafe.Sizeof(winEntry{})))
		fixed.add(lists*topN, int64(cfg.MaxKeyBytes))
		fixed.add(int64(cfg.Hot.Windows+2)*lists*topN, int64(unsafe.Sizeof(histRec{}))+int64(cfg.MaxKeyBytes))
		fixed.add(int64(cfg.Hot.Windows+2), int64(unsafe.Sizeof(histSnap{})))
	}
	aux := byteBound{}
	// flight 内嵌工作记录、交付和凭证；sourceOutcome 为大 V 可能逃逸的临时副本留上界。
	perFlight := int64(unsafe.Sizeof(flight[V]{})) + int64(cfg.MaxKeyBytes) +
		int64(unsafe.Sizeof(sourceOutcome[V]{})) + int64(unsafe.Sizeof(fillCandidate[V]{})) + 512
	perFlight += int64(cfg.Limits.MaxWaitersPerKey) * (int64(unsafe.Sizeof(Future[V]{})) + 256) // Future 内嵌凭证
	aux.add(flights, perFlight)
	aux.add(s*int64(cfg.Limits.FillQueueSize+1), int64(unsafe.Sizeof(flight[V]{}))+int64(unsafe.Sizeof(fillCandidate[V]{}))+256)
	// 未取走的结果通过内嵌交付保留整个 flight，即使注册槽已经归还。
	aux.add(int64(cfg.Limits.MaxWaitersTotal), int64(unsafe.Sizeof(Future[V]{}))+int64(unsafe.Sizeof(flight[V]{}))+int64(unsafe.Sizeof(Handle[V]{}))+256)
	aux.add(controlSlots, int64(unsafe.Sizeof(controlOp[V]{}))+int64(cfg.MaxKeyBytes)+256)
	aux.add(int64(cfg.Limits.MaxExecutorTasks), int64(unsafe.Sizeof(flight[V]{}))+256) // closure and a possibly detached terminal flight
	aux.add(int64(cfg.Limits.MaxOwnedHandles), int64(unsafe.Sizeof(Handle[V]{}))+int64(unsafe.Sizeof(receipt{})))
	if fixed.err != nil {
		return cacheLayout{}, fixed.err
	}
	if aux.err != nil {
		return cacheLayout{}, aux.err
	}
	m := MemoryLimits{FixedBytes: fixed.bytes, AuxBytes: aux.bytes, ResidentBytes: cfg.MaxBytes,
		PendingBytes: cfg.Limits.PendingBytes, ResponseBytes: cfg.Limits.ResponseBytes,
		RegionBytes: s * (region + int64(idxBytes))}
	total := []int64{m.FixedBytes, m.AuxBytes, m.PendingBytes, m.ResponseBytes, m.RegionBytes}
	if lay == layoutObject || cfg.shadow() {
		total = append(total, m.ResidentBytes) // 值在 Go 堆上，不在内存区内
	}
	for _, bytes := range total {
		if bytes > math.MaxInt64-m.TotalBytes {
			return cacheLayout{}, fmt.Errorf("ahkc: managed memory bound overflow")
		}
		m.TotalBytes += bytes
	}
	if cfg.Limits.MaxManagedBytes != 0 && m.TotalBytes > cfg.Limits.MaxManagedBytes {
		return cacheLayout{}, fmt.Errorf("ahkc: memory bound %d exceeds MaxManagedBytes %d", m.TotalBytes, cfg.Limits.MaxManagedBytes)
	}
	return cacheLayout{countMax: countMax, groups: groups, idxBytes: idxBytes, regionBytes: uintptr(region), sideSlots: sideSlots, memory: m}, nil
}
