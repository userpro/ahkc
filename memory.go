package ahkc

import (
	"fmt"
	"math"
	"unsafe"
)

// MemoryLimits is the initialization-time bound on retained, billed library
// memory. It excludes caller-owned results, executor storage and Go's GC/RSS.
// Values and shared backing storage must satisfy the Weigher/Loader contract.
type MemoryLimits struct {
	FixedBytes, AuxBytes                                     int64
	ResidentBytes, PendingBytes, RetiredBytes, ResponseBytes int64
	TotalBytes                                               int64
}

func (c *Cache[V]) MemoryLimits() MemoryLimits {
	if c == nil || c.c == nil {
		return MemoryLimits{}
	}
	return c.c.memory
}

type cacheLayout struct {
	countMax int64
	slots    int
	memory   MemoryLimits
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

func planLayout[V any](cfg Config) (cacheLayout, error) {
	perCount := cfg.MaxEntries / int64(cfg.ShardCount)
	countMax := cfg.MaxEntries
	if perCount < cfg.MaxEntries/2 {
		countMax = 2*perCount + 1
	}
	slotsFloat := math.Ceil(float64(countMax) / cfg.MaxLoadFactor)
	if slotsFloat >= math.MaxInt32 {
		return cacheLayout{}, fmt.Errorf("ahkc: index layout exceeds its supported size")
	}
	slots := nextPow2(int(slotsFloat))
	if slots < cfg.ProbeLimit {
		slots = nextPow2(cfg.ProbeLimit)
	}
	if cfg.ObserveOnly {
		slots = nextPow2(cfg.ProbeLimit) // 保留空索引及统一读/失效路径。
	}
	poolSize := shardPoolSize(cfg, countMax)
	if poolSize < 0 || poolSize > math.MaxInt32 {
		return cacheLayout{}, fmt.Errorf("ahkc: entry pool layout overflow")
	}
	s := int64(cfg.ShardCount)
	flights := s * int64(cfg.FlightStripes) * int64(cfg.FlightsPerStripe)
	controlSlots := int64(cfg.ShardCount+1) * int64(cfg.ControlQueueSize+1)
	fixed := byteBound{}
	fixed.add(1, int64(unsafe.Sizeof(cache[V]{}))+1024)
	fixed.add(s, int64(unsafe.Sizeof(shard[V]{}))+2048)
	fixed.add(s*int64(slots), int64(unsafe.Sizeof(uintptr(0))))
	fixed.add(s*int64(poolSize), int64(unsafe.Sizeof(entry[V]{}))+4)
	fixed.add(s*int64(cfg.GateRows)*int64(cfg.GateWidth), int64(unsafe.Sizeof(heatCell{})))
	fixed.add(s*int64(cfg.ValueRows)*int64(cfg.ValueWidth), int64(unsafe.Sizeof(valueCell{})))
	fixed.add(s*int64(cfg.SampleStripes)*int64(cfg.SampleBufferSize), int64(unsafe.Sizeof(sample{}))+2*int64(unsafe.Sizeof(int(0))))
	fixed.add(s*int64(cfg.SampleStripes), int64(unsafe.Sizeof(sampleStripe{}))+256)
	fixed.add(s*int64(cfg.ReaderSlotsPerShard), int64(unsafe.Sizeof(epochSlot{})))
	fixed.add(s*int64(cfg.WheelLevels)*int64(cfg.WheelSlots), int64(unsafe.Sizeof(wheelBucket[V]{})))
	fixed.add(s*int64(cfg.WheelLevels), int64(unsafe.Sizeof(wheelTask{}))+32)
	fixed.add(s*int64(cfg.FlightStripes), int64(unsafe.Sizeof(flightStripe[V]{}))+256)
	fixed.add(flights, 16)                                                             // registry and completion wake pointers
	fixed.add(s*int64(cfg.FillQueueSize+2*cfg.ControlQueueSize+cfg.RetiredRecords), 8) // 控制队列含失效与额度转移两条
	fixed.add(int64(cfg.RetiredRecords), int64(unsafe.Sizeof(retiredRecord[V]{}))+8)
	fixed.add(int64(cfg.MaxExecutorTasks), int64(unsafe.Sizeof(executorTask{}))+64)
	fixed.add(s, 32*1024) // bounded exact-solver and maintenance scratch per owner
	fixed.add(int64(cfg.MaxNamespaces), int64(unsafe.Sizeof(namespace[V]{}))+int64(cfg.MaxKeyBytes)+4096)
	fixed.add(int64(cfg.MaxNamespaces), cfg.MaxModelBytes)
	// 热点追踪表（固定槽位 SoA；回源表另含 key 副本，请求表含预分配 key 字节区）。
	fixed.add(s*int64(cfg.ObserveHotKeys), hotSlotBytes+int64(cfg.MaxKeyBytes)+256)
	fixed.add(s*int64(cfg.ObserveRequestKeys), reqSlotBytes+int64(cfg.MaxKeyBytes))
	fixed.add(s, int64(unsafe.Sizeof(hotTracker{}))+int64(unsafe.Sizeof(reqTracker{})))
	// 历史窗口：采集堆（请求榜另含 key 字节槽）与环中快照（每份最多两张榜单；+2 为待通知快照与在建快照）。
	if cfg.HotHistoryWindows > 0 || cfg.OnHotWindow != nil {
		topN, lists := int64(cfg.HotHistoryTopN), int64(min(cfg.ObserveHotKeys, 1)+min(cfg.ObserveRequestKeys, 1))
		fixed.add(lists*topN, int64(unsafe.Sizeof(winEntry{})))
		fixed.add(topN, int64(cfg.MaxKeyBytes))
		fixed.add(int64(cfg.HotHistoryWindows+2)*lists*topN, int64(unsafe.Sizeof(HotKeyStat{}))+int64(cfg.MaxKeyBytes))
		fixed.add(int64(cfg.HotHistoryWindows+2), int64(unsafe.Sizeof(HotSnapshot{})))
	}
	aux := byteBound{}
	// flight 内嵌工作记录、交付和凭证；sourceOutcome 为大 V 可能逃逸的临时副本留上界。
	perFlight := int64(unsafe.Sizeof(flight[V]{})) + int64(cfg.MaxKeyBytes) +
		int64(unsafe.Sizeof(sourceOutcome[V]{})) + int64(unsafe.Sizeof(fillCandidate[V]{})) + 512
	perFlight += int64(cfg.MaxWaitersPerKey) * (int64(unsafe.Sizeof(Future[V]{})) + 256) // Future 内嵌凭证
	aux.add(flights, perFlight)
	aux.add(s*int64(cfg.FillQueueSize+1), int64(unsafe.Sizeof(flight[V]{}))+int64(unsafe.Sizeof(fillCandidate[V]{}))+256)
	// 未取走的结果通过内嵌交付保留整个 flight，即使注册槽已经归还。
	aux.add(int64(cfg.MaxWaitersTotal), int64(unsafe.Sizeof(Future[V]{}))+int64(unsafe.Sizeof(flight[V]{}))+int64(unsafe.Sizeof(Handle[V]{}))+256)
	aux.add(controlSlots, int64(unsafe.Sizeof(controlOp[V]{}))+int64(cfg.MaxKeyBytes)+256)
	aux.add(int64(cfg.MaxExecutorTasks), int64(unsafe.Sizeof(flight[V]{}))+256) // closure and a possibly detached terminal flight
	aux.add(int64(cfg.MaxOwnedHandles), int64(unsafe.Sizeof(Handle[V]{}))+int64(unsafe.Sizeof(receipt{})))
	// A decision freezes at most candidate + old entry + eight victims. Load
	// observers can also retain an old model across a concurrent SetModel.
	aux.add(10*s+int64(cfg.MaxConcurrentLoads), cfg.MaxModelBytes)
	aux.add(s*maxFrozenModels*64, int64(unsafe.Sizeof(snapshotCell{}))) // 内置成本模型的惰性快照缓冲
	if fixed.err != nil {
		return cacheLayout{}, fixed.err
	}
	if aux.err != nil {
		return cacheLayout{}, aux.err
	}
	m := MemoryLimits{FixedBytes: fixed.bytes, AuxBytes: aux.bytes, ResidentBytes: cfg.MaxResidentBytes,
		PendingBytes: cfg.PendingBytes, RetiredBytes: cfg.RetiredBytes, ResponseBytes: cfg.ResponseBytes}
	for _, bytes := range []int64{m.FixedBytes, m.AuxBytes, m.ResidentBytes, m.PendingBytes, m.RetiredBytes, m.ResponseBytes} {
		if bytes > math.MaxInt64-m.TotalBytes {
			return cacheLayout{}, fmt.Errorf("ahkc: managed memory bound overflow")
		}
		m.TotalBytes += bytes
	}
	if cfg.MaxManagedBytes != 0 && m.TotalBytes > cfg.MaxManagedBytes {
		return cacheLayout{}, fmt.Errorf("ahkc: memory bound %d exceeds MaxManagedBytes %d", m.TotalBytes, cfg.MaxManagedBytes)
	}
	return cacheLayout{countMax: countMax, slots: slots, memory: m}, nil
}
