package ahkc

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"time"
)

// Executor 接受宿主提交的任务。契约（A.1 TRY_SUBMIT）：
// 接受则至多执行一次；拒绝则不执行；不得 caller-runs 或内联递归执行。
type Executor interface {
	TrySubmit(task func()) bool
}

// goroutineExecutor 是默认执行器：每个任务一个 goroutine。
type goroutineExecutor struct{}

func (goroutineExecutor) TrySubmit(task func()) bool {
	go task()
	return true
}

// CostModel 把请求率与成本类别映射到估值参数（A.4 LOOKUP_COST_MODEL）。
// 所有返回值必须是同口径的有限非负值；越界返回 ok=false（估值降级为 UNAVAILABLE）。
// 自定义模型的方法必须有界、非阻塞且不 panic；保留量遵守 MaxModelBytes。
//
//	r：有效可避免回源率（请求/秒）；c：单次可避免成本；h：持续服务成本/秒；
//	k：移除成本；K：安装成本。
type CostModel interface {
	// Version 单调递增；模型切换需要记录版本，防止新旧口径混比。
	Version() uint64
	Lookup(lambda float64, costClass uint32) (r, c, h, k, K float64, ok bool)
}

// SnapshotCostModel optionally freezes mutable observations at the decision's
// timestamp. The returned model is immutable; nil means unavailable.
type SnapshotCostModel interface {
	CostModel
	SnapshotAt(now int64) CostModel
}

// UnitCostModel identifies the unit shared by c/h/k/K. Models compared in one
// eviction decision must use the same unit. Legacy custom models use "custom".
type UnitCostModel interface {
	CostModel
	CostUnit() string
}

// SizedCostModel declares the retained size of a model and its immutable
// snapshot. Other custom models must honor Config.MaxModelBytes themselves.
type SizedCostModel interface {
	CostModel
	RetainedBytes() int64
}

func modelUnit(m CostModel) string {
	if units, ok := m.(UnitCostModel); ok {
		return units.CostUnit()
	}
	return "custom"
}

// hitCountModel 是内置默认模型：以请求命中数为目标，G = λ * min(W, τ)。
type hitCountModel struct{ version uint64 }

func (m hitCountModel) Version() uint64  { return m.version }
func (m hitCountModel) CostUnit() string { return "requests" }
func (m hitCountModel) Lookup(lambda float64, _ uint32) (r, c, h, k, K float64, ok bool) {
	if !allFiniteNonNegative(lambda) {
		return 0, 0, 0, 0, 0, false
	}
	return lambda, 1, 0, 0, 0, true
}

// Prior 是有限命名空间先验，样本不足时使用；候选与驻留一视同仁。
type Prior struct {
	Lambda float64 // 请求率先验（请求/秒）
	// BuiltAt 由校准时钟给出；0 表示由注册时刻起算。Age 超过 PriorMaxAge 后失效。
	BuiltAt int64
}

// NamespaceConfig 是单个命名空间的配置。
type NamespaceConfig struct {
	// TTL 是硬存活期：从 Flight 注册时刻（保守基准）起算，且 <= MaxTTL。
	// 缓存模式必填；ObserveOnly 可省略，默认 MaxTTL。
	TTL time.Duration
	// Tin 是准入阈值（窗口请求热度），由业务热点定义；默认 3。
	Tin float64
	// Tout 是退出阈值；默认 0.5*Tin。
	Tout float64
	// SampleP 是收益采样概率（每次逻辑请求独立 Bernoulli）；0 表示使用全局值。
	SampleP float64
	// Prior 是可选估值先验。强烈建议配置：没有 Prior 时，收益 sketch 热身前
	// （2×ValueDelta）及样本不足的 key 估值为 UNAVAILABLE，所有填充以 NO_ESTIMATE
	// 拒绝（可观察 Stats.FillUnavailable）。
	// 局限：仅靠 Prior 时同一命名空间内所有 key 使用同一 λ，缓存已满后候选与受害者的
	// 收益在剩余 TTL ≥ HorizonW 时相等，替换净收益为 0（NON_POSITIVE_GAIN）；
	// 要在满载时区分 key 价值，需要 MEASURED 估值或能区分价值的成本模型。
	Prior *Prior
	// Model 是可选成本模型；nil 使用内置命中数目标。
	Model CostModel
	// CostClass 是该命名空间默认成本类别。
	CostClass uint32
}

func (n *NamespaceConfig) withDefaults(global *Config) {
	if n.TTL == 0 && global.ObserveOnly {
		n.TTL = global.MaxTTL
	}
	if n.Tin == 0 {
		n.Tin = 3
	}
	if n.Tout == 0 {
		n.Tout = 0.5 * n.Tin
	}
	if n.SampleP == 0 {
		n.SampleP = global.SampleP
	}
	if n.Model == nil {
		n.Model = hitCountModel{version: 1}
	}
}

// Config 是缓存实例配置。所有数组、池、循环次数和工作预算均来自初始化配置（A.1）。
type Config struct {
	// ShardCount 分片数，会向上取整为 2 的幂；默认 16。
	ShardCount int
	// MaxEntries 全局条数额度 C；缓存模式必填，ObserveOnly 可省略。
	MaxEntries int64
	// MaxResidentBytes 全局驻留计费字节额度 B；缓存模式必填，ObserveOnly 可省略。
	MaxResidentBytes int64
	// MaxKeyBytes 完整 key 上限；默认 512。
	MaxKeyBytes int
	// MaxEntryBytes 单条目计费上限；默认 1MiB。
	MaxEntryBytes int64
	// MaxResultBytes 单次加载结果上限；默认 4MiB。
	MaxResultBytes int64
	// MaxManagedBytes caps the sum of fixed storage, bounded auxiliary state,
	// and every dynamic byte category. Zero derives that upper bound at New.
	MaxManagedBytes int64
	// HeatDelta 热度桶宽度 Δ；默认 1s。
	HeatDelta time.Duration
	// Grace 新发布条目宽限期，>= 2Δ；默认 2Δ。
	Grace time.Duration
	// ColdFor 持续低热度摘除门槛，>= Δ；默认 Δ。
	ColdFor time.Duration
	// ValueDelta 收益观测窗 Δv；默认 10s。
	// 冷启动：收益 sketch 在创建后 2Δv（默认 20s）内尚未热身，此期间只有配置了
	// NamespaceConfig.Prior 的命名空间才能得到估值（PRIOR），否则新填充一律以
	// NO_ESTIMATE 拒绝。
	ValueDelta time.Duration
	// SampleP 全局默认采样概率；默认 1/1024。
	// 估值进入 MEASURED 要求某 key 在窗口内的"样本数"≥NMin，即约需 NMin/SampleP
	// 次请求（默认约 8192 次/窗口）；请求频率低于该量级的 key 只能依赖 Prior。
	SampleP float64
	// NMin 最低样本量门槛；默认 8。仅工程门槛，不等于统计置信保证。
	// 与 SampleP 联动，见 SampleP 的说明。
	NMin float64
	// HorizonW 预测上限 W；默认 60s。
	HorizonW time.Duration
	// FillRatePerSec 每分片填充速率限制；默认 2000。
	FillRatePerSec float64
	// FillBytesPerSec limits billed bytes processed per shard per second.
	// Zero uses FillRatePerSec*MaxEntryBytes; burst permits one largest entry.
	FillBytesPerSec     float64
	RetireBytesPerRound int64
	// MaxConcurrentLoads 真实 I/O 并发上限；默认 256。
	MaxConcurrentLoads int
	// LoadTimeout 缓存侧加载期限（不取消底层 I/O）；默认 10s。
	// 到期通知由完成泵或周期扫描推进（见 DisableTicker），不是 Wait 内的定时器；
	// 需要更严格的调用方期限时请同时给 Fetch 传带超时的 ctx。
	LoadTimeout time.Duration
	// HandoffTimeout 填充交接期限，应显著短于 TTL；默认 2s。
	HandoffTimeout time.Duration
	// PeriodicInterval 定时触发周期；默认 25ms。
	PeriodicInterval time.Duration
	// DisableTicker 为 true 时不启动内置周期触发，由宿主驱动 MaintenanceStep。
	// 注意：以下进展都依赖周期推进，宿主必须以不大于 PeriodicInterval 的节奏调用
	// MaintenanceStep，否则它们会停滞：空闲时的 TTL 摘除与冷却、Fetch 等待者的
	// LoadTimeout 到期通知（Future.Wait 自身不带定时器，只响应结果就绪和 ctx）、
	// 额度转移、退休回收与 Close 达到 RECLAIMED、热点窗口历史与通知。
	DisableTicker bool
	// MaxTTL 最大 TTL；默认 1h。
	MaxTTL time.Duration
	// ObserveOnly 为 true 时实例运行在仅统计模式：热度门卫、稀疏采样与
	// singleflight 合并照常工作，但值永不驻留（填充候选统一以
	// OBSERVE_ONLY 拒绝并计数）。用于先观测业务数据的重复度，再决定
	// 是否/如何启用缓存。Get 不会命中，但仍可能返回 ErrBusy / ErrClosed。
	ObserveOnly bool
	// Namespaces 初始注册的命名空间；也可之后用 RegisterNamespace 注册。
	Namespaces map[string]NamespaceConfig
	// Executor 宿主执行器；nil 使用默认 goroutine 执行器。
	Executor Executor
	// Clock 时间源；nil 使用真实单调时钟。
	Clock Clock

	// Advanced 收纳内部结构尺寸与维护配额等调优项，全部有默认值，通常无需设置。
	Advanced
}

// Advanced 是 Config 的调优子结构（嵌入，字段可经 Config 直接读取）：
// 索引 / 热度门卫 / 估值 sketch / 时间轮 / 队列 / 各类预算与维护配额的尺寸。
// 默认值按核心配置推导，只在压测或特殊负载下才需要调整。
type Advanced struct {
	// MaxNamespaces 命名空间上限；默认 64。
	MaxNamespaces int
	// MaxModelBytes bounds each retained model, including its snapshot and
	// referenced calibration data. Custom models must honor this contract.
	MaxModelBytes int64
	// MaxLoadFactor 索引最大负载率；默认 0.7。
	MaxLoadFactor float64
	// ProbeLimit 固定探测范围；默认 32 且不超过槽数。
	ProbeLimit int
	// HeatCap 桶内计数饱和上限 Hcap；默认 4×最大 Tin，<= 4095。
	HeatCap uint32
	// GateRows / GateWidth 非驻留门卫 d×w；默认 4×4096（每分片）。
	GateRows, GateWidth int
	// GateEpsilon / GateDelta are optional sizing hints: w=ceil(e/epsilon),
	// d=ceil(-ln(delta)). Saturation, sliding buckets and dropped updates
	// prevent applying the original Count-Min error guarantee unchanged.
	GateEpsilon, GateDelta float64
	// HeatRetries 热度更新最大尝试 R；默认 2。
	HeatRetries int
	// ValueRows / ValueWidth 收益 sketch；默认 4×512（每分片）。
	ValueRows, ValueWidth int
	// PriorMaxAge 先验最大年龄；默认 10min。
	PriorMaxAge time.Duration
	// ModelMaxAge is the maximum age of externally calibrated model parameters.
	// The built-in request-count objective does not require calibration.
	ModelMaxAge time.Duration
	// MaxSampleLag 样本最大滞后；默认 2Δv。
	MaxSampleLag time.Duration
	// SampleStripes / SampleBufferSize 样本条带与缓冲；默认 4×1024（每分片）。
	SampleStripes, SampleBufferSize int
	// SieveScanLimit 整次填充扫描上限 B_scan；默认 64。
	SieveScanLimit int
	// VictimLimit 候选集上限 m，<= 8；默认 8。
	VictimLimit int
	// MaxSubsetChecks 单次填充子集检查上限；默认 256（2^m）。
	MaxSubsetChecks int
	// RetireLimitPerRound / RetireBytesPerRound cap all structural removals in
	// a round, including lifecycle cleanup while searching for fill victims.
	RetireLimitPerRound int
	// ReclaimBytesPerRound caps billed bytes submitted for reclamation per round.
	ReclaimBytesPerRound int64
	// MaxExecutorTasks bounds accepted but unfinished callbacks, including
	// loader dispatch, maintenance, completion pumps and cleanup. Zero derives
	// a bound from the configured fixed record/queue capacities.
	MaxExecutorTasks int
	// FlightStripes / FlightsPerStripe 每分片 Flight 条带与槽位；默认 8×64。
	FlightStripes, FlightsPerStripe int
	// MaxWaitersPerKey / MaxWaitersTotal 等待者上限；默认 32 / 16384。
	MaxWaitersPerKey, MaxWaitersTotal int
	// ReaderSlotsPerShard 每分片 epoch 保护槽；默认 128。
	ReaderSlotsPerShard int
	// MaxOwnedPinsPerEntry 拥有型句柄上限；默认 1024。
	MaxOwnedPinsPerEntry int64
	// MaxOwnedHandles bounds active owned handle metadata globally.
	MaxOwnedHandles int
	// PeriodicWorkLimit bounds shard/flight/control visits in one global tick.
	PeriodicWorkLimit int
	// RoundWorkLimit 每轮维护事件数上限；默认 512。
	RoundWorkLimit int
	// RoundSoftTimeBudget 每轮维护软时间预算（事件配额之外的补充长尾约束，
	// 到预算即让出、未完成类别由周期触发续行；首轮类别扫描总是执行）。默认 1ms。
	RoundSoftTimeBudget time.Duration
	// WheelTick 基础 tick；默认 100ms。
	WheelTick time.Duration
	// WheelSlots 每层槽数 K；默认 64。
	WheelSlots int
	// WheelLevels 层数 L；默认 4。总跨度必须覆盖 MaxTTL+滞后余量。
	WheelLevels int
	// MaxWheelLag 允许的时间轮滞后（tick 数）；默认 64。
	MaxWheelLag int64
	// PendingBytes 交接预算 M_pending；默认 MaxResidentBytes/4。
	PendingBytes int64
	// ResponseBytes 响应预算 M_response；默认 MaxConcurrentLoads × MaxResultBytes。
	ResponseBytes int64
	// RetiredBytes 退休预算 M_retired；默认 MaxResidentBytes/4。
	RetiredBytes int64
	// RetiredRecords 退休记录上限；默认 4096。
	RetiredRecords int
	// FillQueueSize 每分片填充队列；默认 256。
	FillQueueSize int
	// ControlQueueSize 每分片控制队列（失效确认与额度转移各一条，容量均为此值）；默认 64。
	ControlQueueSize int
	// ObserveHotKeys 每分片热点 key 追踪槽位数（固定，初始化后不变）：
	// 按 (命名空间, key) 记录窗口加载热度（LoadHeat）、累计真实加载次数
	// （Loads）与被合并的等待请求数（Merged），经 Cache.HotKeys 读取。
	// 0 在 ObserveOnly 模式下默认 128（其余情况默认关闭）；负数表示
	// 在 ObserveOnly 模式下也显式关闭。槽位满时逐出窗口加载热度最低者。
	ObserveHotKeys int
	// HotHistoryWindows 保留最近 N 份窗口快照，默认 0 不保留。
	// 周期维护按 HotHistoryInterval 滚动；需要回源表或请求表。
	HotHistoryWindows int
	// HotHistoryTopN 每份窗口快照保留的热点条数；默认 16。
	HotHistoryTopN int
	// OnHotWindow 窗口通知（可选）：每个历史窗口发布后携带该窗口的快照调用至多一次，
	// 经 Executor.TrySubmit 异步投递。回调串行、Seq 单调、可能缺窗；上一个回调仍在
	// 执行（Stats.HotWindowBusy）或提交失败时丢弃该通知，二者都计入 Stats.HotWindowDropped。
	// 回调必须有界、非阻塞，榜单归回调所有；panic 被隔离（Stats.HotExportPanics）；
	// 慢回调会推迟 Close 到达 RECLAIMED。推荐只发信号，由消费者调用 HotWindowsSince 补齐。
	// 需要 ObserveHotKeys > 0 或 ObserveRequestKeys > 0。
	OnHotWindow func(HotSnapshot)
	// HotHistoryInterval 历史窗口宽度，与 HeatDelta 互相独立；默认等于 HeatDelta。
	// 内置时钟下窗口边界按 Unix 时间对齐（整秒/整分），自定义时钟从时钟原点对齐。
	// 显式设置时范围 [1ms, 24h]，且不小于 (ceil(总追踪槽位/4096)+1) × PeriodicInterval
	// （增量采集需要跨周期完成）。
	HotHistoryInterval time.Duration
	// ObserveRequestKeys 每分片请求热点追踪槽位数（含命中；固定）。默认 0 关闭；
	// 需要启用 HotHistoryWindows 或 OnHotWindow。
	ObserveRequestKeys int
	// ObserveRequestSampleP 请求热点采样率 q，默认 1/1024，范围 [2^-64,1]。
	// 窗口请求数以样本数 × 1/q 估计；锁竞争丢弃计入 HotRequestSamplesDropped。
	ObserveRequestSampleP float64
	// HotKeyLabel 导出映射钩子（可选）：逐条作用于 HotKeys / HotKeyHistory /
	// HotWindowsSince / OnHotWindow 的输出，可返回原样、哈希或截断后的标签；
	// keep=false 丢弃该条；nil 时原样导出。必须并发安全、有界、非阻塞；
	// panic 剔除该条并计入 Stats.HotExportPanics；从不在维护线程内执行。
	HotKeyLabel func(namespace, key string) (label string, keep bool)
}

func (c *Config) applyDefaults() {
	if c.ShardCount <= 0 {
		c.ShardCount = 16
	}
	c.ShardCount = nextPow2(c.ShardCount)
	if c.MaxKeyBytes <= 0 {
		c.MaxKeyBytes = 512
	}
	if c.MaxEntryBytes <= 0 {
		c.MaxEntryBytes = 1 << 20
	}
	if c.MaxResultBytes <= 0 {
		c.MaxResultBytes = 4 << 20
	}
	if c.ObserveOnly {
		if c.MaxEntries == 0 {
			c.MaxEntries = int64(c.ShardCount)
		}
		if c.MaxResidentBytes == 0 {
			c.MaxResidentBytes = c.MaxEntryBytes
		}
	}
	if c.MaxNamespaces <= 0 {
		c.MaxNamespaces = 64
	}
	if c.MaxModelBytes == 0 {
		c.MaxModelBytes = 64 << 10
	}
	if c.MaxLoadFactor <= 0 {
		c.MaxLoadFactor = 0.7
	}
	if c.ProbeLimit <= 0 {
		c.ProbeLimit = 32
	}
	if c.HeatDelta <= 0 {
		c.HeatDelta = time.Second
	}
	// 门卫误差预算推导：有效 ε/δ 对优先于默认尺寸（覆盖 GateRows/GateWidth）。
	if c.GateEpsilon > 0 && c.GateEpsilon < 1 && c.GateDelta > 0 && c.GateDelta < 1 {
		c.GateWidth = int(math.Ceil(math.E / c.GateEpsilon))
		c.GateRows = int(math.Ceil(-math.Log(c.GateDelta)))
	}
	if c.GateRows <= 0 {
		c.GateRows = 4
	}
	if c.GateWidth <= 0 {
		c.GateWidth = 4096
	}
	if c.HeatRetries <= 0 {
		c.HeatRetries = 2
	}
	if c.Grace <= 0 {
		c.Grace = 2 * c.HeatDelta
	}
	if c.ColdFor <= 0 {
		c.ColdFor = c.HeatDelta
	}
	if c.ValueDelta <= 0 {
		c.ValueDelta = 10 * time.Second
	}
	if c.ValueRows <= 0 {
		c.ValueRows = 4
	}
	if c.ValueWidth <= 0 {
		c.ValueWidth = 512
	}
	if c.SampleP <= 0 {
		c.SampleP = 1.0 / 1024
	}
	if c.NMin <= 0 {
		c.NMin = 8
	}
	if c.HorizonW <= 0 {
		c.HorizonW = time.Minute
	}
	if c.PriorMaxAge <= 0 {
		c.PriorMaxAge = 10 * time.Minute
	}
	if c.ModelMaxAge == 0 {
		c.ModelMaxAge = 10 * time.Minute
	}
	if c.MaxSampleLag <= 0 {
		c.MaxSampleLag = 2 * c.ValueDelta
	}
	if c.SampleStripes <= 0 {
		c.SampleStripes = 4
	}
	if c.SampleBufferSize <= 0 {
		c.SampleBufferSize = 1024
	}
	if c.SieveScanLimit <= 0 {
		c.SieveScanLimit = 64
	}
	if c.VictimLimit <= 0 {
		c.VictimLimit = 8
	}
	if c.MaxSubsetChecks <= 0 {
		if c.VictimLimit <= 8 {
			c.MaxSubsetChecks = 1 << uint(c.VictimLimit)
		}
	}
	if c.FillRatePerSec <= 0 {
		c.FillRatePerSec = 2000
	}
	if c.FillBytesPerSec == 0 {
		c.FillBytesPerSec = c.FillRatePerSec * float64(c.MaxEntryBytes)
	}
	if c.MaxConcurrentLoads <= 0 {
		c.MaxConcurrentLoads = 256
	}
	if c.LoadTimeout <= 0 {
		c.LoadTimeout = 10 * time.Second
	}
	if c.HandoffTimeout <= 0 {
		c.HandoffTimeout = 2 * time.Second
	}
	if c.FlightStripes <= 0 {
		c.FlightStripes = 8
	}
	if c.FlightsPerStripe <= 0 {
		c.FlightsPerStripe = 64
	}
	if c.MaxWaitersPerKey <= 0 {
		c.MaxWaitersPerKey = 32
	}
	if c.MaxWaitersTotal <= 0 {
		c.MaxWaitersTotal = 16384
	}
	if c.ReaderSlotsPerShard <= 0 {
		c.ReaderSlotsPerShard = 128
	}
	if c.MaxOwnedPinsPerEntry <= 0 {
		c.MaxOwnedPinsPerEntry = 1024
	}
	if c.MaxOwnedHandles == 0 {
		c.MaxOwnedHandles = c.MaxWaitersTotal
	}
	if c.PeriodicInterval <= 0 {
		c.PeriodicInterval = 25 * time.Millisecond
	}
	if c.PeriodicWorkLimit == 0 {
		c.PeriodicWorkLimit = 512
	}
	if c.RoundWorkLimit <= 0 {
		c.RoundWorkLimit = 512
	}
	if c.RetireLimitPerRound == 0 {
		c.RetireLimitPerRound = c.RoundWorkLimit / 2
	}
	if c.RetireBytesPerRound == 0 {
		if bytes, ok := checkedProduct(int64(c.RetireLimitPerRound), c.MaxEntryBytes); ok {
			c.RetireBytesPerRound = bytes
		} else {
			c.RetireBytesPerRound = -1
		}
	}
	if c.ReclaimBytesPerRound == 0 {
		c.ReclaimBytesPerRound = c.RetireBytesPerRound
	}
	if c.RoundSoftTimeBudget <= 0 {
		c.RoundSoftTimeBudget = time.Millisecond
	}
	if c.WheelTick <= 0 {
		c.WheelTick = 100 * time.Millisecond
	}
	if c.WheelSlots <= 0 {
		c.WheelSlots = 64
	}
	if c.WheelLevels <= 0 {
		c.WheelLevels = 4
	}
	if c.MaxWheelLag <= 0 {
		c.MaxWheelLag = 64
	}
	if c.MaxTTL <= 0 {
		c.MaxTTL = time.Hour
	}
	if c.PendingBytes <= 0 {
		c.PendingBytes = c.MaxResidentBytes / 4
		if c.PendingBytes < c.MaxEntryBytes {
			c.PendingBytes = c.MaxEntryBytes
		}
	}
	if c.ResponseBytes <= 0 {
		// 响应预算按“每个在途加载可能持有 maxResultBytes”推导，
		// 与 MaxConcurrentLoads 自洽；显式设置更小值则收紧加载并发。
		if bytes, ok := checkedProduct(int64(c.MaxConcurrentLoads), c.MaxResultBytes); ok {
			c.ResponseBytes = bytes
		} else {
			c.ResponseBytes = -1
		}
	}
	if c.RetiredBytes <= 0 {
		c.RetiredBytes = c.MaxResidentBytes / 4
		if c.RetiredBytes < c.MaxEntryBytes {
			c.RetiredBytes = c.MaxEntryBytes
		}
	}
	if c.RetiredRecords <= 0 {
		c.RetiredRecords = 4096
	}
	if c.FillQueueSize <= 0 {
		c.FillQueueSize = 256
	}
	if c.ControlQueueSize <= 0 {
		c.ControlQueueSize = 64
	}
	if c.ObserveHotKeys == 0 && c.ObserveOnly {
		c.ObserveHotKeys = 128 // 观察模式开箱即用；显式关闭用负值
	}
	if c.ObserveHotKeys < 0 {
		c.ObserveHotKeys = 0
	}
	if c.HotHistoryTopN <= 0 {
		c.HotHistoryTopN = 16
	}
	if c.HotHistoryInterval == 0 {
		c.HotHistoryInterval = c.HeatDelta
	}
	if c.ObserveRequestKeys > 0 && c.ObserveRequestSampleP == 0 {
		c.ObserveRequestSampleP = 1.0 / 1024
	}
	if c.MaxExecutorTasks == 0 {
		flights, ok := checkedProduct(int64(c.ShardCount), int64(c.FlightStripes))
		if ok {
			flights, ok = checkedProduct(flights, int64(c.FlightsPerStripe))
		}
		if !ok || flights > (math.MaxInt64-int64(c.RetiredRecords)-2*int64(c.ShardCount)-2)/2 {
			c.MaxExecutorTasks = -1
		} else {
			c.MaxExecutorTasks = int(2*flights + int64(c.RetiredRecords) + 2*int64(c.ShardCount) + 2)
		}
	}
	if c.Executor == nil {
		c.Executor = goroutineExecutor{}
	}
	if c.Clock == nil {
		c.Clock = newRealClock()
	}
}

// validate 执行初始化校验；无法满足时拒绝创建实例。
func (c *Config) validate() error {
	if err := validateNumbers(*c); err != nil {
		return err
	}
	if isTypedNil(c.Clock) || isTypedNil(c.Executor) {
		return errors.New("ahkc: Clock/Executor cannot be typed nil")
	}
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.MaxEntries <= 0 {
		add("MaxEntries 必须 > 0")
	}
	if c.MaxEntries > math.MaxInt32 || c.ShardCount > 1<<16 || uint64(c.MaxNamespaces) > math.MaxUint32 {
		add("条目池槽、分片 ID 或命名空间 ID 超出表示范围")
	}
	for _, dimension := range []int{c.MaxKeyBytes, c.ProbeLimit, c.ValueRows, c.ValueWidth, c.SampleStripes, c.SampleBufferSize,
		c.FlightStripes, c.FlightsPerStripe, c.MaxWaitersPerKey, c.MaxWaitersTotal, c.MaxOwnedHandles, c.MaxConcurrentLoads, c.MaxExecutorTasks,
		c.ReaderSlotsPerShard, c.WheelSlots, c.RetiredRecords, c.FillQueueSize, c.ControlQueueSize, c.ObserveHotKeys, c.ObserveRequestKeys, c.HotHistoryWindows, c.HotHistoryTopN} {
		if dimension > 1<<24 {
			add("固定结构维度超过 2^24")
		}
	}
	if c.MaxResidentBytes <= 0 {
		add("MaxResidentBytes 必须 > 0")
	}
	if c.ShardCount <= 0 || c.ShardCount&(c.ShardCount-1) != 0 {
		add("ShardCount 必须是 2 的幂，得到 %d", c.ShardCount)
	}
	if int64(c.ShardCount) > c.MaxEntries {
		add("MaxEntries(%d) 必须 >= ShardCount(%d)", c.MaxEntries, c.ShardCount)
	}
	if c.HeatCap > heatCountMax {
		add("HeatCap(%d) 超过计数位宽上限 %d", c.HeatCap, heatCountMax)
	}
	if c.HeatCap == 0 {
		add("HeatCap 必须 > 0")
	}
	// 门卫误差预算：部分设置或越界均为配置错误（applyDefaults 只在成对
	// 且合法时推导，此处统一拒绝其余组合）。
	if c.GateEpsilon != 0 || c.GateDelta != 0 {
		if !(c.GateEpsilon > 0 && c.GateEpsilon < 1) || !(c.GateDelta > 0 && c.GateDelta < 1) {
			add("GateEpsilon(%v)/GateDelta(%v) 必须成对设置且都在 (0,1) 内", c.GateEpsilon, c.GateDelta)
		}
	}
	if c.GateRows > 64 || c.GateWidth > 1<<22 {
		add("GateRows(%d)/GateWidth(%d) 超出合理上界 [1,64]×[1,2^22]", c.GateRows, c.GateWidth)
	}
	if c.ValueRows > 64 || c.ValueWidth > 1<<22 || c.SampleStripes > 64 || c.SampleBufferSize > 1<<20 ||
		c.FlightStripes > 256 || c.FlightsPerStripe > 256 || c.ReaderSlotsPerShard > 1<<16 || c.WheelSlots > 1<<16 {
		add("统计、Flight、读者或时间轮维度超过支持的固定结构上限")
	}
	if c.HotHistoryWindows < 0 {
		add("HotHistoryWindows 必须 >= 0")
	}
	historyOn := c.HotHistoryWindows > 0 || c.OnHotWindow != nil
	if historyOn && c.ObserveHotKeys <= 0 && c.ObserveRequestKeys <= 0 {
		add("HotHistoryWindows/OnHotWindow 需要 ObserveHotKeys > 0 或 ObserveRequestKeys > 0")
	}
	if c.ObserveRequestKeys > 0 && !historyOn {
		add("ObserveRequestKeys 需要启用 HotHistoryWindows 或 OnHotWindow")
	}
	if q := c.ObserveRequestSampleP; q != 0 && !(q >= math.Ldexp(1, -64) && q <= 1) {
		add("ObserveRequestSampleP(%v) 必须在 [2^-64,1]", q)
	}
	// 与 HeatDelta 不同即视为显式设置；默认值保持既有行为，缺窗由 HotSnapshot.Skipped 如实报告。
	if iv := c.HotHistoryInterval; iv != c.HeatDelta {
		slots := int64(c.ShardCount) * int64(c.ObserveHotKeys+c.ObserveRequestKeys)
		need := (slots+hotCollectBudget-1)/hotCollectBudget + 1
		switch {
		case iv < time.Millisecond || iv > 24*time.Hour:
			add("HotHistoryInterval(%v) 必须在 [1ms,24h]", iv)
		case historyOn && int64(iv) < need*int64(c.PeriodicInterval):
			add("HotHistoryInterval(%v) 必须 >= %d × PeriodicInterval(%v)：增量采集需要跨周期完成", iv, need, c.PeriodicInterval)
		}
	}
	if c.VictimLimit > 8 {
		add("VictimLimit(%d) 必须 <= 8", c.VictimLimit)
	}
	if c.MaxSubsetChecks > 1<<uint(c.VictimLimit) {
		add("MaxSubsetChecks 超过 2^VictimLimit")
	}
	if c.Grace < 2*c.HeatDelta {
		add("Grace(%v) 必须 >= 2*HeatDelta(%v)", c.Grace, c.HeatDelta)
	}
	if c.ColdFor < c.HeatDelta {
		add("ColdFor(%v) 必须 >= HeatDelta(%v)", c.ColdFor, c.HeatDelta)
	}
	if c.SampleP < math.Ldexp(1, -64) || c.SampleP > 1 {
		add("SampleP(%v) 必须在 [2^-64,1]", c.SampleP)
	}
	if c.MaxLoadFactor <= 0 || c.MaxLoadFactor > 1 {
		add("MaxLoadFactor 必须在 (0,1]")
	}
	if c.RoundWorkLimit < classCount*2 {
		add("RoundWorkLimit 必须至少容纳每类维护的进展配额")
	}
	if c.PeriodicWorkLimit < 100 {
		add("PeriodicWorkLimit 必须 >= 100")
	}
	if c.WheelSlots < 2 || c.WheelLevels > 16 {
		add("WheelSlots 必须 >= 2，WheelLevels 必须 <= 16")
	}
	if c.HeatDelta > time.Duration(math.MaxInt64/2) || c.ValueDelta > time.Duration(math.MaxInt64/2) {
		add("双桶窗口跨度溢出")
	}
	// 时间轮总跨度必须覆盖 maxTTL + maxWheelLag*tick + 舍入余量（A.7）。
	span, spanOK := checkedProduct(int64(c.WheelTick), powInt(int64(c.WheelSlots), c.WheelLevels))
	// 最大 delta = ceil(MaxTTL/tick) + lag + 1，且可调度要求 delta < K^L，故余量为 lag+2 个 tick。
	lag, lagOK := checkedProduct(c.MaxWheelLag+2, int64(c.WheelTick))
	if !spanOK || !lagOK || lag > math.MaxInt64-int64(c.MaxTTL) || span < int64(c.MaxTTL)+lag {
		add("时间轮跨度 %v 不足以覆盖 MaxTTL(%v)+滞后余量", time.Duration(span), c.MaxTTL)
	}
	if c.ProbeLimit <= 0 {
		add("ProbeLimit 必须 > 0")
	}
	if c.HandoffTimeout >= c.MaxTTL {
		add("HandoffTimeout(%v) 应显著短于 TTL 上限(%v)", c.HandoffTimeout, c.MaxTTL)
	}
	if c.MaxEntryBytes > c.MaxResidentBytes {
		add("MaxEntryBytes 不能超 MaxResidentBytes")
	}
	if c.MaxEntryBytes > c.MaxResultBytes {
		add("MaxResultBytes 必须至少容纳一个最大条目")
	}
	if len(c.Namespaces) > c.MaxNamespaces {
		add("命名空间数量超过 MaxNamespaces")
	}
	if c.PendingBytes < c.MaxEntryBytes {
		add("PendingBytes(%d) 至少容纳一个最大条目(%d)", c.PendingBytes, c.MaxEntryBytes)
	}
	if c.RetiredBytes < c.MaxEntryBytes {
		add("RetiredBytes(%d) 至少容纳一个最大条目(%d)", c.RetiredBytes, c.MaxEntryBytes)
	}
	if c.ResponseBytes < c.MaxResultBytes {
		add("ResponseBytes(%d) 至少容纳一个最大结果(%d)", c.ResponseBytes, c.MaxResultBytes)
	}
	if c.RetireLimitPerRound < 1 {
		add("RetireLimitPerRound(%d) 必须 >= 1", c.RetireLimitPerRound)
	}
	if c.RetireBytesPerRound < c.MaxEntryBytes {
		add("RetireBytesPerRound(%d) 至少容纳一个最大条目(%d)，否则该条目永远无法摘除", c.RetireBytesPerRound, c.MaxEntryBytes)
	}
	if c.ReclaimBytesPerRound < c.MaxEntryBytes {
		add("ReclaimBytesPerRound 至少容纳一个最大条目")
	}
	for name, ns := range c.Namespaces {
		if name == "" {
			add("命名空间名称不能为空")
		} else if len(name) > c.MaxKeyBytes {
			add("命名空间名称: %w", ErrKeyTooLarge)
		}
		ns.withDefaults(c)
		if err := ns.validate(c); err != nil {
			add("命名空间 %q: %v", name, err)
		}
	}
	return errors.Join(errs...)
}

// Validate numeric inputs before applying defaults, so explicit invalid values
// cannot silently become a different configuration. Reflection is init-only.
func validateNumbers(config any) error {
	v := reflect.ValueOf(config)
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		if name == "ObserveHotKeys" { // a negative value explicitly disables tracking
			continue
		}
		field := v.Field(i)
		switch field.Kind() {
		case reflect.Struct:
			if err := validateNumbers(field.Interface()); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		case reflect.Int, reflect.Int64:
			if field.Int() < 0 {
				return fmt.Errorf("%s 必须非负", name)
			}
		case reflect.Float64:
			if !allFiniteNonNegative(field.Float()) {
				return fmt.Errorf("%s 必须有限且非负", name)
			}
		}
	}
	return nil
}

func (n *NamespaceConfig) validate(c *Config) error {
	if err := validateNumbers(*n); err != nil {
		return err
	}
	if n.TTL <= 0 || n.TTL > c.MaxTTL {
		return errors.New("TTL 必须在 (0, MaxTTL] 内")
	}
	if !(n.Tout > 0 && n.Tout < n.Tin && n.Tin < float64(c.HeatCap)) {
		return errors.New("需要 0 < Tout < Tin < HeatCap")
	}
	if n.SampleP < math.Ldexp(1, -64) || n.SampleP > 1 {
		return errors.New("SampleP 必须在 [2^-64,1] 内")
	}
	if n.Prior != nil && (!allFiniteNonNegative(n.Prior.Lambda) || n.Prior.BuiltAt < 0) {
		return errors.New("先验请求率和时间必须有限且非负")
	}
	return validateModel(n.Model, c.MaxModelBytes)
}

func validateModel(m CostModel, maxBytes int64) error {
	if m == nil {
		return nil
	}
	if isTypedNil(m) {
		return errors.New("ahkc: typed nil model")
	}
	if sized, ok := m.(SizedCostModel); ok {
		if bytes := sized.RetainedBytes(); bytes <= 0 || bytes > maxBytes {
			return errors.New("ahkc: model exceeds MaxModelBytes")
		}
	}
	return nil
}

func isTypedNil(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan, reflect.Func:
		return v.IsNil()
	}
	return false
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		if p > math.MaxInt/2 {
			return 0
		}
		p <<= 1
	}
	return p
}

func powInt(base int64, exp int) int64 {
	if base < 1 || exp < 0 || exp > 63 {
		return 0
	}
	r := int64(1)
	for i := 0; i < exp; i++ {
		if base < 1 || r > math.MaxInt64/base {
			return 0
		}
		r *= base
	}
	return r
}

func checkedProduct(a, b int64) (int64, bool) {
	if a <= 0 || b <= 0 || a > math.MaxInt64/b {
		return 0, false
	}
	return a * b, true
}
