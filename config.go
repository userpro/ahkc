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

// Mode 是实例的运行模式。
type Mode uint8

const (
	// ModeCache 是正常缓存：命中交付驻留值。
	ModeCache Mode = iota
	// ModeShadow 运行影子缓存：准入、估值、淘汰、TTL 与失效全部照缓存模式运行，
	// 但驻留条目不保留 value，且命中对调用方一律表现为 miss（Fetch 照旧回源），
	// 因此业务行为与不开缓存一致。Stats.Hits / Misses 即“若启用缓存”的虚拟命中 / 未命中，
	// 命中率 = Hits/(Hits+Misses)。与缓存模式相同，必须配置容量与命名空间 TTL。
	ModeShadow
	// ModeObserve 是仅统计模式：热度门卫、稀疏采样与 singleflight 合并照常工作，
	// 但值永不驻留（填充候选统一以 OBSERVE_ONLY 拒绝并计数）。用于先观测业务数据的
	// 重复度，再决定是否/如何启用缓存。Get 不会命中，但仍可能返回 ErrBusy / ErrClosed。
	// 回源热点榜和请求热点榜与其他模式一样，需配置 Hot.LoadSlots / Hot.RequestSlots 才会开启。
	ModeObserve
)

func (c *Config) observeOnly() bool { return c.Mode == ModeObserve }
func (c *Config) shadow() bool      { return c.Mode == ModeShadow }

// maxTTL 是 TTL 上限，只为约束时钟加法不溢出。
//
// shortcut: 固定 1 年而非可配置；需要更长存活期时改为 Config 字段。
const maxTTL = 365 * 24 * time.Hour

// observeTTL 是仅统计模式省略 TTL 时的内部默认值。
const observeTTL = time.Hour

// NamespaceConfig 是单个命名空间的配置。
type NamespaceConfig struct {
	// TTL 是硬存活期：从 Flight 注册时刻（保守基准）起算，且不超过 1 年。
	// 缓存模式必填；ModeObserve 可省略，默认 1 小时。
	TTL time.Duration
	// Tin 是准入阈值（窗口请求热度），由业务热点定义；默认 3。
	Tin float64
}

func (n *NamespaceConfig) withDefaults(global *Config) {
	if n.TTL == 0 && global.observeOnly() {
		n.TTL = observeTTL
	}
	if n.Tin == 0 {
		n.Tin = 3
	}
}

// Config 是缓存实例配置。零值即默认；通常只需设置容量字段和命名空间，其余按用途分组：
// Heat 热度，Evict 淘汰，Hot 热点统计，Memory 内存区，Limits 资源上限，Runtime 宿主资源与维护节奏。
type Config struct {
	// Shards 分片数，会向上取整为 2 的幂；默认 16。
	Shards int
	// MaxEntries 全局条数额度 C；缓存模式必填，ModeObserve 可省略。
	MaxEntries int64
	// MaxBytes 全局驻留计费字节额度 B；缓存模式必填，ModeObserve 可省略。
	MaxBytes int64
	// MaxKeyBytes 完整 key 上限；默认 512，不超过 65535。
	MaxKeyBytes int
	// MaxEntryBytes 单条目计费上限；默认 1MiB。
	MaxEntryBytes int64
	// MaxResultBytes 单次加载结果上限；默认 4MiB。
	MaxResultBytes int64
	// Mode 是运行模式；默认 ModeCache。
	Mode Mode
	// Namespaces 初始注册的命名空间；也可之后用 RegisterNamespace 注册。
	Namespaces map[string]NamespaceConfig

	Heat    HeatConfig
	Evict   EvictConfig
	Hot     HotConfig
	Memory  MemoryConfig
	Limits  LimitsConfig
	Runtime RuntimeConfig
}

// HeatConfig 是请求热度（准入与淘汰的依据）的配置。
type HeatConfig struct {
	// Delta 热度桶宽度 Δ；默认 1s。
	Delta time.Duration
	// GateCells 是每分片热度门卫的单元数（每 8 个单元一条缓存行）。默认
	// clamp(每分片条数额度/2, 16384, 2^24)，向上取 8 的倍数——这是按容量的近似；
	// 规模场景应按"每个 Delta 内去重后的未命中 key 数的 4 倍"设置。
	GateCells int
}

// EvictConfig 是收益估值与淘汰的配置。
type EvictConfig struct {
	// Horizon 预测上限 W；默认 60s。
	Horizon time.Duration
	// VictimLimit 候选集上限 m，<= 8；默认 8。精确子集检查上限为 2^VictimLimit。
	VictimLimit int
}

// HotConfig 是热点统计的配置。槽位数取 0 即关闭对应的榜单。
type HotConfig struct {
	// LoadSlots 每分片回源热点 key 追踪槽位数（固定，初始化后不变）：
	// 按 (命名空间, key) 记录窗口加载热度（LoadHeat）、累计真实加载次数
	// （Loads）与被合并的等待请求数（Merged），经 Cache.HotKeys 读取。
	// 槽位满时逐出窗口计数最低者。
	LoadSlots int
	// RequestSlots 每分片请求热点追踪槽位数（含命中；固定）。默认 0 关闭；
	// 需要启用 Windows 或 OnWindow。
	RequestSlots int
	// RequestSampleP 请求热点采样率 q，默认 1/1024，范围 [2^-64,1]。
	// 窗口请求数以样本数 × 1/q 估计；锁竞争丢弃计入 HotRequestSamplesDropped。
	RequestSampleP float64
	// Windows 保留最近 N 份窗口快照，默认 0 不保留。
	// 周期维护按 Interval 滚动；需要回源表或请求表。
	Windows int
	// TopN 每份窗口快照保留的热点条数；默认 16。
	TopN int
	// Interval 历史窗口宽度，与 Heat.Delta 互相独立；默认等于 Heat.Delta。
	// 内置时钟下窗口边界按 Unix 时间对齐（整秒/整分），自定义时钟从时钟原点对齐。
	// 显式设置时范围 [1ms, 24h]，且不小于 (ceil(总追踪槽位/4096)+1) × PeriodicInterval
	// （增量采集需要跨周期完成）。
	Interval time.Duration
	// OnWindow 窗口通知（可选）：每个历史窗口发布后携带该窗口的快照调用至多一次，
	// 经 Executor.TrySubmit 异步投递。回调串行、Seq 单调、可能缺窗；上一个回调仍在
	// 执行（Stats.HotWindowBusy）或提交失败时丢弃该通知，二者都计入 Stats.HotWindowDropped。
	// 回调必须有界、非阻塞，榜单归回调所有；panic 被隔离（Stats.HotExportPanics）；
	// 慢回调会推迟 Close 到达 RECLAIMED。推荐只发信号，由消费者调用 HotWindowsSince 补齐。
	// 需要 LoadSlots > 0 或 RequestSlots > 0。
	OnWindow func(HotSnapshot)
	// Label 导出映射钩子（可选）：逐条作用于 HotKeys / HotKeyHistory /
	// HotWindowsSince / OnWindow 的输出，可返回原样、哈希或截断后的标签；
	// keep=false 丢弃该条；nil 时原样导出。必须并发安全、有界、非阻塞；
	// panic 剔除该条并计入 Stats.HotExportPanics；从不在维护线程内执行。
	Label func(namespace, key string) (label string, keep bool)
}

// MemoryConfig 是内存区的配置：创建时一次预留，运行期不扩容。
type MemoryConfig struct {
	// SlackPercent 是条目区在驻留字节额度之上的余量百分比，同时覆盖碎片与尚未回收的退休块；
	// 默认 15。余量不足时填充以 ALLOCATION_FAILED 优雅拒绝（见 Stats.ArenaLargestFree）。
	SlackPercent int
	// LoadFactor 索引组表的装载率（条目数 / 槽数）；默认 0.85。缓存满载后随机退场与填充会让
	// 一部分 key 落到后继组：0.85 时未命中平均访问约 4 个组（未满载时约 1.5 个），超出探测上限而被
	// 拒绝的填充约 0.4%；调到 0.7 多用约 20% 索引内存，换来未命中约 3 个组、拒绝率不到 1%。
	LoadFactor float64
	// Prefault 为 true 时在创建时预先触碰索引区，避免线上请求路径上的缺页抖动。
	Prefault bool
	// HugePages 为 true 时对 mmap 内存区建议使用透明大页，降低亿级随机访问的 TLB 缺失；
	// 默认关闭，因为缺页时的大页整理可能造成延迟尖刺，建议与 Prefault 一起开启。
	HugePages bool
	// Heap 为 true 时内存区总是放在 Go 堆上；默认在 64 位 linux 上用匿名 mmap
	// （不计入 GOGC 触发线，物理页按需提交），其余平台与 -race 构建总是放在堆上。
	Heap bool
}

// LimitsConfig 收纳并发、超时、队列与各类预算的上限，全部有按核心配置推导的默认值。
type LimitsConfig struct {
	// MaxManagedBytes caps the sum of fixed storage, bounded auxiliary state,
	// and every dynamic byte category. Zero derives that upper bound at New.
	MaxManagedBytes int64
	// MaxNamespaces 命名空间上限；默认 64。
	MaxNamespaces int
	// MaxConcurrentLoads 真实 I/O 并发上限；默认 256。
	MaxConcurrentLoads int
	// LoadTimeout 缓存侧加载期限（不取消底层 I/O）；默认 10s。
	// 到期通知由完成泵或周期扫描推进（见 Runtime.DisableTicker），不是 Wait 内的定时器；
	// 需要更严格的调用方期限时请同时给 Fetch 传带超时的 ctx。
	LoadTimeout time.Duration
	// HandoffTimeout 是已完成的回源结果等待后台填充的最长时间；默认 2s。填充截止 =
	// min(条目硬截止, 回源完成时刻 + HandoffTimeout)，所以它大于某命名空间的 TTL 时由 TTL 兜底，
	// 不必小于 TTL。到期未填充的候选以 EXPIRED 丢弃（业务结果早已交付）；这段时间内到达的
	// 同 key 请求也可直接复用该结果而不再回源。
	HandoffTimeout time.Duration
	// MaxExecutorTasks bounds accepted but unfinished callbacks, including
	// loader dispatch, maintenance, completion pumps and cleanup. Zero derives
	// a bound from the configured fixed record/queue capacities.
	MaxExecutorTasks int
	// FlightStripes / FlightsPerStripe 每分片 Flight 条带与槽位；默认 8×64。
	FlightStripes, FlightsPerStripe int
	// MaxWaitersPerKey / MaxWaitersTotal 等待者上限；默认 32 / 16384。
	MaxWaitersPerKey, MaxWaitersTotal int
	// ReaderSlotsPerShard 每分片 epoch 保护槽（每个独占一条 64 字节缓存行）；默认 1024。
	// 一次 BorrowOn/GetBorrowed 从取得视图到 Release 占用一个槽；同一分片并发持有的视图超过槽数时，
	// 新读者得到 ErrBusy（见 Stats.PinSlotsExhausted）。单个热 key 只落在一个分片上，加分片不会分担。
	// 所以借用视图只用于短时间的拷贝或解析；会阻塞的场景（写慢客户端）请用 Get。
	ReaderSlotsPerShard int
	// MaxOwnedPinsPerEntry 拥有型句柄上限；默认 1024。
	MaxOwnedPinsPerEntry int64
	// MaxOwnedHandles bounds active owned handle metadata globally.
	MaxOwnedHandles int
	// PendingBytes 交接预算 M_pending；默认 MaxBytes/4。
	PendingBytes int64
	// ResponseBytes 响应预算 M_response；默认 MaxConcurrentLoads × MaxResultBytes。
	ResponseBytes int64
	// FillQueueSize 每分片填充队列；默认 256。
	FillQueueSize int
	// ControlQueueSize 每分片控制队列（失效确认队列，容量为此值）；默认 64。
	ControlQueueSize int
}

// RuntimeConfig 是宿主资源与维护节奏。
type RuntimeConfig struct {
	// Executor 宿主执行器；nil 使用默认 goroutine 执行器。
	Executor Executor
	// Clock 时间源；nil 使用真实单调时钟。
	Clock Clock
	// DisableTicker 为 true 时不启动内置周期触发，由宿主驱动 MaintenanceStep。
	// 注意：以下进展都依赖周期推进，宿主必须以不大于 PeriodicInterval 的节奏调用
	// MaintenanceStep，否则它们会停滞：空闲时的 TTL 摘除、Fetch 等待者的
	// LoadTimeout 到期通知（Future.Wait 自身不带定时器，只响应结果就绪和 ctx）、
	// 慢扫描、退休回收与 Close 达到 RECLAIMED、热点窗口历史与通知。
	DisableTicker bool
	// PeriodicInterval 定时触发周期；默认 25ms。
	PeriodicInterval time.Duration
	// PeriodicWorkLimit bounds shard/flight/control visits in one global tick.
	PeriodicWorkLimit int
	// RoundWorkLimit 每轮维护事件数上限；默认 512。
	RoundWorkLimit int
	// RoundSoftTimeBudget 每轮维护软时间预算（事件配额之外的补充长尾约束，
	// 到预算即让出、未完成类别由周期触发续行；首轮类别扫描总是执行）。默认 1ms。
	RoundSoftTimeBudget time.Duration
	// SweepPeriod 是慢扫描覆盖整张索引的目标周期；默认 5 分钟。
	// 过期与失效条目读时已不可见，最晚约一个周期后释放空间。
	SweepPeriod time.Duration
}

// sieveScanLimit 是一次填充里 SIEVE 扫描手最多检查的条目数。
//
// shortcut: 固定 64，不做成配置。它只限制单次填充的扫描量，命中率不敏感；需要调整时再提升为 EvictConfig 字段。
const sieveScanLimit = 64

func (c *Config) applyDefaults() {
	if c.Shards <= 0 {
		c.Shards = 16
	}
	c.Shards = nextPow2(c.Shards)
	if c.MaxKeyBytes <= 0 {
		c.MaxKeyBytes = 512
	}
	if c.MaxEntryBytes <= 0 {
		c.MaxEntryBytes = 1 << 20
	}
	if c.MaxResultBytes <= 0 {
		c.MaxResultBytes = 4 << 20
	}
	if c.observeOnly() {
		if c.MaxEntries == 0 {
			c.MaxEntries = int64(c.Shards)
		}
		if c.MaxBytes == 0 {
			c.MaxBytes = c.MaxEntryBytes
		}
	}
	if c.Limits.MaxNamespaces <= 0 {
		c.Limits.MaxNamespaces = 64
	}
	if c.Memory.SlackPercent == 0 {
		c.Memory.SlackPercent = 15
	}
	if c.Memory.LoadFactor <= 0 {
		c.Memory.LoadFactor = 0.85
	}
	if c.Heat.Delta <= 0 {
		c.Heat.Delta = time.Second
	}
	if c.Heat.GateCells <= 0 {
		c.Heat.GateCells = 16384
		if c.Shards > 0 { // 非法分片数由 validate 拒绝
			perShard := (c.MaxEntries + int64(c.Shards) - 1) / int64(c.Shards)
			c.Heat.GateCells = int(min(max(perShard/2, 16384), 1<<24))
		}
	}
	c.Heat.GateCells = (c.Heat.GateCells + gateLine - 1) / gateLine * gateLine
	if c.Evict.Horizon <= 0 {
		c.Evict.Horizon = time.Minute
	}
	if c.Evict.VictimLimit <= 0 {
		c.Evict.VictimLimit = 8
	}
	if c.Limits.MaxConcurrentLoads <= 0 {
		c.Limits.MaxConcurrentLoads = 256
	}
	if c.Limits.LoadTimeout <= 0 {
		c.Limits.LoadTimeout = 10 * time.Second
	}
	if c.Limits.HandoffTimeout <= 0 {
		c.Limits.HandoffTimeout = 2 * time.Second
	}
	if c.Limits.FlightStripes <= 0 {
		c.Limits.FlightStripes = 8
	}
	if c.Limits.FlightsPerStripe <= 0 {
		c.Limits.FlightsPerStripe = 64
	}
	if c.Limits.MaxWaitersPerKey <= 0 {
		c.Limits.MaxWaitersPerKey = 32
	}
	if c.Limits.MaxWaitersTotal <= 0 {
		c.Limits.MaxWaitersTotal = 16384
	}
	if c.Limits.ReaderSlotsPerShard <= 0 {
		c.Limits.ReaderSlotsPerShard = 1024
	}
	if c.Limits.MaxOwnedPinsPerEntry <= 0 {
		c.Limits.MaxOwnedPinsPerEntry = 1024
	}
	if c.Limits.MaxOwnedHandles == 0 {
		c.Limits.MaxOwnedHandles = c.Limits.MaxWaitersTotal
	}
	if c.Runtime.PeriodicInterval <= 0 {
		c.Runtime.PeriodicInterval = 25 * time.Millisecond
	}
	if c.Runtime.PeriodicWorkLimit == 0 {
		c.Runtime.PeriodicWorkLimit = 512
	}
	if c.Runtime.RoundWorkLimit <= 0 {
		c.Runtime.RoundWorkLimit = 512
	}
	if c.Runtime.RoundSoftTimeBudget <= 0 {
		c.Runtime.RoundSoftTimeBudget = time.Millisecond
	}
	if c.Runtime.SweepPeriod <= 0 {
		c.Runtime.SweepPeriod = 5 * time.Minute
	}
	if c.Limits.PendingBytes <= 0 {
		c.Limits.PendingBytes = c.MaxBytes / 4
		if c.Limits.PendingBytes < c.MaxEntryBytes {
			c.Limits.PendingBytes = c.MaxEntryBytes
		}
	}
	if c.Limits.ResponseBytes <= 0 {
		// 响应预算按“每个在途加载可能持有 maxResultBytes”推导，
		// 与 MaxConcurrentLoads 自洽；显式设置更小值则收紧加载并发。
		if bytes, ok := checkedProduct(int64(c.Limits.MaxConcurrentLoads), c.MaxResultBytes); ok {
			c.Limits.ResponseBytes = bytes
		} else {
			c.Limits.ResponseBytes = -1
		}
	}
	if c.Limits.FillQueueSize <= 0 {
		c.Limits.FillQueueSize = 256
	}
	if c.Limits.ControlQueueSize <= 0 {
		c.Limits.ControlQueueSize = 64
	}
	if c.Hot.TopN <= 0 {
		c.Hot.TopN = 16
	}
	if c.Hot.Interval == 0 {
		c.Hot.Interval = c.Heat.Delta
	}
	if c.Hot.RequestSlots > 0 && c.Hot.RequestSampleP == 0 {
		c.Hot.RequestSampleP = 1.0 / 1024
	}
	if c.Limits.MaxExecutorTasks == 0 {
		flights, ok := checkedProduct(int64(c.Shards), int64(c.Limits.FlightStripes))
		if ok {
			flights, ok = checkedProduct(flights, int64(c.Limits.FlightsPerStripe))
		}
		if !ok || flights > (math.MaxInt64-2*int64(c.Shards)-2)/2 {
			c.Limits.MaxExecutorTasks = -1
		} else {
			c.Limits.MaxExecutorTasks = int(2*flights + 2*int64(c.Shards) + 2)
		}
	}
	if c.Runtime.Executor == nil {
		c.Runtime.Executor = goroutineExecutor{}
	}
	if c.Runtime.Clock == nil {
		c.Runtime.Clock = newRealClock()
	}
}

// validate 执行初始化校验；无法满足时拒绝创建实例。
func (c *Config) validate() error {
	if err := validateNumbers(*c); err != nil {
		return err
	}
	if isTypedNil(c.Runtime.Clock) || isTypedNil(c.Runtime.Executor) {
		return errors.New("ahkc: Clock/Executor cannot be typed nil")
	}
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.Mode > ModeObserve {
		add("Mode(%d) 非法", c.Mode)
	}
	if c.MaxEntries <= 0 {
		add("MaxEntries 必须 > 0")
	}
	if c.MaxEntries > math.MaxInt32 || c.Shards > 1<<16 || uint64(c.Limits.MaxNamespaces) > math.MaxUint32 {
		add("条目池槽、分片 ID 或命名空间 ID 超出表示范围")
	}
	for _, dimension := range []int{c.MaxKeyBytes,
		c.Limits.FlightStripes, c.Limits.FlightsPerStripe, c.Limits.MaxWaitersPerKey, c.Limits.MaxWaitersTotal, c.Limits.MaxOwnedHandles, c.Limits.MaxConcurrentLoads, c.Limits.MaxExecutorTasks,
		c.Limits.ReaderSlotsPerShard, c.Limits.FillQueueSize, c.Limits.ControlQueueSize, c.Hot.LoadSlots, c.Hot.RequestSlots, c.Hot.Windows, c.Hot.TopN} {
		if dimension > 1<<24 {
			add("固定结构维度超过 2^24")
		}
	}
	if c.MaxKeyBytes > math.MaxUint16 { // 条目头里的 key 长度是 16 位
		add("MaxKeyBytes(%d) 不能超过 %d", c.MaxKeyBytes, math.MaxUint16)
	}
	if c.MaxBytes <= 0 {
		add("MaxBytes 必须 > 0")
	}
	if c.Shards <= 0 || c.Shards&(c.Shards-1) != 0 {
		add("Shards 必须是 2 的幂，得到 %d", c.Shards)
	}
	if int64(c.Shards) > c.MaxEntries {
		add("MaxEntries(%d) 必须 >= Shards(%d)", c.MaxEntries, c.Shards)
	}
	if c.Heat.GateCells > 1<<26 {
		add("Heat.GateCells(%d) 超出上界 2^26", c.Heat.GateCells)
	}
	if c.Limits.FlightStripes > 256 || c.Limits.FlightsPerStripe > 256 || c.Limits.ReaderSlotsPerShard > 1<<16 {
		add("Flight 或读者维度超过支持的固定结构上限")
	}
	if c.Hot.Windows < 0 {
		add("Hot.Windows 必须 >= 0")
	}
	historyOn := c.Hot.Windows > 0 || c.Hot.OnWindow != nil
	if historyOn && c.Hot.LoadSlots <= 0 && c.Hot.RequestSlots <= 0 {
		add("Hot.Windows/Hot.OnWindow 需要 Hot.LoadSlots > 0 或 Hot.RequestSlots > 0")
	}
	if c.Hot.RequestSlots > 0 && !historyOn {
		add("Hot.RequestSlots 需要启用 Hot.Windows 或 Hot.OnWindow")
	}
	if q := c.Hot.RequestSampleP; q != 0 && !(q >= math.Ldexp(1, -64) && q <= 1) {
		add("Hot.RequestSampleP(%v) 必须在 [2^-64,1]", q)
	}
	// 与 Heat.Delta 不同即视为显式设置；默认值保持既有行为，缺窗由 HotSnapshot.Skipped 如实报告。
	if iv := c.Hot.Interval; iv != c.Heat.Delta {
		slots := int64(c.Shards) * int64(c.Hot.LoadSlots+c.Hot.RequestSlots)
		need := (slots+hotCollectBudget-1)/hotCollectBudget + 1
		switch {
		case iv < time.Millisecond || iv > 24*time.Hour:
			add("Hot.Interval(%v) 必须在 [1ms,24h]", iv)
		case historyOn && int64(iv) < need*int64(c.Runtime.PeriodicInterval):
			add("Hot.Interval(%v) 必须 >= %d × PeriodicInterval(%v)：增量采集需要跨周期完成", iv, need, c.Runtime.PeriodicInterval)
		}
	}
	if c.Evict.VictimLimit > 8 {
		add("VictimLimit(%d) 必须 <= 8", c.Evict.VictimLimit)
	}
	if c.Memory.SlackPercent < 0 || c.Memory.SlackPercent > 400 {
		add("Memory.SlackPercent(%d) 必须在 [0,400]", c.Memory.SlackPercent)
	}
	if c.Memory.LoadFactor <= 0 || c.Memory.LoadFactor > 1 {
		add("Memory.LoadFactor 必须在 (0,1]")
	}
	if c.Runtime.RoundWorkLimit < classCount*2 {
		add("RoundWorkLimit 必须至少容纳每类维护的进展配额")
	}
	if c.Runtime.PeriodicWorkLimit < 100 {
		add("PeriodicWorkLimit 必须 >= 100")
	}
	if c.Heat.Delta > time.Duration(math.MaxInt64/2) {
		add("双桶窗口跨度溢出")
	}
	if c.MaxEntryBytes > c.MaxBytes {
		add("MaxEntryBytes 不能超 MaxBytes")
	}
	if c.MaxEntryBytes > c.MaxResultBytes {
		add("MaxResultBytes 必须至少容纳一个最大条目")
	}
	if c.MaxResultBytes > math.MaxUint32 { // 条目头里的计费字节数是 32 位
		add("MaxResultBytes(%d) 不能超过 4GiB-1", c.MaxResultBytes)
	}
	if c.Limits.MaxOwnedPinsPerEntry > 1<<30 { // 条目头里的 pin 计数是 32 位，须留出并发瞬时超额的余量
		add("MaxOwnedPinsPerEntry(%d) 不能超过 2^30", c.Limits.MaxOwnedPinsPerEntry)
	}
	if len(c.Namespaces) > c.Limits.MaxNamespaces {
		add("命名空间数量超过 MaxNamespaces")
	}
	if c.Limits.PendingBytes < c.MaxEntryBytes {
		add("PendingBytes(%d) 至少容纳一个最大条目(%d)", c.Limits.PendingBytes, c.MaxEntryBytes)
	}
	if c.Limits.ResponseBytes < c.MaxResultBytes {
		add("ResponseBytes(%d) 至少容纳一个最大结果(%d)", c.Limits.ResponseBytes, c.MaxResultBytes)
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
	if n.TTL <= 0 || n.TTL > maxTTL {
		return errors.New("TTL 必须在 (0, 1 年] 内")
	}
	if !(n.Tin > 0 && n.Tin < float64(heatCountMax)) {
		return errors.New("需要 0 < Tin < 4095（热度计数位宽上限）")
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

func checkedProduct(a, b int64) (int64, bool) {
	if a <= 0 || b <= 0 || a > math.MaxInt64/b {
		return 0, false
	}
	return a * b, true
}
