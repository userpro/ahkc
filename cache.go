package ahkc

import (
	"context"
	"fmt"
	"hash/maphash"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Loader 由宿主实现的真实加载函数。在宿主执行资源上运行（不持有任何内部临界区）；
// 结果大小受 MaxResultBytes 契约约束。
type Loader[V any] func(ctx context.Context, key string) (V, error)

// ResultMetadata can only shorten cache validity or refuse caching.
// HardDeadline uses the cache Clock's monotonic nanoseconds; zero uses namespace TTL.
// NewMonotonicClock shares the default clock's origin. Negative deadlines refuse caching.
// Cost 是该结果的单次回源成本，用于缩放收益（热度 × Cost × 剩余有效期）；
// 0 表示默认值 1，须有限且不小于 0。
type ResultMetadata struct {
	NoCache      bool
	HardDeadline int64
	Cost         float32
}

func roundUp8(n int64) int64 { return (n + 7) &^ 7 }

// blockBytes 是条目块的计费大小：头 + 按 8 字节对齐的 key + 按 8 字节对齐的 value。
func blockBytes(keyLen int, valueLen int64) int64 {
	return EntryHeaderBytes + roundUp8(int64(keyLen)) + roundUp8(valueLen)
}

// valueLayout 是值的存放方式，由构造函数在创建时确定，运行期不做反射。
type valueLayout uint8

const (
	layoutBytes  valueLayout = iota // string / []byte：内容的原始字节
	layoutFlat                      // 无指针定长类型：内存映像
	layoutObject                    // 任意对象：保留在 Go 堆上，调用方给出计费
)

func bytesWeigh[V ~string | ~[]byte](key string, v V) int64 {
	return blockBytes(len(key), int64(len(v)))
}

func flatWeigh[V any](key string, _ V) int64 {
	var zero V
	return blockBytes(len(key), int64(unsafe.Sizeof(zero)))
}

func flatValueType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	case reflect.Array:
		return flatValueType(t.Elem())
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !flatValueType(t.Field(i).Type) {
				return false
			}
		}
		return true
	}
	return false
}

const (
	cacheOpen int32 = iota
	cacheClosing
	cacheReclaimed
)

// hintSlots 是每分片过期提示槽数，须为 2 的幂且不超过 64（按 keyHash 高 6 位散列）。
const hintSlots = 64

// token 是版本令牌：命名空间代际 + 分片 fillFence。
//
// 粒度说明：fillFence 是**分片级**计数器，任何一个 key 的 Invalidate 都会推进它，
// 使该分片内所有携带旧令牌的在途 flight / 填充候选作废（填充以 STALE 拒绝，
// 业务响应不受影响，下次访问会以新令牌重新加载）。这是用粗粒度换取无逐 key
// 状态的有意取舍；高频单 key 失效会让同分片的在途填充反复作废。
type token struct {
	nsGen     uint64
	fillFence uint64
}

// namespace 是命名空间内部表示。
type namespace[V any] struct {
	id   uint32
	name string
	c    *cache[V]
	cfg  NamespaceConfig
	gen  atomic.Uint64
	// reqThreshold 是请求热点追踪的采样阈值：u < reqThreshold 记为追踪样本；追踪关闭时为 0。
	reqThreshold uint64
}

// shardCounters 是请求路径的分片条纹计数器（固定条带，不按 key 建指标）。
// 独占缓存行，避免相邻分片计数器互相失效。
type shardCounters struct {
	hits   atomic.Uint64
	misses atomic.Uint64
	busy   atomic.Uint64
	_      [40]byte
}

// release 归还条目区与索引区，并清掉所有指向它们的指针：释放后仍被持有的 Cache 对象
// 不能让 GC 看到悬空地址（内核可能把该地址段再分给 Go 堆）。构造失败时对半成品分片同样适用。
func (s *shard[V]) release() {
	s.reg.release()
	s.idxReg.release()
	if s.hot != nil {
		s.hot.release()
	}
	if s.req != nil {
		s.req.release()
	}
	if s.arena != nil {
		s.arena.base = nil
	}
	if s.idx != nil {
		s.idx.words = nil
	}
	s.gate.cells = nil
}

// shard 是分片。驻留结构和已提交账目仅维护者修改。
type shard[V any] struct {
	id  int
	c   *cache[V]
	cnt shardCounters

	idx           *index
	gate          heatGate
	hot           *hotTracker // 回源热点 key 追踪（可选，Hot.LoadSlots > 0 时启用）
	req           *reqTracker // 请求热点 key 追踪（可选，Hot.RequestSlots > 0 时启用）
	flightStripes []flightStripe[V]
	epoch         *epochDomain
	policy        policyList

	// 条目区与索引区：New 时一次预留，RECLAIMED 后释放。arena 只由本分片维护者分配与释放。
	reg, idxReg *region
	arena       *arena
	// 对象布局的值侧表（会被 GC 扫描）；槽号记在条目头的 valLen 里。仅维护者分配与归还。
	// sideFree 只存回收的槽，sideNext 是从未用过的槽的起点：新槽按需取用，不在创建时触碰整张表。
	side     []V
	sideFree []uint32
	sideNext uint32

	fillQueue    chan *fillCandidate[V]
	runFn        func()             // 预建的维护任务（避免每次调度分配闭包）
	fs           fillScratch[V]     // 填充决策 scratch（仅维护者使用）
	controlQueue chan *controlOp[V] // 单 key 失效确认

	// 维护 owner 状态机：IDLE→QUEUED→RUNNING。
	ownerState     atomic.Int32 // 0 IDLE 1 QUEUED 2 RUNNING 3 DEAD
	pending        atomic.Bool
	nextAllowedRun atomic.Int64
	// pauseReasons 位：2=EXECUTOR_REJECTED。
	pauseReasons atomic.Int32

	// owner 字段（维护者独占；quota 创建时固定为公平份额，用原子便于观测）。
	countQuota    atomic.Int64
	byteQuota     atomic.Int64
	residentCount atomic.Int64
	residentBytes atomic.Int64
	flightCount   atomic.Int64

	fillFence atomic.Uint64
	entrySeq  uint64 // owner

	// owner 维护游标与配额
	resumeClass int
	// sweepCursor 是慢扫描的唯一游标：过期、代际失效与关闭共用。
	sweepCursor   int
	sweepAt       int64 // 上次慢扫描时刻，用于按流逝时间折算本轮扫描量
	flightScan    atomic.Uint64
	roundActive   bool
	roundDeadline int64
	fillGain      atomic.Uint64 // float64 bits; the shard owner is the sole writer

	// 关闭（BEGIN_CLOSE 直接置位，维护者消费）
	closeRequested atomic.Bool

	// hints 是读者发现的失效条目的有损提示：存索引槽号加一，维护者 Swap 取出并复核语义。
	// 64 个槽按 keyHash 高位分散，零分配、不触发调度；丢失只会推迟退场，不影响读时校验。
	hints [hintSlots]atomic.Uint32

	// 侵入式退休链（owner 独占）；retiredCount 是其长度的原子镜像，供 checkReclaimed 无锁读取。
	retireHead, retireTail blockRef
	reclaimMore            bool // owner 独占：退休链非空且本轮回收有进展，立即续行
	retiredCount           atomic.Int64
}

// currentToken 读取当前令牌（观测用）。
func (s *shard[V]) currentToken(ns *namespace[V]) token {
	return token{nsGen: ns.gen.Load(), fillFence: s.fillFence.Load()}
}

// Cache 是 AHKC 实例。并发安全。
type Cache[V any] struct {
	c *cache[V]
}

type cache[V any] struct {
	cfg    Config
	seed   maphash.Seed
	weigh  func(key string, value V) int64
	layout valueLayout
	// bytesIsString 区分字节布局的 V 是 string 还是 []byte（构造时按头部大小判定，运行期不做反射）。
	bytesIsString bool
	metadata      func(string, V) ResultMetadata
	state         atomic.Int32
	timeLimit     int64
	shards        []*shard[V]

	nsMu     sync.RWMutex
	nsByName map[string]*namespace[V]
	// nsSnap 是命名空间表的只追加不可变快照（写锁内发布）。
	// nsByID 等维护/完成路径高频按 id 读取，走无锁快照，避免每次 RLock。
	nsSnap atomic.Pointer[[]*namespace[V]]

	// 分类预算。
	ioBudget      budget // 真实 I/O 并发
	waiterBudget  budget // 全局 waiter
	pendingBytes  budget
	responseBytes budget
	controlBudget budget
	handleBudget  budget
	memory        MemoryLimits

	flightSeq          atomic.Uint64
	admissions         atomic.Int64 // in-progress load/control admission, outside the Get hit path
	tasks              atomic.Int64 // accepted callbacks, including executor-queued work
	nsCtlOwner         atomic.Bool
	hotCallbackPending atomic.Bool
	taskPool           *executorTaskPool

	tickOwner      atomic.Bool
	periodicCursor int

	nsControlQueue chan *controlOp[V]
	nsCtlDraining  atomic.Bool

	// hotHistory 热点窗口摘要历史（可选；Hot.Windows/Hot.OnWindow
	// 任一启用且存在追踪表时创建）。
	hotHist *hotHistory
	// 请求热点采样（全局同一采样率 q）：阈值与权重 1/q；reqThreshold == 0 表示未启用。
	reqThreshold uint64
	reqInvQ      float64

	closeOnce sync.Once
	closedCh  chan struct{} // 安全回收完成（RECLAIMED）后关闭
	drainedCh chan struct{} // 已受理工作全部终结后关闭（先于 closedCh）
	drained   atomic.Bool
	tickerWG  sync.WaitGroup
	stopTick  chan struct{}

	metrics metrics
}

// Option 是可选配置项。
type Option[V any] func(*cache[V])

// WithResultMetadata evaluates each successful load outside all internal gates.
func WithResultMetadata[V any](metadata func(string, V) ResultMetadata) Option[V] {
	return func(c *cache[V]) { c.metadata = metadata }
}

// NewBytes 创建字节串缓存：值是 string 或 []byte（含命名类型），存放内容的原始字节。
// 配置不满足初始化约束时返回错误。
func NewBytes[V ~string | ~[]byte](cfg Config, opts ...Option[V]) (*Cache[V], error) {
	return newCache(cfg, layoutBytes, bytesWeigh[V], opts)
}

// NewFlat 创建定长值缓存：V 必须是不含指针的定长类型（数值、数组、无指针结构体），
// 直接存放其内存映像，不需要序列化。含指针的类型请用 NewObject。
func NewFlat[V any](cfg Config, opts ...Option[V]) (*Cache[V], error) {
	if !flatValueType(reflect.TypeFor[V]()) {
		return nil, fmt.Errorf("%w: NewFlat requires a pointer-free fixed-size type, use NewObject", ErrInvalidConfig)
	}
	return newCache(cfg, layoutFlat, flatWeigh[V], opts)
}

// NewObject 创建对象缓存：V 可以是任意类型，值保留在 Go 堆上（会被 GC 扫描）。
// weigh 必填，必须返回条目的完整计费大小：EntryHeaderBytes + key + value 引用的全部内存，
// 且大于 0。
func NewObject[V any](cfg Config, weigh func(key string, value V) int64, opts ...Option[V]) (*Cache[V], error) {
	if weigh == nil {
		return nil, fmt.Errorf("%w: nil weigh", ErrInvalidConfig)
	}
	return newCache(cfg, layoutObject, weigh, opts)
}

func newCache[V any](cfg Config, layout valueLayout, weigh func(string, V) int64, opts []Option[V]) (*Cache[V], error) {
	if err := validateNumbers(cfg); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	c := &cache[V]{cfg: cfg, weigh: weigh, layout: layout}
	if layout == layoutBytes {
		var zero V
		c.bytesIsString = unsafe.Sizeof(zero) == unsafe.Sizeof("")
	}
	for _, o := range opts {
		if o == nil {
			return nil, fmt.Errorf("%w: nil option", ErrInvalidConfig)
		}
		o(c)
	}
	lay, err := planLayout[V](cfg, layout)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	if cfg.Limits.MaxManagedBytes == 0 {
		cfg.Limits.MaxManagedBytes = lay.memory.TotalBytes
	}
	timeLimit := maxCacheTime(cfg)
	now := cfg.Runtime.Clock.Now()
	if now < 0 || now >= timeLimit {
		return nil, ErrExhausted
	}
	c.cfg = cfg
	c.seed = maphash.MakeSeed()
	c.nsByName = make(map[string]*namespace[V], len(cfg.Namespaces))
	c.nsControlQueue = make(chan *controlOp[V], cfg.Limits.ControlQueueSize)
	c.closedCh, c.drainedCh, c.stopTick = make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.timeLimit, c.memory = timeLimit, lay.memory
	if cfg.Hot.RequestSlots > 0 { // q >= 1 时阈值取 MaxUint64（几乎必中）
		c.reqThreshold, c.reqInvQ = math.MaxUint64, 1
		if q := cfg.Hot.RequestSampleP; q < 1 {
			c.reqThreshold = uint64(math.Ldexp(q, 64))
			c.reqInvQ = math.Ldexp(1, 64) / float64(c.reqThreshold)
		}
	}
	// 历史窗口按 Unix 时间对齐：内置时钟与 processClockBase 同原点，偏移精确；
	// 自定义时钟从其原点对齐（测试确定）。
	var hotAlign uint64
	if _, ok := cfg.Runtime.Clock.(*realClock); ok {
		hotAlign = uint64(processClockBase.UnixNano())
	}
	hotInterval := uint64(cfg.Hot.Interval)
	hotWindowed := cfg.Hot.Windows > 0 || cfg.Hot.OnWindow != nil
	c.ioBudget.init(int64(cfg.Limits.MaxConcurrentLoads))
	c.waiterBudget.init(int64(cfg.Limits.MaxWaitersTotal))
	c.handleBudget.init(int64(cfg.Limits.MaxOwnedHandles))
	c.pendingBytes.init(cfg.Limits.PendingBytes)
	c.responseBytes.init(cfg.Limits.ResponseBytes)
	c.controlBudget.init(int64(cfg.Shards+1) * int64(cfg.Limits.ControlQueueSize+1))
	c.taskPool = newExecutorTaskPool(c, cfg.Limits.MaxExecutorTasks)
	// 分片额度固定为公平份额，余数分给前几个分片：Σ C_s = MaxEntries，Σ B_s = MaxBytes。
	shards := int64(cfg.Shards)
	perCount, remCount := cfg.MaxEntries/shards, cfg.MaxEntries%shards
	perBytes, remBytes := cfg.MaxBytes/shards, cfg.MaxBytes%shards
	c.shards = make([]*shard[V], cfg.Shards)
	for i := range c.shards {
		s := &shard[V]{
			id:           i,
			c:            c,
			epoch:        newEpochDomain(cfg.Limits.ReaderSlotsPerShard),
			fillQueue:    make(chan *fillCandidate[V], cfg.Limits.FillQueueSize),
			controlQueue: make(chan *controlOp[V], cfg.Limits.ControlQueueSize),
		}
		fail := func(err error) (*Cache[V], error) { // 已建好的分片和本分片的半成品一并归还
			s.release()
			for _, done := range c.shards[:i] {
				done.release()
			}
			return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
		}
		var err error
		if s.reg, err = newRegion(lay.regionBytes, cfg.Memory.Heap, cfg.Memory.HugePages); err != nil {
			return fail(err)
		}
		if s.idxReg, err = newRegion(lay.idxBytes, cfg.Memory.Heap, cfg.Memory.HugePages); err != nil {
			return fail(err)
		}
		if cfg.Memory.Prefault {
			s.idxReg.prefault(s.idxReg.size)
		}
		s.arena = newArena(s.reg.base, s.reg.size)
		s.idx = newIndex(s.idxReg, s.arena, lay.groups)
		s.gate = newHeatGate(unsafe.Slice((*heatCell)(unsafe.Add(s.idxReg.base, indexRegionBytes(lay.groups))), cfg.Heat.GateCells))
		s.policy.a = s.arena
		if lay.sideSlots > 0 {
			s.side = make([]V, lay.sideSlots)
			s.sideFree = make([]uint32, 0, lay.sideSlots)
		}
		s.fs.init(cfg.Evict.VictimLimit)
		s.runFn = func() { c.runMaintenance(s) }
		if cfg.Hot.LoadSlots > 0 {
			if s.hot, err = newHotTracker(cfg.Hot.LoadSlots, cfg.MaxKeyBytes, hotAlign, hotInterval, cfg.Memory.Heap); err != nil {
				return fail(err)
			}
		}
		if cfg.Hot.RequestSlots > 0 {
			if s.req, err = newReqTracker(cfg.Hot.RequestSlots, cfg.MaxKeyBytes, hotAlign, hotInterval, cfg.Memory.Heap); err != nil {
				return fail(err)
			}
		}
		s.countQuota.Store(perCount + b2i(int64(i) < remCount))
		s.byteQuota.Store(perBytes + b2i(int64(i) < remBytes))
		s.sweepAt = now
		s.flightStripes = make([]flightStripe[V], cfg.Limits.FlightStripes)
		for j := range s.flightStripes {
			s.flightStripes[j].init(cfg.Limits.FlightsPerStripe, c)
		}
		c.shards[i] = s
	}
	if hotWindowed {
		c.hotHist = newHotHistory(&cfg, (uint64(now)+hotAlign)/hotInterval, hotAlign)
		c.hotHist.task = c.hotNotify
	}
	for name, ncfg := range cfg.Namespaces {
		if _, err := c.registerNamespace(name, ncfg); err != nil {
			return nil, err // validate 已拦下全部失败条件；此处不可达，故不需要归还内存区
		}
	}
	c.cfg.Namespaces = nil // retain only the initialized configuration copies
	if !cfg.Runtime.DisableTicker {
		c.tickerWG.Add(1)
		go c.tickerLoop()
	}
	return &Cache[V]{c: c}, nil
}

func (c *cache[V]) registerNamespace(name string, ncfg NamespaceConfig) (*namespace[V], error) {
	c.admissions.Add(1)
	defer c.admissions.Add(-1)
	if c.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	if name == "" {
		return nil, fmt.Errorf("%w: empty namespace name", ErrInvalidConfig)
	}
	if len(name) > c.cfg.MaxKeyBytes {
		return nil, ErrKeyTooLarge
	}
	ncfg.withDefaults(&c.cfg)
	if err := ncfg.validate(&c.cfg); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	c.nsMu.Lock()
	defer c.nsMu.Unlock()
	if c.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	cur := c.nsList()
	if _, dup := c.nsByName[name]; dup {
		return nil, ErrNamespaceExists
	}
	if len(cur) >= c.cfg.Limits.MaxNamespaces {
		return nil, ErrOverloaded
	}
	name = strings.Clone(name)
	ns := &namespace[V]{id: uint32(len(cur)), name: name, c: c, cfg: ncfg}
	ns.gen.Store(1)
	ns.reqThreshold = c.reqThreshold
	c.nsByName[name] = ns
	// 发布新快照（写锁内；快照只追加，旧快照读者不受影响）。
	next := make([]*namespace[V], len(cur)+1)
	copy(next, cur)
	next[len(cur)] = ns
	c.nsSnap.Store(&next)
	return ns, nil
}

// nsList 返回当前命名空间快照（可能为 nil）。
func (c *cache[V]) nsList() []*namespace[V] {
	if s := c.nsSnap.Load(); s != nil {
		return *s
	}
	return nil
}

// Namespace 返回已注册命名空间的句柄。
func (c *Cache[V]) Namespace(name string) (*Namespace[V], error) {
	if c == nil || c.c == nil {
		return nil, ErrUninitialized
	}
	if c.c.state.Load() != cacheOpen {
		return nil, ErrClosed
	}
	c.c.nsMu.RLock()
	ns, ok := c.c.nsByName[name]
	c.c.nsMu.RUnlock()
	if !ok {
		return nil, ErrNoSuchNamespace
	}
	return &Namespace[V]{ns: ns}, nil
}

// RegisterNamespace 注册命名空间（受 MaxNamespaces 限制）。
func (c *Cache[V]) RegisterNamespace(name string, ncfg NamespaceConfig) (*Namespace[V], error) {
	if c == nil || c.c == nil {
		return nil, ErrUninitialized
	}
	ns, err := c.c.registerNamespace(name, ncfg)
	if err != nil {
		return nil, err
	}
	return &Namespace[V]{ns: ns}, nil
}

// Namespace 是命名空间句柄。
type Namespace[V any] struct {
	ns *namespace[V]
}

func (c *cache[V]) now() int64 {
	now := c.cfg.Runtime.Clock.Now()
	if now < 0 || now >= c.timeLimit {
		c.stopAdmission()
		return c.timeLimit // safe for all configured deadline additions
	}
	return now
}

func maxCacheTime(cfg Config) int64 {
	margin := int64(maxTTL)
	for _, d := range []int64{int64(cfg.Limits.LoadTimeout), int64(cfg.Limits.HandoffTimeout),
		int64(cfg.Runtime.PeriodicInterval), int64(cfg.Runtime.RoundSoftTimeBudget), int64(cfg.Runtime.SweepPeriod),
		2 * int64(cfg.Heat.Delta), int64(cfg.Evict.Horizon)} {
		if d > margin {
			margin = d
		}
	}
	limit := int64(math.MaxInt64) - margin
	if heatLimit, ok := checkedProduct(int64(1<<heatEpochBits-1), int64(cfg.Heat.Delta)); ok && heatLimit < limit {
		limit = heatLimit
	}
	return limit
}

// stopAdmission also handles checked identity/time exhaustion. Existing work
// keeps its budgets and drains through the ordinary close protocol.
func (c *cache[V]) stopAdmission() {
	if c.state.Load() != cacheOpen {
		return
	}
	c.admissions.Add(1)
	defer func() { c.admissions.Add(-1); c.checkReclaimed() }()
	if c.state.CompareAndSwap(cacheOpen, cacheClosing) {
		for _, s := range c.shards {
			s.closeRequested.Store(true)
			c.signalMaintenance(s)
		}
	}
}

func checkedNext(counter *atomic.Uint64) (uint64, bool) {
	for attempt := 0; attempt < reserveRetries; attempt++ {
		old := counter.Load()
		if old == math.MaxUint64 {
			return 0, false
		}
		if counter.CompareAndSwap(old, old+1) {
			return old + 1, true
		}
	}
	return 0, false
}

func (c *cache[V]) shardOf(keyHash uint64) *shard[V] {
	return c.shards[keyHash&(uint64(len(c.shards))-1)]
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
