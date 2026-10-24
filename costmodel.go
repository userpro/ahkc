package ahkc

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"
)

// 有界查表成本模型。
//
// 单次可避免成本 c 在真实 loader 慢路径测量，并按命名空间 /
// 有限成本类别老化。本模型提供固定类别数的查表结构：
//   - 宿主（或库内自动观测）在加载完成路径以 Observe 更新类别成本 EWMA；
//   - 观测过期（> MaxAge）或缺失时回退到 FallbackCost 先验；
//   - 每次决策冻结版本号，观测更新单调递增版本，防止新旧口径混比；
//   - 输入越界 / 非有限返回 ok=false，估值降级为 UNAVAILABLE（不在线推理）。

// CostTableConfig 配置 CostTableModel。
type CostTableConfig struct {
	// Unit identifies the common unit of c/h/k/K; default "seconds".
	Unit string
	// Classes 固定成本类别数；默认 16，上限 64。
	Classes int
	// Alpha 是 EWMA 平滑系数 (0,1]；默认 0.25。
	Alpha float64
	// MergeFactor 是可避免回源率系数：r = λ × MergeFactor。
	// 必须由“无缓存、保留 singleflight”的回放显式校准；0 不提供该先验。
	MergeFactor float64
	// RateBands is an immutable calibrated grid, at most 1024 cells. Duration
	// and request-rate ranges use the nearest upper bucket; outside the grid
	// returns UNAVAILABLE. It takes precedence over the constant MergeFactor.
	RateBands []RateBand
	// FallbackLoadDuration supplies the duration prior for a rate grid when
	// no fresh load-duration observation exists; zero means unavailable.
	FallbackLoadDuration time.Duration
	// ServeCostPerSec 是持续服务成本 h（收益单位/秒）；默认 0。
	ServeCostPerSec float64
	// RemovalCost 是移除成本 k；默认 0。
	RemovalCost float64
	// InstallCost 是安装成本 K；默认 0。
	InstallCost float64
	// FallbackCost 是 c 的有限先验：无有效观测时使用。
	// 负值表示无先验：观测缺失或过期的类别估值为 UNAVAILABLE（延后新填充）。
	FallbackCost float64
	// MaxAge 观测最大年龄；默认 10min。
	MaxAge time.Duration
	// DisableAutoObserve 为 true 时关闭库内自动观测（默认开启，以 loader 墙钟时长观测）。
	// 需要严格 CPU 口径时置 true，由宿主以实测 CPU 调用 Observe。
	// 注意：墙钟时长包含等待，口径偏保守；严禁用它冒充精确 CPU 成本做跨模型比较。
	DisableAutoObserve bool
	// Clock 时间源；nil 使用真实单调时钟。
	Clock Clock
}

// RateBand describes one calibrated request-rate / load-duration bucket.
type RateBand struct {
	CostClass       uint32
	MaxLambda       float64
	MaxLoadDuration time.Duration
	MergeFactor     float64
}

func calibratedRate(cfg *CostTableConfig, lambda, duration float64, known bool, class uint32) (float64, bool) {
	if len(cfg.RateBands) == 0 {
		return lambda * cfg.MergeFactor, cfg.MergeFactor > 0
	}
	if !known {
		return 0, false
	}
	for _, b := range cfg.RateBands {
		if b.CostClass == class && lambda <= b.MaxLambda && duration <= b.MaxLoadDuration.Seconds() {
			return lambda * b.MergeFactor, true
		}
	}
	return 0, false
}

type costClassCell struct {
	observation atomic.Pointer[costObservation]
}

type costObservation struct {
	cost          float64
	at            int64
	duration      float64
	knownDuration bool
}

// CostTableModel 是有界查表成本模型。并发安全；Observe 可在任意 goroutine 调用。
type CostTableModel struct {
	cfg     CostTableConfig
	cells   []costClassCell
	version atomic.Uint64
	writer  atomic.Bool
	clock   Clock
}

// NewCostTableModel 创建有界查表成本模型；非法配置返回 ErrInvalidConfig。
func NewCostTableModel(cfg CostTableConfig) (*CostTableModel, error) {
	if cfg.Classes == 0 {
		cfg.Classes = 16
	}
	if cfg.Alpha == 0 {
		cfg.Alpha = 0.25
	}
	if cfg.MaxAge == 0 {
		cfg.MaxAge = 10 * time.Minute
	}
	if cfg.Unit == "" {
		cfg.Unit = "seconds"
	}
	if cfg.Classes < 1 || cfg.Classes > 64 || cfg.MaxAge < 0 ||
		!allFiniteNonNegative(cfg.Alpha, cfg.MergeFactor, cfg.ServeCostPerSec, cfg.RemovalCost, cfg.InstallCost) ||
		cfg.Alpha > 1 || cfg.MergeFactor > 1 || math.IsNaN(cfg.FallbackCost) || math.IsInf(cfg.FallbackCost, 0) ||
		cfg.FallbackLoadDuration < 0 || len(cfg.RateBands) > 1024 || len(cfg.Unit) > 64 {
		return nil, fmt.Errorf("%w: CostTableConfig contains an invalid size, cost, rate or age", ErrInvalidConfig)
	}
	if isTypedNil(cfg.Clock) {
		return nil, fmt.Errorf("%w: typed nil Clock", ErrInvalidConfig)
	}
	if cfg.Unit != "seconds" && !cfg.DisableAutoObserve {
		return nil, fmt.Errorf("%w: non-second cost units require DisableAutoObserve", ErrInvalidConfig)
	}
	if len(cfg.RateBands) == 0 && cfg.MergeFactor == 0 {
		return nil, fmt.Errorf("%w: supply calibrated MergeFactor or RateBands", ErrInvalidConfig)
	}
	cfg.Unit = strings.Clone(cfg.Unit)
	cfg.RateBands = slices.Clone(cfg.RateBands)
	slices.SortFunc(cfg.RateBands, func(a, b RateBand) int {
		return cmp.Or(cmp.Compare(a.CostClass, b.CostClass),
			cmp.Compare(a.MaxLoadDuration, b.MaxLoadDuration), cmp.Compare(a.MaxLambda, b.MaxLambda))
	})
	for i, b := range cfg.RateBands {
		if b.CostClass >= uint32(cfg.Classes) || !allFiniteNonNegative(b.MaxLambda, b.MergeFactor) || b.MaxLambda == 0 || b.MergeFactor > 1 || b.MaxLoadDuration < 0 {
			return nil, fmt.Errorf("%w: invalid RateBands[%d]", ErrInvalidConfig, i)
		}
		if i > 0 && b.CostClass == cfg.RateBands[i-1].CostClass && b.MaxLoadDuration == cfg.RateBands[i-1].MaxLoadDuration && b.MaxLambda == cfg.RateBands[i-1].MaxLambda {
			return nil, fmt.Errorf("%w: duplicate RateBands bucket", ErrInvalidConfig)
		}
	}
	if cfg.Clock == nil {
		cfg.Clock = newRealClock()
	}
	return &CostTableModel{cfg: cfg, cells: make([]costClassCell, cfg.Classes), clock: cfg.Clock}, nil
}

func (m *CostTableModel) CostUnit() string {
	if m == nil {
		return ""
	}
	return m.cfg.Unit
}

// RetainedBytes includes the table, current observations, calibration grid and
// one frozen snapshot. Clock ownership belongs to the host.
func (m *CostTableModel) RetainedBytes() int64 {
	if m == nil || len(m.cells) == 0 {
		return 0
	}
	return int64(unsafe.Sizeof(*m)) + int64(len(m.cells))*(int64(unsafe.Sizeof(costClassCell{}))+int64(unsafe.Sizeof(costObservation{}))+32) +
		int64(len(m.cfg.RateBands))*int64(unsafe.Sizeof(RateBand{})) + int64(unsafe.Sizeof(costTableSnapshot{})) + int64(len(m.cells))*int64(unsafe.Sizeof(snapshotCell{})) + 2048
}

// Version 单调递增（观测更新即切换版本）。
func (m *CostTableModel) Version() uint64 {
	if m == nil {
		return 0
	}
	return m.version.Load()
}

// Observe 在完成路径记录一次成本观测（单位：收益单位，与 h/k/K 同口径）。
// cost 必须有限非负，否则忽略。
func (m *CostTableModel) Observe(costClass uint32, cost float64) {
	m.observe(costClass, cost, 0, false)
}

// ObserveLoad records cost and wall duration independently, e.g. CPU cost with
// the duration needed by the calibrated singleflight grid.
func (m *CostTableModel) ObserveLoad(costClass uint32, cost float64, duration time.Duration) {
	if duration < 0 {
		return
	}
	m.observe(costClass, cost, duration.Seconds(), true)
}

func (m *CostTableModel) observe(costClass uint32, cost, duration float64, known bool) {
	if m == nil || costClass >= uint32(len(m.cells)) || math.IsNaN(cost) || math.IsInf(cost, 0) || cost < 0 {
		return
	}
	if !m.writer.CompareAndSwap(false, true) {
		return // observation is a policy hint; never wait for another writer
	}
	defer m.writer.Store(false)
	version := m.version.Load()
	if version > math.MaxUint64-2 {
		return // cannot publish another unique model version
	}
	m.version.Store(version + 1) // odd versions denote an in-progress observation
	cell := &m.cells[costClass]
	if old := cell.observation.Load(); old != nil {
		cost = old.cost*(1-m.cfg.Alpha) + cost*m.cfg.Alpha
		if known && old.knownDuration {
			duration = old.duration*(1-m.cfg.Alpha) + duration*m.cfg.Alpha
		}
	}
	cell.observation.Store(&costObservation{cost: cost, at: m.clock.Now(), duration: duration, knownDuration: known})
	m.version.Store(version + 2)
}

// observeDuration 以时长观测（库内自动观测路径；cost 单位=秒）。
func (m *CostTableModel) observeDuration(costClass uint32, d time.Duration) {
	if m.cfg.DisableAutoObserve {
		return
	}
	m.ObserveLoad(costClass, d.Seconds(), d)
}

// Lookup 查表估值输入（A.4）：r=λ×MergeFactor；c 取观测 EWMA，缺失/过期回退先验。
func (m *CostTableModel) Lookup(lambda float64, costClass uint32) (r, c, h, k, K float64, ok bool) {
	if m == nil || math.IsNaN(lambda) || math.IsInf(lambda, 0) || lambda < 0 || costClass >= uint32(len(m.cells)) {
		return 0, 0, 0, 0, 0, false
	}
	version := m.version.Load()
	if version&1 != 0 {
		return 0, 0, 0, 0, 0, false
	}
	observation := m.cells[costClass].observation.Load()
	c = -1
	duration, known := m.cfg.FallbackLoadDuration.Seconds(), m.cfg.FallbackLoadDuration > 0
	now := m.clock.Now()
	if observation != nil && now >= observation.at && now-observation.at <= int64(m.cfg.MaxAge) {
		c = observation.cost
		duration, known = observation.duration, observation.knownDuration
	} else if m.cfg.FallbackCost >= 0 {
		c = m.cfg.FallbackCost
	}
	if c < 0 {
		return 0, 0, 0, 0, 0, false
	}
	r, ok = calibratedRate(&m.cfg, lambda, duration, known, costClass)
	h, k, K = m.cfg.ServeCostPerSec, m.cfg.RemovalCost, m.cfg.InstallCost
	if !ok || !allFiniteNonNegative(r, c, h, k, K) || m.version.Load() != version {
		return 0, 0, 0, 0, 0, false
	}
	return r, c, h, k, K, true
}

// costTableSnapshot 冻结各类别的成本与加载时长。cfg 在模型构造后只读，快照直接引用而不拷贝；
// 类别表按实际 Classes 数分配（不再固定 64 项）。
type costTableSnapshot struct {
	cfg     *CostTableConfig
	version uint64
	cells   []snapshotCell
}

type snapshotCell struct {
	cost, duration float64
	known          bool
}

func (s *costTableSnapshot) Version() uint64  { return s.version }
func (s *costTableSnapshot) CostUnit() string { return s.cfg.Unit }

func (s *costTableSnapshot) Lookup(lambda float64, class uint32) (r, c, h, k, K float64, ok bool) {
	if !allFiniteNonNegative(lambda) || class >= uint32(len(s.cells)) || s.cells[class].cost < 0 {
		return 0, 0, 0, 0, 0, false
	}
	cell := s.cells[class]
	r, ok = calibratedRate(s.cfg, lambda, cell.duration, cell.known, class)
	c = cell.cost
	h, k, K = s.cfg.ServeCostPerSec, s.cfg.RemovalCost, s.cfg.InstallCost
	return r, c, h, k, K, ok && allFiniteNonNegative(r, c, h, k, K)
}

// SnapshotAt freezes all categories and their age at one timestamp.
func (m *CostTableModel) SnapshotAt(now int64) CostModel {
	if m == nil || len(m.cells) == 0 {
		return nil
	}
	s := &costTableSnapshot{}
	if !m.snapshotOn(now, s) {
		return nil
	}
	return s
}

// snapshotOn 复用维护者独占的存储；公开 SnapshotAt 仍返回独立快照。
func (m *CostTableModel) snapshotOn(now int64, s *costTableSnapshot) bool {
	version := m.Version()
	if version&1 != 0 {
		return false
	}
	if cap(s.cells) < len(m.cells) {
		s.cells = make([]snapshotCell, len(m.cells))
	}
	s.cfg, s.version, s.cells = &m.cfg, version, s.cells[:len(m.cells)]
	for i := range m.cells {
		cell := &s.cells[i]
		cell.cost = -1
		cell.duration, cell.known = m.cfg.FallbackLoadDuration.Seconds(), m.cfg.FallbackLoadDuration > 0
		observation := m.cells[i].observation.Load()
		if observation != nil && now >= observation.at && now-observation.at <= int64(m.cfg.MaxAge) {
			cell.cost = observation.cost
			cell.duration, cell.known = observation.duration, observation.knownDuration
		} else if m.cfg.FallbackCost >= 0 {
			cell.cost = m.cfg.FallbackCost
		}
	}
	return m.Version() == version
}

// costObserver 是库内自动观测接口（加载完成路径调用，有界 EWMA 更新）。
type costObserver interface {
	observeDuration(costClass uint32, d time.Duration)
}

var _ CostModel = (*CostTableModel)(nil)
var _ costObserver = (*CostTableModel)(nil)
