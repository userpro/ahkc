# 使用指南

**中文** | [English](USAGE.md)

[快速开始](../README.zh.md) · [热点统计](OBSERVABILITY.zh.md) · [原理](../ARCHITECTURE.zh.md) · [示例索引](../example/README.zh.md)

## API 概览

泛型实例：`Cache[V]` / `Namespace[V]`（`V` 为值类型；key 固定为完整字符串，命名空间只做隔离，不参与拼接）。

### 只读查询（不发起加载）

```go
v, ok, err := ns.Get(key)        // 返回普通 Go 值，后续保留由调用方承担
b, ok, err := ns.GetBorrowed(key) // 绑定 epoch 的借用视图：用完 b.Release()
h, ok, err := ns.GetOwned(key)   // 拥有型句柄：跨线程/异步持有，用完 h.Release()
```

- `ok=true`：命中；`ok=false, err=nil`：确定不存在/过期/代际不符；
- `err=ErrBusy`：暂时竞争（不是确定不存在），可稍后重试；
- `err=ErrClosed`：实例已停止受理。

借用视图只在单 goroutine 的同步作用域内使用，不能跨 await，不能按值复制。`BorrowOn(r, &b)` 与 `BeginRequestOn(&r, key)` 使用调用方存储，可避免借用句柄分配；复用前先 `Release`，未释放就复用返回 `ErrInvalidRequest`。`b.Clone()` / `h.Clone()` 创建独立拥有型句柄，受每条目 pin 上限及全局 `MaxOwnedHandles` 限制。nil 或释放后的句柄 `Value()` 返回零值，`Release()` 幂等，`Clone()` 返回 `ErrCanceled`。

`GetOwnedOn(r)` 支持复用一次逻辑请求做有限 BUSY 重试，避免重复采样与命中计数；重试由宿主控制，库内每次尝试仍有上限。

### 查询并加载（singleflight）

```go
v, err := ns.Fetch(ctx, key, loader)         // 同步
fut, err := ns.FetchAsync(key, loader)       // 异步：fut.Wait(ctx) / Take() (V, ok, err) / Cancel()；三者必须择一终结
```

- 并发 miss 合并为一次真实回源；等待者数量受 `MaxWaitersPerKey/MaxWaitersTotal` 约束；
- `err=ErrOverloaded`：I/O 并发 / waiter / 响应字节预算耗尽（不排队、不阻塞，业务退避）；
- `err=ErrTimeout`：超过 `LoadTimeout`（**底层 I/O 不被取消**，迟到结果不填充，自行清理）；
- `ctx` 取消只解除自己的等待，不影响其他等待者与底层 I/O；
- **响应成功不代表已缓存**：准入与收益决策在后台交接完成。

`Take` 的 `ok=false` 只表示还在等待；已取消返回 `ok=true, ErrCanceled`，已取走返回 `ok=true, ErrConsumed`。`Wait` 优先返回调用时已经就绪的结果；`Wait(nil)` 返回 `ErrNilContext`，不消费结果。nil loader 仅能用于缓存命中，miss 时返回 `ErrNilLoader`，不启动加载。

同步 `Fetch` / `Invalidate` / `InvalidateNamespace` 在受理前检查 ctx：nil 返回 `ErrNilContext`，已取消返回同时匹配库错误与 `ctx.Err()` 的错误，不执行新操作。受理后取消只停止当前等待，已受理的失效仍会完成。

### Request：一次逻辑请求

```go
var request ahkc.Request[string]
if err := ns.BeginRequestOn(&request, key); err != nil { panic(err) }
r := &request
v, ok, err := ns.GetOn(r)
v, err = ns.FetchOn(ctx, r, loader)
```

同一业务请求的“先查询再加载”应传递同一个 `Request`（`Get(key)`/`Fetch(...)` 便捷形式内部已各自创建）。
`Request` 不可复制或并发使用；`go vet` 能检查按值复制。一次请求结束后可用 `BeginRequestOn` 重新初始化；初始化失败会清空旧请求，防止误用旧 key。把请求传给其他命名空间返回 `ErrInvalidRequest`。

### 失效与命名空间

```go
h, err := ns.InvalidateAsync(key)      // 未受理由 ErrBusy（控制队列满）/ ErrClosed / ErrKeyTooLarge 表示；已受理返回完成凭证
err = ns.Invalidate(ctx, key)          // 同步等待确认
err = c.InvalidateNamespace(ctx, name) // 推进代际：旧代际立即逻辑隐藏，后台分批摘除
nh, err := c.InvalidateNamespaceAsync(name)
ns2, err := c.RegisterNamespace(name, nsCfg)
```

**确认语义**：失效确认表示逻辑失效已完成——确认后新查询不得取得旧可读版本；不等待物理回收。

重复注册返回 `ErrNamespaceExists`；超过 `MaxNamespaces` 返回 `ErrOverloaded`；查找未注册名称返回 `ErrNoSuchNamespace`。nil / 零值 Cache、Namespace 和异步凭证的方法返回 `ErrUninitialized`；`On` 查询仍以 `ErrInvalidRequest` 表示无效请求。只读统计返回空快照，空对象的释放或 Shutdown 无效果。

**粒度说明**：单 key 失效会推进所在**分片**的 `fillFence`，该分片内所有在途加载的填充候选一并作废（以 `STALE` 拒绝，业务响应不受影响，下次访问重新加载）。高频单 key 失效会使同分片的在途填充反复作废。

### 关闭（三阶段）

```go
c.Shutdown()                           // 立即停止受理（新请求 ErrClosed），不等待
err := c.WaitDrained(ctx)              // 已受理的控制 / 填充 / 真实 I/O / 响应全部终结
err = c.WaitReclaimed(ctx)             // 读者、拥有型句柄、退休记录与预算全部清空
err = c.Close(ctx)                     // = Shutdown + WaitReclaimed；ctx 先到期返回 ErrCloseIncomplete（同时满足 errors.Is(err, ctx.Err())）
p := c.CloseProgress()                 // 非阻塞快照：p.Stopped / p.Drained / p.Reclaimed
```

- 未结束的真实 I/O 会阻塞 RECLAIMED：Close 报告 ctx 错误但**不强制结束 I/O**；
- 等待者以 `ErrClosed` 终结；迟到结果仅做有界状态清理。
- `Close(nil)` 返回 `ErrNilContext` 且不停止实例；传入已取消的非 nil ctx 仍会先停止受理。已完成的关闭或失效确认可重复等待，即使 ctx 已取消也返回完成结果。

### 指标

```go
st := c.Stats()  // 命中/未命中/BUSY/过载、加载与合并、填充发布与按原因拒绝、预算余量、窗口通知丢弃等
limits := c.MemoryLimits() // 固定结构、辅助状态及分类字节上限之和
credits, ok := c.Credits() // 一致的 Granted + Free + InTransit 额度账本；竞争时 ok=false
```

`Stats().FillRejected` 在没有拒绝记录时为 nil；可直接读取或 range，写入前须自行分配 map。非空 map 和导出的热点快照均独立归调用方持有，修改不会改变内部状态。

## 配置

内部资源上界在初始化时确定：数组、池、队列、循环次数、工作预算均来自 `Config`，运行期按预算受理工作。
不满足初始化约束（§8.2）时 `New` 直接报错（例如 `Grace >= 2Δ`、`ColdFor >= Δ`、`Tout < Tin`、时间轮跨度覆盖 `MaxTTL` 等）。

`Config` 本体只含容量、热度/估值语义、加载期限与宿主资源等常用项；索引 / 门卫 / sketch / 时间轮 / 队列 / 各类预算 / 维护配额等结构尺寸收纳在嵌入的 `Config.Advanced` 中，全部有按核心配置推导的默认值。
读取时可直接写 `cfg.WheelTick`（字段提升），构造字面量时需写 `Advanced: ahkc.Advanced{WheelTick: ...}`。下表标 ★ 的是 `Advanced` 字段。完整字段与默认值见 [config.go](../config.go) 注释：

| 分组 | 字段 | 默认 | 含义 |
| --- | --- | --- | --- |
| 容量 | `MaxEntries` / `MaxResidentBytes` | 缓存模式必填 | 全局条数/字节额度，分片均分+全局空闲池；ObserveOnly 可省略 |
| 容量 | `MaxKeyBytes` / `MaxEntryBytes` / `MaxResultBytes` | 512 / 1MiB / 4MiB | 完整 key 上限、单条目计费上限、单次加载结果上限 |
| 容量 | `ShardCount` | 16 | 分片数（向上取 2 的幂） |
| 热度 | `HeatDelta` / ★`HeatCap` / ★`GateRows×GateWidth` / ★`GateEpsilon+GateDelta` | 1s / 4×maxTin / 4×4096 / 不设置 | 热度桶宽、饱和上限、门卫尺寸；ε/δ 是尺寸提示，不能直接沿用原 CMS 误差保证 |
| 准入 | `NamespaceConfig.Tin/Tout` | 3 / 1.5 | 进入/退出阈值（滞回） |
| 估值 | `SampleP` / `NMin` / `HorizonW` | 1/1024 / 8 / 60s | 采样概率、最低样本量、预测上限 |
| 替换 | ★`SieveScanLimit` / ★`VictimLimit` / ★`MaxSubsetChecks` | 64 / 8 / 256 | SIEVE 扫描上限、候选集、子集检查上限 |
| 加载 | `MaxConcurrentLoads` / `LoadTimeout` / `HandoffTimeout` | 256 / 10s / 2s | 真实 I/O 并发、缓存侧期限、交接期限 |
| 等待 | ★`MaxWaitersPerKey` / ★`MaxWaitersTotal` | 32 / 16384 | 等待者上限 |
| 持有 | ★`MaxOwnedHandles` | `MaxWaitersTotal` | 拥有型句柄总数；每条目另有 pin 上限 |
| 完整内存 | `MaxManagedBytes` / ★`MaxModelBytes` | 自动推导 / 64KiB | 固定结构 + 有界辅助状态 + 全部分类型字节；每份模型与快照的保留上限 |
| 预算 | ★`PendingBytes` / ★`ResponseBytes` / ★`RetiredBytes` / ★`RetiredRecords` | B/4 / `MaxConcurrentLoads`×`MaxResultBytes` / B/4 / 4096 | 交接/响应/退休预算；响应预算默认与加载并发自洽 |
| 时间轮 | ★`WheelTick` / ★`WheelSlots` / ★`WheelLevels` | 100ms / 64 / 4 | 总跨度必须覆盖 MaxTTL+滞后余量 |
| 维护 | ★`RoundWorkLimit` / ★`RoundSoftTimeBudget` | 512 / 1ms | 每轮事件数上限与软时间预算（§7.1 长尾约束） |
| 维护 | `FillRatePerSec` / `FillBytesPerSec` / `RetireBytesPerRound` / ★`RetireLimitPerRound` / ★`ReclaimBytesPerRound` | 见 Config | 填充条数/字节速率及每轮摘除/回收字节配额（`RetireBytesPerRound` 在 `Config` 本体） |
| 调度 | ★`MaxExecutorTasks` / ★`PeriodicWorkLimit` | 按固定容量推导 / 512 | 已受理未完成回调总数、每次 tick 的分片/Flight/控制访问配额 |
| 观测 | `ObserveOnly` / ★`ObserveHotKeys` / ★`ObserveRequestKeys` / ★`ObserveRequestSampleP` | false / 观察模式 128 / 0 / 1/1024 | 仅统计不缓存；回源与请求热点追踪槽位（每分片）、请求热点采样率 |
| 观测 | ★`HotHistoryWindows` / ★`HotHistoryInterval` / ★`HotHistoryTopN` / ★`OnHotWindow` / ★`HotKeyLabel` | 0 / `HeatDelta` / 16 / nil / nil | 窗口历史、窗口宽度、榜单条数、通知回调、导出映射（详见观测文档） |
| 宿主 | `Executor` / `Clock` | goroutine / 单调时钟 | 见下 |

### 宿主执行器契约（重要）

```go
type Executor interface { TrySubmit(task func()) bool }
```

- **接受**：任务至多执行一次；**拒绝**：不执行；**不得 caller-runs 或内联递归执行**（会破坏有界性与锁序假设）；
- 默认执行器每任务一个 goroutine；接入业务线程池时把池满拒绝如实返回 `false`；
- `DisableTicker=true` 时内置周期触发关闭，由宿主定期调用 `c.MaintenanceStep()`（节奏不应慢于 `PeriodicInterval`，默认 25ms）。空闲时的 TTL 摘除/冷却、`LoadTimeout` 到期通知、额度转移、退休回收（含 `Close` 达到 RECLAIMED）和窗口历史都依赖它；`Future.Wait` 自身没有定时器，只响应结果就绪和 `ctx`。

ObserveOnly 不要求虚设驻留容量和命名空间 TTL：省略时分别使用 `ShardCount`、`MaxEntryBytes` 和 `MaxTTL` 作为内部预算默认值；显式负值仍返回 `ErrInvalidConfig`。

### 计费（Weigher）

```go
c, _ := ahkc.New[MyValue](cfg, ahkc.WithWeigher(func(key string, v MyValue) int64 {
	return int64(len(key)) + v.BackingBytes() + ahkc.EntryOverhead[MyValue]()
}))
```

`DefaultWeigher` 支持拥有完整底层存储的字符串、`[]byte` 及不含引用的平坦值。子串、子切片或指针对象图须提供自定义 Weigher（含指针的值类型未提供时 `New` 返回 `ErrNoWeigher`；weigher 返回非正权重时加载返回 `ErrInvalidWeight`），计入实际保留的完整底层容量；共享存储的账目须跨条目可相加。加载完成后在内部锁之外计费一次，所有成功结果都检查大小，包括冷 key 与 ObserveOnly。

`MaxManagedBytes` 小于计划布局时，`New` 在分配池前拒绝创建。`MemoryLimits` 是按宿主计费与模型大小契约推导的保留上界，不等于 Go 堆或进程 RSS；调用方接收后持有的普通值、宿主执行器存储、loader 临时空间和 GC 未收集垃圾由宿主管理。固定池及已受理回调、控制、句柄、在途授予分别有容量上限。

## 运行语义与契约

| 现象 | 语义 |
| --- | --- |
| `Get` 返回 `ok=false, err=nil` | 按固定探测域完整检查后的确定 miss |
| `ErrBusy` | 暂时竞争（保护槽/gate/令牌），**不是确定不存在**，可重试 |
| `ErrOverloaded` | 分类预算耗尽；未受理的操作不产生效果 |
| `ErrKeyTooLarge` | 完整 key 超过 `MaxKeyBytes`（默认 512）；各入口一致 |
| `ErrExhausted` | 身份序号或缓存时钟范围耗尽；实例拒绝继续受理 |
| `Fetch` 成功 | 业务结果已交付；**不代表已缓存** |
| `ObserveOnly` 模式的 `Get` | 开放且保护槽可用时返回 miss；仍可能返回 `ErrBusy` / `ErrClosed`；填充统一以 `OBSERVE_ONLY` 拒绝并计数 |
| `Fetch` 返回 loader 错误 | 首版不缓存错误；`PanicError` 包装 loader panic |
| loader 结果 > `MaxEntryBytes` 且 ≤ `MaxResultBytes` | 业务结果照常交付，只拒绝缓存 |
| loader 结果 > `MaxResultBytes` | 违反结果大小契约，交付 `ErrResultTooLarge` 并计数 |
| `LoadTimeout` | 缓存侧期限；底层 I/O 不取消，等待者得 `ErrTimeout` |
| 失效确认 | 逻辑失效完成；物理回收后台进行 |
| 观测历史 / 通知异常 | 见[热点统计排障](OBSERVABILITY.zh.md#开销与排障) |
| `Close` 返回 nil | RECLAIMED：驻留/退休/交接清零，全部分类预算归还 |

## 使用陷阱与排查

| 现象 | 原因 | 对策 / 排查 |
| --- | --- | --- |
| 新建缓存后一段时间 `FillPublished` 为 0，`FillRejected` 以 `NO_ESTIMATE` 为主 | 收益 sketch 创建后 `2×ValueDelta`（默认 20s）才热身；热身前只有 `Prior` 能给出估值 | 为命名空间配置 `Prior`；看 `Stats().FillUnavailable / FillPrior / FillMeasured` |
| 中低频 key 长期不入缓存（`NO_ESTIMATE`） | `MEASURED` 要求该 key 窗口内**样本数** ≥ `NMin`，约需 `NMin/SampleP` 次请求（默认约 8192 次/窗口） | 配 `Prior`；按流量调高 `SampleP` 或调低 `NMin` |
| 缓存已满后新 key 几乎不能替换旧 key（`NON_POSITIVE_GAIN`） | 仅靠 `Prior` 时所有 key 同一 λ，候选与受害者剩余 TTL ≥ `HorizonW` 时收益相等，`ΔG=0`；未满时不受影响 | 让估值走 `MEASURED`，或使用能区分 key 价值的成本模型 |
| 某 key 的 `Invalidate` 后同分片其它 key 的填充出现 `STALE` | `fillFence` 是分片级，失效会作废同分片所有在途填充 | 属设计取舍；避免对单 key 高频失效，或减少在途填充窗口 |
| `DisableTicker` 后 waiter 迟迟不超时、`Close` 不返回、TTL 过期条目不释放 | 这些推进都依赖周期调用 | 宿主以不慢于 `PeriodicInterval` 的节奏调用 `MaintenanceStep`；调用方另传带超时的 `ctx` |
| 把 `ErrBusy` 当作 miss 去回源 | `ErrBusy` 只表示本次没判定出来 | 对同一 `Request` 做有界重试 |
| `FetchAsync` 后占用 waiter 槽位不释放 | 未终结 `Future` | 总是 `Wait` / `Take`（ok=true）/ `Cancel` 之一 |
| 自定义值类型 `New` 返回 `ErrNoWeigher` | `DefaultWeigher` 不猜测引用对象图 | `WithWeigher`，计费含完整 backing storage |

## 成本模型与收益估值

准入与替换都使用同一收益口径：`G = (r̂×ĉ − ĥ) × min(W, 剩余 TTL)`，候选与驻留一视同仁。

- 估值三态：**MEASURED**（样本 ≥ `NMin`）/ **PRIOR**（命名空间先验，超龄失效）/ **UNAVAILABLE**（延后新填充，不影响已驻留条目）；
- 丢样、样本滞后超限进入**污染窗口**（2Δv）：期间估值 UNAVAILABLE，不静默使用脏数据；
- 成本模型接口：

```go
type CostModel interface {
	Version() uint64 // 单调递增；模型切换记录版本，防止新旧口径混比
	Lookup(lambda float64, costClass uint32) (r, c, h, k, K float64, ok bool)
}
```

- 内置默认：`hitCountModel`（以请求命中数为目标，G = λ̂·min(W,τ)）；
- `CostTableModel`：固定类别查表 + EWMA 老化。可避免回源率必须提供显式校准的 `MergeFactor` 或 `RateBands`；后者按请求率和加载时长查表，超出范围即 UNAVAILABLE。缺少校准不会自动假设 r=λ。
- `NewCostTableModel(cfg)` 返回 `(*CostTableModel, error)`；须检查错误。非法尺寸、数值、重复网格或缺少合并率校准立即返回 `ErrInvalidConfig`，不再生成不可用模型。旧的一返回值调用须迁移为 `model, err := ...`，见 [完整示例](../example/advanced/main.go)。
- 默认自动观测 loader 墙钟秒数，包含等待；CPU 口径设 `DisableAutoObserve: true`，用 `ObserveLoad(class, cpuCost, wallDuration)` 分别记录成本与加载时长。不同命名空间参加同一决策时必须使用相同单位，`UnitCostModel` 可声明单位。
- 决策固定当前时间、先验身份、模型身份与版本；`SnapshotCostModel` 可提供不可变快照。`SetModel(m)` / `SetPrior(p)` 原子更新、复制先验并返回错误；`SetModel(nil)` 恢复内置命中数目标，`SetPrior(nil)` 关闭先验回退。配置与更新校验均可用 `errors.Is(err, ErrInvalidConfig)` 判断。外部校准模型受 `ModelMaxAge` 限制，内置命中数目标不需校准。
- `WithResultMetadata` 可按结果禁缓存、缩短硬截止时间或设置有限成本类别；`HardDeadline` 使用缓存 Clock 的单调纳秒，不能延长命名空间 TTL。`NewMonotonicClock()` 与默认缓存、默认成本模型使用同一时间原点；默认时钟下可用 `clock.Now() + int64(ttl)` 表达相对截止时间，自定义 Clock 则须共用该实例。


值的复制是 Go 的浅拷贝；slice、map、指针与接口内部仍可能共享存储。发布后应保持值不可变，业务需要修改时自行复制。
