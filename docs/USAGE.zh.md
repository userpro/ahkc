# 使用指南

**中文** | [English](USAGE.md)

[快速开始](../README.zh.md) · [热点统计](OBSERVABILITY.zh.md) · [原理](../ARCHITECTURE.zh.md) · [示例索引](../example/README.zh.md)

## API 概览

泛型实例：`Cache[V]` / `Namespace[V]`（`V` 为值类型；key 固定为完整字符串，命名空间只做隔离，不参与拼接）。值类型由构造函数确定：

```go
c, err := ahkc.NewBytes[string](cfg)                      // string / []byte（含命名类型）
c, err := ahkc.NewFlat[Point](cfg)                        // 数值、数组、无指针结构体
c, err := ahkc.NewObject[*Doc](cfg, func(key string, v *Doc) int64 {
    return ahkc.EntryHeaderBytes + int64(len(key)) + v.RetainedBytes() // 必须覆盖完整保留存储，且大于 0
})
```

`NewFlat` 在构造时检查一次类型不含指针（含指针返回 `ErrInvalidConfig`）；`NewObject` 的 `weigh` 返回不大于 0 时加载返回 `ErrInvalidWeight`。三者的 `Option` 只有 `WithResultMetadata`。

### 只读查询（不发起加载）

```go
v, ok, err := ns.Get(key)         // 返回调用方独占的值
b, ok, err := ns.GetBorrowed(key) // 借用视图：用完 b.Release()
h, ok, err := ns.GetOwned(key)    // 拥有型句柄：跨线程/异步持有，用完 h.Release()
```

- `ok=true`：命中；`ok=false, err=nil`：确定不存在/过期/代际不符；
- `err=ErrBusy`：暂时竞争（不是确定不存在），可稍后重试；
- `err=ErrClosed`：实例已停止受理。

**读取契约按布局区分**

| 布局 | `Get` / `Fetch` 命中 | 借用视图 / 句柄 |
| --- | --- | --- |
| `NewBytes` | 在 guard 内拷贝一次（1 次分配） | 零拷贝，指向内存区，**只在 `Release` 前有效** |
| `NewFlat` | 值拷贝，零分配 | 值拷贝，零分配 |
| `NewObject` | 值拷贝，零分配 | 值拷贝，零分配 |

借用视图只在单 goroutine 的同步作用域内使用，不能跨 await，不能按值复制。`BorrowOn(r, &b)` 与 `BeginRequestOn(&r, key)` 使用调用方存储，可避免借用句柄分配；复用前先 `Release`，未释放就复用返回 `ErrInvalidRequest`。`b.Clone()` / `h.Clone()` 创建独立拥有型句柄，受每条目 pin 上限及全局 `MaxOwnedHandles` 限制。nil 或释放后的句柄 `Value()` 返回零值，`Release()` 幂等，`Clone()` 返回 `ErrCanceled`。

拥有型句柄会推迟所指块的回收，所以应尽快 `Release`；长期持有请 `Get`（拷贝）。`Close` 之后继续使用借用视图是错误：mmap 内存区已归还，访问会触发段错误。

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

同一业务请求的“先查询再加载”应传递同一个 `Request`（`Get(key)`/`Fetch(...)` 便捷形式内部已各自创建）。`Request` 不可复制或并发使用；`go vet` 能检查按值复制。一次请求结束后可用 `BeginRequestOn` 重新初始化；初始化失败会清空旧请求，防止误用旧 key。把请求传给其他命名空间返回 `ErrInvalidRequest`。

### 加载结果元数据

```go
ahkc.WithResultMetadata[string](func(key, v string) ahkc.ResultMetadata {
    return ahkc.ResultMetadata{
        NoCache:      false,                // true：只交付，不缓存
        HardDeadline: clock.Now() + int64(30*time.Second), // 只能缩短命名空间 TTL
        Cost:         5,                    // 单次回源成本，默认 1；须有限且不小于 0
    })
})
```

`Cost` 参与淘汰估值：条目收益 = 热度 × `Cost` × min(`Evict.Horizon`, 剩余 TTL)，大对象、慢后端的 key 因此更不容易被淘汰。不同命名空间参加同一次决策时应使用同一单位。`HardDeadline` 使用缓存 Clock 的单调纳秒，不能延长命名空间 TTL；`NewMonotonicClock()` 与默认缓存使用同一时间原点，默认时钟下可用 `clock.Now() + int64(ttl)` 表达相对截止时间，自定义 Clock 则须共用该实例。metadata 回调在所有内部锁之外执行，须有界、非阻塞。

### 失效与命名空间

```go
h, err := ns.InvalidateAsync(key)      // 未受理由 ErrBusy（控制队列满）/ ErrClosed / ErrKeyTooLarge 表示；已受理返回完成凭证
err = ns.Invalidate(ctx, key)          // 同步等待确认
err = c.InvalidateNamespace(ctx, name) // 推进代际：旧代际立即逻辑隐藏，后台分批摘除
nh, err := c.InvalidateNamespaceAsync(name)
ns2, err := c.RegisterNamespace(name, nsCfg)
```

**确认语义**：失效确认表示逻辑失效已完成——确认后新查询不得取得旧可读版本；不等待物理回收。

重复注册返回 `ErrNamespaceExists`；超过 `MaxNamespaces` 返回 `ErrOverloaded`。

**粒度说明**：单 key 失效会推进所在**分片**的 `fillFence`，该分片内所有在途加载的填充候选一并作废（以 `STALE` 拒绝，业务响应不受影响，下次访问重新加载）。高频单 key 失效会使同分片的在途填充反复作废。

### 关闭（三阶段）

```go
c.Shutdown()                           // 立即停止受理（新请求 ErrClosed），不等待
err := c.WaitDrained(ctx)              // 已受理的控制 / 填充 / 真实 I/O / 响应全部终结
err = c.WaitReclaimed(ctx)             // 读者、拥有型句柄、退休链与预算全部清空，内存区整体归还
err = c.Close(ctx)                     // = Shutdown + WaitReclaimed；ctx 先到期返回 ErrCloseIncomplete（同时满足 errors.Is(err, ctx.Err())）
p := c.CloseProgress()                 // 非阻塞快照：p.Stopped / p.Drained / p.Reclaimed
```

- 未结束的真实 I/O 会阻塞 RECLAIMED：Close 报告 ctx 错误但**不强制结束 I/O**；
- 等待者以 `ErrClosed` 终结；迟到结果仅做有界状态清理。
- 关闭时慢扫描不受周期限制，一次性摘除全部驻留条目；退休链只要有进展就连续推进，30 万条目约 0.3 秒回收完。
- `Close(nil)` 返回 `ErrNilContext` 且不停止实例；传入已取消的非 nil ctx 仍会先停止受理。已完成的关闭或失效确认可重复等待，即使 ctx 已取消也返回完成结果。

### 指标

```go
st := c.Stats()            // 命中/未命中/BUSY/过载、加载与合并、填充发布与按原因拒绝、预算余量、窗口通知丢弃等
limits := c.MemoryLimits() // 固定结构、辅助状态、内存区（RegionBytes）及分类字节上限之和
```

内存区的占用看 `ArenaUsedBytes` / `ArenaFreeBytes`（两者之和是条目区总大小）和 `RetiredPending`（退休链上尚未回收的条目数）：已分配明显大于 `ResidentBytes` 说明退休块还没回收（通常是拥有型句柄没释放）；空闲很多但仍出现 `ALLOCATION_FAILED` 说明碎片（`ArenaLargestFree` 是最大的单个空闲块：它远小于你最大的值时，填充正在被拒绝），调大 `Memory.SlackPercent`。`ErrBusy` 的来源可看 `PinSlotsExhausted`（读者槽耗尽，调大 `ReaderSlotsPerShard`；一个热 key 只在一个分片上，加分片无效，借用后会阻塞的处理函数应改用 `Get`）和 `PinEpochRetriesExhausted`（读者宣布期间 epoch 连续推进）；二者都为 0 而仍有 `ErrBusy`，来自回源合并（stripe 锁竞争，或同 key 的 flight 尚在完成），属于正常的有界重试。

`Stats().FillRejected` 在没有拒绝记录时为 nil；可直接读取或 range，写入前须自行分配 map。常见原因：`NOT_HOT`（热度未达 `Tin`）、`NON_POSITIVE_GAIN`（净收益不为正）、`ALLOCATION_FAILED`（内存区没有足够大的空闲块，碎片或退休块未回收）、`INFEASIBLE`（索引没有安装位或子集不可行）、`STALE`（失效使在途填充作废）。`ExpiredHints` 是读者发现失效条目后登记、被维护者消费的提示数。

## 配置

内部资源上界在初始化时确定：数组、内存区、队列、循环次数、工作预算均来自 `Config`，运行期按预算受理工作。不满足初始化约束时构造函数直接报错（例如非 2 的幂的分片数会被取整、`MaxEntries < Shards`、`Tin` 不在 (0, 4095)、`MaxKeyBytes` 超过 65535、单分片内存区超过 32GiB）。

`Config` 本体只含容量、模式与命名空间；其余按用途分组：`Heat`、`Evict`、`Hot`、`Memory`、`Limits`、`Runtime`。每个字段取零值即用默认值（能由核心配置推导的就推导），所以字面量只写要改的项：`Config{MaxEntries: n, MaxBytes: b, Memory: ahkc.MemoryConfig{Prefault: true}}`。完整字段与默认值见 [config.go](../config.go) 注释：

| 分组 | 字段 | 默认 | 含义 |
| --- | --- | --- | --- |
| 容量 | `MaxEntries` / `MaxBytes` | 缓存模式必填 | 全局条数/字节额度，按公平份额固定分给各分片（余数给前几个分片）；`ModeObserve` 可省略 |
| 容量 | `MaxKeyBytes` / `MaxEntryBytes` / `MaxResultBytes` | 512 / 1MiB / 4MiB | 完整 key 上限（≤65535）、单条目计费上限、单次加载结果上限 |
| 容量 | `Shards` | 16 | 分片数（向上取 2 的幂）；亿级建议 64–256 |
| `Memory` | `SlackPercent` | 15 | 条目区在字节额度之上的余量，覆盖碎片与未回收的退休块；不足时填充以 `ALLOCATION_FAILED` 拒绝 |
| `Memory` | `LoadFactor` | 0.85 | 索引装载率；调到 0.7 多用约 20% 索引内存，未命中降到约 3 个组，拒绝率低于 0.4% |
| `Memory` | `Prefault` / `HugePages` / `Heap` | false | 启动预触碰索引与门卫；建议透明大页（建议与 `Prefault` 一起开）；强制内存区放 Go 堆（非 linux/amd64/arm64 或 `-race` 总是放堆） |
| `Heat` | `Delta` / `GateCells` | 1s / 按容量推导 | 热度桶宽；门卫单元数（每 8 个一条缓存行），默认 clamp(每分片条数/2, 16384, 2^24)，规模场景按“每个 `Delta` 内去重未命中 key 数的 4 倍”设置 |
| 准入 | `NamespaceConfig.Tin` | 3 | 进入阈值（窗口请求热度），范围 (0, 4095) |
| `Evict` | `Horizon` / `VictimLimit` | 60s / 8 | 收益预测上限；候选集上限（≤8，子集检查上限为 2^VictimLimit）。SIEVE 扫描上限是固定常量 64 |
| `Limits` | `MaxConcurrentLoads` / `LoadTimeout` / `HandoffTimeout` | 256 / 10s / 2s | 真实 I/O 并发、缓存侧期限、交接期限（TTL 更短的命名空间以 TTL 为准） |
| `Limits` | `MaxWaitersPerKey` / `MaxWaitersTotal` | 32 / 16384 | 等待者上限 |
| `Limits` | `MaxOwnedHandles` / `MaxOwnedPinsPerEntry` | `MaxWaitersTotal` / 1024 | 拥有型句柄总数；每条目 pin 上限 |
| `Limits` | `MaxManagedBytes` | 自动推导 | 固定结构 + 有界辅助状态 + 内存区 + 全部分类字节；设置得小于计划布局时构造报错 |
| `Limits` | `PendingBytes` / `ResponseBytes` | 额度/4 / `MaxConcurrentLoads`×`MaxResultBytes` | 交接/响应预算。加载开始时按 `MaxResultBytes` 预留响应预算，loader 返回后立即缩小到实际大小，所以默认值只限制同时在跑的加载数，不限制排队等填充的小结果数 |
| `Limits` | `ReaderSlotsPerShard` | 1024 | 每分片的读保护槽，每个独占一条 64 字节缓存行；借用视图从 `BorrowOn` 到 `Release` 占一个槽，超出槽数的读者得到 `ErrBusy`（`Stats.PinSlotsExhausted`） |
| `Limits` | `MaxExecutorTasks` | 按固定容量推导 | 已受理未完成回调总数 |
| `Runtime` | `RoundWorkLimit` / `RoundSoftTimeBudget` / `SweepPeriod` | 512 / 1ms / 5min | 每轮事件数上限、软时间预算、慢扫描覆盖整张索引的目标周期 |
| `Runtime` | `PeriodicInterval` / `PeriodicWorkLimit` / `DisableTicker` | 25ms / 512 / false | tick 周期、每次 tick 的分片/Flight/控制访问配额、由宿主驱动维护 |
| `Runtime` | `Executor` / `Clock` | goroutine / 单调时钟 | 见下 |
| 模式 | `Mode` | `ModeCache` | `ModeCache` / `ModeShadow` / `ModeObserve`，见下 |
| `Hot` | `LoadSlots` / `RequestSlots` / `RequestSampleP` | 0 / 0 / 1/1024 | 回源与请求热点追踪槽位（每分片；各模式一致，0 即关闭）、请求热点采样率 |
| `Hot` | `Windows` / `Interval` / `TopN` / `OnWindow` / `Label` | 0 / `Heat.Delta` / 16 / nil / nil | 窗口历史、窗口宽度、榜单条数、通知回调、导出映射（详见观测文档） |

### 运行模式

- `ModeCache`：正常缓存。
- `ModeShadow`：影子缓存，按真实配置模拟准入、估值、淘汰、TTL 与失效，但驻留条目只存头和 key，命中对调用方表现为 miss（`Fetch` 照旧回源），`Stats.Hits/Misses` 是虚拟命中/未命中，命中率 = `Hits/(Hits+Misses)`。仍须配置容量与 TTL。
- `ModeObserve`：仅统计不缓存，热度门卫与 singleflight 照常，填充候选以 `OBSERVE_ONLY` 拒绝；条目区与索引只保留最小一块，容量与 TTL 可省略（省略时内部分别取 `Shards`、`MaxEntryBytes` 与 1 小时）；显式负值仍返回 `ErrInvalidConfig`。与其他模式一样，热点追踪默认关闭，需设置 `Hot.LoadSlots` / `Hot.RequestSlots`。

### 宿主执行器契约（重要）

```go
type Executor interface { TrySubmit(task func()) bool }
```

- **接受**：任务至多执行一次；**拒绝**：不执行；**不得 caller-runs 或内联递归执行**（会破坏有界性与锁序假设）；
- 默认执行器每任务一个 goroutine；接入业务线程池时把池满拒绝如实返回 `false`；
- `DisableTicker=true` 时内置周期触发关闭，由宿主定期调用 `c.MaintenanceStep()`（节奏不应慢于 `PeriodicInterval`，默认 25ms）。空闲时的慢扫描退场、`LoadTimeout` 到期通知、退休回收（含 `Close` 达到 RECLAIMED）和窗口历史都依赖它；`Future.Wait` 自身没有定时器，只响应结果就绪和 `ctx`。

### 计费

`NewBytes` 与 `NewFlat` 的计费就是实际块大小：`EntryHeaderBytes` + 按 8 字节对齐的 key + 按 8 字节对齐的 value，不需要也不接受自定义计费。`NewObject` 的 `weigh` 在加载完成后、内部锁之外调用一次，所有成功结果都检查大小，包括冷 key 与 `ModeObserve`。

`MemoryLimits` 是按配置推导的保留上界，不等于 Go 堆或进程 RSS：`RegionBytes` 是 mmap 内存区的预留上界，只有被写过的页才占物理内存；调用方接收后持有的普通值、宿主执行器存储、loader 临时空间和 GC 未收集垃圾由宿主管理。`NewObject` 的值与对象侧表在 Go 堆上，计入 `ResidentBytes`。

## 使用陷阱与排查

| 现象 | 原因 | 对策 / 排查 |
| --- | --- | --- |
| 成功回源却没缓存，`FillRejected` 以 `NOT_HOT` 为主 | key 的请求热度未达 `Tin`；门卫尺寸太小也会让热度被碰撞噪声淹没（高估）或被竞争低估 | 看 `ns.Heat(key)`；降低 `Tin` 或加大 `Heat.GateCells` |
| 缓存满后新 key 进不来，`NON_POSITIVE_GAIN` | 候选的收益（热度 × `Cost` × 剩余 TTL）不超过要换出的条目 | 属正常淘汰；若业务上新 key 更值钱，用 `ResultMetadata.Cost` 表达 |
| `ALLOCATION_FAILED` 持续出现 | 内存区没有足够大的连续空闲块：碎片或退休块还没回收 | 加大 `Memory.SlackPercent`；检查是否有长期不 `Release` 的拥有型句柄（`Stats.OwnedHandles`） |
| 过期条目仍计入 `ResidentEntries` | 惰性退场：读到、淘汰扫描到或慢扫描到才摘除 | 属设计；最晚一个 `SweepPeriod` 内退场，永远不会被读到 |
| 某 key 的 `Invalidate` 后同分片其它 key 的填充出现 `STALE` | `fillFence` 是分片级，失效会作废同分片所有在途填充 | 属设计取舍；避免对单 key 高频失效 |
| `DisableTicker` 后 waiter 迟迟不超时、`Close` 不返回、过期条目不释放 | 这些推进都依赖周期调用 | 宿主以不慢于 `PeriodicInterval` 的节奏调用 `MaintenanceStep`；调用方另传带超时的 `ctx` |
| 把 `ErrBusy` 当作 miss 去回源 | `ErrBusy` 只表示本次没判定出来 | 对同一 `Request` 做有界重试；重试预算要大于一次 GC 周期的停顿（见[验证文档](VALIDATION.zh.md#压测)） |
| 冷 key 突发时出现 `ErrOverloaded`，但 CPU 和数据源都很空闲 | 同时最多跑 `MaxConcurrentLoads` 个加载，每个在 loader 返回前占着 `MaxResultBytes` 的响应预算；loader 慢，或在等执行器空位，都会一直占着 | 调大 `MaxConcurrentLoads`（`ResponseBytes` 随之调大），或加快 loader；`Stats.Overloaded` 会计数 |
| `FetchAsync` 后占用 waiter 槽位不释放 | 未终结 `Future` | 总是 `Wait` / `Take`（ok=true）/ `Cancel` 之一 |
| 保存了借用视图里的 string 后读到别的内容 | `NewBytes` 的视图指向会被复用的块 | 需要保存就用 `Get`（拷贝）或先 `Clone` 成拥有型句柄 |
| `NewFlat` 返回 `ErrInvalidConfig` | 类型含指针、`string`、切片、map 或接口 | `string`/`[]byte` 用 `NewBytes`，含指针的用 `NewObject` |
| 小缓存（每分片只有几十条）命中率偏低 | 分片额度固定为公平份额，没有分片间转移 | 调小 `Shards` |
| `MemoryLimits.TotalBytes` 远大于驻留额度 | `ResponseBytes` 默认 `MaxConcurrentLoads × MaxResultBytes` = 1GiB | 调小 `MaxConcurrentLoads` 或显式设置 `ResponseBytes` |

## 已知限制

下列取舍在代码里以 `shortcut:` 注释标出，这里集中列出：

| 限制 | 影响 | 何时需要改 |
| --- | --- | --- |
| 索引探测上限固定为 8 组（56 个槽），退场时不回迁条目 | 满载并长期随机退场后，约 19% 的 key 落在后继组，默认 0.85 装载率下未命中平均访问约 4 个组；超出上限的填充被拒（85% 装载率下约 0.4%，70% 下少得多） | 调低 `Memory.LoadFactor`，或加大 `maxGroups`，或改成删除时回迁（需要给读者加版本重试） |
| 热度门卫默认尺寸按容量近似，不是按窗口内去重 key 数 | 实际去重未命中 key 远多于默认单元数时，热度被碰撞高估，准入变松 | 规模场景显式设置 `Heat.GateCells` |
| 热点追踪的前置过滤草图宽度固定为槽数的 4 倍 | 长尾 key 远多于它时估计值偏高，只会让入槽更保守，不会漏掉已追踪的热点 | 需要更高召回时加大追踪槽位 |
| 请求热点表的身份不比较完整 key，只比较 64 位哈希（含命名空间） | 哈希碰撞（概率约 槽数 × key 数 / 2^64）会合并两个 key 的计数；回源表比较完整 key | 无需处理，除非用作精确计费（本来就不应该） |
| 慢扫描遇到软时间预算耗尽就放弃本轮欠下的扫描量，不累计 | 高负载下覆盖整张索引的周期会比 `SweepPeriod` 长；过期条目读时仍不可见，只是更晚释放空间 | 需要严格覆盖保证时改成累计欠账 |
| 命名空间 TTL 上限固定为 1 年 | 更长存活期的配置会被拒绝 | 需要时改成 `Config` 字段 |
| 影子模式下 follower 沿用 leader 的“已虚拟命中”判定 | 条目恰在飞行中过期时少填一次，下次请求重填，无害 | 无 |
| 条目头里的命名空间代际只存低 32 位 | 条目要被误判有效，需要在被读到或被慢扫描退休之前该命名空间恰好被失效 2^32 的整数倍次，实际不可能 | 需要更强保证时改回 64 位（每条目多 4 字节） |
| 条目头里的退休 epoch 只存低 32 位，用有符号差比较 | 条目在退休链上停留超过 2^31 个 epoch 才会被多挡一圈，只推迟回收、不会提前 | 同上，改回 64 位（每条目多 4 字节） |
| 条目头里的计费字节数与 pin 计数是 32 位 | `MaxResultBytes` 不能超过 4GiB-1，`MaxOwnedPinsPerEntry` 不能超过 2^30，构造时校验 | 单条目需要更大时改回 64 位 |
| 分片条数与字节额度固定为公平份额，没有分片间转移 | 哈希分布的二项偏差使分到超额 key 的分片只能自己淘汰；小缓存更明显 | 额度留余量，或调小 `Shards` |
| 热点榜的 key 存在键池里（TLSF 内存区），预留大小为 槽数 × `MaxKeyBytes` 再加 25%；mmap 后端上只占虚拟地址、按写入的页计内存，但堆后端（`Memory.Heap`、`-race`、非 linux 64 位）上是真实的堆分配（6.5 万槽 × 512 B = 52 MiB，× 4096 B = 332 MiB）。关闭时存活的 key 压实成一块刚好够用的堆内存，所以榜单仍可读 | 键池碎片超过该余量时，需要新块的那次事件被丢弃（计入 `HotEventsDropped` / `HotRequestSamplesDropped`），已追踪的 key 不受影响；测试中未出现 | 加大追踪槽位，或调低 `MaxKeyBytes` |
| 每次填充的 SIEVE 扫描上限固定为常量 64（`sieveScanLimit`） | 它只限制单次填充的工作量，命中率对它不敏感 | 工作负载表明需要时提升为 `Evict` 字段 |
| `ArenaLargestFree` 取最高非空分级的链头块 | 与真正的最大空闲块相差不超过一个分级宽度（约 1/16） | 需要精确值时遍历该分级的链 |
| 平台支持 `mmap` 但预留内存区失败（虚拟内存上限、地址空间耗尽）时 | `New` 返回包装了 `ErrInvalidConfig` 的错误，并归还已经预留的部分；不会回退成同样大小的堆分配，因为 Go 无法从内存耗尽中恢复 | 调低 `MaxBytes` / `MaxEntries`，放宽限制（`ulimit -v`、容器内存上限），或设置 `Memory.Heap` 接受堆后端 |

## 淘汰估值

准入与替换使用同一收益口径：`G = λ × cost × min(Evict.Horizon, 剩余 TTL)`，其中 `λ = 热度 / Heat.Delta`（次/秒），`cost` 是 `ResultMetadata.Cost`（默认 1）。候选取门卫热度（准入已保证不小于 `Tin`）；驻留条目取命中热度加门卫热度（与 `ns.Heat` 同口径）；已过期或已失效的条目损失为 0。

在 SIEVE 有界扫描得到的至多 8 个受害者里枚举全部可行子集（条数、字节、索引安装位置同时核算），选损失之和最小者，仅当 `G(候选) − 旧条目损失 − 受害者损失` 为正才提交。“精确”仅限这个局部候选集，不保证全局最优。

不再有采样估值、先验（Prior）与成本模型：亿级下绝大多数 key 攒不够采样样本，热度在目标规模下始终可用，缓存满载后热 key 能稳定替换冷 key。

`NewObject` 的值是 Go 的浅拷贝；slice、map、指针与接口内部仍可能共享存储。发布后应保持值不可变，业务需要修改时自行复制。
