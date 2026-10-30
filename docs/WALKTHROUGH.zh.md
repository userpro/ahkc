# 源码走读

**中文** | [English](WALKTHROUGH.md)

[架构原理](../ARCHITECTURE.zh.md)解释机制，本页只提供阅读入口。实际实现与函数注释是准则。

## 按一条请求阅读

| 顺序 | 文件 / 入口 | 关注点 |
| --- | --- | --- |
| 1 | [config.go](../config.go)：`applyDefaults` / `validate`；[memory.go](../memory.go)：`planLayout` | 默认值、非法输入、分配前的布局与内存上界 |
| 2 | [cache.go](../cache.go)：`NewBytes` / `NewFlat` / `NewObject` / `newCache` | 三个构造函数、每分片内存区与索引的一次预留、命名空间 |
| 3 | [region.go](../region.go)、[arena.go](../arena.go) | mmap/堆两种后端；TLSF 分配器（O(1) 分配与释放、相邻空闲块合并、32 位块引用） |
| 4 | [entry.go](../entry.go)、[layout.go](../layout.go) | 叠加在块上的无指针块头；按布局的写入、视图、拷贝与计费 |
| 5 | [api.go](../api.go)：`beginRequest` / `lookup` / `recordHeatOnce` | 一次逻辑请求只记热一次；先 pin 再复核状态再读；失效条目登记提示 |
| 6 | [index.go](../index.go)、[heat.go](../heat.go) | 一条缓存行一组的索引（槽 = 标签 + 块引用，组头记溢出计数，无墓碑）；单缓存行热度门卫 |
| 7 | [flight.go](../flight.go)：`getOrLoad` / `runLoader` / `tryPumpFlight` | 唯一 leader、有界等待者、结果邮箱与完成泵 |
| 8 | [future.go](../future.go)：`bindReady` / `Take` / `Cancel` | 竞争状态转换、结果引用及预算只结算一次；缓存命中先拷出再释放句柄 |
| 9 | [fill.go](../fill.go)：`tryPrepareCandidate` / `planFill` / `freezeInput` / `prepareCommit` / `commitFill` | 交付与填充分离；热度 × 成本估值；不可逆点之前完成物理分配 |
| 10 | [sieve.go](../sieve.go)、[solver.go](../solver.go) | 有界候选、局部精确子集搜索 |
| 11 | [maintain.go](../maintain.go)、[retire.go](../retire.go) | 单维护者、分类配额；提示消费与慢扫描；侵入式退休链与回收续行 |
| 12 | [epoch.go](../epoch.go) | 摘链与回收分离，epoch / pin 保护块复用 |

## 按能力定位

| 能力 | 入口 | 最直接的测试 |
| --- | --- | --- |
| 内存区与分配器 | `region.go`、`arena.go` | `arena_test.go`（随机对照模型） |
| 索引与门卫 | `index.go`、`heat.go` | `index_test.go`（含 churn 稳态与并发读者）、`gate_test.go`、`heat_test.go` |
| 淘汰估值与替换 | `fill.go`、`solver.go` | `evict_test.go`、`solver_test.go`、`fill_test.go` |
| 惰性退场 | `maintain.go` 的 `consumeHints` / `sweepStep`；`retire.go` | `lifecycle_test.go`、`TestTTLExpiry` |
| 容量与预算 | `budget.go`、`executor.go` | `budget_test.go` |
| 单 key / 命名空间失效 | `api.go` 中 `Invalidate*`；维护控制队列 | `invalidation_test.go` |
| 读取所有权 | `api.go` 中 `Borrowed` / `Handle` | `TestBorrowedAndClonedHandlesKeepRetiredEntry` |
| 缓存 / 影子 / 仅统计 | `fill.go` 与 `api.go` 中的 `Mode` 分支；`beginRequest` | `TestCacheAndObservationModes`、`shadow_test.go` |
| 热点追踪与历史导出 | `tracker.go`、`observe.go`、`hotwindow.go` | `tracker_test.go`、`observe_test.go`、`hotwindow_test.go` |
| 关闭 | `maintain.go` 中 `checkReclaimed`；`api.go` 中关闭接口 | `TestCloseStagesAreIndependentlyAwaitable` |
| 规模 | `scale_test.go`（默认跳过，环境变量开启） | 见[验证文档](VALIDATION.zh.md) |

## 调试路线

- 成功回源却未缓存：先看 `Stats.FillRejected`，再按原因进入 `tryPrepareCandidate`、`freezeInput`、`prepareCommit` 或 `commitFill`。`NOT_HOT` 看门卫热度，`NON_POSITIVE_GAIN` 看 `freezeInput` 的两侧估值，`ALLOCATION_FAILED` 看 `Stats` 里的拥有型句柄数与 `Memory.SlackPercent`。
- 重复加载：检查完整 flight 身份及失效令牌；失效后出现新版本 leader 属预期，不能只按字符串 key 判断。
- 关闭不完成：看真实 I/O、Future、读者与拥有型句柄，再查退休链；手动维护模式还需继续推进执行器和 `MaintenanceStep`。
- 请求榜缺 key：检查是否采到、槽位是否足够、窗口是否已滚动，以及质量字段。`HotKeys` 只看回源活动。
- 借用视图读到别的内容：视图指向会被复用的块，`Release` 之后不得再用，见[使用指南](USAGE.zh.md)。

新增并发回归优先复用 `newTestEnv` 的手动时钟与执行器，在明确状态点驱动任务。真实并发测试用 race 检查补充；固定 sleep 不能证明某个交错已经发生。
