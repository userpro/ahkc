# 源码走读

**中文** | [English](WALKTHROUGH.md)

[架构原理](../ARCHITECTURE.zh.md)解释机制，本页只提供阅读入口。实际实现与函数注释优先于原始技术方案中的伪代码。

## 按一条请求阅读

| 顺序 | 文件 / 入口 | 关注点 |
| --- | --- | --- |
| 1 | [config.go](../config.go)：`applyDefaults` / `validate`；[memory.go](../memory.go)：`planLayout` | 默认值、非法输入、分配前布局校验 |
| 2 | [cache.go](../cache.go)：`New` / `registerNamespace` | 固定结构、命名空间、采样阈值、宿主资源 |
| 3 | [api.go](../api.go)：`beginRequest` / `lookup` / `recordHeatOnce` | 一次逻辑请求只采样和记热一次，读前 pin |
| 4 | [index.go](../index.go)、[heat.go](../heat.go) | 完整身份、固定探测域、双桶原子热度 |
| 5 | [flight.go](../flight.go)：`getOrLoad` / `runLoader` / `tryPumpFlight` | 唯一 leader、有界等待者、结果邮箱与完成泵 |
| 6 | [future.go](../future.go)：`bindReady` / `Take` / `Cancel` | 竞争状态转换、结果引用及预算只结算一次 |
| 7 | [fill.go](../fill.go)：`tryPrepareCandidate` / `planFill` / `freezeInput` / `commitFill` | 交付与填充分离，冻结模型与条件提交 |
| 8 | [sieve.go](../sieve.go)、[solver.go](../solver.go) | 有界候选、局部精确子集搜索 |
| 9 | [maintain.go](../maintain.go)、[wheel.go](../wheel.go) | 单维护者、分类配额、周期进展和硬 TTL 摘除 |
| 10 | [retire.go](../retire.go)、[epoch.go](../epoch.go)、[entry.go](../entry.go) | 摘链与回收分离，epoch / pin 保护池复用 |

## 按能力定位

| 能力 | 入口 | 最直接的测试 |
| --- | --- | --- |
| 容量与预算 | `budget.go`、`credit.go`、`executor.go` | `budget_test.go`、`TestCreditTransitAndDuplicateDelivery` |
| 收益采样和校准 | `stats.go`、`costmodel.go` | `estimate_test.go`、`calibration_test.go` |
| 单 key / 命名空间失效 | `api.go` 中 `Invalidate*`；维护控制队列 | `invalidation_test.go` |
| 读取所有权 | `api.go` 中 `Borrowed` / `Handle` | `TestBorrowedAndClonedHandlesKeepRetiredEntry` |
| 仅缓存 / 仅统计 / 组合 | `fill.go` 中 `ObserveOnly` 分支；`beginRequest` | `TestCacheAndObservationModes` |
| 热点榜和历史导出 | `observe.go`、`hotwindow.go` | `observe_test.go`、`hotwindow_test.go` |
| 关闭 | `maintain.go` 中 `checkReclaimed`；`api.go` 中关闭接口 | `TestCloseStagesAreIndependentlyAwaitable` |

## 调试路线

- 成功回源却未缓存：先看 `Stats.FillRejected`，再按原因进入 `tryPrepareCandidate`、`freezeInput` 或 `commitFill`。`NO_ESTIMATE` 常见于缺少 Prior、样本热身或污染。
- 重复加载：检查完整 flight 身份及失效令牌；失效后出现新版本 leader 属预期，不能只按字符串 key 判断。
- 关闭不完成：看真实 I/O、Future、读者与拥有型句柄，再查退休账目；手动维护模式还需继续推进执行器和 `MaintenanceStep`。
- 请求榜缺 key：检查是否采到、槽位是否足够、窗口是否已滚动，以及质量字段。`HotKeys` 只看回源活动。

新增并发回归优先复用 `newTestEnv` 的手动时钟与执行器，在明确状态点驱动任务。真实并发测试用 race 检查补充；固定 sleep 不能证明某个交错已经发生。
