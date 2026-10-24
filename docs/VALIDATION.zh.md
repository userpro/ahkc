# 验证与性能

**中文** | [English](VALIDATION.md)

[快速上手](../README.zh.md) · [架构](../ARCHITECTURE.zh.md)

## 状态

Go 1.25.4 / linux amd64：build、vet、gofmt（无输出）、`go test -count=1 ./...`（4s）、`go test -race -count=1 ./...`（36s）全部通过；根包与 `cmd/cacheload` 可交叉编译 386。以下数字均来自本机一次 `./bench/reproduce.sh` 运行，源码指纹（`SourceSHA256`）在 `bench/results/environment.json`。

新增回归覆盖：Request 初始化失败时清旧 key、Future 终态与并发消费、nil/取消 ctx、nil loader、配置与成本模型错误、满队列拒绝、已结算句柄不再固定已关闭的 cache。API 变更（`NewCostTableModel` 返回 error、ObserveOnly 可省略容量与 TTL、ManualExecutor 改为可复用 FIFO 环）见[使用指南](USAGE.zh.md)。

尚未证明：真实 72 小时稳态、32 物理核 / 跨 NUMA 扩展、相同总 M 的原生基线、真实后端净 CPU 收益、业务端到端回放。`TestVirtual72HourLifecycle` 只是模拟 72h。

## 复现

```bash
./bench/reproduce.sh              # 约 5–8 分钟，产物写入 bench/results/（已忽略入库）
DURATION=2s ./bench/reproduce.sh  # 冒烟，不可与本页表格对照
```

一个脚本产出：环境指纹、7 项检查、微基准、5 个压测场景、1 个故意失败样本、3 份模型报告和汇总。原始报告不入库，只有本页数字入库。

## 分配

每组 100 万次读取 / 10 万次同步 miss / 1 万次其他操作，重复 5 组取中位数。"优化前"为上一版快照。

| 路径 | 优化前 B/op、allocs | 当前 B/op、allocs | 当前 ns/op |
| --- | --- | --- | ---: |
| Get / 同步 Fetch 命中 | 0, 0 | 0, 0 | 92–104 |
| 借用读 miss | 32, 1 | 0, 0 | — |
| 异步 Fetch + Wait | 352, 4 | 144, 2 | 448–463 |
| 拥有型读取 被拒绝 / 成功 | — | 0, 0 / 48, 1 | 121 / 175–207 |
| 同步 miss，默认执行器 | 889, 8 | 808, 6 | — |
| 异步 miss / 取消，手动执行器 | 899, 10 | 803, 6 | — |
| 8 个合并等待者，手动执行器 | 2515, 27 | 2419, 23 | — |
| 单 key / 命名空间失效，手动执行器 | 275,5 / 296,5 | 259,3 / 280,3 | — |
| 成本模型填充决策，预热后 | 432, 2 | 0, 0 | — |
| 无拒绝记录的 Stats / 空闲维护 | 48,1 / 8,1 | 0,0 / 0,0 | — |

热度单元、门卫、sketch、样本队列、epoch、时间轮、退休池和求解器均为 0 allocs/op。被拒绝的拥有型读取零分配；成功的一次支付 48B 句柄，异步交付 144B、两次分配（信封 + 值槽）。初始化普通实例 28.64MB、仅统计模式 10.44MB（约少 64%，是构造时分配量，不是 RSS）。观测开关在 关闭 / q=1/1024 / q=1 下为 16.4 / 16.2 / 34.0 ns/op，均零分配 —— 前两者不可区分，q=1 明显更贵。

## 压测

`cacheload` 按毫秒批次提供 110 万 QPS，延迟从预定到达算到值访问与 guard 释放，含积压、有界重试与 GC。验收标准：成功 QPS ≥100 万、P99 ≤3ms、最终错误为零。每组 20s、每场景 2,200 万次到达；混合流量 95% 预热 / 3% 冷查询 / 2% 禁缓存，loader 墙钟 500µs：

| 场景 | 成功 QPS | P50 | P99 | 最终错误 |
| --- | ---: | ---: | ---: | ---: |
| 纯借用命中 | 1,100,009 | 0.551ms | 1.252ms | 0 |
| 95/3/2 混合借用 | 1,099,923 | 0.611ms | 2.381ms | 0 |
| 混合借用，第二种子 | 1,099,952 | 0.620ms | 2.377ms | 0 |
| 混合借用，99.9% 集中 / 64 key | 1,099,943 | 0.615ms | 2.214ms | 6 次中 1 次为 8 |
| 95/3/2 混合拥有型 | 1,099,949 | 0.637ms | 2.457ms | 0 |

BUSY/OVERLOADED 是已计入延迟的尝试次数。

集中场景**不保证**零错误：6 次 20s 运行中 5 次为零，1 次在 2,200 万请求中出现 8 次（约 6×10⁻⁸）。原因是客户端而非库 —— `readHot` 只重试 `ErrBusy` 64 次、每次之间仅 `runtime.Gosched()`，单次运行约 39 万次 BUSY 会耗尽预算；在临时副本中把预算提到 512 后相同负载零错误。`skew-4096keys-load.json`（默认 4096 key）按设计失败：长尾冷却导致每次约 1.35 万次"要求驻留的 key 发生 miss"。

## 微基准

单线程中位数：Get 92.4–94.4ns/op、借用 110.1–110.2ns/op、保护索引基线 67.8–68.7ns/op，即 Get 为基线的 1.34–1.39 倍、借用约 1.6 倍。并行 Get 15.7–16.0ns/op、并行 miss 19.1–20.2ns/op（吞吐倒数，不是延迟）。公开 API 包一致：Get 96.1、借用 104.1、并行 Get 15.4、拥有型热点并行 72.2、完整 miss→load 185µs/op。所有命中路径零分配，约三分之一耗时是 TTL 校验必需的单调时钟读取。`HotKeys(20)` 为 81–85µs、一次 1792B 分配 —— 这是一次完整快照，不是单请求开销。

微基准用 SampleP=1e-12 隔离采样，压测用默认 1/1024。这些是平均值而非 P99；只有朴素 Get 达到 1.34 倍，因此不宣称所有读取模式稳定达到 1.5 倍。

## 检查

7 项全部通过，命令与耗时记录在 `bench/results/checks.json`：build、vet、gofmt、完整测试、race、386 交叉编译（`GOARCH=386 go build` / `go vet`）。不宣称 32 位运行验证。

## 模型报告

### 构造回放

`replay_test.go` 生成六类固定种子流量 —— Zipf、扫描、热点切换、均匀混合尺寸/成本、短 TTL、命名空间失效 —— 每类 2 万次请求，对比 LRU、SIEVE、S3-FIFO、W-TinyLFU-AV 与 native AHKC，驻留容量、TTL 和热度资格相同。参考策略是顺序模型，不是相同总 M 下的原生基线。

避免的合成回源成本 / 全部成本：

| 场景 | AHKC | W-TinyLFU-AV |
| --- | ---: | ---: |
| zipf | 45.1% | 42.7% |
| scan | 32.5% | 30.2% |
| hot-shift | 49.3% | 46.7% |
| uniform-size-cost | 4.7% | 4.0% |
| short-ttl | 38.4% | 36.6% |
| namespace-invalidation | 48.7% | 46.5% |

AHKC 用更低的 Zipf 命中率（55.4% vs 63.9%）换取更低的合成成本；该取舍取决于成本权重，不能推断生产净 CPU 收益。报告依赖并发顺序，重跑比例抖动约 0.1 个百分点，而 `TestSyntheticPolicyReplay` 断言始终通过。

### 搜索损失与合并率校准

- **全驻留枚举**（确定性）：128 个决策、87 个正收益差距、26 个可行性差距；本地收益 4214 vs 全驻留 5939，搜索遗漏损失 1725。仅一次静态决策，不是动态 OPT。
- **合并率校准**（确定性）：真实 loader + 手动时钟，验证 λ × 加载时长共 9 个点。λ=1000、100ms 时每 100 个请求合并为一个真实源，校准 r=10/s；验证的是网格与生命周期，不是真实后端。

## 不变量覆盖

| 不变量 | 测试 |
| --- | --- |
| I1 容量守恒 / I2 额度守恒 | `TestReplacementWhenFull`、`TestConcurrentMixedStress`、`TestCloseBlockedByOrphanIO` |
| I3/I4 TTL 与失效后不再命中、不复活 | `TestTTLExpiry`、`TestInvalidateKeyKillsInFlightFill`、`TestNamespaceInvalidateMidFillStale` |
| I5 求解器等于穷举 | `solver_test.go`（规格用例 + 4000 组随机） |
| I6 单一终态 / I7 唯一 leader / I8 等待者只结算一次 | QUEUED→COMMITTING→终态全路径、`TestSingleflightMerge`、`TestWaiterSettleOnce` |
| I9 安全回收 / I11 每请求一次采样 | `epoch_test.go`、拥有型句柄测试、`TestSampleOncePerRequest` |
| Flight 超时清理、COMPLETED_PENDING 复用 | `TestFlightTimeoutThenLateSourceCleanup`、`TestCompletedPendingReuse` |
| 过载与队列满仍交付 | `TestOverload*`、`TestFillQueueFullStillDelivers` |
| 三种估值状态、门卫 ε/δ 误差预算 | `estimate_test.go`、`TestGateErrorBudgetDerivation`、`TestConfigValidationRejects` |
| 窗口历史：对齐、跳窗、增量采集 | `TestHotKeyHistoryRolls`、`TestHotWindow*` |
| 观测：零分配、并发安全、导出 | `TestObserve*`、`TestHotWindow*`、`TestOnHotWindow*` |
| 关闭、完成泵、模型快照、大小边界 | [review_test.go](../review_test.go)、`TestPumpGateFailureDoesNotLoseSignal` |

结果只对记录的硬件、参数与版本有效；真实 72 小时稳态与生产流量需在目标环境验证。
