# 验证与性能

**中文** | [English](VALIDATION.md)

[快速上手](../README.zh.md) · [架构](../ARCHITECTURE.zh.md)

本页只记录本机实测的数字与命令。结果只对记录的硬件、参数与版本有效；生产环境须在目标硬件上复测。

## 状态

环境：Go 1.25.4、linux amd64、16 核、31 GiB 内存。`go build`、`go vet ./...`、`gofmt -l`（无输出）、`go test -count=1 ./...`（约 5 s）、`go test -race -count=1 .`（约 55 s）、`GOARCH=386 go vet .` 与 `GOARCH=386 go test .`（约 8 s）全部通过。386 是在 64 位机器上运行 32 位测试，不是 32 位硬件验证。

尚未证明：真实 72 小时稳态、跨 NUMA 扩展、亿级条目的实测（下表为千万级实测，亿级为外推）、真实后端净 CPU 收益、业务端到端回放。`TestRepeatedFillAndExpiryLeaksNothing` 只是模拟 72 小时。

## 规模：驻留与 GC

`scale_test.go` 默认跳过，用环境变量开启，填满 N 个变长值后测量：

```bash
AHKC_SCALE_ENTRIES=10000000 AHKC_SCALE_VMIN=16 AHKC_SCALE_VMAX=240 AHKC_SCALE_OBJECT=0 \
  go test -run TestScaleResidentGC -v -timeout 30m .
```

16 分片、`NewBytes[string]`、值长度均匀分布在 [16, 240] 字节、键为 `key-<n>`、热度准入 `Tin=1`：

| 条目数 | 驻留 | 驻留字节 | 预留内存区 | 进程 RSS | Go 存活堆 | GC 扫描堆 | 创建 | 填满 | 随机命中 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 万 | 99% | 443 MiB | 659 MiB | 488 MiB | 3 MiB | 2 MiB | 1 ms | 4.8 s | 629 ns |
| 1000 万 | 99% | 2222 MiB | 3299 MiB | 2379 MiB | 3 MiB | 2 MiB | 1 ms | 24.4 s | 643 ns |

- **GC 扫描量与条目数无关**：条目数 5 倍，Go 存活堆与 GC 扫描堆都不变（3 MiB / 2 MiB）。
- **驻留与预算相符**：驻留字节（含 88 字节块头与对齐）2222 MiB，进程 RSS 2379 MiB（含运行时与索引、门卫等），预留的 3299 MiB 里只有被写过的页才占物理内存。
- **创建近乎瞬时**：1–2 ms，内存区与索引是 `mmap` 的虚拟地址，按需提交。
- “随机命中”包含测试循环本身与基准之外的开销，只用来比较，不是单次读耗时（单次读耗时见下“读路径与分配”）。
- 驻留 99% 而不是 100%：测试里 `MaxEntries` 恰好等于 key 数，而每分片的条数额度固定为 `MaxEntries / Shards`（没有分片间转移）。哈希分布有二项式偏差，分到超额 key 的分片只能自己淘汰：30 万 key、16 分片的复现里，按二项分布估算每分片标准差约 133、超额总和约 850（估算值，不是测量值），实测发布 299,967 个、驻留 299,202 个（差 798），`FillRejected` 只有 32 次（`INFEASIBLE` 27、`NOT_HOT` 4、`NON_POSITIVE_GAIN` 1）。条数额度应留出余量，不要恰好等于预期 key 数；分片越多、每分片条数越少，相对偏差越大。

字节布局与对象布局同口径对照（200 万条目，值长度 [64, 2047] 字节，同一次运行，其余参数同上）：

```bash
AHKC_SCALE_ENTRIES=2000000 go test -run TestScaleResidentGC -v .
```

| 布局 | 驻留 | 进程 RSS | Go 存活堆 | GC 扫描堆 | 填满 | 随机命中 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `NewBytes`（值在内存区） | 99% | 2247 MiB | 3 MiB | 2 MiB | 7.1 s | 700 ns |
| `NewObject`（值在 Go 堆） | 99% | 3302 MiB | 2161 MiB | 37 MiB | 4.0 s | 683 ns |

同样的数据，对象布局的 Go 存活堆是字节布局的约 720 倍，GC 扫描堆约 18 倍，RSS 高约 50%（Go 堆需要 GC 余量）；命中耗时没有差别。对象布局的存活堆随数据量线性增长，这是 `NewObject` 的设计代价，字节与定长布局不受影响。

亿级是外推而不是实测：字节布局的 Go 堆应仍是几 MiB（只含与条目数无关的固定结构），物理内存约为“每条目块头 + key + value + 约 15% 余量”，以实际 `MemoryLimits.RegionBytes` 为准；创建耗时取决于是否开启 `Prefault`（不开则是 mmap 的虚拟预留，毫秒级）。1000 万条目、16 分片已实测，更大规模请在目标机器上用 `scale_test.go` 复测。

## 百万级全周期验收

`TestScaleLifecycle`（默认跳过）用真实时钟和内置周期触发（TTL 与慢扫描周期按规模缩短）跑完一个实例的完整生命周期：填充、未命中探测、命中、满载替换、热度衰减、过期、退休与回收。

```bash
AHKC_SCALE_LIFECYCLE=1000000 go test -run TestScaleLifecycle -v -timeout 10m .
```

16 分片，`NewBytes[string]`，值长度在 [16, 240] 字节均匀分布，100 万条目，字节额度每条 288：

| 检查项 | 实测 |
| --- | --- |
| 创建 | 1 ms |
| 填充 | 1.8 s，55.3 万次/秒，驻留 99.1%（其余是分片额度超额的 key） |
| 内存 | 计费 235 B/条，进程 RSS 增加 265 MiB（281 B/条，比计费多 20%：索引、门卫、运行时）；Go 存活堆 4 MiB、GC 扫描堆 3 MiB，与条目数无关 |
| 满载索引上的未命中 | 平均访问 2.2 个组（上限 8） |
| 借用命中，单协程 | p50 581 ns，p99 1.0 us，p99.9 8.1 us（258 MiB 内存区上的随机 key，已扣除计时开销），40 万次操作 0 分配 |
| 借用命中，16 协程 | 1800 万次/秒 |
| 满载后 25 万个冷 key | 发布 17,346 次替换；受保护的 19,808 个热条目全部保留 |
| 热度衰减后 5 万个新热 key | 驻留 97.1%（其余是分片额度超额的 key）；驻留总数始终不超过 `MaxEntries` |
| 过期 | TTL 之前不移除任何条目；不交付过期值；慢扫描周期 10 s 时，最后一个条目在最后截止时刻后 10.0 s 退场；退休块全部回收 |
| 退休与回收分配 | 100 万条目从过期到回收全程 0 次分配 |
| 全部回收后的空闲空间 | 每分片一整块 19 MiB，共 315 MiB 空闲 |

单协程时延主要来自远大于 CPU 缓存的内存区上的缓存缺失（“读路径”里 4096 个 key 的基准仍是 94 ns）；尾部是操作系统与运行时噪声。

## 热点追踪

`TestScaleHotTracker`（默认跳过）：16 分片共 65536 个请求追踪槽，1 亿个 key 的 Zipf(1.1) 流量，2000 万个采样事件单线程记录：

```bash
AHKC_SCALE_HOT=20000000 go test -run TestScaleHotTracker -v .
```

| 指标 | 实测 |
| --- | ---: |
| 记录耗时 | 64.7 ns/事件，0 分配 |
| 锁竞争丢弃 | 0（单线程，不代表并发下的丢弃率） |
| top-1 万召回率（对照精确计数，精确计数只覆盖前 2^24 名） | 99.7% |
| 窗口采集（65536 槽全部扫一遍） | 5.7 ms |
| 排序并写成列式记录（1 万条） | 2.1 ms |
| 历史占用：1440 个窗口 × 1 万条 | 851 MiB（每条记录含 key 字节），GC 扫描堆 0 MiB |
| 追踪表数组 | 12 MiB |

记录耗时与槽位数的关系（`BenchmarkTrackerRecordBySlots`，80% 冷长尾 key）：

| 每分片槽位 | 256 | 4096 | 65536 |
| --- | ---: | ---: | ---: |
| ns/次 | 71.7 | 80.2 | 45.8 |

耗时不随槽位数线性增长（旧实现在 256、1024、4096、16384 槽分别为 168、543、2181、7349 ns）。65536 槽更快是因为过滤器放行少、更多事件命中已追踪的热 key。召回率与耗时来自单线程回放；多线程竞争下的丢弃率由 `Stats.HotRequestSamplesDropped` 报告，`cacheload -hot-slots` 可在压测下观察。

正确性：`tracker_test.go` 校验定位表与堆的内部不变量、`[rank−err, rank]` 上界、长尾冲击下前 100 名热点不被挤出、窗口滚动与后移删除。

## 读路径与分配

单线程中位数，三次取范围（`go test -bench`）：

| 路径 | ns/op | 分配 |
| --- | ---: | ---: |
| `Get` 命中（`NewBytes[string]`，拷贝一次） | 110–111 | 1 |
| 借用命中（`BeginRequestOn` + `BorrowOn`） | 103–105 | 0 |
| 并行 `Get` 命中（吞吐倒数） | 21–22 | — |
| 并行 miss（吞吐倒数） | 19–20 | — |
| 门卫读写（一次 miss 的热度记录） | 73 | 0 |
| 热度更新 / 饱和跳写 | 6.4 / 4.1 | 0 |
| epoch pin + unpin | 11.3 | 0 |

借用命中零分配由 `TestGetHitAllocContract` 断言；`NewFlat` 的 `Get` 零分配由 `TestFlatGetHitZeroAlloc` 断言；请求热点追踪开启时的借用命中仍为零分配（`TestObserveRequestZeroAlloc`）。借用与 `Get` 命中耗时都含一次单调时钟读取（TTL 校验必需）。

## 一键复现

```bash
./bench/reproduce.sh              # 约 5–8 分钟，产物写入 bench/results/（已忽略入库）
DURATION=2s BENCHTIME=100ms ./bench/reproduce.sh  # 冒烟，数字不可与本页表格对照
```

脚本产出：环境指纹、7 项检查（build、vet、gofmt、test、race、386 build、386 vet）、微基准、分配与初始化基准、观测开关与 `HotKeys` 基准、5 个压测场景（纯借用、混合、混合第二种子、99.9% 集中、拥有型）、策略回放与搜索 oracle 报告，以及汇总。冒烟模式在本机全流程通过（5 个场景全部达标：吞吐约 110 万 QPS，P99 1.26–2.45 ms，错误 0）。千万级规模测试与热点规模测试不在脚本里，用上面的环境变量单独运行。

## 压测

`cacheload` 按毫秒批次提供 110 万 QPS，延迟从预定到达算到值访问与 guard 释放，含积压、有界重试与 GC。验收标准：成功 QPS ≥ 100 万、P99 ≤ 3 ms、最终错误为零。混合流量 95% 预热命中 / 3% 冷查询 / 2% 禁缓存，loader 墙钟 500 µs，64 分片、256 个客户端：

```bash
go run ./cmd/cacheload -duration 20s
# 规模参数：-entries -value-min -value-max -hot-slots -prefault -hugepages
```

同一台机器、同一工具，改造前提交与当前版本交替运行：

| 版本 | 运行数 | P99 范围 | 失败运行（任一最终错误或 P99 > 3 ms） |
| --- | ---: | --- | ---: |
| 改造前 | 53 | 2.37–2.40 ms | 0 |
| 当前 | 65 | 2.37–2.42 ms（一次 5.7 ms） | 4 |

两者平均吞吐都是 110 万 QPS。**当前版本出现过间歇性的最终错误，改造前很少出现，根因没有定位**。已查明的事实：

- 错误文本都是 `ErrBusy`：压测里同一请求有界重试，直到 512 次或距到达 10 ms 仍未判定。失败的运行里一般是 1 到 5 个错误，个别运行几十个。
- **不是读者槽或 epoch**：新增的 `Stats.PinSlotsExhausted`、`PinEpochRetriesExhausted` 在出错的运行里都是 0。
- 临时给 `getOrLoad` 的每个 `ErrBusy` 返回点计数（已撤销）：每次运行里 `ErrBusy` 主要来自“同 key 的 flight 已存在但还没完成”，每 8 秒约 4 千到 1 万次，另有约 200 次 stripe 锁竞争；两次出错运行里前者升到 2.5 万和 5.2 万。最终错误有两种形态：第一次调用就等了 11 到 13 ms 才返回；或连续 512 次立即 `ErrBusy`，耗时 0.7 到 5 ms。两者都像是 flight 的完成被调度停顿拖住，而不是逻辑卡死。
- **与 GC 有关，但不能完全解释**（每格 25 次运行，6 秒）：

  | 版本 | GOGC | 出错运行 / 运行数 |
  | --- | --- | --- |
  | 现在 | 100 | 11 / 150（先后 7、3、1、0、0、0） |
  | 现在 | 1000 | 2 / 50 |
  | 现在 | 关闭 | 0 / 50 |
  | 现在 | 30 | 0 / 50 |
  | 改造前 | 100 | 1 / 100 |
  | 改造前 | 30 | 0 / 100 |

  现在版本的 Go 存活堆约 32 MiB（改造前 67–92 MiB），同样的分配率下 GOGC=100 的 GC 频率约为改造前的 3 倍；但改造前在 GOGC=30（GC 更频繁）下也没出错，所以“GC 更频繁”本身不是充分原因。
- **失败成簇出现**：先后各 25 次一组的运行里是 7、3、1、0，之后最近的 200 次（两个版本、两种 GOGC 各 50 次）全是 0。同一台机器上其他负载的影响没有排除。

结论：没有证据表明库在稳态下会出错，也没有证据排除一个很小的回归；压测工具用 10 ms 重试预算去撞调度停顿，对此很敏感。上线前请在自己的负载下复测，对 `ErrBusy` 做有界重试，并把 `GOMEMLIMIT` 设好以减少 GC 频率（见 [README](../README.zh.md)）。复现时直接读 `Stats` 的 `Pin*` 计数，就能判断是不是读者槽。

## 关闭

关闭时慢扫描一次摘除全部驻留条目，退休链有进展就连续推进（不等周期节流）。30 万条目、16 分片、`NewBytes`：`Close` 从 9.1 s 降到 0.32 s；200 万条目规模测试总耗时从 66 s 降到 8.9 s，1000 万条目为 36.6 s（修复前 324 s）。`TestReclaimContinuesWhileMakingProgress` 与 `TestReclaimBlockedByHandleDoesNotSpin` 分别断言“有进展就续行”和“被句柄挡住时不空转”。

## 索引

组表（一条缓存行 7 个槽，组头记溢出计数，无墓碑）在 70% 装载率下反复先删后插 30 万次后：每组溢出计数与在册条目逐组一致；未命中平均访问约 3.0 个组（稳态 2.5–3.4，与轮换次数无关），全新装载时约 1.3 个组；探测上限拒绝的插入不到 1%。装载率 85% 时同样的 churn 下拒绝率约 0.4%（模拟），未命中约 4 个组。默认装载率取 0.85；更在意填充拒绝率或未命中耗时时，把 `Memory.LoadFactor` 调到 0.7。

## 检查清单

| 检查 | 命令 |
| --- | --- |
| 构建 / 静态检查 / 格式 | `go build ./...`、`go vet ./...`、`gofmt -l .` |
| 测试 | `go test -count=1 ./...` |
| 竞态与指针检查 | `go test -race -count=1 .`（-race 构建内存区放在 Go 堆上，保留 checkptr 覆盖） |
| 32 位 | `GOARCH=386 go vet .`、`GOARCH=386 go test .` |

## 不变量覆盖

| 不变量 | 测试 |
| --- | --- |
| 容量与额度守恒 | `TestReplacementWhenFull`、`TestConcurrentMixedStress`、`TestCloseBlockedByOrphanIO` |
| TTL 与失效后不再命中、不复活 | `TestTTLExpiry`、`TestInvalidateKeyKillsInFlightFill`、`TestNamespaceInvalidateMidFillStale` |
| 求解器等于穷举 | `solver_test.go`（规格用例 + 4000 组随机） |
| 单一终态 / 唯一 leader / 等待者只结算一次 | QUEUED→COMMITTING→终态全路径、`TestSingleflightMerge`、`TestWaiterSettleOnce` |
| 安全回收 | `epoch_test.go`、拥有型句柄测试、`TestReclaimSkipsPinnedPrefix` |
| 分配器正确性 | `arena_test.go`（随机对照模型：块不重叠、账目守恒、释放后完全合并、满额拒绝） |
| 索引 | `index_test.go`（溢出计数逐组一致、churn 稳态、并发读者与唯一写者） |
| 惰性退场 | `lifecycle_test.go`（提示复核不误删、慢扫描覆盖周期、软预算、命名空间代际）、`TestExpiredEntryRetiredByAccessHint` |
| 满载后可替换 | `evict_test.go` |
| Flight 超时清理、COMPLETED_PENDING 复用 | `TestFlightTimeoutThenLateSourceCleanup`、`TestCompletedPendingReuse` |
| 过载与队列满仍交付 | `TestOverload*`、`TestFillQueueFullStillDelivers` |
| 窗口历史：对齐、跳窗、增量采集、列式往返 | `TestHotKeyHistoryRolls`、`TestHotWindow*`、`TestHistoryColumnarRoundTripOrder` |
| 驻留结构无指针 | `TestEntryHeaderLayout`、`TestHistoryRecordIsPointerFree` |
| 关闭、完成泵、大小边界 | [review_test.go](../review_test.go)、`TestPumpGateFailureDoesNotLoseSignal` |
| 构造契约（`NewFlat` 拒绝含指针类型、`NewObject` 要求 `weigh`） | `TestConstructorsValidateValueType` |

## 构造回放

`replay_test.go` 生成六类固定种子流量（种子 20261001）—— Zipf、扫描、热点切换、均匀混合尺寸/成本、短 TTL、命名空间失效 —— 每类 2 万次请求，容量 24 条，对比 LRU、SIEVE、S3-FIFO、W-TinyLFU-AV 与 AHKC，驻留容量、TTL 和热度资格相同。参考策略是顺序模型，不是相同总内存下的原生基线。`TestSyntheticPolicyReplay` 不断言策略优劣。默认只把一小段流量回放给 AHKC，检查库的不变量（驻留条数与字节不超额度、没有在途 I/O、每次加载都已结算）；设置 `AHKC_REPLAY_REPORT=路径` 才会额外跑四个参考策略的完整规模并写出 JSON。

命中率 / 避免的合成回源成本占全部成本的比例（本机一次运行）：

| 场景 | LRU | SIEVE | S3-FIFO | W-TinyLFU-AV | AHKC |
| --- | ---: | ---: | ---: | ---: | ---: |
| zipf | 54.9 / 36.9 | 57.9 / 40.6 | 59.2 / 41.9 | 63.3 / 42.6 | 62.2 / **44.9** |
| scan | 42.5 / 25.3 | 45.9 / 29.4 | 47.2 / 30.5 | 49.6 / 30.9 | 48.8 / **32.7** |
| hot-shift | 56.7 / 39.4 | 61.5 / 43.5 | 61.2 / 44.2 | 71.1 / 47.5 | 68.8 / **47.9** |
| uniform-size-cost | 4.8 / 4.3 | 5.6 / 4.2 | 5.5 / 4.1 | 8.6 / 4.2 | 7.1 / **4.7** |
| short-ttl | 53.3 / 36.1 | 53.8 / 36.4 | 53.7 / 36.2 | 56.2 / **37.1** | 54.8 / 36.8 |
| namespace-invalidation | 56.6 / 39.3 | 61.2 / 43.5 | 61.2 / 43.9 | 70.0 / 47.2 | 64.6 / **49.0** |

读法：AHKC 的避免成本在六类里五类最高、在 short-ttl 上略低于 W-TinyLFU-AV，但命中率在多数场景低于 W-TinyLFU-AV —— 它按“热度 × 成本 × 剩余有效期”淘汰，用较低的命中率换较高的成本避免；该取舍取决于合成的成本权重，不能推断生产净 CPU 收益。报告依赖并发顺序，重跑比例会有小幅抖动。
