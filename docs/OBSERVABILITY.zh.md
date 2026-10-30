# 热点统计与导出

**中文** | [English](OBSERVABILITY.md)

[入门](../README.zh.md) · [使用指南](USAGE.zh.md) · [原理](../ARCHITECTURE.zh.md#热点窗口) · [可运行示例](../example/README.zh.md)

## 先选统计口径

| 接口 | 统计内容 | 配置 |
| --- | --- | --- |
| `ns.Heat(key)` | 当前请求热度：miss 门卫 + 驻留条目热度 | 始终可用，不额外记录访问 |
| `c.HotKeys(n)` | 即时回源榜：真实加载热度、累计加载与合并数 | `Hot.LoadSlots > 0` |
| `snapshot.Top` | 已结束窗口的回源加载与合并次数 | 回源表 + 历史或通知 |
| `snapshot.Requests` | 已结束窗口的请求数估计，包含缓存命中与 Get-only 流量 | 请求表 + 历史或通知 |

普通缓存可选择开启任意一张表。`Mode: ahkc.ModeObserve` 禁止值驻留，其他请求与加载语义保留；热点表在设置 `Hot.LoadSlots` / `Hot.RequestSlots` 之前保持关闭（各模式一致，0 即关闭，负数会被拒绝）。仅统计已有请求时，每次调用一次 `Get`，无需提供 loader；此时回源榜为空。见 [hot-observe](../example/hot-observe/main.go)。

仅统计模式可省略 `MaxEntries`、`MaxBytes` 和命名空间 TTL：条目区只留 4 KiB、索引只留一组，增大 `MaxEntries` 不会增加这部分存储，热度门卫保持原尺寸。加载、等待者与维护资源仍按配置保留，`MemoryLimits` 包含未实际占用的动态预算上限，不等于初始化堆分配量。

统计以一次成功 `BeginRequest` 为请求采样入口，包括后续可能因竞争或过载而未交付的请求。Get→Fetch 或重试复用同一个 `Request`，否则会重复计数。

## 配置

```go
Hot: ahkc.HotConfig{
    LoadSlots:      128,         // 回源表，每分片
    RequestSlots:   64,          // 请求表，每分片
    RequestSampleP: 1.0 / 1024,  // 请求采样率 q
    Windows:        60,
    Interval:       time.Minute, // 与 Heat.Delta 独立
    TopN:           16,          // 每张榜单的上限
    OnWindow: func(ahkc.HotSnapshot) { /* 非阻塞地通知消费者 */ },
},
```

| 字段 | 默认与约束 |
| --- | --- |
| `Hot.LoadSlots` | 各模式均为 0（关闭）；负数会被拒绝 |
| `Hot.RequestSlots` | 0；大于 0 时须启用历史或通知 |
| `Hot.RequestSampleP` | 请求表开启时默认 1/1024；范围 [2^-64, 1] |
| `Hot.Windows` | 0，不保留历史；可单独配置通知 |
| `Hot.Interval` | 默认 `Heat.Delta`；与默认不同时范围 [1ms, 24h]，且启用窗口时须满足增量采集周期校验 |
| `Hot.TopN` | 16，每窗口每榜的条数 |
| `Hot.OnWindow` / `Hot.Label` | nil；前者接收通知，后者映射或剔除导出的 key |

历史或通知至少需要一张追踪表。显式独立窗口的最小宽度为 `(⌈总槽位/16384⌉ + 1) × PeriodicInterval`。槽位按分片整体采集，16384 是每周期的采集目标预算；一个分片较大时可超过该目标。

追踪表对每次记录是 O(1)：定位用开放寻址哈希，逐出用下标最小堆；每分片另有一份保守更新的 Count-Min 前置过滤，估计值没超过当前最小计数的新 key 不入槽，所以长尾冷 key 不会把真实热点挤出榜单。实测见[验证文档](VALIDATION.zh.md#热点追踪)。

## 数值如何理解

- `Heat` 和 `LoadHeat` 都是双桶加权次数，`current + (1-φ) × previous`；宽度为 `Heat.Delta`。`LoadHeat` 不是直接的每秒加载率，除以窗口秒数才能作速率近似。
- `Loads` / `Merged` 是当前槽位入驻以来的累计数，key 被逐出后重置，不是持久累计指标。`HotKeys(n)` 按 `LoadHeat`、`Merged`、`Loads` 降序；`n <= 0` 返回全部已追踪 key。
- `WindowLoads` / `WindowMerged` 是该窗口中 key 入槽以来记录的事件数。`WindowRequests` 是记录的请求样本数乘 `1/q`。
- `WindowError` 是槽位入驻时继承的误差上界（被前置过滤放行时取其估计值作为误差）；榜单按“观测值 + 误差”排序，真实采样数落在 `[观测值, 观测值 + 误差]` 之间。无丢弃和饱和时，它约束的是实际记录到的事件或样本数；请求榜折算 `1/q` 后仍有采样误差，不是对真实请求量的确定区间。

门卫碰撞可高估热度，饱和与竞争可低估；槽位不足会逐出 key。q=1 适合小流量演示，默认采样率适合先控制开销。低频 key 可能没有样本；榜单缺席不等于没有流量。

## 窗口与质量信号

内置时钟按创建时的 Unix 偏移对齐窗口，运行期间不随 NTP 调整；自定义时钟从其原点对齐。历史窗口号是 `(时钟 + 偏移) / Hot.Interval`。

事件按记录时间归窗。迟到事件仅在所属缓冲尚未采集且完好时写回，否则进入最新未采集窗口。已发布历史不再被维护者改写；不是事件时间回放接口。仅发布完整结束的窗口，关闭不强制导出当前未结束窗口。

| 质量字段 | 含义 |
| --- | --- |
| `Skipped` | 与上一快照之间未发布的窗口数 |
| `Partial` | 某些分片的数据已被较新窗口覆盖 |
| `Totals.Evicted` | 采集前被覆盖或逐出的事件数；请求表按样本数计 |
| `Totals.LoadEventsDropped` / `RequestSamplesDropped` | 追踪锁竞争丢弃的事件，按采集时刻归窗 |
| `Totals.Requests` | 已采集样本加竞争丢弃样本后折算 `1/q`；维护滞后仍可丢失窗口数据 |

`Seq` 缺口、`Skipped` 与 `Partial` 含义不同：分别表示消费者漏读、未发布窗口与已发布窗口的数据不全。

## 导出与所有权

| 接口 | 用法 |
| --- | --- |
| `HotKeyHistory(n)` | 最近 n 份，`n <= 0` 读取全部保留历史；时间升序 |
| `HotWindowsSince(seq)` | 全部 `Seq > seq` 的保留快照及覆盖丢失数 `lost`；保存最后的 Seq 为游标 |
| `Hot.OnWindow` | 串行、Seq 单调、至多一次、可缺窗；上个回调未完成或执行器拒绝时丢弃通知 |
| `Hot.Label` | 对所有榜单输出映射 key；`keep=false` 剔除；panic 只剔除该项并计数 |

返回切片和回调中的榜单归消费者所有，修改不会影响内部历史或其他消费者。导出每批榜单使用一块副本；设置标签钩子时在副本上完成映射。标签映射不会改写原始追踪表，也不会改变总量。

回调与映射必须有界、非阻塞；映射可能被并发调用。回调 panic 被隔离并计入 `HotExportPanics`；忙回调计 `HotWindowBusy` 和 `HotWindowDropped`，投递失败计 Dropped。不要在回调内同步等待 `Close`，因为回调本身属于关闭需要等待的工作。

推荐“回调只发信号 + 消费者游标拉取 + 定时兜底”，见 [hot-export](../example/hot-export/main.go)。需要补齐必须启用 `Hot.Windows`，且在历史覆盖前消费。库不内置监控服务；哈希 key 不会消除指标基数，动态热点更适合日志，固定聚合量适合指标。

## 开销与排障

追踪表关闭时不分配槽位。请求追踪写入不增加堆分配，使用短临界区；竞争失败会有原子丢弃计数。回源追踪复用 flight 已拥有的不可变 key。窗口历史以无指针的列式记录加一块 key 字节保存，长期保留的历史不增加 GC 扫描量；只有导出（`HotKeyHistory`、`HotWindowsSince`、`Hot.OnWindow`）时才一次性物化 `HotKeyStat`（一次切片分配加一个拼接字符串），调用方保留的输出由调用方管理。相关回归与基准见[验证文档](VALIDATION.zh.md)。

历史为 nil：检查是否保留历史以及是否跨过窗口边界。没有通知：检查 Busy / Dropped 与关闭状态。`DisableTicker` 时必须持续驱动 `MaintenanceStep`；窗口太短或维护太慢会出现缺窗。仅统计模式的 `Get` 仍可能返回 `ErrBusy` / `ErrClosed`，不能忽略错误。
