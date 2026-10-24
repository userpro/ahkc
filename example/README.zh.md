# 可运行示例

**中文** | [English](README.md)

在仓库根目录执行 `go run ./example/<目录>`。程序内的检查会在示例未展示预期行为时失败；手动时钟仅用于确定性演示，真实服务通常使用默认时钟与 ticker。

| 阅读顺序 | 程序 | 展示能力 |
| --- | --- | --- |
| 1 | [quickstart](quickstart/main.go) | New、Namespace、Fetch、Get、单 key 同步失效、Stats、Close |
| 2 | [observe-modes](observe-modes/main.go) | 同一组请求在仅缓存 / 仅统计 / 组合模式下的行为；请求榜、回源表、历史和通知 |
| 3 | [hot-observe](hot-observe/main.go) | 无 loader 的独立请求统计、Heat、请求窗口榜；零加载和零驻留 |
| 4 | [read-semantics](read-semantics/main.go) | Get、BorrowOn、GetBorrowed、GetOwned、Clone 与 Release；浅拷贝及持有生命周期 |
| 5 | [async-load](async-load/main.go) | FetchAsync、singleflight、Future.Take / Cancel / Wait；取消不影响其他等待者 |
| 6 | [hot-export](hot-export/main.go) | HotKeyLabel、OnHotWindow 信号、HotWindowsSince 游标与 lost、JSON 日志、固定维度指标 |
| 7 | [hotline](hotline/main.go) | 真实时钟与并发流量、GetOn→FetchOn、有界 BUSY 重试、窗口命中率、HotKeys、定期失效 |
| 8 | [advanced](advanced/main.go) | WithWeigher、WithResultMetadata、动态命名空间、SetPrior / SetModel、成本观测、硬截止、异步失效、Credits、MemoryLimits、手动维护与三阶段关闭 |

更短的可执行 API 示例见 [example_test.go](../example_test.go)，运行 `go test -run '^Example' .`。缓存内部的时间轮、冷却、替换和过载边界见[测试索引](../docs/VALIDATION.zh.md)，无需为了演示内部算法复制业务代码。

`hotline` 默认运行 12 秒，可用 `-duration=1s -workers=4 -keys=32` 快速体验；`-skew` 须大于 1。`hot-export` 约 3 秒结束。打印的热度是窗口加权次数，不直接代表每秒速率；采样榜单不能用于精确计费。
