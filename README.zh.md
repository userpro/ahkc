# AHKC：缓存与热点统计

**中文** | [English](README.md)

AHKC 是零第三方依赖的 Go 进程内 SDK，面向亿级条目缓存与数万量级热点统计。它按请求热度准入、按“热度 × 回源成本 × 剩余有效期”淘汰，合并并发回源，并提供请求热点、回源热点和窗口历史。

驻留数据放在创建时一次预留的内存区里，不含 Go 指针，GC 扫描量与条目数无关；类型在构造时显式确定，运行期不做反射。

## 选择构造函数与运行模式

| 值类型 | 构造函数 | 存放方式 | 读取 |
| --- | --- | --- | --- |
| `string`、`[]byte`（含命名类型） | `NewBytes[V]` | 原始字节，进内存区 | 借用视图零拷贝、零分配；`Get` 拷贝一次 |
| 数值、数组、无指针结构体 | `NewFlat[V]` | 内存映像，进内存区 | 值拷贝，零分配 |
| 任意对象（含指针、map、接口） | `NewObject[V](cfg, weigh)` | 值留在 Go 堆（分片侧表），会被 GC 扫描 | 值拷贝，零分配 |

`NewFlat` 在构造时用一次反射确认类型不含指针，含指针的类型会返回 `ErrInvalidConfig`；含指针的值不能安全地拷进 GC 不扫描的内存，请用 `NewObject`，由你给出 `weigh`（返回 `EntryHeaderBytes` + key + value 引用的全部内存）。

| 需求 | 配置 | 行为 |
| --- | --- | --- |
| 仅缓存 | 默认 `Mode: ahkc.ModeCache` | 热度准入、收益淘汰、TTL、singleflight；不生成热点榜单 |
| 仅热点统计 | `Mode: ahkc.ModeObserve`，按需开启请求榜与历史 | 值不驻留，条目区与索引只保留最小一块，仅保留热度门卫；`Get` 可记录请求热度，`Fetch` 另提供回源与合并统计 |
| 缓存 + 热点统计 | 普通缓存上开启 `Hot.LoadSlots` / `Hot.RequestSlots` | 保留缓存能力，同时输出回源榜与包含缓存命中的请求榜 |
| 命中率预估 | `Mode: ahkc.ModeShadow` | 模拟准入与淘汰但不交付值：`Stats.Hits` / `Misses` 为虚拟值，所有请求仍照旧回源 |

`HotKeys` 是**回源榜**，不统计缓存命中。包含命中的请求榜在窗口快照的 `Requests` 中。

## 快速开始

要求 Go 1.23+。仓库当前模块名为 `ahkc`，可在本目录直接运行：

```sh
go run ./example/quickstart
go run ./example/observe-modes
```

其他本地项目可在 `go.mod` 中配置 `require ahkc v0.0.0` 和 `replace ahkc => /path/to/ahkc`。仓库未声明可直接 `go get` 的远程模块地址。

```go
package main

import (
    "context"
    "fmt"
    "time"

    "ahkc"
)

func main() {
    ctx := context.Background()
    c, err := ahkc.NewBytes[string](ahkc.Config{
        MaxEntries: 1_000_000,
        MaxBytes:   1 << 30,
        Namespaces: map[string]ahkc.NamespaceConfig{
            "user": {TTL: time.Minute, Tin: 3},
        },
    })
    if err != nil { panic(err) }
    defer c.Close(ctx)

    ns, err := c.Namespace("user")
    if err != nil { panic(err) }
    value, err := ns.Fetch(ctx, "42", func(ctx context.Context, key string) (string, error) {
        return "user:" + key, nil // 替换为实际数据库或 RPC 调用
    })
    if err != nil { panic(err) }
    fmt.Println(value)
}
```

`Fetch` 成功表示结果已交付，填充在后台进行。key 的请求热度达到 `Tin` 且收益为正才可能驻留；`Tin=1` 时首次请求也可能缓存。

单条回源成本不同（例如大对象、慢后端）时，在加载结果元数据里给出 `Cost`，淘汰时按它加权；默认为 1：

```go
ahkc.WithResultMetadata[string](func(key, v string) ahkc.ResultMetadata {
    return ahkc.ResultMetadata{Cost: 5}
})
```

仅统计已有业务请求，无需 loader：

```go
c, _ := ahkc.NewBytes[string](ahkc.Config{
    Mode:       ahkc.ModeObserve,
    Namespaces: map[string]ahkc.NamespaceConfig{"user": {}},
    Hot: ahkc.HotConfig{RequestSlots: 64, Windows: 60, Interval: time.Minute},
})
// 每个业务请求调用一次 ns.Get(key)，不使用返回值；仍须处理错误。
// ns.Heat(key) 查询当前热度；c.HotKeyHistory(1) 获取最近已完成窗口的请求榜。
```

完整程序见 [仅统计示例](example/hot-observe/main.go)。三种模式的相同请求对照见 [observe-modes](example/observe-modes/main.go)。

## 接入时的关键约定

- `Get` 返回 hit / miss / `ErrBusy` / `ErrClosed`；`ErrBusy` 表示本次未能判定，业务应有界重试或降级。
- “先 Get 再 Fetch”及重试复用同一个 `Request`，避免重复采样与计数；`Request` 和借用视图不可复制。
- **借用视图只在 `Release` 前有效**：`NewBytes` 的视图指向内存区，块在释放后会被复用，不要保存。需要长期持有就用 `Get`（拷贝），或先 `Clone` 出拥有型句柄。`Close` 之后继续使用借用视图是错误（内存区已归还）。
- `FetchAsync` 的 `Future` 必须用 `Wait`、`Take`（`ok=true`）或 `Cancel` 终结；重复取结果返回 `ErrConsumed`。借用视图和拥有型句柄必须 `Release`。
- `LoadTimeout` 和调用方取消不会终止真实 I/O；loader 自行设置数据库或 RPC 期限。`Close` 等待真实 I/O、句柄与预算释放，应使用合理超时。
- `NewObject` 的值是浅拷贝；保持共享值不可变，`weigh` 要覆盖完整保留存储。
- 过期值永不交付，但过期条目不会准点释放：读到时、被淘汰扫描到时，或最晚一个慢扫描周期（默认 5 分钟）内退场，在此之前仍占额度。
- 内存区物理页按需提交，退场的块只回到空闲表复用，进程占用停在历史高水位，`Close` 时整体归还。
- 热点为有界近似统计，受采样、锁竞争、槽位淘汰和维护滞后影响，不适合作为精确计费账本。

## 亿级与数万热点的配置要点

```go
cfg := ahkc.Config{
    Shards:     128,       // 单分片内存区上限 32GiB；分片多也让各分片更均匀
    MaxEntries: 100_000_000,
    MaxBytes:   200 << 30, // 以字节预算为准
    Heat: ahkc.HeatConfig{
        GateCells: 1 << 22, // 约为“每个 Heat.Delta 内去重后的未命中 key 数”的 4 倍
    },
    Hot: ahkc.HotConfig{RequestSlots: 512, RequestSampleP: 1.0 / 16, Windows: 60},
    Memory: ahkc.MemoryConfig{
        Prefault: true, // 启动时预触碰索引与门卫，避免线上缺页抖动
        HugePages: true, // 建议与 Prefault 一起开
    },
}
```

- `MemoryLimits.RegionBytes` 是常驻内存上界（条目区 + 索引 + 门卫，含 `Memory.SlackPercent` 余量，默认 15%）。mmap 内存区不计入 Go 的 GOGC 触发线，建议 `GOMEMLIMIT = 容器上限 − RegionBytes − 其他`。
- `MemoryLimits.TotalBytes` 还包含 `ResponseBytes`（默认 `MaxConcurrentLoads × MaxResultBytes`，是正在运行的加载的上界，默认 256 × 4MiB = 1GiB；加载完成后只占实际大小）。驻留额度很小时它会显得很大，按需调小 `MaxConcurrentLoads` 或 `ResponseBytes`。
- 追踪 1 万到 6.5 万个热点 key 时，每分片槽位保持在几百到几千，`Hot.RequestSampleP` 不高于 1/16。让第 k 名 key 每窗口至少有 n 个样本，需要 `q ≥ n / (f_k × 窗口秒数)`。

## 文档与示例

| 入口 | 内容 |
| --- | --- |
| [使用指南](docs/USAGE.zh.md) | API、配置、读取所有权、失效、关闭与排障 |
| [热点统计](docs/OBSERVABILITY.zh.md) | 统计口径、单独使用、窗口、游标、脱敏与通知 |
| [示例索引](example/README.zh.md) | 按能力查找可运行程序 |
| [架构原理](ARCHITECTURE.zh.md) | 内存模型、数据流、准入与替换、惰性退场与安全回收 |
| [源码走读](docs/WALKTHROUGH.zh.md) | 文件与关键函数的阅读路线 |
| [验证与性能](docs/VALIDATION.zh.md) | 测试入口、实测数据、证据范围 |

```sh
go test ./...
go test -race ./...
go vet ./...
```

范围：进程内、非持久化、不提供跨实例一致性。`MemoryLimits` 是按配置推导的保留上界，不等于进程 RSS。

## 许可证

MIT，见 [LICENSE](LICENSE)。
