# AHKC：缓存与热点统计

**中文** | [English](README.md)

AHKC 是零第三方依赖的 Go 进程内 SDK。它按请求热度与预期收益决定缓存哪些值，合并并发回源，并提供请求热点、回源热点和窗口历史。

## 选择使用方式

| 需求 | 配置 | 行为 |
| --- | --- | --- |
| 仅缓存 | 默认配置 | 热度准入、收益淘汰、TTL、singleflight；不生成热点榜单 |
| 仅热点统计 | `ObserveOnly: true`，按需开启请求榜与历史 | 值不驻留；`Get` 可记录请求热度，`Fetch` 另提供回源与合并统计 |
| 缓存 + 热点统计 | 普通缓存上开启 `ObserveHotKeys` / `ObserveRequestKeys` | 保留缓存能力，同时输出回源榜与包含缓存命中的请求榜 |

`HotKeys` 是**回源榜**，不统计缓存命中。包含命中的请求榜在窗口快照的 `Requests` 中。仅统计模式可省略驻留容量和 TTL；不分配驻留条目池，仍校验结果大小并保留有界采样、加载和维护资源。

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
    c, err := ahkc.New[string](ahkc.Config{
        MaxEntries:       1024,
        MaxResidentBytes: 64 << 20,
        Namespaces: map[string]ahkc.NamespaceConfig{
            "user": {TTL: time.Minute, Tin: 3, Prior: &ahkc.Prior{Lambda: 10}},
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

`Fetch` 成功表示结果已交付，填充在后台进行。达到 `Tin`、取得有效估值且净收益为正时才可能驻留；`Tin=1` 时首次请求也可能缓存。示例中的 `Prior` 为冷启动提供请求率先验，生产中应按流量设置和更新。

仅统计已有业务请求，无需 loader：

```go
// 在上述配置中设置 ObserveOnly: true，并添加：
Advanced: ahkc.Advanced{
    ObserveHotKeys:        -1, // 关闭仅统计模式默认启用的回源榜
    ObserveRequestKeys:    64,
    HotHistoryWindows:     60,
    HotHistoryInterval:    time.Minute,
}
// 每个业务请求调用一次 ns.Get(key)，不使用返回值；仍须处理错误。
// ns.Heat(key) 查询当前热度；c.HotKeyHistory(1) 获取最近已完成窗口的请求榜。
```

完整程序见 [仅统计示例](example/hot-observe/main.go)。三种模式的相同请求对照见 [observe-modes](example/observe-modes/main.go)。

## 接入时的关键约定

- `Get` 返回 hit / miss / `ErrBusy` / `ErrClosed`；`ErrBusy` 表示本次未能判定，业务应有界重试或降级。
- “先 Get 再 Fetch”及重试复用同一个 `Request`，避免重复采样与计数；`Request` 和借用视图不可复制。
- `FetchAsync` 的 `Future` 必须用 `Wait`、`Take`（`ok=true`）或 `Cancel` 终结；重复取结果返回 `ErrConsumed`。借用视图和拥有型句柄必须 `Release`。
- `LoadTimeout` 和调用方取消不会终止真实 I/O；loader 自行设置数据库或 RPC 期限。`Close` 等待真实 I/O、句柄与预算释放，应使用合理超时。
- 返回的 Go 值是浅拷贝；保持共享值不可变。自定义引用类型须提供能覆盖完整保留存储的 `WithWeigher`。
- 热点为有界近似统计，受采样、锁竞争、槽位淘汰和维护滞后影响，不适合作为精确计费账本。

## 文档与示例

| 入口 | 内容 |
| --- | --- |
| [使用指南](docs/USAGE.zh.md) | API、配置、读取所有权、失效、关闭、成本模型与排障 |
| [热点统计](docs/OBSERVABILITY.zh.md) | 统计口径、单独使用、窗口、游标、脱敏与通知 |
| [示例索引](example/README.zh.md) | 按能力查找可运行程序 |
| [架构原理](ARCHITECTURE.zh.md) | 数据流、准入与替换、并发与资源边界 |
| [源码走读](docs/WALKTHROUGH.zh.md) | 文件与关键函数的阅读路线 |
| [验证与性能](docs/VALIDATION.zh.md) | 测试入口、性能复现、证据范围 |
| [原始技术方案](docs/TECH.zh.md) | 设计推导与伪代码；当前行为以实现及使用指南为准 |

```sh
go test ./...
go test -race ./...
go vet ./...
```

范围：进程内、非持久化、不提供跨实例一致性。`MemoryLimits` 是按宿主计费契约推导的保留上界，不等于 Go 堆或进程 RSS。

## 许可证

MIT，见 [LICENSE](LICENSE)。
