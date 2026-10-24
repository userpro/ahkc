// Package ahkc 实现自适应热度缓存库（Adaptive Hotness Key Cache，AHKC）：
// 一个嵌入线上服务的进程内并发缓存 lib。
//
// 核心设计（设计文档见 docs/TECH.md（中文 docs/TECH.zh.md），
// 机制图解见 ARCHITECTURE.md；中文版见 ARCHITECTURE.zh.md）：
//
//   - 时间热度准入：双时间桶近似滑动窗口 + 非驻留 Count-Min 门卫，
//     进入阈值高于退出阈值形成滞回。
//   - 统一收益估计：稀疏采样 + 固定宽度加权频率 sketch，
//     估值状态为 MEASURED / PRIOR / UNAVAILABLE。
//   - SIEVE 有界候选集 + 精确局部替换：扫描上限 B_scan、候选集 m<=8，
//     在冻结快照上枚举全部可行子集取最小损失。
//   - 受限回源合并：同 (key, 版本令牌) 最多一个真实加载（singleflight），
//     Flight 状态机 LOADING/COMPLETING/COMPLETED_PENDING/TIMED_OUT_IO_RUNNING/TERMINAL。
//   - 硬 TTL + 层级时间轮 + 主动冷却扫描。
//   - 三层额度与预算凭证：条数/驻留字节/交接/退休/响应/加载控制分别计量，
//     凭证只结算一次。
//   - 分片 epoch 读者保护 + 拥有型句柄 + 安全回收。
//   - 单维护者调度：分类配额、公平轮转、定时补偿、额度转移。
//   - 观测：ObserveOnly 仅统计不缓存；Heat / HotKeys 把高度重复的数据变成
//     可查询状态；窗口历史（回源热点 + 请求热点）经 HotWindowsSince /
//     OnHotWindow 导出，HotKeyLabel 做 key 脱敏（observe.go、hotwindow.go；
//     见 docs/OBSERVABILITY.md）。
//
// 正确性契约见各文件注释与 README.md；核心不变式由测试覆盖，
// 清单见 docs/VALIDATION.md。
package ahkc
