// Package ahkc 实现自适应热度缓存库（Adaptive Hotness Key Cache，AHKC）：
// 一个嵌入线上服务的进程内并发缓存 lib，面向亿级条目与数万量级热点统计。
//
// 设计机制见 ARCHITECTURE.md（中文 ARCHITECTURE.zh.md），使用见 docs/USAGE.md。核心设计：
//
//   - 类型显式：NewBytes（字节串类）、NewFlat（无指针定长类型）、NewObject（任意对象，须提供计费函数）。
//     前两类把值的内存映像直接存进每分片预留的内存区，运行期不做反射、不需要序列化。
//   - 一次规划、全部预留：条目区（TLSF 分配器）、索引组表、热度门卫在创建时一次预留，运行期不扩容；
//     驻留数据不含 Go 指针，GC 扫描量与条目数无关。
//   - 时间热度准入：双时间桶近似滑动窗口 + 非驻留门卫，进入阈值 Tin 之上才有资格入缓存。
//   - 收益估值：热度 × 单次回源成本（ResultMetadata.Cost）× 剩余有效期；SIEVE 有界候选集 + 精确局部替换，
//     在冻结快照上枚举全部可行子集取最小损失。
//   - 惰性退场：每次读取校验 TTL 与失效代际，过期值永不交付；退场由访问提示、淘汰扫描、慢扫描三条路径完成，
//     不为每个条目维护定时器。
//   - 受限回源合并：同 (key, 版本令牌) 最多一个真实加载（singleflight）。
//   - 分片 epoch 读者保护 + 拥有型句柄 + 安全回收；单维护者调度，读者只做 pin 加读取。
//   - 观测：ModeObserve 仅统计不缓存；ModeShadow 影子缓存按真实准入/淘汰统计虚拟命中率、不让缓存生效；
//     Heat / HotKeys 与窗口历史（回源热点 + 请求热点，经 HotWindowsSince / Hot.OnWindow 导出，
//     Hot.Label 做 key 脱敏）见 observe.go、hotwindow.go、tracker.go 与 docs/OBSERVABILITY.md。
//
// 正确性契约见各文件注释与 README.md；核心不变式由测试覆盖，清单见 docs/VALIDATION.md。
package ahkc
