# 自适应热度缓存库（AHKC）技术方案

**中文** | [English](TECH.md)

> 核心设计与完整算法伪代码
> 定位：嵌入线上服务的进程内缓存 lib。面向 Rust / C++ 的并发与生命周期约束，本文确定架构、算法、语义、伪代码及验收方法，不展开语言绑定实现或分布式能力。

## 1. 方案结论：以有界热路径承接流量，以后台收益决策管理缓存

**采用“时间热度准入 + 统一收益估计 + SIEVE 有界候选集 + 精确局部替换”的方案。** 命中只做受保护查询、硬 TTL 校验和少量有界策略更新；复杂决策、结构维护及批量回收全部移至维护面。缓存只保留达到热度门槛且值得占用资源的结果，持续变冷后主动退出。

主目标是**服务净收益最大化**：节省的真实加载成本，减去缓存查询、统计、填充和回收的全部成本。默认以 CPU 时间计量；I/O 型服务可选择后端调用成本，但一次评估必须固定单位。响应 P99、内存、加载并发和正确性是硬约束。

### 1.1 目标如何达成

| 目标要求 | 核心设计与选型结论 | 达成条件与验收证据 |
|---|---|---|
| 极低命中开销、高并发扩展 | 固定 key 分片；命中不移动链表、不回放全量事件；饱和热度跳写，收益稀疏采样 | 预热借用命中 0 堆分配；无维护、I/O 或无界策略重试；热点倾斜与多核压测通过 |
| 自动发现热点、淘汰冷对象 | 按真实时间推进的双桶门卫；驻留计数独立；进入 / 退出阈值形成滞回 | 新热点发现、阈值震荡、流量归零回放；后台具备约定扫描进展 |
| 有限容量下提高实际收益 | 统一频率、加载成本和剩余 TTL；对有限受害者集合求最小损失组合 | 局部求解与独立求解器一致；真实 trace 的收益覆盖策略 CPU 成本 |
| 线上服务可直接集成 | 异步填充与业务响应解耦；受限回源合并；失效操作有完成确认 | 填充拒绝不改变已成功加载的结果；过载、失效与迟到结果交叉测试通过 |
| 正确且资源有界 | 硬 TTL、版本隔离、安全持有；条数、驻留字节、完整保留量分别限额 | 慢读者、维护暂停、取消失效及 72h 压力中不越界、不复活旧版本 |
| 业界先进且理论可验证 | 低成本数据面结合大小 / 成本感知决策；有限动作集内精确最优 | 对照 SIEVE、S3-FIFO、W-TinyLFU-AV；报告收益、CPU、尾延迟及离线参照差距 |

**可行性结论：上述机制构成覆盖目标的完整方案，具备进入原型与验证阶段的条件。绝对延迟、吞吐和强基线优势须经实测确认，不能由架构描述直接推出。** 首版挑战目标为指定硬件下借用命中 P50 < 150ns、P99 < 1μs，以及 32 个物理核下 50M ops/s；完整测量口径见第 9 节。

### 1.2 核心决策边界

- **热度决定资格，收益决定去留。** 饱和热度不参与精细价值排名，SIEVE 标记不直接决定最终驱逐。
- **允许拒绝一次缓存填充，不允许请求线程接管后台维护。** 查询、真实加载、填充、失效分别给出结果。
- **理论承诺是冻结估值和有限候选集内的精确局部最优。** 未知未来、有限搜索及估值误差使整体在线全局最优不可直接保证；按匹配模型的最优参照验证差距。
- 首版不加入全局 LFU 排序、在线模型推理、额外 window / ghost、自动阈值调参或主动跨分片驱逐。它们须在当前闭环通过验证后，以净收益证明必要性。

**阅读方式：** 第 1 节先给结论，第 2～8 节明确架构与工程契约；[附录 A](#ahkc-pseudocode) 完整定义数据结构和算法伪代码，作为实现规范。各核心部分直接链接对应算法，调研依据与完整验证要求随后展开。

## 2. 核心架构：结构、职责与执行契约

**每个 key 固定归属一个分片；参考索引采用固定探测范围的单写多读并发哈希结构，条目地址在生命周期内稳定；分片维护者是驻留结构的唯一修改者。** 借用读取默认采用分片 epoch 保护，异步持有使用独立拥有型句柄。分片数不等于线程数，执行资源由宿主共享提供。

第 2～7 节共同构成实现契约：第 2 节定义结构与读取，第 3～4 节定义策略计算，第 5 节定义加载与版本提交，第 6～7 节定义资源和进展。以附录的完整参考路径作为实现起点；具体容器、内存布局及原子位宽可以替换，但不得改变以下顺序、状态语义和失败行为，也不能预先将整条路径宣称为 wait-free。

**数据结构与基础算法：** [A.1 结构与原语](#ahkc-a1)、[A.2 预算与索引](#ahkc-a2)、[A.3 查询与热度](#ahkc-a3)。

```text
外部逻辑请求 ──稀疏采样──► 有界样本缓冲 ──► 收益估计
      │                                          │
      ▼                                          │
分片索引 + 读者保护 + 当前时间 TTL 校验              │
      ├─ 有效命中：驻留热度 / SIEVE 标记 → 返回视图   │
      └─ miss：热度门卫 → 有界回源合并 → loader      │
                                      ├─ 返回业务结果
                                      └─ 有界填充候选
                                                 │
分片维护者：资格校验 → SIEVE 形成 Q → 精确选 S* → 资源预留 → 发布
      ├─ 控制：失效与版本栅栏、关闭
      ├─ 生命周期：时间轮过期、持续低热度摘除、安全回收
      └─ 全局维护面：空闲额度转移、进展检查、统计汇总
```

### 2.1 一次请求与一次填充

1. 外部入口确定完整 key，并以统一概率决定是否记录收益样本。一次逻辑请求只计一次；内部重试、二次查询和后台操作不增加热度或请求收益样本。
2. 受保护查询后，以当前单调时间检查可读状态、命名空间代际和硬截止时间。有效命中更新驻留热度及访问标记，返回受保护结果。
3. miss 更新非驻留门卫，再加入或发起有界加载。等待同一次加载的外部请求也各计一次热度。
4. 加载成功先按业务契约交付结果；再检查可缓存性、实际保留大小、版本、TTL 与最新热度，尝试提交填充候选。加载开始时不热、完成时已热的 key 可以取得资格。
5. 维护者重新校验候选，计算收益与替换集合；只有全部资格、净收益和资源约束满足才发布。未发布原因可观测，调用方不等待发布。

### 2.2 核心结构与必要字段

以下是逻辑字段，不要求采用特定语言类型或将所有字段放在同一缓存行。只读身份 / value、命中更新字段和维护字段应分别布局；容量全部在初始化时设上限。

| 结构 | 必须保存的状态 | 修改与生命周期 |
|---|---|---|
| Cache / Namespace | 固定分片表、C/B/M、命名空间配置与 namespaceGen、关闭状态、全局空闲额度 | 受控配置 / 控制操作修改；生命周期覆盖所有未释放对象 |
| RequestContext | 本次逻辑请求的 key / 分片、是否已采样、是否已记录 hit 或 miss 热度 | 调用作用域内临时状态，不持有 Entry；二次查询、重试及查询后加载复用它，防止重复计数 |
| Shard | 索引、热度门卫、收益 sketch、三类输入队列、SIEVE 队列及 hand、独立索引槽 coldCursor、时间轮、额度账目、fillFence、维护 owner / pending 状态 | 驻留结构和已提交账目仅维护者修改；并发预留凭证经额度池取得，其他交互限于约定原子字段和队列 |
| Entry | 完整 key、稳定 entryId、namespaceGen、不可变 value、hardDeadline、计费大小 b、可读状态；热度桶、visited、graceUntil、lowSince；策略 / timer 链接、退休标记、拥有型持有计数 | 身份 / value / 截止时间发布后不可变；请求仅更新热度、visited 和持有计数，其余归维护者 |
| HeatCell | epoch、current、previous | 请求有限次更新，维护者读取 / 推进；复合状态必须一致可读 |
| ValueStats | 双桶加权计数和样本数、统计代际、有效窗口、丢样 / 溢出标记；有限先验与成本表、模型版本 | 维护者独占写；不保存 Entry 或 value 强引用 |
| Flight | 完整 key、flightId、版本令牌、阶段、leader 结果、waiter 集合、实际 I/O 与响应交付额度、单次完成邮箱、可恢复 Work、完成 / 取消状态、各截止时间 | 独立有界并发状态机负责注册、加入和完成；维护者只处理交接及清理，不串行执行 loader |
| FillCandidate | key、结果所有权、大小、有效期基准 / hardDeadline、版本令牌、expectedEntryId、flightId、handoffDeadline、pending 预算凭证、提交阶段与完成单元 | 成功预留后入队；通过阶段转换获得唯一提交权，终态通知 flight |
| Control / Retired | 控制身份、目标及确认状态；退休对象身份、退休 epoch、保留大小、持有计数、回收状态 | 控制必须有终态；退休记录保留至安全释放且账目归还 |

entryId、flightId 不以地址代替，并在可能有旧消息存活期间保持唯一。expectedEntryId 是候选允许替换的旧条目身份；空值表示只允许首次发布。命名空间代际和分片 fence 防失效后复活，expectedEntryId 另防同代际的迟到结果覆盖较新发布。

### 2.3 输入通道与同步边界

| 通道 | 入队 / 受理条件 | 未成功时 | 成功后的责任 |
|---|---|---|---|
| 收益样本 | 固定条带缓冲，有限次入队尝试 | 放弃样本并标记质量窗口 | 按事件时间处理，不影响查询成功与否 |
| 填充候选 | 先预留 pending 槽位、字节及完成交接状态 | 放弃缓存，不改变已成功加载的结果 | 只出现一次发布 / 拒绝终态，释放或转移全部预留 |
| 失效 / 关闭控制 | 独立队列预留成功 | 返回“未受理”，不产生效果 | 保留完成责任；确认表示效果已提交，不只是已入队 |

索引只读访问、Flight 注册 / 完成和维护调度是三套独立同步域。Flight 采用按 key 分条带的短临界区状态机：只做查找、状态转移和额度归属，有限次尝试取得所有权，竞争失败返回 BUSY；不在临界区执行 loader、回调、weigher 或等待 I/O。Flight 必须可由完成路径自行结束，不能依赖填充队列有空位。

**正确性状态不可丢，策略提示可近似。** Entry 发布建立“先初始化、后可见”的同步关系；读取保护、有效状态、代际、预算归属和完成状态使用满足安全证明的同步。热度与 visited 可使用较弱顺序并有限次放弃。单维护者只消除结构写写竞争，不免除与请求读取、Flight 完成及命名空间失效的同步。

并发预留按“取得各项额度 → 全部成功才受理 → 任一失败归还已取得额度”执行；多项预留期间可以暂时少用额度，不能超卖。凭证只有一次转为已使用或归还的终态，释放额度不是可丢策略事件。驻留数量 / 字节的正式变化仍只由维护者提交。

### 2.4 命中读取的固定顺序

1. 确认实例仍受理查询，否则返回 CLOSED；从预注册上下文取得该分片的 epoch 保护，再读取索引，完整 key 比较成功后才能使用 Entry。注册 / 嵌套保护额度不足返回 BUSY，不临时无界分配。
2. 读取当前单调时间 t，随后校验 Entry 可读状态和当前 namespaceGen，并判断 `t < hardDeadline`。发布后的同一 Entry 只能由有效变为失效；重新发布必须使用新 entryId。
3. 有效命中最多进行 R 次驻留热度更新，visited 已置位时跳写，否则做有界置位；使用 t 推进热度，不再为策略重复取时钟。
4. 同步返回绑定 guard 的借用视图；需要跨线程 / await 时，在 guard 内先取得拥有型持有权，再退出 guard。不允许把长期借用跨越 await。
5. 索引确认不存在、TTL 到期或代际不符才返回 MISS。参考索引的插入和查询使用同一固定探测域，完整检查后可确认不存在；其他索引若在预算内无法完整判定，则返回 BUSY，不能伪装为确定的 miss。逻辑失效和物理清理由维护者完成。

索引必须给出完整查询、单条发布、按 entryId 条件摘除的语义，以及碰撞拒绝、完整探测后的不存在判定与无法完成判定时的 BUSY 行为。首版预留索引最大容量，不做在线扩容或重分片；具体布局由原型比较确定，不能改变这些契约。

## 3. 热度准入与冷却：按时间识别热点，不以全实例 QPS 定义冷热

**算法伪代码：** [A.3 双桶与门卫](#ahkc-a3)、[A.7 主动冷却](#ahkc-a7)。

**用双时间桶近似最近 Δ 时间内的请求量；进入阈值高于退出阈值，计数只需区分阈值附近。** 这使冷却时间不随实例总流量变化，也避免对极热 key 持续执行计数写操作。

### 3.1 统一计数规则

计数状态为 `epoch、current、previous`。设当前桶已过去比例为 φ：

```text
H(t) = current + (1 − φ) × previous
T_in > T_out；H_cap > T_in
```

跨过一个桶时将 current 移入 previous 并清零 current；跨过至少两个桶则全部清零。按访问或扫描惰性推进，不在桶边界重置全表。桶内计数达到 H_cap 后跳过写入，直到换桶。

一次计数更新遵循同一转移：读取一致快照 → 按 `e=floor(t/Δ)` 归一化桶 → `current=min(current+1,H_cap)` → 尝试提交新快照，失败最多重试 R 次。若快照已推进到 e 之后，说明当前请求的时间已落后，放弃本次统计，禁止回退 epoch。桶推进与加一必须作为一致状态提交，不能先公开新 epoch 再逐字段清零旧计数。读取热度只需在快照副本上归一化，不必为了查询强制写回。

这是近似滑动窗口。桶内流量分布、计数饱和和竞争中放弃更新都会影响误差，不能将 H(t) 解释成精确请求数。时间与代际字段必须定义溢出处理，禁止回绕后将旧状态当作新状态。

### 3.2 非驻留门卫与驻留热度

| 部分 | 确定行为 | 初始参数 |
|---|---|---|
| 非驻留门卫 | 每分片固定 d×w 单元，完整 key 映射至 d 行；查询取各行 H 的最小值；仅 miss 更新 | d=4；每单元最多 R=2 次原子更新尝试，共至多 8 次 |
| 驻留热度 | 每条目独立双桶计数，命中时更新；达到上限跳写 | 每次最多 R=2 次更新尝试 |
| 阈值 | H≥T_in 才能新准入；持续 H<T_out 才冷却 | Δ=1s；T_out=0.5T_in；H_cap=4×实例最大 T_in |
| 驻留宽限 | 新发布条目从零计数，先完整观察，避免刚进入即退出 | grace≥2Δ；首版新 Entry 从零观察，主动刷新不启用 |

门卫宽度 w 根据**窗口内非驻留基数和流量**确定，不能仅由缓存条数决定。哈希碰撞可能高估，更新丢失或饱和可能低估；CMS 原始误差保证不能原样用于这个修改后的统计量。最终以精确窗口回放确定宽度和阈值。[Count-Min Sketch](https://dimacs.rutgers.edu/~graham/pubs/papers/cm-full.pdf)

### 3.3 主动冷却的状态机

维护者按公平扫描顺序执行：

```text
已过期 → 逻辑失效并安排摘除
仍在 grace → 本轮跳过
H ≥ T_out → 清除 low_since
H < T_out 且 low_since 为空 → 记录当前时间
H < T_out 且已持续 coldFor → 重读状态与热度，仍低则摘除
```

冷却使用固定索引槽 coldCursor，不依赖只有填充时才前进的 SIEVE hand；定时触发按槽位配额覆盖整个索引，插入和删除不改变游标。新条目受 grace 保护；无法读取有效热度时清除 lowSince，重新观察。最终重读后到逻辑摘除之间发生的命中可与冷却操作重叠，不承诺逐请求精确的连续低热度检测。

`coldFor≥Δ` 为起点。“持续低热度”定义为连续维护观察满足条件，不声称追踪了每一个瞬时峰值。高低阈值之间保留当前驻留状态，减少阈值附近反复进出。

设完整扫描周期不超过 P、额外调度滞后不超过 L，条目已结束 grace 后停止访问，则逻辑冷却延迟预算为 `2Δ + coldFor + 2P + L`。首次发布场景另计剩余 grace；物理释放还需等待读者退出。宿主不提供定时维护时，读取仍满足 TTL，但不能承诺空闲期间按时摘除。

例如 Δ=1s、T_in=20、T_out=10、coldFor=1s：约 20 次窗口访问取得准入资格；驻留后降至 10 以下并持续观察 1s 才冷却。过期或完全退出后默认重新积累门卫热度；短 TTL 场景必须联测发现延迟和回源成本。

## 4. 收益与替换：统一估值，精确求解有限动作集

**算法伪代码：** [A.4 收益估计](#ahkc-a4)、[A.5 精确选择与提交](#ahkc-a5)。

**对每次填充，先估算候选在剩余有效期内的净价值，再寻找释放所需容量的最小损失集合。** 热度只做资格判断；大对象必须与为它腾空间的全部对象比较。

### 4.1 估值输入及不足时的行为

命中、miss 和加载等待者均在外部入口独立采样，使用相同口径。样本含 key 策略指纹、命名空间代际、事件时间和当次概率 p，不携带条目或 value 的强引用。维护者使用固定预算统计结构，按事件时间汇总和老化；策略指纹碰撞只影响策略，索引仍比较完整 key。

独立抽样且样本无传输丢失时，窗口请求数估计 `N̂=Σ样本 1/p_j` 满足期望为真实请求数；后续结构碰撞、样本丢失和未来预测不自动继承无偏性。禁止按固定 key 哈希决定永久采样与否；改变 p 时保留每条样本原概率。

首版收益统计使用独立于热度门卫的固定宽度频率 sketch，由维护者独占写入。每行保存观测窗 Δ_v 的双桶加权计数和样本次数：样本按事件时间加入对应桶，加权增量为 1/p_j；查询按第 3 节双桶公式加权并取行最小值，再除以 Δ_v 得到请求率估计。计数不做阈值饱和；暖机不足、迟到至窗口之外或数值溢出均暴露为质量状态。行数从 4 起测，宽度按采样后的窗口基数校准，碰撞误差进入估值回放。

| 估值输入 | 取得方式与落地规则 |
|---|---|
| 请求率 λ̂ | 按上述 sketch 查询；统计重建后重新暖机，覆盖完整双桶观察期后才作为独立估值使用 |
| 估值质量 | 记录近似样本量、窗口年龄及丢弃状态；初始最低样本量 n_min=8，仅作可调工程门槛，不等于统计置信保证 |
| 低样本估值 | 默认使用已校准的有限命名空间先验 λ₀、c₀，候选与驻留一视同仁；未提供或校准过期时，延后依赖该估值的新填充 |
| 单次可避免成本 c | 在真实 loader 慢路径测量，并按命名空间 / 有限成本类别老化；不为获取样本额外回源 |
| 有效回源率 r̂ | 校准 trace 按“无缓存、保留 singleflight”重放，忙期间到达为 waiter；生成命名空间内按请求率、加载时长分桶的有界查表模型，在线维护查表得到估计 |
| 估值降级 | 任意已知样本丢弃或数值溢出污染对应分片 / 窗口；该窗口不用于收益填充，恢复完整健康双桶后重新启用。模型过期单独禁止使用对应模型 |

先验和回源模型作为命名空间配置由校准 trace 产生，记录目标负载、版本与有效期；负载超出校准范围时标记模型失效，不能临时把“零样本”当零热度。模型切换需要记录模式和年龄，防止新旧口径混比。样本丢弃可能与热点相关，不能简单除以平均保留率纠正。

每次估值只输出三种状态：健康完整窗口且样本量达标为 MEASURED；健康但暖机 / 样本不足、且先验有效为 PRIOR；其余为 UNAVAILABLE。查表成本模型必须同时有效；UNAVAILABLE 不允许发起依赖该值的填充。已知丢样不能以 PRIOR 掩盖。每次决策固定统计时间与不可变模型版本，不将上次计算的 G 原样沿用到新的剩余 TTL。另记录样本已消费前缀和最大滞后；滞后超过 maxSampleLag 时不可用于收益填充。MEASURED 不等于全部并发请求已经入账。

样本在维护者当前桶或前一桶内时写入对应桶，过旧则丢弃并标记；不按出队时间当成新请求。成本表采用固定命名空间 / 有限大小类别：加载 CPU 与时长在完成路径取得，h、k、K 在原型基准中校准为有界查表参数，均有版本和最大年龄。首版不在命中路径估算成本或训练模型。

p=1/1024 时，50M 请求/s 约产生 4.88 万样本/s，仅表示成本量级；单 key 每窗的预期样本量约为 pλΔ_v，未必达到 n_min。原型必须验证先验占比、发现延迟与估值质量，再在固定缓冲和 CPU 预算内选择 p；不能同时假定极低采样成本与所有 key 的高精度估值。

### 4.2 决策目标

所有对象在同一决策时刻 t、同一预测上限 W、同一成本单位下估值：

```text
τ_i = hardDeadline_i − t
G_i = ∫[0, min(W, τ_i)] (r̂_i(u) × ĉ_i(u) − ĥ_i(u)) du
L_v = G_v + k_v
```

G_i 为保留对象预计获得的未来净收益；r̂ 是**可避免的真实加载率**，h 为预计服务该对象的持续成本，k_v 为移除成本，K_a 为候选尚未支付的安装成本。已经完成的 loader 是沉没成本，不从候选未来收益中重复扣减。查询及策略的公共成本另计入服务总账。

模型输入 r̂、c、h、k、K 必须是同口径的有限非负值，G 可以为负；τ≤0 的对象先按过期处理，不计算负时间积分。未完成或失效的估值不得默认填 0。

初版可在短期平稳假设下取 r̂、c、h 为常量，得到 `G_i=(r̂_i×ĉ_i−ĥ_i)×min(W,τ_i)`。不得将有合并加载的每个请求都乘以一次完整 loader 成本；若改用请求命中数作为目标，明确统一为 `G_i=λ̂_i×min(W,τ_i)`，并独立评估服务 CPU。

### 4.3 形成候选集与计算容量缺口

SIEVE 只提供低开销搜索顺序：新条目加入队列头部；命中仅尝试置访问位；扫描手从尾向头推进、到头回绕。遇到已置位条目则清位并跳过，否则可纳入 Q。命中不调整链表。[SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo)

每次填充总扫描上限 `B_scan=64`，候选集上限 `m=8`；这是**整次填充**的预算。先按生命周期配额处理已过期或已确认冷却条目，再重算容量，不能借此无限清表。每次最多访问起始队列中的一圈条目；Q 内 entryId 唯一，排除待替换的同 key 旧条目。扫描结束保留手的位置；预算内无可行集合则拒绝本次填充，不转为全表搜索。

扫描中遇到失效 / 到期条目，仅在生命周期及退休预算允许时清理，否则跳过并仍计驻留占用；不把它作为有未来收益的 Q 成员。visited 是提示：扫描取位并清零与命中置位允许竞争，Q 冻结后新命中不追溯撤销本次计划。候选形成结束后再冻结 N_s、Bytes_s 与估值时间，避免边清理边沿用旧缺口。

冻结候选及 Q 的身份、版本、大小、估值、TTL 和资源账目。令 a 为新条目，O 为必须替换的同 key 旧条目集合，O 至多一个元素且不得出现在 Q；首次发布 O 为空。对分片条数额度 C_s、字节额度 B_s：

```text
ΔN = 1 − |O|；ΔBytes = b_a − Σ[O] b_o
r_n = max(0, N_s + ΔN − C_s)
r_b = max(0, Bytes_s + ΔBytes − B_s)
F(Q) = {S⊆Q：|S|≥r_n，Σ[S] b_v≥r_b，组合资源检查通过}
```

“组合资源检查”对整个 `O∪S` 同时核算退休字节、退休记录 / 回收交接槽位、删除工作配额、时间轮可调度范围和索引安装位置。默认保守假定全部移除对象都需退休：`retiredUsed+retiredReserved+Σ[O∪S] b_v≤retiredLimit`，并检查最终驻留量与其余分类预算。不能预支未来 guard 退出或析构带来的空间；逐个对象可删除，不代表同时删除这一组也可行。b 的可加计费口径见第 6 节。

### 4.4 精确选择与提交

```text
S* = argmin[S∈F(Q)] Σ[S] L_v
ΔG* = G_a − K_a − Σ[O] L_o − Σ[S*] L_v
发布条件：资格仍成立、资源仍可用、ΔG* > 0
```

| 情形 | 精确求解方法 |
|---|---|
| 无容量缺口、空集合资源可行，且所有 L_v≥0 | S*=空集 |
| 受害者等大小 b、L_v≥0，其他资源约束不区分组合 | 取损失最小的 k 个，k=max(r_n, ceil(r_b/b))；不足 k 个则不可行 |
| 一般情况 | 枚举实际 Q 的全部子集，m≤8 时最多 256 个；按完整约束筛选后取最小损失 |

若所需对象估值不可用、F(Q) 为空或求解未完成，拒绝候选；不能将中途找到的集合标注为精确最优。并列解依次优先更少退休字节、更少删除条目，再按稳定身份打破平局。默认收益门槛为 0；额外安全余量会改变允许动作集，须单独评估。

**例：** 新对象大小 6、收益 14、安装成本 1；Q 中有 `(大小6,损失13)`、`(3,4)`、`(3,5)`。需要释放 6 时，选择后两项，净增益为 `14−1−4−5=4`；只取第一个的净增益为 0，应拒绝该替换。

候选提交阶段为 `QUEUED→COMMITTING→终态`。过期 / 取消方只能将 QUEUED 改成终态；维护者只有成功取得 COMMITTING 才能修改驻留集合，之后由它唯一给出终态。取消标记不代表 payload 已释放，预算待实际持有责任结束再归还。

提交顺序固定如下，第一个不可逆动作之前必须完成所有可能失败的准备：

1. **准备：** 预留目标驻留、退休、索引安装和回收交接资源；完成新 Entry 初始化。普通分配失败、预算不足或无可保证的索引安装位置在此拒绝，原驻留集合保持可读。驻留 / 索引预留可依赖本次锁定移除完成后的净空位，不能把这些额度提前归还给其他计划；物理内存预留不能依赖未来回收。
2. **复核并取得提交权：** 检查候选令牌、expectedEntryId、hardDeadline、handoffDeadline，以及 O/S* 的身份、计量和状态；确认未超过交接期限后，尝试 QUEUED→COMMITTING，失败则归还准备资源。热度 / visited 的并发变化允许按已冻结估值决策；身份或资源变化则拒绝，不无限重算。
3. **移除：** 将 O/S* 标为不可读，按 entryId 从索引、策略队列和时间轮摘除，登记退休 epoch；将驻留计费转移至已预留的退休账目。
4. **发布：** 完成 pending→resident 账目转移，链接新条目的策略 / timer 元数据，再使已完整初始化的新 Entry 对索引读取可见。相对于同分片失效串行提交；相对于命名空间代际变化按第 5 节处理。
5. **终结：** 释放多余预留，写入一次发布 / 拒绝结果，解除 flight 的交接保留；物理回收另行推进。

第 3 步之后不执行用户回调或可能常规失败的分配。索引必须提供预留后可完成的写入保证；不要求多 key 替换对读者整体原子可见。若命名空间恰好失效或硬 TTL 在提交间隙过去，旧代际 / 已过期结果不能通过读取检查；已完成驱逐仍有效，按“提交失效”终结并受预算约束清理，局部收益承诺不覆盖这种外部状态中断。

同 key 替换由 O 统一进入容量、收益与退休计算；已过期 / 已逻辑失效旧条目的未来 G 取 0，但仍计移除和保留成本。初始加载产生的候选只有 expectedEntryId 仍匹配才可安装，不自动覆盖期间发布的新版本；主动刷新默认关闭。

**同 key 与内存账目示例：** 分片 C_s=3、B_s=12，驻留 A旧=2、B=6、C=4；A旧已过期但仍在索引。新 A 大小 6，身份条件匹配，故 O={A旧}，r_n=0、r_b=4。若最优 S*={C} 且净收益为正，提交后驻留为新 A 与 B，共 2 条、12 字节；A旧与 C 另占 6 字节退休预算。新 A 原已计入 pending，提交只转移其账目。退休余量不足 6 时必须在移除前拒绝，不能只看到最终驻留未超 12 就认为可提交。此例的“字节”是缩小后的计费单位。

### 4.5 最优性结论及其验证边界

对冻结后的 Q 和估值，候选收益为常量，因此最小化受害者总损失等价于最大化本次净收益；完整枚举覆盖全部可行子集，再与“不操作、收益 0”比较，得到**该动作集内精确最优解**。快速路径必须与通用解一致。

该结论不覆盖未进入 Q 的驻留对象，也不消除预测误差。理论参照中，等大小静态选择是 Top-K；变大小选择含 0/1 背包问题，按“收益 / 字节”贪心不能保证整数最优。例如容量 10，对象 `(6,12)、(5,9)、(5,9)`，密度贪心得 12，最优为 18。以整数模型求小规模最优、以线性松弛给静态上界；动态策略另用匹配约束的离线模型，见第 9 节。

集合比较借鉴 W-TinyLFU-AV 的大小感知准入思想；本方案的有界子集求解是额外设计，其收益和成本需自行验证。[W-TinyLFU-AV](https://arxiv.org/abs/2105.08770)、[CacheSack](https://www.usenix.org/conference/atc22/presentation/yang-tzu-wei)

## 5. 线上语义：硬 TTL、受限加载、失效不复活

**算法伪代码：** [A.6 加载与响应](#ahkc-a6)、[A.7 失效与时间轮](#ahkc-a7)、[A.9 关闭](#ahkc-a9)。

**查询正确性独立于后台进度；成功加载的业务响应独立于缓存准入；失效完成必须形成明确版本边界。**

### 5.1 TTL 与条目生命周期

```text
候选 → 已发布可读 → 逻辑失效 → 已摘链 → 退休待回收 → 已释放
```

命中以受保护读取后的**当前单调时间**判定：`now < hardDeadline` 且条目有效、命名空间代际匹配。不能用维护线程定期刷新的旧时钟判断。参考实现从不晚于 loader 开始的 Flight 注册时刻保守计时，业务提供更早截止时间时取更早值；排队和发布不延长有效期。可选抖动只缩短 TTL，默认关闭。

TTL 保证落在查询校验的线性化时点；之后线程被暂停或调用方继续持有视图，不会撤销已取得的对象。显式失效也不回收正在合法持有的内存。

采用有界层级时间轮安排后台过期，配置 tick、槽数和最大覆盖范围；每次处理精确复核截止时间和条目版本。队列标记与时间轮同时存在时分别计入元数据。暂停后追赶按预算分批处理。无论时间轮是否及时运行，读取都不能命中过期对象。

首版每层固定 K 个槽，层 ℓ 的步长为 `tick×K^ℓ`。将截止时间向上取整为基础 tick 编号 T，按剩余 tick 数选择能覆盖它的最低层，槽位为 `floor(T/K^ℓ) mod K`；总层数必须覆盖配置的最大 TTL。基础层回绕时推进高层并降层重挂。处理条目先比较真实 hardDeadline：已到期则逻辑失效，尚未到期则降层或重挂至未来处理点，禁止提前过期或挂回已经处理完的槽。每条目仅有一个有效 timer 成员；摘链同时撤销它，取消 / 级联仍校验 entryId。长暂停按槽、条目及级联配额保存游标追赶，其余维护类别保有各自配额。

### 5.2 回源合并与填充交接

**同一 `(命名空间, 完整key, namespaceGen, fillFence)` 最多一个实际活动 leader；版本变化后新旧加载可以并行，但全部占真实 I/O 额度。** 既有等待者的业务响应交付与是否缓存分开终结。[singleflight 语义参照](https://pkg.go.dev/golang.org/x/sync/singleflight)

| Flight 状态 | 允许的动作 | 离开状态时的责任 |
|---|---|---|
| LOADING | 在同版本及 waiter 预算内加入；leader 在锁外加载 | 真正 I/O 结束才释放 I/O 额度；成功结果先进入已预留的响应交付责任 |
| COMPLETING | 源操作已完成，先交付已有等待者，再在 gate 外准备缓存候选；新 join 短暂 BUSY | 完成阶段保存在固定 Work 槽；暂停后续行，不能重复消费结果或创建候选 |
| TIMED_OUT_IO_RUNNING | 已等待请求终结为超时；同身份新请求返回 BUSY，不再创建 leader | 底层真正结束后清理孤儿记录，结果不再填充 |
| COMPLETED_PENDING | 候选已通过初筛、交接预留并入队；当前令牌匹配、未达 hardDeadline / handoffDeadline 且 waiter 预算允许时才可复用 | 发布、拒绝、失效或交接超时后停止新加入；不因 waiter 增加延长有效期 |
| TERMINAL | 不再加入；已登记响应分别交付 / 取消 | 按各项实际持有责任恰好一次归还额度；迟到回调按 flightId 忽略，记录仍被持有时继续计量 |

首次 miss 的固定步骤为：

1. 更新一次门卫；读取 namespaceGen 与 fillFence，受保护观察当前条目身份，复核令牌未变化，得到 expectedEntryId。有限次无法形成稳定观察则返回 BUSY，不循环等失效停止。
2. 在 Flight 短临界区查找同身份记录；可加入则预留 waiter 槽位。创建 leader 前先取得活动记录、真实 I/O、最大结果响应交付和首个 waiter 的额度；失败全部归还，不调用 loader。
3. leader 注册后再次检查缓存与令牌。已有效命中则在 guard 内取得拥有型持有权，退出 guard 后通知等待者并释放未使用的加载额度；令牌过期或索引返回 BUSY 则终结为可重试状态并归还未使用额度。只有确认 miss 才退出所有读者保护和临界区，调用 loader。
4. 真正加载结束后释放 I/O 额度，并按响应契约将成功结果交付已登记等待者。与此同时检查可缓存性、大小、截止时间和最新门卫；pending 槽位 / 字节预留成功才产生唯一候选。
5. 候选期限固定为 `min(hardDeadline, loadCompletedAt+handoffTimeout)`。Flight 通过 Delivery 保留响应，candidate 独立持有待填充 Entry，底层不可变结果可共享但保留责任分别记账；发布或拒绝通过预留的完成状态通知 Flight，不依赖另一个可能满的普通队列。

handoffDeadline 约束取得 COMMITTING 提交权的校验时点；已开始提交者由维护者完成，Flight 超时不能与其同时发布互相矛盾的填充终态。到交接期限仍未发布时停止新请求复用 completed_pending，但不提前释放提交者仍持有的结果；若已在 COMMITTING，同身份新请求返回 BUSY，直到本次提交终结，不再创建竞争 leader。提交耗时单独受维护预算约束，不能将调度暂停伪装为已经完成；hardDeadline 始终由每次读取独立保证。

响应交付预留与 pending 预留分别存在：前者在 loader 开始前按 maxResultBytes 保证有界交付能力，后者允许因缓存压力失败。慢消费者仍未接收的结果继续计入响应预算，不能在发出通知时就视为已交付。宿主必须限制 loader 最大输出及临时缓冲；违反结果上限属于加载契约错误，不承诺为任意大结果保留内存。

等待者取消仅解除自己的等待。请求取消或 loader 超时不等于底层 I/O 结束；缓存加载截止后仍占 I/O 额度并进入 TIMED_OUT_IO_RUNNING，直到真实完成。响应和 waiter 的取消 / 完成竞争必须只结算一次。首版不自动缓存错误、不做过期值兜底、不启用提前刷新。

### 5.3 失效顺序与关闭

普通填充可丢弃，失效走独立有界控制路径：未受理立即返回明确状态；已受理必须最终完成或返回失败，不能静默丢失。成功确认表示**逻辑失效已完成**，不表示物理内存已释放。

| 操作 | 提交规则与对外保证 |
|---|---|
| 单 key 失效 | 分片维护者递增 fillFence，再使目标条目不可读，随后确认；旧 fence 加载结果不得恢复为可读版本 |
| 命名空间失效 | 推进 namespaceGen；读取检查代际，旧代际立即逻辑隐藏，后台分批摘除 |
| 迟到填充 / timer | 同时校验身份、代际及版本；旧 timer 不能删除新条目，地址复用不能作为身份判据 |
| 关闭 | 停止受理新工作，结束已受理控制和等待者；分别报告停止完成与安全回收完成，超时不能强制释放仍被持有的对象 |

填充发布与失效必须具有一致的提交顺序；不能只在很早的时刻检查一次版本后无条件发布。失效确认后开始的查询不得取得旧可读版本；与失效重叠的查询允许按线性化顺序返回。失效不默认撤销早先已发起业务加载的响应，但旧结果不能重新进入可读缓存。

**两类版本各自生效：** 单 key 失效与该分片填充在同一维护者内排序；成功确认在 fence 推进和目标逻辑失效之后。命名空间失效的线性化点是受控地发布新 namespaceGen，不等待所有分片扫描。若它与填充并发，旧代际 Entry 即使已安装也不能通过读取代际检查；维护者发现这种情况即将候选终结为 STALE，并安排有预算的清理。

候选还必须通过条件安装：expectedEntryId 为空只允许当前 key 仍不存在；非空只允许该旧身份仍在索引。旧身份已被正常摘除也保守拒绝，下一次 miss 重新获取条件，不保存无限历史 key 墓碑来放宽规则。由此产生的额外填充拒绝需在短 TTL / 慢加载回放中单列。

例：候选 A 观察到旧 entryId=7，随后同 key 已发布 entryId=8；即使 namespaceGen、fillFence 相同，A 也因 expectedEntryId 不匹配被拒绝。单 key 失效后只比较 entryId 仍不足，必须同时拒绝旧 fence；两项检查不能互相替代。

fillFence 避免为无限历史 key 保存 tombstone，代价是同分片其他 key 的旧在途填充也会被保守拒绝，其已驻留值不受影响。高频失效场景必须量化此项损失。命名空间总数、代际及关闭状态也必须有界并处理溢出。

## 6. 容量与原生内存：限制完整保留量，慢读者只影响准入

**算法伪代码：** [A.2 预算凭证](#ahkc-a2)、[A.8 安全回收](#ahkc-a8)、[A.9 额度转移](#ahkc-a9)。

**条数 / 驻留计费字节决定可容纳内容，完整受管保留量决定库的资源安全；两者分别计量。** value 默认不可变，借用视图用于低开销同步访问，拥有型句柄用于跨线程或异步持有。

每个 Entry 的 b 包括 key、value 的保留容量与条目元数据，必须可相加；跨 Entry 共享分配默认按各条目保守计费。M 则汇总所有受管保留责任，允许保守重复计费但禁止漏计。若支持有稳定分配身份的物理去重，只作用于 M，不能直接改变第 4 节以 Σb 计算的驻留容量约束。

### 6.1 三层额度与资源账目

全局条数 C、驻留字节 B 以额度授予分片，始终满足：

```text
Σ C_s + C_free + C_inTransit = C
Σ B_s + B_free + B_inTransit = B
N_s ≤ C_s；Bytes_s ≤ B_s
```

每个分片另有初始化固定的索引上限 `C_s^max`，要求 `C_s≤C_s^max`，索引槽位及最大负载率据此预留。额度转移不能突破接收分片的物理上限；满分片仍可通过本地可行替换腾出位置，否则拒绝候选并暴露容量碎片。首版不通过请求触发扩容解决它。

额度仅在维护面流转，在途额度由有界 grant token 保存，所有权只转移一次。首版转移超过实际占用及已承诺提交量的**空闲额度**：捐出方先扣减，全局池接收，再授予接收方；不临时双花，不驱逐其他分片热点来满足本次填充。无额度可拒绝当前候选，请求不等待跨分片调配。

| 保留类别 | 必须计入的内容 | 耗尽行为 |
|---|---|---|
| 固定 M_fixed | 最大索引槽位、门卫、收益统计和样本缓冲、时间轮、求解 scratch、读者槽位、指标条带 | 初始化校验预算；首版在线不扩容 |
| 驻留 M_resident | 索引中仍由缓存保留的 key、value、条目元数据；已逻辑失效但未摘除者仍计量 | 拒绝新增或执行有预算的替换 |
| 交接 M_pending | 完成结果、待填充候选及其实际保留对象 | 拒绝新交接，业务结果照常交付 |
| 退休 M_retired | 摘链但仍被读者持有的对象、旧索引桶及待释放批次 | 停止增加退休量及依赖它的新填充 |
| 响应 M_response | loader 完成后仍由库持有、等待已登记消费者接收的结果；开始加载前按 maxResultBytes 预留 | 拒绝发起新 loader；已预留响应不因填充拒绝而丢失 |
| 加载与控制 M_aux | 活动 / 孤儿加载记录、等待者、已受理控制、回收交接记录 | 按各自数量与字节上限拒绝新受理 |

要求 `M_fixed + M_resident + M_pending + M_retired + M_response + M_aux ≤ M`，并同时限制已使用量与未兑现预留。每项保留责任有唯一账目归属；同一个候选在 pending→resident→retired 转移时不能漏计。独立的响应与缓存持有责任可保守分别计费。槽位限制和字节限制同时存在，额度归还只执行一次。

新候选从 pending 转为 resident 不代表新分配了一份 value；旧条目进入 retired 也不代表物理空间已释放。因此提交期间的额外分配必须另外预留，不能用“驱逐后会空出来”抵扣尚未释放的内存。Flight 从表中摘除同样不等于记录和响应已经释放，仍被回调 / 消费者持有的保留量继续记账。

退休预算已满时，TTL / 失效仍可先逻辑隐藏条目并继续计入原占用，待有预算再摘链；不能为了确认失效提前释放，也不能以“不可读”为由不再记账。

### 6.2 持有、回收与字节口径

借用必须绑定读者保护作用域，禁止返回生命周期无约束的裸引用。拥有型句柄可延长 value 生命周期，其引用计数及单热点竞争单独测量，不与借用路径混报。

首版选择**每分片独立 epoch 回收域**：读者在访问索引前进入受限保护槽，读取期间保持保护，借用结束后退出；维护者先阻止新读者取得已摘链对象，记录退休 epoch，再等待该回收协议确认所有可能观察旧指针的保护期结束。需要跨 await 的对象先转换为拥有型持有，不让 epoch 长期覆盖异步等待。epoch 的保护机制可参考 [Crossbeam Epoch](https://docs.rs/crossbeam-epoch/latest/crossbeam_epoch/)，但不能直接假定某个库默认的收集时机符合本方案。

释放 Entry 需要同时满足：已从索引及所有维护结构摘除、epoch 安全条件满足、拥有型持有计数为零。单独看到“当前没有活跃读者”不足以替代完整 epoch 协议。具体实现必须证明保护发布、指针读取、摘链和安全 epoch 推进之间的同步顺序。

回收由维护面分批执行；guard 退出和拥有型句柄释放只完成有界状态更新，不承担批量扫描或任意析构。退休记录自带可重访的回收状态，最后一个句柄释放无需向可能已满的普通队列投递关键消息。慢读者导致预算不足时暂停新增保留，不提前释放；独立分片回收域使局部慢读者不直接阻止所有分片的 epoch 推进。

字节预算覆盖**实际保留分配的安全上界**：缓冲区按 capacity，包含 key 与元数据，不用 value 的逻辑长度代替。任意泛型对象若仅提供业务 weight，只能承诺 weight 上限。结果正式移交后由宿主持有的普通业务副本、loader 临时缓冲、分配器碎片和 RSS 不等同于库预算；宿主需限制这些资源。缓存拥有型句柄仍由库跟踪，不能按普通业务副本提前移出账目。

## 7. 工作量与过载：每类维护有配额，请求不承担积压

**算法伪代码：** [A.9 维护与调度](#ahkc-a9)。

**单维护者保证结构更新有序，分类配额保证相互不饿死；有限队列之外，还要限制每轮实际工作量。**

### 7.1 调度契约

每轮为控制操作、过期 / 冷却、样本汇总、填充求解、扫描和回收分别配置非零进展配额，并限制事件数、扫描步数、子集检查数、删除对象数及释放字节。软时间预算作为补充，不能仅靠“最多处理 K 个事件”约束长尾。

每轮先服务有限控制配额，再服务到期 / 冷却、样本、填充和回收各自配额；到软时间预算时从未完成类别续行，而非每次都从控制开始。各类别内部采用保持游标的 FIFO / 轮转顺序。一个待填充候选不能因资源不足长期挡在队头：本轮给出拒绝终态，后续请求重新竞争。

宿主提供执行器与定时触发，维护 owner 状态为 `IDLE→QUEUED→RUNNING`：生产者先发布输入和 pending，再尝试将 IDLE 改为 QUEUED，成功者提交任务；正在 QUEUED / RUNNING 时只记录待处理工作。RUNNING 完成配额后，有待续工作则安排下一轮；退出至 IDLE 后必须重查输入 / pending 并尝试重新取得调度权，以覆盖生产者与退出交叉。移交 owner 后旧任务不再写驻留结构。

调度被拒绝时回到可重试状态，保留 pending、暂停新填充，由定时触发有界重试，禁止递归重试或 caller-runs。定时触发不依赖新请求，负责空闲时的 TTL、冷却、Flight 期限与回收进展。执行器停摆时这些完成时限不可保证，但 TTL 读取、预算和禁止提前释放的约束仍成立。

loader、任意用户回调、weigher 和不可控析构不在结构维护临界区执行。首版初始化预留最大索引及固定结构；将来允许扩容必须另行核算新旧索引重叠，不能沿用首版固定预算结论。

### 7.2 有界不等于便宜

| 路径 | 算法工作上限与主要成本 |
|---|---|
| 有效命中 | 一次受保护查询、当前时间校验；驻留计数至多 R 次尝试；访问位置位及一次采样判断；不移动链表 |
| miss 策略 | dR 次计数尝试；有界 flight 注册 / 加入；资源不足显式返回 |
| 一次填充 | 总扫描≤B_scan；最多 2^m 个子集，直接计算上界 O(m·2^m)；优先走等价精确快速路径 |
| 维护轮次 | 按类别独立限制处理量、扫描量、退休 / 释放字节；保留进度继续下一轮 |

50M 请求/s、5% miss 若全触发 256 子集检查，仅检查次数即达 6.4 亿/s。因此必须设置每分片填充条数 / 字节速率和求解预算，测量快速路径占比与每次真实成本。预算过小会降低命中收益，不能只以 Get 吞吐评价设计。

维护能力按 `样本率×处理成本 + 填充率×决策成本 + 索引槽数/P×扫描成本 + 过期及回收成本` 配置 CPU。实际队列年龄、扫描周期或退休积压持续超标，说明维护能力不足，需要降低准入速率或增加宿主预算。

### 7.3 确定的过载行为

| 压力 / 故障 | 必须行为 |
|---|---|
| 填充队列、额度、求解或退休预算不足 | 拒绝缓存候选；成功 loader 结果正常交付 |
| loader 真实并发、活动 key 或 waiter 满 | 拒绝新加载 / 等待并返回可区分的过载结果；不绕过额度偷偷调用 loader |
| 控制路径满 | 拒绝受理新控制；已受理操作保留完成责任 |
| 样本丢弃、统计重建或估值失效 | 按第 4 节进入 PRIOR 或 UNAVAILABLE；污染窗口禁止收益填充，驻留 TTL 与热度维护继续 |
| 同时过期、维护暂停后恢复 | 读取严格 miss；后台分批处理，真实加载仍受上限保护 |
| 慢读者或 loader 忽略取消 | 保留对象 / I/O 持续计量，限制新增；不提前释放、不无限重启 |

## 8. 接入与配置：一个完整默认算法，参数均关联预算和验证

**宿主负责业务正确性和执行资源，lib 负责缓存复用及其生命周期。** 接入前必须确定以下契约，之后才能将方案落为实现任务。

### 8.1 对外操作的语义

| 操作 | 结果与持有方式 | 明确边界 |
|---|---|---|
| 只读查询 | HIT + 借用视图，或 MISS / BUSY / CLOSED；拥有型查询显式选择 | 不发起加载；BUSY 不是确定不存在 |
| 查询并加载 | 已有值或 loader 结果以可异步交付的拥有型结果返回；加载错误与过载分别报告 | 不跨 await 保留 epoch guard；响应成功不代表已缓存 |
| 条件填充 | 本方案默认由 Flight 生成，终态为 PUBLISHED / REJECTED / STALE / EXPIRED | 非对外无条件 Put；必须携带版本、身份条件及预算凭证 |
| key / 命名空间失效 | 未受理，或已受理后的完成凭证 | 成功确认逻辑效果，不等待所有读者释放 |
| 维护 / 关闭 | 有界推进；关闭分别确认停止受理、工作终结、安全回收 | 尚有 I/O / 持有者时不伪造完成，不无期限占用调用线程 |

这些是行为契约，不规定函数签名、同步框架或 ABI。查询后再加载必须传递同一个 RequestContext，或由组合入口内部完成；两个互不关联的调用不能由 lib 自动推断属于同一业务请求。

| 必须明确的契约 | 内容 |
|---|---|
| 完整 key | 包含影响结果的业务维度、权限 / 租户隔离和数据版本；限定 key 大小及哈希 / 比较成本；策略哈希不替代完整相等比较 |
| 结果 | 不可变性、可缓存条件、实际保留大小、最大结果大小、借用 / 拥有型返回方式 |
| 有效期与失效 | TTL 基准、最大 TTL、单 key / 命名空间失效确认语义 |
| 加载 | 同步 / 异步方式、超时与取消能力、真实并发、waiter 限制及过载处理 |
| 维护 | 可用 CPU、触发周期、最大可接受调度滞后与关闭期限 |
| 估值 | 固定主目标和 W；经校准的先验、成本模型及有效期；无可用估值时延后填充 |

### 8.2 配置与初始化约束

建议起始参数如下，均须通过第 9 节验证后固化：

| 参数组 | 首版起点 / 必填项 |
|---|---|
| 容量与安全 | C、B、M、maxEntryBytes、maxResultBytes 必填；命名空间数、读者 / 嵌套 guard 槽位、响应及所有队列 / 等待 / 退休预算显式限额 |
| 分片 | 8 或 16 起测，同时对比 1 / 4 / 32；固定 C_s^max、槽数与最大负载率，probeLimit 从 32 起测且不超过槽数；后台仅转移不超物理上限的空闲额度 |
| 热度 | Δ=1s；T_in 由业务热点定义；T_out=0.5T_in；H_cap=4×最大 T_in；grace≥2Δ，coldFor≥Δ |
| 统计 | 门卫 d=4、R=2，w 按非驻留工作集回放确定；收益采样 p=1/1024 起测，n_min=8 起测 |
| 估值时间 | 固定观测窗、预测上限 W、先验 / 模型最大年龄；与热点持续时间、TTL 联合校准 |
| 替换 | B_scan=64、m≤8、收益门槛 0；填充条数 / 字节速率按维护能力设定 |
| 生命周期 | 硬 TTL；抖动默认 0；handoffTimeout 显著短于 TTL；P、L 与冷却目标联立确定 |

配置变更不得破坏已有预算及状态语义。首版仅允许有明确转换规则的在线变化，例如带概率版本的采样调整；容量缩小、统计重建和代际切换必须有完成状态，不能瞬时宣称已生效。

初始化还需验证：各分类预算与预留总额不超 M；单对象能进入必要的响应 / pending / resident / retired 路径；阈值可被计数位宽表示；时间轮覆盖最大 TTL、允许滞后及舍入余量；所有更新 / 扫描 / 维护配额有合法上限。无法满足时拒绝创建实例，不能依靠上线后反复拒绝填充来掩盖不可用配置。

### 8.3 实现模块与交付边界

| 模块 | 输入 → 输出 | 必须先通过的局部验证 |
|---|---|---|
| 索引与读者保护 | 完整 key / Entry → 受保护命中、条件发布、按身份摘除 | 发布可见性、BUSY 语义、并发摘除、跨作用域持有 |
| 热度与冷却 | 外部 hit / miss、时间 → 准入资格、冷却状态 | 桶推进、饱和、有限失败、grace 与独立全扫描 |
| 收益与求解 | 样本、成本表、候选、Q、预算快照 → 最优可行计划或拒绝 | 三种估值状态、事件时间、O/S 不重叠、独立精确解对照 |
| Flight 与响应 | 请求、令牌、loader 完成 / 取消 → 业务结果、唯一候选 | 同身份 leader 唯一、实际 I/O 计量、响应交付及交接终态 |
| 提交与失效 | 计划、控制操作 → 可读 Entry、失效确认或拒绝 | 预留→复核→移除→发布顺序、expectedEntryId、代际竞态 |
| 资源与维护 | 额度凭证、队列、时间、读者退出 → 有界进展和安全回收 | 不超卖、结算一次、类别公平、无丢唤醒、慢读者恢复 |

模块可独立实现和验证，但不得各自重定义 key 身份、时间源、字节口径或版本令牌。进入编码前应固化共享状态与结果枚举；具体哈希表布局、原子打包和语言容器属于实现选型，需满足本文契约后再比较性能。

**规范检查入口：** [A.10 不变式](#ahkc-a10) 将上述模块对应到可直接写入模型 / 属性测试的断言。

## 9. 验证与交付：先证明正确和有界，再证明性能与策略收益

**验收分三条线同时完成：状态机不变式、算法收益、完整服务性能。** 纯命中微基准或单条 trace 的命中率不能代表方案达标。

### 9.1 目标与测试一一对应

| 验证项 | 方法 | 通过条件 / 输出 |
|---|---|---|
| TTL、失效、版本 | 交错查询 / 发布 / 失效 / 旧加载完成 / 旧 timer，测试维护长暂停 | 校验时刻无过期命中；失效完成后新请求不能读旧版本；旧操作不破坏新条目 |
| 热点发现与冷却 | 精确窗口对照；阈值边缘、桶边界、热点突变、停止访问 | 给出误准入 / 漏准入、新热点发现时间；在 P、L 条件成立时满足冷却预算 |
| 局部求解 | 小集合独立穷举 / 整数求解器对照，含多受害者、负损失和组合退休约束 | 枚举、快速路径及不操作选择一致；同 key 更新不漏算旧值 |
| 估值与有限搜索 | 预测对实际未来可避免收益；有限 Q 对全驻留集合的小规模精确解 | 分开报告采样 / 先验误差、候选遗漏损失及收益提升，避免归因混淆 |
| 内存与并发回收 | 慢读者、替换与释放交叉、索引满容量 / 碰撞、失效时退休满 | 无悬空访问、ABA、重复释放；所有受管预算始终成立 |
| 回源与过载 | 同 key 突发、取消无效、交接空窗、填充满但响应已预留、慢消费者、关闭交错 | 同身份合并有效；真实 I/O 与尚未移交响应均计量；无无限重启和等待者泄漏 |
| 条件提交 | 同代际旧候选覆盖新 entryId、旧身份已摘除、提交中途命名空间失效 / TTL 到期 | 条件不符拒绝；无法回滚时保证旧代际不可读、保留量有界，单列被中断的提交 |
| 长期进展 | 调度拒绝、恢复、零请求冷却、72h 稳态及流量抖动 | 各维护类别有进展，队列 / 保留量无失控趋势，可恢复正常准入 |

并发模型及原生内存问题在实现阶段用适用工具辅助验证，例如 [Loom](https://github.com/tokio-rs/loom)、[Miri](https://github.com/rust-lang/miri)、[ThreadSanitizer](https://clang.llvm.org/docs/ThreadSanitizer.html)；工具检查不能替代状态机测试和真实负载。

### 9.2 性能口径与发布要求

- **热路径：** 预热借用命中 0 堆分配；争取中位成本不超过同 key、同 TTL、同读者保护与返回语义的纯索引基线 1.5 倍。P50 < 150ns、P99 < 1μs 是指定工作集的挑战目标。
- **扩展性：** 测 1 / 2 / 4 / 8 / 16 / 32 物理核，跨 NUMA 单列；32 核挑战 50M ops/s，须同时满足尾延迟、维护进展和资源约束。
- **压力矩阵：** 均匀、Zipf、单 key 占 90% / 99.9%、扫描及热点切换；实际命中率 100% / 99% / 95% / 50% / 0%；工作集覆盖 L2、LLC 与超 LLC；混合 64B / 1KiB / 16KiB 及大对象、短 TTL 和集中到期。
- **完整服务：** 固定到达率的开放式回放计入排队、拒绝、真实加载时长和维护 CPU；正常流量满足宿主 P99 与可持续吞吐要求，异常流量下有界且可恢复。

每份报告固定硬件、核数 / SMT、频率与 NUMA、编译优化、分配器、分片及参数。标明 key 准备、value 访问、guard / 句柄释放是否计时；借用与拥有型分开，微基准与服务压测分开。多轮重复并保留原始数据，不用并行吞吐倒数冒充单次延迟。

最终成本账目为：

```text
净 CPU 节省 / 秒
= 避免的真实加载 CPU / 秒
− 查询、热度、采样与句柄 CPU / 秒
− 填充、维护、回收与切换 CPU / 秒
```

默认以此净收益及服务 P99 做选型决定；后端成本目标则另报 CPU 和下游 QPS。若集合优化增加的成本超过收益，应先调整 m、采样与填充预算，再决定是否保留该策略，不能以“局部最优”替代收益证据。

### 9.3 先进性与最优参照

使用相同总内存、TTL、热度资格、返回语义和加载时序，比较完整方案与 SIEVE、S3-FIFO、W-TinyLFU-AV；另选与最终语言匹配的原生 lib 基线，记录补齐语义的成本。对门卫、稀疏估值、集合优化和空闲额度转移分别做消融，说明改进来源。

离线参照遵循模型一致性：等大小、等 miss 成本的基础实验可用 Belady/MIN；变大小基础实验可用 FOO/PFOO 的 miss 数界；带 TTL、热度资格及加载可用时刻的完整小 trace 使用相应约束整数模型。若资格依赖驻留状态，oracle 也须重放它，不能固定本方案产生的候选轨迹后称全局 OPT。[FOO/PFOO](https://arxiv.org/abs/1711.03709)

报告实际收益、强基线收益及匹配上界的原始值；分开量化估值误差、有限搜索和资源限制的影响。静态收益上界不能直接当动态多轮策略上界，miss 数界也不能直接当净 CPU 收益界。

### 9.4 交付门槛与必要观测

| 阶段 | 必须交付的证据 | 下一步门槛 |
|---|---|---|
| 算法模型 | 热度 / 估值 / 替换状态定义、trace 模拟器、精确局部求解对照与强基线报告 | 策略收益可解释，采样及有限 Q 的损失可接受 |
| 原生原型 | 索引与保护选型、资源账目、并发测试、热路径及维护成本报告 | 正确性和资源不变式通过，达到约定性能预算 |
| 服务验证 | 真实加载回放、极端过载及 72h 报告、集成和退出行为 | 完整服务净收益及 P99 达标；对强基线优势或取舍明确 |

观测采用固定条带与低比例详细采样，不按 key 创建指标。至少保留：真实加载与命中收益、填充拒绝原因、样本丢弃 / 先验比例、求解检查数和增益、门卫误差、分片额度余量、队列年龄、实际扫描周期、退休字节 / 最老读者、孤儿加载数及失效完成延迟。告警从预算和服务期限推导。

## 10. 业界选型依据：采用成熟机制，对组合效果独立负责

| 研究 / 工程路线 | 本方案采用的结论 | 适用边界 |
|---|---|---|
| [TinyLFU](https://arxiv.org/abs/1512.00727) / [W-TinyLFU-AV](https://arxiv.org/abs/2105.08770) | 准入与驻留策略分离；变大小候选与释放空间的集合比较 | 本方案使用时间热度和统一成本收益，不能沿用原算法实验结论 |
| [SIEVE](https://www.usenix.org/conference/nsdi24/presentation/zhang-yazhuo) | 命中不移动链表，访问位与扫描手降低策略维护成本 | 仅借用候选生成；附加热度与优化器后的并发性能重新测量 |
| [S3-FIFO](https://www.cs.cmu.edu/~rvinayak/papers/s3-fifo-sosp-2023-fifo-queues-are-all-you-need-for-cache-eviction.pdf) | 低成本 FIFO / 快速过滤路线有竞争力，作为强基线 | 首版不再叠加它的多队列与 ghost，避免重复资格机制 |
| [Caffeine](https://github.com/ben-manes/caffeine/wiki/Design) / [Ristretto](https://github.com/dgraph-io/ristretto) | 将策略维护与请求解耦，有界缓冲允许放弃部分策略工作 | TTL、失效确认、硬资源预算及原生回收由本方案单独保证 |
| [CacheSack](https://www.usenix.org/conference/atc22/presentation/yang-tzu-wei) / [FOO](https://arxiv.org/abs/1711.03709) | 成本和容量可显式建模；用最优解或界判断策略差距 | 研究场景与约束不同，不作为本 lib 的性能承诺 |

本方案的先进性在于：在严格服务语义和有界资源前提下，将低成本命中路径、收益感知准入与可精确验证的局部决策组合为闭环。发布结论必须由第 9 节证据支撑；架构评审通过表示方案可实施、可验证，不等于性能指标已经实测达成。

<a id="ahkc-pseudocode"></a>

## 附录 A：核心数据结构与算法伪代码（实现规范）

**本附录与正文共同构成规范，给出一条能够逐步翻译为工程代码的完整参考路径。** 正文说明决策与依据，本附录明确字段、调用顺序和失败处理。算法不是生产代码，数组布局、SIMD、原子内存序优化需在不改变语义的前提下验证。

| 模块 | 入口与算法 | 对应正文 |
|---|---|---|
| 结构与基础原语 | A.1～A.2：字段、预算凭证、固定探测索引 | 第 2、6 节 |
| 查询与热度 | A.3：逻辑请求、受保护查询、双桶、门卫 | 第 2～3 节 |
| 收益估计 | A.4：事件处理、质量状态、成本收益计算 | 第 4.1～4.2 节 |
| 准入与替换 | A.5：候选集、精确子集求解、条件提交 | 第 4.3～4.5 节 |
| 加载与交付 | A.6：Flight、完成邮箱、响应、填充交接 | 第 5.2 节 |
| 失效与过期 | A.7：控制提交、冷却、时间轮及恢复 | 第 3.3、5.1、5.3 节 |
| 回收与进展 | A.8～A.9：epoch、析构交接、调度、额度转移、关闭 | 第 6～7 节 |

<a id="ahkc-a1"></a>

### A.1 约定、基础原语与完整逻辑结构

记号约定：`atomic<T>` 是原子状态；`owner<T>` 只允许指定维护者写；`immutable<T>` 发布后不可变；`Ref<T>` 持有有界池对象，最后一个引用结束前不得复用其槽位。所有数组、池、循环次数和工作预算均来自初始化配置。`NONE`、`BUSY`、`MISS` 和失败结果不可混用。

以下 `key` 均为包含 namespaceId 的完整键；`namespace`、`shard`、`stats` 等省略前缀时，均从当前请求 / Entry / Flight 的归属解析。`e.namespace` 是 namespaces[e.namespaceId] 的只读简写。同名字段访问不代表额外字典或全局共享可变状态。

正确性参考模型采用顺序一致原子操作；明确标注的策略计数 / visited 可较弱同步。工程上改为 acquire/release 等内存序必须单独证明，不能机械地把所有原子替换为 relaxed。以下基础原语可以由成熟库提供，其契约是算法的一部分：

| 原语 | 必须满足的契约 |
|---|---|
| `TRY_GATE(g, R)` | 最多 R 次非阻塞尝试；成功持有短临界区，否则 BUSY；退出不执行用户回调 |
| `TRY_PUSH / TRY_POP` | 有界队列；成功仅在完整消息可见后成立；失败不遗留会永久阻塞队列的空洞；队列内部发布仍需符合并发协议 |
| `TRY_POOL_REF` | 从固定池取得独占槽位 / 引用，耗尽失败；旧 Ref 尚存时不得复用槽位 |
| `Ref / ownedPins` | 引用计数不得回绕；增加持有前检查配置上限，不能取得新引用时返回 BUSY；已有持有仍可正常释放 |
| `MARK_READY(waiter)` | 仅通知宿主就绪机制，不内联执行业务回调；等待注册与就绪检查不得丢唤醒 |
| `CAS_OWN_READY_VALUE` | 对带结果的阶段做原子匹配与拥有权转移；只有成功者可以解引用 / 移走结果，终态不回退 |
| `TRY_SUBMIT(task)` | 接受则至多执行一次；拒绝则不执行；不得采用 caller-runs 或内联递归执行 |
| `HOST_LOAD / DISPOSE` | 只在宿主加载 / 回收执行资源上运行；不持有索引、Flight 或维护临界区；结果大小受契约约束 |
| `CHECKED_NEXT(id)` | 单调生成不重复身份；将溢出时停止受理并要求重建，不能回绕 |

这些原语不封装热度、估值、选受害者或提交策略；相关算法全部在后文展开。初始参考值：PIN_RETRIES、TOKEN_RETRIES、RESERVE_RETRIES、FLIGHT_RETRIES 均为 2，PUMP_STEPS_PER_RUN=4，NOTIFY_BATCH=32；均作为有界配置验证，不能替换为无限重试。

```text
Config:
    shardCount, namespaceLimit, maxKeyBytes
    C, B, M                             // 条数、可加驻留计费、完整受管预算
    shard[i].countMax, slotCount, probeLimit
    heatDelta, Tin[ns], Tout[ns], Hcap, heatRows, heatWidth, heatRetries
    grace, coldFor, valueDelta, valueRows, valueWidth, sampleP[ns], nMin
    maxSampleLag, horizonW, priorMaxAge, costMaxAge
    maxEntryBytes, maxResultBytes, maxConcurrentLoads, loadTimeout, handoffTimeout
    flightStripeCount, flightsPerStripe, maxWaitersPerKey, maxWaitersTotal
    readerSlotsPerShard, maxGuardsPerContext
    sieveScanLimit, victimLimit <= 8
    wheelTick, wheelSlots K, wheelLevels L, maxTTL, maxWheelLag
    perClassWorkLimits, fillRateLimit, periodicInterval
    fixedLimitsForAllByteBudgetsAndObjectPools

Cache:
    atomic state = OPEN                 // OPEN -> CLOSING -> RECLAIMED；停止受理单独报告
    Namespace namespaces[namespaceLimit]
    Shard shards[shardCount]
    owner globalFreeCount, globalFreeBytes
    owner creditTransferState           // 单协调者；不能直接修改正在运行的分片
    fixed namespaceControlQueue

Namespace:
    atomic gen
    immutable key/TTL/costClass rules
    atomic<Ref<ImmutableModel>> versionedPriorAndCostModel
    // 有限大小、有限年龄、固定收益单位；更新者原子发布，旧快照用完后回收

Shard:
    atomic ownerState = IDLE            // IDLE / QUEUED / RUNNING
    atomic pending, pauseReasons                 // WHEEL_REBASE / EXECUTOR_REJECTED / CLOSING 等位
    atomic nextAllowedRunTime, taskSequence
    owner resumeClass
    owner countQuota, byteQuota, residentCount, residentBytes
    atomic fillFence
    owner entrySeq
    Index index
    HeatCell gate[heatRows][heatWidth]
    ValueStats valueStats
    fixed sampleStripes[], fillQueue, controlQueue
    owner policyList, hand, coldCursor   // hand 是队列位置；coldCursor 是固定索引槽编号
    TimerWheel wheel
    FlightStripe flights[flightStripeCount]
    EpochDomain epoch
    fixed retiredPool, pendingPool, deliveryPool, completionPool
    Budget pendingBytes, responseBytes, retiredBytes
    fixed cleanupJobPools               // 各保留类别均有可结算的回收记录
    owner workQuota, fillRateTokens

Index:
    atomic<EMPTY | TOMBSTONE | Entry*> slots[slotCount]
    immutable hashSeed, probeLimit      // 首版固定线性探测参考实现；不搬移已发布条目

Entry:
    immutable key, keyHash, entryId, namespaceId, namespaceGen
    immutable valueRef, hardDeadline, billedBytes, costClass, domainLifetimeRef
    atomic readable                     // PREPARED 不可见；发布后 true -> false
    HeatCell heat
    atomic visited = false, ownedPins = 0
    owner graceUntil, lowSince = NONE, indexSlot
    owner policyPrev, policyNext
    owner timerEpoch, timerLevel, timerSlot, timerPrev, timerNext
    owner residencyReceipt              // 本条目的计费责任；同一转移只结算一次

HeatCell:
    atomic consistent {epoch, current, previous}

ValueCell:
    owner epoch, weightedCurrent, weightedPrevious, samplesCurrent, samplesPrevious

ValueStats:
    owner cells[valueRows][valueWidth], builtAt, lastFaultAt, modelVersion
    owner boundedCostTable, aggregateWatermark
    // 每个样本条带另有 atomic faultFlag；丢样不能因标记竞争而静默消失

RequestContext:
    immutable cache, key, shard, namespace
    sampledOnce = false, heatClass = UNRECORDED   // HIT / MISS 最多选一次

Token:
    namespaceGen, fillFence

FlightStripe:
    gate
    fixed Ref<Flight> slots[flightsPerStripe]

Flight:
    immutable key, namespaceId, shardId, stripeId, flightId, token, expectedEntryId
    immutable registeredAt, loadDeadline, validityBase
    phase = LOADING                     // 由 stripe gate 保护；含 COMPLETING 过渡态
    atomic sourcePending = true         // timeout 不得提前清除此项
    Ref<Delivery> delivery = NONE
    Ref<FillCell> fillCell = NONE
    reuseDeadline = 0                   // 与 phase 一样由 stripe gate 保护
    fixed waiterSlots[maxWaitersPerKey]
    ioReceipt, responseReservation, recordReceipt
    OneShotMailbox sourceOutcome         // 完成回调不用争抢 gate 才能保存结果
    atomic pumpNeeded, completionOwner
    guardedByCompletionToken work = NONE         // 固定 Work 联合体，阶段与所有权可恢复

Waiter:
    atomic state = WAITING               // WAITING -> READY -> TAKEN，或 CANCELLED
    Ref<Delivery> delivery = NONE
    slotReceipt, deadline

Delivery:
    immutable outcome                   // 不可变业务结果或错误；也可封装缓存拥有型句柄
    atomic internalRefs                 // Flight bridge 和未移交 waiter 的持有
    responseReceipt, reservedCleanupJob
    atomic cleanupState

FillCell:
    atomic phase = QUEUED                // QUEUED / COMMITTING / 终态
    Ref<Flight> flight                   // 可持有身份记录，不能隐式多持有 value
    // 终态原因包含在同一个阶段值中发布，不能先宣布终态再补写原因

FillCandidate:
    immutable token, expectedEntryId, handoffDeadline
    Entry* preparedEntry                // 未公开；payload/TTL 已初始化，entryId 由维护者补齐
    Ref<FillCell> cell
    pendingReceipt, reservedCleanupJob

Work:
    stage, afterNotify, capturedWaiterRefs[maxWaitersPerKey], notifyCursor
    optional inlineError                // 有界错误码；不存在时通过 Delivery 交付
    reuseDeadline=0, immutable metadata, candidate ownership or late source ownership
    // 每个 Flight 至多一个 Work；每个阶段一次性向前推进

CleanupJob:
    atomic state = RESERVED              // RESERVED -> READY -> SUBMITTING -> RUNNING -> DONE -> ACCOUNTED
    kind = PENDING_ENTRY | DELIVERY | LATE_SOURCE
    ownedPayload or Ref<Delivery>, Receipt, ownerPoolRefs
    // 所有权在 READY 之前建立；并非一个无界回调闭包

Control:
    immutable operationId, kind, namespaceId, optional completeKey
    atomic completion = PENDING         // PENDING -> SUCCESS / 明确失败原因
    recordReceipt, reservedCompletionSlot
    // operationId 与完成槽预留成功后才能入队；目标键同样受 maxKeyBytes 限制

RetiredRecord:
    Entry* entry, entryId, retireEpoch, billedBytes
    retiredReceipt, reservedCleanupJob
    atomic state = WAIT_SAFE            // WAIT_SAFE -> SUBMITTING -> DISPOSING -> DONE -> ACCOUNTED

EpochDomain:
    atomic epoch
    ReaderSlot slots[readerSlotsPerShard] // INACTIVE 或 ACTIVE(epoch)；不搬动活跃槽
    owner advanceScanCursor, scanEpoch, scanBlocked

TimerWheel:
    owner epoch, cursorTick, rebaseCursor, rebasing
    owner buckets[L][K]                 // 侵入式链表；每桶带 epoch，旧 epoch 可惰性清空
    owner resumableTickAndCascadeState

Budget:
    immutable limit
    atomic available                    // available + used + reserved = limit

Receipt:
    immutable budget, amount
    atomic settled = false              // 归还一次；转移所有权不等于归还
```

固定池的存储在 M_fixed 中记一次；槽位凭证限制占用，不再虚构一份物理内存。value / key 等动态保留量按所属责任记账。跨对象共享默认保守计费。调度、样本、成本观测和完成单元也不得使用无界按 key 字典。

<a id="ahkc-a2"></a>

### A.2 预算凭证与可条件预留的索引

**容量额度可以基于同一维护者的删除计划复用，物理保留预算不能预支尚未完成的回收。** 普通预算凭证用于已经存在的分类上限；分片驻留量在提交时串行结算。

```text
TRY_RESERVE(budget, amount):
    repeat at most RESERVE_RETRIES:
        old = budget.available.load()
        if old < amount: return FULL
        if CAS(budget.available, old, old - amount):
            return Receipt(budget, amount)        // 凭证槽位已预留，不在成功后再失败分配
    return BUSY

RETURN_ONCE(receipt):
    if CAS(receipt.settled, false, true):
        receipt.budget.available.fetch_add(receipt.amount)

TRY_BUNDLE(requirements):
    acquired = fixed small list
    for (budget, amount) in requirements:         // 项数固定，不含按 key 扩展
        r = TRY_RESERVE(budget, amount)
        if r failed:
            RETURN_ONCE(each acquired)
            return r.failure
        acquired.append(r)
    return acquired                              // 后续交付给唯一持有者

PROBES(index, keyHash):
    start = MIX(index.hashSeed, keyHash) mod index.slotCount
    return [(start+j) mod slotCount for j in 0 .. probeLimit-1]

INDEX_FIND(index, key, guard):
    require guard is active before first pointer load
    for slot in PROBES(index, HASH(key)):
        p = index.slots[slot].load_acquire()
        if p == EMPTY: return ABSENT
        if p != TOMBSTONE and EXACT_KEY_EQUAL(p.key, key): return FOUND(p, slot)
    return ABSENT                                // 不是因预算失败而随意推断不存在，见下方证明

INDEX_INSTALL_SLOT(index, key, removalSet):       // 维护者；不实际删除
    for slot in PROBES(index, HASH(key)):
        p = index.slots[slot].load()
        if p in {EMPTY, TOMBSTONE} or p belongs to removalSet:
            return slot
    return NO_POSITION

INDEX_REMOVE(entry):                             // 唯一维护者
    assert index.slots[entry.indexSlot] == entry
    entry.readable.store(false)
    index.slots[entry.indexSlot].store_release(TOMBSTONE)

INDEX_PUBLISH(entry, reservedSlot):
    assert index.slots[reservedSlot] in {EMPTY, TOMBSTONE}
    entry.indexSlot = reservedSlot
    entry.readable.store(true)
    index.slots[reservedSlot].store_release(entry) // 单条发布可见点
```

参考索引规定**插入也只能落在该 key 的固定 probeLimit 个位置内**，且不搬移条目，因而完整检查这些位置后可以确认 ABSENT。它不会因积累 tombstone 就无限延长查找；找不到合法插入位置时拒绝填充。若替换为支持搬移 / 扩容的索引，必须另外实现其读取协议；无法完整判定时返回 BUSY，不能照搬上述 ABSENT 结论。probeLimit 可从 32 起测，索引槽数按负载率预留，碰撞导致的拒绝必须单列。

EMPTY 是从未写入的位置；删除只产生 TOMBSTONE，不能清成 EMPTY 破坏探测链。发布和摘除均只由维护者执行；查询与替换重叠时可观察替换中的真实缺口。`INDEX_INSTALL_SLOT` 不拿“未来可能回收的内存”作为插入资源，只预留本次删除后确定可用的槽位。

<a id="ahkc-a3"></a>

### A.3 逻辑请求、命中路径与双桶热度

```text
BEGIN_REQUEST(cache, key):
    validate bounded key and namespace
    ctx = RequestContext(cache, key, HASH_TO_SHARD(key))
    p = current namespace sample probability
    if independent BERNOULLI(p):
        g = namespace.gen.load()
        sample = {namespaceId, policyHash(key,g), namespaceGen=g, eventTime=NOW(), p}
        stripe = fixed stripe selected by registered request context
        if not TRY_PUSH(stripe.queue, sample): stripe.faultFlag.store(true)
        else SIGNAL_MAINTENANCE(ctx.shard)
    ctx.sampledOnce = true
    return ctx

NORMALIZE_HEAT(snapshot, time, delta):
    e = floor(time / delta)
    if snapshot.epoch > e: return UNKNOWN         // 请求被暂停后恢复，不使桶倒退
    if e == snapshot.epoch: return snapshot
    if e == snapshot.epoch + 1:
        return {e, current=0, previous=snapshot.current}
    return {e, current=0, previous=0}

HEAT_ADD(cell, time):
    repeat at most heatRetries:
        old = cell.load_consistent()
        x = NORMALIZE_HEAT(old, time, heatDelta)
        if x == UNKNOWN: return SKIPPED
        if x.epoch == old.epoch and x.current == Hcap: return SATURATED
        x.current = min(x.current + 1, Hcap)
        if CAS_CONSISTENT(cell, old, x): return UPDATED
    return SKIPPED

HEAT_READ(cell, time):
    x = NORMALIZE_HEAT(cell.load_consistent(), time, heatDelta)
    if x == UNKNOWN: return UNKNOWN
    phi = (time mod heatDelta) / heatDelta
    return x.current + (1-phi) * x.previous

GATE_ADD(shard, key, time):
    for row in 0 .. heatRows-1:
        HEAT_ADD(shard.gate[row][ROW_HASH(row,key) mod heatWidth], time)

GATE_READ(shard, key, time):
    hs = [HEAT_READ(selected row cell,time) for each fixed row]
    if any UNKNOWN: return UNKNOWN
    return min(hs)

RECORD_HEAT_ONCE(ctx, result, entry, time):
    if ctx.heatClass != UNRECORDED: return
    ctx.heatClass = result
    if result == HIT: HEAT_ADD(entry.heat, time)
    else if result == MISS: GATE_ADD(ctx.shard, ctx.key, time)

READ_INTERNAL(cache, shard, key, returnMode):      // 内部调用不自动计逻辑请求
    if cache.state != OPEN: return CLOSED
    guard = PIN(shard.epoch)                      // A.8；先保护后读指针
    if guard failed: return BUSY
    found = INDEX_FIND(shard.index, key, guard)
    t = NOW()
    if found == ABSENT:
        UNPIN(guard); return MISS(time=t, expected=NONE)
    e = found.entry
    if not e.readable or e.namespaceGen != namespace.gen or t >= e.hardDeadline:
        id = e.entryId
        UNPIN(guard); return MISS(time=t, expected=id)
    if returnMode == BORROW:
        return HIT(BorrowView(e, guard), time=t)   // guard 随视图存活
    if bounded non-overflowing increment of e.ownedPins fails:
        UNPIN(guard); return BUSY
    handle = CacheHandle(e)                      // Entry 自身持有域生命周期，不逐命中增加全局引用计数
    UNPIN(guard)
    return HIT(handle, time=t)

GET(ctx, mode):
    r = READ_INTERNAL(ctx.cache, ctx.shard, ctx.key, mode)
    if r is HIT:
        RECORD_HEAT_ONCE(ctx, HIT, r.entry, r.time)
        if not r.entry.visited.load_relaxed(): r.entry.visited.store_relaxed(true)
    else if r is MISS:
        RECORD_HEAT_ONCE(ctx, MISS, NONE, r.time)
    return r                                     // BUSY 不计作确定的 miss
```

拥有型 HIT 在退出 guard 后仍由 handle 保护 Entry，允许后续有界热度更新；若此时已被摘除，统计只影响旧 Entry，不会改变可读状态。借用 guard 不得跨 await。sample 的事件时间是入口时间；仅被采样请求额外取得这个时间，TTL 校验仍使用查询时的当前时间。

<a id="ahkc-a4"></a>

### A.4 样本处理与收益计算

**MEASURED 是“按可用样本估值”的工程状态，不是无误差证明。** 样本传输滞后也要计量；估值使用同一维护轮次冻结的统计快照，不暗示全部并发请求已经进入计数。

```text
DRAIN_SAMPLES(shard, quota):
    for each fixed sample stripe:                 // 扫描成本包含在样本配额
        if stripe.faultFlag.exchange(false): stats.lastFaultAt = NOW()
    while quota permits one complete sample update:
        s = TRY_POP_NEXT_SAMPLE_ROUND_ROBIN()
        if none: break
        t = NOW(); epoch = floor(t / valueDelta)
        if s.eventTime > t or floor(s.eventTime/valueDelta) < epoch-1:
            stats.lastFaultAt = t; continue
        if s.namespaceGen != current namespace.gen: continue
        weight = 1 / s.p
        if not finite(weight) or s.p <= 0 or s.p > 1:
            stats.lastFaultAt = t; continue
        for each fixed value row:
            c = stats.cells[row][ROW_HASH(row,s.policyHash) mod valueWidth]
            NORMALIZE_VALUE_CELL_IN_OWNER(c, epoch) // 与双桶平移相同，计数不饱和
            bucket = current if event epoch == epoch else previous
            c[bucket].weighted += weight
            c[bucket].samples += 1
            if numeric overflow: stats.lastFaultAt = t
    update bounded queue age / consumed-prefix watermark

ESTIMATE(item, snapshotTime, statsSnapshot):
    if snapshotTime >= item.hardDeadline: return EXPIRED
    if any unconsumed fault flag or any sample prefix lag > maxSampleLag:
        return UNAVAILABLE
    if snapshotTime < stats.lastFaultAt + 2*valueDelta: return UNAVAILABLE
    model = statsSnapshot.model         // 本次决策冻结的不可变 Ref，所有候选共用
    if model expired, incompatible, or cost class outside calibrated support:
        return UNAVAILABLE
    selectedCells = rows selected by policyHash(item.key,item.namespaceGen)
    weighted = min(DOUBLE_BUCKET_VALUE(c.weighted, snapshotTime) over selectedCells)
    samples  = min(DOUBLE_BUCKET_VALUE(c.samples, snapshotTime) over selectedCells)
    warm = snapshotTime >= stats.builtAt + 2*valueDelta
    if warm and samples >= nMin:
        lambda = weighted / valueDelta; mode = MEASURED
    else if prior exists and its age <= priorMaxAge:
        lambda = prior.lambda[namespace,costClass]; mode = PRIOR
    else: return UNAVAILABLE
    (r,c,h,k,K) = LOOKUP_COST_MODEL(model, lambda, item.costClass)
    if any input is non-finite or negative: return UNAVAILABLE
    duration = min(horizonW, item.hardDeadline-snapshotTime)
    G = (r*c-h) * duration
    return {G, k, removalLoss=G+k, installCost=K, mode, snapshotTime, model.version}
```

`NORMALIZE_VALUE_CELL_IN_OWNER` 与 `NORMALIZE_HEAT` 的桶轮换相同，但分别维护加权和与样本数，且没有 Hcap。`LOOKUP_COST_MODEL` 在有限请求率 / 大小 / 加载时长类别中查表，区间内可固定次数插值，越界返回 UNAVAILABLE，不执行在线推理。r 由保留 singleflight 的校准回放得到；c 的 CPU 成本不能用加载墙钟时长冒充。h、k、K 来自固定版本基准；线上成本观测只在完成路径收集，由维护者在固定类别中汇总、更新年龄。首版无需在线训练才能运行。

<a id="ahkc-a5"></a>

### A.5 候选生成、精确替换与提交

**求解器接收真实可执行的动作集，不能先求一个容量解，再遗漏索引槽位、退休对象数或提交工作量。** 以下计算全部位于分片唯一维护者内。

```text
COLLECT_Q(shard, oldEntry, t, lifeQuota):
    Q = fixed array[victimLimit]; anchor = NONE
    limit = min(sieveScanLimit, policyList.countAtStart)
    repeat at most limit times:
        if policyList empty: break
        e = hand if hand exists else policyList.tail
        if e == anchor: break                    // 不因删除后队列变短而重复绕圈
        hand = NEXT_TOWARD_HEAD_OR_WRAP(e)
        if not e.readable or t >= e.hardDeadline or e.namespaceGen != namespace.gen:
            e.readable.store(false)
            removed = TRY_RETIRE(e, lifeQuota)    // A.7～A.8；无预算仍留索引计量
            if not removed and anchor == NONE: anchor = e
            continue
        if anchor == NONE: anchor = e            // 后续本轮不再删除此锚点
        if e == oldEntry: continue
        if e.visited.exchange(false): continue
        Q.append(e)
        if Q.size == victimLimit: break
    return Q

MATCH_EXPECTED(candidate, current):
    if candidate.expectedEntryId == NONE: return current == ABSENT
    return current is FOUND and current.entryId == candidate.expectedEntryId

FREEZE_INPUT(a, O, Q, shard):
    t = NOW()
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace): return STALE
    if t >= min(a.entry.hardDeadline,a.handoffDeadline): return EXPIRED
    if GATE_READ(shard,a.key,t) is UNKNOWN or < Tin[a.namespace]: return NOT_HOT
    ss = freeze stats view and one Ref to namespace.versionedPriorAndCostModel
    av = ESTIMATE(a.entry,t,ss)
    if av unavailable: return NO_ESTIMATE
    oldLoss = 0
    for o in O:
        ov = {G=0,k=ss.model.removeCost(o)} if o invalid/expired else ESTIMATE(o,t,ss)
        if ov unavailable: return NO_ESTIMATE
        oldLoss += ov.G + ov.k
    for v in Q:
        if v invalid/expired: exclude v from Q    // 不追加新搜索；实际 Q 随之缩小
        else:
            vv = ESTIMATE(v,t,ss)
            if vv unavailable: return NO_ESTIMATE
            save immutable {entryId,billedBytes,loss=vv.G+vv.k} for v
    freeze N=residentCount, Bytes=residentBytes, countQuota, byteQuota
    freeze retired byte availability, record/job slots, removal-work availability
    probeSlots = PROBES(index,a.keyHash)
    installWithoutQ = any probe slot is EMPTY/TOMBSTONE or contains member of O
    installMask = bitmask of Q members whose current indexSlot is in probeSlots
    return Snapshot(t,a,O,Q,av,oldLoss,all frozen resources)

FEASIBLE(snapshot, mask):
    R = O union Q[mask]                           // O 与 Q 不相交
    postN = N + 1 - size(R)
    postB = Bytes + a.billedBytes - SUM(billedBytes of R)
    if postN > countQuota or postB > byteQuota: return false
    if postN > index.countMax: return false
    if SUM(billedBytes of R) > retiredBytesAvailable: return false
    if size(R) > min(retiredRecordSlots, retiredJobSlots, removalWorkLeft): return false
    if not installWithoutQ and (mask & installMask) == 0: return false
    if wheel is rebasing or candidate deadline outside wheel schedulable range: return false
    return true                                  // 入队前已保证 candidate 完整 pending 责任

EXACT_SELECT(snapshot):
    m = Q.size
    if m == 0: return 0 if FEASIBLE(snapshot,0) else INFEASIBLE
    if every Q.loss >= 0 and FEASIBLE(snapshot,0): return mask=0
    if all Q.b equal and all Q.loss >= 0 and installWithoutQ:
        k = max(0, N+1-size(O)-countQuota,
                   ceil(max(0,Bytes+a.b-SUM(O.b)-byteQuota)/Q[0].b))
        if k <= m:
            mask = stable k smallest losses
            if FEASIBLE(snapshot,mask): return mask
        // 所有同大小、同数量组合的资源要求相同；无解即可拒绝
        return INFEASIBLE
    best = NONE
    sums[0] = {count=0, bytes=0, loss=0}           // 最多 256 项，固定 scratch
    for mask in 0 .. (1<<m)-1:
        if subsetWorkQuota exhausted: return WORK_LIMIT
        if mask != 0:
            bit = LOWEST_SET_BIT(mask); prev = mask without bit
            v = Q[INDEX(bit)]
            sums[mask] = {count=sums[prev].count+1,
                          bytes=sums[prev].bytes+v.billedBytes,
                          loss=sums[prev].loss+v.loss}
        if FEASIBLE(snapshot,mask):
            rank = (sums[mask].loss, sums[mask].bytes, sums[mask].count, stableIds(mask))
            if best == NONE or rank < best.rank: best = (mask,rank)
    return best.mask if best exists else INFEASIBLE

PLAN_FILL(a, shard):
    if a.cell.phase != QUEUED: return ALREADY_TERMINAL
    current = INDEX_FIND_IN_OWNER(index,a.key)
    if not MATCH_EXPECTED(a,current): return STALE
    O = {current.entry} if current is FOUND else {}
    if O-only replacement fits local count/bytes, retired resources and index position:
        Q = []                                   // 空闲快速路径明确选用空 Q，不做无意义扫描
    else: Q = COLLECT_Q(shard,O.singleOrNone,NOW(),lifeQuota)
    // COLLECT_Q 可能执行生命周期清理，所以在它之后复核旧身份与占用
    if not MATCH_EXPECTED(a,INDEX_FIND_IN_OWNER(index,a.key)): return STALE
    x = FREEZE_INPUT(a,O,Q,shard)
    if x failed: return x.reason
    selected = EXACT_SELECT(x)
    if selected failed: return selected.reason
    gain = x.av.G - x.av.installCost - x.oldLoss - SUM(Q[selected].loss)
    if gain <= 0: return NON_POSITIVE_GAIN
    return Plan(x, removals=O union Q[selected], gain)

PREPARE_COMMIT(a,plan):
    require shard owner retained until commit or rejection completes
    if identities, countQuota, byteQuota or required work resources no longer match:
        return STALE_PLAN
    lease = TRY_BUNDLE(retired bytes, retired record slots, retired cleanup job slots)
    if lease failed: return RESOURCE_LIMIT
    slot = INDEX_INSTALL_SLOT(index,a.key,plan.removals)
    if slot == NO_POSITION: RETURN_ONCE(lease); return INDEX_LIMIT
    a.entry.entryId = (shard.id, CHECKED_NEXT(shard.entrySeq))
    return Prepared(plan,lease,slot)              // 不再需要常规分配；不提前捐出计划释放额度

COMMIT_FILL(a,prepared):
    t = NOW()
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace): reject STALE
    if not MATCH_EXPECTED(a,INDEX_FIND_IN_OWNER(index,a.key)): reject STALE
    if t >= min(a.entry.hardDeadline,a.handoffDeadline): reject EXPIRED
    if any planned entry identity/resource differs: reject STALE_PLAN
    if not CAS(a.cell.phase,QUEUED,COMMITTING): reject ALREADY_TERMINAL
    // 此点后普通超时方无权改变 fill 终态；无用户回调、无失败分配
    for e in prepared.removals:
        INDEX_REMOVE(e)
        UNLINK_POLICY_AND_TIMER_FIX_HAND(e)
        residentCount -= 1; residentBytes -= e.billedBytes
        TRANSFER_RESIDENT_TO_RETIRED(e, prepared.preReservedReceiptFor(e))
        REGISTER_RETIRED(e, epoch.current)        // 记录已由 prepared 预留
    if cache.state != OPEN or a.token != CURRENT_TOKEN(shard,a.namespace)
       or NOW() >= a.entry.hardDeadline:
        DISPOSE_PENDING_USING_RESERVED_JOB(a)    // 未发布对象继续算 pending，直到真实释放
        FINISH_FILL_AS_OWNER(a.cell,STALE_OR_EXPIRED)
        RELEASE_UNUSED_PREPARED_RESOURCES()
        return
    e = MOVE(a.preparedEntry)
    residentCount += 1; residentBytes += e.billedBytes
    TRANSFER_PENDING_TO_RESIDENT_RECEIPT(a,e)     // 新账目先成立，再归还旧 pending 凭证
    e.graceUntil = NOW()+grace; e.lowSince = NONE
    LINK_POLICY_AT_HEAD(e); WHEEL_ADD(e)
    INDEX_PUBLISH(e,prepared.slot)
    // namespace invalidation / 时间流逝仍可能与发布交错，读取检查是最终保障
    if e.namespaceGen != namespace.gen or NOW() >= e.hardDeadline or cache.state != OPEN:
        e.readable.store(false)                  // 保留 resident 计费；后续有预算再退休
        outcome = STALE_OR_EXPIRED
    else: outcome = PUBLISHED
    FINISH_FILL_AS_OWNER(a.cell,outcome)
    RELEASE_UNUSED_PREPARED_RESOURCES()
    SIGNAL_FLIGHT_PUMP(a.cell.flight)
    return
  reject reason:
    RELEASE_UNUSED_PREPARED_RESOURCES()
    REJECT_QUEUED_IF_STILL_OWNER(a,reason)        // 未改变驻留集合

PROCESS_FILL(a):
    if a.cell.phase is terminal:
        DISPOSE_PENDING_USING_RESERVED_JOB(a); return
    plan = PLAN_FILL(a,shard)
    if plan failed: REJECT_QUEUED_IF_STILL_OWNER(a,plan.reason); return
    prepared = PREPARE_COMMIT(a,plan)
    if prepared failed: REJECT_QUEUED_IF_STILL_OWNER(a,prepared.reason); return
    COMMIT_FILL(a,prepared)

CANCEL_FILL_CELL(cell,reason):                    // 超时 / 关闭方只改阶段
    CAS(cell.phase,QUEUED,TERMINAL(reason))
    SIGNAL_FLIGHT_PUMP(cell.flight)               // 不能在这里释放队列消费者还在用的 payload

REJECT_QUEUED_IF_STILL_OWNER(a,reason):
    CAS(a.cell.phase,QUEUED,TERMINAL(reason))      // 已被取消时保留原终态
    assert a.cell.phase is terminal              // 当前独占消费者未交给其他提交者
    DISPOSE_PENDING_USING_RESERVED_JOB(a)
    SIGNAL_FLIGHT_PUMP(a.cell.flight)

REJECT_UNENQUEUED_CANDIDATE(a,reason):
    REJECT_QUEUED_IF_STILL_OWNER(a,reason)         // 失败入队没有转移 candidate 所有权

FINISH_FILL_AS_OWNER(cell,reason):
    assert CAS(cell.phase,COMMITTING,TERMINAL(reason)) succeeds
    SIGNAL_FLIGHT_PUMP(cell.flight)
```

`INDEX_FIND_IN_OWNER` 使用同一探测规则，维护者持有结构所有权而无需借用 guard。`UNLINK_POLICY_AND_TIMER_FIX_HAND` 是固定次数链表改链：修正 head/tail，删除 hand 时先取后继，单元素删除后 hand 置 NONE，并撤销当前 timer；coldCursor 是索引槽号，不随删除移动。`TRANSFER_*` 只移动已有保留责任，实际空间释放只发生在 A.8 的回收完成之后。伪代码中的 a.entry/key/namespace、Q.b 是相应结构字段的只读简写，不是另一份未定义状态。

等大小快速路径还依赖本规范列出的组合资源仅由条数 / 字节及已解决的索引位置决定；新增 tenant 限制、不同退休类别或其他组合约束时，必须关闭此快速路径或重新证明。m=0 时单独判空集合，不访问 Q[0]。枚举中途超出工作量，整次拒绝，不能提交当前暂定最佳解。

<a id="ahkc-a6"></a>

### A.6 加载合并、响应交付与填充握手

**Flight 使用短临界区管理注册，真实完成通过预分配的单次完成邮箱交接。** 完成回调暂时拿不到 Flight gate 也不能丢结果或泄漏 I/O 额度。COMPLETING 是明确的过渡态：已有等待者可以完成，新请求短暂返回 BUSY，只有通过初筛并成功入队后才允许 completed_pending 复用。

```text
READ_STABLE_TOKEN_AND_EXPECTED(ctx):
    repeat at most TOKEN_RETRIES:
        t1 = {namespace.gen.load(), shard.fillFence.load()}
        guard = PIN(shard.epoch)
        if failed: return BUSY
        x = INDEX_FIND(index,ctx.key,guard)
        expected = x.entryId if FOUND else NONE
        UNPIN(guard)
        t2 = {namespace.gen.load(), shard.fillFence.load()}
        if t1 == t2: return (t1,expected)
    return BUSY

GET_OR_LOAD(ctx):
    hit = GET(ctx,OWNED)
    if hit != MISS: return hit
    (token,expected) = READ_STABLE_TOKEN_AND_EXPECTED(ctx)
    if failed: return BUSY
    stripe = FLIGHT_STRIPE(ctx.key)
    if TRY_GATE(stripe.gate,FLIGHT_RETRIES) failed: return BUSY
    f = find matching (complete key,token) in fixed stripe slots
    if f exists:
        if f.phase == LOADING:
            w = TRY_ATTACH_WAITER(f)              // 同时预留 per-key / total waiter 槽位
        else if f.phase == COMPLETED_PENDING and f.fillCell not terminal
                and token == CURRENT_TOKEN() and NOW() < f.reuseDeadline:
            w = TRY_ATTACH_WAITER(f)
            if success: BIND_READY(w,f.delivery)
        else:
            unlock; SIGNAL_FLIGHT_PUMP(f); return BUSY
        unlock; return Future(w) if attached else OVERLOADED
    bundle = TRY_BUNDLE(flight record/pool slot, I/O permit,
                        maxResultBytes response reservation + delivery cleanup record,
                        first waiter slot)
    if failed: unlock; return OVERLOADED
    f = initialize Flight(token,expected,unique flightId,bundle)
    f.registeredAt = NOW(); f.loadDeadline = f.registeredAt + loadTimeout
    f.validityBase = f.registeredAt               // 比实际调用更早，作为保守 TTL 基准
    register f before unlocking                   // 同身份后续请求只能 join
    w = f.firstWaiter
    unlock
    r = READ_INTERNAL(cache,shard,key,OWNED)       // 二次检查，无额外外部计数
    if r == HIT: FINISH_SOURCE(f,CACHED(r.handle)); return Future(w)
    if r in {BUSY,CLOSED} or token != CURRENT_TOKEN() or NOW() >= f.loadDeadline:
        FINISH_SOURCE(f,ERROR(RETRY_OR_CLOSED)); return Future(w)
    HOST_LOAD(key, completion=(outcome => FINISH_SOURCE(f,LOADED(outcome))))
    return Future(w)

TRY_ATTACH_WAITER(f):                            // 已持有 stripe gate
    drop registry Refs of TAKEN/CANCELLED waiters within fixed waiterSlots
    i = first empty slot in f.waiterSlots
    if none: return OVERLOADED
    bundle = TRY_BUNDLE(global waiter permit, waiter pool slot)
    if failed: return OVERLOADED
    w = initialize Waiter(WAITING,bundle,deadline=logical request deadline)
    f.waiterSlots[i] = Ref(w)
    return w                                    // 旧 Work 快照的 Ref 未结束时，旧池槽不能复用

FINISH_SOURCE(f,outcome):                         // 真正 I/O 完成，或 leader 确认没有启动 I/O
    attach library NOW() as source completion time; retain immutable kind/metadata
    if not f.sourceOutcome.COMPLETE_ONCE(outcome): return DUPLICATE_COMPLETION
    RETURN_ONCE(f.ioReceipt)                      // timeout 路径不能执行此归还
    f.sourcePending.store(false)
    SIGNAL_FLIGHT_PUMP(f)
    TRY_PUMP_FLIGHT(f)                            // 失败由专用完成重试 / 定时触发继续

TRY_PUMP_FLIGHT(f):
    if CAS(f.completionOwner,false,true) failed: return
    f.pumpNeeded.exchange(false)                 // 领取已有通知；执行期间的新通知继续保留
    repeat at most PUMP_STEPS_PER_RUN:
        if f.work == NONE:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            PICK_FLIGHT_WORK_UNDER_GATE(f)        // 下方定义；最多建立一个固定工作记录
            unlock
            if f.work == NONE: break
        w = f.work
        if w.stage == NOTIFY:
            for next at most NOTIFY_BATCH registered waiters:
                if w.inlineError exists: READY_INLINE_ERROR(waiter,w.inlineError)
                else: BIND_READY(waiter,f.delivery)
            if all captured waiters processed: w.stage=w.afterNotify
        else if w.stage == CHECK_LATE_SOURCE:
            WORK_CHECK_LATE_SOURCE_STEP(f,w)
        else if w.stage == PREPARE_FILL:
            w.candidate = TRY_PREPARE_CANDIDATE(f,f.delivery,w.metadata,w.reuseDeadline)
            w.stage = SET_TERMINAL if candidate failed else ATTACH_CELL
        else if w.stage == ATTACH_CELL:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            f.fillCell = w.candidate.cell         // 完成关系先于入队建立
            unlock
            w.stage=ENQUEUE
        else if w.stage == ENQUEUE:
            if not TRY_PUSH(shard.fillQueue,w.candidate):
                REJECT_UNENQUEUED_CANDIDATE(w.candidate,QUEUE_FULL)
            else:
                MOVE candidate ownership to queue; SIGNAL_MAINTENANCE(shard)
            w.stage=SET_PENDING_OR_TERMINAL       // 即使维护者已极速完成，也不会覆盖其终态
        else if w.stage in {SET_TERMINAL,SET_PENDING_OR_TERMINAL}:
            if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: break
            terminal = w.stage==SET_TERMINAL or f.fillCell==NONE or f.fillCell terminal
            f.phase = TERMINAL if terminal else COMPLETED_PENDING
            f.reuseDeadline=w.reuseDeadline
            if terminal: CLEAR_BRIDGE_REF(f)
            f.work=NONE
            REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f)
            unlock
        else if w.stage == DISPOSE_LATE:
            MOVE late source outcome and response receipt to preReserved cleanup job
            mark job READY; w.stage=SET_TERMINAL
    needRetry = f.work != NONE or atomic mailbox/fill flags need handling
    if needRetry: f.pumpNeeded=true              // 仍持有 completionOwner 时读取 Work
    f.completionOwner=false
    // 释放后不读取另一执行者可能修改的 Work / phase；固定周期扫描补偿并发信号
    if f.pumpNeeded: schedule bounded completion retry, or wait periodic tick

PICK_FLIGHT_WORK_UNDER_GATE(f):
    require f.work==NONE
    now=NOW()
    if f.phase==LOADING and (now>=f.loadDeadline or cache.state!=OPEN):
        f.phase=TIMED_OUT_IO_RUNNING
        f.work=Work(NOTIFY, inlineError=TIMEOUT_OR_CLOSED,
                    capturedWaiters=fixed snapshot, afterNotify=CHECK_LATE_SOURCE)
        // CHECK_LATE_SOURCE：若 source 仍未完成，清除 work 但保留 orphan 状态；
        // 已完成则取邮箱一次并转 DISPOSE_LATE，无 payload 时释放空 response 预留。
        return
    if f.sourceOutcome has unconsumed result:
        source=TAKE_SOURCE_ONCE(f.sourceOutcome)
        if f.phase==TIMED_OUT_IO_RUNNING:
            f.work=Work(DISPOSE_LATE,source); return
        if source is ERROR without retained payload:
            RELEASE_EMPTY_RESPONSE_RESERVATION(f)
            f.phase=COMPLETING                    // 快照后不再允许新 waiter 加入，避免漏通知
            f.work=Work(NOTIFY,inlineError=source.error,
                        capturedWaiters=fixed snapshot,afterNotify=SET_TERMINAL)
            return
        sourceKind=source.kind                   // MOVE 前保存标量判别，不再访问已移走的 source
        f.delivery=BUILD_DELIVERY_IN_RESERVED_SLOT(MOVE(source),MOVE(f.responseReservation))
        f.phase=COMPLETING                        // 新 join 短暂 BUSY，原 waiters 先交付
        if sourceKind in {CACHED,ERROR_WITH_PAYLOAD}:
            f.work=Work(NOTIFY,capturedWaiters=fixed snapshot,
                        afterNotify=SET_TERMINAL,reuseDeadline=0)
            return
        metadata=f.delivery.outcome.cacheMetadata // 有限只读字段，无额外 payload 持有
        deadline=min(CALCULATE_HARD_DEADLINE(f,metadata),
                     metadata.realCompletedAt+handoffTimeout)
        f.work=Work(NOTIFY,capturedWaiters=fixed snapshot,
                    afterNotify=PREPARE_FILL,metadata,reuseDeadline=deadline)
        return
    if f.phase==COMPLETED_PENDING:
        if f.fillCell terminal:
            f.phase=TERMINAL; CLEAR_BRIDGE_REF(f)
        else if now>=f.reuseDeadline or f.token!=CURRENT_TOKEN() or cache.state!=OPEN:
            CANCEL_FILL_CELL(f.fillCell,EXPIRED_OR_STALE)
            CLEAR_BRIDGE_REF(f)
            if f.fillCell terminal: f.phase=TERMINAL
            // COMMITTING 不被超时方终结；保持记录，同身份 join 返回 BUSY
    REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f)

WORK_CHECK_LATE_SOURCE_STEP(f,w):
    if f.sourcePending:
        f.work=NONE                              // 等真实完成邮箱；仍保留 I/O/response 责任
    else:
        source=TAKE_SOURCE_ONCE(f.sourceOutcome)
        if source has payload: w.source=source; w.stage=DISPOSE_LATE
        else: RELEASE_EMPTY_RESPONSE_RESERVATION(f); w.stage=SET_TERMINAL

REMOVE_FLIGHT_IF_SETTLED_UNDER_GATE(f):
    if f.phase!=TERMINAL or f.sourcePending or f.work!=NONE: return
    if f.fillCell exists and not terminal: return
    CLEAR_BRIDGE_REF(f)
    f.fillCell=NONE                               // 主动拆除 Flight <-> FillCell 引用环
    remove stripe registry slot only if it still names this flightId
    // 已就绪 waiter / 回调 Ref 仍可持有 Flight；池槽和记录额度等最后一个 Ref 结束再归还

CLEAR_BRIDGE_REF(f):
    d=MOVE(f.delivery)
    if d exists: DROP_INTERNAL_REF(d)             // 只更新状态，不析构 payload

TRY_PREPARE_CANDIDATE(f,d,metadata,deadline):
    if cache.state != OPEN or f.token != CURRENT_TOKEN(): return STALE
    if NOW() >= deadline or not metadata.cacheable: return INELIGIBLE
    if GATE_READ(shard,key,NOW()) is UNKNOWN or < Tin[namespace]: return NOT_HOT
    b = BOUNDED_MEASURE_FULL_ENTRY(key,d.outcome.value) // gate 外；包含 key、容量、元数据
    if b > maxEntryBytes or measure invalid: return TOO_LARGE
    bundle = TRY_BUNDLE(pending bytes=b, candidate/pool slot,
                        pending cleanup job, FillCell/pool slot)
    if failed: return RESOURCE_LIMIT
    e = TRY_ALLOCATE_UNPUBLISHED_ENTRY_WITHIN_RESERVATION()
    if allocation failed:
        dispose partial allocation through reserved job; return ALLOCATION_FAILED
    e.key=bounded key copy; e.keyHash=HASH(key)
    e.namespaceId=f.namespaceId; e.namespaceGen=f.token.namespaceGen
    e.entryId=UNASSIGNED; e.valueRef=immutable shared reference to d.outcome.value
    e.hardDeadline=CALCULATE_HARD_DEADLINE(f,metadata); e.billedBytes=b
    e.costClass=metadata.validatedCostClass; e.domainLifetimeRef=shard lifetime reference
    e.readable=false; e.visited=false; e.ownedPins=0
    e.heat={epoch=floor(NOW()/heatDelta),current=0,previous=0}
    initialize all policy/timer links to NONE
    cell = FillCell(QUEUED,Ref(f))
    return FillCandidate(e,cell,f.token,f.expectedEntryId,deadline,bundle)

BIND_READY(w,d):
    acquire one internal delivery Ref
    if CAS(w.state,WAITING,READY(d)) fails: DROP_INTERNAL_REF(d); return
    MARK_READY(w)                                 // READY 转移将该 Ref 交给 waiter

READY_INLINE_ERROR(w,error):
    if CAS(w.state,WAITING,READY_INLINE(bounded error code)) succeeds:
        MARK_READY(w)

TAKE_RESULT(w):
    if CAS_OWN_READY_VALUE(w.state,READY_INLINE(error),TAKEN) succeeds:
        RETURN_ONCE(w.slotReceipt); return ERROR(error)
    if CAS_OWN_READY_VALUE(w.state,READY(d),TAKEN) fails: return NOT_READY_OR_CANCELLED
    result = TRANSFER_OR_CLONE_IMMUTABLE_VALUE_TO_HOST(d.outcome)
    DROP_INTERNAL_REF(d)
    RETURN_ONCE(w.slotReceipt)
    return result

CANCEL_WAITER(w):
    old = ATOMIC_TRANSITION_TO_CANCELLED_IF_WAITING_OR_READY(w.state)
    if old was READY(d): DROP_INTERNAL_REF(d)
    if old was WAITING or READY or READY_INLINE: RETURN_ONCE(w.slotReceipt)
    // TAKEN 不再归还；取消不取消其他 waiter，也不提前归还真实 I/O 额度

DROP_INTERNAL_REF(d):
    if d.internalRefs.fetch_sub(1) == 1:
        mark d.reservedCleanupJob READY           // 不在调用线程析构任意 value

EXPIRE_WAITERS_STEP(f,quota):
    if TRY_GATE(f.stripe.gate,FLIGHT_RETRIES) failed: return RETRY_LATER
    ws = next bounded batch of Ref<Waiter> from f's fixed waiter slots
    unlock
    for w in ws:
        if NOW() >= w.deadline: READY_INLINE_ERROR(w,TIMEOUT)
    // 只改变仍 WAITING 的结果；READY 成功结果不被事后超时撤销
```

Work 是 Flight 预留工作槽中的带标签记录：保存 stage、afterNotify、等待者扫描位置、只读结果元数据、reuseDeadline（缺省为 0）、candidate 或迟到 source 的独占持有。工作阶段只由 completionOwner 修改；取得 gate 失败保留当前阶段并让出。重入从该阶段继续，不能重新消费邮箱、重复通知同一等待者或再次创建候选。完成邮箱、工作槽和周期性扫描使这条路径不依赖普通填充队列是否有空位。带 payload 的错误走 Delivery；只有没有保留对象的有界错误码能走 READY_INLINE 并释放空响应预留。

`CURRENT_TOKEN` 返回当前命名空间代际与分片 fence；注册使用上面的稳定双读，提交由单维护者复核，命名空间并发推进仍由读取代际检查兜住。`CALCULATE_HARD_DEADLINE` 取保守有效性基准加 TTL，业务声明的更早截止时间只会缩短它；抖动只做缩短，结果大小 / CPU 元数据不合法时按契约拒绝。

Delivery 与 candidate 可以零拷贝共享底层不可变结果，但**响应保留和缓存保留分别由 M_response 与 M_pending 覆盖**。completed_pending 读取 Delivery，不解引用维护者正在移动所有权的 preparedEntry。清除 bridge 只减少一个内部 Ref；慢 waiter 尚未取走时响应仍记账。响应真实释放或移交后才结算，Flight 从索引摘除不等于其池记录可立即复用。

<a id="ahkc-a7"></a>

### A.7 失效、固定槽冷却扫描与层级时间轮

**参考实现用固定索引槽游标执行冷却，保证扫描进展不被队列持续插入改变；SIEVE hand 仍只服务受害者选择。** 读取过期判断独立于以下维护工作。

```text
SUBMIT_CONTROL(cache,operation):
    if cache.state != OPEN: return CLOSED
    r = TRY_BUNDLE(control record, reserved completion slot)
    if failed: return BUSY                       // 没有接受，不产生待生效承诺
    queue = fixed namespace or shard control queue selected by operation
    if not TRY_PUSH(queue,Control(operation,r)):
        RETURN_ONCE(r); return BUSY
    SIGNAL_CONTROL_OWNER(queue)
    return ACCEPTED(completionHandle)            // 只表示已接收，效果以完成句柄为准

COMPLETE_CONTROL_ONCE(control,result):
    publish result by single terminal CAS in preReserved completion slot
    notify host readiness without inline callback
    // 记录槽在 owner 与 completionHandle 的 Ref 都结束后归还；不依赖普通填充队列

INVALIDATE_KEY_IN_OWNER(control):
    shard.fillFence = CHECKED_NEXT(shard.fillFence)
    e = INDEX_FIND_IN_OWNER(index,control.completeKey)
    if e found:
        e.readable.store(false)                  // 先完成逻辑效果
        TRY_RETIRE(e,controlCleanupQuota)         // 无退休额度也不影响逻辑确认
    COMPLETE_CONTROL_ONCE(control,SUCCESS)

INVALIDATE_NAMESPACE_IN_CONTROL_OWNER(control):
    namespace.gen = CHECKED_NEXT(namespace.gen)   // 命名空间失效的线性化点
    mark namespace cleanup pending in fixed namespace bitset
    COMPLETE_CONTROL_ONCE(control,SUCCESS)        // 不等所有分片扫描

COLD_SWEEP(shard,quota):
    repeat at most quota.indexSlots times:
        i = shard.coldCursor
        shard.coldCursor = (i+1) mod index.slotCount
        e = index.slots[i]
        if e in {EMPTY,TOMBSTONE}: continue
        t = NOW()
        if not e.readable or e.namespaceGen != namespace.gen or t >= e.hardDeadline:
            e.readable.store(false); TRY_RETIRE(e,quota); continue
        if t < e.graceUntil: continue
        h = HEAT_READ(e.heat,t)
        if h == UNKNOWN: e.lowSince=NONE; continue
        if h >= Tout[e.namespace]: e.lowSince = NONE; continue
        if e.lowSince == NONE: e.lowSince=t; continue
        if t-e.lowSince >= coldFor:
            h2 = HEAT_READ(e.heat,NOW())
            if h2 == UNKNOWN: e.lowSince=NONE; continue
            if h2 < Tout[e.namespace]:
                e.readable.store(false); TRY_RETIRE(e,quota)
            else if h2 >= Tout[e.namespace]: e.lowSince=NONE

TRY_RETIRE(e,quota):
    if quota cannot pay one remove: return false
    lease = TRY_BUNDLE(retired bytes=e.billedBytes, retired record slot, cleanup job slot)
    if failed: return false                       // 索引 / resident 账目仍保留
    INDEX_REMOVE(e)
    UNLINK_POLICY_AND_TIMER_FIX_HAND(e)
    residentCount -= 1; residentBytes -= e.billedBytes
    TRANSFER_RESIDENT_TO_RETIRED(e,lease)
    REGISTER_RETIRED(e,epoch.current)
    consume one removal quota
    return true

WHEEL_ADD(e):
    require candidate already passed wheel capacity/range check before COMMITTING
    T = ceil(e.hardDeadline / wheelTick)
    base = wheel.tickInProgress if present else wheel.cursorTick
    delta = max(1,T-base)
    level = smallest ell in [0,L-1] with delta < K^(ell+1)
    slot = floor(T/K^level) mod K
    bucket = wheel.buckets[level][slot]
    if bucket.epoch != wheel.epoch: reset bucket head/tail; bucket.epoch=wheel.epoch
    append e to bucket; write e.timerEpoch/level/slot/links

WHEEL_REMOVE(e):
    if e.timerEpoch == wheel.epoch and e has timer member:
        unlink e using its bucket and intrusive links
    clear e timer membership                     // 旧 epoch 的残留指针不可用于改链

START_WHEEL_REBASE(targetTick):
    wheel.epoch = CHECKED_NEXT(wheel.epoch)
    wheel.cursorTick = targetTick
    clear fixed pending tick/cascade task descriptors
    wheel.rebasing=true; wheel.rebaseCursor=0
    shard.pauseReasons.add(WHEEL_REBASE)
    // 不遍历清空全部节点；旧 timer 成员通过 epoch 逻辑作废

WHEEL_REBASE_STEP(quota):
    while quota permits one index slot and rebaseCursor < index.slotCount:
        e = index.slots[rebaseCursor++]
        if e in {EMPTY,TOMBSTONE}: continue
        WHEEL_REMOVE(e)                           // 旧 epoch 只清本条目成员标记
        if e invalid or expired or stale namespace:
            e.readable=false; TRY_RETIRE(e,quota)
        else: WHEEL_ADD(e)
    if rebaseCursor == index.slotCount:
        wheel.rebasing=false
        shard.pauseReasons.remove(WHEEL_REBASE)   // 不清除其他暂停原因

ADVANCE_WHEEL(now,quota):
    target = floor(now/wheelTick)
    if not wheel.rebasing and target-wheel.cursorTick > maxWheelLag:
        START_WHEEL_REBASE(target)
    if wheel.rebasing: WHEEL_REBASE_STEP(quota); return
    while quota permits a tick/task/item:
        if no tick task in progress:
            if wheel.cursorTick >= target: return
            k = wheel.cursorTick+1
            make fixed tasks for each ell with k mod K^ell == 0,
                ordered highest ell down to 0; each task names (wheelEpoch,ell,slot)
            wheel.tickInProgress=k
        task = first unfinished task
        if task bucket epoch stale or bucket empty:
            mark task done
        else:
            e = remove first timer member from bucket
            if e invalid or namespace stale or NOW() >= e.hardDeadline:
                e.readable=false; TRY_RETIRE(e,quota)
            else: WHEEL_ADD(e)                    // 降层；精确 TTL 防止提前失效
        if all tasks for k done:
            wheel.cursorTick=k; clear in-progress tasks
```

时间轮总跨度必须大于 `maxTTL + maxWheelLag×wheelTick + 舍入余量`；重建扫描也有进展预算，期间停止新填充，控制、读取和 Flight 响应继续。重建过慢而再度超出允许滞后时重复进入受限恢复并暴露维护能力不足。所有 due/cascade 任务只保存固定桶身份和 epoch，不保存可能失效的裸链表游标。删除 timer 后退休额度不足的失效条目，由独立 coldCursor 再次尝试摘除。

<a id="ahkc-a8"></a>

### A.8 epoch 保护、拥有型句柄与实际回收

**PIN 必须先宣布保护，再读索引；物理释放同时等待 epoch 安全与拥有型计数归零。** 下列顺序一致参考协议用两次 epoch 推进分隔旧读者，不用“看起来当前没人读”替代安全条件。

```text
PIN(domain):
    slot = TAKE_PREASSIGNED_GUARD_SLOT_FROM_CONTEXT(domain)
    if no slot: return BUSY
    repeat at most PIN_RETRIES:
        g = domain.epoch.load_SC()
        slot.store_SC(ACTIVE(g))
        if domain.epoch.load_SC() == g: return Guard(domain,slot,g)
        slot.store_SC(INACTIVE)
    RETURN_CONTEXT_GUARD_SLOT(slot)
    return BUSY                                  // slot 仍为 INACTIVE，不泄漏嵌套保护额度

UNPIN(guard):
    guard.slot.store_SC(INACTIVE)
    RETURN_CONTEXT_GUARD_SLOT(guard.slot)          // 不运行回收扫描或析构

CLONE_CACHE_HANDLE(handle):
    require an existing owning handle remains alive during clone
    if bounded non-overflowing increment of handle.entry.ownedPins fails: return BUSY
    return another owning handle

DROP_CACHE_HANDLE(handle):
    handle.entry.ownedPins.fetch_sub_SC(1)
    // 退休记录本来就在有界池内；不依赖另一个可能满的通知队列才能回收

ADVANCE_EPOCH_STEP(domain,quota):
    if no scan active:
        scanEpoch=domain.epoch; advanceScanCursor=0; scanBlocked=false
    while quota permits one reader slot and cursor < readerSlotsPerShard:
        s = domain.slots[cursor++].load_SC()
        if s is ACTIVE and s.epoch < scanEpoch: scanBlocked=true
    if cursor == readerSlotsPerShard:
        if not scanBlocked and domain.epoch == scanEpoch:
            domain.epoch.store_SC(CHECKED_NEXT(scanEpoch))
        clear scan state                         // 每次完整扫描最多推进一个 epoch

REGISTER_RETIRED(e,g):
    r = preReservedRetiredRecord
    r.entry=e; r.entryId=e.entryId; r.retireEpoch=g
    r.retiredReceipt=retired receipt; r.state=WAIT_SAFE
    keep r in fixed retired pool until disposal really completes

RECLAIM_STEP(quota):
    ADVANCE_EPOCH_STEP(epoch,quota.readerScan)
    for next fixed retired records within quota.recordScan:
        if r.state == WAIT_SAFE:
            if epoch.current < r.retireEpoch+2: continue
            if r.entry.ownedPins.load_SC() != 0: continue
            if quota cannot pay a disposal handoff: continue
            // 此时无 guard 能再从已摘除索引取得该 Entry，也无 owning handle 可复制
            if CAS(r.state,WAIT_SAFE,SUBMITTING):
                if not TRY_SUBMIT(r.reservedCleanupJob(DISPOSE_ENTRY_AND_MARK_DONE,r)):
                    CAS(r.state,SUBMITTING,WAIT_SAFE)
                // 接受后提交方不再覆写阶段，任务可能已经完成
        if CAS(r.state,DONE,ACCOUNTED):
            RETURN_ONCE(r.retiredReceipt)
            drop pool owner's record Ref once; slots reusable only after all Ref lifetimes end

DISPOSE_ENTRY_AND_MARK_DONE(r):                    // 回收执行器；无索引/Flight/维护临界区
    assert CAS(r.state,SUBMITTING,DISPOSING) succeeds
    DISPOSE(r.entry.valueRef, r.entry.key, r.entry)
    r.state.store_release(DONE)                   // r 是独立预分配记录，不能嵌在刚释放的 Entry 内

DISPOSE_PENDING_USING_RESERVED_JOB(a):
    move a.preparedEntry to a.reservedCleanupJob if not already moved
    mark cleanup job READY
    // pendingReceipt 由该 job 持有，真实释放后 RETURN_ONCE；不能只因 cell 终态就归还

PROGRESS_CLEANUP_JOBS(pool,quota):                 // pending 与 Delivery 共用此阶段协议
    for next fixed records within quota.recordScan:
        if CAS(job.state,READY,SUBMITTING):
            if not TRY_SUBMIT(RUN_CLEANUP_JOB(job)):
                CAS(job.state,SUBMITTING,READY)
        if CAS(job.state,DONE,ACCOUNTED):
            RETURN_ONCE(job.receipt)
            drop pool owner Ref once; reuse slot only after remaining metadata Refs end

RUN_CLEANUP_JOB(job):
    assert CAS(job.state,SUBMITTING,RUNNING) succeeds
    if job.kind == PENDING_ENTRY: DISPOSE(owned entry/value/key)
    if job.kind == DELIVERY:
        assert job.delivery.internalRefs == 0; DISPOSE(job.delivery.outcome)
    if job.kind == LATE_SOURCE: DISPOSE(job.sourceOutcome)
    job.state.store_release(DONE)
```

回收任务可能非常快，提交方不能在它完成后又覆写 DONE。pending / Delivery cleanup job 也采用同一握手：`READY→SUBMITTING→RUNNING→DONE`，只有确定执行器拒绝时才把 SUBMITTING 恢复 READY；任务被接受后，终态只由任务发布。

待释放的 pending / Delivery 记录也由固定游标按预算扫描。所有分类统一由 RUN_CLEANUP_JOB 释放持有对象，并在 DONE→ACCOUNTED 后结算预算。其 job 已预留，调度拒绝只影响进展，不能让释放责任丢失；未真实结束前不退字节额度。固定索引 / 读者槽保留至缓存对象实际销毁，不能仅因业务对象已回收就释放仍被注册上下文或在途调用使用的固定结构。

<a id="ahkc-a9"></a>

### A.9 维护调度、额度转移与关闭

**维护使用唯一任务身份、分类配额和定时补偿；等待时间推进或读者退出的工作不能触发无休止的立即重调度。** Flight 完成工作使用独立的固定工作槽及就绪机制，不能排在可能已满的普通填充队列后。

```text
SIGNAL_FLIGHT_PUMP(f):
    f.pumpNeeded.store(true)
    mark f's preallocated completion record ready // 固定记录以位/状态聚合通知，无按事件分配
    request bounded dispatch from completion executor
    // 调度拒绝保留 ready，周期扫描重试；TRY_PUMP_FLIGHT 用 completionOwner 排除并行执行

SIGNAL_MAINTENANCE(shard):
    shard.pending.store(true)
    ENSURE_MAINTENANCE_TASK(shard)

ENSURE_MAINTENANCE_TASK(shard):
    if before shard.nextAllowedRunTime: return    // 周期触发负责到期重试
    ticket = next task identity from preallocated task record
    if not CAS(shard.ownerState,IDLE,QUEUED(ticket)): return
    if not TRY_SUBMIT(task=RUN_MAINTENANCE(shard,ticket)):
        CAS(shard.ownerState,QUEUED(ticket),IDLE)
        shard.pauseReasons.add(EXECUTOR_REJECTED)
        shard.pending=true                       // 不递归重试，不让调用方 drain

RUN_MAINTENANCE(shard,ticket):
    if not CAS(shard.ownerState,QUEUED(ticket),RUNNING(ticket)): return
    shard.pauseReasons.remove(EXECUTOR_REJECTED)
    quota = bounded round allowance
    classes = [CONTROL, LIFECYCLE, SAMPLES, FILL, RECLAIM]
    while quota permits one complete bounded work unit:
        cls = next class using saved resumeClass and per-class allowance
        if cls == CONTROL: process at most allocated accepted control operations
        if cls == LIFECYCLE:
            ADVANCE_WHEEL(NOW(),classQuota)
            COLD_SWEEP(shard,classQuota)           // 固定索引槽覆盖，空闲请求期仍推进
        if cls == SAMPLES: DRAIN_SAMPLES(shard,classQuota)
        if cls == FILL and pauseReasons empty:
            if fill rate tokens and classQuota allow complete worst-case decision:
                a = TRY_POP(fillQueue)
                if a exists: PROCESS_FILL(a)
        if cls == FILL and admission paused:
            discard bounded queued candidates with explicit terminal reasons
        if cls == RECLAIM:
            RECLAIM_STEP(classQuota)
            PROGRESS_CLEANUP_JOBS(pendingJobs,classQuota)
            PROGRESS_CLEANUP_JOBS(deliveryJobs,classQuota)
        save next resumeClass
        if all classes used allowance or soft time reached: break
    shard.pending=false
    CAS(shard.ownerState,RUNNING(ticket),IDLE)
    // 此后旧任务不再写驻留结构；重新检查覆盖 enqueue 与退出交错
    if shard.pending or IMMEDIATE_WORK_REMAINS(shard):
        if round CPU/rate allowance remains: ENSURE_MAINTENANCE_TASK(shard)
        else: retain pending and set nextAllowedRunTime for periodic refill

PERIODIC_TICK(cache):
    for bounded batch of fixed shards:
        refill time-based maintenance/fill allowances
        SIGNAL_MAINTENANCE(shard)
        for bounded Flight/Delivery pool records:
            EXPIRE_WAITERS_STEP(record.flight,waiterScanQuota)
            if mailbox, deadline, fill terminal, or retained continuation needs progress:
                SIGNAL_FLIGHT_PUMP(record.flight)
    schedule next bounded tick through host scheduler
```

`IMMEDIATE_WORK_REMAINS` 包含已发布队列输入、未完成的轮次 / 时间轮追赶和可运行的完成工作；仅有 future timer、等待 guard 的退休对象或未结束 I/O 时，等待周期触发 / 真实事件，不忙轮询。每个周期只处理固定批次分片 / 记录，保留全局游标；实际最大扫描间隔纳入 P、L 和加载超时服务等级。

额度转移只允许移动空闲额度，最多保留固定数量转移记录，每个 grant token 有唯一所有权。转移包含在途状态，不能只用非原子采集的分片数值推断额度丢失。

```text
DONOR_TAKE_IDLE_IN_OWNER(request):
    freeN = max(0,countQuota-residentCount-localKeepReserveN)
    freeB = max(0,byteQuota-residentBytes-localKeepReserveB)
    amount = componentwise min(request.wanted,(freeN,freeB))
    // 当前没有跨轮悬挂的提交；占用 owner 的提交必须先完成，不能捐出其资源
    countQuota -= amount.count; byteQuota -= amount.bytes
    complete request once with UNIQUE_GRANT_TOKEN(amount,owner=IN_TRANSIT)

COORDINATOR_RECEIVE_IDLE(grant):
    if transfer grant ownership IN_TRANSIT -> GLOBAL succeeds once:
        globalFreeCount += grant.count; globalFreeBytes += grant.bytes

COORDINATOR_ISSUE(receiver,amount):
    if globalFree insufficient: return
    globalFree -= amount
    token = UNIQUE_GRANT_TOKEN(amount,owner=IN_TRANSIT)
    if receiver control queue does not accept token:
        COORDINATOR_RECEIVE_IDLE(token)           // 归还一次，不能同时继续投递

RECEIVER_APPLY_IN_OWNER(token):
    if countQuota+token.count > index.countMax:
        return token to coordinator through its preallocated completion record
    else if transfer token ownership IN_TRANSIT -> RECEIVER succeeds once:
        countQuota += token.count; byteQuota += token.bytes
        acknowledge applied                      // 重复观察完成不能再次加额度
```

全局守恒账目为 `Σ分片额度 + 全局空闲 + 在途额度 = 配置额度`，分别适用于条数和字节。状态转移以 grant 所有权为准，执行顺序确保额度从未同时可被两方使用；多分片指标快照不提供跨原子字段的一次性事务视图。协调者不直接修改分片额度，不为本次候选同步驱逐其他分片条目。

```text
BEGIN_CLOSE(cache):
    if CAS(cache.state,OPEN,CLOSING) succeeds:
        stop accepting new request/registration/control admission
        mark fixed per-shard close work pending
    return CloseHandle with distinct stopped/work/reclaimed completion states

CLOSE_SHARD_STEP(shard,quota):
    finish or explicitly fail already accepted control operations within quota
    CANCEL_FILL_CELL(each queued candidate within quota,CLOSED)
    // COMMITTING 由原维护者结算；同分片 owner 不会并行运行另一个提交者
    for bounded occupied index slots:
        entry.readable=false; TRY_RETIRE(entry,quota)
    for bounded Flight records:
        stop new joins; make current waiters terminal according to close contract
        retain source mailbox, real I/O permit and response responsibility until source ends
        schedule completion pump / candidate cleanup
    keep epoch, timer and cleanup progress active

POLL_CLOSE(cache):
    stopped = no new admission can succeed
    workDone = all accepted controls terminal and all fill work settled
               and all real sources finished and all waiter outcomes settled
    reclaimed = workDone and no indexed/retired/pending/Delivery retention remains
                and no borrowed guard/cache owning handle/dependent callback Ref remains
    if stopped: report STOPPED_ADMISSION
    if workDone: report WORK_DRAINED
    if reclaimed: set cache.state=RECLAIMED; report RECLAIMED
    if user deadline reached before completion: report remaining categories and return
    // 任何超时都不等价于强制结束 I/O、归还保留预算或释放被引用内存
```

<a id="ahkc-a10"></a>

### A.10 伪代码级不变式与实现验证入口

每个实现模块至少需要保持以下断言；它们比函数名、位宽或容器选型更稳定：

```text
I1  对每个分片：residentCount <= countQuota <= index.countMax；residentBytes <= byteQuota。
I2  每项预算 available >= 0；used + reserved + available == limit；Receipt 最多归还一次。
I3  每个索引可读 Entry 已完整初始化；完整 key 匹配；读取时 namespaceGen、readable、TTL 有效。
I4  每个已发布 Entry 只出现在自己的固定探测范围；任何删除不把 TOMBSTONE 清成 EMPTY。
I5  O 与 Q 不相交；每个受害者只移除 / 计费一次；新旧身份不同；无未预留的不可逆提交。
I6  candidate 只有一个 COMMITTING 所有者；终态不可回退；超时与提交不能各自给出相反终态。
I7  同一 key+token 最多一个实际源操作；timeout 不归还尚未结束源操作的 I/O 凭证。
I8  每个 waiter 至多一次交付结果 / 归还槽位；就绪后由宿主 take 或 cancel 完成结算，业务交付不依赖填充。
I9  所有索引读取先 PIN；拥有型引用在保护期内取得；物理回收满足两代 epoch 与 ownedPins=0。
I10  active timer 成员至多一个；timer epoch/entryId 匹配；过期和代际失效不依赖 timer 进展。
I11  每个逻辑请求至多采样一次、记录一次 hit/miss 热度；内部查询不额外增加样本。
I12  所有接受的控制、完成邮箱、候选和回收责任均可从有界结构中追踪到终态。
```

验收顺序：先以确定性时钟和小容量结构验证 A.2～A.5 的状态 / 数学结果，再用可控交错测试 A.6～A.9 的所有权、完成与预算归还，最后测真实负载的成本和服务收益。即使顺序一致伪代码通过模型检查，也不自动代表换成弱内存序的实现已正确；每次优化都需保留相同的不变式与并发历史语义。

设计自检使用独立顺序模型核对了 6,000 次固定探测索引变更、3,500 组子集求解与独立组合枚举对照、250 组时间轮场景，以及热度边界、终态顺序和 epoch 生命周期示例，均通过。该结果只验证这些算法关系，不代表原生并发、弱内存序或性能目标已经通过验证。
