# 硬核拆解 Kylin 智能推荐引擎：从查询历史到模型自动演进 —— Mockup 试跑、Proposer 责任链与推荐审批闭环

> **作者**：Huang Sheng (mrhs121)  
> **日期**：2026-08-21  
> **分类**：`Apache Kylin` · `智能推荐` · `自动建模` · `Proposer` · `查询历史` · `源码剖析`

---

## 0. 导读：MOLAP 最后的"手工环节"如何被自动化？

前面的连载讲清了 Kylin 的两条主链路：查询期"SQL → 模型匹配 → 预计算加速"，构建期"模型定义 → Cuboid 物化"。但两条链路都有一个共同的前提——**得先有一个建好的模型**。而建模恰恰是 MOLAP 体系里最依赖人的环节：

- 维度选哪些？度量建哪些？聚合组怎么划？——需要建模者**预判**业务的查询模式；
- 业务在变，查询在变，模型却是静态的——上线三个月后，总有一批 SQL 掉进 Pushdown，也总有一批索引再没人查过，白白吃着构建资源。

Kylin 的**智能推荐引擎**（`rec-service` 模块，也是 Kylin 区别于其它 OLAP 引擎的独有能力）把这个环节反了过来：**不再让人预判查询，而是让真实发生的查询历史反向驱动模型演进**——自动从 SQL 推导出模型/维度/度量/索引的推荐项，人只做最后的审批。

本文拆解这条完整链路的源码实现：

```
查询历史(掉 Pushdown 的慢 SQL)
  → QueryHistoryAccelerateScheduler 定时捞取
  → ProposerJob (独立进程) 责任链推演
      SQLAnalysisProposer: Mockup 试跑, 拿到 OlapContext
      GreedyModelTreesBuilder: 按事实表聚类成 ModelTree
      ModelSelect/ModelOpt: 复用或演进模型 (CC/维度/度量)
      IndexPlanOpt/Shrink: 推导并归并索引
  → RawRecItem 落库 (INITIAL → RECOMMENDED)
  → 用户在 Web UI 审批 → APPLIED, 触发索引构建
```

核心源码索引：

| 组件 | 位置 | 职责 |
|---|---|---|
| `SmartMaster` / `AbstractContext` | `rec-service/.../rec/` | 推荐引擎门面与上下文族 |
| `ChainedProposer` + 8 个 Proposer | 同上 | 责任链主体 |
| `MockupQueryExecutor` | `rec/query/mockup/` | 无数据试跑，套取查询引擎的分析结果 |
| `GreedyModelTreesBuilder` | `rec/model/` | OlapContext → ModelTree 贪心聚类 |
| `IndexSuggester` | `rec/index/` | 维度/度量 → Layout 推导 |
| `ProposerJob` | `rec/ProposerJob.java` | 进程级隔离的推荐任务入口 |
| `RawRecItem` / `JdbcRawRecStore` | `core-metadata/.../recommendation/` | 推荐项实体与持久化 |
| `QueryHistoryAccelerateScheduler` | `rec-service/.../rest/service/` | 查询历史定时加速 |
| `OptRecService` / `OptRecApproveService` | 同上 | 推荐展示与审批落地 |

---

## 1. 总体设计：一个"内容可变"的责任链

推荐引擎的门面是 `SmartMaster`（`SmartMaster.java:38`），它薄得出奇——真正的骨架是 **Context + Proposer 责任链**：

```java
public void runWithContext(Consumer<AbstractContext> hook) {
    getContext().getProposers().execute();   // 责任链推演
    getContext().saveMetadata();             // 结果落库
    ...
    genDiagnoseInfo();                       // 每条 SQL 的 SUCCESS/PENDING/FAILED 诊断汇总
}
```

`AbstractContext`（`AbstractContext.java:74`）是贯穿全链的"黑板"：输入是 `sqlArray`，中间态是 `modelContexts`（每个待建/待改模型一个）与 `accelerateInfoMap`（每条 SQL 的加速结果），并由子类决定**用哪条责任链**（`createProposers()` 抽象方法）：

| Context 子类 | 使用场景 | 责任链构成 |
|---|---|---|
| `SmartContext` | 全自动模式（旧 Smart Mode） | SQLAnalysis → ModelSelect → ModelOpt → ModelInfoAdjust → ModelRename → IndexPlanSelect → IndexPlanOpt → IndexPlanShrink |
| `ModelCreateContext` | 白盒建模：从 SQL 生成新模型 | SQLAnalysis → ModelOpt → ...（**没有 ModelSelect**——不复用，直接创建） |
| `ModelReuseContext` | 半自动模式：只优化已有模型 | SQLAnalysis → ModelSelect → ViewModelSelect → ModelOpt → ...（多了模型视图匹配） |

同一批算法组件，通过 Context 多态拼出三种产品形态（自动建模/SQL 建模/查询加速）——这是整个模块最值得借鉴的架构决策：**算法与策略分离，策略由 Context 声明**。

`ChainedProposer.execute()`（`ChainedProposer.java:60`）逐个执行并在每步结束后打出 `SUCCESS x, PENDING y, FAILED z` 的统计——排查"为什么这条 SQL 没生成推荐"时，这段日志能直接定位挂在哪个 Proposer。

另一个贯穿性设计是**失败隔离**：每条 SQL 的命运独立记录在 `AccelerateInfo` 里（成功→关联的 Layout 列表；失败→异常栈；PENDING→人话提示，如 `No model matches the SQL`）。一批 500 条 SQL 里有 30 条解析失败，不影响其余 470 条产出推荐。

---

## 2. 第一步：Mockup 试跑 —— 复用查询引擎当"分析器"

推荐的第一个难题：**如何理解一条 SQL 需要什么**？维度、度量、Join 关系……这正是查询引擎第 2~3 篇里 Calcite + OlapContext 干的事。Kylin 没有为推荐重写一套 SQL 分析器，而是直接**把查询引擎当库调用**——`SQLAnalysisProposer`（`SQLAnalysisProposer.java:54`）通过 `MockupQueryExecutor` 对每条 SQL 做一次"假查询"：

```java
// MockupQueryExecutor: 真解析, 假执行
queryExec.executeQuery(QueryUtil.massageSql(queryParams));
...
// 无论成败, 收集本条 SQL 切分出的全部 OlapContext
record.noteOlapContexts();
```

细节都在"假"字上：

- Mockup 环境把下推引擎替换成 `MockupPushDownRunner`，**只走到计划分析层，不触发真正的 Spark 扫描**——推荐过程零数据 IO；
- SQL 先过与真实查询完全一致的 `QueryUtil.massageSql` 管道（连载一的 Query Massage），保证 BI 方言在推荐链路里同样被抹平；
- `NoRealizationFoundException`（没有模型可匹配）在这里**不算失败**——这恰恰是最需要推荐的 SQL；只有语法错误、表不存在等才记入失败，且 `SqlSyntaxAdvisor` 会把常见错误转译成给人看的修改建议。

试跑的产出就是查询引擎的核心中间产物——**`OlapContext`**（事实表、JoinsGraph、groupBy 列、过滤列、聚合函数五元组）。推荐引擎接下来的一切推导都基于它。**同一套代码既服务查询也服务推荐**，这保证了"推荐出的模型一定能被查询匹配"——因为两边对 SQL 的理解字节级一致。这是整个设计里最聪明的一步。

---

## 3. 第二步：GreedyModelTreesBuilder —— 从 OlapContext 到模型雏形

一批 SQL 试跑完，得到一堆散落的 OlapContext。哪些该归入同一个模型？`GreedyModelTreesBuilder`（`GreedyModelTreesBuilder.java:69`）做**贪心聚类**：

```
1. 按 (事实表, SQL Hint 中的模型优先级) 分组
     —— 事实表相同的查询才可能共享模型
2. 每组内 TreeBuilder 贪心合并 Join 图, 产出 ModelTree
3. 等价 ModelTree 去重合并 (OlapContext 并入已有树)
```

`ModelTree` 就是"模型的雏形"：根事实表 + 合并后的 Join 关系 + 归属的全部 OlapContext。合并 Join 图时有一个精妙的别名问题：不同 SQL 里同一张维表的别名千奇百怪（`d`、`dates`、`dt`...），怎么判断两个 Join 是"同一个位置"？`getUniqueTblAliasBasedOnPosInGraph`（`GreedyModelTreesBuilder.java:163`）的答案是**用 Join 图里从根到该表的路径作为规范别名**——同一张表如果通过相同的外键链被 Join 进来，无论 SQL 里叫什么，都会得到相同的层级别名（如 `LINEORDER_LO_ORDERDATE_DATES`），从而被正确合并；而同一张表通过两条不同外键链接入（角色扮演维度，如"下单日期"和"发货日期"都指向 DATES），会得到两个不同别名，正确地保留为两个 TableRef。

---

## 4. 第三步：模型层 Proposer —— 复用优先，演进兜底

### 4.1 ModelSelectProposer：能复用就绝不新建

`ModelSelectProposer`（`ModelSelectProposer.java:70`）拿每个 ModelTree 与项目下已有模型做匹配（本质是 JoinsGraph 子图判定，与查询期模型匹配同源）：

- 完全匹配 → 该 ModelTree 的推荐全部挂到已有模型上（半自动模式的常态）；
- 匹配不上 → `SmartContext` 下允许流转到新建模型；`ModelReuseContext` 下则记 PENDING（`NO_MODEL_MATCH_PENDING_MSG`），提示用户先建模——**半自动模式永远不会偷偷建模型**，这是产品边界。

### 4.2 ModelOptProposer：三个子 Proposer 的嵌套链

`ModelOptProposer` 内部又是一条链（`rec/model/` 包的 `ModelMaster`）：

**① ComputedColumnProposer**——SQL 里出现 `SUM(price * (1 - discount))` 这类表达式度量时，预计算无法直接物化（连载二讲过度量必须是原子列）。它把表达式收集为**计算列候选**（`collectLatentCCSuggestions`），用 Spark 对 CC 表达式做类型推演（`ComputedColumnEvalUtil`——不跑数据，只推导 Schema），并通过 `ComputedColumnUtil` 做跨模型同名/同表达式冲突检查。CC 建好后，后续维度/度量推导就能把 `price * (1 - discount)` 当普通列对待。

**② QueryScopeProposer**（`QueryScopeProposer.java:60`）——把 OlapContext 的需求"翻译"进模型定义：groupBy/过滤列 → 候选维度；聚合函数 → 候选度量（复用已有 measure，新度量分配递增 ID）；处理"同一列既当维度又被 `COUNT(DISTINCT)`"的 dimensionAsMeasure 纠结场景。

**③ JoinProposer**——把 ModelTree 中新出现的 Join 边合并进模型的 Join 树（`AppendJoinRule`），已有边保持不动——**模型演进是增量的、非破坏性的**。

### 4.3 防退化的三道闸

推荐系统最怕"越推越烂"。源码里能看到三道防线：`AntiFlatChecker`（不该打平的维表——比如拉链表——上的度量直接拒绝）、`ColExcludedChecker`（用户显式排除的列不进推荐）、以及后面 IndexSuggester 对超长维度组合的熔断。推荐引擎宁可 PENDING 也不输出一个会把构建拖垮的模型。

---

## 5. 第四步：索引层 Proposer —— 从查询形态到 Layout

模型定型后，`IndexPlanOptProposer` 驱动 `IndexSuggester`（`IndexSuggester.java:113`）为每个 OlapContext 推导物理索引：

- **聚合查询** → AggIndex：维度 = groupBy ∪ 过滤列（映射为模型列 ID），度量 = 聚合函数映射的 measure ID；维度排序参考过滤频率（过滤越频繁越靠前，利于 Parquet 谓词下推）；`suggestShardBy` 为高基数首维建议 shard-by 分桶——这些正是查询引擎第 4 篇 FilePruner 三层裁剪的"构建期铺垫"；
- **明细查询**（`SELECT *` / 无聚合）→ TableIndex，维度为投影列全集；
- 生成的候选 Layout 与已有 IndexPlan 比对：已存在 → 直接把 SQL 挂到该 Layout（`AccelerateInfo.relatedLayouts`）；不存在 → 产出新 Layout 推荐。

最后的 `IndexPlanShrinkProposer`（`IndexPlanShrinkProposer.java`，352 行）做**归并收缩**：本轮新增的多个 Layout 之间若存在包含关系（{A,B,C} 可回答 {A,B} 的查询且行数估算相近），合并为一个——防止 100 条相似 SQL 推出 100 个碎索引。这是推荐质量的关键一环：**推荐的价值不在多，而在少而准**。

---

## 6. 工程骨架：ProposerJob 进程隔离与 RawRecItem 状态机

### 6.1 为什么推荐要 fork 独立进程？

`ProposerJob.propose()`（`ProposerJob.java:100`）不在 Kylin 主进程里直接跑责任链，而是：

```
1. extractDumpResource: 项目/模型/IndexPlan/表 元数据打包到 tmp 目录
2. JobRunnerFactory 创建 Runner (默认 ForkBasedJobRunner)
3. fork 子进程执行 ProposerJob.main → 责任链推演 → ContextOutput 序列化到文件
4. 父进程 mergeResultIntoContext: 读回结果合并
```

动机与构建任务的 spark-submit 如出一辙（任务执行框架篇 4.1 节）：推荐要跑 Calcite 解析 + CC 类型推演，批量 SQL 下是 CPU/内存密集操作，**fork 隔离保证一批畸形 SQL 的解析风暴不会拖垮 Query/Job 主进程**；元数据快照同时保证推演基于一致性视图。`InMemoryJobRunner` 则用于 UT 与轻量场景——又是一处"策略可替换"。

### 6.2 RawRecItem：推荐项的五种类型与状态机

责任链的产物不是直接改模型，而是写入 `RawRecItem`（`RawRecItem.java:172`，存于专门的 JDBC 表）：

```
类型: COMPUTED_COLUMN(1) / DIMENSION(2) / MEASURE(3) / ADDITIONAL_LAYOUT(4) / REMOVAL_LAYOUT(5)
状态: INITIAL → RECOMMENDED → APPLIED
                     ↘ DISCARD    ↘ BROKEN (依赖的列/模型没了)
```

注意 `REMOVAL_LAYOUT`——推荐引擎不仅做加法还做减法：`TopRecsUpdateScheduler` 结合查询命中统计（`LayoutMetric`），把长期无人问津的索引推荐为"待删除"，这就是**索引生命周期闭环**：查询驱动生、无人查驱动死。

推荐项之间有依赖（Layout 依赖新维度，维度依赖新 CC），审批时由 `OptRecV2` 的引用图（`ref/` 包：`LayoutRef → DimensionRef/MeasureRef → CCRef`）保证级联一致——勾选一个 Layout 推荐，它依赖的 CC/维度/度量推荐被自动连带。

### 6.3 触发源：查询历史加速的定时闭环

全链路的起点是 `QueryHistoryAccelerateScheduler`（`QueryHistoryAccelerateScheduler.java:57`）：固定周期从查询历史表捞取新增查询（游标 offset 记录消费位点），经 `AccelerateRuleUtil` 按**加速规则**（FavoriteRule：耗时阈值、查询次数、提交用户/用户组白名单）过滤出"值得加速"的 SQL，喂给 `RawRecService.generateRawRecommendations` → `ProposerJob`。至此闭环完整：

```mermaid
flowchart LR
    QH["查询历史<br/>(含 Pushdown 慢查询)"] -->|定时捞取+规则过滤| PJ["ProposerJob<br/>(fork 进程责任链)"]
    PJ -->|"RawRecItem<br/>(RECOMMENDED)"| UI["Web UI 推荐列表"]
    UI -->|用户审批| APPLY["OptRecApproveService<br/>(UnitOfWork 事务改模型+IndexPlan)"]
    APPLY -->|新 Layout| BUILD["索引构建任务<br/>(JdbcJobScheduler 调度)"]
    BUILD -->|构建完成| QUERY["后续查询命中新索引"]
    QUERY -->|命中统计| QH
```

审批落地（`OptRecApproveService.approveRawRecItems`）在 `EnhancedUnitOfWork` 事务里把推荐项转写为真实的模型/IndexPlan 变更（元数据引擎篇的通道），随后触发的索引构建走作业调度篇的 job_lock 抢单——**推荐引擎是站在前面全部四个系列肩膀上的最顶层闭环**。

---

## 7. 生产使用与排障速查

| 现象 | 排查方向 |
|---|---|
| 查询历史里的慢 SQL 没生成推荐 | 依次检查：项目是否开启半自动模式；SQL 是否命中加速规则（FavoriteRule 的耗时/次数阈值）；`diagnosis log for auto-modeling` 日志段里该 SQL 是 PENDING 还是 FAILED |
| 推荐 PENDING: `No model matches the SQL` | 半自动模式不新建模型；SQL 的 Join 形态与现有模型不匹配（用查询引擎篇 3 的 JoinsGraph 规则核对），需先手工建模或放宽 partial match |
| 表达式度量没有推出 CC | 检查 `ComputedColumnProposer` 日志：类型推演失败（表达式含不支持函数）或与既有 CC 同名冲突；确认未开启"仅复用用户自定义 CC"限制 |
| 推荐的索引太碎/太多 | 关注 IndexPlanShrinkProposer 是否生效；调低单次加速的 SQL 批量；用 REMOVAL_LAYOUT 推荐定期清理低频索引 |
| ProposerJob 失败 `Failed to exec job` | 看 `$KYLIN_HOME/tmp/{jobId}` 下的子进程日志（uploadLogs 会归集）；常见为元数据快照缺表（模型 broken）或子进程内存不足 |
| 审批后查询仍未加速 | 审批只改元数据，需等索引构建任务完成（Job 页面确认）；历史 Segment 需要补建新索引（VacantIndexPruningRule 会跳过未构建的段） |

---

## 8. 总结：查询驱动的模型生命周期

把智能推荐放进全系列的版图里，Kylin 的模型生命周期就从"静态设计"变成了"动态演进"：

1. **理解查询**：复用查询引擎做 Mockup 试跑，推荐与查询对 SQL 的理解字节级一致——推出的模型必然可命中；
2. **推演变更**：Context 声明策略、Proposer 责任链执行算法，同一套组件拼出自动建模/SQL 建模/查询加速三种形态；从 CC 到维度度量到 Layout，推导链层层递进且有防退化闸门；
3. **闭环落地**：RawRecItem 状态机 + 引用图保证推荐可审计、可级联；查询命中统计反哺 REMOVAL_LAYOUT，索引有生有死；
4. **工程隔离**：fork 进程 + 元数据快照，推荐的计算风暴不伤主进程。

一句话收束：**查询引擎负责"用好模型"，构建引擎负责"造好模型"，推荐引擎负责"让模型跟着查询自己长"**——这是 Kylin 从"预计算工具"走向"自治数仓加速层"的关键一跃。

---

> **交叉阅读**：
> - Mockup 试跑复用的 SQL 解析与 OlapContext 切分 → [查询引擎 (二):OlapRel 算子体系](2026-08-18-kylin-query-engine-02-olap-rel-and-rules.md) / [(三):OlapContext 与模型匹配](2026-08-18-kylin-query-engine-03-olap-context-and-cbo.md)；
> - 推荐审批走的事务通道 → [元数据引擎:ResourceStore 与 UnitOfWork](2026-08-21-kylin-metadata-resourcestore-unitofwork-auditlog.md)；
> - 审批后触发的索引构建 → [作业调度:JdbcJobScheduler](2026-08-21-kylin-job-scheduler-jdbc-lock-leader-election.md) / [构建引擎:Segment Build 全链路](2026-08-20-kylin-spark-segment-build-pipeline.md)。
