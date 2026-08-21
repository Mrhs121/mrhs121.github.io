# 硬核拆解 Kylin 查询引擎 (实战篇)：一条 SSB 查询的完整旅程 —— 从 SQL 文本到一次 Shuffle 出结果

> **作者**：Huang Sheng (mrhs121)  
> **日期**：2026-08-21  
> **分类**：`Apache Kylin` · `SSB` · `全链路走查` · `Calcite` · `Apache Spark` · `源码剖析`

---

## 0. 为什么要写这篇实战篇？

在前六篇连载中，我们按阶段纵向拆解了 Kylin 查询引擎的每一个组件：Query Massage、OlapRel 与 47 条优化规则、JoinsGraph 与模型匹配、CalciteToSparkPlaner、Sparder 运行时、Pushdown 兜底。

但组件视角有一个天然的问题：**读者很难在脑中"回放"一条真实 SQL 的完整旅程**——它在每个阶段进来时长什么样、出去时变成了什么样、每一步的决策依据是什么。

本文换一个视角：拿 SSB（Star Schema Benchmark）中最经典的 **Q2.1** 作为贯穿全文的主角，从 SQL 文本出发，一站不落地走完六大阶段，把前六篇的知识点在一条真实查询上"演一遍"。每一节都会标注对应的连载篇目，方便交叉阅读。

```sql
-- SSB Q2.1
SELECT SUM(LO_REVENUE) AS revenue, D_YEAR, P_BRAND
FROM LINEORDER
JOIN DATES    ON LO_ORDERDATE = D_DATEKEY
JOIN PART     ON LO_PARTKEY   = P_PARTKEY
JOIN SUPPLIER ON LO_SUPPKEY   = S_SUPPKEY
WHERE P_CATEGORY = 'MFGR#12' AND S_REGION = 'AMERICA'
GROUP BY D_YEAR, P_BRAND
ORDER BY D_YEAR, P_BRAND
```

这条查询在明细层面意味着：三张维表 Join 一张数十亿行的事实表，过滤后全量聚合。我们将看到 Kylin 如何把它变成**"扫一张几十万行的预聚合 Parquet 表 + 一次 Shuffle"**。

---

## 1. 前置设定：SSB 模型与 Index 布局

假设建模阶段已定义好如下 Model（这是后续一切匹配的"锚"）：

- **Join 树**（以 `LINEORDER` 为中心的星型拓扑）：
  - `LINEORDER INNER JOIN DATES    ON LO_ORDERDATE = D_DATEKEY`
  - `LINEORDER INNER JOIN PART     ON LO_PARTKEY   = P_PARTKEY`
  - `LINEORDER INNER JOIN SUPPLIER ON LO_SUPPKEY   = S_SUPPKEY`
  - `LINEORDER INNER JOIN CUSTOMER ON LO_CUSTKEY   = C_CUSTKEY`（本查询未用到）
- **维度与内部列 ID**（模型会为每个列分配数字 ID，Layout 的 Parquet 文件里列名就是这些 ID）：

| 列 | 内部列 ID |
|---|---|
| `D_YEAR` | 3 |
| `P_BRAND` | 7 |
| `P_CATEGORY` | 8 |
| `S_REGION` | 11 |
| `S_NATION` | 12 |
| `LO_REVENUE`（度量参数）| 15 |

- **聚合组展开后的部分 Layout（Cuboid）**：

| Layout ID | 维度集合 | 度量 | 预估行数 |
|---|---|---|---|
| `10001` | {3, 8, 11} | SUM(15), COUNT() | ~2 万 |
| `20001` | {3, 7, 8, 11} | SUM(15), COUNT() | ~70 万 |
| `30001` | {3, 7, 8, 11, 12} | SUM(15), COUNT() | ~1500 万 |
| `20000000001`（明细 Table Index）| 全列 | — | ~60 亿 |

构建完成后 HDFS 上的物理布局（每个 Segment × 每个 Layout 一个目录）：

```
/kylin/parquet/{model_id}/
├── seg_20260101_20260201/
│   ├── 10001/part-*.parquet
│   ├── 20001/part-*.parquet      ← 列名为 "3","7","8","11",SUM_15,COUNT_1
│   └── 30001/part-*.parquet
├── seg_20260201_20260301/
│   └── ...
```

关键认知：`20001` 目录里存的**不是明细**，而是 `(D_YEAR, P_BRAND, P_CATEGORY, S_REGION)` 粒度下**已经聚合好的** SUM 与 COUNT——构建期 `CuboidAggregator` 的产物（参见构建引擎篇）。

---

## 2. 阶段一 & 二：入口、鉴权与 Query Massage

> 对应 [第 1 篇：全生命周期总览](2026-08-18-kylin-query-engine-01-overview.md)

Q2.1 通过 JDBC 到达 `QueryExec` 后：

1. `QueryContext` 生成全局 Query ID，启动 `SQL_PARSE_AND_OPTIMIZE` 计时跨度；
2. ACL 校验：当前用户对 `LINEORDER` 等四张表有读权限，未配置行列级安全，直接放行；
3. 缓存搜索：假设首次执行，未命中；
4. **Query Massage**：Q2.1 是手写标准 SQL，`DefaultQueryTransformer` / `EscapeTransformer` 均无改动；若 REST 请求带了 `limit=500`，`appendLimitOffset` 会追加 `LIMIT 500`。本文假设无分页参数，SQL 文本原样进入下一阶段。

> 实战提示：BI 工具生成的 Q2.1 变体（如 Tableau 会写成 `{fn YEAR(...)}` 或包一层子查询）正是在这一步被抹平的。如果排查"手写 SQL 命中、BI SQL 不命中"的问题，先对比 massage 前后的 SQL 文本（`kylin.log` 中有输出）。

---

## 3. 阶段三：Calcite 解析与 OlapRel 转换

> 对应 [第 2 篇：OlapRel 算子体系与 47 条优化规则](2026-08-18-kylin-query-engine-02-olap-rel-and-rules.md)

### 3.1 初始逻辑计划

`SqlParser` → `SqlValidator` → `SqlToRelConverter` 之后，Q2.1 的 `NONE` 规约逻辑树：

```
LogicalSort(sort0=$1, sort1=$2)
└─ LogicalAggregate(group={D_YEAR, P_BRAND}, revenue=SUM(LO_REVENUE))
   └─ LogicalProject(D_YEAR, P_BRAND, LO_REVENUE)
      └─ LogicalFilter(P_CATEGORY='MFGR#12' AND S_REGION='AMERICA')
         └─ LogicalJoin(LO_SUPPKEY = S_SUPPKEY)
            ├─ LogicalJoin(LO_PARTKEY = P_PARTKEY)
            │  ├─ LogicalJoin(LO_ORDERDATE = D_DATEKEY)
            │  │  ├─ LogicalTableScan(LINEORDER)
            │  │  └─ LogicalTableScan(DATES)
            │  └─ LogicalTableScan(PART)
            └─ LogicalTableScan(SUPPLIER)
```

### 3.2 规则的实际触发情况

Q2.1 足够"干净"，47 条规则中真正起作用的是一小部分——这也是实战和百科的差别，**大多数规则是为不规整 SQL 准备的兜底**：

| 规则 | 是否触发 | 说明 |
|---|---|---|
| `OlapTableScanRule` 等 13 条 Converter | ✅ | 全部算子转为 `OLAP` 规约 |
| `OlapFilterJoinRule` | ✅ | `P_CATEGORY`/`S_REGION` 谓词穿透 Join 下推 |
| `RightJoinToLeftJoinRule` | ❌ | 没有 RIGHT JOIN |
| `SumBasicOperatorRule` | ❌ | `SUM(LO_REVENUE)` 已是原子列 |
| `ScalarSubqueryJoinRule` | ❌ | 无子查询 |

如果用户写的是 `SUM(LO_REVENUE * 1.1)` 或 `SUM(CAST(LO_REVENUE AS DOUBLE))`，则 `SumBasicOperatorRule` / `OlapSumCastTransposeRule` 会介入，把表达式还原成能命中预计算度量 `SUM(15)` 的原子形态——**没有这批规则，这些微小变形都会导致 Cube 失配掉入 Pushdown**。

优化完成后得到 `OlapRel` 物理树（结构同上，算子全部换成 `Olap*Rel`，顶端多一个 `OlapToEnumerableConverter` 桥接节点）。

---

## 4. 阶段四：OlapContext 切分与模型匹配

> 对应 [第 3 篇：JoinsGraph、OlapContext 与多级剪枝](2026-08-18-kylin-query-engine-03-olap-context-and-cbo.md)

### 4.1 Context 切分：一刀不切

`QueryContextCutter` 贪心首切：Q2.1 是标准"星型 Join + 单层聚合"，整棵树落入**一个** `OlapContext`，收集到：

```
OlapContext #0
  firstTableScan : LINEORDER
  joinsGraph     : LINEORDER ─INNER→ DATES / PART / SUPPLIER
  groupByColumns : {D_YEAR, P_BRAND}
  filterColumns  : {P_CATEGORY, S_REGION}
  aggregations   : [SUM(LO_REVENUE)]
  allColumns     : {D_YEAR, P_BRAND, P_CATEGORY, S_REGION, LO_REVENUE}
```

### 4.2 JoinsGraph 匹配：Partial Match 生效

查询图只有 3 条边，模型图有 4 条（多一条到 `CUSTOMER`）。`JoinsGraph.match()` 从中心事实表出发递归匹配出度边：

- 3 条查询边逐一在模型图中找到同构对应（表、FK=PK 列对、Join 类型全部一致）；
- 模型中剩余的 `LINEORDER → CUSTOMER` 边未被查询引用——若它是 LEFT JOIN，直接按 Partial Match 放行；若模型定义为 INNER JOIN（SSB 常见建模），则依赖"维表 PK 唯一且 FK 非空"的完整性假设或 `kylin.query.match-partial-inner-join-model` 配置放行。**这是生产上"查询明明合法却不命中模型"的高频原因**，排查时优先核对这条配置与模型 Join 类型。

匹配通过，产出 `tableAliasMap`，候选 Model 锁定。

### 4.3 Layout 裁决：三个候选，一次淘汰赛

查询需要的维度集合 $D_{query} = \{3, 7, 8, 11\}$（GROUP BY ∪ FILTER），度量需要 `SUM(15)`：

| Layout | 维度检查 | 度量检查 | 结论 |
|---|---|---|---|
| `10001` {3,8,11} | ❌ 缺 P_BRAND(7) | — | 淘汰 |
| `20001` {3,7,8,11} | ✅ 精确相等 | ✅ SUM(15) | **候选，代价最低** |
| `30001` {3,7,8,11,12} | ✅ 超集 | ✅ | 候选，行数多 20 倍 |
| 明细 Table Index | ✅ | 现算 | 候选，代价最高 |

CBO 打分按"行数 × 权重 + 字节数"排序：`20001`（~70 万行）完胜 `30001`（~1500 万行）与明细（60 亿行）。选中 `20001` 封装为 `NLayoutCandidate` 绑定进 `StorageContext`。

同时因为 $D_{query}$ 与 Layout 维度**完全相等**且不涉及一对多派生，`isExactlyAggregate` 本应为 true——但注意：查询只 GROUP BY 了 {3,7}，而 Layout 粒度是 {3,7,8,11}，二者不相等，所以 **`isExactlyAggregate = false`，Spark 端仍需二次聚合上卷**。只有当用户恰好 `GROUP BY D_YEAR, P_BRAND, P_CATEGORY, S_REGION` 时才会触发精确聚合短路。

### 4.4 Segment 剪枝：本查询剪不动

Q2.1 的 WHERE 中**没有分区时间列**（`LO_ORDERDATE`）的过滤，`SegmentPruningRule` 提取不到时间常量，所有已构建 Segment 都进入扫描列表。这正是 SSB Q2 系列与 Q1 系列（带 `D_YEAR = 1993` 时间过滤）在 Kylin 上表现差异的来源之一——**给高频查询的时间谓词设好模型分区列，Segment 剪枝才有用武之地**。

---

## 5. 阶段五：CalciteToSparkPlaner 双栈转译实录

> 对应 [第 4 篇：CalciteToSparkPlaner 与 FilePruner](2026-08-18-kylin-query-engine-04-calcite-to-spark-planer.md)

这是全链路"魔法浓度"最高的一步。我们逐步记录主计算栈 `stack` 的演变：

改写后的 OlapRel 树（自顶向下）：

```
OlapSortRel
└─ OlapAggregateRel        (SUM(15) → 二次聚合)
   └─ OlapProjectRel
      └─ OlapFilterRel     (8='MFGR#12' AND 11='AMERICA')
         └─ OlapJoinRel    (isRuntimeJoin = false！)
            └─ ... 三层 Join 与 4 个 TableScan
```

后序遍历过程中的栈状态：

| 步骤 | 访问节点 | 动作 | 栈内容（左为栈顶）|
|---|---|---|---|
| 1 | `OlapJoinRel`(顶层) | `isRuntimeJoin=false` → **跳过整棵 Join 子树的遍历**，调用 `TableScanPlan.createOlapTable()` 生成对 Layout `20001` 的单表扫描 | `[Scan(20001)]` |
| 2 | `OlapFilterRel` | 弹出 1 个，`SparderRexVisitor` 把 RexNode 转成 `col("8")==="MFGR#12" && col("11")==="AMERICA"`，包 Filter 压回 | `[Filter(Scan)]` |
| 3 | `OlapProjectRel` | 弹 1 压 1 | `[Project(Filter(Scan))]` |
| 4 | `OlapAggregateRel` | `isExactlyAggregate=false` → 生成真正的 `Aggregate(groupBy(3,7), sum(SUM_15))` | `[Agg(...)]` |
| 5 | `OlapSortRel` | 弹 1 压 1 | `[Sort(Agg(...))]` |

第 1 步就是**物化 Join 消除**：Calcite 视角里的 3 个 Join + 4 个 TableScan，在 Spark 视角里坍缩成一个 `LogicalRelation`——因为 Join 早在构建期做完了。**查询期 Q2.1 没有任何 Join。**

### FilePruner 的三层裁剪

构造 FileScan 时，`FilePruner` 对 `20001` 的文件列表做裁剪：

1. **Segment 层**：无时间过滤，全部保留（见 4.4）；
2. **Shard 层**：若建模时对 `20001` 设置了 `shard by P_BRAND`，本查询过滤列是 `P_CATEGORY`，对不上，无法裁剪；若 shard by `P_CATEGORY` 则能直接砍掉大部分文件——**shard-by 列要选高频等值过滤列**；
3. **Parquet 层**：`8='MFGR#12'`、`11='AMERICA'` 作为 PushedFilters 下推，靠 RowGroup min/max 与字典页跳过无关数据块。

---

## 6. 阶段六：Spark 物理执行与结果回传

> 对应 [第 5 篇：Sparder 运行时与复杂度量 UDAF](2026-08-18-kylin-query-engine-05-sparder-runtime-and-udaf.md)

Sparder（常驻 SparkSession）拿到 LogicalPlan 后，Catalyst 生成的物理计划（Spark UI / `explain` 可见）：

```
TakeOrderedAndProject(orderBy=[3 ASC, 7 ASC])
+- *(2) HashAggregate(keys=[3, 7], functions=[sum(SUM_15)])           ← final
   +- Exchange hashpartitioning(3, 7, 200)                            ← 唯一一次 Shuffle
      +- *(1) HashAggregate(keys=[3, 7], functions=[partial_sum(SUM_15)])
         +- *(1) ColumnarToRow
            +- FileScan parquet [3,7,8,11,SUM_15]
                 Location: .../seg_*/20001
                 PushedFilters: [EqualTo(8,MFGR#12), EqualTo(11,AMERICA)]
                 ReadSchema: struct<3:int,7:string,8:string,11:string,SUM_15:decimal>
```

几个值得注意的点：

- **partial + final 两段聚合**：partial 在扫描节点本地把 70 万行先聚成几千行，Shuffle 的数据量极小；
- **`sum(SUM_15)` 而非 `sum(LO_REVENUE)`**：输入已是预聚合值，这里是"二次聚合"语义（如果度量是精确去重，此处会换成 Bitmap UDAF 的 `Reuse` 合并，见第 5 篇）；
- Q2.1 没有复杂度量，不涉及 Bitmap/HLLC；结果集只有几百行，`ResultPlan` 流式收回 Driver，经 `QueryResultMasks`（无脱敏规则，直通）与 SchemaProcessor 类型还原后返回 JDBC。

### 账本：预计算到底省了什么

| 对比项 | Pushdown 查明细（Spark SQL 直查 Hive）| 命中 Layout 20001 |
|---|---|---|
| 扫描行数 | ~60 亿（LINEORDER）+ 三张维表 | ~70 万（预聚合行）|
| Join 次数 | 3 次（含一次大表 Shuffle Join）| **0 次** |
| Shuffle 次数 | ≥ 4 次 | **1 次**（且数据量为 KB 级）|
| 典型耗时 | 分钟级 | **亚秒级** |

---

## 7. 如何在生产上验证这条链路？

走查不能只停留在理论，以下是逐环节的验证手段：

1. **命中了哪个模型 / Layout**：
   - Web UI 查询历史（Query History）中每条查询会显示命中的 Model 与 Index ID（如 `20001`）；
   - `kylin.log` 中检索 Query ID，可看到 `RealizationChooser` 的候选评估与最终选择日志；
2. **为什么没命中**（掉 Pushdown 时）：日志中的 `NoRealizationFoundException` 附带原因（Join 不匹配 / 维度缺失 / 度量缺失），按第 4 节的匹配三部曲逐项核对；
3. **扫描量是否符合预期**：查询响应的 `scanned_rows` / `scanned_bytes` 指标，以及 Spark UI 中 FileScan 节点的 input size——如果远大于 Layout 预估行数，检查 Segment/Shard 剪枝是否失效；
4. **物理计划**：`EXPLAIN` 或 Spark UI SQL 页签，确认没有意料之外的 Join（若出现 Runtime Join，说明部分维表未被打平，触发了聚合下推场景，见第 6 篇 `tryEnhancedAggPushDown`）。

---

## 8. 总结：六阶段在一条查询上的投影

把 Q2.1 的旅程压缩成一张表：

| 阶段 | 输入 | 关键决策 | 输出 |
|---|---|---|---|
| 一/二 前置与 Massage | SQL 文本 | 无需改写 | 标准 SQL |
| 三 Calcite 优化 | SQL | 谓词下推；度量已是原子形态 | OlapRel 树 |
| 四 Model Match | OlapRel 树 | 单 Context；Partial Match 忽略 CUSTOMER；`20001` 胜出 | NLayoutCandidate |
| 五 Create Spark Plan | OlapRel + Layout | **Join 整体消除**；仍需二次聚合；三层文件裁剪 | Catalyst LogicalPlan |
| 六 Spark Execute | LogicalPlan | partial+final 聚合，1 次 Shuffle | 结果集 |

一句话收束：**Q2.1 的三表 Join + 60 亿行聚合，经过"Join 图匹配 → 维度集合包含 → 代价排序"锁定 Layout 20001，被改写成"扫一张 4 维预聚合表 + 过滤 + 二次 SUM"，一次 Shuffle 出结果。查询快的所有秘密，都藏在构建期。**

---

> **交叉阅读**：
> - 想知道 Layout `20001` 是如何被构建出来的 → [构建引擎：Spark Segment Build 全链路](2026-08-20-kylin-spark-segment-build-pipeline.md)；
> - 想知道构建顺序为什么是先 `30001` 后 `20001`（父子派生）→ [Adaptive Spanning Tree](2026-08-17-kylin-adaptive-spanning-tree.md)；
> - 如果度量是 `COUNT(DISTINCT LO_CUSTKEY)`，编码从哪来 → [全局字典 (上)](2026-08-19-kylin-global-dictionary-v2-distributed-bucket.md) / [(下)](2026-08-19-kylin-global-dictionary-v3-delta-acid.md)。
