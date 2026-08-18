# 硬核拆解 Kylin 查询引擎 (四)：跨越代数鸿沟 —— CalciteToSparkPlaner 双栈编译与物化消除

> **作者**：Huang Sheng (mrhs121)  
> **日期**：2026-08-18  
> **分类**：`Apache Kylin` · `Apache Spark` · `Calcite` · `Catalyst` · `编译器设计` · `源码剖析`

---

## 0. 导读与核心问题

在前面几篇中，我们见证了 Calcite 如何将一条原始 SQL 解析为 `OlapRel` 物理关系代数树，并通过 `OlapContext` 匹配出了最优的物理 Layout（Cuboid）。

但至此，所有的优化仍然停留在 **Calcite 的逻辑世界** 中。要让分布式计算霸主 **Apache Spark** 真正执行这个计划，系统必须跨越一道巨大的鸿沟：
- **Calcite 体系**：基于 `RelNode`（如 `OlapTableScan`、`OlapFilterRel`、`OlapAggregateRel`）与 `RexNode`（行表达式）；
- **Spark Catalyst 体系**：基于 `LogicalPlan`（如 `UnresolvedRelation`、`Filter`、`Aggregate`）与 `Expression`。

两个世界的数据结构、类型系统和表达式语义完全不同。承担这个跨引擎“编译器/转译器”重任的，正是 `CalciteToSparkPlaner.scala`。

本文将深入源码，彻底拆解 `CalciteToSparkPlaner` 的双栈后序遍历架构、物化 Join 剪枝消除、表达式转译与智能文件剪枝。

---

## 1. 架构总览：CalciteToSparkPlaner 的双栈计算模型

`CalciteToSparkPlaner` 继承了 Calcite 的 `RelVisitor`，采用了**深度优先后序遍历（Post-order Traversal）与双栈计算模型（Dual-Stack Model）**：

```mermaid
flowchart TD
    subgraph CalciteWorld ["Calcite OlapRel 关系代数树 (输入)"]
        Tree["OlapRel 物理树 (DFS 遍历)"]
    end

    subgraph DualStack ["双栈驱动调度中心"]
        MainStack["<b>stack: ArrayDeque[LogicalPlan]</b><br/>主计算栈 (存放子树生成的 Spark 算子)"]
        SetStack["<b>setOpStack: ArrayDeque[Int]</b><br/>集合操作栈 (记录 Union/Minus 入栈深度快照)"]
    end

    subgraph Translators ["算子转换工厂 (Transpiler Factories)"]
        F1["<b>TableScanPlan</b><br/>Parquet / Delta 读取 + 列重命名"]
        F2["<b>FilterPlan & SparderRexVisitor</b><br/>RexNode 递归转换为 Spark Expression"]
        F3["<b>AggregatePlan</b><br/>精准聚合短路 + 度量 UDAF 绑定"]
        F4["<b>JoinPlan</b><br/>运行时 Shuffle / Broadcast Join"]
        F5["<b>WindowPlan / SortPlan / LimitPlan</b><br/>开窗函数、排序与分页限制"]
    end

    subgraph SparkWorld ["Spark Catalyst 世界 (输出)"]
        RootLP["Spark Catalyst LogicalPlan"]
    end

    Tree --> DualStack
    DualStack --> Translators
    Translators --> MainStack
    MainStack -->|"getResult()"| RootLP
```

### 双栈设计的精妙之处
1. **主计算栈 `stack: ArrayDeque[LogicalPlan]`**：
   - 遍历子节点时，子节点生成的 Spark `LogicalPlan` 会被依次压入栈顶；
   - 当前父节点根据自身算子类型，从 `stack` 弹出对应数量的子算子（如 Filter 弹 1 个，Join 弹 2 个），组装生成新的父 `LogicalPlan` 并重新压回栈顶；
2. **集合操作栈 `setOpStack: ArrayDeque[Int]`**：
   - `UNION` 和 `MINUS` 是 N 元操作符（可包含任意多个子分支）；
   - 在进入 `OlapUnionRel` 时，`setOpStack.offerLast(stack.size())` 记录当前栈深快照；遍历完所有子节点后，通过快照差值一次性弹出该 Union 下属的全部子 `LogicalPlan`，天然支持复杂多层级嵌套集合运算。

---

## 2. 核心调度主循环源码逐行剖析

位于 `CalciteToSparkPlaner.scala:44-113` 的 `visit` 方法是整个编译器的调度引擎：

```scala
override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
  // 1. 若遇到集合算子，记录当前 stack 深度快照
  if (node.isInstanceOf[OlapUnionRel]) {
    unionLayer = unionLayer + 1
  }
  if (node.isInstanceOf[OlapUnionRel] || node.isInstanceOf[OlapMinusRel]) {
    setOpStack.offerLast(stack.size())
  }

  // 2. 核心优化：若为已物化的非运行时 Join，直接跳过遍历子节点（整棵子树被剪枝消除！）
  if (!(node.isInstanceOf[OlapJoinRel] && !node.asInstanceOf[OlapJoinRel].isRuntimeJoin) &&
    !(node.isInstanceOf[OlapNonEquiJoinRel] && !node.asInstanceOf[OlapNonEquiJoinRel].isRuntimeJoin)) {
    node.childrenAccept(this) // 后序遍历递归访问子节点
  }

  // 3. 模式匹配：消费 stack 中的子 Plan，构造当前节点的 Spark LogicalPlan 并压入 stack
  stack.offerLast(node match {
    case rel: OlapTableScan       => convertTableScan(rel)
    case rel: OlapFilterRel      => 
      logTime("filter") { FilterPlan.filter(stack.pollLast(), rel, dataContext) }
    case rel: OlapProjectRel     => 
      logTime("project") { ProjectPlan.select(stack.pollLast(), rel, dataContext) }
    case rel: OlapAggregateRel   => 
      logTime("agg") { AggregatePlan.agg(stack.pollLast(), rel) }
    case rel: OlapJoinRel        => convertJoinRel(rel)
    case rel: OlapNonEquiJoinRel => convertNonEquiJoinRel(rel)
    case rel: OlapSortRel        => 
      logTime("sort") { SortPlan.sort(stack.pollLast(), rel, dataContext) }
    case rel: OlapLimitRel       => 
      logTime("limit") { LimitPlan.limit(stack.pollLast(), rel, dataContext) }
    case rel: OlapWindowRel      => 
      logTime("window") { WindowPlan.window(stack.pollLast(), rel, dataContext) }
    case rel: OlapUnionRel       => 
      val size = setOpStack.pollLast()
      var unionBlocks = Range(0, stack.size() - size).map(_ => stack.pollLast())
      if (KylinConfig.getInstanceFromEnv.isCollectUnionInOrder) unionBlocks = unionBlocks.reverse
      logTime("union") { plan.UnionPlan.union(unionBlocks, rel, dataContext) }
    case rel: OlapMinusRel       => 
      val size = setOpStack.pollLast()
      logTime("minus") { plan.MinusPlan.minus(Range(0, stack.size() - size).map(_ => stack.pollLast()).reverse, rel, dataContext) }
    case rel: OlapValuesRel      => 
      logTime("values") { ValuesPlan.values(rel) }
    case rel: OlapModelViewRel   => 
      logTime("modelview") { stack.pollLast() } // 视图节点透明穿透
  })

  if (node.isInstanceOf[OlapUnionRel]) {
    unionLayer = unionLayer - 1
  }
}
```

---

## 3. 核心算子转译与黑科技

### 3.1 物化 Join 消除（Materialized Join Elimination）

这是 Kylin 在执行阶段实现数十倍加速的关键。

```mermaid
flowchart TD
    subgraph CalciteTree ["Calcite 视角: 逻辑多表关联"]
        JoinNode["OlapJoinRel (isRuntimeJoin = false)"]
        FactScan["OlapTableScan (事实表)"]
        DimScan["OlapTableScan (维表)"]
        JoinNode --> FactScan
        JoinNode --> DimScan
    end

    subgraph Optimization ["CalciteToSparkPlaner 转译处理"]
        Skip["1. 剪枝跳过子节点遍历 (childrenAccept 被绕开)<br/>2. 识别到该 Join 已物化在 Layout 中"]
    end

    subgraph SparkPlan ["Spark Catalyst 视角: 零 Shuffle 单表扫描"]
        SingleScan["<b>TableScanPlan.createOlapTable()</b><br/>扫描预计算 Layout Parquet 文件 (直接包含维表字段)"]
    end

    CalciteTree --> Optimization
    Optimization --> SparkPlan
```

在 `CalciteToSparkPlaner.scala:125-143` 中：
```scala
private def convertJoinRel(rel: OlapJoinRel): LogicalPlan = {
  if (!rel.isRuntimeJoin) {
    // 模型内 Join: 事实表与维表早已在构建期被打平成单一宽表
    val execFunc = rel.getContext.genExecFunc(rel)
    createTablePlan(rel, execFunc) // 直接生成单表扫描计划
  } else {
    // 跨模型运行时 Join: 从栈中弹出左右两路子计划，生成真正的 Spark Join
    val right = stack.pollLast()
    val left = stack.pollLast()
    plan.JoinPlan.join(Seq(left, right), rel)
  }
}
```
**效果**：在 Spark 端消除了昂贵的多表 Shuffle Hash Join，只剩下本地磁盘/内存的高速列存扫描！

---

### 3.2 表扫描计划工厂：`TableScanPlan`

位于 `TableScanPlan.scala:62-89`：
```scala
def createOlapTable(rel: OlapRel): LogicalPlan = logTime("table scan", debug = true) {
  val session: SparkSession = SparderEnv.getSparkSession
  val olapContext = rel.getContext
  val storage = olapContext.getStorageContext
  val batchSeg = storage.getBatchCandidate.getPrunedSegments
  val streamSeg = storage.getStreamCandidate.getPrunedSegments
  val realizations = olapContext.getRealization.getRealizations.asScala.toList

  val plans = realizations.map(_.asInstanceOf[NDataflow]).map(dataflow => {
    if (dataflow.isStreaming) {
      tableScan(rel, dataflow, olapContext, session, streamSeg, storage.getStreamCandidate)
    } else {
      tableScan(rel, dataflow, olapContext, session, batchSeg, storage.getBatchCandidate)
    }
  })

  // 若同时包含流批分段，自动以 Project + Union 进行合并
  if (plans.size == 1) plans.head else Project(plans.head.output, Union(plans))
}
```
- 读取已选定 Layout 的物理 Parquet / Delta 文件；
- 依据元数据进行精确列投影与别名重映射；
- 天然支持历史 Batch 段与实时 Streaming 段的流批无缝合并。

---

### 3.3 表达式转译器：`SparderRexVisitor`

在 `FilterPlan.scala` 中，Calcite 的行表达式（`RexNode`）必须翻译为 Spark Catalyst 表达式：

```scala
val visitor = new SparderRexVisitor(plan.output.map(_.name), rel.getInput.getRowType, dataContext)
val filterColumn = rel.getCondition.accept(visitor).asInstanceOf[Column]
Filter(filterColumn.expr, plan)
```

`SparderRexVisitor` 递归处理各类 Calcite 操作符映射：
- **基础比较符**：`=`, `>`, `<`, `<>`, `IS NULL`, `BETWEEN`, `IN`；
- **逻辑运算符**：`AND`, `OR`, `NOT`；
- **复杂函数**：`CASE WHEN` 转换为 Spark `when().otherwise()` 链式调用；`CAST` 转换为 Spark `cast()` 操作；
- **动态参数替换**：将 JDBC 预编译占位符（`?`）替换为 `DataContext` 中的实际常量值。

---

### 3.4 精确聚合短路（Exact Aggregation Shortcut）

在 `AggregatePlan.scala` 中：

```scala
if (rel.getContext != null && rel.getContext.isExactlyAggregate && !rel.getContext.isNeedToManyDerived) {
  // 精确命中 Layout 维度组合，跳过 Spark Aggregate 算子，直接转为 Project 投影！
  SparkOperation.project(projects, plan)
} else {
  // 需要做残余二次聚合，生成 Spark 原生 Aggregate 算子并绑定 UDAF
  SparkOperation.agg(groupList, aggList, plan)
}
```

若当前查询的 Group By 维度与底层物理 Layout 的维度完全一致（如都是 `[dt, city]`），则底层存储的数据本身就是唯一的。此时 Spark **无需再在内存中维护聚合哈希表（Agg Hash Map）**，直接转为轻量级投影输出，节约大量 CPU 算力。

---

## 4. 智能文件剪枝决策：`computeFilePruningMode`

针对 Delta Lake（V3）或超大规模 Segment 文件读取，`CalciteToSparkPlaner.scala:218-248` 设计了自适应文件剪枝策略：

```scala
private def computeFilePruningMode(): PruningMode = {
  val v3FileNumLimit = config.getV3FilePruningNumLimit
  val v3FileSizeLimit = config.getV3FilePruningSizeLimit

  val isLargeScan = fileNum >= v3FileNumLimit || totalSize > v3FileSizeLimit
  if (isLargeScan) {
    // 文件数或体积过大：下推到 Spark 集群 Executor 并行执行文件元数据裁剪 (CLUSTER 模式)
    FilePruningMode.CLUSTER
  } else {
    // 文件量较小：直接在 Driver 本地做单线程裁剪 (LOCAL 模式)，避免分布式 Task 的启动开销
    SparderEnv.getSparkSession.sparkContext.setLocalProperty("spark.databricks.delta.stats.skipping", "false")
    FilePruningMode.LOCAL
  }
}
```

---

## 5. 总结与下篇预告

`CalciteToSparkPlaner` 是连接 Calcite 代数世界与 Spark 计算世界的超级桥梁：
1. **极简双栈后序遍历**：消除了复杂的递归状态回溯，优雅支持 N 元集合操作；
2. **物理剪枝与算子折叠**：通过物化 Join 剪枝与精确聚合短路，在生成 Spark 逻辑计划时便剔除了大量冗余计算；
3. **全算子工厂化**：`TableScanPlan`、`AggregatePlan`、`FilterPlan` 各司其职，保证了表达式转换与文件扫描的极致性能。

---

> **下一篇预告**：
> 在 **《硬核拆解 Kylin 查询引擎 (五)：Sparder 运行时内核 —— 复杂度量 UDAF 与高性能执行机制》** 中，我们将把目光聚焦于 Spark 执行层：
> - `SparderEnv` 如何管理常驻 `SparkSession` 实现多租户亚秒级响应？
> - `RoaringBitmap` 精确去重与 `HLLC` 近似去重在 Spark UDAF 中的二进制合并实现；
> - 百分位数（`Percentile`）与 `TopN` 度量的高性能计算内幕。
