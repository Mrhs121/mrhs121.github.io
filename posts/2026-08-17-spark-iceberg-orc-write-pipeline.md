# 源码全景拆解：Spark 如何通过 DataSource V2 与 File Format API 写入 Iceberg 表

> **作者**：Huang Sheng (mrhs121)  
> **日期**：2026-08-17  
> **分类**：`Apache Iceberg` · `Apache Spark` · `DataSource V2` · `存储引擎` · `源码剖析`  

---

## 0. 导读与背景问题

在现代数据湖仓架构中，**Apache Iceberg** 已经成为事实上的开放表格式标准。在 Iceberg 中，我们可以自由指定不同的底层数据存储格式（例如 Parquet、ORC、Avro，甚至是面向 AI 场景的 Lance、Vortex）。

当我们在 Spark 中执行一条非常朴素的写入语句时：
```sql
CREATE TABLE prod.db.orc_table (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES (
    'write.format.default' = 'orc'
);

INSERT INTO prod.db.orc_table VALUES (1, 'Alice', TIMESTAMP '2026-08-17 12:00:00');
```

表面上只是一句简单的 `INSERT`，但背后却跨越了 **Spark Catalyst 优化器**、**Spark DataSource V2 (DSv2) 扩展规范**、**Iceberg Format Model 架构**、**底层格式写入器（如 `orc-core`）** 以及 **Iceberg ACID 事务机制**。

**本文将结合 Spark 3.5 与 Iceberg 源码，从 Driver 端的物理计划生成，到 Executor 端的 6 阶微步骤对象装配，再到底层 `InternalRow` 转换为 `VectorizedRowBatch` 落盘并最终提交 Snapshot，进行全景源码级串透。**

---

## 1. 架构全景与调用时序

整个写入链路跨越了 **Driver 计算规划**、**分布式 Task 调度**、**Executor 物理落盘** 和 **Driver 事务提交** 四个核心阶段：

```
+----------------------------------------------------------------------------------------------------+
|                                    1. DRIVER 端：SQL 解析与规划                                      |
| Spark: Catalyst 解析 SQL ──> AppendDataExec (物理计划)                                             |
| Iceberg: SparkCatalog.loadTable() ──> SparkTable ──> SparkWriteBuilder ──> SparkWrite (BatchWrite) |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ (RDD / Task 分发)
+----------------------------------------------------------------------------------------------------+
|                                 2. EXECUTOR 端：Task 写入与格式适配                                   |
| Spark: DataWritingSparkTask.run() ──> writerFactory.createWriter()                                 |
| Iceberg: SparkWrite.WriterFactory ──> SparkFileWriterFactory (识别 format=ORC)                     |
|                                         └──> ORC.write() ──> 构造 OrcFileAppender                  |
|                                                                                                    |
| 循环写数据：                                                                                        |
| Spark Iterator[InternalRow]                                                                        |
|      │                                                                                             |
|      ▼                                                                                             |
| Iceberg: OrcFileAppender.add(InternalRow)                                                          |
|      │                                                                                             |
|      ▼                                                                                             |
| Iceberg: SparkOrcWriter.write(row, VectorizedRowBatch)  (Spark 内存行 -> ORC 列式 Batch)           |
|      │ (当 batch 满 1024 行时)                                                                      |
|      ▼                                                                                             |
| Apache ORC: org.apache.orc.Writer.addRowBatch() ──> 物理磁盘 (.orc)                                |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ (Task 执行完毕)
+----------------------------------------------------------------------------------------------------+
|                                 3. EXECUTOR 端：Task Commit & 指标收集                              |
| Iceberg: dataWriter.commit() ──> OrcFileAppender.close()                                           |
|       └──> OrcMetrics 提取 Footer/Stripe 统计信息 ──> 生成 DataFile(ORC, min/max, nulls)           |
| Spark: 收集 Task 提交结果 (DataWritingSparkTaskResult) 返回给 Driver                                |
+----------------------------------------------------------------------------------------------------+
                                                 │
                                                 ▼ (Driver 收集全部 Task 结果)
+----------------------------------------------------------------------------------------------------+
|                                4. DRIVER 端：Iceberg Snapshot Commit                               |
| Spark: V2TableWriteExec.writeWithV2() ──> batchWrite.commit(messages)                              |
| Iceberg: SparkWrite.commit() ──> AppendFiles.commit() ──> 写 Manifest ──> 更新元数据 JSON           |
+----------------------------------------------------------------------------------------------------+
```

---

## 2. 第一阶段：Driver 端——SQL 规划与 DataSource V2 绑定

### 2.1 目录插件加载：`SparkCatalog`
当 SQL 语句中包含 Catalog 前缀（如 `prod.db.orc_table`）时，Spark 的 `CatalogManager` 会根据配置反射加载 Iceberg 实现的 `SparkCatalog`：
- **源码文件**：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkCatalog.java`

```java
@Override
public SparkTable loadTable(Identifier ident) {
    Table table = load(ident); // 从底座 (Hive Metastore / REST / Glue / JDBC) 加载 Iceberg Table
    return new SparkTable(table, !cacheEnabled);
}
```

### 2.2 Catalyst 生成物理写计划：`AppendDataExec`
Spark Catalyst 优化器在将逻辑计划解析为物理计划时，识别到目标表实现了 `SupportsWrite` 接口，生成物理节点 `AppendDataExec`：
- **源码文件**：`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala` (Line 288)

```scala
case class AppendDataExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    write: Write, // 持有 Iceberg 的 SparkWrite
    tableName: String,
    transaction: Option[Transaction] = None) extends V2ExistingTableWriteExec
```

### 2.3 构建写入上下文：`SparkWriteBuilder` 与 `SparkWrite`
- **源码文件**：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkTable.java`
- `SparkTable.newWriteBuilder(info)` 根据写入模式（Append / Overwrite / Dynamic Partition Overwrite）创建 `SparkWriteBuilder`。
- `SparkWriteBuilder.buildForBatch()` 最终生成 `SparkWrite`，其实现了 Spark 标准的 `org.apache.spark.sql.connector.write.BatchWrite` 接口。

---

## 3. 第二阶段：Executor 端——从 Task 启动到 ORC Appender 装配的 6 阶微步骤

当 Spark Driver 将 RDD Partitions 派发到集群 Executor 上时，进入了最核心的**对象工厂装配期**。下面拆解这期间发生的 6 个微步骤：

### Step 1：Spark Task 线程启动，索取 `DataWriter`
- **源码文件**：`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala` (Line 680-692)

```scala
def run(writerFactory: DataWriterFactory, context: TaskContext, iter: Iterator[InternalRow], ...): Unit = {
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()

    // 此时的 writerFactory 实际是 Iceberg 序列化下发的 SparkWrite.WriterFactory
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]

    write(dataWriter, iterWithMetrics)
}
```

### Step 2：Iceberg `WriterFactory` 组装文件生成器与写入工厂
- **源码文件**：`spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java` (Line 705-743)

```java
@Override
public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
    Table table = tableBroadcast.value();
    PartitionSpec spec = table.specs().get(outputSpecId);
    FileIO io = table.io();

    // 1. OutputFileFactory：根据分区规则、Task ID 和格式后缀生成唯一数据文件路径 (如 .../00000-0-uuid.orc)
    OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, partitionId, taskId)
        .format(format) // 从表属性获取：FileFormat.ORC
        .build();

    // 2. SparkFileWriterFactory：负责具体文件格式 Writer 的构建
    SparkFileWriterFactory writerFactory = SparkFileWriterFactory.builderFor(table)
        .dataFileFormat(format)            // FileFormat.ORC
        .dataSchema(writeSchema)           // Iceberg Schema
        .dataSparkType(dsSchema)           // Spark StructType
        .writeProperties(writeProperties)
        .build();

    // 3. 根据是否分区返回对应的包装器
    if (spec.isUnpartitioned()) {
        return new UnpartitionedDataWriter(writerFactory, fileFactory, io, spec, targetFileSize);
    } else {
        return new PartitionedDataWriter(...);
    }
}
```

### Step 3：`RollingDataWriter` 触发物理文件打开
- **源码文件**：`iceberg/core/src/main/java/org/apache/iceberg/io/RollingDataWriter.java`
- `UnpartitionedDataWriter` 内部委托给 `RollingDataWriter`，它负责监控已写文件大小（当达到 `write.target-file-size-bytes`，如 512MB 时自动滚动开启新文件）。
- 在首次写入前，调用 `writerFactory.newDataWriter(file, spec, partition)`。

### Step 4：`RegistryBasedFileWriterFactory` 查询全局格式模型注册表
- **源码文件**：`iceberg/data/src/main/java/org/apache/iceberg/data/RegistryBasedFileWriterFactory.java` (Line 99-124)

```java
@Override
public DataWriter<T> newDataWriter(EncryptedOutputFile file, PartitionSpec spec, StructLike partition) {
    // 通过 (FileFormat.ORC, InternalRow.class) 在全局注册表中匹配 Builder
    FileWriterBuilder<DataWriter<T>, S> builder =
        FormatModelRegistry.dataWriteBuilder(dataFileFormat, inputType, file);

    return builder
        .schema(dataSchema)          // Iceberg Schema
        .engineSchema(inputSchema()) // Spark StructType
        .setAll(properties)
        .spec(spec)
        .build();
}
```

### Step 5：`FormatModelRegistry` 命中 `SparkFormatModels`
- **源码文件**：`iceberg/core/src/main/java/org/apache/iceberg/formats/FormatModelRegistry.java`
- **源码文件**：`iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkFormatModels.java`

在类加载时，`SparkFormatModels` 已经静态注册了如下格式适配模型：
```java
// SparkFormatModels.java 第 64-72 行
FormatModelRegistry.register(
    ORCFormatModel.create(
        InternalRow.class,  // inputType
        StructType.class,   // schemaType
        (icebergSchema, fileSchema, engineSchema) ->
            new SparkOrcWriter(icebergSchema, fileSchema), // 绑定 SparkOrcWriter
        ...
    )
);
```

### Step 6：`ORCFormatModel` 组装底层 `OrcFileAppender` 与 `SparkOrcWriter`
- **源码文件**：`iceberg/orc/src/main/java/org/apache/iceberg/orc/ORCFormatModel.java`
- **源码文件**：`iceberg/orc/src/main/java/org/apache/iceberg/orc/OrcFileAppender.java`

在 `builder.build()` 执行时：
1. **注入 Iceberg Field ID**：通过 `ORCSchemaUtil.convert(schema)` 将 Iceberg Schema 转为 ORC `TypeDescription`，并在列属性中打上 `iceberg.id` 标签（确保未来 Schema 演进的安全性）。
2. **初始化原生 ORC Writer**：调用 `ORC.newFileWriter(...)` 实例化 Apache ORC 官方的 `org.apache.orc.Writer`。
3. **分配内存批次**：创建 `VectorizedRowBatch`（默认批次大小 1024 行）。
4. **绑定转换器**：实例化 `SparkOrcWriter`。

至此，数据写入通道被完全打通！

---

## 4. 第三阶段：数据行转换与物理落盘

当 Spark Task 进入迭代循环 `while (iter.hasNext) dataWriter.write(iter.next())` 时：

```
Spark Iterator[InternalRow]
       │ (逐行传入)
       ▼
OrcFileAppender.add(InternalRow)
       │
       ▼
SparkOrcWriter.write(row, VectorizedRowBatch)
       │ (按列解析 Spark InternalRow 并填入 ORC ColumnVector)
       ▼
VectorizedRowBatch (size += 1)
       │ (当 size == 1024 时)
       ▼
org.apache.orc.Writer.addRowBatch(batch) ──> 刷入本地/云端存储 (HDFS/S3/OSS/GCS)
```

- **源码文件**：`iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/data/SparkOrcWriter.java` (Line 58-60)
```java
@Override
public void write(InternalRow value, VectorizedRowBatch output) {
    // 遍历所有字段的 Writer，将 Spark InternalRow 转换为 ORC 底层列数组
    writer.writeRow(value, output);
}
```

- **源码文件**：`iceberg/orc/src/main/java/org/apache/iceberg/orc/OrcFileAppender.java` (Line 93-100)
```java
@Override
public void add(D datum) {
    valueWriter.write(datum, batch); // 填充单行
    if (batch.size == this.batchSize) {
        writer.addRowBatch(batch);   // 批次满，刷盘
        batch.reset();
    }
}
```

---

## 5. 第四阶段：Task Commit 与 Manifest 统计指标提取

当 Task 数据写完后，必须在关闭文件时生成符合 Iceberg 规范的 `DataFile` 对象：

### 5.1 提取列级别统计指标（`OrcMetrics`）
- **源码文件**：`iceberg/orc/src/main/java/org/apache/iceberg/orc/OrcFileAppender.java`
- `OrcFileAppender.close()` 会调用 `OrcMetrics.fromTableConfig(...)`，扫描刚刚生成的 ORC 文件 Footer 和 Stripe 统计：
  - `recordCount`: 总行数
  - `columnSizes`: 每列物理占用字节数
  - `valueCounts` / `nullValueCounts` / `nanValueCounts`: 每列值与空值统计
  - `lowerBounds` / `upperBounds`: 每列二进制极值（供 Scan 规划做下推裁剪）
  - `splitOffsets`: Stripe 起始偏移量（供分布式并行读取分片）

### 5.2 生成 `DataFile` 并返回给 Driver
`BaseTaskWriter` 组装生成一个包含完备元数据的 `DataFile`：
```java
DataFile dataFile = DataFiles.builder(spec)
    .withPath(file.location())
    .withFormat(FileFormat.ORC)
    .withFileSizeInBytes(file.length())
    .withMetrics(appender.metrics())
    .withPartition(partition)
    .build();
```
Spark Task 执行 `dataWriter.commit()`，将结果封装在 `DataWritingSparkTaskResult` 中发回 Driver。

---

## 6. 第五阶段：Driver 端——ACID 快照原子提交（Snapshot Commit）

所有的 Task 成功返回后，Spark Driver 执行最后的提交动作：
- **源码文件**：`spark/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/v2/WriteToDataSourceV2Exec.scala` (Line 651)
```scala
batchWrite.commit(messages)
```

- **源码文件**：`iceberg/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/source/SparkWrite.java`
```java
AppendFiles append = table.newAppend();
for (DataFile file : files) {
    append.appendFile(file);
}
append.commit(); // 触发 Iceberg Core 的 Snapshot 提交
```

在 `append.commit()` 内部：
1. **写 Manifest File**（Avro 格式）：记录所有新加入的 `.orc` `DataFile` 路径和列统计信息。
2. **写 Manifest List**（Avro 格式）：指向所有的 Manifest 文件。
3. **原子更新 Table Metadata**（JSON 格式）：通过底层 Catalog（如 Hive Metastore 的 CAS 或 REST Catalog 的原子提交接口）将表的 `current-snapshot-id` 指向新的 Snapshot。

---

## 7. 架构思考：Iceberg File Format API 的精妙之处

通过上面的全景剖析，我们可以清晰体会到 Iceberg 架构设计的优雅与严谨：

| 层次 | 组件 | 职责 |
| :--- | :--- | :--- |
| **计算引擎层** | Apache Spark (Catalyst / DSv2) | 负责物理执行计划生成、分布式调度、Shuffle 以及提供 `InternalRow` 数据流。 |
| **连接器与胶水层** | `iceberg-spark` (`SparkFormatModels`, `SparkOrcWriter`) | 实现 Spark DSv2 接口，将 Spark 内部数据模型（`InternalRow`）平滑映射到 Iceberg 格式层。 |
| **通用格式适配层** | `iceberg-core` / `iceberg-data` (`FormatModelRegistry`) | 提供引擎无关、格式无关的可插拔抽象，支持根据表属性 `write.format.default` 动态路由。 |
| **物理存储层** | `iceberg-orc` / `iceberg-parquet` (`OrcFileAppender`, `OrcMetrics`) | 负责 `iceberg.id` 元数据注入、Stripe 写入以及提取用于元数据剪枝的精细化统计信息。 |
| **事务与元数据层** | `iceberg-core` (`Snapshot`, `Manifest`) | 维护 ACID 特性，基于原子快照更新实现数据湖的高性能与强一致。 |

这种高度解耦的分层设计，不仅让 Spark、Flink、Trino 可以无缝复用底层的存储能力，也为未来支持 **Lance**、**Vortex** 等面向 AI 时代的新型存储格式铺平了道路。

---

## 8. 参考资料与延伸阅读

- **Apache Iceberg 官方文献与设计规范**：
  - [Introducing the Apache Iceberg File Format API (Iceberg Official Blog)](https://iceberg.apache.org/blog/apache-iceberg-file-format-api/)
  - [Apache Iceberg File Format API 官方架构设计文档 (Google Doc)](https://docs.google.com/document/d/1sF_d4tFxJsZWsZFCyCL9ZE7YuI7-P3VrzMLIrrTIxds)
  - [apache/iceberg PR #12774: Introducing File Format API](https://github.com/apache/iceberg/pull/12774)
  - [apache/iceberg PR #15253: Parquet Format Model Implementation](https://github.com/apache/iceberg/pull/15253)
  - [apache/iceberg PR #15255: ORC Format Model Implementation](https://github.com/apache/iceberg/pull/15255)
  - [apache/iceberg PR #15254: Avro Format Model Implementation](https://github.com/apache/iceberg/pull/15254)
- **PyIceberg 格式路由与生态 Issue**：
  - [apache/iceberg-python Issue #3100: File Format API for PyIceberg](https://github.com/apache/iceberg-python/issues/3100)
  - [apache/iceberg-python Issue #20: Support for ORC format in PyIceberg](https://github.com/apache/iceberg-python/issues/20)
  - [apache/iceberg-python PR #790: Add support for writing ORC](https://github.com/apache/iceberg-python/pull/790)
- **Apache Spark 与存储格式规范**：
  - [Apache Spark DataSource V2 API 规范 (SPARK-22386)](https://issues.apache.org/jira/browse/SPARK-22386)
  - [Apache Spark 官方编程指南 (Spark SQL & DataFrames)](https://spark.apache.org/docs/latest/sql-programming-guide.html)
  - [Apache ORC 官方规范与存储格式文档](https://orc.apache.org/specification/)

