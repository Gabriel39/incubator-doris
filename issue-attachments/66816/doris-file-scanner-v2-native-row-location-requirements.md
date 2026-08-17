# File Scanner V2 Native Reader 支持 Iceberg/Paimon 文件路径与物理行位置

## 1. 功能背景

在排查湖表数据问题、定位异常记录以及构造行级数据操作时，需要知道每条查询结果来自哪个物理数据文件，以及它在该文件中的物理行位置。

本功能在 Doris File Scanner V2 的 Native Parquet/ORC Reader 中提供 Iceberg 和 Paimon 的文件路径与物理行位置 metadata columns。

## 2. 功能目标

支持用户在查询 Iceberg/Paimon 表时，显式查询每条可见记录对应的：

- 物理数据文件路径；
- 物理数据文件内从 0 开始的绝对行位置。

具体列定义如下：

| 表格式 | 文件路径列 | 物理行位置列 |
| --- | --- | --- |
| Iceberg | `_file STRING` | `_pos BIGINT` |
| Paimon | `__paimon_file_path STRING` | `__paimon_row_index BIGINT` |

列名和语义分别遵循：

- [Iceberg metadata column 规范](https://iceberg.apache.org/spec/?h=snapshot)
- [Paimon hidden metadata column 定义](https://paimon.apache.org/docs/master/spark/sql-query/)

## 3. 术语与语义

### 3.1 文件路径

- 文件路径表示记录所在物理数据文件在 Iceberg/Paimon 元数据中记录的原始路径。
- Iceberg `_file` 返回 Iceberg data file metadata 中记录的路径。
- Paimon `__paimon_file_path` 返回 `RawFile.path()` 对应的路径。
- 不得返回 BE 本地缓存路径、临时路径或 reader 内部路径。
- 即使 Doris 为读取文件进行了 URI scheme 或 endpoint 归一化，对用户返回的仍应是表元数据中的原始路径。

### 3.2 物理行位置

- 物理行位置从 `0` 开始。
- 行位置是相对于整个物理数据文件的绝对位置。
- 行位置不是以下任一种序号：
  - File Scanner split 内偏移；
  - Parquet row group 内偏移；
  - ORC stripe 内偏移；
  - 过滤后的结果集序号；
  - 查询结果输出顺序。
- `(file_path, row_position)` 在同一个表快照中可以唯一定位一条物理记录。
- 数据文件经过 compaction 或 rewrite 后，同一逻辑记录的文件路径和物理行位置允许发生变化。

### 3.3 与逻辑 Row ID 的区别

本功能提供的是物理行位置，不提供跨文件重写保持稳定的逻辑 Row ID。

以下能力不在本需求范围内：

- Iceberg V3 `_row_id` 的新增或增强；
- Paimon row-tracking `_ROW_ID`；
- 根据逻辑 Row ID 定位、更新或删除记录。

## 4. 查询示例

### 4.1 Iceberg

```sql
SELECT id, _file, _pos
FROM iceberg_catalog.db.table
WHERE _pos >= 0
ORDER BY _file, _pos;
```

### 4.2 Paimon

```sql
SELECT id, __paimon_file_path, __paimon_row_index
FROM paimon_catalog.db.table
ORDER BY __paimon_file_path, __paimon_row_index;
```

### 4.3 参与过滤和聚合

```sql
SELECT _file, COUNT(*)
FROM iceberg_catalog.db.table
GROUP BY _file;
```

```sql
SELECT *
FROM paimon_catalog.db.table
WHERE __paimon_file_path = 's3://bucket/path/data.parquet'
  AND __paimon_row_index BETWEEN 100 AND 200;
```

## 5. 支持范围

### 5.1 支持

- File Scanner V2；
- Native Parquet Reader；
- Native ORC Reader；
- Iceberg 数据表；
- 能够转换为 Native RawFile split 的 Paimon 数据表；
- 普通扫描和谓词过滤；
- lazy materialization；
- runtime filter；
- Iceberg position delete；
- Iceberg equality delete；
- Iceberg deletion vector；
- Paimon deletion vector；
- 文件 sub-split 并行扫描；
- snapshot/time travel 查询。

### 5.2 不支持

- Paimon JNI Reader；
- Paimon native/JNI 混合 split 的 metadata column 查询；
- `force_jni_scanner=true` 下的 Paimon metadata column 查询；
- File Scanner V1；
- Iceberg/Paimon 非 Parquet、ORC 数据文件；
- Paimon 稳定逻辑 `_ROW_ID`；
- Iceberg V3 `_row_id` 的新增或增强。

### 5.3 不支持场景的行为

当查询引用本功能提供的 metadata columns，但实际 scan range 不能使用 File Scanner V2 Native Parquet/ORC Reader 时，必须明确报错。

禁止以下行为：

- 返回 `NULL`；
- 返回默认值或错误值；
- 忽略 metadata column；
- 只返回 native split 的部分结果；
- 在已经输出部分结果后再报错。

建议错误信息包含 metadata column、表格式和实际 reader 类型，例如：

```text
Metadata column '<column>' is only supported by FileScannerV2 native
Parquet/ORC reader for <table_format>; actual reader is <reader_type>.
```

## 6. 验收标准

### AC-1：列定义与可见性

- Iceberg 提供：
  - `_file STRING NOT NULL`
  - `_pos BIGINT NOT NULL`
- Paimon 提供：
  - `__paimon_file_path STRING NOT NULL`
  - `__paimon_row_index BIGINT NOT NULL`
- 默认 `SELECT *` 不包含这些 metadata columns。
- 用户可以在 `show_hidden_columns=false` 时显式引用这些列。
- `show_hidden_columns=true` 时，`DESC` 和 `SELECT *` 能看到对应 metadata columns。
- Metadata columns 不得被当成 Parquet/ORC 中的真实物理列读取。

### AC-2：文件路径正确性

- Iceberg `_file` 返回 Iceberg data file metadata 中记录的原始路径。
- Paimon `__paimon_file_path` 返回 `RawFile.path()` 对应的原始路径。
- 不得返回 BE 本地缓存路径、临时路径或内部 reader 路径。
- 对 `s3a://`、`oss://` 等经过 Doris 存储路径归一化的文件，返回值仍应保持表元数据中的原始路径。
- 同一个物理文件中的所有记录返回相同路径。

### AC-3：物理行位置正确性

- 第一条物理记录的行位置为 `0`。
- 行位置跨 Parquet row group 连续。
- 行位置跨 ORC stripe 连续。
- 文件被切成多个 scan range 后，行位置仍然是文件级绝对位置。
- 不同 split 不得产生重复或重叠的 `(file_path, row_position)`。
- 多 BE 并行执行不能改变行位置。

### AC-4：过滤后保持原始行位置

假设物理文件包含行位置 `0,1,2,3,4`，查询过滤后只保留原始位置 `1,4`，返回结果必须仍为：

```text
1
4
```

不得重新编号为：

```text
0
1
```

以下过滤路径均须满足该要求：

- 普通 WHERE 谓词；
- Parquet dictionary/page/row-group pruning；
- ORC SARG/stripe pruning；
- lazy materialization；
- runtime filter。

### AC-5：Delete/DV 处理正确

- 被 Iceberg position delete 删除的记录不出现在结果中。
- 被 Iceberg equality delete 删除的记录不出现在结果中。
- 被 Iceberg/Paimon deletion vector 删除的记录不出现在结果中。
- 删除过滤后，剩余记录的 row position 必须保持原始文件位置，不能重新编号。
- Metadata columns 与业务列必须逐行对齐，不能出现错位。

### AC-6：Metadata column 表达式正确

以下场景结果正确：

- 仅投影文件路径；
- 仅投影行位置；
- 同时投影业务列、文件路径和行位置；
- metadata column 出现在 `WHERE`；
- metadata column 出现在 `ORDER BY`；
- metadata column 出现在 `GROUP BY`；
- metadata column 参与比较、范围和聚合表达式；
- 多表 JOIN 中分别引用两张表的 metadata columns。

Metadata column 谓词暂不要求下推到文件 reader，但最终结果必须正确。

### AC-7：聚合下推正确性

对于以下查询：

```sql
SELECT COUNT(*)
FROM iceberg_table
WHERE _file = '...';
```

或者：

```sql
SELECT COUNT(*)
FROM paimon_table
WHERE __paimon_row_index >= 100;
```

- 不得使用会跳过 metadata predicate 的文件级或表级 COUNT 下推。
- 如果当前聚合下推无法计算 metadata predicate，必须退化成实际扫描。
- 不得返回未应用 metadata predicate 的文件总行数。

### AC-8：Native-only 边界

当查询引用 metadata columns 时：

- 所有 split 均为 File Scanner V2 Native Parquet/ORC：正常执行。
- Paimon 全部为 JNI split：查询失败并明确提示不支持。
- Paimon 同时包含 native 和 JNI split：整个查询失败。
- `force_jni_scanner=true`：查询失败。
- File Scanner V2 被关闭：查询失败。
- 数据文件格式不是 Parquet/ORC：查询失败。
- 必须在返回第一批结果前失败，不允许产生部分结果。

不引用 metadata columns 的已有 JNI、mixed split 和 File Scanner V1 查询行为保持不变。

### AC-9：Snapshot 与文件重写

- Snapshot/time travel 查询返回所选快照对应的数据文件路径和行位置。
- 查询历史快照时，不得错误返回最新快照的文件路径。
- compaction/rewrite 前后的 `(file_path, row_position)` 可以变化。
- 同一快照重复查询时，结果必须一致，不受并发调度和 scan range 划分影响。

### AC-10：性能与资源

- 未引用 metadata columns 时，不增加行位置列物化，不产生可观测的额外数据文件 IO。
- 引用 metadata columns 时，不额外读取数据文件中的物理列。
- 文件路径应优先使用常量列或等价低内存表示，避免在中间 Block 中为每行重复复制完整路径字符串。
- 行位置列的额外内存开销应接近 `8 × rows`。
- 大文件、多 row group/stripe 扫描不能出现明显内存膨胀或 OOM。

## 7. 风险点

| 编号 | 风险描述 | 来源 | 影响面 | 严重程度 |
| --- | --- | --- | --- | --- |
| R1 | row position 被计算成 split/row-group/stripe 内偏移 | 白盒 | 返回错误定位信息 | P0 |
| R2 | 谓词、lazy read、delete/DV 后 metadata 与业务列错位 | 黑盒+白盒 | 错误数据定位 | P0 |
| R3 | Paimon mixed split 静默返回 NULL 或部分结果 | 黑盒 | Wrong result | P0 |
| R4 | metadata predicate 被 COUNT 下推跳过 | 白盒 | 聚合结果错误 | P0 |
| R5 | 返回路径是归一化路径或本地缓存路径 | 黑盒 | 无法和表元数据关联 | P1 |
| R6 | 文件 sub-split 产生重复行号 | 白盒 | 行定位不唯一 | P1 |
| R7 | metadata column 被错误识别为物理列 | 白盒 | Missing column 或读取失败 | P1 |
| R8 | 每行复制长文件路径导致内存膨胀 | 白盒 | 性能退化或 OOM | P2 |
| R9 | 新功能影响未引用 metadata column 的旧查询 | 兼容性 | 查询行为回归 | P1 |

## 8. 核心测试用例

| 用例 | 目标 | 覆盖风险 | 测试维度 | 前置条件 | 负载描述 | 执行预期 |
| --- | --- | --- | --- | --- | --- | --- |
| TC-01 | 验证 Iceberg Parquet 基本语义 | R1、R5、R7 | 功能/正确性 | 多 row group Iceberg Parquet 表 | 查询 `_file/_pos` | 路径正确，位置从 0 开始且跨 row group 连续 |
| TC-02 | 验证 Iceberg ORC 基本语义 | R1、R5、R7 | 功能/正确性 | 多 stripe Iceberg ORC 表 | 查询 `_file/_pos` | 路径正确，位置跨 stripe 连续 |
| TC-03 | 验证过滤和 lazy materialization | R1、R2 | 正确性 | 数据物理位置已知 | 使用列谓词、runtime filter、lazy read | 返回原始物理位置，不重新编号 |
| TC-04 | 验证 Iceberg delete | R2 | 正确性 | 分别构造 position/equality/DV delete | 查询 metadata columns | 删除行不返回，剩余行位置保持不变 |
| TC-05 | 验证 Paimon native 与 DV | R1、R2、R5 | 正确性 | Native Parquet/ORC Paimon 表，包含 DV | 查询路径和行位置 | DV 行被过滤，剩余位置与原文件一致 |
| TC-06 | 验证文件 sub-split | R1、R6 | 正确性/性能 | 小 `file_split_size`，单文件切成多个 range | 并行扫描全部 range | `(path,row_index)` 无重复、无遗漏 |
| TC-07 | 验证 JNI/mixed split 拒绝 | R3、R9 | 异常/兼容性 | JNI、mixed、force JNI 三种场景 | 引用 metadata columns | 整个查询在返回结果前明确失败；普通列查询不受影响 |
| TC-08 | 验证 metadata predicate 与 COUNT | R4 | 正确性 | 多文件且文件行数已知 | `COUNT(*) WHERE path/position predicate` | 不错误下推，计数与实际扫描一致 |
| TC-09 | 验证 snapshot/time travel | R5、R6 | 正确性 | rewrite 前后两个快照 | 分别查询两个快照 | 各自返回对应快照的文件路径和行位置 |
| TC-10 | 验证资源开销 | R8、R9 | 性能 | 大文件、长对象存储路径 | 对比投影/不投影 metadata columns | 无额外文件 IO；未投影无明显开销；路径不在中间 Block 中按行重复大量分配 |

## 9. 覆盖度检查

- R1：TC-01、TC-02、TC-03、TC-05、TC-06 覆盖。
- R2：TC-03、TC-04、TC-05 覆盖。
- R3：TC-07 覆盖。
- R4：TC-08 覆盖。
- R5：TC-01、TC-02、TC-05、TC-09 覆盖。
- R6：TC-06、TC-09 覆盖。
- R7：TC-01、TC-02 覆盖。
- R8：TC-10 覆盖。
- R9：TC-07、TC-10 覆盖。

所有 P0/P1 风险均至少由一个核心测试用例覆盖。

