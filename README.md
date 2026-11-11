# 渡轮结算 · Iceberg v2 票号删除核对与离线压实

用于核对湖仓源快照中，某航次日分区里的票号是否已被 **Iceberg v2 位置删除**
（positional delete）标记，并支持把一个数据文件离线**压实**（compaction）：
清掉已撤销的票，同时清理位置删除文件，避免其他文件里已删的票重新入账。
网页接收有效文件清单和对应 Parquet 副本。核对功能输入整数票号，逐条给出原文件、
从 0 起算的文件内行号、是否已删除以及命中的删除文件；压实功能返回可下载的 ZIP 包。

## 快速启动

```bash
docker compose up --build
```

打开 <http://localhost:8000>，点 **“载入样例”** 即可运行内置演示。接口健康检查：

```bash
curl http://localhost:8000/healthz
```

停止：

```bash
docker compose down
```

## 网页用法

1. 上传 **有效文件清单**（CSV 或 JSON）。
2. 上传清单中所有 `data` / `position_delete` 文件的 **Parquet 副本**，可多选。
   文件名必须与清单的 `uploaded_filename` 一致。
3. 填写整数票号（逗号、空格、分号或换行分隔），以及票号列名（默认
   `ticket_no`）。
4. 开始核对。

上传文件只保存在一次请求的临时目录中，请求结束后删除；不会按用户文件名
拼接服务器路径。

### 清单格式

CSV（推荐）：

```csv
original_path,uploaded_filename,file_type,data_sequence_number
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/data/data-0001.parquet,data-0001.parquet,data,7
s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05/delete-pos/delete-pos-0001.parquet,delete-pos-0001.parquet,position_delete,12
```

JSON 也支持，顶层可以是数组，或包含 `files` / `entries` 数组的对象：

```json
[
  {
    "original_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "file_type": "data",
    "data_sequence_number": 7
  }
]
```

列名可使用以下别名：

| 含义 | 可用列名 |
| --- | --- |
| 原始路径 | `original_path`, `original_file_path`, `source_path`, `path` |
| 上传文件名 | `uploaded_filename`, `uploaded_file`, `filename` |
| 文件类型 | `file_type`, `content`, `type` |
| 数据序列号 | `data_sequence_number`, `sequence_number`, `seq` |

`file_type` 可选值：

| 值 | 行为 |
| --- | --- |
| `data` / `0` | 数据文件，参与扫描 |
| `position_delete` / `positional_delete` / `1` / `delete` | 位置删除文件，参与匹配 |
| `equality_delete` / `2` | 等值删除，**忽略**；不要求上传 Parquet 副本 |

只处理位置删除。位置删除 Parquet 必须包含 `file_path`（字符串）和 `pos`
（整数）两列，其他列（例如 Iceberg 可选的 `row`）会被忽略。

## 核对规则

- **行组跳读**：对每个数据文件的每个行组读取 `ticket_no` 列的 min/max 统计。
  票号落在统计范围外时跳过该行组；统计缺失（`statistics` 为 `None` 或没有
  min/max）时读取该行组。统计范围包含票号时也读取。
- **原始行号**：行号始终是数据文件中从 0 起算的原始行号 =
  前面所有行组的行数之和 + 行组内原始下标。筛选或只读取某个行组都不会重置、
  压缩或重新编号。
- **位置删除匹配**：删除记录必须同时满足
  1. 删除记录的 `file_path` 精确等于清单中的 `original_path`；
  2. `pos` 精确等于该命中行的从 0 起算的行号；
  3. 删除文件的数据序列号 **不小于** 数据文件的数据序列号。

  不满足序列号条件的候选删除文件会显示为“被规则忽略的删除记录”，但不会
  把票判成已删除。
- **全有或全无**：清单中的任何一个 `data` / `position_delete` 副本缺失，
  或任一文件打不开、行组读取失败，整次核对都会中止并明确列出文件名，不显示
  任何票号的部分结论。未出现在清单中的上传文件会被忽略并给出警告。

## 内置样例

`sample/` 中的演示数据由 `tools/make_sample.py` 生成：

- `data-0001.parquet`：5 个行组，每个 20 行。
  - 行组 0–3 的票号范围分别是 1000–1019、2000–2019、3000–3019、4000–4019。
  - 票号 **4242** 在第 5 个行组（索引 4），全局行号 **99**。
  - 核对 4242 时，前 4 个行组全部被统计范围排除并跳过；读取第 5 个行组后
    仍按原始行号 99 命中 `delete-pos-0001.parquet`。
- `data-0002.parquet`：写入时关闭了列统计，票号 **8888** 在第 2 个行组
  （全局行号 **19**）。因为没有统计，两个行组都会被读取，随后命中
  `delete-pos-0002.parquet`。
- `delete-pos-0001.parquet`：**同一份位置删除文件在两个数据文件上都生效**——
  指向 `data-0001.parquet` 的行 3（票号 1003）、行 99（票号 4242），以及
  `data-0002.parquet` 的行 3（票号 7003）；另有一条指向 `data-0002.parquet`
  行 99 的越界记录（该文件只有 20 行），永不命中。
- `delete-pos-stale-0001.parquet`：也指向 4242 / 行 99，但数据序列号是 6，
  小于数据文件的 7，因此核对时被忽略；压实时不删行，但记录仍会被清掉。
- 清单中的 `equality_delete` 项没有上传副本，核对仍正常完成，因为它被忽略；
  压实则会直接拒绝含等值删除的清单。
- 数据与删除文件的 Parquet schema 都带显式字段 ID，压实重写后保持不变。

样例默认票号：`4242, 1003, 4218, 8888, 9999`。其中 1003 已删除，4218
存在但未删除，9999 不存在。

## 离线压实

网页下方的 **“离线压实”** 卡片接收：

1. 同一航次日分区的 Iceberg v2 有效文件清单（CSV 或 JSON，格式同上）。
2. **目标数据文件**与清单中**全部位置删除文件**的 Parquet 副本（可多选）。
3. 目标数据文件在清单中的 `uploaded_filename`。

### 压实规则

- **拒绝等值删除**：清单含任何 `equality_delete` 条目时直接中止，不生成压实包，
  并指出对应文件名。
- **匹配方式**：按目标文件的 `original_path` 与从 0 起算的原始行号匹配；
  位置删除文件的数据序列号 **不小于** 目标文件数据序列号的记录才删行
  （相等也生效）。序列号更小的记录不删行。
- **删除文件清理**：位置删除文件中所有指向目标旧路径的记录全部移除
  （包括序列号过期、行号越界的记录），指向其他路径的记录原样保留顺序。
- **重写保真**：重写后的 Parquet 保留列结构（含已有字段 ID、可空性、嵌套类型
  和表级元数据）以及剩余行的相对顺序。
- **序列号不变**：替换文件沿用原数据序列号，并在包内清单中逐文件标明。
- **退役与不打包**：数据行被全部清空的文件只列为 `retired`，不写入 ZIP；
  没有任何变化的删除文件不打包也不列条目。
- **全有或全无**：缺件、文件打不开或写入复读失败时，指出具体文件名并中止，
  绝不返回压实包。

### 压实包结构

ZIP 内只放发生替换的新 Parquet（扁平文件名，统一 `replaced-` 前缀，避免路径
穿越）和 `compaction-manifest.json`：

```text
compaction-data-0001.zip
├── replaced-data-0001.parquet
├── replaced-delete-pos-0001.parquet
└── compaction-manifest.json
```

清单按旧路径（`old_path`）标明每个替换/退役文件的状态、替换文件名与数据序列号：

```json
{
  "schema_version": 1,
  "target": {
    "old_path": "s3://.../data/data-0001.parquet",
    "uploaded_filename": "data-0001.parquet",
    "data_sequence_number": 7
  },
  "summary": { "replaced": 2, "retired": 1, "unchanged": 1 },
  "files": [
    {
      "old_path": "s3://.../data/data-0001.parquet",
      "uploaded_filename": "data-0001.parquet",
      "file_type": "data",
      "data_sequence_number": 7,
      "status": "replaced",
      "replacement_file": "replaced-data-0001.parquet",
      "original_rows": 100,
      "remaining_rows": 98,
      "removed_rows": 2
    }
  ],
  "warnings": []
}
```

### 样例压实

压实卡片上的两个按钮直接下载内置样例压实包：

- 目标 `data-0001.parquet`：删除票号 1003、4242 两行；`delete-pos-0001.parquet`
  中指向 `data-0002.parquet` 的两条记录原样保留；`delete-pos-stale-0001.parquet`
  清空后退役。
- 目标 `data-0002.parquet`：删除票号 7003、8888 两行；同一份
  `delete-pos-0001.parquet` 中指向 `data-0001.parquet` 的记录原样保留；
  `delete-pos-0002.parquet` 清空后退役。

样例压实使用不含等值删除条目的 `sample/manifest-compact.csv`；核对页继续使用
含等值删除条目的 `sample/manifest.csv`。

## 本地开发与测试

需要 Python 3.11+、Flask、PyArrow。

```bash
python3 -m pip install -r requirements.txt
python3 app.py                     # http://localhost:8000
python3 -m pytest -q               # 65 个测试
```

重新生成样例（会覆盖 `sample/` 下的 Parquet 和清单）：

```bash
python3 tools/make_sample.py
```

## 目录

```
app.py                     Flask 路由、上传处理、票号解析、压实包下载
checker/manifest.py        清单解析与校验
checker/engine.py          PyArrow 行组跳读、行号计算、位置删除匹配
checker/compactor.py       离线压实：删行、删除文件清理、ZIP 与包内清单
checker/errors.py          CheckError（始终带文件名）
templates/index.html       网页（核对 + 离线压实入口）
static/style.css           样式
sample/                    内置样例（清单 + Parquet）
tools/make_sample.py       样例生成脚本
tests/                     pytest 测试
Dockerfile                 容器镜像
docker-compose.yml         启动配置
```

## 已知边界

- 位置删除文件会整体读入内存建立 `(file_path, pos)` 索引；压实也会整体读入
  目标数据文件与位置删除文件，适用于常见规模。
- 每个票号独立扫描数据文件。一次输入大量票号时耗时随票号数线性增长。
- 压实每次只处理一个目标数据文件；多个数据文件需分别发起请求。
- 重写后的 Parquet 使用 snappy 压缩并按 PyArrow 默认行组大小落盘；列结构、
  字段 ID 与剩余行顺序保持不变，但不保证行组边界与原文件一致。
- 路径按字符串精确比较，不解析 `s3://`、`file://` 等 URI 等价关系。
  请保证清单的 `original_path` 与删除记录中的 `file_path` 写法完全一致。
