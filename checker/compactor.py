"""Offline compaction of one Iceberg v2 data file.

A compaction request names one ``data`` manifest entry (the *target*) and
supplies Parquet replicas of the target and of **every** positional delete
file in the same manifest.  The rules mirror the on-call procedure:

* A manifest containing any equality-delete entry is refused: equality
  deletes cannot be resolved from Parquet replicas.
* Data rows are matched by the target's ``original_path`` and the row's
  zero-based index in the original data file.  A positional delete only
  removes a row when the delete file's data sequence number is greater than
  or equal to the target data file's.
* Every positional-delete row naming the target's old path is purged from
  the delete file, regardless of sequence number (stale or out-of-range
  records are purged too).  Records naming other paths are kept untouched.
* Rewrites preserve the Parquet schema (including existing field ids) and
  the order of the remaining rows.  Every replacement keeps the original
  data sequence number, recorded in the bundled manifest.
* A file whose rows are all removed is listed as ``retired`` and is not
  packed; files that did not change are not packed either.
* Any missing replica or read/write failure aborts the whole run with a
  ``CheckError`` naming the offending file; no ZIP is ever returned for a
  failed run.
"""

from __future__ import annotations

import io
import json
import os
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .engine import DELETE_PATH_COLUMN, DELETE_POS_COLUMN, _require_integer_column
from .errors import CheckError
from .manifest import FileKind, Manifest, ManifestEntry

REPLACEMENT_PREFIX = "replaced-"
ZIP_MANIFEST_NAME = "compaction-manifest.json"
SCHEMA_VERSION = 1

STATUS_REPLACED = "replaced"
STATUS_RETIRED = "retired"
STATUS_UNCHANGED = "unchanged"


@dataclass
class FileRewrite:
    """What happened to one manifest file during compaction."""

    entry: ManifestEntry
    status: str
    original_rows: int
    remaining_rows: int
    removed_rows: int
    replacement_file: str | None = None

    @property
    def packed(self) -> bool:
        return self.status == STATUS_REPLACED and self.replacement_file is not None


@dataclass
class _DeletePlan:
    entry: ManifestEntry
    table: pa.Table
    kept_table: pa.Table
    target_reference_count: int
    stale_reference_count: int
    out_of_range_count: int


@dataclass
class CompactionResult:
    manifest: Manifest
    target_entry: ManifestEntry
    rewrites: list[FileRewrite]
    warnings: list[str] = field(default_factory=list)
    zip_buffer: bytes = b""
    zip_name: str = ""
    manifest_payload: dict = field(default_factory=dict)

    @property
    def packed_rewrites(self) -> list[FileRewrite]:
        return [rewrite for rewrite in self.rewrites if rewrite.packed]

    @property
    def retired_rewrites(self) -> list[FileRewrite]:
        return [r for r in self.rewrites if r.status == STATUS_RETIRED]


def _replacement_name(entry: ManifestEntry) -> str:
    """Flat, traversal-safe ZIP entry name for a replacement Parquet."""
    base = os.path.basename(entry.uploaded_filename.replace("\\", "/")).strip()
    if not base or base in {".", ".."} or "/" in base:
        raise CheckError(
            f"文件 “{entry.uploaded_filename}” 的文件名无法安全地用于压实包。",
            files=[entry.uploaded_filename],
        )
    return f"{REPLACEMENT_PREFIX}{base}"


def _require_no_equality_deletes(manifest: Manifest) -> None:
    equality = manifest.equality_delete_files
    if equality:
        names = [entry.uploaded_filename for entry in equality]
        raise CheckError(
            "清单包含等值删除（equality delete）条目，无法仅凭 Parquet 副本"
            "离线压实；请先在表侧处理等值删除后再申请压实。",
            files=names,
        )


def _resolve_target(
    manifest: Manifest, target_filename: str
) -> ManifestEntry:
    target_filename = (target_filename or "").strip()
    if not target_filename:
        raise CheckError(
            "请填写要压实的目标数据文件在清单中的 uploaded_filename。"
        )
    for entry in manifest.entries:
        if entry.uploaded_filename == target_filename:
            if entry.kind is not FileKind.DATA:
                raise CheckError(
                    f"目标 “{target_filename}” 的文件类型是 "
                    f"{entry.raw_file_type}，不是 data 数据文件；压实目标必须是"
                    "数据文件。",
                    files=[target_filename],
                )
            return entry
    options = "、".join(entry.uploaded_filename for entry in manifest.data_files)
    raise CheckError(
        f"清单中找不到目标数据文件 “{target_filename}”。清单中的数据文件为："
        f"{options or '（无）'}。",
        files=[target_filename],
    )


def _check_presence(
    manifest: Manifest,
    target: ManifestEntry,
    uploaded_paths: Mapping[str, Path],
) -> None:
    required = [target, *manifest.position_delete_files]
    missing = [
        entry.uploaded_filename
        for entry in required
        if not (
            (path := uploaded_paths.get(entry.uploaded_filename)) is not None
            and Path(path).is_file()
        )
    ]
    if missing:
        raise CheckError(
            "压实所需的以下 Parquet 副本缺失，已中止且不生成压实包。",
            files=missing,
        )


def _open_parquet(path: Path, label: str, role: str, original_path: str):
    try:
        return pq.ParquetFile(str(path))
    except Exception as exc:
        raise CheckError(
            f"无法读取{role} “{label}”（原始路径 {original_path}）：{exc}",
            files=[label],
        ) from None


def _validate_target_row(
    pos: object, row_index: int, label: str
) -> int:
    if pos is None or not isinstance(pos, int) or isinstance(pos, bool):
        raise CheckError(
            f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）的 pos "
            "不是整数。",
            files=[label],
        )
    if pos < 0:
        raise CheckError(
            f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）的 "
            f"pos={pos} 为负数。",
            files=[label],
        )
    return pos


def _plan_delete_file(
    entry: ManifestEntry,
    path: Path,
    target: ManifestEntry,
    target_row_count: int,
) -> _DeletePlan | None:
    """Read one positional delete file and plan its rewrite.

    Returns ``None`` when the file has no record naming the target, in which
    case it stays unchanged and is not packed.
    """
    label = entry.uploaded_filename
    parquet = _open_parquet(
        path, label, "位置删除文件", entry.original_path
    )
    schema = parquet.schema_arrow
    try:
        path_field = schema.field(DELETE_PATH_COLUMN)
    except KeyError:
        raise CheckError(
            f"删除文件 “{label}” 缺少列 “{DELETE_PATH_COLUMN}”。位置删除文件"
            "必须包含 file_path 和 pos。",
            files=[label],
        ) from None
    if not (
        pa.types.is_string(path_field.type)
        or pa.types.is_large_string(path_field.type)
    ):
        raise CheckError(
            f"删除文件 “{label}” 的列 “{DELETE_PATH_COLUMN}” 类型为 "
            f"{path_field.type}，不是字符串类型。",
            files=[label],
        )
    _require_integer_column(
        schema, DELETE_POS_COLUMN, file_label=label, role="删除文件"
    )
    try:
        table = parquet.read()
    except Exception as exc:
        raise CheckError(
            f"无法读取位置删除文件 “{label}”（原始路径 "
            f"{entry.original_path}）：{exc}",
            files=[label],
        ) from None

    record_paths = table.column(DELETE_PATH_COLUMN).to_pylist()
    record_positions = table.column(DELETE_POS_COLUMN).to_pylist()
    target_flags = [
        isinstance(record_path, str) and record_path == target.original_path
        for record_path in record_paths
    ]
    if not any(target_flags):
        return None

    target_indices = [i for i, flagged in enumerate(target_flags) if flagged]
    stale = 0
    out_of_range = 0
    for row_index in target_indices:
        record_path = record_paths[row_index]
        if not isinstance(record_path, str) or record_path == "":
            raise CheckError(
                f"位置删除文件 “{label}” 第 {row_index} 行（从 0 起算）的 "
                "file_path 不是有效的非空字符串。",
                files=[label],
            )
        pos = _validate_target_row(record_positions[row_index], row_index, label)
        if entry.sequence_number < target.sequence_number:
            stale += 1
            continue
        if pos >= target_row_count:
            out_of_range += 1

    kept_table = table.filter(
        pc.invert(pa.array(target_flags, type=pa.bool_()))
    )
    if kept_table.schema != table.schema:
        raise CheckError(
            f"重写位置删除文件 “{label}” 时列结构发生变化，已中止。",
            files=[label],
        )
    return _DeletePlan(
        entry=entry,
        table=table,
        kept_table=kept_table,
        target_reference_count=len(target_indices),
        stale_reference_count=stale,
        out_of_range_count=out_of_range,
    )


def _collect_delete_positions(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    target: ManifestEntry,
    target_row_count: int,
    warnings: list[str],
) -> tuple[set[int], list[_DeletePlan]]:
    positions: set[int] = set()
    plans: list[_DeletePlan] = []
    for entry in manifest.position_delete_files:
        plan = _plan_delete_file(
            entry,
            uploaded_paths[entry.uploaded_filename],
            target,
            target_row_count,
        )
        if plan is None:
            continue
        plans.append(plan)
        label = entry.uploaded_filename
        if plan.stale_reference_count:
            warnings.append(
                f"位置删除文件 “{label}” 的数据序列号 "
                f"{entry.sequence_number} 小于目标文件的 "
                f"{target.sequence_number}，其中 "
                f"{plan.stale_reference_count} 条指向目标旧路径的记录不删行，"
                "但仍会从删除文件中移除。"
            )
        if plan.out_of_range_count:
            warnings.append(
                f"位置删除文件 “{label}” 有 {plan.out_of_range_count} 条"
                "指向目标旧路径的记录行号超出目标文件行数，未删行，记录仍"
                "会移除。"
            )
        if entry.sequence_number >= target.sequence_number:
            paths = plan.table.column(DELETE_PATH_COLUMN).to_pylist()
            poses = plan.table.column(DELETE_POS_COLUMN).to_pylist()
            for row_index, record_path in enumerate(paths):
                if record_path == target.original_path:
                    pos = _validate_target_row(poses[row_index], row_index, label)
                    if pos < target_row_count:
                        positions.add(pos)
    return positions, plans


def _write_and_verify(
    table: pa.Table, destination: Path, label: str
) -> None:
    try:
        pq.write_table(table, destination, compression="snappy")
        written = pq.ParquetFile(str(destination))
        if written.metadata.num_rows != table.num_rows:
            raise ValueError(
                f"写入 {table.num_rows} 行，但复读为 "
                f"{written.metadata.num_rows} 行"
            )
        if written.schema_arrow != table.schema:
            raise ValueError("复读后的列结构与待写入表不一致")
    except CheckError:
        raise
    except Exception as exc:
        raise CheckError(
            f"重写文件 “{label}” 时写入或复读失败：{exc}",
            files=[label],
        ) from None


def _build_manifest_payload(
    target: ManifestEntry,
    rewrites: list[FileRewrite],
    warnings: list[str],
) -> dict:
    files_payload: list[dict] = []
    replaced = retired = unchanged = 0
    for rewrite in rewrites:
        if rewrite.status == STATUS_UNCHANGED:
            unchanged += 1
            continue
        if rewrite.status == STATUS_RETIRED:
            retired += 1
        else:
            replaced += 1
        files_payload.append(
            {
                "old_path": rewrite.entry.original_path,
                "uploaded_filename": rewrite.entry.uploaded_filename,
                "file_type": rewrite.entry.kind.value,
                "data_sequence_number": rewrite.entry.sequence_number,
                "status": rewrite.status,
                "replacement_file": rewrite.replacement_file,
                "original_rows": rewrite.original_rows,
                "remaining_rows": rewrite.remaining_rows,
                "removed_rows": rewrite.removed_rows,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "target": {
            "old_path": target.original_path,
            "uploaded_filename": target.uploaded_filename,
            "data_sequence_number": target.sequence_number,
        },
        "summary": {
            "replaced": replaced,
            "retired": retired,
            "unchanged": unchanged,
        },
        "files": files_payload,
        "warnings": warnings,
    }


def run_compaction(
    manifest: Manifest,
    uploaded_paths: Mapping[str, Path],
    target_filename: str,
) -> CompactionResult:
    """Compact one target data file and return its ZIP package in memory."""
    _require_no_equality_deletes(manifest)
    target = _resolve_target(manifest, target_filename)
    if not manifest.position_delete_files:
        raise CheckError(
            "清单中没有 position_delete 位置删除文件，目标数据文件无需压实。",
            files=[target.uploaded_filename],
        )
    _check_presence(manifest, target, uploaded_paths)

    warnings: list[str] = []
    referenced = {entry.uploaded_filename for entry in manifest.entries}
    unused = sorted(name for name in uploaded_paths if name not in referenced)
    if unused:
        warnings.append(
            "以下上传文件未出现在清单中，已忽略：" + "、".join(unused)
        )

    target_label = target.uploaded_filename
    target_parquet = _open_parquet(
        uploaded_paths[target_label],
        target_label,
        "数据文件",
        target.original_path,
    )
    try:
        data_table = target_parquet.read()
    except Exception as exc:
        raise CheckError(
            f"无法读取目标数据文件 “{target_label}”（原始路径 "
            f"{target.original_path}）：{exc}",
            files=[target_label],
        ) from None
    target_row_count = data_table.num_rows

    delete_positions, plans = _collect_delete_positions(
        manifest, uploaded_paths, target, target_row_count, warnings
    )
    if not plans:
        raise CheckError(
            f"没有任何位置删除文件指向目标 “{target_label}” 的旧路径 "
            f"{target.original_path}，目标文件无需压实。",
            files=[target_label],
        )

    keep_mask = [True] * target_row_count
    for pos in delete_positions:
        keep_mask[pos] = False
    kept_data = data_table.filter(pa.array(keep_mask, type=pa.bool_()))
    if kept_data.schema != data_table.schema:
        raise CheckError(
            f"重写目标数据文件 “{target_label}” 时列结构发生变化，已中止。",
            files=[target_label],
        )

    rewrites: list[FileRewrite] = []
    output_rows: list[tuple[str, Path]] = []

    with tempfile.TemporaryDirectory(prefix="iceberg-compact-") as temp:
        work_dir = Path(temp)

        if kept_data.num_rows == 0:
            rewrites.append(
                FileRewrite(
                    entry=target,
                    status=STATUS_RETIRED,
                    original_rows=target_row_count,
                    remaining_rows=0,
                    removed_rows=target_row_count,
                )
            )
        else:
            replacement = _replacement_name(target)
            destination = work_dir / replacement
            _write_and_verify(kept_data, destination, target_label)
            rewrites.append(
                FileRewrite(
                    entry=target,
                    status=STATUS_REPLACED,
                    original_rows=target_row_count,
                    remaining_rows=kept_data.num_rows,
                    removed_rows=target_row_count - kept_data.num_rows,
                    replacement_file=replacement,
                )
            )
            output_rows.append((replacement, destination))

        for plan in plans:
            label = plan.entry.uploaded_filename
            if plan.kept_table.num_rows == 0:
                rewrites.append(
                    FileRewrite(
                        entry=plan.entry,
                        status=STATUS_RETIRED,
                        original_rows=plan.table.num_rows,
                        remaining_rows=0,
                        removed_rows=plan.target_reference_count,
                    )
                )
                continue
            replacement = _replacement_name(plan.entry)
            destination = work_dir / replacement
            _write_and_verify(plan.kept_table, destination, label)
            rewrites.append(
                FileRewrite(
                    entry=plan.entry,
                    status=STATUS_REPLACED,
                    original_rows=plan.table.num_rows,
                    remaining_rows=plan.kept_table.num_rows,
                    removed_rows=plan.target_reference_count,
                    replacement_file=replacement,
                )
            )
            output_rows.append((replacement, destination))

        for entry in manifest.position_delete_files:
            if not any(plan.entry is entry for plan in plans):
                rewrites.append(
                    FileRewrite(
                        entry=entry,
                        status=STATUS_UNCHANGED,
                        original_rows=0,
                        remaining_rows=0,
                        removed_rows=0,
                    )
                )

        payload = _build_manifest_payload(target, rewrites, warnings)
        manifest_bytes = json.dumps(
            payload, ensure_ascii=False, indent=2
        ).encode("utf-8")

        buffer = io.BytesIO()
        try:
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
                for arcname, destination in output_rows:
                    archive.write(destination, arcname=arcname)
                archive.writestr(ZIP_MANIFEST_NAME, manifest_bytes)
        except Exception as exc:
            raise CheckError(
                f"组装压实包失败：{exc}",
                files=[target_label],
            ) from None

    zip_name = (
        f"compaction-{Path(target.uploaded_filename).stem}.zip"
    )
    return CompactionResult(
        manifest=manifest,
        target_entry=target,
        rewrites=rewrites,
        warnings=warnings,
        zip_buffer=buffer.getvalue(),
        zip_name=zip_name,
        manifest_payload=payload,
    )
