from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from checker.compactor import (
    REPLACEMENT_PREFIX,
    ZIP_MANIFEST_NAME,
    run_compaction,
)
from checker.errors import CheckError
from checker.manifest import parse_manifest

SAMPLE = Path(__file__).resolve().parents[1] / "sample"
BASE = "s3://ferry-lake/warehouse/settlement/voyage_date=2026-11-05"


def compact_inputs():
    manifest = parse_manifest((SAMPLE / "manifest-compact.csv").read_bytes())
    uploads = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in [*manifest.data_files, *manifest.position_delete_files]
    }
    return manifest, uploads


def zip_entries(result) -> dict[str, bytes]:
    archive = zipfile.ZipFile(io.BytesIO(result.zip_buffer))
    return {name: archive.read(name) for name in archive.namelist()}


def test_sample_compact_target_data1_removes_rows_and_rewrites_delete_file():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0001.parquet")

    by_name = {r.entry.uploaded_filename: r for r in result.rewrites}
    data_rewrite = by_name["data-0001.parquet"]
    assert data_rewrite.status == "replaced"
    assert (data_rewrite.original_rows, data_rewrite.remaining_rows) == (100, 98)
    assert data_rewrite.removed_rows == 2
    assert data_rewrite.replacement_file == "replaced-data-0001.parquet"

    entries = zip_entries(result)
    table = pq.read_table(io.BytesIO(entries["replaced-data-0001.parquet"]))
    tickets = table.column("ticket_no").to_pylist()
    assert 1003 not in tickets and 4242 not in tickets
    # Remaining rows keep their original relative order.
    assert tickets == sorted(tickets)
    assert len(tickets) == 98

    delete_rewrite = by_name["delete-pos-0001.parquet"]
    assert delete_rewrite.status == "replaced"
    assert delete_rewrite.original_rows == 4
    assert delete_rewrite.remaining_rows == 2
    delete_table = pq.read_table(
        io.BytesIO(entries["replaced-delete-pos-0001.parquet"])
    )
    remaining = delete_table.to_pylist()
    # Records naming other data files survive untouched, in original order.
    assert remaining == [
        {"file_path": f"{BASE}/data/data-0002.parquet", "pos": 3},
        {"file_path": f"{BASE}/data/data-0002.parquet", "pos": 99},
    ]


def test_same_position_delete_file_also_compacts_second_data_file():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0002.parquet")

    by_name = {r.entry.uploaded_filename: r for r in result.rewrites}
    assert by_name["data-0002.parquet"].status == "replaced"
    assert (
        by_name["data-0002.parquet"].original_rows,
        by_name["data-0002.parquet"].remaining_rows,
    ) == (20, 18)

    entries = zip_entries(result)
    table = pq.read_table(io.BytesIO(entries["replaced-data-0002.parquet"]))
    tickets = table.column("ticket_no").to_pylist()
    # Row 3 (ticket 7003) removed via delete-pos-0001 and row 19 (8888)
    # removed via delete-pos-0002.
    assert 7003 not in tickets and 8888 not in tickets
    assert tickets[:4] == [7000, 7001, 7002, 7004]

    delete_table = pq.read_table(
        io.BytesIO(entries["replaced-delete-pos-0001.parquet"])
    )
    # The data-0001 records inside the same delete file stay intact.
    assert delete_table.to_pylist() == [
        {"file_path": f"{BASE}/data/data-0001.parquet", "pos": 3},
        {"file_path": f"{BASE}/data/data-0001.parquet", "pos": 99},
    ]


def test_stale_delete_does_not_remove_rows_but_is_purged_and_retired():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    by_name = {r.entry.uploaded_filename: r for r in result.rewrites}

    # Stale file's sole record points at data-0001: it is purged, the file
    # becomes empty and is retired (not packed), but row 99 is still removed
    # by the fresh delete-pos-0001.
    stale = by_name["delete-pos-stale-0001.parquet"]
    assert stale.status == "retired"
    assert stale.replacement_file is None
    entries = zip_entries(result)
    assert not any("stale" in name for name in entries)
    assert any("小于" in warning for warning in result.warnings)

    # delete-pos-0002 never references data-0001, so it stays unchanged and
    # is neither rewritten nor packed.
    assert by_name["delete-pos-0002.parquet"].status == "unchanged"
    assert "replaced-delete-pos-0002.parquet" not in entries


def test_rewrite_preserves_schema_field_ids_nullability_and_order():
    manifest, uploads = compact_inputs()
    original = pq.ParquetFile(str(SAMPLE / "data-0001.parquet"))
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    entries = zip_entries(result)
    rewritten = pq.ParquetFile(
        io.BytesIO(entries["replaced-data-0001.parquet"])
    )
    assert rewritten.schema_arrow == original.schema_arrow
    expected_ids = {
        "ticket_no": b"1",
        "passenger_name": b"2",
        "fare_cents": b"3",
        "booking_status": b"4",
        "voyage_date": b"5",
    }
    for field in rewritten.schema_arrow:
        assert field.metadata.get(b"PARQUET:field_id") == expected_ids[field.name]

    delete_original = pq.ParquetFile(str(SAMPLE / "delete-pos-0001.parquet"))
    delete_rewritten = pq.ParquetFile(
        io.BytesIO(entries["replaced-delete-pos-0001.parquet"])
    )
    assert delete_rewritten.schema_arrow == delete_original.schema_arrow


def test_replacement_keeps_original_data_sequence_number_in_manifest():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    entries = zip_entries(result)
    payload = json.loads(entries[ZIP_MANIFEST_NAME])

    target_block = next(
        item
        for item in payload["files"]
        if item["uploaded_filename"] == "data-0001.parquet"
    )
    assert target_block["data_sequence_number"] == 7
    assert target_block["status"] == "replaced"
    assert target_block["replacement_file"] == "replaced-data-0001.parquet"

    delete_block = next(
        item
        for item in payload["files"]
        if item["uploaded_filename"] == "delete-pos-stale-0001.parquet"
    )
    assert delete_block["status"] == "retired"
    assert delete_block["replacement_file"] is None
    assert delete_block["data_sequence_number"] == 6

    # Unchanged files are not listed at all.
    listed = {item["uploaded_filename"] for item in payload["files"]}
    assert "delete-pos-0002.parquet" not in listed
    assert payload["target"]["old_path"].endswith("/data/data-0001.parquet")


def test_zip_contains_only_replacement_parquets_and_manifest():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    entries = zip_entries(result)
    assert set(entries) == {
        "replaced-data-0001.parquet",
        "replaced-delete-pos-0001.parquet",
        ZIP_MANIFEST_NAME,
    }
    for name in entries:
        assert not name.startswith("/") and ".." not in name
    assert result.zip_name == "compaction-data-0001.zip"


def test_equality_delete_manifest_is_rejected():
    manifest = parse_manifest((SAMPLE / "manifest.csv").read_bytes())
    uploads = {
        entry.uploaded_filename: SAMPLE / entry.uploaded_filename
        for entry in manifest.required_entries
    }
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "data-0001.parquet")
    assert "delete-eq-0001.parquet" in excinfo.value.files
    assert "等值删除" in excinfo.value.message


def test_missing_replica_aborts_and_names_file():
    manifest, uploads = compact_inputs()
    del uploads["delete-pos-0002.parquet"]
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "data-0002.parquet")
    assert excinfo.value.files == ["delete-pos-0002.parquet"]
    assert "缺失" in excinfo.value.message


def test_corrupt_target_aborts_with_file_name(tmp_path: Path):
    bad = tmp_path / "broken.parquet"
    bad.write_bytes(b"not parquet at all")
    manifest, uploads = compact_inputs()
    uploads["data-0001.parquet"] = bad
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "data-0001.parquet")
    assert excinfo.value.files == ["data-0001.parquet"]
    assert "无法读取数据文件" in excinfo.value.message


def test_corrupt_delete_file_aborts_with_file_name(tmp_path: Path):
    bad = tmp_path / "broken-delete.parquet"
    bad.write_bytes(b"not parquet at all")
    manifest, uploads = compact_inputs()
    uploads["delete-pos-0001.parquet"] = bad
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "data-0001.parquet")
    assert excinfo.value.files == ["delete-pos-0001.parquet"]


def test_unknown_target_is_rejected():
    manifest, uploads = compact_inputs()
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "ghost.parquet")
    assert excinfo.value.files == ["ghost.parquet"]


def test_position_delete_entry_cannot_be_target():
    manifest, uploads = compact_inputs()
    with pytest.raises(CheckError) as excinfo:
        run_compaction(manifest, uploads, "delete-pos-0001.parquet")
    assert excinfo.value.files == ["delete-pos-0001.parquet"]
    assert "data" in excinfo.value.message


def test_sequence_gate_equal_sequence_still_deletes(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "ticket_no": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["a", "b", "c"]),
            }
        ),
        data_path,
    )
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": pa.array(
                    ["s3://b/data.parquet", "s3://b/data.parquet"]
                ),
                "pos": pa.array([0, 2], type=pa.int64()),
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,5\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,5\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_compaction(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        "data.parquet",
    )
    entries = zip_entries(result)
    table = pq.read_table(io.BytesIO(entries["replaced-data.parquet"]))
    assert table.column("ticket_no").to_pylist() == [2]
    # All delete records named the target -> delete file is retired.
    statuses = {r.entry.uploaded_filename: r.status for r in result.rewrites}
    assert statuses["delete.parquet"] == "retired"


def test_lower_sequence_delete_keeps_rows_but_still_purges_record(
    tmp_path: Path,
):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1, 2, 3]}), data_path)
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {"file_path": ["s3://b/data.parquet"], "pos": [1]}
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,9\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,4\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_compaction(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        "data.parquet",
    )
    entries = zip_entries(result)
    table = pq.read_table(io.BytesIO(entries["replaced-data.parquet"]))
    assert table.num_rows == 3  # stale delete removes nothing
    statuses = {r.entry.uploaded_filename: r.status for r in result.rewrites}
    # But the stale record naming the target is still purged -> retired.
    assert statuses["delete.parquet"] == "retired"


def test_fully_deleted_data_file_is_retired_and_not_packed(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1, 2]}), data_path)
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": ["s3://b/data.parquet", "s3://b/data.parquet"],
                "pos": [0, 1],
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,3\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,8\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_compaction(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        "data.parquet",
    )
    entries = zip_entries(result)
    assert "replaced-data.parquet" not in entries
    statuses = {r.entry.uploaded_filename: r.status for r in result.rewrites}
    assert statuses["data.parquet"] == "retired"
    assert statuses["delete.parquet"] == "retired"
    payload = json.loads(entries[ZIP_MANIFEST_NAME])
    data_block = next(
        item for item in payload["files"] if item["uploaded_filename"] == "data.parquet"
    )
    assert data_block["status"] == "retired"
    assert data_block["remaining_rows"] == 0


def test_out_of_range_position_is_kept_in_data_but_purged(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1, 2]}), data_path)
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": [
                    "s3://b/data.parquet",
                    "s3://b/other.parquet",
                ],
                "pos": [40, 7],
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,3\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,8\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_compaction(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        "data.parquet",
    )
    entries = zip_entries(result)
    data_table = pq.read_table(io.BytesIO(entries["replaced-data.parquet"]))
    assert data_table.num_rows == 2  # nothing valid to delete
    delete_table = pq.read_table(
        io.BytesIO(entries["replaced-delete.parquet"])
    )
    # Out-of-range target record purged; the other-path record survives.
    assert delete_table.to_pylist() == [
        {"file_path": "s3://b/other.parquet", "pos": 7}
    ]
    assert any("超出" in warning for warning in result.warnings)


def test_unused_uploads_are_warned_not_fatal(tmp_path: Path):
    manifest, uploads = compact_inputs()
    extra = tmp_path / "extra.parquet"
    pq.write_table(pa.table({"x": [1]}), extra)
    uploads["extra.parquet"] = extra
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    assert any("extra.parquet" in warning for warning in result.warnings)


def test_no_delete_references_target_aborts(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1]}), data_path)
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {"file_path": ["s3://b/other.parquet"], "pos": [0]}
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,3\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,8\n"
    )
    manifest = parse_manifest(manifest_text)
    with pytest.raises(CheckError, match="无需压实"):
        run_compaction(
            manifest,
            {"data.parquet": data_path, "delete.parquet": delete_path},
            "data.parquet",
        )


def test_duplicate_positions_delete_each_row_once(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1, 2, 3]}), data_path)
    delete_path = tmp_path / "delete.parquet"
    pq.write_table(
        pa.table(
            {
                "file_path": ["s3://b/data.parquet", "s3://b/data.parquet"],
                "pos": [0, 0],
            }
        ),
        delete_path,
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,3\n"
        "s3://b/delete.parquet,delete.parquet,position_delete,8\n"
    )
    manifest = parse_manifest(manifest_text)
    result = run_compaction(
        manifest,
        {"data.parquet": data_path, "delete.parquet": delete_path},
        "data.parquet",
    )
    rewrite = result.rewrites[0]
    assert rewrite.removed_rows == 1
    entries = zip_entries(result)
    assert pq.read_table(
        io.BytesIO(entries["replaced-data.parquet"])
    ).num_rows == 2


def test_replacement_names_are_flat_and_prefixed():
    manifest, uploads = compact_inputs()
    result = run_compaction(manifest, uploads, "data-0001.parquet")
    for rewrite in result.packed_rewrites:
        assert rewrite.replacement_file.startswith(REPLACEMENT_PREFIX)
        assert "/" not in rewrite.replacement_file
        assert ".." not in rewrite.replacement_file


def test_delete_file_with_wrong_path_column_type_aborts(tmp_path: Path):
    data_path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"ticket_no": [1, 2]}), data_path)
    bad_delete = tmp_path / "bad-delete.parquet"
    pq.write_table(
        pa.table({"file_path": [1, 2], "pos": [0, 1]}), bad_delete
    )
    manifest_text = (
        "original_path,uploaded_filename,file_type,data_sequence_number\n"
        "s3://b/data.parquet,data.parquet,data,3\n"
        "s3://b/bad-delete.parquet,bad-delete.parquet,position_delete,8\n"
    )
    manifest = parse_manifest(manifest_text)
    with pytest.raises(CheckError) as excinfo:
        run_compaction(
            manifest,
            {"data.parquet": data_path, "bad-delete.parquet": bad_delete},
            "data.parquet",
        )
    assert excinfo.value.files == ["bad-delete.parquet"]
    assert "file_path" in excinfo.value.message


def test_empty_target_filename_is_rejected():
    manifest, uploads = compact_inputs()
    with pytest.raises(CheckError, match="目标数据文件"):
        run_compaction(manifest, uploads, "  ")
