from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest

from app import app

SAMPLE = Path(__file__).resolve().parents[1] / "sample"


@pytest.fixture()
def client():
    app.config.update(TESTING=True)
    with app.test_client() as client:
        yield client


def _all_replica_files():
    files = []
    for name in (
        "data-0001.parquet",
        "data-0002.parquet",
        "delete-pos-0001.parquet",
        "delete-pos-0002.parquet",
        "delete-pos-stale-0001.parquet",
    ):
        files.append(
            (
                io.BytesIO((SAMPLE / name).read_bytes()),
                name,
                "application/octet-stream",
            )
        )
    return files


def test_index_shows_offline_compaction_entry(client):
    html = client.get("/").get_data(as_text=True)
    assert "离线压实" in html
    assert "/compact" in html
    assert "生成压实包" in html
    # Both sample targets are reachable from the page.
    assert "/sample/compact?target=data-0001.parquet" in html
    assert "/sample/compact?target=data-0002.parquet" in html


def test_sample_compaction_download_for_first_target(client):
    response = client.get("/sample/compact?target=data-0001.parquet")
    assert response.status_code == 200
    assert response.content_type == "application/zip"
    assert (
        response.headers["Content-Disposition"]
        == "attachment; filename=compaction-data-0001.zip"
    )
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    assert set(archive.namelist()) == {
        "replaced-data-0001.parquet",
        "replaced-delete-pos-0001.parquet",
        "compaction-manifest.json",
    }
    payload = json.loads(archive.read("compaction-manifest.json"))
    assert payload["target"]["uploaded_filename"] == "data-0001.parquet"
    assert payload["target"]["data_sequence_number"] == 7


def test_sample_compaction_download_for_second_target(client):
    response = client.get("/sample/compact?target=data-0002.parquet")
    assert response.status_code == 200
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    payload = json.loads(archive.read("compaction-manifest.json"))
    assert payload["target"]["uploaded_filename"] == "data-0002.parquet"
    # Same delete file participates on both targets; here its data-0001
    # records must survive.
    names = archive.namelist()
    assert "replaced-delete-pos-0001.parquet" in names


def test_sample_compaction_bad_target_renders_compact_error(client):
    response = client.get("/sample/compact?target=missing.parquet")
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "压实未完成" in html
    assert "missing.parquet" in html
    assert "没有生成任何压实包" in html


def test_upload_compaction_round_trip(client):
    response = client.post(
        "/compact",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest-compact.csv").read_bytes()),
                "manifest-compact.csv",
            ),
            "files": _all_replica_files(),
            "target_file": "data-0001.parquet",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 200
    assert response.content_type == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(response.data))
    payload = json.loads(archive.read("compaction-manifest.json"))
    statuses = {item["uploaded_filename"]: item["status"] for item in payload["files"]}
    assert statuses["data-0001.parquet"] == "replaced"
    assert statuses["delete-pos-0001.parquet"] == "replaced"
    assert statuses["delete-pos-stale-0001.parquet"] == "retired"


def test_upload_compaction_rejects_equality_delete_manifest(client):
    response = client.post(
        "/compact",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest.csv").read_bytes()),
                "manifest.csv",
            ),
            "files": _all_replica_files(),
            "target_file": "data-0001.parquet",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "压实未完成" in html
    assert "等值删除" in html
    assert "delete-eq-0001.parquet" in html


def test_upload_compaction_missing_replica_names_file(client):
    files = [
        item
        for item in _all_replica_files()
        if item[1] != "delete-pos-0002.parquet"
    ]
    response = client.post(
        "/compact",
        data={
            "manifest": (
                io.BytesIO((SAMPLE / "manifest-compact.csv").read_bytes()),
                "manifest-compact.csv",
            ),
            "files": files,
            "target_file": "data-0002.parquet",
        },
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    html = response.get_data(as_text=True)
    assert "压实未完成" in html
    assert "delete-pos-0002.parquet" in html


def test_upload_compaction_without_manifest_is_rejected(client):
    response = client.post(
        "/compact",
        data={"files": _all_replica_files(), "target_file": "data-0001.parquet"},
        content_type="multipart/form-data",
    )
    assert response.status_code == 400
    assert "压实未完成" in response.get_data(as_text=True)


def test_checker_still_works_alongside_compaction(client):
    response = client.get("/sample")
    assert response.status_code == 200
    assert "核对结果" in response.get_data(as_text=True)
