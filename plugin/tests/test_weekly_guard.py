from __future__ import annotations

import hashlib
import json
from pathlib import Path

from scripts.weekly_guard import build_freeze_manifest, publish_weekly, verify_freeze_manifest


def test_independent_acceptance_single_file_is_sealed(tmp_path):
    plugin=tmp_path/"plugin"
    plugin.mkdir()
    (plugin/"a.py").write_text("safe")
    outside=tmp_path/"outside.py"
    outside.write_text("accepted")
    manifest=tmp_path/"freeze.json"
    payload=build_freeze_manifest([("plugin",plugin)],manifest)
    payload["single_files"]={"independent_acceptance":{
        "path":str(outside),"sha256":_sha(outside)}}
    manifest.write_text(json.dumps(payload))
    assert verify_freeze_manifest(manifest)["ok"]
    outside.write_text("changed")
    assert not verify_freeze_manifest(manifest)["ok"]


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_weekly_publication_never_rewrites_base_and_quarantines_history(tmp_path):
    source = tmp_path / "runtime"
    database = tmp_path / "database"
    plugin = tmp_path / "plugin"
    skill = tmp_path / "skill"
    plugin.mkdir()
    skill.mkdir()
    (plugin / "plugin.txt").write_text("frozen", encoding="utf-8")
    (skill / "SKILL.md").write_text("frozen", encoding="utf-8")
    freeze = tmp_path / "freeze.json"
    build_freeze_manifest([("plugin", plugin), ("skill", skill)], freeze)

    base_path = database / "cache/dividends_hk/00700.json"
    _write(base_path, {
        "symbol": "00700.HK",
        "records": [
            {"id": "old", "ex_date": "2026-07-03", "amount": 1.0},
        ],
    })
    base_sha = _sha(base_path)
    _write(source / ".cache/dividends_hk/00700.json", {
        "symbol": "00700.HK",
        "records": [
            {"id": "old", "ex_date": "2026-07-03", "amount": 2.0},
            {"id": "backfill", "ex_date": "2026-06-01", "amount": 3.0},
            {"id": "weekly", "ex_date": "2026-08-28", "amount": 4.0},
        ],
    })

    result = publish_weekly(
        source,
        database,
        "2026-08-28",
        publish=True,
        authorization="PUBLISH_CURRENT_WEEK",
        freeze_manifest=freeze,
    )

    assert result["status"] == "PUBLISHED"
    assert result["formal_existing_files_rewritten"] == 0
    assert result["historical_revisions_published"] == 0
    assert result["historical_backfills_published"] == 0
    assert _sha(base_path) == base_sha
    run_root = next((database / "state/weekly_runs").iterdir())
    assert (run_root / "MANIFEST_SHA256_SIZE.txt").is_file()
    assert (run_root / "incremental_catalog.json").is_file()

    published = list((database / "state/published_versions/cache/dividends_hk/00700.json").glob("*.json"))
    assert len(published) == 1
    assert json.loads(published[0].read_text(encoding="utf-8"))["append_records"]["records"] == [
        {"id": "weekly", "ex_date": "2026-08-28", "amount": 4.0}
    ]
    quarantined = [
        path for path in (database / "state/quarantine").rglob("*.json")
        if path.is_file()
    ]
    kinds = {json.loads(path.read_text(encoding="utf-8"))["kind"] for path in quarantined}
    assert kinds == {"historical_revision", "historical_backfill"}


def test_second_run_does_not_publish_duplicate_weekly_record(tmp_path):
    source = tmp_path / "runtime"
    database = tmp_path / "database"
    seal = tmp_path / "sealed"
    seal.mkdir()
    (seal / "fixed.txt").write_text("fixed", encoding="utf-8")
    freeze = tmp_path / "freeze.json"
    build_freeze_manifest([("sealed", seal)], freeze)
    _write(database / "cache/news/00700.json", {"records": []})
    _write(source / ".cache/news/00700.json", {
        "records": [{"id": "n1", "date": "2026-08-28", "title": "new"}],
    })

    first = publish_weekly(
        source, database, "2026-08-28", publish=True,
        authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze,
    )
    second = publish_weekly(
        source, database, "2026-08-28", publish=True,
        authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze,
    )

    assert first["counts"]["weekly_append"] == 1
    assert second["counts"].get("weekly_append", 0) == 0


def test_freeze_drift_blocks_before_any_write(tmp_path):
    source = tmp_path / "runtime"
    database = tmp_path / "database"
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    target = sealed / "fixed.txt"
    target.write_text("fixed", encoding="utf-8")
    freeze = tmp_path / "freeze.json"
    build_freeze_manifest([("sealed", sealed)], freeze)
    target.write_text("drift", encoding="utf-8")
    _write(source / ".cache/news/00700.json", {"records": []})

    result = publish_weekly(
        source, database, "2026-08-28", publish=True,
        authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze,
    )

    assert result["status"] == "BLOCKED_PLUGIN_OR_SKILL_DRIFT"
    assert not database.exists()


def test_outside_fixed_60_and_missing_authorization_block_writes(tmp_path):
    source = tmp_path / "runtime"
    database = tmp_path / "database"
    sealed = tmp_path / "sealed"
    sealed.mkdir()
    (sealed / "fixed.txt").write_text("fixed", encoding="utf-8")
    freeze = tmp_path / "freeze.json"
    build_freeze_manifest([("sealed", sealed)], freeze)
    _write(source / ".cache/news/99999.json", {
        "records": [{"date": "2026-08-28", "title": "outside"}],
    })

    leaked = publish_weekly(
        source, database, "2026-08-28", publish=True,
        authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze,
        allowed_stems={"00700"},
    )
    assert leaked["status"] == "BLOCKED_OUTSIDE_FIXED_60"
    assert not database.exists()

    (source / ".cache/news/99999.json").unlink()
    _write(source / ".cache/news/00700.json", {"records": []})
    unauthorized = publish_weekly(
        source, database, "2026-08-28", publish=True,
        authorization="", freeze_manifest=freeze,
        allowed_stems={"00700"},
    )
    assert unauthorized["status"] == "BLOCKED_MISSING_CURRENT_WEEK_AUTHORIZATION"
    assert not database.exists()
