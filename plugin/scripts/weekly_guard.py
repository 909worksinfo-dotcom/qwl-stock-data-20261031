"""Append-only weekly publication guard for the fixed-60 QWL database

Collectors may replace their working-cache files with a provider's complete
response.  This module keeps that mutable work area outside the formal
database and publishes only additive weekly deltas.  Existing formal files are
never rewritten
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Callable
from scripts import weekly_record_contract as record_contract


PUBLISH_TOKEN = "PUBLISH_CURRENT_WEEK"
RECORD_KEY_CONTRACT = "typed-v3"
DEFAULT_DATABASE_ROOT = Path(
    "/Users/bytedance/Desktop/60只股票全量历史数据_20260816"
)
DEFAULT_FREEZE_MANIFEST = Path(
    "/Users/bytedance/.codex/qwl-stock-data-freeze/frozen_sources.json"
)

_LIST_FIELDS = {
    "records", "rows", "items", "events", "announcements", "reports",
    "balance", "income", "cashflow", "data", "daily_history", "versions", "intervals",
}
_IDENTIFIER_FIELDS = (
    "id", "event_id", "announcement_id", "report_id", "notice_id",
    "公告编号", "资讯代码", "art_code",
)
_EVENT_DATE_FIELDS = (
    "event_date", "ex_date", "trade_date", "date", "announcement_date",
    "notice_date", "publish_date", "effective_date", "record_date",
    "timestamp", "time", "as_of", "as_of_date", "source_published_on",
    "effective_from", "交易日期",
)
_AVAILABLE_FIELDS = (
    "available_on", "available_at", "published_at", "publish_time",
    "announcement_time",
)
_SECONDARY_KEY_FIELDS = (
    "type", "event_type", "category", "title", "report_date", "period",
    "end_date", "fiscal_year", "fiscal_period", "currency",
    "dataset", "section",
)
_SYMBOL_SCOPED_DATASETS = {
    "announcements", "block_trades_a", "consensus", "dividends",
    "dividends_hk", "earnings_calendar", "financial_versions",
    "fund_flow_ths", "fundamentals", "hk_action_evidence",
    "hk_market_disclosures", "hk_share_snapshot", "hk_short_selling",
    "listing", "margin", "microstructure", "news", "northbound",
    "ownership_a", "ownership_hk", "repurchases_a", "share_changes",
    "qfq_basis_evidence",
    "southbound", "statements", "statements_hk", "suspensions",
}


def _canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_value(value: Any) -> str:
    return _sha_bytes(_canonical(value).encode("utf-8"))

def _record_sha(record: dict) -> str:
    # Ingestion provenance changes on every full response, but is not a
    # business-value revision. Original provenance remains in immutable snapshots.
    return _sha_value({k:v for k,v in record.items() if k not in {
        "captured_at", "fetched_at", "ingested_at", "updated_at", "source_sha256", "序号"}})


def _parse_day(value: Any) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text[:10].replace("/", "-")
    if len(text) == 8 and text.isdigit():
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def week_window(as_of_date: str) -> tuple[date, date]:
    end = date.fromisoformat(as_of_date)
    start = end.fromordinal(end.toordinal() - end.weekday())
    return start, end


def _first_day(record: dict, fields: Iterable[str]) -> date | None:
    for field in fields:
        parsed = _parse_day(record.get(field))
        if parsed is not None:
            return parsed
    return None


def _business_key(record: dict) -> str:
    if record.get("content_hash") and record.get("dataset"):
        return _canonical({k:record.get(k) for k in
                           ("dataset", "section", "report_date", "content_hash")})
    if "交易日期" in record and "买方营业部" in record:
        # A day can contain multiple block trades, including the same broker
        # pair. Provider row ordinals are unstable across query windows.
        return _canonical({k:record.get(k) for k in (
            "交易日期", "证券代码", "买方营业部", "卖方营业部", "成交价", "成交量")})
    for field in _IDENTIFIER_FIELDS:
        value = record.get(field)
        if value not in (None, ""):
            return f"{field}:{value}"
    primary = _first_day(record, _EVENT_DATE_FIELDS)
    parts = [("event_day", primary.isoformat() if primary else None)]
    for field in _SECONDARY_KEY_FIELDS:
        value = record.get(field)
        if value not in (None, ""):
            parts.append((field, value))
    if len(parts) == 1 and primary is None:
        return f"content:{_sha_value(record)}"
    return _canonical(parts)


def _record_class(record: dict, week_start: date, week_end: date) -> str:
    event_day = _first_day(record, _EVENT_DATE_FIELDS)
    available_day = _first_day(record, _AVAILABLE_FIELDS)
    if event_day is not None and event_day < week_start:
        return "historical"
    if event_day is None:
        return "undated"
    if event_day > week_end or (available_day and available_day > week_end):
        return "future"
    if week_start <= event_day <= week_end:
        return "current_week"
    return "historical"


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _freeze_files(root: Path) -> list[Path]:
    return [
        path for path in sorted(root.rglob("*"))
        if path.is_file()
        and not any(
            part in {"__pycache__", ".pytest_cache", ".git"}
            for part in path.parts
        )
        and path.name not in {".DS_Store", "frozen_sources.json"}
        and path.suffix != ".pyc"
    ]


def _list_fields(document: Any, relative: Path | None = None) -> dict[str, list[dict]]:
    if not isinstance(document, dict):
        return {}
    result = {}
    for key, value in document.items():
        allowed = key in _LIST_FIELDS or (key == 'historical_records' and relative is not None
            and relative.parts[:2] == ('cache', 'hk_short_selling'))
        if allowed and isinstance(value, list) and all(
            isinstance(item, dict) for item in value
        ):
            result[key] = value
    return result


def _effective_records(database_root: Path, relative: Path, base: Any) -> dict[str, list[dict]]:
    fields = {key: list(value) for key, value in _list_fields(base).items()}
    overlay = database_root / "state/published_versions" / relative
    if overlay.exists():
        for delta_path in sorted(overlay.glob("*.json")):
            delta = _json(delta_path)
            # v1.2 stored some daily series as full document snapshots.
            # Import only absent keys from those legacy snapshots, never
            # replace established historical records while resolving a view.
            for field, records in _list_fields(delta.get("document", {})).items():
                current = fields.setdefault(field, [])
                keys = {_business_key(r): r for r in current}
                for record in records:
                    key = _business_key(record)
                    if key not in keys:
                        current.append(record)
                        keys[key] = record
                    elif _record_sha(keys[key]) != _record_sha(record):
                        day = _first_day(record, (*_EVENT_DATE_FIELDS, "report_date"))
                        if day is None or day.isoformat() >= delta.get("week_start", ""):
                            raise ValueError(f"conflicting legacy current-week JSON key: {relative}:{key}")
            for field, records in delta.get("append_records", {}).items():
                current = fields.setdefault(field, list(base.get(field, [])) if
                    field == 'historical_records' and delta.get('record_key_contract') == 'typed-v3' else [])
                if delta.get("schema_version", 1) < 2:
                    # Preserve prior official v1 event multiplicity. Some
                    # providers expose no unique trade ID; do not delete or
                    # relabel already published evidence while reading it.
                    current.extend(records)
                    continue
                schema = delta.get("schema_version", 1)
                typed = schema >= 3
                # Announcement URL identity was introduced after existing v3
                # versions had already been created under the legacy key.  Do
                # not reinterpret those immutable versions retroactively.
                if (relative.parts[:2] == ("cache", "announcements")
                        and delta.get("record_key_contract") not in {'typed-v2', 'typed-v3'}):
                    typed = False
                if (relative.parts[:2] in {('cache', 'earnings_calendar'), ('cache', 'hk_short_selling')}
                        and delta.get('record_key_contract') != 'typed-v3'):
                    typed = False
                key_of = ((lambda r: record_contract.business_key(r, relative, _business_key))
                          if typed else _business_key)
                keys = {key_of(r) for r in current}
                for record in records:
                    key = key_of(record)
                    if key in keys:
                        raise ValueError(f"duplicate/conflicting formal JSON key: {relative}:{key}")
                    keys.add(key)
                    current.append(record)
    return fields


def effective_json(database_root: Path, relative: Path) -> dict:
    target = database_root / relative
    base = _json(target) if target.is_file() else {}
    if not isinstance(base, dict):
        raise ValueError(f"unsupported JSON root: {relative}")
    document = dict(base)
    overlay = database_root / "state/published_versions" / relative
    for delta_path in sorted(overlay.glob("*.json")):
        delta = _json(delta_path)
        if delta.get("kind") == "document_version" and "document" in delta:
            document = delta["document"]
        document.update(delta.get("metadata", {}))
    document.update(_effective_records(database_root, relative, base))
    return document


@dataclass
class Change:
    relative: str
    kind: str
    payload: dict[str, Any]


def _plan_json(
    source: Path,
    target: Path,
    relative: Path,
    database_root: Path,
    week_start: date,
    week_end: date,
) -> list[Change]:
    candidate = _json(source)
    if relative.parts[:2] == ("cache", "qfq_basis_evidence"):
        return [Change(str(relative), "qfq_overlap_drift", {
            "evidence": candidate,
            "authorization_required": True,
            "historical_records_published": 0,
        })]
    base = effective_json(database_root, relative)
    candidate_fields = _list_fields(candidate, relative)
    if not candidate_fields:
        if _sha_value(candidate) == _sha_value(base):
            return []
        return [Change(str(relative), "document_version" if _record_class(candidate, week_start, week_end) == "current_week" else "undated_document", {
            "source_sha256": _sha_value(candidate),
            "document": candidate,
        })]

    effective = _list_fields(base, relative)
    key_of = lambda r: record_contract.business_key(r, relative, _business_key)
    append_records: dict[str, list[dict]] = {}
    revisions: list[dict] = []
    quarantine: list[dict] = []
    invalid_current: list[dict] = []
    for field, records in candidate_fields.items():
        stored = effective.get(field, [])
        by_key: dict[str, list[dict]] = {}
        exact = {_record_sha(item) for item in stored}
        for item in stored:
            by_key.setdefault(key_of(item), []).append(item)
        for item in records:
            item_hash = _record_sha(item)
            if item_hash in exact:
                continue
            key = key_of(item)
            classification = _record_class(record_contract.classification_record(item, relative), week_start, week_end)
            if (relative.parts[:2] == ('cache', 'hk_short_selling') and field == 'historical_records'
                    and _first_day(item, ('available_on', 'available_at')) is None):
                classification = 'availability_unverified'
            if any(record_contract.equivalent(old, item, relative, candidate) for old in by_key.get(key, [])):
                continue
            if key in by_key:
                revisions.append({
                    "field": field,
                    "business_key": key,
                    "classification": classification,
                    "previous": by_key[key],
                    "candidate": item,
                })
            elif classification == "current_week":
                if relative.parts[:2] == ('cache', 'benchmarks') and relative.name.startswith('industry_'):
                    from qwl.datasources.controlled_benchmarks import METHOD
                    if not isinstance(item.get('derivation'), dict) or item['derivation'].get('method') != METHOD:
                        invalid_current.append({'date': item.get('date'), 'reason': 'new_benchmark_missing_controlled_provenance'})
                        continue
                append_records.setdefault(field, []).append(item)
                exact.add(item_hash)
                by_key.setdefault(key, []).append(item)
            else:
                quarantine.append({
                    "field": field,
                    "business_key": key,
                    "classification": classification,
                    "candidate": item,
                })
    changes = []
    if invalid_current:
        changes.append(Change(str(relative), 'invalid_source', {'issues': invalid_current}))
    if append_records:
        changes.append(Change(str(relative), "weekly_append", {
            "append_records": append_records,
            "metadata": {k: v for k, v in candidate.items()
                         if k not in candidate_fields and not isinstance(v, (dict, list))},
        }))
    else:
        # A repaired provider failure can return the same monthly observations.
        # Record only the new collection status in this week's version, never
        # overwrite those older observations or re-publish their rows.
        status_metadata = {
            k: candidate.get(k) for k in ("error", "errors", "provider_errors")
            if k in base and base.get(k) and not candidate.get(k)}
        if status_metadata:
            changes.append(Change(str(relative), "weekly_append", {
                "append_records": {}, "metadata": status_metadata,
                "version_basis": "current_week_collection_status"}))
    if revisions:
        changes.append(Change(str(relative), "historical_revision", {
            "revisions": revisions,
            "authorization_required": True,
        }))
    if quarantine:
        changes.append(Change(str(relative), "historical_backfill", {
            "records": quarantine,
            "authorization_required": True,
        }))
    return changes


def parquet_times(frame):
    import pandas as pd
    if isinstance(frame.index, pd.DatetimeIndex):
        return pd.DatetimeIndex(frame.index)
    column = next((c for c in ("date", "trade_date", "timestamp", "datetime",
                              "source_published_on") if c in frame.columns), None)
    if column is None:
        return pd.DatetimeIndex([pd.NaT] * len(frame))
    return pd.DatetimeIndex(pd.to_datetime(frame[column], errors="coerce"))


def parquet_days(frame):
    return parquet_times(frame).normalize()


def parquet_keys(frame):
    times = parquet_times(frame)
    keys = []
    for pos, row in enumerate(frame.to_dict("records")):
        if row.get("report_url"):
            keys.append(_canonical({k: row.get(k) for k in
                                    ("report_url", "fiscal_year", "fiscal_period")}))
        else:
            keys.append(str(times[pos]) if str(times[pos]) != "NaT" else _business_key(row))
    return keys


def effective_parquet(target: Path, overlay: Path | None = None):
    import pandas as pd
    frames = [pd.read_parquet(target)] if target.is_file() else []
    if overlay:
        frames.extend(pd.read_parquet(p) for p in sorted(overlay.glob("*.parquet")) if p.is_file())
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames)
    keys = parquet_keys(combined)
    if len(set(keys)) != len(keys):
        raise ValueError(f"duplicate/conflicting formal Parquet key: {target.name}")
    return combined


def _parquet_plan(source: Path, target: Path, week_start: date, week_end: date,
                  overlay: Path | None = None) -> dict[str, Any]:
    import pandas as pd

    incoming = pd.read_parquet(source)
    incoming_days = parquet_days(incoming)
    stored = effective_parquet(target, overlay)
    stored_by_day = {key: pos for pos, key in enumerate(parquet_keys(stored))}
    incoming_keys = parquet_keys(incoming)
    append_positions = []
    revisions = []
    quarantine = []
    seen_incoming = {}
    for pos, day in enumerate(incoming_days):
        day_key = None if pd.isna(day) else str(day.date())
        business_key = incoming_keys[pos]
        new = incoming.iloc[pos].to_dict()
        if business_key in seen_incoming:
            if seen_incoming[business_key] != _record_sha(new):
                raise ValueError(f"conflicting source Parquet key: {source.name}:{business_key}")
            continue
        seen_incoming[business_key] = _record_sha(new)
        if business_key in stored_by_day:
            old = stored.iloc[stored_by_day[business_key]].to_dict()
            new = incoming.iloc[pos].to_dict()
            if _record_sha(old) != _record_sha(new):
                revisions.append({"date": day_key, "timestamp": business_key, "previous": old, "candidate": new})
        elif day_key and week_start <= day.date() <= week_end and (
            not _first_day(new, _AVAILABLE_FIELDS) or _first_day(new, _AVAILABLE_FIELDS) <= week_end
        ):
            append_positions.append(pos)
        else:
            quarantine.append({"date": day_key, "row": incoming.iloc[pos].to_dict()})
    return {
        "kind": "parquet_delta",
        "append_positions": append_positions,
        "revisions": revisions,
        "quarantine": quarantine,
        "incoming": incoming,
    }


def verify_freeze_manifest(manifest_path: Path = DEFAULT_FREEZE_MANIFEST) -> dict[str, Any]:
    if not manifest_path.is_file():
        return {"ok": False, "error": f"freeze manifest missing: {manifest_path}"}
    manifest = _json(manifest_path)
    if not manifest.get("roots") or any(not r.get("files") for r in manifest["roots"]):
        return {"ok": False, "error": "empty freeze manifest"}
    drift = []
    for root in manifest.get("roots", []):
        base = Path(root["path"])
        expected_files = root.get("files", {})
        actual_relatives = {str(path.relative_to(base)) for path in _freeze_files(base)} if base.is_dir() else set()
        for relative in sorted(actual_relatives - set(expected_files)):
            drift.append({
                "root": root.get("name"),
                "relative": relative,
                "expected": None,
                "actual": "UNSEALED_FILE",
            })
        for relative, expected in expected_files.items():
            path = base / relative
            actual = _sha_bytes(path.read_bytes()) if path.is_file() else None
            if actual != expected:
                drift.append({"root": root.get("name"), "relative": relative, "expected": expected, "actual": actual})
    for name, item in manifest.get("single_files", {}).items():
        path = Path(item["path"])
        actual = _sha_bytes(path.read_bytes()) if path.is_file() else None
        if actual != item.get("sha256"):
            drift.append({"root": name, "relative": str(path),
                          "expected": item.get("sha256"), "actual": actual})
    return {
        "ok": not drift,
        "manifest": str(manifest_path),
        "sealed_at": manifest.get("sealed_at"),
        "file_count": sum(len(root.get("files", {})) for root in manifest.get("roots", []))
                      + len(manifest.get("single_files", {})),
        "drift": drift,
    }


def _source_files(source_root: Path, collected_after: str | None) -> list[tuple[Path, Path]]:
    threshold = datetime.fromisoformat(collected_after).timestamp() if collected_after else None
    mappings = ((source_root / ".cache", Path("cache")), (source_root / "state/history", Path("state/history")))
    files = []
    for source_dir, target_prefix in mappings:
        if not source_dir.exists():
            continue
        for path in source_dir.rglob("*"):
            if not path.is_file() or "__pycache__" in path.parts or path.name == ".DS_Store":
                continue
            if threshold is not None and path.stat().st_mtime + 1 < threshold:
                continue
            files.append((path, target_prefix / path.relative_to(source_dir)))
    return sorted(files, key=lambda pair: str(pair[1]))


def file_inventory(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): _sha_bytes(p.read_bytes())
            for p in root.rglob("*") if p.is_file()}


def verify_existing(root: Path, before: dict[str, str]) -> list[str]:
    return [rel for rel, digest in before.items()
            if not (root / rel).is_file() or _sha_bytes((root / rel).read_bytes()) != digest]


def publish_weekly(
    source_root: Path,
    database_root: Path,
    as_of_date: str,
    *,
    publish: bool = False,
    authorization: str = "",
    collected_after: str | None = None,
    freeze_manifest: Path = DEFAULT_FREEZE_MANIFEST,
    allowed_stems: set[str] | None = None,
    acceptance: Callable | None = None,
) -> dict[str, Any]:
    freeze = verify_freeze_manifest(freeze_manifest)
    if not freeze["ok"]:
        return {"status": "BLOCKED_PLUGIN_OR_SKILL_DRIFT", "freeze": freeze}
    if publish and authorization != PUBLISH_TOKEN:
        return {"status": "BLOCKED_MISSING_CURRENT_WEEK_AUTHORIZATION", "freeze": freeze}
    if publish:
        orphaned = [str(p) for p in (database_root / "state/weekly_runs").glob("*")
                    if p.is_dir() and not p.name.endswith(".failed")
                    and not (p / "publication.json").is_file()]
        if orphaned:
            return {"status": "BLOCKED_INTERRUPTED_PUBLICATION", "runs": orphaned}
    week_start, week_end = week_window(as_of_date)
    run_id = f"{week_start.isoformat()}_{week_end.isoformat()}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    changes: list[Change] = []
    parquet_plans: list[tuple[Path, Path, dict[str, Any]]] = []
    source_files = _source_files(source_root, collected_after)
    leaked = []
    if allowed_stems is not None:
        for _, relative in source_files:
            parts = relative.parts
            stem = relative.stem.removesuffix("_raw")
            if (
                len(parts) >= 3
                and parts[0] == "cache"
                and parts[1] in _SYMBOL_SCOPED_DATASETS
                and stem not in allowed_stems
            ) or (
                len(parts) >= 3
                and parts[:2] == ("state", "history")
                and stem not in allowed_stems
            ):
                leaked.append(str(relative))
    if leaked:
        return {
            "status": "BLOCKED_OUTSIDE_FIXED_60",
            "outside_fixed_60": leaked,
            "freeze": freeze,
        }
    for source, relative in source_files:
        target = database_root / relative
        if source.suffix.lower() == ".json":
            try:
                changes.extend(_plan_json(source, target, relative, database_root, week_start, week_end))
            except (OSError, json.JSONDecodeError) as exc:
                changes.append(Change(str(relative), "invalid_source", {"error": str(exc)}))
        elif source.suffix.lower() == ".parquet":
            parquet_plans.append((source, relative, _parquet_plan(
                source, target, week_start, week_end,
                database_root / "state/published_versions" / relative)))
        elif not target.is_file() or _sha_bytes(source.read_bytes()) != _sha_bytes(target.read_bytes()):
            changes.append(Change(str(relative), "undated_document", {"bytes": source.stat().st_size, "source_sha256": _sha_bytes(source.read_bytes())}))

    counts: dict[str, int] = {}
    for change in changes:
        counts[change.kind] = counts.get(change.kind, 0) + 1
    for _, _, plan in parquet_plans:
        counts[plan["kind"]] = counts.get(plan["kind"], 0) + 1
        if plan.get("revisions"):
            counts["historical_revision"] = counts.get("historical_revision", 0) + len(plan["revisions"])
        if plan.get("quarantine"):
            counts["historical_backfill"] = counts.get("historical_backfill", 0) + len(plan["quarantine"])

    summary = {
        "status": "PREVIEW" if not publish else "PUBLISHING",
        "run_id": run_id,
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "source_file_count": len(source_files),
        "counts": counts,
        "formal_existing_files_rewritten": 0,
        "historical_revisions_published": 0,
        "historical_backfills_published": 0,
        "freeze": freeze,
    }
    if not publish:
        summary["changes"] = [{"relative": c.relative, "kind": c.kind} for c in changes]
        return summary

    if any(c.kind == "invalid_source" for c in changes):
        return {**summary, "status": "BLOCKED_INVALID_SOURCE"}
    before = file_inventory(database_root)

    run_root = database_root / "state/weekly_runs" / run_id
    snapshot_root = run_root / "provider_full_responses"
    staged_root = run_root / "staged"
    published_root = staged_root / "state/published_versions"
    quarantine_root = staged_root / "state/quarantine" / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    written = []
    committed: list[Path] = []
    try:
        for source, relative in source_files:
            destination = snapshot_root / Path(f"{relative}.gz")
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source.open("rb") as source_handle, gzip.open(destination, "wb", compresslevel=6) as target_handle:
                shutil.copyfileobj(source_handle, target_handle)
            written.append(str(destination.relative_to(run_root)))

        for change in changes:
            relative = Path(change.relative)
            if change.kind in {"weekly_append", "document_version"}:
                destination = published_root / relative / f"{run_id}.json"
            else:
                destination = quarantine_root / relative / f"{change.kind}.json"
            destination.parent.mkdir(parents=True, exist_ok=True)
            body = {
                "schema_version": 3,
                "record_key_contract": RECORD_KEY_CONTRACT,
                "run_id": run_id,
                "week_start": week_start.isoformat(),
                "week_end": week_end.isoformat(),
                "source_relative": change.relative,
                "kind": change.kind,
                **change.payload,
            }
            destination.write_text(json.dumps(body, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
            written.append(str(destination.relative_to(staged_root)))

        for source, relative, plan in parquet_plans:
            import pandas as pd

            if plan["kind"] == "document_version":
                destination = published_root / relative / f"{run_id}{source.suffix}"
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)
                written.append(str(destination.relative_to(staged_root)))
                continue
            if plan["append_positions"]:
                destination = published_root / relative / f"{run_id}.parquet"
                destination.parent.mkdir(parents=True, exist_ok=True)
                plan["incoming"].iloc[plan["append_positions"]].to_parquet(destination)
                written.append(str(destination.relative_to(staged_root)))
            if plan["revisions"] or plan["quarantine"]:
                destination = quarantine_root / relative / "parquet_conflicts.json"
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(json.dumps({
                    "schema_version": 1,
                    "run_id": run_id,
                    "revisions": plan["revisions"],
                    "historical_backfills": plan["quarantine"],
                    "authorization_required": True,
                }, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
                written.append(str(destination.relative_to(staged_root)))

        # A candidate is audited as base + prior versions + staged versions.
        # Failed acceptance never moves staged files into the official layer.
        if acceptance is not None:
            pre = acceptance(database_root, staged_root)
            current_conflicts = [
                {"domain": "current_revision", "relative": c.relative,
                 "reason": "existing_current_week_key_conflict", "blocking": True}
                for c in changes if c.kind == "historical_revision"
                and any(r["classification"] in {"current_week", "undated"}
                        for r in c.payload["revisions"])]
            current_conflicts.extend(
                {"domain": "current_revision", "relative": str(relative),
                 "reason": "existing_current_week_key_conflict", "blocking": True}
                for _, relative, plan in parquet_plans
                if any(not r.get("date") or week_start.isoformat() <= r["date"] <= week_end.isoformat()
                       for r in plan["revisions"]))
            if current_conflicts:
                pre = {**pre, "passed": False, "issues": pre.get("issues", []) + current_conflicts}
            summary["prepublication_acceptance"] = pre
            if not pre.get("passed"):
                raise ValueError("PREPUBLICATION_ACCEPTANCE_FAILED:" + _canonical(pre))
        drift = verify_existing(database_root, before)
        if drift:
            raise ValueError("FORMAL_EXISTING_FILE_DRIFT:" + _canonical(drift))
        if not verify_freeze_manifest(freeze_manifest)["ok"]:
            raise ValueError("PLUGIN_OR_SKILL_DRIFT_DURING_RUN")
        for staged in sorted(staged_root.rglob("*")):
            if not staged.is_file():
                continue
            destination = database_root / staged.relative_to(staged_root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                raise FileExistsError(f"append-only destination exists: {destination}")
            os.replace(staged, destination)
            committed.append(destination)

        post = acceptance(database_root, None) if acceptance is not None else {"passed": True, "scope": "append_only"}
        if not post.get("passed"):
            raise ValueError("POSTPUBLICATION_ACCEPTANCE_FAILED:" + _canonical(post))
        drift = verify_existing(database_root, before)
        if drift:
            raise ValueError("FORMAL_EXISTING_FILE_DRIFT:" + _canonical(drift))
        summary["formal_acceptance"] = post
        summary["existing_files_verified"] = len(before)

        manifest_entries = []
        for path in sorted([
            item for item in snapshot_root.rglob("*") if item.is_file()
        ] + committed, key=lambda item: str(item)):
            relative = str(path.relative_to(database_root))
            manifest_entries.append({
                "relative": relative,
                "bytes": path.stat().st_size,
                "sha256": _sha_bytes(path.read_bytes()),
            })
        manifest_path = run_root / "MANIFEST_SHA256_SIZE.txt"
        manifest_path.write_text("".join(
            f"{item['sha256']}  {item['bytes']}  {item['relative']}\n"
            for item in manifest_entries
        ), encoding="utf-8")
        catalog_path = run_root / "incremental_catalog.json"
        catalog_path.write_text(json.dumps({
            "schema_version": 1,
            "run_id": run_id,
            "base_manifest": "MANIFEST_SHA256_SIZE.txt",
            "base_manifest_sha256": (
                _sha_bytes((database_root / "MANIFEST_SHA256_SIZE.txt").read_bytes())
                if (database_root / "MANIFEST_SHA256_SIZE.txt").is_file()
                else None
            ),
            "entries": manifest_entries,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        audit_path = run_root / "publication.json"
        summary.update({
            "status": "PUBLISHED",
            "written_files": [str(path.relative_to(database_root)) for path in committed],
            "provider_snapshot_files": [
                str(path.relative_to(database_root))
                for path in snapshot_root.rglob("*") if path.is_file()
            ],
            "incremental_manifest": str(manifest_path.relative_to(database_root)),
            "incremental_manifest_sha256": _sha_bytes(manifest_path.read_bytes()),
            "incremental_catalog": str(catalog_path.relative_to(database_root)),
        })
        audit_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        shutil.rmtree(staged_root, ignore_errors=True)
        return summary
    except Exception as exc:
        for destination in reversed(committed):
            destination.unlink(missing_ok=True)
        failed = run_root.with_name(f"{run_root.name}.failed")
        if run_root.exists():
            os.replace(run_root, failed)
        return {**summary, "status": "FAILED", "error": f"{type(exc).__name__}: {exc}",
                "committed_files_remaining": 0, "failed_run": str(failed)}


def build_freeze_manifest(roots: list[tuple[str, Path]], destination: Path) -> dict[str, Any]:
    """Create a manual seal. This helper is intentionally not an MCP tool"""
    payload = {"schema_version": 1, "sealed_at": datetime.now(timezone.utc).isoformat(), "roots": []}
    for name, root in roots:
        files = {}
        for path in _freeze_files(root):
            files[str(path.relative_to(root))] = _sha_bytes(path.read_bytes())
        payload["roots"].append({"name": name, "path": str(root), "files": files})
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix="freeze-", dir=destination.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return payload
