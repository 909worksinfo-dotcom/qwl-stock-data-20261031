"""Append-only report versions for the fixed stock universe."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

from .. import paths
from ..schemas import StockRef, Universe
from . import availability


def _directory() -> Path:
    directory = paths.CACHE_DIR / "financial_versions"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


_DERIVED_METADATA_FIELDS = {
    "published_at",
    "source_published_at",
    "source_published_on",
    "timestamp_precision",
    "available_on",
    "fields_cny",
    "currency_conversion",
}


def _hash(payload: dict) -> str:
    """Hash source financial content, excluding derived PIT/FX metadata."""
    content = {
        key: value
        for key, value in payload.items()
        if key not in _DERIVED_METADATA_FIELDS
    }
    encoded = json.dumps(
        content, ensure_ascii=False, sort_keys=True, default=str,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read(path: Path) -> dict:
    try:
        value = json.load(open(path, encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _observation_available_on(ref: StockRef) -> str:
    resolved = availability.publication_availability(
        date.today().isoformat(),
        ref.market,
        force_date_only=True,
    )
    return resolved["available_on"]


def _payload_publication(ref: StockRef, payload: dict) -> dict | None:
    """Resolve an explicit source publication carried by a report payload."""
    published_at = (
        payload.get("published_at")
        or payload.get("source_published_at")
    )
    resolved = availability.publication_availability(
        published_at,
        ref.market,
        force_date_only=payload.get("timestamp_precision") != "exact",
    )
    report_date = str(
        payload.get("report_date") or payload.get("date") or ""
    )[:10]
    if (
        resolved
        and report_date
        and resolved["source_published_on"] < report_date
    ):
        return None
    return resolved


def archive_stock(ref: StockRef) -> dict:
    """Archive changed report payloads without backdating newly observed revisions."""
    from . import evidence

    path = _directory() / f"{ref.ak_symbol}.json"
    archive = _read(path)
    versions = archive.get("versions", [])
    if not isinstance(versions, list):
        versions = []
    # Existing observations, including their PIT metadata and multiplicity,
    # are immutable evidence. A weekly archive is not a baseline repair tool.
    # Build the lookup below without changing/re-hashing/deduplicating old rows.
    duplicates_removed = 0
    metadata_updated = 0
    existing_hashes = {
        (
            item.get("dataset"),
            item.get("section"),
            item.get("report_date"),
            item.get("content_hash"),
        )
        for item in versions
    }
    seen_identities = {
        (item.get("dataset"), item.get("section"), item.get("report_date"))
        for item in versions
    }
    observed_at = datetime.now(timezone.utc).isoformat()
    observation_available = _observation_available_on(ref)
    candidates = []

    current = evidence._load_fundamentals_current(ref)
    if current:
        for report in current.get("reports", []):
            candidates.append(("fundamentals", "summary", report))

    statement_dir = "statements" if ref.market.value == "A" else "statements_hk"
    raw = _read(paths.CACHE_DIR / statement_dir / f"{ref.ak_symbol}.json")
    publication_by_date = {
        item.get("report_date"): item
        for item in (current or {}).get("reports", [])
    }
    for section in ("income", "balance", "cashflow"):
        for report in raw.get(section, []) if isinstance(raw, dict) else []:
            report_date = str(report.get("report_date") or "")[:10]
            publication = publication_by_date.get(report_date, {})
            if not publication:
                publication = _payload_publication(ref, report) or {}
            publication_fields = {
                key: publication.get(key)
                for key in (
                    "source_published_at",
                    "source_published_on",
                    "timestamp_precision",
                    "available_on",
                )
                if publication.get(key) is not None
            }
            candidates.append((
                "statements",
                section,
                {
                    **report,
                    "published_at": (
                        report.get("published_at")
                        or publication.get("source_published_at")
                    ),
                    **publication_fields,
                },
            ))

    added = 0
    for dataset, section, payload in candidates:
        report_date = str(
            payload.get("report_date") or payload.get("date") or ""
        )[:10]
        if not report_date:
            continue
        content_hash = _hash(payload)
        key = (dataset, section, report_date, content_hash)
        if key in existing_hashes:
            continue
        identity = (dataset, section, report_date)
        source_available = payload.get("available_on")
        is_revision = identity in seen_identities
        version_available = (
            max(source_available, observation_available)
            if is_revision and source_available
            else observation_available
            if is_revision
            else source_available
        )
        versions.append({
            "dataset": dataset,
            "section": section,
            "report_date": report_date,
            "content_hash": content_hash,
            "first_seen_at": observed_at,
            "available_on": version_available,
            "source_published_on": payload.get("source_published_on"),
            "timestamp_precision": payload.get("timestamp_precision"),
            "version_basis": (
                "first_seen_revision"
                if is_revision else "legacy_source_publication_baseline"
            ),
            "payload": payload,
        })
        existing_hashes.add(key)
        seen_identities.add(identity)
        added += 1
    versions.sort(key=lambda item: (
        item.get("report_date") or "",
        item.get("dataset") or "",
        item.get("section") or "",
        item.get("available_on") or "",
        item.get("first_seen_at") or "",
    ))
    with open(path, "w", encoding="utf-8") as handle:
        json.dump({
            "symbol": ref.symbol,
            "tracking_started_on": (
                archive.get("tracking_started_on") or date.today().isoformat()
            ),
            "history_before_tracking": "not_reconstructable_from_current_sources",
            "versions": versions,
        }, handle, ensure_ascii=False, indent=2)
    return {
        "symbol": ref.symbol,
        "added": added,
        "metadata_updated": metadata_updated,
        "duplicates_removed": duplicates_removed,
        "versions": len(versions),
    }


def archive_universe(universe: Universe) -> dict:
    allowed = {stock.ak_symbol for stock in universe.stocks}
    results = {
        stock.symbol: archive_stock(stock)
        for stock in universe.stocks
    }
    leaked = {path.stem for path in _directory().glob("*.json")} - allowed
    if leaked:
        raise RuntimeError(f"financial version cache leak: {sorted(leaked)}")
    return results


def audit_hk_statement_baselines(universe: Universe) -> dict:
    """Read-only weekly inspection; missing PIT never authorizes a baseline rewrite."""
    findings = []
    for ref in universe.stocks:
        if ref.market.value != "HK":
            continue
        path = paths.CACHE_DIR / "financial_versions" / f"{ref.ak_symbol}.json"
        for index, item in enumerate(_read(path).get("versions", [])):
            if not item.get("available_on"):
                findings.append({"symbol": ref.symbol, "index": index,
                                 "report_date": item.get("report_date"),
                                 "reason": "historical_version_missing_availability"})
    return {"baselines_updated": 0, "parser_revisions_removed": 0,
            "historical_findings": findings, "maintenance_authorization_required": bool(findings)}


def repair_hk_statement_baselines(universe: Universe) -> dict:
    """Merge parser-only HK statement enrichment into historical baselines."""
    updated = 0
    removed = 0
    for ref in (
        stock for stock in universe.stocks if stock.market.value == "HK"
    ):
        archive_path = _directory() / f"{ref.ak_symbol}.json"
        statement_path = (
            paths.CACHE_DIR / "statements_hk" / f"{ref.ak_symbol}.json"
        )
        archive = _read(archive_path)
        current = _read(statement_path)
        versions = archive.get("versions", [])
        if not versions or not current:
            continue
        for section in ("income", "balance", "cashflow"):
            for report in current.get(section, []):
                report_date = str(report.get("report_date") or "")[:10]
                matches = [
                    item for item in versions
                    if item.get("dataset") == "statements"
                    and item.get("section") == section
                    and item.get("report_date") == report_date
                ]
                baseline = next(
                    (
                        item for item in matches
                        if item.get("version_basis")
                        == "legacy_source_publication_baseline"
                    ),
                    None,
                )
                if baseline is None:
                    continue
                prior_payload = baseline.get("payload") or {}
                enriched = {
                    **report,
                    "published_at": (
                        report.get("published_at")
                        or prior_payload.get("published_at")
                    ),
                }
                for key in (
                    "source_published_at", "source_published_on",
                    "timestamp_precision", "available_on",
                ):
                    value = baseline.get(key) or prior_payload.get(key)
                    if value is not None:
                        enriched[key] = value
                new_hash = _hash(enriched)
                if baseline.get("content_hash") != new_hash:
                    baseline["payload"] = enriched
                    baseline["content_hash"] = new_hash
                    baseline["parser_repaired_on"] = date.today().isoformat()
                    updated += 1
                duplicate_ids = {
                    id(item)
                    for item in matches
                    if item is not baseline
                    and _hash(item.get("payload") or {}) == new_hash
                }
                if duplicate_ids:
                    versions = [
                        item for item in versions
                        if id(item) not in duplicate_ids
                    ]
                    removed += len(duplicate_ids)
        versions.sort(key=lambda item: (
            item.get("report_date") or "",
            item.get("dataset") or "",
            item.get("section") or "",
            item.get("available_on") or "",
            item.get("first_seen_at") or "",
        ))
        archive["versions"] = versions
        with open(archive_path, "w", encoding="utf-8") as handle:
            json.dump(archive, handle, ensure_ascii=False, indent=2)
    return {
        "baselines_updated": updated,
        "parser_revisions_removed": removed,
    }
