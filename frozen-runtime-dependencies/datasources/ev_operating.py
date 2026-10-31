"""PIT monthly vehicle deliveries for the BYD-related EV event panel."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date
from io import BytesIO

import requests
from pdfminer.high_level import extract_text

from .. import paths
from .byd_operating import load_byd_operating_history


_HK_SYMBOLS = {
    "09868": "09868.HK",
    "09866": "09866.HK",
    "02015": "02015.HK",
}
_MONTH = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")
_DELIVERY_PATTERNS = (
    re.compile(r"共交付新車\s*([\d,]+)\s*輛"),
    re.compile(r"交付新車\s*([\d,]+)\s*輛"),
    re.compile(r"交付\s*([\d,]+)\s*輛汽車"),
    re.compile(r"交付量[^\d]{0,20}([\d,]+)\s*輛"),
)


def _cache_path():
    directory = paths.CACHE_DIR / "ev_operating"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / "byd_related.json"


def parse_hk_delivery_text(text: str) -> dict:
    compact = re.sub(r"\s+", "", text)
    for pattern in _DELIVERY_PATTERNS:
        match = pattern.search(compact)
        if not match:
            continue
        deliveries = int(match.group(1).replace(",", ""))
        if 100 <= deliveries <= 2_000_000:
            return {
                "parse_status": "parsed",
                "deliveries": deliveries,
            }
    return {"parse_status": "failed_delivery"}


def _sales_like(record: dict) -> bool:
    title = str(record.get("title") or "")
    return (
        _MONTH.search(title) is not None
        and ("交付" in title or "交付量" in title)
        and "展望" not in title
    )


def _derived(records: list[dict]) -> list[dict]:
    by_symbol: dict[str, list[dict]] = {}
    for record in records:
        if record.get("parse_status") == "parsed":
            by_symbol.setdefault(str(record["symbol"]), []).append(record)
    for symbol_records in by_symbol.values():
        ordered = sorted(symbol_records, key=lambda item: item["report_month"])
        by_month = {item["report_month"]: item for item in ordered}
        prior_yoy: list[float] = []
        for index, record in enumerate(ordered):
            deliveries = int(record["deliveries"])
            previous = ordered[index - 1] if index else None
            prior_year = by_month.get(
                f"{int(record['report_month'][:4]) - 1:04d}-"
                f"{record['report_month'][5:]}"
            )
            record["deliveries_mom"] = (
                deliveries / int(previous["deliveries"]) - 1.0
                if previous and int(previous["deliveries"]) > 0 else None
            )
            record["deliveries_yoy"] = (
                deliveries / int(prior_year["deliveries"]) - 1.0
                if prior_year and int(prior_year["deliveries"]) > 0 else None
            )
            yoy = record["deliveries_yoy"]
            record["expected_deliveries_yoy"] = None
            record["sales_surprise_scale"] = None
            record["sales_surprise_z"] = None
            if yoy is not None and len(prior_yoy) >= 6:
                expectation_values = prior_yoy[-6:]
                scale_values = prior_yoy[-12:]
                expectation = float(sorted(expectation_values)[
                    len(expectation_values) // 2
                ])
                median = float(sorted(scale_values)[len(scale_values) // 2])
                deviations = sorted(abs(value - median) for value in scale_values)
                mad = float(deviations[len(deviations) // 2])
                scale = max(0.10, 1.4826 * mad)
                record["expected_deliveries_yoy"] = expectation
                record["sales_surprise_scale"] = scale
                record["sales_surprise_z"] = max(
                    -3.0, min(3.0, (float(yoy) - expectation) / scale)
                )
            if yoy is not None:
                prior_yoy.append(float(yoy))
    return records


def _byd_records() -> list[dict]:
    output = []
    for row in load_byd_operating_history():
        output.append({
            "symbol": "002594.SZ",
            "report_month": row["report_month"],
            "available_on": row["available_on"],
            "source_published_at": row.get("source_published_at"),
            "timestamp_precision": row.get("timestamp_precision"),
            "source": row.get("source"),
            "pdf_url": row.get("pdf_url"),
            "document_sha256": row.get("document_sha256"),
            "parse_status": "parsed",
            "deliveries": int(row["sales"]),
        })
    return output


def collect_ev_operating_history(
    *,
    force: bool = False,
    budget_seconds: float = 480.0,
) -> dict:
    target = _cache_path()
    if target.exists() and not force:
        payload = json.loads(target.read_text(encoding="utf-8"))
        return {
            "status": "cached",
            "records": len(payload.get("records", [])),
            "parsed": sum(
                row.get("parse_status") == "parsed"
                for row in payload.get("records", [])
            ),
            "path": str(target),
        }
    records = _byd_records()
    session = requests.Session()
    started = time.monotonic()
    for code, symbol in _HK_SYMBOLS.items():
        source_path = paths.CACHE_DIR / "announcements" / f"{code}.json"
        blob = json.loads(source_path.read_text(encoding="utf-8"))
        announcements = [
            row for row in blob.get("announcements", []) if _sales_like(row)
        ]
        for announcement in announcements:
            month_match = _MONTH.search(str(announcement.get("title") or ""))
            record = {
                "symbol": symbol,
                "report_month": (
                    f"{int(month_match.group(1)):04d}-"
                    f"{int(month_match.group(2)):02d}"
                ),
                "available_on": announcement.get("available_on"),
                "source_published_at": announcement.get(
                    "source_published_at"
                ),
                "timestamp_precision": announcement.get(
                    "timestamp_precision"
                ),
                "source": "HKEX official filing",
                "title": announcement.get("title"),
                "pdf_url": announcement.get("url"),
            }
            if time.monotonic() - started > budget_seconds:
                record["parse_status"] = "timeout"
                records.append(record)
                continue
            if not record["available_on"] or not record["pdf_url"]:
                record["parse_status"] = "invalid_announcement_metadata"
                records.append(record)
                continue
            try:
                response = session.get(
                    str(record["pdf_url"]),
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=45,
                )
                response.raise_for_status()
                content = response.content
                parsed = parse_hk_delivery_text(
                    extract_text(BytesIO(content))
                )
                record.update({
                    "document_sha256": hashlib.sha256(content).hexdigest(),
                    "document_bytes": len(content),
                    **parsed,
                })
            except Exception as exc:  # noqa: BLE001
                record.update({
                    "parse_status": "failed_download_or_parse",
                    "error": f"{type(exc).__name__}: {str(exc)[:160]}",
                })
            records.append(record)
    records = _derived(records)
    payload = {
        "schema_version": 1,
        "generated_on": date.today().isoformat(),
        "symbols": ["002594.SZ", *_HK_SYMBOLS.values()],
        "source": "CNINFO and HKEX official monthly operating filings",
        "records": sorted(
            records,
            key=lambda item: (
                str(item["symbol"]), str(item["report_month"])
            ),
        ),
    }
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    parsed = [
        row for row in records if row.get("parse_status") == "parsed"
    ]
    return {
        "status": "OK",
        "records": len(records),
        "parsed": len(parsed),
        "failed": len(records) - len(parsed),
        "by_symbol": {
            symbol: sum(row["symbol"] == symbol for row in parsed)
            for symbol in payload["symbols"]
        },
        "path": str(target),
    }


def load_ev_operating_history() -> list[dict]:
    target = _cache_path()
    if not target.exists():
        collect_ev_operating_history()
    payload = json.loads(target.read_text(encoding="utf-8"))
    return [
        row for row in payload.get("records", [])
        if row.get("parse_status") == "parsed"
        and row.get("available_on")
    ]
