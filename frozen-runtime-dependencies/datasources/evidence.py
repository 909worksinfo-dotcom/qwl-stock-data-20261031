"""Point-in-time evidence loader shared by online analysis and backtests.

Raw collectors intentionally preserve provider-native records. This module is
the only place that converts those caches into data visible at a decision
cutoff. In particular, report periods are never treated as publication dates.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

from .. import paths
from ..schemas import Market, StockRef
from . import availability

_HK_RESULT_TERMS = (
    "ANNUAL RESULTS",
    "FINAL RESULTS",
    "INTERIM RESULTS",
    "QUARTERLY RESULTS",
    "FIRST QUARTER RESULTS",
    "THIRD QUARTER RESULTS",
    "RESULTS ANNOUNCEMENT",
    "ANNOUNCEMENT OF THE RESULTS",
    "RESULTS FOR THE",
    "UNAUDITED RESULTS",
    "UNAUDITED FINANCIAL RESULTS",
    "QUARTER RESULTS",
    "AUDITED RESULTS",
    "年度业绩",
    "全年业绩",
    "末期业绩",
    "中期业绩",
    "季度业绩",
    "第一季度业绩",
    "第一季度财务业绩",
    "未经审计财务业绩",
    "第三季度业绩",
    "業績公告",
    "年度業績",
    "全年業績",
    "中期業績",
    "季度業績",
    "第一季度財務業績",
    "未經審計財務業績",
    "業績公佈",
    "業績公布",
    "业绩公布",
)
_HK_RESULT_EXCLUDES = (
    "SUPPLEMENTAL",
    "CLARIFICATION",
    "POLL RESULTS",
    "DATE OF BOARD",
    "BOARD MEETING",
    "DELAY IN PUBLICATION",
    "EXPECTED RESULTS",
    "补充",
    "補充",
    "澄清",
    "董事会会议",
    "董事會會議",
    "延迟刊发",
    "延遲刊發",
    "盈利预告",
    "盈利預告",
)


def _read_json(path: Path) -> dict | None:
    try:
        return _read_json_versioned(str(path), path.stat().st_mtime_ns)
    except OSError:
        return None


@lru_cache(maxsize=4096)
def _read_json_versioned(path: str, mtime_ns: int) -> dict | None:
    del mtime_ns  # cache-key only
    try:
        with open(path, "r", encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, dict) else None
    except Exception:  # noqa: BLE001
        return None


def _date_value(value) -> date | None:
    if value in (None, "", "None", "nan"):
        return None
    if type(value) is date:
        return value
    return _date_text(str(value))


@lru_cache(maxsize=32768)
def _date_text(text: str) -> date | None:
    text = text.strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    dayfirst = bool(re.match(r"^\d{1,2}/\d{1,2}/\d{4}", text))
    try:
        return pd.to_datetime(text, dayfirst=dayfirst).date()
    except Exception:  # noqa: BLE001
        # HKEX sometimes emits DD/MM/YYYY.
        try:
            return pd.to_datetime(text, dayfirst=True).date()
        except Exception:  # noqa: BLE001
            return None


def _announcement_date(record: dict) -> date | None:
    for key in ("publish_datetime", "publish_date", "published_at", "公告时间"):
        parsed = _date_value(record.get(key))
        if parsed is not None:
            return parsed
    return None


def _announcement_availability(record: dict, market: Market) -> dict | None:
    return availability.announcement_availability(record, market)


def _announcements(ref: StockRef) -> list[dict]:
    blob = _read_json(paths.CACHE_DIR / "announcements" / f"{ref.ak_symbol}.json")
    if not blob:
        return []
    records = blob.get("announcements")
    return records if isinstance(records, list) else []


def _is_hk_results(record: dict) -> bool:
    value = f"{record.get('category', '')} {record.get('title', '')}".upper()
    return not any(term.upper() in value for term in _HK_RESULT_EXCLUDES) and any(
        term.upper() in value for term in _HK_RESULT_TERMS
    )


def _a_publication_map(ref: StockRef) -> dict[str, dict]:
    blob = _read_json(paths.CACHE_DIR / "statements" / f"{ref.ak_symbol}.json")
    if not blob:
        return {}
    candidates: dict[str, list[dict]] = {}
    for statement in ("income", "balance", "cashflow"):
        for record in blob.get(statement, []):
            report_date = str(record.get("report_date") or "")
            published_at = str(record.get("published_at") or "")
            if report_date and published_at:
                resolved = availability.publication_availability(
                    published_at, Market.A, force_date_only=True
                )
                if resolved:
                    candidates.setdefault(report_date, []).append(resolved)
    return {
        key: min(values, key=lambda item: item["source_published_on"])
        for key, values in candidates.items()
    }


def _hk_publication_map(ref: StockRef, report_dates: list[str]) -> dict[str, dict]:
    announcements = sorted(
        (
            (_announcement_date(record), _announcement_availability(record, Market.HK), record)
            for record in _announcements(ref)
            if _is_hk_results(record)
        ),
        key=lambda item: item[0] or date.max,
    )
    out = {}
    for report_date_raw in report_dates:
        report_date = _date_value(report_date_raw)
        if report_date is None:
            continue
        match = next(
            (
                resolved
                for published, resolved, _ in announcements
                if published is not None
                and resolved is not None
                and report_date <= published <= report_date + timedelta(days=200)
            ),
            None,
        )
        if match is not None:
            out[report_date.isoformat()] = match
    return out


def _load_fundamentals_current(ref: StockRef) -> dict | None:
    """Return fundamentals enriched with verifiable publication dates.

    Reports without publication evidence remain present for audit but are
    marked ``published_at=None`` and therefore cannot be selected by PIT logic.
    """
    blob = _read_json(paths.CACHE_DIR / "fundamentals" / f"{ref.ak_symbol}.json")
    if not blob:
        return None
    data = blob.get("data")
    if not isinstance(data, dict):
        return None
    raw_reports = data.get("reports")
    if not isinstance(raw_reports, list):
        return data
    report_dates = [str(item.get("date") or "") for item in raw_reports]
    publication_map = (
        _a_publication_map(ref)
        if ref.market == Market.A
        else _hk_publication_map(ref, report_dates)
    )
    reports = []
    for item in raw_reports:
        report_date = str(item.get("date") or "")[:10]
        publication = publication_map.get(report_date)
        reports.append({
            "report_date": report_date,
            "published_at": (
                publication.get("source_published_at") if publication else None
            ),
            "source_published_on": (
                publication.get("source_published_on") if publication else None
            ),
            "available_on": publication.get("available_on") if publication else None,
            "timestamp_precision": (
                publication.get("timestamp_precision") if publication else None
            ),
            # Retain date for old readers, but it is explicitly the report date.
            "date": report_date,
            "fields": item.get("fields", {}),
        })
    return {
        **data,
        "pit_date_field": "available_on",
        "publication_source": (
            "statement_notice_date" if ref.market == Market.A else "official_results_announcement"
        ),
        "reports": reports,
    }


def load_fundamentals(ref: StockRef) -> dict | None:
    """Load publication-versioned fundamentals when the archive is available."""
    current = _load_fundamentals_current(ref)
    archive = _read_json(
        paths.CACHE_DIR / "financial_versions" / f"{ref.ak_symbol}.json"
    )
    versions = archive.get("versions", []) if archive else []
    reports = []
    for version in versions:
        if version.get("dataset") != "fundamentals":
            continue
        payload = version.get("payload")
        if not isinstance(payload, dict):
            continue
        reports.append({
            **payload,
            "available_on": version.get("available_on"),
            "financial_version_hash": version.get("content_hash"),
            "financial_version_basis": version.get("version_basis"),
            "financial_version_first_seen_at": version.get("first_seen_at"),
        })
    if not reports:
        return current
    reports.sort(key=lambda item: (
        item.get("report_date") or item.get("date") or "",
        item.get("available_on") or "",
        item.get("financial_version_first_seen_at") or "",
    ))
    return {
        **(current or {}),
        "revision_tracking_started_on": archive.get("tracking_started_on"),
        "history_before_revision_tracking": archive.get(
            "history_before_tracking"
        ),
        "reports": reports,
    }


def load_microstructure(ref: StockRef, cutoff: date) -> list[dict]:
    blob = _read_json(paths.CACHE_DIR / "microstructure" / f"{ref.ak_symbol}.json")
    records = blob.get("records", []) if blob else []
    return [
        record for record in records
        if (_date_value(record.get("date")) or date.max) <= cutoff
    ]


def load_hkd_cny_rate(cutoff: date) -> dict | None:
    """Latest published HKD/CNY central parity visible at ``cutoff``."""
    blob = _read_json(paths.CACHE_DIR / "fx" / "HKDCNY.json")
    records = blob.get("records", []) if blob else []
    visible = []
    for record in records:
        observed = _date_value(record.get("date"))
        available_on = _date_value(
            record.get("available_on") or record.get("date")
        )
        if observed is None or available_on is None:
            continue
        if observed <= cutoff and available_on <= cutoff:
            raw = record.get("hkd_cny")
            if raw is None and record.get("mid") is not None:
                raw = float(record["mid"]) / 100.0
            try:
                rate = float(raw)
            except (TypeError, ValueError):
                continue
            if rate > 0:
                visible.append({
                    **record,
                    "hkd_cny": rate,
                    "available_on": available_on.isoformat(),
                })
    return visible[-1] if visible else None


def average_hkd_cny_rate(start: date, end: date) -> float | None:
    """Average official daily central parity over an accounting period."""
    if end < start:
        return None
    blob = _read_json(paths.CACHE_DIR / "fx" / "HKDCNY.json")
    records = blob.get("records", []) if blob else []
    rates = []
    for record in records:
        observed = _date_value(record.get("date"))
        available_on = _date_value(
            record.get("available_on") or record.get("date")
        )
        if observed is None or available_on is None:
            continue
        if not (start <= observed <= end and available_on <= end):
            continue
        raw = record.get("hkd_cny")
        if raw is None and record.get("mid") is not None:
            raw = float(record["mid"]) / 100.0
        try:
            rate = float(raw)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            rates.append(rate)
    return float(sum(rates) / len(rates)) if rates else None


def load_fund_flow(ref: StockRef, cutoff: date) -> dict | None:
    if ref.market != Market.A:
        return None
    blob = _read_json(paths.CACHE_DIR / "fund_flow_ths" / f"{ref.ak_symbol}.json")
    history = blob.get("daily_history", []) if blob else []
    eligible_history = []
    for item in history:
        trade_date = _date_value(item.get("date"))
        if trade_date is None:
            continue
        resolved = availability.publication_availability(
            trade_date.isoformat(), Market.A, force_date_only=True
        )
        available_on = _date_value(resolved.get("available_on")) if resolved else None
        if available_on is not None and available_on <= cutoff:
            eligible_history.append({**item, "available_on": available_on.isoformat()})
    if eligible_history:
        recent = eligible_history[-5:]
        return {
            "as_of_date": eligible_history[-1]["date"],
            "available_on": eligible_history[-1]["available_on"],
            "available": True,
            "source": blob.get("daily_history_source"),
            "horizons": {
                "instant": {
                    "net_inflow": eligible_history[-1].get("main_net_inflow"),
                },
                "5d": {
                    "net_inflow": sum(
                        float(item.get("main_net_inflow") or 0) for item in recent
                    ),
                },
            },
            "daily_records": recent,
        }
    snapshots = blob.get("snapshots", []) if blob else []
    eligible = [
        item for item in snapshots
        if (_date_value(item.get("as_of_date")) or date.max) <= cutoff
        and item.get("available")
    ]
    return eligible[-1] if eligible else None


def load_lhb(ref: StockRef, cutoff: date) -> list[dict]:
    if ref.market != Market.A:
        return []
    blob = _read_json(paths.CACHE_DIR / "lhb" / f"{ref.ak_symbol}.json")
    records = blob.get("events", []) if blob else []
    return [
        record for record in records
        if (_date_value(record.get("list_date")) or date.max) <= cutoff
    ]


def load_connect_holdings(ref: StockRef, cutoff: date) -> list[dict]:
    directory = "northbound" if ref.market == Market.A else "southbound"
    blob = _read_json(paths.CACHE_DIR / directory / f"{ref.ak_symbol}.json")
    records = blob.get("records", []) if blob else []
    date_keys = ("持股日期", "date")
    visible_by_date = {}
    for record in records:
        observed = next(
            (_date_value(record.get(key)) for key in date_keys if record.get(key)),
            None,
        )
        if observed is not None and observed <= cutoff:
            visible_by_date[observed] = record
    out = [
        visible_by_date[observed]
        for observed in sorted(visible_by_date)
    ]
    if out:
        latest = next(
            (_date_value(out[-1].get(key)) for key in date_keys if out[-1].get(key)),
            None,
        )
        if latest is None or (cutoff - latest).days > 15:
            return []
    return out


def load_benchmark_evidence(ref: StockRef, cutoff: date) -> dict:
    benchmark = "CSI300" if ref.market == Market.A else "HSI"
    blob = _read_json(
        paths.CACHE_DIR / "benchmarks" / f"index_{benchmark}.json"
    )
    records = blob.get("records", []) if blob else []
    visible = [
        record for record in records
        if (_date_value(record.get("date")) or date.max) <= cutoff
    ]
    industry = _read_json(paths.CACHE_DIR / "industry" / "industry_map.json")
    return {
        "name": benchmark,
        "records": visible,
        "industry_group": (
            industry.get(ref.ak_symbol) if isinstance(industry, dict) else None
        ),
    }


def symbol_evidence(ref: StockRef, cutoff: date) -> dict:
    """Load every currently model-consumable evidence family as of cutoff."""
    return {
        "microstructure": load_microstructure(ref, cutoff),
        "fund_flow": load_fund_flow(ref, cutoff),
        "lhb": load_lhb(ref, cutoff),
        "connect_holdings": load_connect_holdings(ref, cutoff),
        "benchmark": load_benchmark_evidence(ref, cutoff),
        "hkd_cny": (
            load_hkd_cny_rate(cutoff) if ref.market == Market.HK else None
        ),
        "non_cash_action_dates": [
            value.isoformat() for value in non_cash_action_dates(ref, cutoff)
        ],
    }


def non_cash_action_dates(ref: StockRef, cutoff: date, *, respect_record_availability: bool = False) -> list[date]:
    """Return effective stock-distribution/split dates known by the cutoff."""
    dates: list[date] = []
    if ref.market == Market.A:
        blob = _read_json(paths.CACHE_DIR / "dividends" / f"{ref.ak_symbol}.json")
        records = blob.get("records", []) if blob else []
        for record in records:
            ratio = (
                record.get("送转股份-送转总比例")
                or record.get("送转总比例")
                or 0
            )
            try:
                positive = float(ratio) > 0
            except (TypeError, ValueError):
                positive = False
            announced = _date_value(
                record.get("最新公告日期")
                or record.get("业绩披露日期")
                or record.get("预案公告日")
            )
            resolved = (
                availability.publication_availability(
                    announced.isoformat(), Market.A, force_date_only=True
                )
                if announced else None
            )
            available_on = (
                _date_value(resolved.get("available_on")) if resolved else None
            )
            if respect_record_availability and record.get('available_on'):
                explicit = _date_value(record['available_on'])
                if explicit is None:
                    continue
                available_on = max(available_on, explicit) if available_on else explicit
            effective = _date_value(
                record.get("除权除息日") or record.get("A股除权除息日")
            )
            if positive and available_on and effective \
                    and available_on <= cutoff and effective <= cutoff:
                dates.append(effective)
        for record in _announcements(ref):
            title = str(record.get("title", "")).upper()
            if any(term in title for term in ("配股除权", "配股股份上市", "股份合并", "股份拆细")):
                observed = _announcement_date(record)
                known = _announcement_availability(record, Market.A)
                available_on = _date_value(known.get("available_on")) if known else None
                if observed and available_on and available_on <= cutoff:
                    dates.append(observed)
    else:
        blob = _read_json(
            paths.CACHE_DIR / "hk_action_evidence" / f"{ref.ak_symbol}.json"
        )
        for record in blob.get("records", []) if blob else []:
            if record.get("action_type") not in {
                "split_or_consolidation", "rights_issue"
            }:
                continue
            published = _date_value(
                record.get("available_on") or record.get("published_at")
            )
            effective = _date_value(record.get("effective_date"))
            # Announcement/proposal dates are not mechanical price-adjustment
            # dates. Only an official effective date may invalidate raw-price
            # factor windows.
            if published and effective and published <= cutoff and effective <= cutoff:
                dates.append(effective)
    return sorted(set(dates))


def corporate_action_dates(ref: StockRef, cutoff: date, *, respect_record_availability: bool = False) -> list[date]:
    """Return known ex-dates that make raw close-to-close returns non-comparable."""
    dates = set(non_cash_action_dates(ref, cutoff, respect_record_availability=respect_record_availability))
    if ref.market == Market.A:
        blob = _read_json(paths.CACHE_DIR / "dividends" / f"{ref.ak_symbol}.json")
        records = blob.get("records", []) if blob else []
        for record in records:
            announced = _date_value(
                record.get("最新公告日期")
                or record.get("业绩披露日期")
                or record.get("预案公告日")
            )
            effective = _date_value(
                record.get("除权除息日") or record.get("A股除权除息日")
            )
            resolved = (
                availability.publication_availability(
                    announced.isoformat(), Market.A, force_date_only=True
                )
                if announced else None
            )
            available_on = (
                _date_value(resolved.get("available_on")) if resolved else None
            )
            if respect_record_availability and record.get('available_on'):
                explicit = _date_value(record['available_on'])
                if explicit is None:
                    continue
                available_on = max(available_on, explicit) if available_on else explicit
            cash = record.get("现金分红-现金分红比例") or 0
            stock = record.get("送转股份-送转总比例") or 0
            try:
                material = float(cash) > 0 or float(stock) > 0
            except (TypeError, ValueError):
                material = False
            if material and available_on and effective \
                    and available_on <= cutoff and effective <= cutoff:
                dates.add(effective)
    else:
        blob = _read_json(
            paths.CACHE_DIR / "dividends_hk" / f"{ref.ak_symbol}.json"
        )
        records = blob.get("records", []) if blob else []
        for record in records:
            announced = _date_value(record.get("最新公告日期"))
            effective = _date_value(record.get("除净日"))
            resolved = (
                availability.publication_availability(
                    announced.isoformat(), Market.HK, force_date_only=True
                )
                if announced else None
            )
            available_on = (
                _date_value(resolved.get("available_on")) if resolved else None
            )
            if respect_record_availability and record.get('available_on'):
                explicit = _date_value(record['available_on'])
                if explicit is None:
                    continue
                available_on = max(available_on, explicit) if available_on else explicit
            description = str(record.get("分红方案") or "").strip()
            if description and available_on and effective \
                    and available_on <= cutoff and effective <= cutoff:
                dates.add(effective)
    return sorted(dates)
