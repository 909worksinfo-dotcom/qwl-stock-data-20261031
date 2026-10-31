"""Extended per-symbol data collection (M6 data parity, route A).

Fills the gap vs the reference project — for the SAME 60-stock set only:
  - three financial statements (income / balance / cashflow), per-symbol PIT
  - A-share dividends / stock transfers, per-symbol PIT
  - announcements: fetched market-wide-by-date (the only available shape) then
    IMMEDIATELY filtered to the 60 codes; the full-market frame is never written
    to disk. Three guards ensure only the 60 land: (1) isin-filter before write,
    (2) whitelist assert per record, (3) read-back verification.
  - share capital extracted from the balance sheet (no flaky individual_info).

All caches live under .cache/{statements,dividends,announcements}/ and contain
ONLY the 60 universe+comparables symbols. Nothing else is persisted.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections import Counter
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import structlog

from .. import config, paths
from ..schemas import Market, StockRef, Universe
from . import akshare_provider as akp
from . import availability as pit_availability
from . import calendar as qcal
from . import evidence

log = structlog.get_logger("qwl.extended")


def _dir(name: str) -> Path:
    d = paths.CACHE_DIR / name
    d.mkdir(parents=True, exist_ok=True)
    return d


def sixty_code_maps(universe: Universe) -> tuple[set, dict]:
    """Return (set of ak_symbols, {ak_symbol: StockRef}) for the 60-stock set."""
    codes = {s.ak_symbol for s in universe.stocks}
    by_ak = {s.ak_symbol: s for s in universe.stocks}
    return codes, by_ak


def _latest_dated_record(
    records: list[dict],
    date_key: str = "date",
) -> date | None:
    observed = []
    for record in records:
        try:
            observed.append(
                date.fromisoformat(str(record.get(date_key) or "")[:10])
            )
        except ValueError:
            continue
    return max(observed) if observed else None


def _expected_symbol_session(ref: StockRef, as_of: date) -> date:
    """Return the expected last observation, respecting known suspensions."""
    expected = qcal.latest_expected_session(ref.market, as_of)
    path = paths.CACHE_DIR / "suspensions" / f"{ref.ak_symbol}.json"
    try:
        intervals = json.load(open(path, encoding="utf-8")).get(
            "intervals", []
        )
    except Exception:  # noqa: BLE001
        intervals = []
    for interval in intervals:
        try:
            start = date.fromisoformat(str(interval["effective_from"])[:10])
            end = date.fromisoformat(str(interval["effective_to"])[:10])
        except (KeyError, TypeError, ValueError):
            continue
        if start <= expected <= end:
            return qcal.latest_expected_session(
                ref.market, start - timedelta(days=1)
            )
    return expected


def _merge_dated_records_append_only(
    existing: list[dict],
    incoming: list[dict],
    date_key: str = "date",
) -> tuple[list[dict], list[dict], int]:
    """Append new dates and quarantine revisions without changing old rows."""
    stored = {
        str(record.get(date_key) or "")[:10]: dict(record)
        for record in existing
        if record.get(date_key)
    }
    conflicts = []
    appended = 0
    for record in incoming:
        observed = str(record.get(date_key) or "")[:10]
        if not observed:
            continue
        if observed not in stored:
            stored[observed] = dict(record)
            appended += 1
            continue
        prior = stored[observed]
        fields = sorted(
            key for key in set(prior) | set(record)
            if key != date_key and prior.get(key) != record.get(key)
        )
        if fields:
            conflicts.append({
                "date": observed,
                "fields": fields,
                "stored": {key: prior.get(key) for key in fields},
                "incoming": {key: record.get(key) for key in fields},
            })
    return (
        [stored[key] for key in sorted(stored)],
        conflicts,
        appended,
    )


# --------------------------------------------------------------------------- #
# Statements + dividends (per-symbol, concurrent + wall-clock budget)
# --------------------------------------------------------------------------- #
def _run_budgeted(tasks: dict, worker, budget_s: float, max_workers: int = 6) -> dict:
    """Run worker(item) over tasks concurrently with a wall-clock budget.

    ``tasks`` maps key -> item. Returns {key: result_status}. Stragglers past the
    budget are abandoned (never block); their key is marked 'timeout'.
    """
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    ex = ThreadPoolExecutor(max_workers=max_workers)
    futs = {ex.submit(worker, item): key for key, item in tasks.items()}
    deadline = time.time() + budget_s
    pending = set(futs.keys())
    out: dict = {}
    while pending and time.time() < deadline:
        done, pending = wait(pending, timeout=max(0.5, deadline - time.time()),
                             return_when=FIRST_COMPLETED)
        for fut in done:
            key = futs[fut]
            try:
                out[key] = fut.result()
            except Exception:  # noqa: BLE001
                out[key] = "failed"
    for fut, key in futs.items():
        out.setdefault(key, "timeout")
    ex.shutdown(wait=False, cancel_futures=True)
    return out


def collect_statements(universe: Universe, only_a: bool = True, polite: float = 0.5,
                       budget_s: float = 240.0, force: bool = False) -> dict:
    """Fetch + cache three statements per A-share symbol (concurrent, budgeted)."""
    d = _dir("statements")
    a_stocks = [s for s in universe.stocks if s.market == Market.A]

    def worker(ref):
        if not force and (d / f"{ref.ak_symbol}.json").exists():
            return "cached"  # already have it
        prefix = ref.tencent_code[:2]
        data = akp.fetch_a_statements(ref.ak_symbol, prefix)
        if data is None:
            return "failed"
        blob = {"symbol": ref.symbol, "ak_symbol": ref.ak_symbol,
                "cached_at": datetime.now().isoformat(), **data}
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        from .financial_versions import archive_stock
        archive_stock(ref)
        return "cached"

    tasks = {ref.symbol: ref for ref in a_stocks}
    res = _run_budgeted(tasks, worker, budget_s=budget_s)
    for s in universe.stocks:
        if s.market != Market.A:
            res[s.symbol] = "skip_hk"
    return res


def collect_hk_statements(universe: Universe, budget_s: float = 180.0, force: bool = False) -> dict:
    """Fetch + cache HK three statements per HK symbol (concurrent, budgeted)."""
    d = _dir("statements_hk")
    hk_stocks = [s for s in universe.stocks if s.market == Market.HK]

    def worker(ref):
        if not force and (d / f"{ref.ak_symbol}.json").exists():
            return "cached"
        data = akp.fetch_hk_statements(ref.ak_symbol)
        if data is None:
            return "failed"
        balance_issues = _hk_balance_issues(data)
        if balance_issues:
            return f"failed_balance_validation:{len(balance_issues)}"
        blob = {"symbol": ref.symbol, "ak_symbol": ref.ak_symbol,
                "cached_at": datetime.now().isoformat(), **data}
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        from .financial_versions import archive_stock
        archive_stock(ref)
        return "cached"

    tasks = {ref.symbol: ref for ref in hk_stocks}
    res = _run_budgeted(tasks, worker, budget_s=budget_s)
    for s in universe.stocks:
        if s.market != Market.HK:
            res[s.symbol] = "skip_a"
    return res


def _hk_balance_issues(data: dict) -> list[dict]:
    """Validate the minimum comparable HK balance-sheet fields."""
    issues = []
    required = (
        "资产总额", "负债总额", "总权益", "现金及现金等价物"
    )
    for report in data.get("balance", []):
        fields = report.get("fields") or {}
        missing = [
            field for field in required if _num(fields.get(field)) is None
        ]
        if missing:
            issues.append({
                "report_date": report.get("report_date"),
                "issue": "missing_required_fields",
                "fields": missing,
            })
            continue
        assets = float(fields["资产总额"])
        liabilities = float(fields["负债总额"])
        equity = float(fields["总权益"])
        if assets <= 0 or abs(assets - liabilities - equity) > max(
            1.0, abs(assets) * 0.005
        ):
            issues.append({
                "report_date": report.get("report_date"),
                "issue": "balance_equation_failed",
                "assets": assets,
                "liabilities": liabilities,
                "equity": equity,
            })
    return issues


def collect_dividends(universe: Universe, polite: float = 0.4,
                      budget_s: float = 120.0, force: bool = False) -> dict:
    """Fetch + cache A-share dividends per symbol (concurrent, budgeted)."""
    d = _dir("dividends")
    a_stocks = [s for s in universe.stocks if s.market == Market.A]

    def worker(ref):
        if not force and (d / f"{ref.ak_symbol}.json").exists():
            return "cached"
        recs = akp.fetch_a_dividends(ref.ak_symbol)
        if recs is None:
            return "failed"
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "records": recs}, f, ensure_ascii=False, indent=2)
        return "cached"

    tasks = {ref.symbol: ref for ref in a_stocks}
    res = _run_budgeted(tasks, worker, budget_s=budget_s)
    for s in universe.stocks:
        if s.market != Market.A:
            res[s.symbol] = "skip_hk"
    return res


# --------------------------------------------------------------------------- #
# Announcements (market-wide -> filtered to 60, route A with 3 guards)
# --------------------------------------------------------------------------- #
def collect_announcements(universe: Universe, weeks: int = 8, polite: float = 0.6) -> dict:
    """Fetch market-wide announcements by date, filter to the 60, cache only 60.

    Guard 1: isin-filter the full-market frame in memory before any write.
    Guard 2: assert every code about to be written is in the 60 whitelist.
    Guard 3: read back the cache and verify the code set is a subset of the 60.
    The full-market frame is a local variable, never written to disk.
    """
    import akshare as ak
    import socket

    codes, by_ak = sixty_code_maps(universe)
    d = _dir("announcements")
    today = date.today()
    # trading-ish days: just iterate calendar days back `weeks` weeks; non-trading
    # days simply return few/no rows. Keep the window small (route A discipline).
    n_days = weeks * 7
    kept_rows: list[dict] = []
    days_ok = 0
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(15)
    try:
        for i in range(n_days):
            day = today - timedelta(days=i)
            ymd = day.strftime("%Y%m%d")
            try:
                full = ak.stock_notice_report(symbol="全部", date=ymd)  # market-wide
            except Exception:  # noqa: BLE001
                continue
            if full is None or full.empty or "代码" not in full.columns:
                continue
            days_ok += 1
            # GUARD 1: filter to the 60 in memory; drop the rest immediately.
            sub = full[full["代码"].astype(str).isin(codes)]
            for _, row in sub.iterrows():
                code = str(row["代码"])
                # GUARD 2: whitelist assert — never write a non-60 code.
                assert code in codes, f"leak guard: {code} not in universe"
                kept_rows.append({
                    "ak_symbol": code,
                    "symbol": by_ak[code].symbol,
                    "title": str(row.get("公告标题", "")),
                    "category": str(row.get("公告类型", "")),
                    "publish_date": str(row.get("公告日期", "")),
                    "url": str(row.get("网址", "")),
                })
            del full, sub  # explicit: full-market data goes out of scope now
            time.sleep(polite)
    finally:
        socket.setdefaulttimeout(old)

    # write per-symbol, only the 60
    by_symbol: dict[str, list] = {}
    for r in kept_rows:
        by_symbol.setdefault(r["ak_symbol"], []).append(r)

    written = {}
    for ak_symbol, rows in by_symbol.items():
        assert ak_symbol in codes  # GUARD 2 (again, at write time)
        rows = _with_announcement_availability(
            rows, by_ak[ak_symbol].market
        )
        with open(d / f"{ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": by_ak[ak_symbol].symbol, "announcements": rows},
                      f, ensure_ascii=False, indent=2)
        written[ak_symbol] = len(rows)

    # GUARD 3: read back, verify only-60 invariant
    on_disk = {p.stem for p in d.glob("*.json")}
    leaked = on_disk - codes
    if leaked:
        raise RuntimeError(f"announcement cache leak: {leaked}")

    log.info("announcements_collected", days_scanned=days_ok, symbols_with_news=len(written),
             total_rows=len(kept_rows))
    return {"days_scanned": days_ok, "symbols_with_news": len(written),
            "total_rows": len(kept_rows), "leaked": list(leaked)}


# --------------------------------------------------------------------------- #
# HK announcements (HKEXnews market-wide -> filtered to 60, 3 guards)
# --------------------------------------------------------------------------- #
def collect_hk_announcements(universe: Universe, days: int = 56, polite: float = 0.5) -> dict:
    """Fetch HK announcements from HKEXnews, PER-SYMBOL via stockId resolution.

    HKEXnews market-wide results are thousands/day; instead we resolve each HK
    code to its HKEXnews stockId (prefix.do) and query that stock's filings
    directly (titleSearchServlet with stockId). Only our HK codes are ever
    queried or written — isolation is inherent.
    """
    import socket
    import requests

    codes, by_ak = sixty_code_maps(universe)
    hk_stocks = [s for s in universe.stocks if s.market == Market.HK]
    d = _dir("announcements")
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
               "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml"}
    end = date.today()
    start = end - timedelta(days=days)
    sess = requests.Session()
    old = socket.getdefaulttimeout(); socket.setdefaulttimeout(20)
    written = 0
    total = 0
    try:
        for ref in hk_stocks:
            # 1) resolve stockId
            try:
                pr = sess.get("https://www1.hkexnews.hk/search/prefix.do",
                              params={"callback": "c", "lang": "ZH", "type": "A", "name": ref.ak_symbol},
                              headers=headers, timeout=15)
                txt = pr.text
                js = json.loads(txt[txt.index("(") + 1: txt.rindex(")")])
                info = [x for x in js.get("stockInfo", []) if str(x.get("code", "")).zfill(5) == ref.ak_symbol]
                if not info:
                    continue
                stock_id = info[0]["stockId"]
            except Exception:  # noqa: BLE001
                continue
            # 2) per-stock announcements
            try:
                r = sess.get("https://www1.hkexnews.hk/search/titleSearchServlet.do",
                             params={"sortDir": "0", "sortByOptions": "DateTime", "category": "0",
                                     "market": "SEHK", "stockId": str(stock_id), "documentType": "-1",
                                     "fromDate": start.strftime("%Y%m%d"), "toDate": end.strftime("%Y%m%d"),
                                     "title": "", "searchType": "1", "t": "2", "lang": "ZH"},
                             headers=headers, timeout=20)
                payload = r.json()
                rows = payload.get("result")
                if isinstance(rows, str):
                    rows = json.loads(rows)
            except Exception:  # noqa: BLE001
                continue
            recs = []
            for row in (rows or []):
                if not _hk_row_has_code(row, ref.ak_symbol):
                    continue
                code = ref.ak_symbol
                recs.append({
                    "ak_symbol": code, "symbol": ref.symbol,
                    "title": str(row.get("TITLE", "") or row.get("SHORT_TEXT", "")),
                    "publish_datetime": str(row.get("DATE_TIME", "")),
                    "url": "https://www1.hkexnews.hk" + str(row.get("FILE_LINK", "")),
                })
            if recs:
                assert ref.ak_symbol in codes  # GUARD 2
                recs = _with_announcement_availability(recs, Market.HK)
                with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
                    json.dump({"symbol": ref.symbol, "market": "HK", "announcements": recs},
                              f, ensure_ascii=False, indent=2)
                written += 1
                total += len(recs)
            time.sleep(polite)
    finally:
        socket.setdefaulttimeout(old)
    # GUARD 3: read back
    on_disk = {p.stem for p in d.glob("*.json")}
    leaked = on_disk - codes
    if leaked:
        raise RuntimeError(f"HK announcement leak: {leaked}")
    return {"hk_symbols_with_news": written, "total_kept": total, "leaked": list(leaked)}


def collect_historical_announcements(
    universe: Universe,
    start_date: str = "20230101",
    end_date: str | None = None,
    budget_s: float = 900.0,
    include_a: bool = True,
    include_hk: bool = True,
) -> dict:
    """Backfill official A/H announcements from 2023 for only the selected 60.

    A shares use per-symbol CNINFO queries. HKEX queries are split into 90-day
    windows to avoid result truncation. Empty result sets are persisted so
    coverage can distinguish "no announcements" from a failed/missing file.
    """
    import akshare as ak
    import requests

    codes, by_ak = sixty_code_maps(universe)
    d = _dir("announcements")
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = (
        datetime.strptime(end_date, "%Y%m%d").date()
        if end_date else date.today()
    )
    deadline = time.time() + budget_s
    result = {}

    # CNINFO calls are serial: its stock-map helper and pagination are global.
    for ref in (s for s in universe.stocks if include_a and s.market == Market.A):
        if time.time() > deadline:
            result[ref.symbol] = "timeout"
            continue
        try:
            frame = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=ref.ak_symbol,
                market="沪深京",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
            )
        except Exception:  # noqa: BLE001
            result[ref.symbol] = "failed"
            continue
        records = []
        if frame is not None and not frame.empty:
            for _, row in frame.iterrows():
                code = str(row.get("代码", "")).zfill(6)
                if code != ref.ak_symbol:
                    continue
                records.append({
                    "ak_symbol": code,
                    "symbol": ref.symbol,
                    "title": str(row.get("公告标题", "")),
                    "category": "CNINFO",
                    "publish_datetime": str(row.get("公告时间", "")),
                    "publish_date": str(row.get("公告时间", ""))[:10],
                    "url": str(row.get("公告链接", "")),
                    "source": "CNINFO",
                })
        path = d / f"{ref.ak_symbol}.json"
        prior = {}
        try:
            prior = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        retained = [
            record for record in prior.get("announcements", [])
            if (
                (observed := _parse_record_date(record)) is None
                or observed < start
                or observed > end
            )
        ]
        records = _with_announcement_availability(
            _dedupe_announcement_records([*retained, *records]), Market.A
        )
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "market": "A",
                       "range": [start.isoformat(), end.isoformat()],
                       "announcements": records},
                      f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
        "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
    }
    session = requests.Session()
    for ref in (s for s in universe.stocks if include_hk and s.market == Market.HK):
        if time.time() > deadline:
            result[ref.symbol] = "timeout"
            continue
        try:
            response = session.get(
                "https://www1.hkexnews.hk/search/prefix.do",
                params={"callback": "c", "lang": "ZH", "type": "A",
                        "name": ref.ak_symbol},
                headers=headers,
                timeout=20,
            )
            text = response.text
            payload = json.loads(text[text.index("(") + 1:text.rindex(")")])
            matches = [
                item for item in payload.get("stockInfo", [])
                if str(item.get("code", "")).zfill(5) == ref.ak_symbol
            ]
            stock_id = matches[0]["stockId"]
        except Exception:  # noqa: BLE001
            result[ref.symbol] = "failed"
            continue

        records = []
        window_start = start
        failed = False
        while window_start <= end:
            if time.time() > deadline:
                failed = True
                break
            window_end = min(window_start + timedelta(days=89), end)
            try:
                response = session.get(
                    "https://www1.hkexnews.hk/search/titleSearchServlet.do",
                    params={
                        "sortDir": "0", "sortByOptions": "DateTime",
                        "category": "0", "market": "SEHK",
                        "stockId": str(stock_id), "documentType": "-1",
                        "fromDate": window_start.strftime("%Y%m%d"),
                        "toDate": window_end.strftime("%Y%m%d"),
                        "title": "", "searchType": "1", "t": "2", "lang": "ZH",
                    },
                    headers=headers,
                    timeout=30,
                )
                page = response.json().get("result")
                if isinstance(page, str):
                    page = json.loads(page)
                for row in page or []:
                    if not _hk_row_has_code(row, ref.ak_symbol):
                        continue
                    code = ref.ak_symbol
                    records.append({
                        "ak_symbol": code,
                        "symbol": ref.symbol,
                        "title": str(row.get("TITLE", "") or row.get("SHORT_TEXT", "")),
                        "category": str(row.get("CATEGORY_NAME", "HKEX")),
                        "publish_datetime": str(row.get("DATE_TIME", "")),
                        "publish_date": str(row.get("DATE_TIME", ""))[:10],
                        "url": "https://www1.hkexnews.hk"
                               + str(row.get("FILE_LINK", "")),
                        "source": "HKEXnews",
                    })
            except Exception:  # noqa: BLE001
                failed = True
                break
            window_start = window_end + timedelta(days=1)
        if failed:
            result[ref.symbol] = "failed"
            continue
        path = d / f"{ref.ak_symbol}.json"
        prior = {}
        try:
            prior = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        retained = [
            record for record in prior.get("announcements", [])
            if (
                (observed := _parse_record_date(record)) is None
                or observed < start
                or observed > end
            )
        ]
        records = _with_announcement_availability(
            _dedupe_announcement_records([*retained, *records]), Market.HK
        )
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "market": "HK",
                       "range": [start.isoformat(), end.isoformat()],
                       "announcements": records},
                      f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"

    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"historical announcement cache leak: {leaked}")
    return result


def _dedupe_announcement_records(records: list[dict]) -> list[dict]:
    unique = {}
    for record in records:
        key = (
            record.get("url") or "",
            record.get("publish_datetime") or record.get("publish_date") or "",
            record.get("title") or "",
        )
        unique[key] = record
    return sorted(
        unique.values(),
        key=lambda record: (
            record.get("publish_datetime") or record.get("publish_date") or "",
            record.get("title") or "",
        ),
    )


def _with_announcement_availability(
    records: list[dict],
    market: Market,
) -> list[dict]:
    out = []
    for record in records:
        resolved = pit_availability.announcement_availability(record, market)
        out.append({
            **record,
            "source_published_at": (
                resolved.get("source_published_at") if resolved else None
            ),
            "source_published_on": (
                resolved.get("source_published_on") if resolved else None
            ),
            "timestamp_precision": (
                resolved.get("timestamp_precision") if resolved else None
            ),
            "available_on": resolved.get("available_on") if resolved else None,
        })
    return out


def _hk_row_has_code(row: dict, expected: str) -> bool:
    """Match HKEX rows that may list primary and RMB counter codes together."""
    import re

    values = re.split(r"<br\s*/?>|,|\s+", str(row.get("STOCK_CODE", "")))
    return expected in {value.zfill(5) for value in values if value.isdigit()}


def collect_listing_intervals(universe: Universe, budget_s: float = 300.0) -> dict:
    """Persist observed listing intervals for the fixed 60-stock universe.

    ``effective_from`` is the first raw trading-bar date from SINA/THS. Current
    listing status is independently checked through a Tencent quote snapshot.
    The evidence is explicit about being observed-first-trade rather than an
    inferred legal listing notice.
    """
    from . import tencent

    codes, _ = sixty_code_maps(universe)
    d = _dir("listing")
    try:
        snapshots = tencent.fetch_snapshots([s.tencent_code for s in universe.stocks])
    except Exception:  # noqa: BLE001
        snapshots = {}
    deadline = time.time() + budget_s
    result = {}
    for ref in universe.stocks:
        if time.time() > deadline:
            result[ref.symbol] = "timeout"
            continue
        if ref.market == Market.A:
            frame = akp.fetch_a_daily_sina(ref.tencent_code)
            source = "stock_zh_a_daily(sina)"
            if frame is None or frame.empty:
                frame = akp.fetch_a_daily_ths(ref.ak_symbol, tail=0)
                source = "ths_public_line"
        else:
            frame = akp.fetch_hk_daily_amount(ref.ak_symbol)
            source = "stock_hk_daily(sina)"
        if frame is None or frame.empty:
            result[ref.symbol] = "failed"
            continue
        currently_listed = ref.tencent_code in snapshots
        record = {
            "symbol": ref.symbol,
            "market": ref.market.value,
            "effective_from": frame.index.min().date().isoformat(),
            "effective_to": None if currently_listed else date.today().isoformat(),
            "currently_listed": currently_listed,
            "source": source,
            "authority": "observed_first_trade+current_tencent_quote",
            "captured_at": datetime.now().isoformat(),
            "positive_trade_dates_since_2023": [
                ts.date().isoformat()
                for ts in frame.index
                if ts.date() >= date(2023, 1, 1)
            ],
        }
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"listing cache leak: {leaked}")
    return result


def collect_suspension_intervals(
    universe: Universe,
    start_date: str = "20230101",
) -> dict:
    """Build conservative suspension intervals from official announcements.

    Positive-volume raw bars terminate a purported interval, preventing a
    narrative announcement from suppressing observed trading.
    """
    codes, _ = sixty_code_maps(universe)
    d = _dir("suspensions")
    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = date.today()
    result = {}
    for ref in universe.stocks:
        path = paths.CACHE_DIR / "announcements" / f"{ref.ak_symbol}.json"
        blob = {}
        try:
            blob = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        announcements = blob.get("announcements", [])
        events = []
        for record in announcements:
            title = " ".join(str(record.get("title", "")).upper().split())
            event = (
                _a_suspension_event(title)
                if ref.market == Market.A else _hk_suspension_event(title)
            )
            if event is None:
                continue
            resolved = pit_availability.announcement_availability(
                record, ref.market
            )
            published = (
                date.fromisoformat(resolved["source_published_on"])
                if resolved else None
            )
            if published is None or not start <= published <= end:
                continue
            effective = date.fromisoformat(resolved["available_on"])
            events.append((effective, event, record.get("url"), title))

        positive_dates = set()
        listing_path = paths.CACHE_DIR / "listing" / f"{ref.ak_symbol}.json"
        try:
            listing = json.load(open(listing_path, encoding="utf-8"))
            positive_dates.update(
                date.fromisoformat(value)
                for value in listing.get("positive_trade_dates_since_2023", [])
            )
        except Exception:  # noqa: BLE001
            pass
        for candidate in (
            paths.STATE_DIR / "history" / f"{ref.symbol.replace('.', '_')}_raw.parquet",
            paths.STATE_DIR / "history" / f"{ref.symbol.replace('.', '_')}.parquet",
        ):
            if not candidate.exists():
                continue
            try:
                frame = pd.read_parquet(candidate)
                positive_dates.update({
                    ts.date() for ts, value in frame["volume"].items()
                    if float(value) > 0
                })
            except Exception:  # noqa: BLE001
                pass
            break

        intervals = []
        suspended_from = None
        evidence_url = None
        for effective, event, url, title in sorted(events):
            if event == "suspend":
                if suspended_from is None:
                    suspended_from, evidence_url = effective, url
                continue
            if suspended_from is None:
                continue
            effective_to = effective - timedelta(days=1)
            positive = next(
                (day for day in sorted(positive_dates)
                 if suspended_from <= day <= effective_to),
                None,
            )
            if positive is not None:
                effective_to = positive - timedelta(days=1)
            if effective_to >= suspended_from:
                intervals.append({
                    "effective_from": suspended_from.isoformat(),
                    "effective_to": effective_to.isoformat(),
                    "source": "CNINFO" if ref.market == Market.A else "HKEXnews",
                    "suspend_evidence": evidence_url,
                    "resume_evidence": url,
                })
            suspended_from = None
            evidence_url = None
        if suspended_from is not None:
            positive = next(
                (day for day in sorted(positive_dates) if day >= suspended_from),
                None,
            )
            effective_to = positive - timedelta(days=1) if positive else None
            if effective_to is None or effective_to >= suspended_from:
                intervals.append({
                    "effective_from": suspended_from.isoformat(),
                    "effective_to": effective_to.isoformat() if effective_to else None,
                    "source": "CNINFO" if ref.market == Market.A else "HKEXnews",
                    "suspend_evidence": evidence_url,
                    "resume_evidence": None,
                })
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "range": [start.isoformat(), end.isoformat()],
                       "intervals": intervals}, f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"suspension cache leak: {leaked}")
    return result


def _parse_record_date(record: dict) -> date | None:
    for key in ("publish_datetime", "publish_date", "published_at"):
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            return pd.to_datetime(str(value), dayfirst="/" in str(value)).date()
        except Exception:  # noqa: BLE001
            continue
    return None


def _a_suspension_event(title: str) -> str | None:
    if any(term in title for term in ("可转债", "可转换公司债券", "债券停牌", "暂停转股")):
        return None
    if "复牌" in title:
        return "resume"
    if "停牌" in title:
        return "suspend"
    return None


def _hk_suspension_event(title: str) -> str | None:
    if ("RESUMPTION OF TRADING" in title or "恢复买卖" in title or "恢復買賣" in title) \
            and "CONTINUED SUSPENSION" not in title:
        return "resume"
    if "TRADING HALT" in title or "短暂停牌" in title or "短暫停牌" in title:
        return "suspend"
    if (
        ("SUSPENSION OF TRADING" in title or "暂停买卖" in title or "暫停買賣" in title)
        and "POSSIBLE SUSPENSION" not in title
        and "BOND" not in title
        and "NOTE" not in title
    ):
        return "suspend"
    return None


def _classify_hk_action_title(title: str) -> list[tuple[str, str]]:
    """Return precise action/phase pairs for an HKEX announcement title."""
    normalized = " ".join(str(title).upper().split())
    actions: list[tuple[str, str]] = []

    split_terms = (
        "SHARE CONSOLIDATION", "SHARE SUBDIVISION", "CAPITAL REORGANISATION",
        "股份合并", "股份合併", "股份拆细", "股份拆細", "股本重组", "股本重組",
    )
    rights_terms = ("RIGHTS ISSUE", "OPEN OFFER", "供股")
    placement_terms = ("PLACING OF", "PLACING AND SUBSCRIPTION", "配售")

    def phase(default: str = "proposal") -> str:
        if any(term in normalized for term in ("BECOMES EFFECTIVE", "EFFECTIVE DATE", "生效")):
            return "effective"
        if any(term in normalized for term in ("COMPLETION", "COMPLETED", "完成")):
            return "completion"
        if any(term in normalized for term in ("POLL RESULTS", "投票表決結果", "表决结果")):
            return "approval"
        return default

    if any(term in normalized for term in split_terms):
        actions.append(("split_or_consolidation", phase()))
    if any(term in normalized for term in rights_terms):
        actions.append(("rights_issue", phase()))
    if any(term in normalized for term in placement_terms):
        # "首次公开发售后股份奖励" contains "公开发售" but is not an open offer.
        if not any(term in normalized for term in ("首次公開發售", "首次公开发售", "IPO")):
            actions.append(("placement", phase()))

    debt_terms = ("債券", "债券", "NOTES", "BONDS")
    repurchase_terms = (
        "SHARE REPURCHASE", "SHARE BUY-BACK", "SHARE BUYBACK",
        "股份回購", "股份回购", "股份購回", "回購股份", "回购股份", "購回股份",
    )
    if any(term in normalized for term in repurchase_terms) \
            and not any(term in normalized for term in debt_terms):
        if "翌日披露" in normalized or "NEXT DAY DISCLOSURE" in normalized:
            repurchase_phase = "transaction_report"
        elif any(term in normalized for term in (
            "一般授權", "一般授权", "GENERAL MANDATE", "股東週年大會通告", "股东周年大会通告",
        )):
            repurchase_phase = "mandate"
        elif any(term in normalized for term in (
            "UPDATE", "進展", "进展", "充分使用", "COMPLETION", "完成",
        )):
            repurchase_phase = "update"
        else:
            repurchase_phase = "plan"
        actions.append(("repurchase", repurchase_phase))
    return actions


_CN_DIGITS = {
    "零": 0, "〇": 0, "○": 0, "一": 1, "二": 2, "三": 3, "四": 4,
    "五": 5, "六": 6, "七": 7, "八": 8, "九": 9,
}


def _cn_number(value: str) -> int:
    """Parse Chinese date numbers such as 二零二三, 十七 and 二十七."""
    if len(value) == 4 and all(char in _CN_DIGITS for char in value):
        return int("".join(str(_CN_DIGITS[char]) for char in value))
    if "十" in value:
        left, right = value.split("十", 1)
        tens = _CN_DIGITS.get(left, 1) if left else 1
        units = _CN_DIGITS.get(right, 0) if right else 0
        return tens * 10 + units
    if value in _CN_DIGITS:
        return _CN_DIGITS[value]
    raise ValueError(f"unsupported Chinese number: {value}")


def _dates_in_hk_pdf(text: str) -> list[date]:
    compact = re.sub(r"\s+", "", text)
    dates = []
    for year, month, day in re.findall(
        r"(20\d{2})年(\d{1,2})月(\d{1,2})日", compact
    ):
        try:
            dates.append(date(int(year), int(month), int(day)))
        except ValueError:
            continue
    for year, month, day in re.findall(
        r"([二零〇○一二三四五六七八九十]{4})年"
        r"([一二三四五六七八九十]{1,3})月"
        r"([一二三四五六七八九十]{1,3})日",
        compact,
    ):
        try:
            dates.append(date(_cn_number(year), _cn_number(month), _cn_number(day)))
        except ValueError:
            continue
    return sorted(set(dates))


def _enrich_hk_action_pdf(
    url: str,
    action_type: str,
    phase: str,
    published: date | None,
) -> dict:
    """Extract auditable fields from a small set of capital-action PDFs."""
    try:
        from pdfminer.high_level import extract_text

        response = requests.get(
            url, headers={"User-Agent": "Mozilla/5.0"}, timeout=45
        )
        response.raise_for_status()
        content = response.content
        text = extract_text(BytesIO(content))
        compact = re.sub(r"\s+", "", text)
    except Exception as exc:  # noqa: BLE001
        return {"pdf_parse_status": "failed",
                "pdf_parse_error": f"{type(exc).__name__}: {str(exc)[:120]}"}

    result = {
        "pdf_parse_status": "parsed",
        "document_sha256": hashlib.sha256(content).hexdigest(),
        "document_bytes": len(content),
        "document_text_chars": len(text),
    }
    observed_dates = _dates_in_hk_pdf(text)
    if published:
        observed_dates = [value for value in observed_dates if value <= published]
    result["document_dates"] = [value.isoformat() for value in observed_dates]
    if phase in {"completion", "effective"} and observed_dates:
        result["effective_date"] = observed_dates[-1].isoformat()

    if action_type == "placement":
        share_matches = re.findall(
            r"(?:合共|發行|发行)([\d,]{5,})股(?:新)?(?:認購|认购)股份",
            compact,
        )
        if not share_matches:
            share_matches = re.findall(
                r"([\d,]{5,})股(?:新)?(?:認購|认购)股份", compact
            )
        if share_matches:
            shares_issued = max(
                int(value.replace(",", "")) for value in share_matches
            )
            result["shares_issued"] = shares_issued
            large_numbers = [
                int(value.replace(",", ""))
                for value in re.findall(r"\b\d{1,3}(?:,\d{3}){2,}\b", text)
            ]
            if large_numbers:
                post_shares = max(large_numbers)
                if post_shares >= shares_issued:
                    result["post_event_issued_shares"] = post_shares
                    result["implied_dilution_ratio"] = round(
                        shares_issued / post_shares, 10
                    )

    if action_type in {"split_or_consolidation", "rights_issue"}:
        ratio_match = re.search(
            r"每(\d+(?:\.\d+)?)股.{0,80}?"
            r"(?:合併為|合并为|拆細為|拆细为|獲發|获发)"
            r"(\d+(?:\.\d+)?)股",
            compact,
        )
        if ratio_match:
            before, after = float(ratio_match.group(1)), float(ratio_match.group(2))
            if before:
                result["ratio"] = after / before
    return result


def collect_hk_action_evidence(universe: Universe) -> dict:
    """Classify official HKEX split/rights/placement/repurchase evidence."""
    codes, _ = sixty_code_maps(universe)
    d = _dir("hk_action_evidence")
    result = {}
    for ref in (s for s in universe.stocks if s.market == Market.HK):
        path = paths.CACHE_DIR / "announcements" / f"{ref.ak_symbol}.json"
        try:
            announcement_blob = json.load(open(path, encoding="utf-8"))
            announcements = announcement_blob.get("announcements", [])
        except Exception:  # noqa: BLE001
            announcement_blob = {}
            announcements = []
        records = []
        for announcement in announcements:
            title = " ".join(str(announcement.get("title", "")).upper().split())
            for action_type, phase in _classify_hk_action_title(title):
                published_raw = str(
                    announcement.get("publish_datetime")
                    or announcement.get("publish_date")
                    or ""
                )
                published = _parse_record_date(announcement)
                available = pit_availability.announcement_availability(
                    announcement, Market.HK
                )
                record = {
                    "action_type": action_type,
                    "phase": phase,
                    "published_at": published.isoformat() if published else None,
                    "published_at_raw": published_raw,
                    "available_on": (
                        available.get("available_on") if available else None
                    ),
                    "timestamp_precision": (
                        available.get("timestamp_precision") if available else None
                    ),
                    "effective_date": (
                        published.isoformat()
                        if published and phase == "effective"
                        else None
                    ),
                    "title": announcement.get("title"),
                    "url": announcement.get("url"),
                    "source": "HKEXnews",
                    "ratio": _action_ratio_from_title(title),
                    "is_price_adjusting": action_type in {
                        "split_or_consolidation", "rights_issue"
                    },
                    "evidence_level": (
                        "official_transaction_filing"
                        if phase == "transaction_report"
                        else "official_announcement"
                    ),
                }
                if action_type in {
                    "split_or_consolidation", "rights_issue", "placement"
                } and record["url"]:
                    enrichment = _enrich_hk_action_pdf(
                        record["url"], action_type, phase, published
                    )
                    record.update(enrichment)
                    if enrichment.get("ratio") is not None:
                        record["ratio"] = enrichment["ratio"]
                records.append(record)
        records.sort(key=lambda item: (str(item.get("published_at")), item["action_type"]))
        counts = {}
        for record in records:
            key = f"{record['action_type']}:{record['phase']}"
            counts[key] = counts.get(key, 0) + 1
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "range": announcement_blob.get("range"),
                "source": "HKEXnews official filings",
                "records": records,
                "counts": counts,
                "absence_semantics": (
                    "An empty action class means no matching official filing title "
                    "in the archived announcement window, not a provider failure."
                ),
            }, f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"HK action evidence leak: {leaked}")
    return result


def _action_ratio_from_title(title: str) -> float | None:
    import re

    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:FOR|:)\s*(\d+(?:\.\d+)?)", title)
    if not match:
        return None
    before, after = float(match.group(1)), float(match.group(2))
    return after / before if before else None


# --------------------------------------------------------------------------- #
# FX (HKD/CNY) + industry mapping + split events (all 60-scoped)
# --------------------------------------------------------------------------- #
def collect_fx(
    days: int | None = None,
    start_date: str = "19940101",
    end_date: str | None = None,
    universe: Universe | None = None,
) -> dict:
    """Cache official HKD/CNY history and current Bank of China quotations."""
    import socket
    import akshare as ak
    from curl_cffi import requests as curl_requests

    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(15)
    try:
        end = (
            datetime.strptime(end_date, "%Y%m%d").date()
            if end_date else date.today()
        )
        start = (
            end - timedelta(days=days)
            if days is not None
            else datetime.strptime(start_date, "%Y%m%d").date()
        )
        archive_page = curl_requests.get(
            "https://www.safe.gov.cn/safe/2020/1218/17833.html",
            impersonate="chrome",
            timeout=30,
        )
        archive_page.raise_for_status()
        matches = re.findall(
            r'href="([^"]+\.xlsx?[^"]*)"', archive_page.text, re.I
        )
        if not matches:
            raise RuntimeError("SAFE FX archive link not found")
        archive_url = matches[0]
        if archive_url.startswith("/"):
            archive_url = f"https://www.safe.gov.cn{archive_url}"
        archive_response = curl_requests.get(
            archive_url, impersonate="chrome", timeout=60
        )
        archive_response.raise_for_status()
        archive_df = pd.read_excel(BytesIO(archive_response.content))
        archive_df["日期"] = pd.to_datetime(
            archive_df["日期"], errors="coerce"
        )
        archive_df = archive_df[
            (archive_df["日期"].dt.date >= start)
            & (archive_df["日期"].dt.date <= end)
        ]
        archive_max = archive_df["日期"].max()
        live_start = (
            max(start, archive_max.date() - timedelta(days=45))
            if pd.notna(archive_max) else start
        )
        live_df = ak.currency_boc_sina(
            symbol="港币",
            start_date=live_start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        ) if live_start <= end else pd.DataFrame()
    except Exception as e:  # noqa: BLE001
        return {"status": "failed", "error": str(e)[:60]}
    finally:
        socket.setdefaulttimeout(old)
    if archive_df.empty and (live_df is None or live_df.empty):
        return {"status": "empty"}
    records = {}
    for _, row in archive_df.iterrows():
        observed = pd.to_datetime(row.get("日期"), errors="coerce")
        mid = pd.to_numeric(row.get("港元"), errors="coerce")
        if pd.isna(observed) or pd.isna(mid) or not 50 <= float(mid) <= 150:
            continue
        observed_on = observed.date().isoformat()
        record = {
            "date": observed_on,
            "source_published_at": f"{observed_on}T09:15:00+08:00",
            "source_published_on": observed_on,
            "timestamp_precision": "exact",
            "available_on": observed_on,
            "mid": float(mid),
            "buy": None,
            "sell": None,
            "mid_cny_per_100_hkd": float(mid),
            "hkd_cny": float(mid) / 100.0,
            "buy_hkd_cny": None,
            "sell_hkd_cny": None,
            "source": "SAFE official CFETS RMB central parity archive",
        }
        records[observed_on] = record
    for _, row in live_df.iterrows():
        observed = pd.to_datetime(row.get("日期"), errors="coerce")
        mid = pd.to_numeric(row.get("央行中间价"), errors="coerce")
        if pd.isna(observed) or pd.isna(mid) or not 50 <= float(mid) <= 150:
            continue
        observed_on = observed.date().isoformat()
        buy = pd.to_numeric(row.get("中行汇买价"), errors="coerce")
        sell = pd.to_numeric(row.get("中行钞卖价/汇卖价"), errors="coerce")
        record = {
            "date": observed_on,
            "source_published_at": f"{observed_on}T09:15:00+08:00",
            "source_published_on": observed_on,
            "timestamp_precision": "exact",
            "available_on": observed_on,
            # Source quotes CNY per 100 HKD.
            "mid": float(mid),
            "buy": float(buy) if pd.notna(buy) else None,
            "sell": float(sell) if pd.notna(sell) else None,
            "mid_cny_per_100_hkd": float(mid),
            "hkd_cny": float(mid) / 100.0,
            "buy_hkd_cny": float(buy) / 100.0 if pd.notna(buy) else None,
            "sell_hkd_cny": float(sell) / 100.0 if pd.notna(sell) else None,
            "source": "CFETS central parity and Bank of China quotes via Sina/AKShare",
        }
        official = records.get(observed_on)
        if official is not None:
            official.update({
                "buy": record["buy"],
                "sell": record["sell"],
                "buy_hkd_cny": record["buy_hkd_cny"],
                "sell_hkd_cny": record["sell_hkd_cny"],
                "validation_mid_hkd_cny": record["hkd_cny"],
                "validation_abs_diff": abs(
                    official["hkd_cny"] - record["hkd_cny"]
                ),
            })
        else:
            records[observed_on] = record
    recs = [records[key] for key in sorted(records)]
    if not recs:
        return {"status": "empty_valid_mid_rate"}
    d = _dir("fx")
    with open(d / "HKDCNY.json", "w", encoding="utf-8") as f:
        comparisons = [
            record["validation_abs_diff"] for record in recs
            if record.get("validation_abs_diff") is not None
        ]
        json.dump({
            "pair": "HKD/CNY",
            "quote_definition": "CNY per 1 HKD",
            "primary_field": "hkd_cny",
            "source": [
                "SAFE official CFETS RMB central parity archive",
                "CFETS central parity and Bank of China quotes via Sina/AKShare",
            ],
            "source_unit": "CNY per 100 HKD",
            "archive_url": archive_url,
            "cross_check": {
                "source": "Bank of China central parity via Sina/AKShare",
                "overlap_rows": len(comparisons),
                "max_abs_diff": max(comparisons) if comparisons else None,
            },
            "records": recs,
        }, f, ensure_ascii=False, indent=2)
    conversion = (
        standardize_hk_statements_cny(universe)
        if universe is not None else None
    )
    return {
        "status": "OK",
        "rows": len(recs),
        "range": [recs[0]["date"], recs[-1]["date"]],
        "statement_conversion": conversion,
    }


def standardize_hk_statements_cny(universe: Universe) -> dict:
    """Convert HKD statement amounts using closing/period-average PIT FX."""
    from . import evidence

    directory = _dir("statements_hk")
    summary = {"files": 0, "converted_reports": 0, "unavailable_reports": 0}
    for ref in (stock for stock in universe.stocks if stock.market == Market.HK):
        path = directory / f"{ref.ak_symbol}.json"
        try:
            blob = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        changed = False
        for section in ("income", "balance", "cashflow"):
            for report in blob.get(section, []):
                currency = report.get("currency")
                fields = report.get("fields") or {}
                if currency == "CNY":
                    report["fields_cny"] = fields
                    report["currency_conversion"] = {
                        "status": "not_required",
                        "rate": 1.0,
                    }
                    changed = True
                    continue
                if currency != "HKD":
                    report["fields_cny"] = None
                    report["currency_conversion"] = {
                        "status": "unsupported_currency",
                        "currency": currency,
                    }
                    summary["unavailable_reports"] += 1
                    changed = True
                    continue
                report_date = pd.to_datetime(
                    report.get("report_date"), errors="coerce"
                )
                if pd.isna(report_date):
                    summary["unavailable_reports"] += 1
                    continue
                report_day = report_date.date()
                if section == "balance":
                    rate_record = evidence.load_hkd_cny_rate(report_day)
                    rate = (
                        rate_record.get("hkd_cny") if rate_record else None
                    )
                    method = "closing_rate_on_or_before_report_date"
                    period_start = None
                else:
                    start_raw = report.get("start_date")
                    start = pd.to_datetime(start_raw, errors="coerce")
                    period_start = (
                        start.date()
                        if pd.notna(start)
                        else date(report_day.year, 1, 1)
                    )
                    rate = evidence.average_hkd_cny_rate(
                        period_start, report_day
                    )
                    method = "period_average_mid_rate"
                if rate is None:
                    report["fields_cny"] = None
                    report["currency_conversion"] = {
                        "status": "fx_unavailable",
                        "method": method,
                    }
                    summary["unavailable_reports"] += 1
                    changed = True
                    continue
                report["fields_cny"] = {
                    key: (
                        float(value) * float(rate)
                        if value is not None else None
                    )
                    for key, value in fields.items()
                }
                report["currency_conversion"] = {
                    "status": "converted",
                    "from": "HKD",
                    "to": "CNY",
                    "rate": float(rate),
                    "method": method,
                    "period_start": (
                        period_start.isoformat() if period_start else None
                    ),
                    "period_end": report_day.isoformat(),
                    "fx_source": "SAFE/CFETS RMB central parity",
                }
                summary["converted_reports"] += 1
                changed = True
        if changed:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(blob, handle, ensure_ascii=False, indent=2)
            summary["files"] += 1
    return summary


def collect_industry_map(universe: Universe) -> dict:
    """Build the 60-stock industry map.

    Primary source is our OWN curated grouping: comparables.yaml carries a
    ``group`` per comparable, and the 12 core stocks are assigned to the same 6
    groups here. This is deterministic, offline, and exactly the granularity
    neutralization needs — more reliable than scraping SW/THS membership (whose
    endpoints are frequently unreachable in-region). Only the 60 are written.
    """
    codes, by_ak = sixty_code_maps(universe)

    # core-12 -> group (same 6 industry buckets used for comparables)
    core_groups = {
        "002594": "auto", "300866": "consumer_electronics", "002463": "pcb_semi",
        "01585": "home_appliance", "603345": "food", "01810": "consumer_electronics",
        "09988": "internet", "002920": "auto", "02015": "auto",
        "000977": "pcb_semi", "688169": "consumer_electronics", "603365": "home_appliance",
    }

    # comparables groups from the yaml
    comp_path = paths.CONFIG_DIR / "comparables.yaml"
    mapping: dict[str, str] = dict(core_groups)
    if comp_path.exists():
        import yaml
        raw = yaml.safe_load(open(comp_path, encoding="utf-8"))
        for c in raw.get("stocks", []):
            ak_sym = str(c.get("ak_symbol", ""))
            grp = c.get("group")
            if ak_sym and grp:
                mapping[ak_sym] = grp

    # keep only universe codes (isolation)
    mapping = {k: v for k, v in mapping.items() if k in codes}
    for k in mapping:
        assert k in codes, f"industry leak: {k}"
    d = _dir("industry")
    with open(d / "industry_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    return {"status": "OK", "mapped": len(mapping), "of_total": len(universe.stocks)}


def derive_splits_from_dividends(universe: Universe) -> dict:
    """Derive split/stock-transfer events from the already-cached A-share dividend
    data (送转 ratios). No new network calls; no other stocks touched."""
    div_dir = _dir("dividends")
    out_dir = _dir("splits")
    n = 0
    for ref in universe.stocks:
        if ref.market != Market.A:
            continue
        p = div_dir / f"{ref.ak_symbol}.json"
        if not p.exists():
            continue
        recs = json.load(open(p, encoding="utf-8")).get("records", [])
        splits = []
        for r in recs:
            # 送转总比例 > 0 implies a stock-transfer/split-like action
            ratio = r.get("送转股份-送转总比例") or r.get("送转总比例")
            try:
                rv = float(ratio) if ratio not in (None, "", "nan") else 0.0
            except (ValueError, TypeError):
                rv = 0.0
            if rv and rv > 0:
                splits.append({"report_date": r.get("报告期"),
                               "publish_date": r.get("业绩披露日期"),
                               "transfer_ratio_per_10": rv})
        if splits:
            with open(out_dir / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
                json.dump({"symbol": ref.symbol, "splits": splits}, f, ensure_ascii=False, indent=2)
            n += 1
    return {"status": "OK", "symbols_with_splits": n}


# --------------------------------------------------------------------------- #
# Sentiment / game-flow signals (A-share only): 龙虎榜 / 北向 / 融资融券
# All are strictly scoped to the 60. LHB + margin arrive market-wide and are
# filtered to the 60 with the same 3-guard discipline as announcements.
# --------------------------------------------------------------------------- #
_LHB_KEEP_COLS = {
    "上榜日": "list_date", "解读": "interpretation", "收盘价": "close",
    "涨跌幅": "pct_chg", "龙虎榜净买额": "lhb_net_buy", "龙虎榜买入额": "lhb_buy",
    "龙虎榜卖出额": "lhb_sell", "龙虎榜成交额": "lhb_turnover",
    "市场总成交额": "market_turnover", "净买额占总成交比": "net_buy_ratio",
    "成交额占总成交比": "turnover_ratio", "换手率": "turnover_rate",
    "流通市值": "float_mktcap", "上榜原因": "reason",
    "上榜后1日": "post_1d", "上榜后2日": "post_2d",
    "上榜后5日": "post_5d", "上榜后10日": "post_10d",
}


def collect_lhb(universe: Universe, start_date: str | None = None,
                end_date: str | None = None, budget_s: float = 300.0) -> dict:
    """龙虎榜 (dragon-tiger list) events for the 60, market-wide -> filtered.

    ``stock_lhb_detail_em(start_date, end_date)`` returns the WHOLE-MARKET list of
    LHB events (rich: net-buy, reason, post-event 1/2/5/10-day returns). We fetch
    it once for the window, then filter to the 60 A-share codes. Same 3 guards as
    announcements: (1) isin-filter before write, (2) per-record whitelist assert,
    (3) read-back verification. The full-market frame is never written to disk.

    Default window covers ~2 years back (the weekly panel window). HK stocks have
    no LHB concept and are skipped.
    """
    import socket
    import akshare as ak

    codes, by_ak = sixty_code_maps(universe)
    a_codes = {s.ak_symbol for s in universe.stocks if s.market == Market.A}
    d = _dir("lhb")
    today = date.today()
    start = start_date or (today - timedelta(days=750)).strftime("%Y%m%d")
    end = end_date or today.strftime("%Y%m%d")

    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(30)
    try:
        try:
            full = ak.stock_lhb_detail_em(start_date=start, end_date=end)  # market-wide
        except Exception as e:  # noqa: BLE001
            return {"status": "failed", "error": f"{type(e).__name__}: {str(e)[:60]}"}
    finally:
        socket.setdefaulttimeout(old)

    if full is None or full.empty or "代码" not in full.columns:
        return {"status": "empty", "symbols_with_lhb": 0, "total_rows": 0, "leaked": []}

    # GUARD 1: filter the full-market frame to the 60 A-share codes in memory.
    sub = full[full["代码"].astype(str).isin(a_codes)]
    by_symbol: dict[str, list] = {}
    for _, row in sub.iterrows():
        code = str(row["代码"])
        assert code in codes, f"lhb leak guard: {code}"  # GUARD 2
        rec = {"ak_symbol": code, "symbol": by_ak[code].symbol}
        for cn, en in _LHB_KEEP_COLS.items():
            v = row.get(cn)
            rec[en] = None if (v is None or (isinstance(v, float) and pd.isna(v))) else (
                v if isinstance(v, (int, float, str)) else str(v))
        by_symbol.setdefault(code, []).append(rec)
    del full, sub  # full-market data leaves scope, never persisted

    written = {}
    for ak_symbol, rows in by_symbol.items():
        assert ak_symbol in codes  # GUARD 2 (write time)
        rows.sort(key=lambda r: str(r.get("list_date") or ""))
        with open(d / f"{ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": by_ak[ak_symbol].symbol, "source": "stock_lhb_detail_em",
                       "window": [start, end], "events": rows}, f, ensure_ascii=False, indent=2)
        written[ak_symbol] = len(rows)

    # GUARD 3: read back, verify only-60 invariant
    on_disk = {p.stem for p in d.glob("*.json")}
    leaked = on_disk - codes
    if leaked:
        raise RuntimeError(f"lhb cache leak: {leaked}")

    log.info("lhb_collected", symbols_with_lhb=len(written),
             total_rows=sum(written.values()), window=[start, end])
    return {"status": "OK", "symbols_with_lhb": len(written),
            "total_rows": sum(written.values()), "window": [start, end],
            "leaked": list(leaked)}


def collect_northbound(universe: Universe, budget_s: float = 120.0, force: bool = False) -> dict:
    """北向 (Stock-Connect) daily per-stock holdings for the 60 A-shares.

    STALE BY DESIGN: the exchanges stopped publishing daily per-stock northbound
    holdings on 2024-08-16, so every series here ends at/near that date and has
    NO overlap with a 2024-09+ weekly window. We still archive it (per-symbol,
    concurrent, budgeted) for completeness and tag ``frozen`` + the last date so
    the feature layer can see it is unusable for the current window.
    """
    d = _dir("northbound")
    a_stocks = [s for s in universe.stocks if s.market == Market.A]

    def worker(ref):
        if not force and (d / f"{ref.ak_symbol}.json").exists():
            return "cached"
        recs = akp.fetch_a_northbound(ref.ak_symbol)
        if not recs:
            blob = {
                "symbol": ref.symbol,
                "ak_symbol": ref.ak_symbol,
                "source": "stock_hsgt_individual_em",
                "frozen": True,
                "last_date": None,
                "availability": "no_published_history_or_not_eligible",
                "note": "per-stock northbound halted 2024-08-16",
                "records": [],
            }
            with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
                json.dump(blob, f, ensure_ascii=False, indent=2)
            return "no_records"
        recs, quality = _normalize_northbound_market_caps(ref, recs)
        last_date = str(recs[-1].get("持股日期", "")) if recs else ""
        blob = {"symbol": ref.symbol, "ak_symbol": ref.ak_symbol,
                "source": "stock_hsgt_individual_em", "frozen": True,
                "availability": "historical_until_disclosure_end",
                "last_date": last_date, "note": "per-stock northbound halted 2024-08-16",
                "data_quality": quality,
                "records": recs}
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        return "cached"

    tasks = {ref.symbol: ref for ref in a_stocks}
    res = _run_budgeted(tasks, worker, budget_s=budget_s)
    for s in universe.stocks:
        if s.market != Market.A:
            res[s.symbol] = "skip_hk"
    # read-back isolation
    codes, _ = sixty_code_maps(universe)
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"northbound cache leak: {leaked}")
    return res


def _normalize_northbound_market_caps(
    ref: StockRef,
    records: list[dict],
) -> tuple[list[dict], dict]:
    """Quarantine market-cap rows whose price/share units are inconsistent."""
    action_dates = evidence.non_cash_action_dates(ref, date.today())
    normalized = []
    counts = Counter()
    for record in records:
        close = _num(record.get("当日收盘价"))
        shares = _num(record.get("持股数量"))
        market_cap = _num(record.get("持股市值"))
        observed = _parse_record_date({
            "published_at": record.get("持股日期")
        })
        mismatch = (
            close is not None
            and shares is not None
            and market_cap not in (None, 0)
            and not math.isclose(
                close * shares,
                market_cap,
                rel_tol=2e-4,
                abs_tol=10.0,
            )
        )
        if not mismatch:
            normalized.append(record)
            continue
        nearby_action = (
            observed is not None
            and any(
                0 <= (action_date - observed).days <= 3
                for action_date in action_dates
            )
        )
        status = (
            "corporate_action_unit_mismatch"
            if nearby_action else "source_unit_mismatch"
        )
        normalized.append({
            **record,
            "持股市值_原始值": record.get("持股市值"),
            "持股市值": None,
            "hold_market_cap_status": status,
        })
        counts[status] += 1
    return normalized, {
        "market_cap_quarantined": sum(counts.values()),
        **dict(counts),
    }


def collect_margin_sse(universe: Universe, days: int = 200, polite: float = 0.25,
                       budget_s: float = 300.0) -> dict:
    """融资融券 (margin) daily balances for the SSE-listed A-shares in the 60.

    ``stock_margin_detail_sse(date)`` returns the whole SSE margin table for one
    trading day; we iterate recent dates, filter to our SH codes, and accumulate
    a per-symbol series. Same 3-guard isolation. SZSE's endpoint (szse.cn) is
    unreachable in-region, so Shenzhen names get NO margin data — this is an
    honest, documented asymmetry (SH-only).

    Args:
        days: how many calendar days back to scan (weekends/holidays return empty).
    """
    import socket
    import akshare as ak

    codes, by_ak = sixty_code_maps(universe)
    sh_codes = {s.ak_symbol for s in universe.stocks
                if s.market == Market.A and s.tencent_code.startswith("sh")}
    d = _dir("margin")
    today = date.today()

    by_symbol: dict[str, list] = {c: [] for c in sh_codes}
    days_ok = 0
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(20)
    deadline = time.time() + budget_s
    try:
        for i in range(days):
            if time.time() > deadline:
                break
            day = today - timedelta(days=i)
            if day.weekday() >= 5:  # skip Sat/Sun quickly
                continue
            ymd = day.strftime("%Y%m%d")
            try:
                full = ak.stock_margin_detail_sse(date=ymd)  # whole-SSE for the day
            except Exception:  # noqa: BLE001
                continue
            if full is None or full.empty or "标的证券代码" not in full.columns:
                continue
            days_ok += 1
            # GUARD 1: filter to our SH codes in memory
            sub = full[full["标的证券代码"].astype(str).isin(sh_codes)]
            for _, row in sub.iterrows():
                code = str(row["标的证券代码"])
                assert code in codes, f"margin leak guard: {code}"  # GUARD 2
                by_symbol[code].append({
                    "date": str(row.get("信用交易日期", ymd)),
                    "margin_balance": _num(row.get("融资余额")),
                    "margin_buy": _num(row.get("融资买入额")),
                    "margin_repay": _num(row.get("融资偿还额")),
                    "short_volume": _num(row.get("融券余量")),
                    "short_sell_volume": _num(row.get("融券卖出量")),
                })
            del full, sub
            time.sleep(polite)
    finally:
        socket.setdefaulttimeout(old)

    written = {}
    margin_conflicts = {}
    for code, rows in by_symbol.items():
        if not rows:
            continue
        assert code in codes  # GUARD 2 (write time)
        from .margin_contract import merge_working_records
        prior_path = d / f"{code}.json"
        prior = json.loads(prior_path.read_text(encoding="utf-8")) if prior_path.exists() else {}
        rows, conflicts, _ = merge_working_records(prior.get("records", []), rows)
        if conflicts:
            margin_conflicts[code] = conflicts
        with open(d / f"{code}.json", "w", encoding="utf-8") as f:
            json.dump({**prior, "symbol": by_ak[code].symbol,
                       "source": prior.get("source", "stock_margin_detail_sse"),
                       "exchange": "SSE", "records": rows,
                       "sse_response": by_symbol[code], "history_conflicts": conflicts},
                      f, ensure_ascii=False, indent=2)
        written[code] = len(rows)

    # GUARD 3: read-back
    on_disk = {p.stem for p in d.glob("*.json")}
    leaked = on_disk - codes
    if leaked:
        raise RuntimeError(f"margin cache leak: {leaked}")

    log.info("margin_sse_collected", days_scanned=days_ok, sh_symbols=len(sh_codes),
             symbols_with_margin=len(written))
    return {"status": "CONFLICT" if margin_conflicts else "OK", "conflicts": margin_conflicts,
            "days_scanned": days_ok, "sh_symbols_targeted": len(sh_codes),
            "symbols_with_margin": len(written), "total_rows": sum(written.values()),
            "szse_available": False, "leaked": list(leaked)}


def collect_margin_history_em(
    universe: Universe,
    budget_s: float = 600.0,
    force: bool = False,
) -> dict:
    """Backfill per-stock SH/SZ margin history from EastMoney DataCenter."""
    d = _dir("margin")
    a_stocks = [stock for stock in universe.stocks if stock.market == Market.A]
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"

    def worker(ref):
        path = d / f"{ref.ak_symbol}.json"
        existing = {}
        if path.exists():
            try:
                existing = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                existing = {}
        expected = _expected_symbol_session(ref, date.today())
        latest = _latest_dated_record(existing.get("records", []))
        from .margin_contract import current_shape_complete
        if (
            not force
            and existing.get("source") == "RPTA_WEB_RZRQ_GGMX"
            and current_shape_complete(existing.get("records", []), expected)
            and latest is not None
            and latest >= expected
        ):
            return "cached"
        page = 1
        rows = []
        while True:
            response = requests.get(
                url,
                params={
                    "reportName": "RPTA_WEB_RZRQ_GGMX",
                    "columns": "ALL",
                    "source": "WEB",
                    "sortColumns": "DATE",
                    "sortTypes": "-1",
                    "pageNumber": str(page),
                    "pageSize": "500",
                    "filter": f'(SCODE="{ref.ak_symbol}")',
                },
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": (
                        f"https://data.eastmoney.com/rzrq/stock/{ref.ak_symbol}.html"
                    ),
                },
                timeout=30,
            )
            response.raise_for_status()
            result = response.json().get("result") or {}
            data = result.get("data") or []
            rows.extend(data)
            pages = int(result.get("pages") or 0)
            if not data or page >= pages:
                break
            page += 1
        records = []
        action_dates = set(
            evidence.non_cash_action_dates(ref, date.today())
        )
        for row in rows:
            observed = _parse_record_date({"published_at": row.get("DATE")})
            if observed is None:
                continue
            record = {
                "date": observed.isoformat(),
                "market": row.get("MARKET"),
                "margin_balance": _num(row.get("RZYE")),
                "margin_buy": _num(row.get("RZMRE")),
                "margin_repay": _num(row.get("RZCHE")),
                "margin_net_buy": _num(row.get("RZJME")),
                "short_balance": _num(row.get("RQYE")),
                "short_volume": _num(row.get("RQYL")),
                "short_sell_volume": _num(row.get("RQMCL")),
                "short_repay_volume": _num(row.get("RQCHL")),
                "short_net_sell_volume": _num(row.get("RQJMG")),
                "margin_short_balance": _num(row.get("RZRQYE")),
                "float_market_cap": _num(row.get("SZ")),
                "margin_balance_ratio": _num(row.get("RZYEZB")),
            }
            records.append(_normalize_margin_record(
                record, action_dates
            ))
        records.sort(key=lambda item: item["date"])
        from .margin_contract import merge_working_records
        records, conflicts, appended = merge_working_records(
            existing.get("records", []), records, enrich=True
        )
        anomaly_counts = Counter(
            record.get("margin_repay_status")
            or record.get("short_repay_volume_status")
            for record in records
            if (
                record.get("margin_repay_status")
                or record.get("short_repay_volume_status")
            )
        )
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "source": "RPTA_WEB_RZRQ_GGMX",
                "data_shape": "per_stock_daily_history",
                "availability": "available" if records else "no_reported_history",
                "data_quality": {
                    "negative_repayment_quarantined": sum(
                        anomaly_counts.values()
                    ),
                    **dict(anomaly_counts),
                },
                "refresh": {
                    "refreshed_at": datetime.now().isoformat(),
                    "expected_latest_date": expected.isoformat(),
                    "latest_date": (
                        _latest_dated_record(records).isoformat()
                        if _latest_dated_record(records) else None
                    ),
                    "appended_rows": appended,
                    "conflict_rows": len(conflicts),
                    "policy": (
                        "append_new_dates_preserve_existing_conflicts"
                    ),
                },
                "history_conflicts": conflicts,
                "records": records,
            }, f, ensure_ascii=False, indent=2)
        latest = _latest_dated_record(records)
        if conflicts:
            return "conflict"
        if not records:
            return "no_records"
        if latest is None or latest < expected:
            return "stale"
        return "updated" if appended else "cached"

    result = _run_budgeted(
        {stock.symbol: stock for stock in a_stocks},
        worker,
        budget_s=budget_s,
        max_workers=8,
    )
    codes, _ = sixty_code_maps(universe)
    leaked = {path.stem for path in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"margin history cache leak: {leaked}")
    return result


def _normalize_margin_record(
    record: dict,
    action_dates: set[date],
) -> dict:
    """Quarantine impossible negative repayment flows without losing evidence."""
    out = dict(record)
    observed = _parse_record_date({
        "published_at": record.get("date")
    })
    for field in ("margin_repay", "short_repay_volume"):
        value = _num(record.get(field))
        if value is None or value >= 0:
            continue
        status = (
            "corporate_action_adjustment"
            if observed in action_dates else "source_anomaly"
        )
        out[f"{field}_raw"] = value
        out[field] = None
        out[f"{field}_status"] = status
    return out


def normalize_flow_anomalies(universe: Universe) -> dict:
    """Apply margin/northbound quarantine rules to existing fixed-60 caches."""
    result = Counter()
    for ref in (stock for stock in universe.stocks if stock.market == Market.A):
        margin_path = paths.CACHE_DIR / "margin" / f"{ref.ak_symbol}.json"
        if margin_path.exists():
            blob = json.load(open(margin_path, encoding="utf-8"))
            action_dates = set(
                evidence.non_cash_action_dates(ref, date.today())
            )
            records = [
                _normalize_margin_record(record, action_dates)
                for record in blob.get("records", [])
            ]
            counts = Counter(
                record.get("margin_repay_status")
                or record.get("short_repay_volume_status")
                for record in records
                if (
                    record.get("margin_repay_status")
                    or record.get("short_repay_volume_status")
                )
            )
            blob["records"] = records
            blob["data_quality"] = {
                "negative_repayment_quarantined": sum(counts.values()),
                **dict(counts),
            }
            with open(margin_path, "w", encoding="utf-8") as handle:
                json.dump(blob, handle, ensure_ascii=False, indent=2)
            result["margin_records_quarantined"] += sum(counts.values())

        northbound_path = (
            paths.CACHE_DIR / "northbound" / f"{ref.ak_symbol}.json"
        )
        if northbound_path.exists():
            blob = json.load(open(northbound_path, encoding="utf-8"))
            records, quality = _normalize_northbound_market_caps(
                ref, blob.get("records", [])
            )
            blob["records"] = records
            blob["data_quality"] = quality
            with open(northbound_path, "w", encoding="utf-8") as handle:
                json.dump(blob, handle, ensure_ascii=False, indent=2)
            result["northbound_market_caps_quarantined"] += quality[
                "market_cap_quarantined"
            ]
    return dict(result)


def _num(v):
    """Coerce a possibly-str/NaN numeric cell to float or None."""
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


# --------------------------------------------------------------------------- #
# Market microstructure (A-share): turnover(成交额) + float shares + turnover-rate
# Source: SINA stock_zh_a_daily (reachable in-region; carries the fields the
# Tencent OHLCV primary omits). Per-symbol -> inherently 60-scoped. Closes the
# turnover / float-shares gaps with a FULL daily history (not a snapshot).
# --------------------------------------------------------------------------- #
def collect_market_microstructure(universe: Universe, budget_s: float = 300.0,
                                  force: bool = False, tail: int = 520) -> dict:
    """Fetch + cache per-symbol microstructure daily series (turnover/amount).

    A-share (SINA ``stock_zh_a_daily``): [amount, outstanding_share, turnover].
    HK (SINA ``stock_hk_daily``): [amount] only (no free per-share float/turnover).

    ``tail`` keeps the most recent N trading days (default ~2 years) to bound the
    cache size — enough to cover the weekly panel window. Concurrent + budgeted.
    Per-symbol calls are inherently isolated; a read-back guard still asserts
    only-60. This closes the turnover / float-shares gaps with a FULL daily
    history (not a snapshot), across both markets.
    """
    d = _dir("microstructure")

    def fetch_one(ref):
        if ref.market == Market.A:
            try:
                df = akp.fetch_a_daily_sina(ref.tencent_code)
            except Exception:  # noqa: BLE001
                # A transient SINA failure should use the already supported
                # independent THS source, with its derived-float provenance.
                df = None
            source = "stock_zh_a_daily(sina)"
            float_method = "reported"
            if df is None or df.empty:
                df = akp.fetch_a_daily_ths(ref.ak_symbol, tail=tail)
                source = "ths_public_line"
                float_method = "derived_from_volume_div_turnover"
            fields = ["amount", "outstanding_share", "turnover"]
            note = (
                "amount=成交额(元); outstanding_share=流通股本(股); "
                f"turnover=换手率(比例); float_method={float_method}"
            )
        else:
            df = akp.fetch_hk_daily_tencent_extended(ref.ak_symbol, count=700)
            source = "tencent_hk_extended_kline"
            fields = ["amount", "outstanding_share", "turnover"]
            note = (
                "amount=成交额(港元); turnover=供应商报告换手率(比例); "
                "outstanding_share=成交量/换手率推导的报告口径股份分母，"
                "不是经监管核验的自由流通股本"
            )
            if df is None or df.empty:
                df = akp.fetch_hk_daily_eastmoney(ref.ak_symbol)
                source = "eastmoney_hk_raw_kline"
            if df is None or df.empty:
                df = akp.fetch_hk_daily_amount(ref.ak_symbol)
                source = "stock_hk_daily(sina)"
                fields = ["amount"]
                note = "amount=成交额(港元); 港股换手率历史源暂时失败"
        if df is None or df.empty:
            return None
        if tail and len(df) > tail:
            df = df.tail(tail)
        recs = []
        for idx, row in df.iterrows():
            rec = {"date": idx.date().isoformat()}
            for c in fields:
                rec[c] = _num(row.get(c)) if c in df.columns else None
            recs.append(rec)
        return {"symbol": ref.symbol, "ak_symbol": ref.ak_symbol,
                "market": ref.market.value,
                "source": source,
                "fields": fields, "note": note, "records": recs}

    # SERIAL, MAIN-THREAD loop. sina's stock_zh_a_daily/stock_hk_daily execute a
    # JS token via mini_racer (V8), whose isolate is NOT safe across threads —
    # a ThreadPoolExecutor (even 1 worker) segfaults (address_pool_manager Check
    # failed). Live probes proved main-thread serial is stable, so we loop here
    # with a wall-clock budget and rely on the provider's socket timeout per call.
    res: dict = {}
    deadline = time.time() + budget_s
    for ref in universe.stocks:
        path = d / f"{ref.ak_symbol}.json"
        existing = {}
        if path.exists():
            try:
                existing = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                existing = {}
        expected = _expected_symbol_session(ref, date.today())
        latest = _latest_dated_record(existing.get("records", []))
        if not force and latest is not None and latest >= expected:
            res[ref.symbol] = "cached"
            continue
        if time.time() > deadline:
            res[ref.symbol] = "timeout"
            continue
        try:
            blob = fetch_one(ref)
        except Exception:  # noqa: BLE001
            blob = None
        if blob is None:
            res[ref.symbol] = "failed"
            continue
        records, conflicts, appended = _merge_dated_records_append_only(
            existing.get("records", []), blob["records"]
        )
        blob["records"] = records
        merged_latest = _latest_dated_record(records)
        blob["refresh"] = {
            "refreshed_at": datetime.now().isoformat(),
            "expected_latest_date": expected.isoformat(),
            "latest_date": merged_latest.isoformat()
            if merged_latest else None,
            "appended_rows": appended,
            "conflict_rows": len(conflicts),
            "policy": "append_new_dates_preserve_existing_conflicts",
        }
        if conflicts:
            blob["history_conflicts"] = conflicts
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        latest = merged_latest
        if conflicts:
            res[ref.symbol] = "conflict"
        elif latest is None or latest < expected:
            res[ref.symbol] = "stale"
        else:
            res[ref.symbol] = "updated" if appended else "cached"

    # read-back isolation
    codes, _ = sixty_code_maps(universe)
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"microstructure cache leak: {leaked}")
    return res


def _cn_amount(value) -> float | None:
    """Parse THS money strings such as ``-1044.22万`` / ``1.35亿`` to yuan."""
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in ("", "-", "--", "nan", "None"):
        return None
    scale = 1.0
    if text.endswith("亿"):
        scale, text = 1e8, text[:-1]
    elif text.endswith("万"):
        scale, text = 1e4, text[:-1]
    try:
        return round(float(text) * scale, 2)
    except ValueError:
        return None


def _pct_ratio(value) -> float | None:
    """Parse a percentage string to a ratio (``1.25%`` -> ``0.0125``)."""
    if value is None:
        return None
    text = str(value).strip().replace("%", "")
    try:
        return round(float(text) / 100.0, 8)
    except ValueError:
        return None


def collect_fund_flow_ths(universe: Universe, force: bool = False) -> dict:
    """Collect THS all-market fund-flow snapshots, then persist only our A-shares.

    The source exposes current and 3/5/10/20-day cross-sectional rankings, not a
    historical daily API. Each run therefore appends one explicitly timestamped
    snapshot per symbol; it must never be presented as reconstructed history.
    mini_racer/V8 makes this collector main-thread serial by design.
    """
    import akshare as ak

    codes, by_ak = sixty_code_maps(universe)
    a_codes = {s.ak_symbol for s in universe.stocks if s.market == Market.A}
    d = _dir("fund_flow_ths")
    horizon_map = {
        "instant": "即时",
        "3d": "3日排行",
        "5d": "5日排行",
        "10d": "10日排行",
        "20d": "20日排行",
    }
    by_code: dict[str, dict] = {code: {} for code in a_codes}
    source_rows = {}
    for key, indicator in horizon_map.items():
        try:
            full = ak.stock_fund_flow_individual(symbol=indicator)
        except Exception:  # noqa: BLE001
            source_rows[key] = 0
            continue
        if full is None or full.empty or "股票代码" not in full.columns:
            source_rows[key] = 0
            continue
        full["股票代码"] = full["股票代码"].astype(str).str.zfill(6)
        sub = full[full["股票代码"].isin(a_codes)]  # GUARD 1
        source_rows[key] = len(sub)
        for _, row in sub.iterrows():
            code = str(row["股票代码"])
            assert code in codes  # GUARD 2
            if key == "instant":
                by_code[code][key] = {
                    "latest": _num(row.get("最新价")),
                    "pct_change": _pct_ratio(row.get("涨跌幅")),
                    "turnover": _pct_ratio(row.get("换手率")),
                    "inflow": _cn_amount(row.get("流入资金")),
                    "outflow": _cn_amount(row.get("流出资金")),
                    "net_inflow": _cn_amount(row.get("净额")),
                    "amount": _cn_amount(row.get("成交额")),
                }
            else:
                by_code[code][key] = {
                    "latest": _num(row.get("最新价")),
                    "pct_change": _pct_ratio(row.get("阶段涨跌幅")),
                    "continuous_turnover": _pct_ratio(row.get("连续换手率")),
                    "net_inflow": _cn_amount(row.get("资金流入净额")),
                }
        del full, sub

    captured_at = datetime.now().isoformat()
    as_of = date.today().isoformat()
    written = 0
    for code in sorted(a_codes):
        path = d / f"{code}.json"
        snapshots = []
        if path.exists():
            try:
                snapshots = json.load(open(path, encoding="utf-8")).get("snapshots", [])
            except Exception:  # noqa: BLE001
                snapshots = []
        snapshots = [x for x in snapshots if x.get("as_of_date") != as_of]
        snapshots.append({
            "as_of_date": as_of,
            "captured_at": captured_at,
            "available": bool(by_code[code]),
            "horizons": by_code[code],
        })
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": by_ak[code].symbol,
                "source": "stock_fund_flow_individual(THS)",
                "data_shape": "point_in_time_cross_section",
                "snapshots": snapshots[-260:],
            }, f, ensure_ascii=False, indent=2)
        written += 1

    leaked = {p.stem for p in d.glob("*.json")} - codes  # GUARD 3
    if leaked:
        raise RuntimeError(f"THS fund-flow cache leak: {leaked}")
    return {"status": "OK", "files": written, "source_rows": source_rows,
            "leaked": list(leaked)}


def collect_fund_flow_history(
    universe: Universe,
    budget_s: float = 300.0,
    force: bool = False,
) -> dict:
    """Backfill the reachable EastMoney daily history for selected A-shares."""
    import os

    d = _dir("fund_flow_ths")
    a_stocks = [stock for stock in universe.stocks if stock.market == Market.A]
    has_tushare = bool(os.environ.get("TUSHARE_TOKEN"))

    def worker(ref):
        path = d / f"{ref.ak_symbol}.json"
        existing = {}
        if path.exists():
            try:
                existing = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                existing = {}
        expected = _expected_symbol_session(ref, date.today())
        existing_latest = _latest_dated_record(
            existing.get("daily_history", [])
        )
        if (
            existing_latest is not None
            and existing_latest >= expected
            and not force
        ):
            return "cached"
        market = "sh" if ref.tencent_code.startswith("sh") else "sz"
        frame = (
            akp.fetch_a_fund_flow_tushare(ref.ak_symbol, market)
            if has_tushare else None
        )
        if frame is None or frame.empty:
            frame = akp.fetch_a_fund_flow(ref.ak_symbol, market)
        if frame is None or frame.empty:
            return "failed"
        incoming_records = []
        for _, row in frame.iterrows():
            record = {"date": row["date"].date().isoformat()}
            for column in frame.columns:
                if column in {"date", "_1", "_2"}:
                    continue
                record[column] = _num(row.get(column))
            incoming_records.append(record)
        records, conflicts, appended = _merge_dated_records_append_only(
            existing.get("daily_history", []), incoming_records
        )
        latest = _latest_dated_record(records)
        blob = {
            **existing,
            "symbol": ref.symbol,
            "source": existing.get("source", "stock_fund_flow_individual(THS)"),
            "daily_history_source": frame.attrs.get("source"),
            "daily_history_source_semantics": frame.attrs.get(
                "source_semantics"
            ),
            "daily_history_semantics": (
                "provider-reported order-size fund-flow history; each trade-date "
                "observation is available after that session closes"
            ),
            "daily_history_window": [
                records[0]["date"] if records else None,
                records[-1]["date"] if records else None,
            ],
            "public_source_limit": (
                "EastMoney endpoint returns only the latest 120 sessions and "
                "ignores requested historical end dates"
            ),
            "historical_backfill_status": (
                "available"
                if frame.attrs.get("source") == "tushare_moneyflow_authorized"
                else "authorized_source_required"
            ),
            "historical_backfill_options": [
                "Tushare moneyflow with a configured token",
                "licensed order-flow data",
            ],
            "daily_history_refresh": {
                "refreshed_at": datetime.now().isoformat(),
                "expected_latest_date": expected.isoformat(),
                "latest_date": latest.isoformat() if latest else None,
                "appended_rows": appended,
                "conflict_rows": len(conflicts),
                "policy": "append_new_dates_preserve_existing_conflicts",
            },
            "daily_history_conflicts": conflicts,
            "daily_history": records,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)
        if conflicts:
            return "conflict"
        if latest is None or latest < expected:
            return "stale"
        return "updated" if appended else "cached"

    result = _run_budgeted(
        {stock.symbol: stock for stock in a_stocks},
        worker,
        budget_s=budget_s,
        max_workers=6,
    )
    codes, _ = sixty_code_maps(universe)
    leaked = {path.stem for path in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"fund-flow history cache leak: {leaked}")
    return result


def collect_margin_market_aggregate() -> dict:
    """Cache full SH/SZ market-level margin histories from Jin10.

    This closes the Shenzhen *market-regime* history gap, while keeping the
    still-unavailable per-stock Shenzhen balances explicitly separate.
    """
    import akshare as ak

    d = _dir("margin_market")
    out = {}
    for market, fn in (
        ("SH", ak.macro_china_market_margin_sh),
        ("SZ", ak.macro_china_market_margin_sz),
    ):
        try:
            df = fn()
        except Exception:  # noqa: BLE001
            out[market] = {"status": "failed", "rows": 0}
            continue
        records = []
        for _, row in df.iterrows():
            rec = {}
            for key, value in row.items():
                if key == "日期":
                    rec["date"] = str(value)
                else:
                    rec[str(key)] = _num(value)
            records.append(rec)
        with open(d / f"{market}.json", "w", encoding="utf-8") as f:
            json.dump({"market": market, "source": "jin10", "records": records},
                      f, ensure_ascii=False, indent=2)
        out[market] = {"status": "cached", "rows": len(records)}
    return out


def collect_margin_eligibility(universe: Universe, as_of_date: str | None = None) -> dict:
    """Cache per-stock margin eligibility and broker margin ratios for all A-shares."""
    import akshare as ak

    codes, by_ak = sixty_code_maps(universe)
    a_stocks = [s for s in universe.stocks if s.market == Market.A]
    d = _dir("margin_eligibility")
    end = datetime.strptime(as_of_date, "%Y%m%d").date() if as_of_date else date.today()
    tables = {}
    used_date = None
    for offset in range(10):
        day = end - timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        ymd = day.strftime("%Y%m%d")
        current = {}
        for market in ("沪市", "深市"):
            try:
                df = ak.stock_margin_ratio_pa(symbol=market, date=ymd)
            except Exception:  # noqa: BLE001
                df = None
            if df is not None and not df.empty:
                current[market] = df
        if len(current) == 2:
            tables, used_date = current, ymd
            break
    if not tables:
        return {"status": "failed", "files": 0}

    eligible: dict[str, dict] = {}
    for market, df in tables.items():
        for _, row in df.iterrows():
            code = str(row.get("证券代码", "")).zfill(6)
            if code not in codes:
                continue  # GUARD 1
            eligible[code] = {
                "eligible": True,
                "market": market,
                "financing_ratio": _num(row.get("融资比例")),
                "short_ratio": _num(row.get("融券比例")),
            }
    for ref in a_stocks:
        assert ref.ak_symbol in codes  # GUARD 2
        rec = eligible.get(ref.ak_symbol, {
            "eligible": False,
            "market": "沪市" if ref.tencent_code.startswith("sh") else "深市",
            "financing_ratio": None,
            "short_ratio": None,
        })
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "as_of_date": used_date,
                       "source": "stock_margin_ratio_pa(pingan)", **rec},
                      f, ensure_ascii=False, indent=2)
    leaked = {p.stem for p in d.glob("*.json")} - codes  # GUARD 3
    if leaked:
        raise RuntimeError(f"margin eligibility cache leak: {leaked}")
    return {"status": "OK", "as_of_date": used_date, "files": len(a_stocks),
            "eligible": len(eligible), "leaked": list(leaked)}


def collect_connect_flows(tail: int = 520) -> dict:
    """Cache market-level northbound/southbound histories.

    Post-2024 northbound per-stock fields remain unavailable by regulation; the
    aggregate series is stored honestly and naturally contains null net-flow
    fields after the disclosure change. Southbound continues to be populated.
    """
    import akshare as ak

    d = _dir("connect_flows")
    out = {}
    for key, symbol in (("northbound", "北向资金"), ("southbound", "南向资金")):
        try:
            df = ak.stock_hsgt_hist_em(symbol=symbol)
        except Exception:  # noqa: BLE001
            out[key] = {"status": "failed", "rows": 0}
            continue
        if tail and len(df) > tail:
            df = df.tail(tail)
        records = []
        for _, row in df.iterrows():
            rec = {}
            for name, value in row.items():
                if name == "日期":
                    rec["date"] = str(value)
                else:
                    rec[str(name)] = _num(value) if not isinstance(value, str) else value
            records.append(rec)
        with open(d / f"{key}.json", "w", encoding="utf-8") as f:
            json.dump({
                "series": key,
                "source": "stock_hsgt_hist_em",
                "units": {
                    "当日成交净买额": "亿元",
                    "买入成交额": "亿元",
                    "卖出成交额": "亿元",
                    "历史累计净买额": "万亿元",
                    "持股市值": "元",
                },
                "note": (
                    "北向逐股/净买额披露自2024-08-16后停止，后续空值原样保留"
                    if key == "northbound"
                    else "南向成交和持股披露持续有效"
                ),
                "records": records,
            }, f, ensure_ascii=False, indent=2)
        out[key] = {"status": "cached", "rows": len(records)}
    return out


def collect_southbound_holdings(universe: Universe, days: int = 1000,
                                tail: int = 520) -> dict:
    """Fetch southbound daily holdings for only the selected HK symbols.

    The raw EastMoney report supports a server-side SECURITY_CODE filter. This
    avoids downloading the full HK market and keeps the request and disk scope
    restricted to the 15 selected HK names.
    """
    import requests

    codes, by_ak = sixty_code_maps(universe)
    hk_codes = sorted(s.ak_symbol for s in universe.stocks if s.market == Market.HK)
    d = _dir("southbound")
    end = date.today()
    start = end - timedelta(days=days)
    quoted = ",".join(f'"{code}"' for code in hk_codes)
    filters = (
        '(INTERVAL_TYPE="1")(RN=1)'
        f"(SECURITY_CODE in ({quoted}))"
        f"(TRADE_DATE>='{start.isoformat()}')(TRADE_DATE<='{end.isoformat()}')"
    )
    params = {
        "sortColumns": "TRADE_DATE",
        "sortTypes": "-1",
        "pageSize": "1000",
        "pageNumber": "1",
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": filters,
        "reportName": "RPT_MUTUAL_STOCK_HOLDRANKS",
    }
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    all_rows = []
    try:
        first = requests.get(url, params=params, timeout=30).json()
        result = first.get("result") or {}
        pages = int(result.get("pages") or 0)
        all_rows.extend(result.get("data") or [])
        for page in range(2, pages + 1):
            params["pageNumber"] = str(page)
            result = requests.get(url, params=params, timeout=30).json().get("result") or {}
            all_rows.extend(result.get("data") or [])
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:80]}"}

    by_code: dict[str, list] = {code: [] for code in hk_codes}
    for row in all_rows:
        code = str(row.get("SECURITY_CODE", "")).zfill(5)
        if code not in by_code:
            continue  # GUARD 1
        by_code[code].append({
            "date": str(row.get("TRADE_DATE", ""))[:10],
            "close": _num(row.get("CLOSE_PRICE")),
            "pct_change": _num(row.get("CHANGE_RATE")),
            "hold_shares": _num(row.get("HOLD_SHARES")),
            "hold_market_cap": _num(row.get("HOLD_MARKET_CAP")),
            "hold_ratio_total": _num(row.get("TOTAL_SHARES_RATIO")),
            "hold_shares_change": _num(row.get("HOLD_SHARES_CHANGE")),
            "market_cap_change": _num(row.get("ADD_MARKET_CAP")),
        })
    eligible_codes = None
    eligibility_source = None
    try:
        response = requests.get(
            "https://www.bocifp.com/en/ah/hdata",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        response.raise_for_status()
        eligible_codes = {
            match.zfill(5)
            for match in re.findall(
                r"/underlying/underlying-info/code/(\d+)",
                response.text,
            )
        }
        eligibility_source = "BOCI current Southbound eligible-stock list"
    except Exception:  # noqa: BLE001
        eligible_codes = None
    with_data = 0
    for code, records in by_code.items():
        assert code in codes  # GUARD 2
        unique_by_date = {}
        for record in records:
            observed_on = record["date"]
            prior = unique_by_date.get(observed_on)
            if prior is not None and prior != record:
                raise RuntimeError(
                    "conflicting southbound records for "
                    f"{code} on {observed_on}"
                )
            unique_by_date[observed_on] = record
        records = sorted(
            unique_by_date.values(), key=lambda item: item["date"]
        )
        records = records[-tail:] if tail else records
        with_data += bool(records)
        currently_eligible = (
            code in eligible_codes if eligible_codes is not None else None
        )
        availability = (
            "historical_available"
            if records else
            "not_eligible_current"
            if currently_eligible is False else
            "no_published_history"
        )
        with open(d / f"{code}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": by_ak[code].symbol,
                "source": "RPT_MUTUAL_STOCK_HOLDRANKS",
                "eligible": bool(records) or currently_eligible is True,
                "currently_eligible": currently_eligible,
                "eligibility_source": eligibility_source,
                "availability": availability,
                "records": records,
            }, f, ensure_ascii=False, indent=2)
    leaked = {p.stem for p in d.glob("*.json")} - codes  # GUARD 3
    if leaked:
        raise RuntimeError(f"southbound cache leak: {leaked}")
    return {"status": "OK", "files": len(by_code), "symbols_with_data": with_data,
            "source_rows": len(all_rows), "leaked": list(leaked)}


def collect_hk_dividends(universe: Universe, budget_s: float = 90.0) -> dict:
    """Cache HK dividend histories for all selected HK names, including empties."""
    d = _dir("dividends_hk")
    hk_stocks = [s for s in universe.stocks if s.market == Market.HK]

    def worker(ref):
        records = akp.fetch_hk_dividends(ref.ak_symbol)
        if records is None:
            return "failed"
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "source": "stock_hk_dividend_payout_em",
                       "records": records}, f, ensure_ascii=False, indent=2)
        return "cached"

    result = _run_budgeted({s.symbol: s for s in hk_stocks}, worker,
                           budget_s=budget_s, max_workers=4)
    codes, _ = sixty_code_maps(universe)
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"HK dividend cache leak: {leaked}")
    return result


def collect_hk_share_snapshots(universe: Universe, budget_s: float = 90.0) -> dict:
    """Cache exact latest issued/H-share counts for all selected HK names."""
    d = _dir("hk_share_snapshot")
    hk_stocks = [s for s in universe.stocks if s.market == Market.HK]

    def worker(ref):
        fields = akp.fetch_hk_share_snapshot(ref.ak_symbol)
        if fields is None:
            return "failed"
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "captured_at": datetime.now().isoformat(),
                "source": "stock_hk_financial_indicator_em",
                "data_shape": "latest_snapshot",
                "fields": fields,
            }, f, ensure_ascii=False, indent=2)
        return "cached"

    result = _run_budgeted({s.symbol: s for s in hk_stocks}, worker,
                           budget_s=budget_s, max_workers=4)
    codes, _ = sixty_code_maps(universe)
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"HK share snapshot cache leak: {leaked}")
    return result


def collect_share_changes(universe: Universe, start_date: str = "20000101",
                          budget_s: float = 180.0) -> dict:
    """Cache exact CNINFO share-capital changes for all selected A-shares.

    Kept separate from daily microstructure: this is the official event/PIT
    evidence used to validate reported or derived outstanding-share values.
    """
    d = _dir("share_changes")
    codes, _ = sixty_code_maps(universe)
    end_date = date.today().strftime("%Y%m%d")
    deadline = time.time() + budget_s
    result = {}
    for ref in (s for s in universe.stocks if s.market == Market.A):
        if time.time() > deadline:
            result[ref.symbol] = "timeout"
            continue
        records = akp.fetch_a_share_changes(ref.ak_symbol, start_date, end_date)
        if records is None:
            result[ref.symbol] = "failed"
            continue
        with open(d / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "source": "CNINFO p_stock2215",
                       "records": records}, f, ensure_ascii=False, indent=2)
        result[ref.symbol] = "cached"
    leaked = {p.stem for p in d.glob("*.json")} - codes
    if leaked:
        raise RuntimeError(f"share-change cache leak: {leaked}")
    return result


def _audit_freshness(universe: Universe, as_of: date) -> tuple[dict, list[str]]:
    """Audit latest dates separately from file/schema coverage."""
    results = {}
    errors = []
    specs = {
        "microstructure": (
            universe.stocks, "records", lambda blob: False
        ),
        "fund_flow_ths": (
            [stock for stock in universe.stocks if stock.market == Market.A],
            "daily_history",
            lambda blob: False,
        ),
        "margin": (
            [stock for stock in universe.stocks if stock.market == Market.A],
            "records",
            lambda blob: blob.get("availability") == "no_reported_history",
        ),
        "southbound": (
            [stock for stock in universe.stocks if stock.market == Market.HK],
            "records",
            lambda blob: blob.get("eligible") is False,
        ),
    }
    for dataset, (stocks, records_key, exempt) in specs.items():
        items = {}
        for ref in stocks:
            path = paths.CACHE_DIR / dataset / f"{ref.ak_symbol}.json"
            try:
                blob = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                blob = {}
            expected = _expected_symbol_session(ref, as_of)
            latest = _latest_dated_record(blob.get(records_key, []))
            if exempt(blob):
                status = "exempt"
            elif latest is None:
                status = "missing_date"
            elif latest < expected:
                status = "stale"
            else:
                status = "fresh"
            items[ref.symbol] = {
                "status": status,
                "latest_date": latest.isoformat() if latest else None,
                "expected_date": expected.isoformat(),
            }
            if status not in {"fresh", "exempt"}:
                errors.append(
                    f"{dataset}/{ref.ak_symbol}.json:{status}:"
                    f"latest={latest.isoformat() if latest else None}:"
                    f"expected={expected.isoformat()}"
                )
        results[dataset] = {
            "complete": all(
                item["status"] in {"fresh", "exempt"}
                for item in items.values()
            ),
            "items": items,
            "counts": dict(Counter(
                item["status"] for item in items.values()
            )),
        }

    global_specs = {
        "benchmarks": (
            paths.CACHE_DIR / "benchmarks",
            "records",
            {
                **{f"index_{name}": Market.A for name in (
                    "CSI300", "SSE", "SZSE"
                )},
                **{f"index_{name}": Market.HK for name in (
                    "HSI", "HSCEI", "HSTECH"
                )},
                **{
                    f"industry_{name}": Market.A
                    for name in (
                        "auto", "consumer_electronics", "food",
                        "home_appliance", "internet", "pcb_semi",
                    )
                },
            },
        ),
        "connect_flows": (
            paths.CACHE_DIR / "connect_flows",
            "records",
            {"northbound": Market.A, "southbound": Market.HK},
        ),
        "fx": (
            paths.CACHE_DIR / "fx",
            "records",
            {"HKDCNY": Market.HK},
        ),
    }
    for dataset, (directory, records_key, files) in global_specs.items():
        items = {}
        for stem, market in files.items():
            try:
                blob = json.load(open(
                    directory / f"{stem}.json", encoding="utf-8"
                ))
            except Exception:  # noqa: BLE001
                blob = {}
            latest = _latest_dated_record(blob.get(records_key, []))
            expected = qcal.latest_expected_session(market, as_of)
            status = (
                "missing_date" if latest is None
                else "stale" if latest < expected
                else "fresh"
            )
            items[stem] = {
                "status": status,
                "latest_date": latest.isoformat() if latest else None,
                "expected_date": expected.isoformat(),
            }
            if status != "fresh":
                errors.append(
                    f"{dataset}/{stem}.json:{status}:"
                    f"latest={latest.isoformat() if latest else None}:"
                    f"expected={expected.isoformat()}"
                )
        results[dataset] = {
            "complete": all(
                item["status"] == "fresh" for item in items.values()
            ),
            "items": items,
            "counts": dict(Counter(
                item["status"] for item in items.values()
            )),
        }
    return results, sorted(errors)


def _history_frame_mismatches(
    stored: pd.DataFrame,
    snapshot: pd.DataFrame,
) -> tuple[int, list[str]]:
    stored = stored.copy()
    snapshot = snapshot.copy()
    stored.index = pd.to_datetime(stored.index).normalize()
    snapshot.index = pd.to_datetime(snapshot.index).normalize()
    overlap = stored.index.intersection(snapshot.index)
    columns = [
        column for column in ("open", "high", "low", "close", "volume")
        if column in stored.columns and column in snapshot.columns
    ]
    count = 0
    dates = set()
    for observed in overlap:
        for column in columns:
            left = stored.at[observed, column]
            right = snapshot.at[observed, column]
            if pd.isna(left) and pd.isna(right):
                continue
            try:
                equal = math.isclose(
                    float(left), float(right), rel_tol=1e-9, abs_tol=1e-8
                )
            except (TypeError, ValueError):
                equal = left == right
            if not equal:
                count += 1
                dates.add(observed.date().isoformat())
    return count, sorted(dates)


def _audit_history_consistency(
    universe: Universe,
    as_of: date,
) -> tuple[dict, list[str]]:
    errors = []
    meta_paths = [
        path for path in paths.SNAPSHOTS_DIR.glob("*/*/meta.json")
        if str(path.parent.parent.name) <= as_of.isoformat()
    ]
    latest_meta = None
    if meta_paths:
        latest_meta = max(
            meta_paths,
            key=lambda path: (
                str(path.parent.parent.name), path.stat().st_mtime_ns
            ),
        )
    from .price_basis import comparison, differences
    snapshot_meta = json.loads(latest_meta.read_text()) if latest_meta else {}
    week_start = as_of - timedelta(days=as_of.weekday())
    basis_complete = True
    items = {}
    for ref in universe.stocks:
        expected = _expected_symbol_session(ref, as_of)
        safe = ref.symbol.replace(".", "_")
        snapshot_path = (
            latest_meta.parent / "ohlcv" / f"{safe}.parquet"
            if latest_meta else None
        )
        snapshot = None
        adjusted = None
        reference_error = None
        symbol_meta = snapshot_meta.get('symbols', {}).get(ref.symbol, {})
        if snapshot_path and snapshot_path.exists():
            try:
                snapshot = pd.read_parquet(snapshot_path)
                if (symbol_meta.get('ohlcv_basis') != 'none'
                        and not str(symbol_meta.get('ohlcv_source', '')).endswith('_raw')):
                    raise ValueError('raw snapshot basis is not explicit')
                adjusted_path = latest_meta.parent / 'ohlcv_standardized' / f'{safe}.parquet'
                if adjusted_path.exists():
                    adjusted = pd.read_parquet(adjusted_path)
                    if (symbol_meta.get('standardized_basis') != 'qfq'
                            or adjusted.attrs.get('price_basis') != 'qfq'
                            or adjusted.empty
                            or not symbol_meta.get('standardized_anchor_date')
                            or str(symbol_meta['standardized_anchor_date']) > as_of.isoformat()
                            or pd.to_datetime(adjusted.index).max().date().isoformat()
                               != str(symbol_meta['standardized_anchor_date'])):
                        raise ValueError('invalid standardized basis or future anchor')
            except Exception:  # noqa: BLE001
                reference_error = 'invalid_snapshot_price_basis'
                snapshot = None
        if adjusted is None:
            basis_complete = False
        views = {}
        for view, filename in (
            ("raw", f"{safe}_raw.parquet"),
            ("standardized", f"{safe}.parquet"),
        ):
            path = paths.STATE_DIR / "history" / filename
            latest = None
            mismatch_count = 0
            mismatch_dates = []
            basis_details = {}
            status = "missing"
            if path.exists():
                try:
                    frame = pd.read_parquet(path)
                    latest = (
                        pd.to_datetime(frame.index).max().date()
                        if not frame.empty else None
                    )
                    status = (
                        "stale" if latest is None or latest < expected
                        else "fresh"
                    )
                    if reference_error or snapshot is None:
                        status = 'invalid'
                        basis_details['error'] = reference_error or 'missing_raw_reference'
                    elif view == 'raw':
                        diff = differences(frame, snapshot)
                        mismatch_count, mismatch_dates = diff['cells'], diff['dates']
                        basis_details = {'comparison_basis': 'none', 'basis_verified': True}
                        if diff['overlap_rows'] < min(60, len(frame)):
                            status = 'invalid'
                        elif mismatch_count:
                            status = 'conflict'
                    else:
                        basis_details = comparison(frame, snapshot, adjusted,
                                                   week_start=week_start, as_of=as_of)
                        mismatch_count = basis_details['snapshot_mismatch_cells']
                        mismatch_dates = basis_details['snapshot_mismatch_dates']
                        if basis_details['status'] != 'fresh':
                            status = basis_details['status']
                        basis_details.pop('status')
                except Exception:  # noqa: BLE001
                    status = "invalid"
            views[view] = {
                "status": status,
                "latest_date": latest.isoformat() if latest else None,
                "expected_date": expected.isoformat(),
                "snapshot_mismatch_cells": mismatch_count,
                "snapshot_mismatch_dates": mismatch_dates,
                **basis_details,
            }
            if status != "fresh":
                errors.append(
                    f"history/{filename}:{status}:"
                    f"latest={latest.isoformat() if latest else None}:"
                    f"expected={expected.isoformat()}"
                )
        items[ref.symbol] = views
    result = {
        "complete": not errors and latest_meta is not None and basis_complete,
        "basis_complete": basis_complete,
        "latest_snapshot": str(latest_meta.parent) if latest_meta else None,
        "items": items,
        "counts": dict(Counter(
            view["status"]
            for item in items.values()
            for view in item.values()
        )),
    }
    if latest_meta is None:
        errors.append("history:no immutable snapshot available")
    elif not basis_complete:
        errors.append('history:legacy standardized basis lacks explicit qfq reference; not certified complete')
    return result, sorted(errors)


def audit_data_coverage(
    universe: Universe,
    as_of_date: str | date | None = None,
) -> dict:
    """Persist a full-domain coverage manifest for the fixed 60-stock universe."""
    if isinstance(as_of_date, str):
        as_of = date.fromisoformat(as_of_date)
    else:
        as_of = as_of_date or date.today()
    all_codes = {s.ak_symbol for s in universe.stocks}
    a_codes = {s.ak_symbol for s in universe.stocks if s.market == Market.A}
    hk_codes = {s.ak_symbol for s in universe.stocks if s.market == Market.HK}
    dense = {
        "fundamentals": all_codes,
        "financial_versions": all_codes,
        "statements": a_codes,
        "statements_hk": hk_codes,
        "announcements": all_codes,
        "dividends": a_codes,
        "dividends_hk": hk_codes,
        "hk_share_snapshot": hk_codes,
        "microstructure": all_codes,
        "fund_flow_ths": a_codes,
        "northbound": a_codes,
        "southbound": hk_codes,
        "listing": all_codes,
        "suspensions": all_codes,
        "hk_action_evidence": hk_codes,
    }
    optional = {
        "margin": a_codes,
        "margin_eligibility": a_codes,
        "share_changes": a_codes,
        "ownership_a": a_codes,
        "ownership_hk": hk_codes,
        "hk_market_disclosures": hk_codes,
        "consensus": all_codes,
        "hk_short_selling": hk_codes,
        "news": all_codes,
        "earnings_calendar": all_codes,
        "repurchases_a": a_codes,
        "block_trades_a": a_codes,
    }
    sparse = {
        "lhb": a_codes,
        "splits": a_codes,
    }
    global_specs = {
        "benchmarks": {
            "index_CSI300", "index_HSCEI", "index_HSI", "index_HSTECH",
            "index_SSE", "index_SZSE", "industry_auto",
            "industry_consumer_electronics", "industry_food",
            "industry_home_appliance", "industry_internet",
            "industry_pcb_semi",
        },
        "connect_flows": {"northbound", "southbound"},
        "fx": {"HKDCNY"},
        "industry": {"industry_map"},
        "macro": {"lpr", "shibor", "cpi", "pmi", "commodity_price_index"},
        "margin_market": {"SH", "SZ"},
    }
    datasets = {}
    clean = True
    for mode, specs in (
        ("core_dense", dense),
        ("optional_extended", optional),
        ("sparse_event_or_eligibility", sparse),
    ):
        for name, expected in specs.items():
            directory = paths.CACHE_DIR / name
            found = {p.stem for p in directory.glob("*.json")} if directory.exists() else set()
            leaked = sorted(found - all_codes)
            missing = sorted(expected - found)
            provider_errors = []
            if mode in {"core_dense", "optional_extended"}:
                for path in directory.glob("*.json"):
                    try:
                        blob = json.load(open(path, encoding="utf-8"))
                    except Exception:  # noqa: BLE001
                        provider_errors.append(path.stem)
                        continue
                    if blob.get("error") or blob.get("errors"):
                        provider_errors.append(path.stem)
                    if name == "statements_hk" and _hk_balance_issues(blob):
                        provider_errors.append(path.stem)
                    if name == "consensus":
                        estimates = blob.get("normalized_estimates", [])
                        invalid_horizon = False
                        for record in estimates:
                            try:
                                horizon = (
                                    int(record["fiscal_year"])
                                    - date.fromisoformat(
                                        str(record["updated_at"])[:10]
                                    ).year
                                )
                            except (KeyError, TypeError, ValueError):
                                invalid_horizon = True
                                break
                            if horizon not in range(0, 5):
                                invalid_horizon = True
                                break
                        no_change_revision = any(
                            record.get("eps_delta") in (None, 0, 0.0)
                            and record.get("target_price_delta")
                            in (None, 0, 0.0)
                            and record.get("rating_from")
                            == record.get("rating_to")
                            for record in blob.get("revisions", [])
                        )
                        if (
                            blob.get("normalization_version") != 4
                            or invalid_horizon
                            or no_change_revision
                        ):
                            provider_errors.append(path.stem)
                    if name == "margin" and any(
                        _num(record.get(field)) is not None
                        and _num(record.get(field)) < 0
                        for record in blob.get("records", [])
                        for field in (
                            "margin_repay", "short_repay_volume"
                        )
                    ):
                        provider_errors.append(path.stem)
                    if name == "northbound":
                        for record in blob.get("records", []):
                            close = _num(record.get("当日收盘价"))
                            shares = _num(record.get("持股数量"))
                            market_cap = _num(record.get("持股市值"))
                            if (
                                close is not None
                                and shares is not None
                                and market_cap not in (None, 0)
                                and not math.isclose(
                                    close * shares,
                                    market_cap,
                                    rel_tol=2e-4,
                                    abs_tol=10.0,
                                )
                            ):
                                provider_errors.append(path.stem)
                                break
            complete = (
                not leaked
                and (
                    mode not in {"core_dense", "optional_extended"}
                    or (not missing and not provider_errors)
                )
            )
            if mode == "core_dense":
                clean = clean and complete
            datasets[name] = {
                "mode": mode,
                "expected": len(expected),
                "files": len(found),
                "missing": missing,
                "leaked": leaked,
                "provider_errors": sorted(provider_errors),
                "complete": complete,
            }
    for name, expected in global_specs.items():
        directory = paths.CACHE_DIR / name
        found = (
            {path.stem for path in directory.glob("*.json")}
            if directory.exists() else set()
        )
        missing = sorted(expected - found)
        unexpected = sorted(found - expected)
        provider_errors = []
        for path in directory.glob("*.json"):
            try:
                blob = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                provider_errors.append(path.stem)
                continue
            if isinstance(blob, dict) and (
                blob.get("error") or blob.get("errors")
            ):
                provider_errors.append(path.stem)
            if name == "industry" and set(blob) != all_codes:
                provider_errors.append(path.stem)
            if name == "fx":
                records = blob.get("records", []) if isinstance(blob, dict) else []
                valid = [
                    record for record in records
                    if record.get("date")
                    and record.get("available_on")
                    and record.get("source_published_at")
                    and _num(record.get("hkd_cny")) is not None
                ]
                dates = sorted(record["date"] for record in valid)
                if (
                    blob.get("primary_field") != "hkd_cny"
                    or len(valid) < 7000
                    or not dates
                    or dates[0] > "1994-01-06"
                    or dates[-1] < (date.today() - timedelta(days=7)).isoformat()
                ):
                    provider_errors.append(path.stem)
            if name == "macro":
                records = (
                    blob.get("records", [])
                    if isinstance(blob, dict) else []
                )
                if (
                    not records
                    or blob.get("pit_policy_version") != 1
                    or any(
                        not record.get("available_on")
                        or not record.get("timestamp_precision")
                        for record in records
                    )
                ):
                    provider_errors.append(path.stem)
        complete = not missing and not unexpected and not provider_errors
        clean = clean and complete
        datasets[name] = {
            "mode": "global_fixed",
            "expected": len(expected),
            "files": len(found),
            "missing": missing,
            "leaked": unexpected,
            "provider_errors": sorted(set(provider_errors)),
            "complete": complete,
        }
    integrity_errors = []

    def scan_duplicate_lists(value, file_name: str, value_path: str = "$"):
        if isinstance(value, dict):
            for key, nested in value.items():
                scan_duplicate_lists(
                    nested, file_name, f"{value_path}.{key}"
                )
        elif isinstance(value, list):
            if value and all(isinstance(item, dict) for item in value):
                signatures = [
                    json.dumps(
                        item, ensure_ascii=False, sort_keys=True,
                        default=str, separators=(",", ":"),
                    )
                    for item in value
                ]
                duplicates = len(signatures) - len(set(signatures))
                if duplicates:
                    integrity_errors.append(
                        f"{file_name}:{value_path}:"
                        f"{duplicates} exact duplicate records"
                    )
            for index, nested in enumerate(value):
                if isinstance(nested, (dict, list)):
                    scan_duplicate_lists(
                        nested, file_name, f"{value_path}[{index}]"
                    )

    for path in paths.CACHE_DIR.rglob("*.json"):
        try:
            scan_duplicate_lists(
                json.load(open(path, encoding="utf-8")),
                str(path.relative_to(paths.CACHE_DIR)),
            )
        except Exception:  # noqa: BLE001
            integrity_errors.append(
                f"{path.relative_to(paths.CACHE_DIR)}:invalid JSON"
            )

    for code in a_codes:
        path = paths.CACHE_DIR / "ownership_a" / f"{code}.json"
        try:
            datasets_a = json.load(
                open(path, encoding="utf-8")
            ).get("datasets", {})
        except Exception:  # noqa: BLE001
            continue
        for dataset_name, observed_key in (
            ("holder_counts", "股东户数统计截止日"),
            ("top10_holders", "截至日期"),
            ("top10_float_holders", "截止日期"),
        ):
            for record in datasets_a.get(dataset_name, []):
                published = str(record.get("source_published_on") or "")[:10]
                observed = str(record.get(observed_key) or "")[:10]
                if published and observed and published < observed:
                    integrity_errors.append(
                        f"ownership_a/{code}.json:{dataset_name}:"
                        f"publication {published} precedes observation {observed}"
                    )

        version_path = (
            paths.CACHE_DIR / "financial_versions" / f"{code}.json"
        )
        try:
            versions = json.load(
                open(version_path, encoding="utf-8")
            ).get("versions", [])
        except Exception:  # noqa: BLE001
            continue
        identities = set()
        for version in versions:
            identity = (
                version.get("dataset"),
                version.get("section"),
                version.get("report_date"),
                version.get("content_hash"),
            )
            if identity in identities:
                integrity_errors.append(
                    f"financial_versions/{code}.json:"
                    f"duplicate version {identity[:3]}"
                )
            identities.add(identity)
            if (
                version.get("dataset") == "statements"
                and not version.get("available_on")
            ):
                integrity_errors.append(
                    f"financial_versions/{code}.json:"
                    f"statement {identity[1:3]} missing available_on"
                )

    for code in hk_codes:
        path = paths.CACHE_DIR / "southbound" / f"{code}.json"
        try:
            records = json.load(open(path, encoding="utf-8")).get(
                "records", []
            )
        except Exception:  # noqa: BLE001
            continue
        dates = [record.get("date") for record in records]
        if len(dates) != len(set(dates)):
            integrity_errors.append(
                f"southbound/{code}.json:duplicate trade dates"
            )

    for path in (paths.CACHE_DIR / "benchmarks").glob("industry_*.json"):
        try:
            blob = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        counts = [
            record.get("constituent_count")
            for record in blob.get("records", [])
        ]
        if (
            blob.get("return_currency") != "CNY"
            or blob.get("corporate_action_policy")
            != "neutral_on_effective_date"
            or not counts
            or min(counts) != len(blob.get("symbols", []))
        ):
            integrity_errors.append(
                f"benchmarks/{path.name}:invalid derived-index semantics"
            )

    for meta_path in paths.SNAPSHOTS_DIR.glob("*/*/meta.json"):
        try:
            snapshot_meta = json.load(open(meta_path, encoding="utf-8"))
            snapshot_as_of = pd.Timestamp(snapshot_meta["as_of"])
        except Exception:  # noqa: BLE001
            integrity_errors.append(
                f"{meta_path}:invalid snapshot metadata"
            )
            continue
        future_rows = 0
        for parquet_path in (meta_path.parent / "ohlcv").glob("*.parquet"):
            try:
                frame = pd.read_parquet(parquet_path)
                future_rows += int(
                    (pd.to_datetime(frame.index) > snapshot_as_of).sum()
                )
            except Exception:  # noqa: BLE001
                integrity_errors.append(
                    f"{parquet_path}:invalid snapshot parquet"
                )
        validation_path = meta_path.parent / "validation.json"
        validation = {}
        if validation_path.exists():
            try:
                validation = json.load(
                    open(validation_path, encoding="utf-8")
                )
            except Exception:  # noqa: BLE001
                integrity_errors.append(
                    f"{validation_path}:invalid validation evidence"
                )
        if future_rows and (
            validation.get("status") != "invalid_for_replay"
            or int(validation.get("future_row_count") or 0) != future_rows
        ):
            integrity_errors.append(
                f"{meta_path.parent}:future rows not quarantined"
            )

    integrity_errors = sorted(set(integrity_errors))
    freshness, freshness_errors = _audit_freshness(universe, as_of)
    history_consistency, history_errors = _audit_history_consistency(
        universe, as_of
    )
    record_integrity_complete = not integrity_errors
    freshness_complete = not freshness_errors
    history_consistency_complete = not history_errors
    all_datasets_complete = all(
        value["complete"] for value in datasets.values()
    )
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "as_of_date": as_of.isoformat(),
        "universe_size": len(all_codes),
        "core_dense_complete": clean,
        "dense_complete": clean,
        "optional_extended_complete": all(
            value["complete"]
            for value in datasets.values()
            if value["mode"] == "optional_extended"
        ),
        "scoped_directories_clean": all(
            not value["leaked"] for value in datasets.values()
        ),
        "record_integrity_complete": record_integrity_complete,
        "record_integrity_errors": integrity_errors,
        "freshness_complete": freshness_complete,
        "freshness_errors": freshness_errors,
        "freshness": freshness,
        "history_consistency_complete": history_consistency_complete,
        "history_consistency_errors": history_errors,
        "history_consistency": history_consistency,
        "all_domains_complete": (
            all_datasets_complete
            and record_integrity_complete
            and freshness_complete
            and history_consistency_complete
        ),
        "datasets": datasets,
        "audit_scope": [
            "all fixed-60 per-symbol data domains",
            "all fixed global benchmark, FX, macro, and market series",
            "session-level PIT availability and corporate actions",
            "financial publication, currency, unit, and revision semantics",
            "microstructure, connect, margin, ownership, event, and consensus histories",
        ],
        "ignored_audit_topics": [],
        "known_irreducible_gaps": [
            "Northbound per-stock daily disclosure after 2024-08-16: discontinued by regulation",
            "HKEX CCASS public-search terms prohibit programmatic/systematic retrieval; Southbound aggregate holdings are retained instead",
            "HKEX has no A-share-style per-stock block-trade table; licensed transaction data is required",
        ],
    }
    d = _dir("coverage")
    with open(d / "data_coverage.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return manifest
