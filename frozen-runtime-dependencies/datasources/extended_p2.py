"""P2 extended information collectors for the fixed 60-stock universe."""

from __future__ import annotations

import json
import hashlib
import re
import time
from collections import Counter
from datetime import date, datetime, timedelta
from html import unescape
from io import StringIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests

from .. import paths
from ..schemas import Market, Universe
from . import akshare_provider as akp
from . import evidence
from . import tencent_kline
from . import availability as pit_availability


def _dir(name: str) -> Path:
    path = paths.CACHE_DIR / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _primitive(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _records(frame: pd.DataFrame | None) -> list[dict]:
    if frame is None or frame.empty:
        return []
    return [
        {str(key): _primitive(value) for key, value in row.to_dict().items()}
        for _, row in frame.iterrows()
    ]


def _dedupe_records(records: list[dict]) -> list[dict]:
    """Remove exact provider duplicates while preserving source order."""
    seen = set()
    unique = []
    for record in records:
        identity = json.dumps(
            record, ensure_ascii=False, sort_keys=True, default=str,
            separators=(",", ":"),
        )
        if identity in seen:
            continue
        seen.add(identity)
        unique.append(record)
    return unique


def _guard_codes(directory: Path, allowed: set[str], label: str) -> None:
    leaked = {p.stem for p in directory.glob("*.json")} - allowed
    if leaked:
        raise RuntimeError(f"{label} cache leak: {sorted(leaked)}")


def _availability_fields(
    value,
    market: Market,
    *,
    force_date_only: bool = True,
) -> dict:
    resolved = pit_availability.publication_availability(
        value,
        market,
        force_date_only=force_date_only,
    )
    if not resolved:
        return {
            "source_published_at": None,
            "source_published_on": None,
            "timestamp_precision": "unknown",
            "available_on": None,
            "availability_status": "publication_date_unavailable",
        }
    return {
        **resolved,
        "availability_status": "available",
    }


def _enrich_records(
    records: list[dict],
    market: Market,
    date_keys: tuple[str, ...],
    *,
    force_date_only: bool = True,
) -> list[dict]:
    out = []
    for record in records:
        value = next(
            (record.get(key) for key in date_keys if record.get(key) not in (None, "")),
            None,
        )
        out.append({
            **record,
            **_availability_fields(
                value,
                market,
                force_date_only=force_date_only,
            ),
        })
    return out


def _ths_a_holder_supplement(symbol: str) -> dict:
    """Parse THS F10 holder counts, float holders, and pledge summaries."""
    from bs4 import BeautifulSoup

    response = requests.get(
        f"https://basic.10jqka.com.cn/{symbol}/holder.html",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()
    response.encoding = "gbk"
    soup = BeautifulSoup(response.text, "lxml")
    result = {
        "holder_counts": [],
        "top10_float_holders": [],
        "pledges": [],
    }

    holder = soup.find(id="holdernum")
    if holder is not None:
        labels = [
            item.get_text(" ", strip=True)
            for item in holder.select(".left_thead table.tbody tr th")
        ]
        dates = [
            item.get_text(" ", strip=True)
            for item in holder.select(".data_tbody table.top_thead th")
        ]
        rows = holder.select(".data_tbody table.tbody tr")
        values = [
            [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
            for row in rows[:len(labels)]
        ]
        for index, observed in enumerate(dates):
            record = {
                "股东户数统计截止日": observed,
                "来源": "THS F10 holder page",
            }
            for label, row_values in zip(labels, values):
                if index < len(row_values):
                    record[label] = row_values[index]
            result["holder_counts"].append(record)

    flow = soup.find(id="flowholder")
    if flow is not None:
        for anchor in flow.select("a.fdates"):
            target = anchor.get("targ")
            container = flow.find(id=target) if target else None
            table = container.find("table") if container else None
            if table is None:
                continue
            try:
                records = _records(pd.read_html(StringIO(str(table)))[0])
            except Exception:  # noqa: BLE001
                continue
            for record in records:
                result["top10_float_holders"].append({
                    "截止日期": anchor.get_text(" ", strip=True),
                    "来源": "THS F10 holder page",
                    **record,
                })

    top = soup.find(id="tenholder")
    if top is not None:
        anchor = top.select_one("a.tdates")
        target = anchor.get("targ") if anchor else None
        container = top.find(id=target) if target else None
        table = container.find("table") if container else None
        if table is not None:
            try:
                records = _records(pd.read_html(StringIO(str(table)))[0])
            except Exception:  # noqa: BLE001
                records = []
            observed = anchor.get_text(" ", strip=True) if anchor else None
            for record in records:
                pledge = record.get("质押占其直接持股比")
                if pledge in (None, "", "无质押", "0", "0%"):
                    continue
                result["pledges"].append({
                    "报告期": observed,
                    "来源": "THS F10 top-holder pledge summary",
                    **record,
                })
    return result


def collect_a_ownership(
    universe: Universe, budget_s: float = 600.0, force: bool = False
) -> dict:
    """Collect holder counts/top holders/lockups/pledges/holder changes for A shares."""
    import akshare as ak

    allowed = {s.ak_symbol for s in universe.stocks}
    stocks = [s for s in universe.stocks if s.market == Market.A]
    directory = _dir("ownership_a")
    def worker(ref):
        path = directory / f"{ref.ak_symbol}.json"
        if path.exists() and not force:
            try:
                if not json.load(open(path, encoding="utf-8")).get("errors"):
                    return ref.symbol, "cached"
            except Exception:  # noqa: BLE001
                pass
        datasets = {}
        errors = {}
        availability = {}
        calls = {
            "holder_counts": lambda: ak.stock_zh_a_gdhs_detail_em(ref.ak_symbol),
            "top10_holders": lambda: ak.stock_main_stock_holder(ref.ak_symbol),
            "top10_float_holders": lambda: ak.stock_circulate_stock_holder(ref.ak_symbol),
            "lockup_releases": lambda: ak.stock_restricted_release_queue_em(ref.ak_symbol),
            "pledges": lambda: ak.stock_gpzy_individual_pledge_ratio_detail_em(ref.ak_symbol),
            "holder_changes": lambda: ak.stock_shareholder_change_ths(ref.ak_symbol),
        }
        for key, call in calls.items():
            try:
                datasets[key] = _dedupe_records(_records(call()))
                availability[key] = "available" if datasets[key] else "empty"
            except Exception as exc:  # noqa: BLE001
                datasets[key] = []
                if isinstance(exc, (TypeError, KeyError)):
                    availability[key] = "empty_or_unsupported"
                else:
                    availability[key] = "failed"
                    errors[key] = f"{type(exc).__name__}: {str(exc)[:100]}"
        if any(not datasets[key] for key in (
            "holder_counts", "top10_float_holders", "pledges"
        )):
            try:
                supplement = _ths_a_holder_supplement(ref.ak_symbol)
            except Exception as exc:  # noqa: BLE001
                supplement = {}
                errors["ths_holder_supplement"] = (
                    f"{type(exc).__name__}: {str(exc)[:100]}"
                )
            for key in ("holder_counts", "top10_float_holders", "pledges"):
                if datasets[key] or not supplement.get(key):
                    continue
                datasets[key] = supplement[key]
                availability[key] = (
                    "available_summary_only"
                    if key == "pledges"
                    else "available_fallback"
                )
            if not datasets["pledges"] and "ths_holder_supplement" not in errors:
                availability["pledges"] = "confirmed_no_current_pledge"
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "captured_at": datetime.now().isoformat(),
                "datasets": datasets,
                "availability": availability,
                "errors": errors,
            }, f, ensure_ascii=False, indent=2)
        return ref.symbol, "cached" if not errors else "partial"

    summary = _run_parallel(stocks, worker, budget_s=budget_s, max_workers=6)
    _guard_codes(directory, allowed, "A ownership")
    return summary


def collect_hk_ownership(universe: Universe, budget_s: float = 180.0) -> dict:
    """Collect HK major-holder snapshots and disclosed holding changes from THS."""
    from io import StringIO

    from bs4 import BeautifulSoup

    allowed = {s.ak_symbol for s in universe.stocks}
    stocks = [s for s in universe.stocks if s.market == Market.HK]
    directory = _dir("ownership_hk")

    def worker(ref):
        try:
            response = requests.get(
                "https://stockpage.10jqka.com.cn/basicweb/176/"
                f"HK{int(ref.ak_symbol):04d}/holder.html",
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
            response.raise_for_status()
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text, "lxml")
            snapshots = []
            main = soup.find(id="main")
            if main is not None:
                for table in main.find_all("table"):
                    try:
                        frame = pd.read_html(StringIO(str(table)))[0]
                    except Exception:  # noqa: BLE001
                        continue
                    report_date = table.get("data-date")
                    for record in _records(frame):
                        snapshots.append({"report_date": report_date, **record})
            changes = []
            change = soup.find(id="change")
            if change is not None:
                for table in change.find_all("table"):
                    try:
                        changes.extend(
                            _records(pd.read_html(StringIO(str(table)))[0])
                        )
                    except Exception:  # noqa: BLE001
                        continue
            snapshots = _dedupe_records(snapshots)
            changes = _dedupe_records(changes)
            error, status = None, "cached"
        except Exception as exc:  # noqa: BLE001
            snapshots, changes = [], []
            error, status = f"{type(exc).__name__}: {str(exc)[:100]}", "failed"
        with open(directory / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "source": "THS HK F10 holder page",
                "captured_at": datetime.now().isoformat(),
                "major_holder_snapshots": snapshots,
                "holding_changes": changes,
                "error": error,
            }, f, ensure_ascii=False, indent=2)
        return ref.symbol, status

    summary = _run_parallel(stocks, worker, budget_s=budget_s, max_workers=4)
    _guard_codes(directory, allowed, "HK ownership")
    return summary


def _hk_di_rows(page_text: str, base_url: str) -> tuple[list[dict], set[int]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(page_text, "lxml")
    table = None
    for candidate in soup.find_all("table"):
        first = candidate.find("tr")
        if first and "Form Serial Number" in first.get_text(" ", strip=True):
            table = candidate
            break
    if table is None:
        return [], set()
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
        if len(cells) < 8 or not cells[0]:
            continue
        if not re.fullmatch(r"(?:DA|CS)\d{8}E\d+", cells[0]):
            continue
        event_date = pd.to_datetime(cells[7], dayfirst=True, errors="coerce")
        if pd.isna(event_date):
            continue
        link = tr.find("a", href=True)
        rows.append({
            "form_serial": cells[0],
            "holder_or_director": cells[1],
            "reason_code": cells[2],
            "shares_involved": cells[3],
            "average_price": cells[4],
            "present_interest": cells[5],
            "present_interest_pct": cells[6],
            "relevant_event_date": cells[7],
            "form_url": (
                requests.compat.urljoin(base_url, link["href"]) if link else None
            ),
        })
    pages = set()
    for link in soup.find_all("a", href=True):
        match = re.search(r"(?:[?&])pg=(\d+)", link["href"])
        if match:
            pages.add(int(match.group(1)))
    total_match = re.search(
        r"Total\s+records\s*:\s*(\d+)",
        soup.get_text(" ", strip=True),
        flags=re.IGNORECASE,
    )
    if total_match:
        total = int(total_match.group(1))
        pages.update(range(1, (total + 49) // 50 + 1))
    return rows, pages


def collect_hk_market_disclosures(
    universe: Universe,
    start_date: str = "20030101",
    budget_s: float = 600.0,
) -> dict:
    """Collect official HKEX DI histories and explicit market-data limitations."""
    from bs4 import BeautifulSoup

    stocks = [stock for stock in universe.stocks if stock.market == Market.HK]
    allowed = {stock.ak_symbol for stock in universe.stocks}
    directory = _dir("hk_market_disclosures")
    start = datetime.strptime(start_date, "%Y%m%d").strftime("%d/%m/%Y")
    end = date.today().strftime("%d/%m/%Y")
    base = "https://di.hkex.com.hk/di/"

    def worker(ref):
        search_params = {
            "sa1": "cl", "scsd": start, "sced": end,
            "sc": ref.ak_symbol, "src": "Main", "lang": "EN",
        }
        response = requests.get(
            base + "NSSrchCorpList.aspx",
            params=search_params,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30,
        )
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        all_forms = next(
            (
                link.get("href") for link in soup.find_all("a", href=True)
                if "NSAllFormList.aspx" in link.get("href", "")
            ),
            None,
        )
        records = []
        if all_forms:
            forms_url = requests.compat.urljoin(base, all_forms)
            first = requests.get(
                forms_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30
            )
            first.raise_for_status()
            rows, pages = _hk_di_rows(first.text, forms_url)
            records.extend(rows)
            for page in sorted(pages - {1}):
                page_response = requests.get(
                    forms_url,
                    params={"pg": page},
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=30,
                )
                page_response.raise_for_status()
                page_rows, _ = _hk_di_rows(page_response.text, forms_url)
                records.extend(page_rows)
        unique = {record["form_serial"]: record for record in records}
        records = sorted(
            unique.values(),
            key=lambda item: (
                pd.to_datetime(
                    item.get("relevant_event_date"), dayfirst=True, errors="coerce"
                ),
                item["form_serial"],
            ),
        )
        with open(directory / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "range": [
                    datetime.strptime(start, "%d/%m/%Y").date().isoformat(),
                    date.today().isoformat(),
                ],
                "official_di": {
                    "source": "HKEX Disclosure of Interests",
                    "records": records,
                    "availability": "available",
                },
                "ccass": {
                    "availability": "not_programmatically_collected",
                    "reason": (
                        "HKEX public search terms prohibit programmatic/systematic "
                        "retrieval; no Tushare token or Futu OpenD is configured"
                    ),
                    "covered_alternative": (
                        "official Southbound aggregate holdings remain in "
                        ".cache/southbound"
                    ),
                },
                "block_trades": {
                    "availability": "not_separately_disclosed",
                    "reason": (
                        "HKEX has no A-share-style per-stock block-trade table; "
                        "off-exchange trades require licensed transaction data"
                    ),
                },
            }, f, ensure_ascii=False, indent=2)
        return ref.symbol, "cached"

    result = _run_parallel(stocks, worker, budget_s=budget_s, max_workers=4)
    _guard_codes(directory, allowed, "HK market disclosures")
    return result


def collect_a_market_events(
    universe: Universe,
    start_date: str = "20230101",
    end_date: str | None = None,
) -> dict:
    """Collect A-share repurchases and block trades, filtered to selected codes."""
    import akshare as ak

    allowed = {s.ak_symbol for s in universe.stocks}
    a_codes = {s.ak_symbol for s in universe.stocks if s.market == Market.A}
    out = {"repurchases": {}, "block_trades": {}}

    try:
        frame = ak.stock_repurchase_em()
        frame["股票代码"] = frame["股票代码"].astype(str).str.zfill(6)
        frame = frame[frame["股票代码"].isin(a_codes)]
        for code, group in frame.groupby("股票代码"):
            out["repurchases"][code] = _records(group)
    except Exception as exc:  # noqa: BLE001
        out["repurchase_error"] = f"{type(exc).__name__}: {str(exc)[:100]}"

    start = datetime.strptime(start_date, "%Y%m%d").date()
    end = datetime.strptime(end_date, "%Y%m%d").date() if end_date else date.today()
    cursor = start
    while cursor <= end:
        month_end = min(
            (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
            - timedelta(days=1),
            end,
        )
        try:
            frame = ak.stock_dzjy_mrmx(
                symbol="A股",
                start_date=cursor.strftime("%Y%m%d"),
                end_date=month_end.strftime("%Y%m%d"),
            )
            if frame is not None and not frame.empty:
                frame["证券代码"] = frame["证券代码"].astype(str).str.zfill(6)
                frame = frame[frame["证券代码"].isin(a_codes)]
                for code, group in frame.groupby("证券代码"):
                    out["block_trades"].setdefault(code, []).extend(_records(group))
        except Exception as exc:  # noqa: BLE001
            out.setdefault("block_trade_errors", []).append({
                "month": cursor.strftime("%Y-%m"),
                "error": f"{type(exc).__name__}: {str(exc)[:100]}",
            })
        cursor = month_end + timedelta(days=1)

    for directory_name, key in (
        ("repurchases_a", "repurchases"),
        ("block_trades_a", "block_trades"),
    ):
        directory = _dir(directory_name)
        for ref in (s for s in universe.stocks if s.market == Market.A):
            with open(directory / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
                json.dump({"symbol": ref.symbol, "records": out[key].get(ref.ak_symbol, [])},
                          f, ensure_ascii=False, indent=2)
        _guard_codes(directory, allowed, directory_name)
    return {
        "repurchase_symbols": len(out["repurchases"]),
        "block_trade_symbols": len(out["block_trades"]),
        "repurchase_error": out.get("repurchase_error"),
        "block_trade_errors": out.get("block_trade_errors", []),
    }


def collect_consensus(
    universe: Universe, budget_s: float = 600.0, force: bool = False
) -> dict:
    """Collect A-share research forecasts and HK broker consensus."""
    import akshare as ak

    allowed = {s.ak_symbol for s in universe.stocks}
    directory = _dir("consensus")
    def worker(ref):
        path = directory / f"{ref.ak_symbol}.json"
        existing = {}
        if path.exists() and not force:
            try:
                existing = json.load(open(path, encoding="utf-8"))
                if not existing.get("error") \
                        and existing.get("normalization_version") == 4:
                    return ref.symbol, "cached"
            except Exception:  # noqa: BLE001
                pass
        source = (
            "stock_research_report_em"
            if ref.market == Market.A else "stock_hk_profit_forecast_et"
        )
        records = existing.get("records", [])
        error = existing.get("error")
        status = "cached"
        if not records or force:
            try:
                if ref.market == Market.A:
                    frame = ak.stock_research_report_em(symbol=ref.ak_symbol)
                    source = "stock_research_report_em"
                else:
                    frame = ak.stock_hk_profit_forecast_et(
                        symbol=ref.ak_symbol, indicator="盈利预测概览"
                    )
                    source = "stock_hk_profit_forecast_et"
                records = _records(frame)
                error = None
            except Exception as exc:  # noqa: BLE001
                records = []
                status = "failed"
                error = f"{type(exc).__name__}: {str(exc)[:100]}"
        historical_records = existing.get("historical_records", [])
        if not historical_records:
            historical_records = existing.get("records", [])
        by_hash = {
            hashlib.sha256(json.dumps(
                item, ensure_ascii=False, sort_keys=True, default=str
            ).encode()).hexdigest(): item
            for item in [*historical_records, *records]
        }
        historical_records = list(by_hash.values())
        normalized, history, revisions = _normalize_consensus(
            ref, historical_records
        )
        snapshot_hash = hashlib.sha256(json.dumps(
            records, ensure_ascii=False, sort_keys=True, default=str
        ).encode()).hexdigest()
        capture_history = existing.get("capture_history", [])
        if not any(item.get("content_hash") == snapshot_hash
                   for item in capture_history):
            capture_history.append({
                "captured_on": date.today().isoformat(),
                "content_hash": snapshot_hash,
                "record_count": len(records),
            })
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "source": source,
                       "records": records,
                       "historical_records": historical_records,
                       "capture_history": capture_history,
                       "source_history_capability": (
                           "report_level_history"
                           if ref.market == Market.A
                           else "local_snapshots_accumulating_from_first_capture"
                       ),
                       "normalization_version": 4,
                       "normalized_estimates": normalized,
                       "consensus_history": history,
                       "revisions": revisions,
                       "error": error},
                      f, ensure_ascii=False, indent=2)
        return ref.symbol, status

    summary = _run_parallel(
        list(universe.stocks), worker, budget_s=budget_s, max_workers=6
    )
    _guard_codes(directory, allowed, "consensus")
    return summary


def _normalize_consensus(ref, records: list[dict]) -> tuple[list, list, list]:
    """Normalize A/H broker estimates and build point-in-time consensus history."""
    estimates = []
    for row in records:
        if ref.market == Market.A:
            updated = _primitive(row.get("日期"))
            broker = _primitive(row.get("机构"))
            rating = _primitive(row.get("东财评级"))
            by_year = {}
            for key, value in row.items():
                match = re.match(r"(\d{4})-盈利预测-(收益|市盈率)$", str(key))
                if match:
                    by_year.setdefault(int(match.group(1)), {})[
                        "eps" if match.group(2) == "收益" else "pe"
                    ] = _primitive(value)
            for fiscal_year, values in by_year.items():
                try:
                    updated_year = date.fromisoformat(
                        str(updated)[:10]
                    ).year
                except (TypeError, ValueError):
                    continue
                if (
                    not updated_year <= fiscal_year <= updated_year + 4
                    or all(values.get(key) is None for key in ("eps", "pe"))
                ):
                    continue
                estimates.append({
                    "updated_at": str(updated)[:10],
                    "broker": broker,
                    "fiscal_year": fiscal_year,
                    "eps": values.get("eps"),
                    "pe": values.get("pe"),
                    "target_price": None,
                    "rating": rating,
                    "estimate_currency": "CNY",
                    "report_title": _primitive(row.get("报告名称")),
                    "report_url": _primitive(row.get("报告PDF链接")),
                })
        else:
            updated = _primitive(row.get("更新日期"))
            try:
                fiscal_year = int(row.get("财政年度"))
            except (TypeError, ValueError):
                continue
            estimates.append({
                "updated_at": str(updated)[:10],
                "broker": _primitive(row.get("证券商")),
                "fiscal_year": fiscal_year,
                # EastMoney HK expresses per-share forecasts in cents.
                "eps": (
                    float(row["每股盈利"]) / 100.0
                    if row.get("每股盈利") is not None else None
                ),
                "pe": None,
                "target_price": _primitive(row.get("目标价")),
                "rating": _primitive(row.get("评级")),
                "estimate_currency": "HKD",
                "net_profit_million": _primitive(row.get("纯利/亏损")),
                "dividend_per_share": (
                    float(row["每股派息"]) / 100.0
                    if row.get("每股派息") is not None else None
                ),
            })

    clean = []
    for estimate in estimates:
        try:
            observed = date.fromisoformat(str(estimate["updated_at"])[:10])
        except (TypeError, ValueError):
            continue
        available = pit_availability.publication_availability(
            observed.isoformat(), ref.market, force_date_only=True
        )
        clean.append({
            **estimate,
            "available_on": available["available_on"] if available else None,
            "timestamp_precision": "date_only",
            "has_numeric_estimate": any(
                estimate.get(key) is not None
                for key in ("eps", "pe", "target_price")
            ),
        })
    clean.sort(key=lambda item: (
        item.get("updated_at") or "",
        item.get("broker") or "",
        item.get("fiscal_year") or 0,
    ))

    revisions = []
    prior = {}
    for estimate in clean:
        key = (estimate.get("broker"), estimate.get("fiscal_year"))
        previous = prior.get(key)
        if previous:
            revision = {
                "updated_at": estimate["updated_at"],
                "available_on": estimate["available_on"],
                "broker": estimate.get("broker"),
                "fiscal_year": estimate.get("fiscal_year"),
                "eps_delta": _numeric_delta(estimate.get("eps"), previous.get("eps")),
                "target_price_delta": _numeric_delta(
                    estimate.get("target_price"), previous.get("target_price")
                ),
                "rating_from": previous.get("rating"),
                "rating_to": estimate.get("rating"),
            }
            if (
                revision["eps_delta"] not in (None, 0, 0.0)
                or revision["target_price_delta"] not in (None, 0, 0.0)
                or revision["rating_from"] != revision["rating_to"]
            ):
                revisions.append(revision)
        prior[key] = estimate
    revisions = _dedupe_records(revisions)

    state = {}
    history = []
    for updated_at in sorted({item["updated_at"] for item in clean}):
        observed = date.fromisoformat(updated_at)
        for estimate in (item for item in clean if item["updated_at"] == updated_at):
            state[(estimate.get("broker"), estimate.get("fiscal_year"))] = estimate
        active = [
            item for item in state.values()
            if (observed - date.fromisoformat(item["updated_at"])).days <= 365
        ]
        for fiscal_year in sorted({
            item["fiscal_year"] for item in active if item.get("fiscal_year")
        }):
            group = [item for item in active if item["fiscal_year"] == fiscal_year]
            numeric_group = [
                item for item in group if item.get("has_numeric_estimate")
            ]
            eps = [float(item["eps"]) for item in group if item.get("eps") is not None]
            targets = [
                float(item["target_price"])
                for item in group if item.get("target_price") is not None
            ]
            history.append({
                "as_of_date": updated_at,
                "available_on": max(
                    (item.get("available_on") or updated_at) for item in group
                ),
                "fiscal_year": fiscal_year,
                "coverage": len({
                    item.get("broker") for item in numeric_group
                }),
                "numeric_coverage": len({
                    item.get("broker") for item in numeric_group
                }),
                "rating_coverage": len({
                    item.get("broker") for item in group
                    if item.get("rating") is not None
                }),
                "eps_mean": float(pd.Series(eps).mean()) if eps else None,
                "eps_median": float(pd.Series(eps).median()) if eps else None,
                "eps_min": min(eps) if eps else None,
                "eps_max": max(eps) if eps else None,
                "target_price_mean": (
                    float(pd.Series(targets).mean()) if targets else None
                ),
                "target_price_median": (
                    float(pd.Series(targets).median()) if targets else None
                ),
                "rating_counts": dict(Counter(
                    str(item.get("rating") or "unrated") for item in group
                )),
            })
    return clean, history, revisions


def _numeric_delta(current, previous) -> float | None:
    try:
        if current is None or previous is None:
            return None
        return float(current) - float(previous)
    except (TypeError, ValueError):
        return None


def _eastmoney_news(symbol: str, page_size: int = 20) -> list[dict]:
    """Direct parser avoiding AKShare's pyarrow-invalid ``\\u`` regex."""
    inner = {
        "uid": "", "keyword": symbol, "type": ["cmsArticleWebOld"],
        "client": "web", "clientType": "web", "clientVersion": "curr",
        "param": {"cmsArticleWebOld": {
            "searchScope": "default", "sort": "default",
            "pageIndex": 1, "pageSize": page_size,
            "preTag": "<em>", "postTag": "</em>",
        }},
    }
    params = {"cb": "cb", "param": json.dumps(inner, ensure_ascii=False), "_": "1"}
    headers = {
        "Accept": "*/*",
        "Referer": f"https://so.eastmoney.com/news/s?keyword={symbol}",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
        ),
    }
    try:
        # AKShare depends on curl_cffi. EastMoney currently rejects or stalls
        # plain requests but accepts a normal browser TLS fingerprint.
        from curl_cffi import requests as browser_requests
    except ImportError:  # pragma: no cover - compatibility with older AKShare
        browser_requests = None
    last_error = None
    for attempt in range(3):
        try:
            if browser_requests is not None:
                response = browser_requests.get(
                    "https://search-api-web.eastmoney.com/search/jsonp",
                    params=params,
                    headers=headers,
                    impersonate="chrome120",
                    timeout=30,
                )
            else:
                response = requests.get(
                    "https://search-api-web.eastmoney.com/search/jsonp",
                    params=params,
                    headers=headers,
                    timeout=30,
                )
            response.raise_for_status()
            break
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (2 ** attempt))
    else:
        raise last_error
    text = response.text
    payload = json.loads(text[text.find("(") + 1:text.rfind(")")])
    if payload.get("code") not in (None, 0):
        raise RuntimeError(f"EastMoney news code={payload.get('code')}")
    result = payload.get("result")
    if not isinstance(result, dict) or "cmsArticleWebOld" not in result:
        raise RuntimeError("EastMoney news response has no result set")
    records = []
    for row in result.get("cmsArticleWebOld", []):
        clean = lambda value: re.sub(r"</?em>", "", str(value or "")).replace("\u3000", "")
        records.append({
            "published_at": row.get("date"),
            "title": clean(row.get("title")),
            "content": clean(row.get("content")),
            "media": row.get("mediaName"),
            "url": row.get("url") or (
                "http://finance.eastmoney.com/a/" + str(row.get("code")) + ".html"
            ),
        })
    return records


def collect_news(
    universe: Universe, budget_s: float = 300.0, force: bool = False
) -> dict:
    """Collect the latest per-symbol news snapshot; this is not historical news."""
    allowed = {s.ak_symbol for s in universe.stocks}
    directory = _dir("news")
    def worker(ref):
        path = directory / f"{ref.ak_symbol}.json"
        if path.exists() and not force:
            try:
                if not json.load(open(path, encoding="utf-8")).get("error"):
                    return ref.symbol, "cached"
            except Exception:  # noqa: BLE001
                pass
        try:
            records = _eastmoney_news(ref.ak_symbol)
            status, error = "cached", None
        except Exception as exc:  # noqa: BLE001
            records = []
            status, error = "failed", f"{type(exc).__name__}: {str(exc)[:100]}"
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "captured_at": datetime.now().isoformat(),
                "data_shape": "latest_search_snapshot",
                "records": records,
                "error": error,
            }, f, ensure_ascii=False, indent=2)
        return ref.symbol, status

    summary = _run_parallel(
        list(universe.stocks), worker, budget_s=budget_s, max_workers=4
    )
    _guard_codes(directory, allowed, "news")
    return summary


def collect_earnings_calendar(universe: Universe) -> dict:
    """Build report publication events and upcoming board-meeting evidence."""
    import akshare as ak

    directory = _dir("earnings_calendar")
    allowed = {s.ak_symbol for s in universe.stocks}
    scheduled: dict[str, list[dict]] = {}
    for period in ("2025年报", "2026一季", "2026中报", "2026三季"):
        try:
            frame = ak.stock_report_disclosure("沪深京", period)
        except Exception:  # noqa: BLE001
            continue
        if frame is None or frame.empty or "股票代码" not in frame.columns:
            continue
        frame["股票代码"] = frame["股票代码"].astype(str).str.zfill(6)
        frame = frame[frame["股票代码"].isin(allowed)]
        for _, row in frame.iterrows():
            code = str(row["股票代码"])
            scheduled.setdefault(code, []).append({
                "event": "scheduled_disclosure",
                "period": period,
                "first_scheduled": _primitive(row.get("首次预约")),
                "last_changed": next(
                    (_primitive(row.get(key)) for key in ("三次变更", "二次变更", "初次变更")
                     if _primitive(row.get(key)) is not None),
                    None,
                ),
                "actual": _primitive(row.get("实际披露")),
                "source": "stock_report_disclosure",
            })
    for ref in universe.stocks:
        events = list(scheduled.get(ref.ak_symbol, []))
        data = evidence.load_fundamentals(ref) or {}
        for report in data.get("reports", []):
            if report.get("published_at"):
                events.append({
                    "event": "results_publication",
                    "report_date": report.get("report_date") or report.get("date"),
                    "published_at": report.get("published_at"),
                    "available_on": report.get("available_on"),
                })
        announcement = paths.CACHE_DIR / "announcements" / f"{ref.ak_symbol}.json"
        try:
            records = json.load(open(announcement, encoding="utf-8")).get("announcements", [])
        except Exception:  # noqa: BLE001
            records = []
        for record in records:
            title = str(record.get("title", "")).upper()
            if any(term in title for term in (
                "BOARD MEETING", "董事会会议", "董事會會議",
                "业绩公告", "業績公告", "ANNUAL RESULTS", "INTERIM RESULTS",
            )):
                events.append({
                    "event": "announcement_evidence",
                    "published_at": record.get("publish_datetime") or record.get("publish_date"),
                    "available_on": (
                        (
                            pit_availability.announcement_availability(
                                record, ref.market
                            )
                            or {}
                        ).get("available_on")
                    ),
                    "title": record.get("title"),
                    "url": record.get("url"),
                })
        with open(directory / f"{ref.ak_symbol}.json", "w", encoding="utf-8") as f:
            json.dump({"symbol": ref.symbol, "events": events},
                      f, ensure_ascii=False, indent=2)
    _guard_codes(directory, allowed, "earnings calendar")
    return {"files": len(universe.stocks)}


def _industry_benchmark_records(
    universe: Universe,
    symbols: list[str],
    *,
    pit_by_event: bool = False,
    return_daily: bool = False,
) -> list[dict]:
    """Build a CNY equal-weight peer index without raw-action jumps."""
    refs = {stock.symbol: stock for stock in universe.stocks}
    closes = {}
    for symbol in symbols:
        path = (
            paths.STATE_DIR / "history"
            / f"{symbol.replace('.', '_')}_raw.parquet"
        )
        if symbol not in refs or not path.exists():
            continue
        frame = pd.read_parquet(path)
        if "close" not in frame or frame.empty:
            continue
        closes[symbol] = frame["close"].sort_index().rename(symbol)
    if not closes:
        return []

    local_prices = pd.concat(closes.values(), axis=1).sort_index()
    fx_blob = {}
    try:
        fx_blob = json.load(open(
            paths.CACHE_DIR / "fx" / "HKDCNY.json",
            encoding="utf-8",
        ))
    except Exception:  # noqa: BLE001
        pass
    fx_by_available_on = {}
    for record in fx_blob.get("records", []):
        available_on = pd.to_datetime(
            record.get("available_on"), errors="coerce"
        )
        try:
            rate = float(record.get("hkd_cny"))
        except (TypeError, ValueError):
            continue
        if pd.notna(available_on) and rate > 0:
            fx_by_available_on[available_on] = rate
    fx = pd.Series(list(fx_by_available_on.values()), index=pd.DatetimeIndex(list(fx_by_available_on)), dtype=float).sort_index()
    fx = fx.reindex(local_prices.index, method="ffill")

    returns = {}
    active = {}
    cutoff = local_prices.index[-1].date()
    for symbol, original in closes.items():
        ref = refs[symbol]
        first = original.first_valid_index()
        last = original.last_valid_index()
        is_active = (
            (local_prices.index >= first)
            & (local_prices.index <= last)
        )
        value = original.reindex(local_prices.index).ffill()
        if ref.market == Market.HK:
            value = value * fx
        series_return = value.pct_change(fill_method=None)
        series_return.loc[~is_active] = float("nan")
        action_dates = evidence.corporate_action_dates(ref, cutoff)
        if pit_by_event:
            action_dates = [day for day in action_dates
                            if day in evidence.corporate_action_dates(ref, day, respect_record_availability=True)]
        action_index = pd.to_datetime(action_dates).intersection(
            series_return.index
        )
        if pit_by_event:
            action_index = action_index[(action_index >= first) & (action_index <= last)]
        series_return.loc[action_index] = 0.0
        returns[symbol] = series_return
        active[symbol] = pd.Series(is_active, index=local_prices.index)

    return_panel = pd.DataFrame(returns)
    active_panel = pd.DataFrame(active)
    equal_return = return_panel.mean(axis=1, skipna=True).fillna(0.0)
    if return_daily:
        return [{"date": observed.date().isoformat(), "return": float(value),
                 "constituent_count": int(active_panel.loc[observed].sum()),
                 "invalid_members": [symbol for symbol in symbols if symbol in returns
                     and active_panel.loc[observed, symbol] and pd.isna(return_panel.loc[observed, symbol])
                     and observed != closes[symbol].first_valid_index()],
                 "weights": {symbol: (1.0 / int(return_panel.loc[observed].notna().sum())
                             if pd.notna(return_panel.loc[observed, symbol]) else 0.0)
                             for symbol in returns}}
                for observed, value in equal_return.items()]
    level = (1.0 + equal_return).cumprod() * 1000.0
    return [
        {
            "date": observed.date().isoformat(),
            "equal_weight_level": float(value),
            "constituent_count": int(active_panel.loc[observed].sum()),
        }
        for observed, value in level.items()
    ]


def collect_benchmarks(universe: Universe) -> dict:
    """Cache broad-market indices and derived equal-weight industry benchmarks."""
    directory = _dir("benchmarks")
    indices = {
        "CSI300": "sh000300", "SSE": "sh000001", "SZSE": "sz399001",
        "HSI": "hkHSI", "HSCEI": "hkHSCEI", "HSTECH": "hkHSTECH",
    }
    out = {}
    for name, code in indices.items():
        try:
            frame = tencent_kline.fetch_daily_range(
                code,
                date(2022, 9, 1),
                date.today(),
                adjust="none",
                count=1500,
            )
            records = [
                {"date": idx.date().isoformat(), **{
                    key: _primitive(value) for key, value in row.to_dict().items()
                }}
                for idx, row in frame.iterrows()
            ]
            with open(directory / f"index_{name}.json", "w", encoding="utf-8") as f:
                json.dump({"name": name, "code": code, "source": "tencent_raw",
                           "records": records}, f, ensure_ascii=False, indent=2)
            out[name] = len(records)
        except Exception:  # noqa: BLE001
            out[name] = 0

    # Derived peers are explicitly labelled, never presented as official indices.
    import yaml

    raw = yaml.safe_load(open(paths.CONFIG_DIR / "comparables.yaml", encoding="utf-8"))
    groups = {}
    for item in raw.get("stocks", []):
        groups.setdefault(item.get("group", "unknown"), []).append(item["symbol"])
    for group, symbols in groups.items():
        import os
        controlled_home = os.environ.get("QWL_BENCHMARK_INPUT_HOME")
        if controlled_home:
            from .controlled_benchmarks import build_document
            input_home = Path(controlled_home)
            base_path = input_home / ".cache/benchmarks" / f"industry_{group}.json"
            baseline = json.loads(base_path.read_text(encoding="utf-8")) if base_path.exists() else {}
            document = build_document(universe, symbols, baseline, os.environ["QWL_BENCHMARK_AS_OF"], input_home)
            with (directory / f"industry_{group}.json").open("w", encoding="utf-8") as handle:
                json.dump(document, handle, ensure_ascii=False, indent=2)
            out[f"industry_{group}"] = len(document["records"])
            continue
        records = _industry_benchmark_records(universe, symbols)
        if not records:
            continue
        with open(directory / f"industry_{group}.json", "w", encoding="utf-8") as f:
            json.dump({
                "group": group,
                "source": "derived_selected_peer_equal_weight_cny",
                "symbols": symbols,
                "return_currency": "CNY",
                "corporate_action_policy": "neutral_on_effective_date",
                "closed_market_policy": "carry_forward_valuation",
                "records": records,
            }, f, ensure_ascii=False, indent=2)
    return out


def _parse_nbs_release_schedule(
    year: int,
    html: str,
    source_url: str,
) -> dict[tuple[str, str], dict]:
    """Parse CPI/PMI release timestamps from an official NBS annual schedule."""
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:  # noqa: BLE001
        return {}
    frame = next(
        (table for table in tables if table.shape[1] >= 14),
        None,
    )
    if frame is None:
        return {}
    out = {}
    for series, title in (
        ("cpi", "消费价格"),
        ("pmi", "采购经理"),
    ):
        rows = frame[
            frame.iloc[:, 1].astype(str).str.contains(title, na=False)
        ]
        if rows.empty:
            continue
        for month in range(1, 13):
            cell_values = [
                str(value)
                for value in rows.iloc[:, month + 1].tolist()
                if str(value) not in {"nan", "……", "..."}
            ]
            if not cell_values:
                continue
            date_text = " ".join(cell_values)
            days = [
                int(value)
                for value in re.findall(
                    r"(\d{1,2})/[一二三四五六日天]",
                    date_text,
                )
            ]
            times = re.findall(r"\b(\d{1,2}:\d{2})\b", date_text)
            for day_index, day in enumerate(days):
                try:
                    published = date(year, month, day)
                except ValueError:
                    continue
                if series == "cpi" or day <= 7:
                    period_end = published.replace(day=1) - timedelta(days=1)
                    period = period_end.strftime("%Y-%m")
                else:
                    period = published.strftime("%Y-%m")
                clock = (
                    times[min(day_index, len(times) - 1)]
                    if times else None
                )
                raw = (
                    f"{published.isoformat()} {clock}"
                    if clock else published.isoformat()
                )
                resolved = pit_availability.publication_availability(
                    raw,
                    Market.A,
                    force_date_only=clock is None,
                )
                if resolved:
                    out[(series, period)] = {
                        **resolved,
                        "availability_basis": (
                            "official_nbs_annual_release_schedule"
                        ),
                        "publication_source_url": source_url,
                    }
    return out


def _fetch_nbs_release_calendar() -> dict[tuple[str, str], dict]:
    """Fetch official annual release schedules for the macro cache period."""
    from bs4 import BeautifulSoup
    from curl_cffi import requests as browser_requests

    archive = "https://www.stats.gov.cn/sj/fbrc/ljxxfb/"
    links = {
        date.today().year: "https://www.stats.gov.cn/sj/fbrc/bnxxfb/"
    }
    for page in (archive, urljoin(archive, "index_1.html")):
        try:
            response = browser_requests.get(
                page, impersonate="chrome120", timeout=30
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            continue
        soup = BeautifulSoup(response.text, "lxml")
        for anchor in soup.find_all("a", href=True):
            title = anchor.get_text(" ", strip=True)
            match = re.search(r"(20\d{2})年.*发布日程", title)
            if match:
                links[int(match.group(1))] = urljoin(
                    page, anchor["href"]
                )

    calendar = {}
    for year, url in sorted(links.items()):
        if year < 2008:
            continue
        try:
            response = browser_requests.get(
                url, impersonate="chrome120", timeout=30
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            continue
        calendar.update(
            _parse_nbs_release_schedule(year, response.text, url)
        )
    return calendar


def _macro_record_identity(series: str, record: dict) -> str | None:
    if series in {"cpi", "pmi"}:
        match = re.match(
            r"(\d{4})年(\d{2})月份",
            str(record.get("月份") or ""),
        )
        return f"{match.group(1)}-{match.group(2)}" if match else None
    for key in ("TRADE_DATE", "日期"):
        if record.get(key):
            parsed = pd.to_datetime(record[key], errors="coerce")
            return parsed.date().isoformat() if pd.notna(parsed) else None
    return None


def _macro_pit_fields(
    series: str,
    record: dict,
    release_calendar: dict[tuple[str, str], dict],
    first_seen_at: str,
) -> dict:
    identity = _macro_record_identity(series, record)
    if identity is None:
        return {}
    scheduled = release_calendar.get((series, identity))
    if scheduled:
        return scheduled
    if series == "shibor":
        resolved = pit_availability.publication_availability(
            f"{identity} 11:00:00",
            Market.A,
        )
        return {
            **(resolved or {}),
            "availability_basis": "official_daily_fixing_time",
            "publication_source_url": "https://www.shibor.org/",
        }
    if series in {"lpr", "commodity_price_index"}:
        resolved = pit_availability.publication_availability(
            identity,
            Market.A,
            force_date_only=True,
        )
        return {
            **(resolved or {}),
            "availability_basis": (
                "official_rate_effective_date"
                if series == "lpr"
                else "provider_observation_date_conservative_next_session"
            ),
        }
    observed = pd.to_datetime(first_seen_at, errors="coerce")
    if pd.isna(observed):
        observed = pd.Timestamp(date.today())
    resolved = pit_availability.publication_availability(
        observed.date().isoformat(),
        Market.A,
        force_date_only=True,
    )
    return {
        "source_published_at": None,
        "source_published_on": None,
        "timestamp_precision": "first_seen",
        "available_on": resolved.get("available_on") if resolved else None,
        "availability_basis": "local_first_seen_conservative",
        "first_seen_at": observed.isoformat(),
    }


def collect_macro() -> dict:
    """Cache the macro/rate/commodity series used by the weekly regime layer."""
    import akshare as ak

    directory = _dir("macro")
    calls = {
        "lpr": ak.macro_china_lpr,
        "shibor": ak.macro_china_shibor_all,
        "cpi": ak.macro_china_cpi,
        "pmi": ak.macro_china_pmi,
        "commodity_price_index": ak.macro_china_commodity_price_index,
    }
    release_calendar = _fetch_nbs_release_calendar()
    summary = {}
    for name, call in calls.items():
        path = directory / f"{name}.json"
        existing = {}
        if path.exists():
            try:
                existing = json.load(open(path, encoding="utf-8"))
            except Exception:  # noqa: BLE001
                existing = {}
        first_seen_at = (
            existing.get("first_seen_at")
            or datetime.fromtimestamp(
                path.stat().st_mtime
            ).isoformat()
            if path.exists() else datetime.now().isoformat()
        )
        prior_by_identity = {
            _macro_record_identity(name, record): record
            for record in existing.get("records", [])
        }
        try:
            records = _records(call())
            status, error = "cached", None
        except Exception as exc:  # noqa: BLE001
            records = []
            status, error = "failed", f"{type(exc).__name__}: {str(exc)[:100]}"
        enriched = []
        for record in records:
            identity = _macro_record_identity(name, record)
            prior = prior_by_identity.get(identity, {})
            pit = {
                key: prior.get(key)
                for key in (
                    "source_published_at", "source_published_on",
                    "timestamp_precision", "available_on",
                    "availability_basis", "publication_source_url",
                    "first_seen_at",
                )
                if prior.get(key) is not None
            }
            if not pit.get("available_on"):
                pit = _macro_pit_fields(
                    name, record, release_calendar, first_seen_at
                )
            enriched.append({**record, **pit})
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "series": name,
                "first_seen_at": first_seen_at,
                "pit_policy_version": 1,
                "records": enriched,
                "error": error,
            }, f, ensure_ascii=False, indent=2)
        summary[name] = {
            "status": status,
            "rows": len(enriched),
            "pit_rows": sum(
                bool(record.get("available_on")) for record in enriched
            ),
            "official_schedule_rows": sum(
                record.get("availability_basis")
                == "official_nbs_annual_release_schedule"
                for record in enriched
            ),
        }
    return summary


def _parse_hkex_short_report(text: str, allowed: set[str]) -> tuple[str | None, dict]:
    """Parse an HKEX full-day short-selling report after in-memory filtering."""
    plain = re.sub(r"<[^>]+>", " ", text)
    match = re.search(
        r"(?:TRADING DATE|日期)\s*:\s*(\d{1,2}\s+[A-Z]{3}\s+\d{4})",
        plain,
    )
    trading_date = None
    if match:
        try:
            trading_date = datetime.strptime(match.group(1), "%d %b %Y").date().isoformat()
        except ValueError:
            pass
    rows = {}
    for line in plain.splitlines():
        match = re.match(
            r"\s*(\d{1,5})\s+(.+?)\s+([\d,]+)\s+([\d,]+)\s*$",
            line,
        )
        if not match:
            continue
        code = match.group(1).zfill(5)
        if code not in allowed:
            continue
        rows[code] = {
            "date": trading_date,
            "name": match.group(2).strip(),
            "short_volume": int(match.group(3).replace(",", "")),
            "short_turnover": int(match.group(4).replace(",", "")),
        }
    return trading_date, rows


def _parse_sfc_short_csv(text: str, allowed: set[str]) -> dict[str, list[dict]]:
    """Parse one official SFC weekly file, retaining only selected HK codes."""
    frame = pd.read_csv(StringIO(text), dtype={"Stock Code": str})
    required = {
        "Date",
        "Stock Code",
        "Stock Name",
        "Aggregated Reportable Short Positions (Shares)",
        "Aggregated Reportable Short Positions (HK$)",
    }
    if not required.issubset(frame.columns):
        raise ValueError("unexpected SFC short-position CSV schema")
    frame["Stock Code"] = (
        frame["Stock Code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(5)
    )
    frame = frame[frame["Stock Code"].isin(allowed)]
    rows: dict[str, list[dict]] = {code: [] for code in allowed}
    for _, row in frame.iterrows():
        observed = pd.to_datetime(row["Date"], dayfirst=True, errors="coerce")
        if pd.isna(observed):
            continue
        code = str(row["Stock Code"])
        rows[code].append({
            "date": observed.date().isoformat(),
            "name": _primitive(row["Stock Name"]),
            "reportable_short_shares": _primitive(
                row["Aggregated Reportable Short Positions (Shares)"]
            ),
            "reportable_short_value_hkd": _primitive(
                row["Aggregated Reportable Short Positions (HK$)"]
            ),
        })
    return rows


def _sfc_short_csv_links(page_text: str, start_date: str) -> list[str]:
    """Extract official weekly CSV URLs without persisting the full-market page."""
    links = re.findall(
        r'href=["\']([^"\']*Short_Position_Reporting_Aggregated_Data_'
        r'(\d{8})\.csv[^"\']*)["\']',
        page_text,
        flags=re.IGNORECASE,
    )
    result = set()
    for raw_url, ymd in links:
        if ymd < start_date:
            continue
        # The old hostname currently serves a certificate for www.sfc.hk.
        url = unescape(raw_url).replace("https://www.hksfc.org/", "https://www.sfc.hk/")
        if url.startswith("/"):
            url = "https://www.sfc.hk" + url
        result.add(url)
    return sorted(result)


def _scaled_market_number(value) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if text in {"", "-", "--", "nan"}:
        return None
    scale = 1.0
    if text[-1:].upper() == "B":
        scale, text = 1e9, text[:-1]
    elif text[-1:].upper() == "M":
        scale, text = 1e6, text[:-1]
    elif text[-1:].upper() == "K":
        scale, text = 1e3, text[:-1]
    try:
        return float(text.replace("%", "")) * scale
    except ValueError:
        return None


def _parse_aastocks_short_history(html: str, captured_on: date) -> list[dict]:
    """Parse the rolling 15-session per-stock short-selling table."""
    tables = pd.read_html(StringIO(html))
    table = next(
        (
            frame for frame in tables
            if "Short Selling Ratio (SSR)" in frame.columns
            and "Short Selling Turnover" in frame.columns
        ),
        None,
    )
    if table is None:
        return []
    records = []
    for _, row in table.iterrows():
        try:
            month, day = (int(value) for value in str(row["Date"]).split("/"))
            observed = date(captured_on.year, month, day)
            if observed > captured_on + timedelta(days=7):
                observed = date(captured_on.year - 1, month, day)
        except (TypeError, ValueError):
            continue
        records.append({
            "date": observed.isoformat(),
            "price": _scaled_market_number(row.get("1Price")),
            "short_volume": _scaled_market_number(row.get("Short Selling Volume")),
            "short_turnover": _scaled_market_number(
                row.get("Short Selling Turnover")
            ),
            "total_turnover": _scaled_market_number(row.get("Turnover")),
            "short_ratio": (
                (_scaled_market_number(row.get("Short Selling Ratio (SSR)")) or 0)
                / 100.0
            ),
            "short_ratio_3d": (
                (_scaled_market_number(row.get("SSR 3-day SMA")) or 0) / 100.0
            ),
            "short_ratio_5d": (
                (_scaled_market_number(row.get("SSR 5-day SMA")) or 0) / 100.0
            ),
            "source": "AAStocks rolling short-selling history",
        })
    unique = {record["date"]: record for record in records}
    return sorted(unique.values(), key=lambda item: item["date"])


def _futu_daily_short_histories(codes: list[str]) -> tuple[dict, str]:
    """Optional full daily history when a local Futu OpenD is available."""
    try:
        import futu
    except ImportError:
        return {}, "futu_package_not_installed"
    try:
        context = futu.OpenQuoteContext(host="127.0.0.1", port=11111)
    except Exception as exc:  # noqa: BLE001
        return {}, f"futu_opend_unavailable:{type(exc).__name__}"
    histories = {}
    try:
        for code in codes:
            records = []
            next_key = None
            while next_key != "-1":
                result = context.get_daily_short_volume(
                    f"HK.{code}",
                    next_key=next_key,
                    num=50,
                )
                if not isinstance(result, tuple) or len(result) != 3:
                    break
                ret, _, frame = result
                if ret != futu.RET_OK or frame is None or frame.empty:
                    break
                for _, row in frame.iterrows():
                    records.append({
                        "date": str(row.get("timestamp_str") or "")[:10],
                        "price": _primitive(row.get("close_price")),
                        "short_volume": _primitive(
                            row.get("short_sell_shares_traded")
                        ),
                        "short_turnover": _primitive(
                            row.get("short_sell_turnover")
                        ),
                        "total_turnover": _primitive(row.get("turnover")),
                        "short_ratio": (
                            float(row.get("short_sell_turnover") or 0)
                            / float(row.get("turnover") or 1)
                        ),
                        "source": "Futu OpenD authorized daily short volume",
                    })
                next_key = str(frame.attrs.get("next_key", "-1"))
            histories[code] = sorted(
                {
                    item["date"]: item for item in records if item.get("date")
                }.values(),
                key=lambda item: item["date"],
            )
    finally:
        context.close()
    return histories, "available"


def collect_hk_short_selling(
    universe: Universe,
    start_date: str = "20230101",
    budget_s: float = 600.0,
) -> dict:
    """Cache HKEX daily turnover and SFC weekly short positions since 2023.

    SFC files contain only positions above the statutory reporting threshold.
    A missing code therefore means "not reportable", not a proven zero position.
    """
    from concurrent.futures import ThreadPoolExecutor, wait

    allowed = {s.ak_symbol for s in universe.stocks}
    hk = {s.ak_symbol: s for s in universe.stocks if s.market == Market.HK}
    directory = _dir("hk_short_selling")
    authorized_daily, authorized_status = _futu_daily_short_histories(
        sorted(hk)
    )

    # Current full-day transaction snapshot from HKEX.
    response = requests.get(
        "https://www.hkex.com.hk/chi/stat/smstat/ssturnover/ncms/ashtmain_c.htm",
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()
    response.encoding = "big5"
    trading_date, current_rows = _parse_hkex_short_report(response.text, set(hk))

    # Historical weekly reportable short positions from the SFC.
    index_url = (
        "https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/"
        "Aggregated-reportable-short-positions-of-specified-shares"
    )
    index_response = requests.get(
        index_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30
    )
    index_response.raise_for_status()
    links = _sfc_short_csv_links(index_response.text, start_date)
    historical: dict[str, list[dict]] = {code: [] for code in hk}
    failures = []

    def fetch_week(url: str):
        result = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        result.raise_for_status()
        return _parse_sfc_short_csv(result.text, set(hk))

    executor = ThreadPoolExecutor(max_workers=8)
    futures = {executor.submit(fetch_week, url): url for url in links}
    done, pending = wait(futures, timeout=budget_s)
    for future in done:
        url = futures[future]
        try:
            by_code = future.result()
        except Exception as exc:  # noqa: BLE001
            failures.append({"url": url, "error": f"{type(exc).__name__}: {str(exc)[:100]}"})
            continue
        for code, records in by_code.items():
            historical[code].extend(records)
    for future in pending:
        failures.append({"url": futures[future], "error": "budget_timeout"})
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)

    # A secondary rolling daily source closes the one-day-only gap and is
    # accumulated locally. Official HKEX current-day evidence remains primary.
    def fetch_daily(code: str):
        from curl_cffi import requests as browser_requests

        response = browser_requests.get(
            "https://www.aastocks.com/en/stocks/market/shortselling/"
            "stock-short-selling-ratio.aspx",
            params={"symbol": code},
            impersonate="chrome120",
            timeout=30,
        )
        response.raise_for_status()
        return _parse_aastocks_short_history(response.text, date.today())

    daily_executor = ThreadPoolExecutor(max_workers=6)
    daily_futures = {
        daily_executor.submit(fetch_daily, code): code for code in hk
    }
    daily_done, daily_pending = wait(daily_futures, timeout=min(budget_s, 180.0))
    daily_history = {}
    for future in daily_done:
        code = daily_futures[future]
        try:
            daily_history[code] = future.result()
        except Exception:  # noqa: BLE001
            daily_history[code] = []
    for future in daily_pending:
        future.cancel()
    daily_executor.shutdown(wait=False, cancel_futures=True)

    captured_at = datetime.now().isoformat()
    for code, ref in hk.items():
        records = sorted(historical[code], key=lambda item: item["date"])
        path = directory / f"{code}.json"
        prior_daily = []
        if path.exists():
            try:
                prior_daily = json.load(open(path, encoding="utf-8")).get(
                    "daily_history", []
                )
            except Exception:  # noqa: BLE001
                prior_daily = []
        merged_daily = {
            item["date"]: item
            for item in [
                *prior_daily,
                *daily_history.get(code, []),
                *authorized_daily.get(code, []),
            ]
            if item.get("date")
        }
        with open(directory / f"{code}.json", "w", encoding="utf-8") as f:
            json.dump({
                "symbol": ref.symbol,
                "captured_at": captured_at,
                "source": {
                    "daily_turnover": "HKEX official full-day short-selling report",
                    "weekly_positions": "SFC aggregated reportable short positions",
                },
                "data_shape": "current_daily_turnover_plus_weekly_reportable_positions",
                "record": current_rows.get(code),
                "daily_history": sorted(
                    merged_daily.values(), key=lambda item: item["date"]
                ),
                "daily_history_semantics": (
                    "AAStocks rolling 15-session secondary history accumulated "
                    "locally; HKEX current-day report remains primary evidence"
                ),
                "daily_public_window": "rolling_15_sessions",
                "historical_daily_backfill_status": authorized_status,
                "historical_daily_backfill_options": [
                    "Futu OpenD get_daily_short_volume",
                    "HKEX Data Marketplace licensed historical files",
                ],
                "authorized_daily_records": len(
                    authorized_daily.get(code, [])
                ),
                "historical_records": records,
                "history_window": [
                    records[0]["date"] if records else None,
                    records[-1]["date"] if records else None,
                ],
                "history_files_discovered": len(links),
                "history_files_scanned": len(done),
                "history_files_failed": len(failures),
                "absence_semantics": (
                    "Missing weekly observations mean no reportable position in the "
                    "SFC aggregate file, not a proven zero short position."
                ),
            }, f, ensure_ascii=False, indent=2)
    _guard_codes(directory, allowed, "HK short selling")
    return {
        "files": len(hk),
        "trading_date": trading_date,
        "symbols_with_daily_short_sales": len(current_rows),
        "history_files_discovered": len(links),
        "history_files_scanned": len(done),
        "history_files_failed": len(failures),
        "symbols_with_reportable_history": sum(bool(rows) for rows in historical.values()),
        "symbols_with_daily_history": sum(
            bool(rows) for rows in daily_history.values()
        ),
        "failures": failures,
    }


def _run_parallel(items, worker, *, budget_s: float, max_workers: int) -> dict:
    """Bounded, resumable runner for request-only providers (no mini_racer)."""
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures = {executor.submit(worker, item): item for item in items}
    pending = set(futures)
    deadline = time.time() + budget_s
    out = {}
    while pending and time.time() < deadline:
        done, pending = wait(
            pending,
            timeout=max(0.2, deadline - time.time()),
            return_when=FIRST_COMPLETED,
        )
        for future in done:
            item = futures[future]
            symbol = getattr(item, "symbol", str(item))
            try:
                key, status = future.result()
                out[key] = status
            except Exception:  # noqa: BLE001
                out[symbol] = "failed"
    for future in pending:
        item = futures[future]
        out[getattr(item, "symbol", str(item))] = "timeout"
        future.cancel()
    executor.shutdown(wait=False, cancel_futures=True)
    return out


def _apply_matched_availability(
    record: dict,
    resolved: dict,
    *,
    basis: str,
    evidence_url: str | None = None,
    evidence_title: str | None = None,
) -> dict:
    return {
        **record,
        **resolved,
        "availability_status": "available",
        "availability_basis": basis,
        "availability_evidence_url": evidence_url,
        "availability_evidence_title": evidence_title,
    }


def _record_date(record: dict, keys: tuple[str, ...]) -> str | None:
    value = next(
        (record.get(key) for key in keys if record.get(key) not in (None, "")),
        None,
    )
    parsed = pd.to_datetime(value, errors="coerce")
    return parsed.date().isoformat() if pd.notna(parsed) else None


def _invalidate_impossible_publication(
    records: list[dict],
    observation_keys: tuple[str, ...],
) -> list[dict]:
    """Reject provider dates that claim publication before the observation.

    The raw provider fields remain untouched for audit. Only derived PIT fields
    are cleared so a later official match or the conservative first-seen
    fallback determines availability.
    """
    out = []
    for record in records:
        observed_raw = _record_date(record, observation_keys)
        published_raw = _record_date(record, ("source_published_on",))
        if (
            observed_raw is not None
            and published_raw is not None
            and published_raw < observed_raw
        ):
            out.append({
                **record,
                "source_published_at": None,
                "source_published_on": None,
                "timestamp_precision": "unknown",
                "available_on": None,
                "availability_status": "publication_date_inconsistent",
                "availability_basis": "provider_publication_precedes_observation",
                "source_publication_issue": (
                    f"provider publication {published_raw} precedes "
                    f"observation {observed_raw}"
                ),
            })
        else:
            out.append(record)
    return out


def _first_seen_fallback(
    record: dict,
    market: Market,
    first_seen_at: str,
) -> dict:
    if record.get("available_on"):
        return record
    observed = pd.to_datetime(first_seen_at, errors="coerce")
    if pd.isna(observed):
        observed = pd.Timestamp(date.today())
    resolved = pit_availability.publication_availability(
        observed.date().isoformat(),
        market,
        force_date_only=True,
    )
    return {
        **record,
        "source_published_at": None,
        "source_published_on": None,
        "timestamp_precision": "first_seen",
        "available_on": resolved.get("available_on") if resolved else None,
        "availability_status": (
            "scheduled_available_from_first_seen"
            if record.get("availability_status") == "scheduled_not_yet_official"
            else "available_from_first_seen"
        ),
        "availability_basis": "local_first_seen_conservative",
        "source_publication_status": "unavailable",
        "first_seen_at": observed.isoformat(),
    }


def _match_a_ownership_availability(blob: dict, ref) -> None:
    """Bind report-derived ownership rows and unlock events to official filings."""
    report_publications = evidence._a_publication_map(ref)
    datasets = blob.get("datasets", {})
    report_keys = {
        "holder_counts": ("股东户数统计截止日",),
        "top10_holders": ("截至日期",),
        "top10_float_holders": ("截止日期",),
        "pledges": ("报告期",),
    }
    for dataset, keys in report_keys.items():
        rows = []
        for record in datasets.get(dataset, []):
            if record.get("available_on"):
                rows.append(record)
                continue
            report_date = _record_date(record, keys)
            resolved = report_publications.get(report_date)
            rows.append(
                _apply_matched_availability(
                    record,
                    resolved,
                    basis="matched_financial_report_publication",
                )
                if resolved else record
            )
        datasets[dataset] = rows

    announcements = evidence._announcements(ref)
    candidates = []
    for item in announcements:
        title = str(item.get("title") or "")
        if not any(term in title for term in (
            "限售股上市流通", "限售股份上市流通", "解除限售上市流通",
            "解禁上市流通", "限售股解禁",
        )):
            continue
        published = evidence._announcement_date(item)
        resolved = evidence._announcement_availability(item, Market.A)
        if published and resolved:
            candidates.append((published, resolved, item))

    unlock_rows = []
    today = date.today()
    for record in datasets.get("lockup_releases", []):
        if record.get("available_on"):
            unlock_rows.append(record)
            continue
        effective_raw = _record_date(record, ("解禁时间",))
        effective = (
            date.fromisoformat(effective_raw) if effective_raw else None
        )
        matches = [
            item for item in candidates
            if effective is not None
            and item[0] <= effective
            and (effective - item[0]).days <= 180
        ]
        if matches:
            published, resolved, item = max(matches, key=lambda value: value[0])
            unlock_rows.append(_apply_matched_availability(
                record,
                resolved,
                basis="matched_official_unlock_announcement",
                evidence_url=item.get("url"),
                evidence_title=item.get("title"),
            ))
        elif effective and effective > today:
            unlock_rows.append({
                **record,
                "availability_status": "scheduled_not_yet_official",
                "availability_basis": "provider_schedule_without_official_filing",
            })
        else:
            unlock_rows.append(record)
    datasets["lockup_releases"] = unlock_rows
    first_seen_at = blob.get("captured_at") or date.today().isoformat()
    for dataset, records in datasets.items():
        datasets[dataset] = [
            _first_seen_fallback(record, Market.A, first_seen_at)
            for record in records
        ]


def _normalized_holder_name(value) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _numeric_tokens(value) -> list[float]:
    return [
        float(token.replace(",", ""))
        for token in re.findall(r"-?[\d,]+(?:\.\d+)?", str(value or ""))
        if token not in {"", "-"}
    ]


def _hk_change_metric_match(record: dict, candidate: dict) -> bool:
    change_values = _numeric_tokens(record.get("持股变动(万股)"))
    change_shares = (
        abs(change_values[0]) * 10000 if change_values else None
    )
    percentage_values = _numeric_tokens(record.get("变动后占比(%)"))
    percentage = percentage_values[0] if percentage_values else None
    disclosed_shares = _numeric_tokens(candidate.get("shares_involved"))
    disclosed_percentages = _numeric_tokens(
        candidate.get("present_interest_pct")
    )
    shares_match = (
        change_shares is not None
        and any(
            abs(value - change_shares)
            <= max(10.0, abs(change_shares) * 0.02)
            for value in disclosed_shares
        )
    )
    percentage_match = (
        percentage is not None
        and any(
            abs(value - percentage) <= 0.02
            for value in disclosed_percentages
        )
    )
    return shares_match or percentage_match


def _match_hk_ownership_availability(blob: dict, ref) -> None:
    """Bind periodic snapshots to results filings and changes to official DI forms."""
    report_dates = [
        value
        for record in blob.get("major_holder_snapshots", [])
        if (value := _record_date(record, ("report_date",)))
    ]
    reports = evidence._hk_publication_map(ref, report_dates)
    snapshots = []
    for record in blob.get("major_holder_snapshots", []):
        if record.get("available_on"):
            snapshots.append(record)
            continue
        report_date = _record_date(record, ("report_date",))
        publication = reports.get(report_date)
        if publication:
            resolved = {
                key: publication.get(key)
                for key in (
                    "source_published_at", "source_published_on",
                    "timestamp_precision", "available_on",
                )
            }
            snapshots.append(_apply_matched_availability(
                record,
                resolved,
                basis="matched_official_results_publication",
            ))
        else:
            snapshots.append(record)
    blob["major_holder_snapshots"] = snapshots

    disclosure_path = (
        paths.CACHE_DIR / "hk_market_disclosures" / f"{ref.ak_symbol}.json"
    )
    disclosure_blob = {}
    try:
        disclosure_blob = json.load(open(disclosure_path, encoding="utf-8"))
    except Exception:  # noqa: BLE001
        pass
    by_event_date: dict[str, list[dict]] = {}
    for item in disclosure_blob.get("official_di", {}).get("records", []):
        if not re.fullmatch(
            r"(?:DA|CS)\d{8}E\d+",
            str(item.get("form_serial") or ""),
        ):
            continue
        event_date = pd.to_datetime(
            item.get("relevant_event_date"), dayfirst=True, errors="coerce"
        )
        if pd.notna(event_date) and item.get("available_on"):
            by_event_date.setdefault(
                event_date.date().isoformat(), []
            ).append(item)

    changes = []
    for record in blob.get("holding_changes", []):
        if record.get("available_on"):
            changes.append(record)
            continue
        event_date = _record_date(record, ("日期",))
        candidates = by_event_date.get(event_date or "", [])
        holder = _normalized_holder_name(record.get("股东名称"))
        named = [
            item for item in candidates
            if holder
            and (
                holder in _normalized_holder_name(item.get("holder_or_director"))
                or _normalized_holder_name(item.get("holder_or_director")) in holder
            )
        ]
        metric_matches = [
            item for item in candidates
            if _hk_change_metric_match(record, item)
        ]
        matches = (
            named
            or metric_matches
            or (candidates if len(candidates) == 1 else [])
        )
        if matches:
            match = min(matches, key=lambda item: item["available_on"])
            resolved = {
                key: match.get(key)
                for key in (
                    "source_published_at", "source_published_on",
                    "timestamp_precision", "available_on",
                )
            }
            changes.append(_apply_matched_availability(
                record,
                resolved,
                basis=(
                    "matched_hkex_di_event_name"
                    if named else
                    "matched_hkex_di_event_metrics"
                    if metric_matches else
                    "matched_hkex_di_unique_event"
                ),
                evidence_url=match.get("form_url"),
                evidence_title=match.get("form_serial"),
            ))
        else:
            changes.append(record)
    blob["holding_changes"] = changes
    first_seen_at = blob.get("captured_at") or date.today().isoformat()
    blob.setdefault("captured_at", first_seen_at)
    for key in ("major_holder_snapshots", "holding_changes"):
        blob[key] = [
            _first_seen_fallback(record, Market.HK, first_seen_at)
            for record in blob.get(key, [])
        ]


def backfill_a_unlock_announcements(
    universe: Universe,
    start_date: str = "20000101",
    end_date: str = "20221231",
    budget_s: float = 300.0,
) -> dict:
    """Backfill only official unlock-category filings for the selected A shares."""
    import akshare as ak

    allowed = {stock.ak_symbol for stock in universe.stocks}
    stocks = [stock for stock in universe.stocks if stock.market == Market.A]
    directory = _dir("announcements")

    def worker(ref):
        try:
            frame = ak.stock_zh_a_disclosure_report_cninfo(
                symbol=ref.ak_symbol,
                market="沪深京",
                category="解禁",
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:  # noqa: BLE001
            return ref.symbol, f"failed:{type(exc).__name__}"
        records = []
        if frame is not None and not frame.empty:
            for _, row in frame.iterrows():
                if str(row.get("代码", "")).zfill(6) != ref.ak_symbol:
                    continue
                record = {
                    "ak_symbol": ref.ak_symbol,
                    "symbol": ref.symbol,
                    "title": str(row.get("公告标题", "")),
                    "category": "CNINFO:解禁",
                    "publish_datetime": str(row.get("公告时间", "")),
                    "publish_date": str(row.get("公告时间", ""))[:10],
                    "url": str(row.get("公告链接", "")),
                    "source": "CNINFO",
                }
                resolved = pit_availability.announcement_availability(
                    record, Market.A
                )
                records.append({
                    **record,
                    **(resolved or {}),
                })
        path = directory / f"{ref.ak_symbol}.json"
        existing = {}
        try:
            existing = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        merged = {}
        for record in [*existing.get("announcements", []), *records]:
            identity = (
                record.get("url")
                or (
                    record.get("title"),
                    record.get("publish_datetime") or record.get("publish_date"),
                )
            )
            merged[str(identity)] = record
        values = sorted(
            merged.values(),
            key=lambda item: (
                item.get("source_published_on")
                or item.get("publish_date")
                or item.get("publish_datetime")
                or "",
                item.get("title") or "",
            ),
        )
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({
                **existing,
                "symbol": ref.symbol,
                "announcements": values,
            }, handle, ensure_ascii=False, indent=2)
        return ref.symbol, f"cached:{len(records)}"

    result = _run_parallel(stocks, worker, budget_s=budget_s, max_workers=6)
    _guard_codes(directory, allowed, "A unlock announcements")
    return result


def backfill_hk_results_announcements(
    universe: Universe,
    start_date: str = "20000101",
    end_date: str = "20201231",
    budget_s: float = 300.0,
) -> dict:
    """Backfill results-title filings for only the selected Hong Kong stocks."""
    from . import extended

    stocks = [stock for stock in universe.stocks if stock.market == Market.HK]
    allowed = {stock.ak_symbol for stock in universe.stocks}
    directory = _dir("announcements")
    base = "https://www1.hkexnews.hk"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml",
    }

    def worker(ref):
        session = requests.Session()
        try:
            lookup = session.get(
                base + "/search/prefix.do",
                params={
                    "callback": "c", "lang": "EN", "type": "A",
                    "name": ref.ak_symbol,
                },
                headers=headers,
                timeout=30,
            )
            text = lookup.text
            payload = json.loads(text[text.index("(") + 1:text.rindex(")")])
            match = next(
                item for item in payload.get("stockInfo", [])
                if str(item.get("code", "")).zfill(5) == ref.ak_symbol
            )
            response = session.get(
                base + "/search/titleSearchServlet.do",
                params={
                    "sortDir": "0", "sortByOptions": "DateTime",
                    "category": "0", "market": "SEHK",
                    "stockId": str(match["stockId"]), "documentType": "-1",
                    "fromDate": start_date, "toDate": end_date,
                    "title": "RESULTS", "searchType": "1",
                    "t": "2", "lang": "EN", "rowRange": "500",
                },
                headers=headers,
                timeout=45,
            )
            response.raise_for_status()
            result = response.json().get("result")
            rows = json.loads(result) if isinstance(result, str) else (result or [])
        except Exception as exc:  # noqa: BLE001
            return ref.symbol, f"failed:{type(exc).__name__}"
        records = []
        for row in rows:
            if not extended._hk_row_has_code(row, ref.ak_symbol):
                continue
            record = {
                "ak_symbol": ref.ak_symbol,
                "symbol": ref.symbol,
                "title": str(row.get("TITLE", "") or row.get("SHORT_TEXT", "")),
                "category": str(row.get("CATEGORY_NAME", "HKEX Results search")),
                "publish_datetime": str(row.get("DATE_TIME", "")),
                "publish_date": str(row.get("DATE_TIME", ""))[:10],
                "url": base + str(row.get("FILE_LINK", "")),
                "source": "HKEXnews",
            }
            resolved = pit_availability.announcement_availability(
                record, Market.HK
            )
            records.append({**record, **(resolved or {})})
        path = directory / f"{ref.ak_symbol}.json"
        existing = {}
        try:
            existing = json.load(open(path, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            pass
        merged = {}
        for record in [*existing.get("announcements", []), *records]:
            identity = (
                record.get("url")
                or (
                    record.get("title"),
                    record.get("publish_datetime") or record.get("publish_date"),
                )
            )
            merged[str(identity)] = record
        with open(path, "w", encoding="utf-8") as handle:
            json.dump({
                **existing,
                "symbol": ref.symbol,
                "announcements": sorted(
                    merged.values(),
                    key=lambda item: (
                        item.get("source_published_on")
                        or item.get("publish_datetime")
                        or "",
                        item.get("title") or "",
                    ),
                ),
            }, handle, ensure_ascii=False, indent=2)
        return ref.symbol, f"cached:{len(records)}"

    result = _run_parallel(stocks, worker, budget_s=budget_s, max_workers=4)
    _guard_codes(directory, allowed, "HK results announcements")
    return result


def normalize_p2_availability(universe: Universe) -> dict:
    """Add conservative record-level PIT metadata to existing fixed-60 assets."""
    allowed = {stock.ak_symbol for stock in universe.stocks}
    updated = Counter()

    def rewrite(ref, directory: str, transform) -> None:
        path = paths.CACHE_DIR / directory / f"{ref.ak_symbol}.json"
        if not path.exists():
            return
        blob = json.load(open(path, encoding="utf-8"))
        transform(blob, ref)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(blob, handle, ensure_ascii=False, indent=2)
        updated[directory] += 1

    for ref in universe.stocks:
        if ref.market == Market.A:
            def ownership_a(blob, _ref):
                datasets = blob.get("datasets", {})
                specs = {
                    "holder_counts": ("股东户数公告日期",),
                    "top10_holders": ("公告日期",),
                    "top10_float_holders": ("公告日期",),
                    "pledges": ("公告日期",),
                    "holder_changes": ("公告日期",),
                }
                for key, date_keys in specs.items():
                    datasets[key] = _enrich_records(
                        _dedupe_records(datasets.get(key, [])),
                        Market.A,
                        date_keys,
                    )
                for key, observation_keys in {
                    "holder_counts": ("股东户数统计截止日",),
                    "top10_holders": ("截至日期",),
                    "top10_float_holders": ("截止日期",),
                }.items():
                    datasets[key] = _invalidate_impossible_publication(
                        datasets.get(key, []),
                        observation_keys,
                    )
                datasets["lockup_releases"] = _enrich_records(
                    _dedupe_records(datasets.get("lockup_releases", [])),
                    Market.A,
                    (),
                )
                _match_a_ownership_availability(blob, _ref)

            rewrite(ref, "ownership_a", ownership_a)
            rewrite(
                ref,
                "repurchases_a",
                lambda blob, _ref: blob.update(records=_enrich_records(
                    blob.get("records", []),
                    Market.A,
                    ("最新公告日期",),
                )),
            )
            rewrite(
                ref,
                "block_trades_a",
                lambda blob, _ref: blob.update(records=_enrich_records(
                    blob.get("records", []),
                    Market.A,
                    ("交易日期",),
                )),
            )
            rewrite(
                ref,
                "share_changes",
                lambda blob, _ref: blob.update(records=_enrich_records(
                    blob.get("records", []),
                    Market.A,
                    ("公告日期",),
                )),
            )
            rewrite(
                ref,
                "dividends",
                lambda blob, _ref: blob.update(records=_enrich_records(
                    blob.get("records", []),
                    Market.A,
                    ("最新公告日期", "业绩披露日期", "预案公告日"),
                )),
            )
        else:
            def disclosures(blob, _ref):
                records = blob.get("official_di", {}).get("records", [])
                out = []
                for record in records:
                    match = re.fullmatch(
                        r"(?:DA|CS)(\d{8})E\d+",
                        str(record.get("form_serial") or ""),
                    )
                    event_date = pd.to_datetime(
                        record.get("relevant_event_date"),
                        dayfirst=True,
                        errors="coerce",
                    )
                    if not match or pd.isna(event_date):
                        continue
                    published = datetime.strptime(
                        match.group(1), "%Y%m%d"
                    ).date().isoformat()
                    out.append({
                        **record,
                        **_availability_fields(published, Market.HK),
                        "availability_basis": "hkex_di_form_serial_date",
                    })
                blob.setdefault("official_di", {})["records"] = out

            rewrite(ref, "hk_market_disclosures", disclosures)

            def ownership_hk(blob, _ref):
                for key in ("major_holder_snapshots", "holding_changes"):
                    blob[key] = _enrich_records(
                        _dedupe_records(blob.get(key, [])), Market.HK, ()
                    )
                _match_hk_ownership_availability(blob, _ref)

            rewrite(ref, "ownership_hk", ownership_hk)
            rewrite(
                ref,
                "dividends_hk",
                lambda blob, _ref: blob.update(records=_enrich_records(
                    blob.get("records", []),
                    Market.HK,
                    ("最新公告日期",),
                )),
            )

            def short_selling(blob, _ref):
                if isinstance(blob.get("record"), dict):
                    blob["record"] = _enrich_records(
                        [blob["record"]], Market.HK, ("date",)
                    )[0]
                for key in ("daily_history", "historical_records"):
                    blob[key] = _enrich_records(
                        blob.get(key, []), Market.HK, ("date",)
                    )
                blob["daily_public_window"] = "rolling_15_sessions"
                blob.setdefault(
                    "historical_daily_backfill_status",
                    "authorized_source_required",
                )
                blob["historical_daily_backfill_options"] = [
                    "Futu OpenD get_daily_short_volume",
                    "HKEX Data Marketplace licensed historical files",
                ]

            rewrite(ref, "hk_short_selling", short_selling)

        rewrite(
            ref,
            "news",
            lambda blob, stock: blob.update(records=_enrich_records(
                blob.get("records", []),
                stock.market,
                ("published_at",),
                force_date_only=False,
            )),
        )

    for directory in updated:
        _guard_codes(paths.CACHE_DIR / directory, allowed, directory)
    return {"files_updated": dict(updated), "universe_size": len(allowed)}


def collect_p2(universe: Universe) -> dict:
    """High-level P2 refresh. Long collectors expose their own partial statuses."""
    result = {
        "a_unlock_announcements": backfill_a_unlock_announcements(universe),
        "hk_results_announcements": backfill_hk_results_announcements(universe),
        "ownership_a": collect_a_ownership(universe),
        "ownership_hk": collect_hk_ownership(universe),
        "hk_market_disclosures": collect_hk_market_disclosures(
            universe, start_date="20030101"
        ),
        "market_events_a": collect_a_market_events(universe),
        "consensus": collect_consensus(universe),
        "news": collect_news(universe),
        "earnings_calendar": collect_earnings_calendar(universe),
        "benchmarks": collect_benchmarks(universe),
        "macro": collect_macro(),
        "hk_short_selling": collect_hk_short_selling(universe),
    }
    result["pit_normalization"] = normalize_p2_availability(universe)
    return result
