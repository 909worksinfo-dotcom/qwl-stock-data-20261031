"""AKShare provider — fallback OHLCV + fundamentals/events/flow.

Design notes grounded in live probing (2026):
  - AKShare's eastmoney-backed endpoints are unreliable in-region and have NO
    built-in timeout, so we wrap EVERY call in ``_guarded`` which enforces a
    socket timeout and converts any failure into a ``None`` result (never hang,
    never crash the pipeline — the caller decides if a None is fatal).
  - OHLCV here is a *fallback* behind tencent_kline. A-share fallback uses the
    Tencent-backed ``stock_zh_a_hist_tx`` (probed OK); HK fallback uses
    ``stock_hk_hist`` (eastmoney; may fail, hence tertiary).
  - Fundamentals / events / flow are strictly PER-SYMBOL. The only market-wide
    interfaces (e.g. margin) are fetched once then filtered to the universe by
    the collector — never scanned repeatedly.
"""

from __future__ import annotations

import socket
from datetime import date
from typing import Any, Callable, Optional

import pandas as pd
import requests

# Hard ceiling for any akshare network call. akshare has no timeout param, so we
# set it globally at call time and restore afterwards.
_AK_TIMEOUT = 20
# Best-effort (fundamentals/flow) uses a tighter budget: these ride on the
# unreliable eastmoney endpoints and must not stall the interactive tool.
_AK_BEST_EFFORT_TIMEOUT = 8


def _guarded(fn: Callable[[], Any], timeout: int = _AK_TIMEOUT) -> Optional[Any]:
    """Run an akshare call with a socket timeout; return None on ANY failure."""
    old = socket.getdefaulttimeout()
    socket.setdefaulttimeout(timeout)
    try:
        return fn()
    except Exception:  # noqa: BLE001 — intentional: fallbacks must never raise
        return None
    finally:
        socket.setdefaulttimeout(old)


def _ak():
    import akshare as ak  # imported lazily so base import stays fast

    return ak


# --------------------------------------------------------------------------- #
# OHLCV fallbacks
# --------------------------------------------------------------------------- #
def fetch_a_daily_tx(ak_symbol_prefixed: str, start: str, end: str, adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """A-share daily via Tencent-backed akshare (fallback for tencent_kline).

    Args:
        ak_symbol_prefixed: e.g. "sz002594" / "sh603345".
    Returns normalized OHLCV DataFrame indexed by date, or None.
    """
    if adjust == 'qfq':
        # AKShare's convenience function may silently return day when qfqday
        # is absent. Use the same Tencent feed with the explicit response key
        # contract instead of relabelling an unadjusted response from a request
        from . import tencent_kline
        try:
            result = tencent_kline.fetch_daily_range(ak_symbol_prefixed,
                pd.Timestamp(start).date(),pd.Timestamp(end).date(),adjust='qfq')
        except (requests.RequestException, ValueError):
            return None
        if result.attrs.get('price_basis') != 'qfq' or result.attrs.get('response_key') != 'qfqday':
            return None
        return result
    ak = _ak()
    df = _guarded(
        lambda: ak.stock_zh_a_hist_tx(
            symbol=ak_symbol_prefixed, start_date=start, end_date=end, adjust=adjust
        )
    )
    if df is None or df.empty:
        return None
    # columns: date, open, close, high, low, amount
    df = df.rename(columns={"amount": "volume"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    for c in ("open", "high", "low", "close", "volume"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df]
    result = df[keep]
    result.attrs["price_basis"] = "qfq" if adjust == "qfq" else "none"
    result.attrs["adjustment_request"] = adjust or "none"
    return result


def fetch_hk_daily(ak_symbol: str, start: str, end: str, adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """HK daily via eastmoney-backed akshare (tertiary; may time out in-region).

    Args:
        ak_symbol: 5-digit HK code, e.g. "01810".
    """
    ak = _ak()
    df = _guarded(
        lambda: ak.stock_hk_hist(
            symbol=ak_symbol, period="daily", start_date=start, end_date=end, adjust=adjust
        )
    )
    if df is None or df.empty:
        return None
    # columns (zh): 日期 开盘 收盘 最高 最低 成交量 ...
    rename = {"日期": "date", "开盘": "open", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"}
    df = df.rename(columns=rename)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    for c in ("open", "high", "low", "close", "volume"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = [c for c in ("open", "high", "low", "close", "volume") if c in df]
    result = df[keep]
    result.attrs["price_basis"] = "qfq" if adjust == "qfq" else "none"
    result.attrs["adjustment_request"] = adjust or "none"
    return result


def fetch_hk_daily_sina(ak_symbol: str, start: str, end: str,
                        adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """HK OHLCV from Sina with an explicit adjustment request."""
    ak = _ak()
    df = _guarded(
        lambda: ak.stock_hk_daily(symbol=ak_symbol, adjust=adjust),
        timeout=_AK_TIMEOUT,
    )
    if df is None or df.empty:
        return None
    df = df.copy()
    rename = {"日期":"date","开盘":"open","收盘":"close","最高":"high",
              "最低":"low","成交量":"volume"}
    df = df.rename(columns=rename)
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date").sort_index()
    required = ["open","high","low","close","volume"]
    if any(column not in df.columns for column in required):
        return None
    for column in required:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    result = df[required].loc[
        (df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))
    ]
    if result.empty:
        return None
    result.attrs["price_basis"] = "qfq" if adjust == "qfq" else "none"
    result.attrs["adjustment_request"] = adjust or "none"
    return result


# --------------------------------------------------------------------------- #
# Fundamentals (per-symbol)
# --------------------------------------------------------------------------- #
# We store a PIT-aware TIME SERIES of reports (not just one row) so the feature
# layer can pick the latest report whose date <= as_of, avoiding look-ahead.
def _reports_from_df(df: pd.DataFrame, date_col: str) -> list[dict]:
    """Convert a financial DataFrame into a list of {date, fields} sorted asc."""
    reports = []
    for _, row in df.iterrows():
        d = row.get(date_col)
        try:
            ds = pd.Timestamp(d).date().isoformat()
        except Exception:  # noqa: BLE001
            continue
        reports.append({"date": ds, "fields": _stringify(row.to_dict())})
    reports.sort(key=lambda r: r["date"])
    return reports


def fetch_a_fundamentals(ak_symbol: str) -> Optional[dict]:
    """A-share key financial indicators via akshare (per-symbol).

    Returns a PIT time series: {source, date_col, reports: [{date, fields}...]}
    sorted ascending by report date, or None. The feature layer selects the
    latest report on/before as_of (point-in-time) to avoid look-ahead bias.
    """
    ak = _ak()
    df = _guarded(lambda: ak.stock_financial_analysis_indicator(symbol=ak_symbol, start_year="2022"),
                  timeout=_AK_BEST_EFFORT_TIMEOUT)
    if df is None or df.empty:
        return None
    date_col = df.columns[0]  # first column is 日期
    reports = _reports_from_df(df, date_col)
    if not reports:
        return None
    return {"source": "stock_financial_analysis_indicator", "date_col": "日期", "reports": reports}


def fetch_hk_fundamentals(ak_symbol: str) -> Optional[dict]:
    """HK financial indicators via akshare (per-symbol). PIT time series."""
    ak = _ak()
    df = _guarded(lambda: ak.stock_financial_hk_analysis_indicator_em(symbol=ak_symbol, indicator="报告期"),
                  timeout=_AK_BEST_EFFORT_TIMEOUT)
    if df is None or df.empty:
        return None
    date_col = "REPORT_DATE" if "REPORT_DATE" in df.columns else df.columns[0]
    reports = _reports_from_df(df, date_col)
    if not reports:
        return None
    return {"source": "stock_financial_hk_analysis_indicator_em", "date_col": "REPORT_DATE", "reports": reports}


# --------------------------------------------------------------------------- #
# Flow / game signals (A-share, per-symbol)
# --------------------------------------------------------------------------- #
def fetch_a_fund_flow(ak_symbol: str, market: str) -> Optional[pd.DataFrame]:
    """Individual daily fund-flow history via EastMoney with browser TLS."""
    import time

    from curl_cffi import requests as browser_requests

    market_code = 1 if market == "sh" else 0
    params = {
        "lmt": "0",
        "klt": "101",
        "secid": f"{market_code}.{ak_symbol}",
        "fields1": "f1,f2,f3,f7",
        "fields2": (
            "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65"
        ),
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "_": int(time.time() * 1000),
    }
    payload = None
    for host in (
        "https://push2his.eastmoney.com",
        "http://push2his.eastmoney.com",
    ):
        try:
            response = browser_requests.get(
                host + "/api/qt/stock/fflow/daykline/get",
                params=params,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://data.eastmoney.com/",
                },
                impersonate="chrome120",
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("data"):
                break
        except Exception:  # noqa: BLE001
            continue
    rows = (payload or {}).get("data", {}).get("klines") or []
    if not rows:
        return None
    columns = [
        "date",
        "main_net_inflow",
        "small_net_inflow",
        "medium_net_inflow",
        "large_net_inflow",
        "extra_large_net_inflow",
        "main_net_ratio",
        "small_net_ratio",
        "medium_net_ratio",
        "large_net_ratio",
        "extra_large_net_ratio",
        "close",
        "pct_change",
        "_1",
        "_2",
    ]
    frame = pd.DataFrame([row.split(",") for row in rows], columns=columns)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in columns[1:-2]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    frame.attrs["source"] = "eastmoney_push2his_browser_tls"
    return frame


def fetch_a_fund_flow_tushare(
    ak_symbol: str,
    market: str,
    start_date: str = "20220901",
    end_date: str | None = None,
) -> Optional[pd.DataFrame]:
    """Optional full history via Tushare when ``TUSHARE_TOKEN`` is configured."""
    import os

    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        return None
    ts_code = f"{ak_symbol}.{'SH' if market == 'sh' else 'SZ'}"
    fields = [
        "ts_code", "trade_date",
        "buy_sm_amount", "sell_sm_amount",
        "buy_md_amount", "sell_md_amount",
        "buy_lg_amount", "sell_lg_amount",
        "buy_elg_amount", "sell_elg_amount",
        "net_mf_amount",
    ]
    response = requests.post(
        "https://api.tushare.pro",
        json={
            "api_name": "moneyflow",
            "token": token,
            "params": {
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date or date.today().strftime("%Y%m%d"),
            },
            "fields": ",".join(fields),
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") not in (0, None):
        return None
    data = payload.get("data") or {}
    columns = data.get("fields") or []
    items = data.get("items") or []
    if not columns or not items:
        return None
    raw = pd.DataFrame(items, columns=columns)

    def net(buy: str, sell: str) -> pd.Series:
        return (
            pd.to_numeric(raw.get(buy), errors="coerce")
            - pd.to_numeric(raw.get(sell), errors="coerce")
        ) * 10000.0

    frame = pd.DataFrame({
        "date": pd.to_datetime(raw["trade_date"], format="%Y%m%d", errors="coerce"),
        "main_net_inflow": (
            pd.to_numeric(raw["net_mf_amount"], errors="coerce") * 10000.0
        ),
        "small_net_inflow": net("buy_sm_amount", "sell_sm_amount"),
        "medium_net_inflow": net("buy_md_amount", "sell_md_amount"),
        "large_net_inflow": net("buy_lg_amount", "sell_lg_amount"),
        "extra_large_net_inflow": net("buy_elg_amount", "sell_elg_amount"),
    }).dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    frame.attrs["source"] = "tushare_moneyflow_authorized"
    frame.attrs["source_semantics"] = (
        "Tushare active-buy/sell order-size moneyflow; amounts converted "
        "from 10,000 CNY to CNY"
    )
    return frame if not frame.empty else None


def fetch_a_northbound(ak_symbol: str) -> Optional[list]:
    """Northbound (HSGT / Stock-Connect) daily holdings for one A-share, per-symbol.

    Columns: 持股日期, 当日收盘价, 当日涨跌幅, 持股数量, 持股市值,
             持股数量占A股百分比, 今日增持股数, 今日增持资金, 今日持股市值变化.

    IMPORTANT (probed 2026): the exchanges STOPPED publishing daily per-stock
    northbound holdings on **2024-08-16** (regulatory change). EastMoney's series
    is frozen at that date, so this returns an ARCHIVE that ends ~2024-08-16 with
    NOTHING after. It does NOT overlap a 2024-09+ weekly prediction window and is
    collected for completeness only — the collector flags it as stale/unusable.
    """
    ak = _ak()
    df = _guarded(lambda: ak.stock_hsgt_individual_em(symbol=ak_symbol),
                  timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    if df is None or df.empty:
        return None
    recs = []
    for _, row in df.iterrows():
        recs.append(_stringify(row.to_dict()))
    return recs


def fetch_a_daily_sina(tencent_code: str) -> Optional[pd.DataFrame]:
    """A-share daily via SINA (``stock_zh_a_daily``) — carries fields the primary
    Tencent OHLCV source omits: **成交额 (amount)**, **流通股本
    (outstanding_share)**, and **换手率 (turnover)**.

    This is the source that closes the turnover/float-shares data gaps: unlike the
    eastmoney endpoints (dead in-region) it is reachable, returns a FULL daily
    HISTORY (not just a snapshot), and works across main/ChiNext/STAR boards.

    Args:
        tencent_code: e.g. "sz002594" / "sh603345" (sina uses the same prefix).

    Returns a DataFrame indexed by date with columns
    [amount, outstanding_share, turnover] (qfq-adjusted), or None. We keep only
    the value-add columns here (OHLCV still comes from tencent_kline) to avoid
    duplicating the price series.
    """
    ak = _ak()
    df = _guarded(lambda: ak.stock_zh_a_daily(symbol=tencent_code, adjust="qfq"),
                  timeout=_AK_TIMEOUT)
    if df is None or df.empty or "amount" not in df.columns:
        return None
    df = df.copy()
    try:
        df["date"] = pd.to_datetime(df["date"])
    except Exception:  # noqa: BLE001
        return None
    df = df.set_index("date").sort_index()
    keep = [c for c in ("amount", "outstanding_share", "turnover") if c in df.columns]
    if not keep:
        return None
    for c in keep:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[keep]


def fetch_hk_daily_amount(ak_symbol: str) -> Optional[pd.DataFrame]:
    """HK daily via SINA (``stock_hk_daily``) — provides **成交额 (amount)** that
    the Tencent HK OHLCV source omits. Returns a DataFrame indexed by date with
    an ``amount`` column (qfq-adjusted), or None. HK has no free per-share
    float/turnover field here, so we keep amount only.

    Args:
        ak_symbol: 5-digit HK code, e.g. "01810".
    """
    ak = _ak()
    df = _guarded(lambda: ak.stock_hk_daily(symbol=ak_symbol, adjust="qfq"),
                  timeout=_AK_TIMEOUT)
    if df is None or df.empty or "amount" not in df.columns:
        return None
    df = df.copy()
    try:
        df["date"] = pd.to_datetime(df["date"])
    except Exception:  # noqa: BLE001
        return None
    df = df.set_index("date").sort_index()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    return df[["amount"]]


def fetch_hk_daily_eastmoney(ak_symbol: str) -> Optional[pd.DataFrame]:
    """HK raw daily amount/turnover with a derived share denominator."""
    from curl_cffi import requests as browser_requests

    params = {
        "secid": f"116.{ak_symbol}",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "0",
        "end": "20500000",
        "lmt": "1000000",
    }
    payload = None
    for host in (
        "http://push2his.eastmoney.com",
        "https://63.push2his.eastmoney.com",
        "https://push2his.eastmoney.com",
        "https://33.push2his.eastmoney.com",
    ):
        for attempt in range(4):
            try:
                response = browser_requests.get(
                    host + "/api/qt/stock/kline/get",
                    params=params,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://quote.eastmoney.com/",
                    },
                    timeout=30,
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("data"):
                    break
            except Exception:  # noqa: BLE001
                if attempt < 3:
                    import time
                    time.sleep(0.3 * (2 ** attempt))
                continue
        if payload and payload.get("data"):
            break
    rows = (payload or {}).get("data", {}).get("klines") or []
    records = []
    for raw in rows:
        parts = raw.split(",")
        if len(parts) < 11:
            continue
        try:
            observed = pd.Timestamp(parts[0])
            volume = float(parts[5])
            amount = float(parts[6])
            turnover = float(parts[10]) / 100.0
        except (TypeError, ValueError):
            continue
        records.append({
            "date": observed,
            "amount": amount,
            "turnover": turnover,
            "outstanding_share": volume / turnover if turnover > 0 else None,
        })
    if not records:
        return None
    frame = pd.DataFrame(records).set_index("date").sort_index()
    frame.attrs["source"] = "eastmoney_hk_raw_kline"
    frame.attrs["outstanding_share_method"] = (
        "derived_from_volume_div_provider_reported_turnover"
    )
    return frame[["amount", "outstanding_share", "turnover"]]


def fetch_hk_daily_tencent_extended(
    ak_symbol: str,
    count: int = 700,
) -> Optional[pd.DataFrame]:
    """HK daily amount and provider-reported turnover from Tencent HK K-lines."""
    response = requests.get(
        "https://web.ifzq.gtimg.cn/appstock/app/hkfqkline/get",
        params={"param": f"hk{ak_symbol},day,,,{count},qfq"},
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json().get("data", {}).get(f"hk{ak_symbol}", {})
    rows = data.get("qfqday") or data.get("day") or []
    records = []
    for row in rows:
        if len(row) < 9:
            continue
        try:
            observed = pd.Timestamp(row[0])
            volume = float(row[5])
            turnover = float(row[7]) / 100.0
            # Tencent expresses HK turnover in thousands of HKD.
            amount = float(row[8]) * 1000.0
        except (TypeError, ValueError):
            continue
        records.append({
            "date": observed,
            "amount": amount,
            "turnover": turnover,
            "outstanding_share": volume / turnover if turnover > 0 else None,
        })
    if not records:
        return None
    frame = pd.DataFrame(records).set_index("date").sort_index()
    frame.attrs["source"] = "tencent_hk_extended_kline"
    frame.attrs["outstanding_share_method"] = (
        "derived_from_volume_div_provider_reported_turnover"
    )
    return frame[["amount", "outstanding_share", "turnover"]]


def fetch_a_daily_ths(ak_symbol: str, tail: int = 520) -> Optional[pd.DataFrame]:
    """A-share daily microstructure via the public THS line endpoint.

    This is the fallback for special instruments that SINA omits (notably the
    CDR 689009). THS rows contain volume, amount and turnover. ``turnover`` is
    published as percent; we normalize it to a ratio and derive
    ``outstanding_share = volume / turnover``. The derivation is explicitly
    tagged in ``DataFrame.attrs`` and is cross-checked against CNINFO share
    changes by the collector.
    """
    import json

    import requests

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": f"https://stockpage.10jqka.com.cn/{ak_symbol}/",
    }
    base = f"https://d.10jqka.com.cn/v6/line/hs_{ak_symbol}/01"

    def get_payload(name: str) -> Optional[dict]:
        try:
            r = requests.get(f"{base}/{name}.js", headers=headers, timeout=_AK_TIMEOUT)
            r.raise_for_status()
            text = r.text
            left, right = text.find("("), text.rfind(")")
            if left < 0 or right <= left:
                return None
            return json.loads(text[left + 1:right])
        except Exception:  # noqa: BLE001
            return None

    latest = get_payload("last")
    if not latest:
        return None
    years = sorted((latest.get("year") or {}).keys(), reverse=True)
    payloads = []
    estimated = 0
    for year in years:
        payload = get_payload(year)
        if payload:
            payloads.append(payload)
            estimated += int((latest.get("year") or {}).get(year, 0) or 0)
        if tail and estimated >= tail:
            break
    if not payloads:
        payloads = [latest]

    by_date: dict[str, dict] = {}
    for payload in payloads:
        for raw in str(payload.get("data") or "").split(";"):
            parts = raw.split(",")
            if len(parts) < 8:
                continue
            try:
                volume = float(parts[5])
                amount = float(parts[6])
                turnover = float(parts[7]) / 100.0
                outstanding = volume / turnover if turnover > 0 else None
                by_date[parts[0]] = {
                    "date": pd.Timestamp(parts[0]),
                    "amount": amount,
                    "outstanding_share": outstanding,
                    "turnover": turnover,
                }
            except (TypeError, ValueError):
                continue
    if not by_date:
        return None
    df = pd.DataFrame.from_records(list(by_date.values())).set_index("date").sort_index()
    if tail and len(df) > tail:
        df = df.tail(tail)
    df.attrs["source"] = "ths_public_line"
    df.attrs["outstanding_share_method"] = "derived_from_volume_div_turnover"
    return df[["amount", "outstanding_share", "turnover"]]


def fetch_hk_dividends(ak_symbol: str) -> Optional[list]:
    """HK dividend payout history from EastMoney F10 (per-symbol)."""
    ak = _ak()
    df = _guarded(lambda: ak.stock_hk_dividend_payout_em(symbol=ak_symbol),
                  timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    if df is None:
        return None
    return [_stringify(row.to_dict()) for _, row in df.iterrows()]


def fetch_hk_share_snapshot(ak_symbol: str) -> Optional[dict]:
    """Exact latest HK issued-share snapshot from EastMoney F10."""
    ak = _ak()
    df = _guarded(lambda: ak.stock_hk_financial_indicator_em(symbol=ak_symbol),
                  timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    if df is None or df.empty:
        return None
    row = df.iloc[0]
    fields = (
        "法定股本(股)",
        "已发行股本(股)",
        "已发行股本-H股(股)",
        "总市值(港元)",
        "港股市值(港元)",
        "每手股",
    )
    out = {}
    for field in fields:
        value = row.get(field)
        try:
            out[field] = float(value) if value is not None and pd.notna(value) else None
        except (TypeError, ValueError):
            out[field] = None
    return out


def fetch_a_share_changes(ak_symbol: str, start: str, end: str) -> Optional[list]:
    """CNINFO share-capital change history, including exact tradable shares.

    CNINFO is an independent official disclosure source used to validate SINA
    and the THS-derived float-share series. Calls must remain serial because the
    AKShare implementation initializes mini_racer/V8 for its request token.
    """
    ak = _ak()
    df = _guarded(
        lambda: ak.stock_share_change_cninfo(
            symbol=ak_symbol, start_date=start, end_date=end
        ),
        timeout=_AK_TIMEOUT,
    )
    if df is None:
        return None
    return [_stringify(row.to_dict()) for _, row in df.iterrows()]



# --------------------------------------------------------------------------- #
# Three financial statements (per-symbol, PIT time series) — M6 data parity
# --------------------------------------------------------------------------- #
# Key line items we extract from each statement (English EM fields). Everything
# is stored as a PIT series keyed by REPORT_DATE with NOTICE_DATE as publish date.
_PROFIT_FIELDS = ["TOTAL_OPERATE_INCOME", "OPERATE_INCOME", "TOTAL_OPERATE_COST",
                  "OPERATE_PROFIT", "NETPROFIT", "PARENT_NETPROFIT"]
_BALANCE_FIELDS = ["TOTAL_ASSETS", "TOTAL_LIABILITIES", "TOTAL_EQUITY",
                   "TOTAL_PARENT_EQUITY", "SHARE_CAPITAL", "MONETARYFUNDS"]
_CASHFLOW_FIELDS = ["NETCASH_OPERATE", "NETCASH_INVEST", "NETCASH_FINANCE",
                    "CONSTRUCT_LONG_ASSET"]


def _em_prefixed(ak_symbol: str, market_prefix: str) -> str:
    """EM statement APIs want e.g. 'SZ002594' / 'SH603345'."""
    return f"{market_prefix.upper()}{ak_symbol}"


def _statement_series(df: pd.DataFrame, fields: list[str]) -> list[dict]:
    """Rows -> [{report_date, published_at, fields{...}}] sorted ascending."""
    if df is None or df.empty:
        return []
    out = []
    for _, row in df.iterrows():
        rd = row.get("REPORT_DATE")
        try:
            rd_s = pd.Timestamp(rd).date().isoformat()
        except Exception:  # noqa: BLE001
            continue
        pub = row.get("NOTICE_DATE")
        try:
            pub_s = pd.Timestamp(pub).date().isoformat() if pub is not None else None
        except Exception:  # noqa: BLE001
            pub_s = None
        vals = {}
        for f in fields:
            v = row.get(f)
            try:
                vals[f] = float(v) if v is not None and pd.notna(v) else None
            except (ValueError, TypeError):
                vals[f] = None
        out.append({"report_date": rd_s, "published_at": pub_s, "fields": vals})
    out.sort(key=lambda r: r["report_date"])
    return out


def fetch_a_statements(ak_symbol: str, market_prefix: str) -> Optional[dict]:
    """Fetch the three A-share statements (per-symbol) as PIT series.

    Returns {income, balance, cashflow: [{report_date, published_at, fields}]}.
    """
    ak = _ak()
    em = _em_prefixed(ak_symbol, market_prefix)
    prof = _guarded(lambda: ak.stock_profit_sheet_by_report_em(symbol=em), timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    bal = _guarded(lambda: ak.stock_balance_sheet_by_report_em(symbol=em), timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    cf = _guarded(lambda: ak.stock_cash_flow_sheet_by_report_em(symbol=em), timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    income = _statement_series(prof, _PROFIT_FIELDS)
    balance = _statement_series(bal, _BALANCE_FIELDS)
    cashflow = _statement_series(cf, _CASHFLOW_FIELDS)
    if not income and not balance and not cashflow:
        return None
    return {"source": "eastmoney_statements", "income": income,
            "balance": balance, "cashflow": cashflow}


def fetch_a_dividends(ak_symbol: str) -> Optional[list]:
    """A-share dividend / stock-transfer history with a THS fallback.

    EastMoney returns empty for issuers whose plans are consistently
    ``不分配不转增`` (e.g. 688981). THS preserves those decisions, which lets the
    cache distinguish an explicit no-distribution plan from a collection gap.
    """
    ak = _ak()
    df = _guarded(lambda: ak.stock_fhps_detail_em(symbol=ak_symbol), timeout=_AK_BEST_EFFORT_TIMEOUT)
    if df is None or df.empty:
        df = _guarded(lambda: ak.stock_fhps_detail_ths(symbol=ak_symbol),
                      timeout=_AK_BEST_EFFORT_TIMEOUT + 4)
    if df is None or df.empty:
        return None
    recs = []
    for _, row in df.iterrows():
        recs.append(_stringify(row.to_dict()))
    return recs


# HK three statements (long format: STD_ITEM_NAME + AMOUNT). We keep a curated
# set of standard line items per statement, pivoted into a PIT series keyed by
# REPORT_DATE. HK filings carry no NOTICE_DATE here, so PIT uses report_date with
# a conservative publish lag applied by the feature layer.
_HK_INCOME_ITEMS = ["营业额", "营运收入", "销售成本", "毛利", "除税前利润", "股东应占溢利", "本期溢利"]
_HK_BALANCE_ITEMS = ["资产总额", "负债总额", "股东权益", "总权益", "现金及现金等价物"]
_HK_CASHFLOW_ITEMS = [
    "经营业务现金净额", "投资业务现金净额", "融资业务现金净额",
    "现金净额", "期末现金",
]
_HK_ITEM_ALIASES = {
    "004009999": "资产总额",
    "004025999": "负债总额",
    "004030999": "股东权益",
    "004036999": "总权益",
    "004002010": "现金及现金等价物",
    "总资产": "资产总额",
    "总负债": "负债总额",
    "现金及等价物": "现金及现金等价物",
}

_HK_STMT_SPEC = {
    "income": ("利润表", _HK_INCOME_ITEMS),
    "balance": ("资产负债表", _HK_BALANCE_ITEMS),
    "cashflow": ("现金流量表", _HK_CASHFLOW_ITEMS),
}

_CURRENCY_CODES = {
    "人民币": "CNY",
    "人民幣": "CNY",
    "港元": "HKD",
    "港币": "HKD",
    "港幣": "HKD",
    "美元": "USD",
}


def fetch_hk_statement_metadata(ak_symbol: str) -> dict[str, dict]:
    """Fetch report-level currency, accounting standard, and report type."""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_CUSTOM_HKSK_APPFN_CASHFLOW_SUMMARY",
        "columns": (
            "SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,START_DATE,REPORT_DATE,"
            "FISCAL_YEAR,CURRENCY,ACCOUNT_STANDARD,REPORT_TYPE"
        ),
        "filter": f'(SECUCODE="{ak_symbol}.HK")',
        "source": "F10",
        "client": "PC",
    }

    def fetch():
        response = requests.get(url, params=params, timeout=_AK_TIMEOUT)
        response.raise_for_status()
        return response.json()

    payload = _guarded(fetch, timeout=_AK_TIMEOUT)
    try:
        rows = payload["result"]["data"][0]["REPORT_LIST"]
    except (KeyError, IndexError, TypeError):
        return {}
    out = {}
    for row in rows:
        try:
            report_date = pd.Timestamp(row.get("REPORT_DATE")).date().isoformat()
        except Exception:  # noqa: BLE001
            continue
        raw_currency = str(row.get("CURRENCY") or "").strip()
        out[report_date] = {
            "start_date": (
                str(row.get("START_DATE"))[:10]
                if row.get("START_DATE") else None
            ),
            "currency": _CURRENCY_CODES.get(raw_currency),
            "currency_raw": raw_currency or None,
            "amount_unit": "base_currency_unit",
            "unit_multiplier": 1.0,
            "accounting_standard": row.get("ACCOUNT_STANDARD"),
            "report_type": row.get("REPORT_TYPE"),
        }
    return out


def _hk_statement_series(
    df: pd.DataFrame,
    items: list[str],
    metadata: dict[str, dict] | None = None,
) -> list[dict]:
    """Pivot HK long-format statement to [{report_date, fields{item: amount}}]."""
    if df is None or df.empty or "STD_ITEM_NAME" not in df.columns:
        return []
    by_date: dict[str, dict] = {}
    for _, row in df.iterrows():
        source_name = str(row.get("STD_ITEM_NAME", ""))
        source_code = str(row.get("STD_ITEM_CODE", ""))
        name = _HK_ITEM_ALIASES.get(
            source_code,
            _HK_ITEM_ALIASES.get(source_name, source_name),
        )
        if name not in items:
            continue
        rd = row.get("REPORT_DATE")
        try:
            rd_s = pd.Timestamp(rd).date().isoformat()
        except Exception:  # noqa: BLE001
            continue
        amt = row.get("AMOUNT")
        try:
            val = float(amt) if amt is not None and pd.notna(amt) else None
        except (ValueError, TypeError):
            val = None
        by_date.setdefault(rd_s, {})[name] = val
    metadata = metadata or {}
    out = []
    for report_date, fields in by_date.items():
        report_meta = metadata.get(report_date, {})
        currency = report_meta.get("currency")
        out.append({
            "report_date": report_date,
            "published_at": None,
            "start_date": report_meta.get("start_date"),
            "currency": currency,
            "currency_raw": report_meta.get("currency_raw"),
            "amount_unit": report_meta.get("amount_unit"),
            "unit_multiplier": report_meta.get("unit_multiplier"),
            "accounting_standard": report_meta.get("accounting_standard"),
            "report_type": report_meta.get("report_type"),
            "fields": fields,
            "fields_cny": fields if currency == "CNY" else None,
        })
    out.sort(key=lambda r: r["report_date"])
    return out


def _fill_hk_balance_cash_from_cashflow(statements: dict) -> None:
    """Use the reported cash-flow ending cash when balance cash is omitted."""
    cash_by_date = {
        report["report_date"]: report.get("fields", {}).get("期末现金")
        for report in statements.get("cashflow", [])
    }
    for report in statements.get("balance", []):
        fields = report.get("fields") or {}
        if fields.get("现金及现金等价物") is not None:
            continue
        ending_cash = cash_by_date.get(report.get("report_date"))
        if ending_cash is not None:
            fields["现金及现金等价物"] = ending_cash
            report.setdefault("field_sources", {})[
                "现金及现金等价物"
            ] = "cashflow_statement_ending_cash"


def fetch_hk_statements(ak_symbol: str) -> Optional[dict]:
    """Fetch HK three statements (per-symbol) as PIT series (curated line items)."""
    ak = _ak()
    out: dict = {"source": "eastmoney_hk_statements"}
    metadata = fetch_hk_statement_metadata(ak_symbol)
    got_any = False
    for key, (sym_cn, items) in _HK_STMT_SPEC.items():
        df = _guarded(
            lambda s=sym_cn: ak.stock_financial_hk_report_em(stock=ak_symbol, symbol=s, indicator="报告期"),
            timeout=_AK_BEST_EFFORT_TIMEOUT + 6,
        )
        series = _hk_statement_series(df, items, metadata)
        out[key] = series
        got_any = got_any or bool(series)
    _fill_hk_balance_cash_from_cashflow(out)
    return out if got_any else None



def _stringify(d: dict) -> dict:
    """Coerce values to JSON/parquet-friendly primitives."""
    out = {}
    for k, v in d.items():
        try:
            if pd.isna(v):
                out[str(k)] = None
                continue
        except (ValueError, TypeError):
            pass
        out[str(k)] = v if isinstance(v, (int, float, str, bool)) else str(v)
    return out
