"""Tencent daily K-line provider (web.ifzq.gtimg.cn/appstock).

This is the *primary* OHLCV source for BOTH A-share and HK, chosen after live
probing showed:
  - eastmoney (akshare stock_hk_hist / stock_zh_a_hist) is unreliable in-region
    (RemoteDisconnected / ReadTimeout, per 2026 anti-crawl tightening);
  - this single endpoint returns qfq-adjusted daily bars for A-share and HK in
    one fast JSON call:  [date, open, close, high, low, volume(, extra)].

We request forward-adjusted (qfq) bars so factor computation sees a continuous
price series. Volume units differ (A-share: 手/lots; HK: shares) but factors use
volume only relatively (z-scores / ratios), so absolute units don't matter.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import requests

_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
_BACKUP_URL = "https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get"
_TIMEOUT = 20
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://gu.qq.com/",
}

# adjust -> the JSON sub-key Tencent uses
_ADJUST_KEY = {"qfq": "qfqday", "hfq": "hfqday", "none": "day"}


def _response(session, param, expected=None):
    """Same Tencent payload contract over two verified HTTPS endpoints"""
    failure = None
    for url in (_URL, _BACKUP_URL):
        try:
            response = session.get(url, params={'param': param}, headers=_HEADERS, timeout=_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get('code') != 0:
                raise ValueError('Tencent response schema/code rejected')
            if expected and not payload.get('data', {}).get(expected[0], {}).get(expected[1]):
                raise ValueError(f'explicit {expected[1]} response required; no raw fallback')
            return payload
        except (requests.RequestException, ValueError) as exc:
            failure = exc
    raise failure


def fetch_daily(
    tencent_code: str,
    count: int = 320,
    adjust: str = "qfq",
    session: requests.Session | None = None,
    strict_adjustment: bool = False,
) -> pd.DataFrame:
    """Fetch up to ``count`` most-recent daily bars for one symbol.

    Args:
        tencent_code: e.g. "sz002594" / "hk01810".
        count: number of trailing daily bars (default 320 ≈ 15 months).
        adjust: "qfq" | "hfq" | "none".

    Returns:
        DataFrame indexed by ``date`` (datetime64) with float columns
        [open, high, low, close, volume], sorted ascending. Empty DataFrame if
        the payload has no data.

    Raises:
        requests.RequestException on network failure (caller handles fallback).
        ValueError if the payload is malformed.
    """
    param = f"{tencent_code},day,,,{int(count)},{adjust}"
    sess = session or requests
    js = _response(sess, param, (tencent_code,_ADJUST_KEY[adjust]) if strict_adjustment else None)
    if js.get("code") != 0:
        raise ValueError(f"tencent kline code={js.get('code')} msg={js.get('msg')}")

    node = js.get("data", {}).get(tencent_code, {})
    key = _ADJUST_KEY.get(adjust, "qfqday")
    if strict_adjustment and not node.get(key):
        raise ValueError(f"explicit {key} response required; no raw fallback")
    rows = node.get(key) or node.get("day") or []
    if not rows:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    recs = []
    for r in rows:
        # r = [date, open, close, high, low, volume, (optional dict)]
        if len(r) < 6:
            continue
        try:
            recs.append(
                {
                    "date": pd.Timestamp(r[0]),
                    "open": float(r[1]),
                    "close": float(r[2]),
                    "high": float(r[3]),
                    "low": float(r[4]),
                    "volume": float(r[5]),
                }
            )
        except (ValueError, TypeError):
            continue

    df = pd.DataFrame.from_records(recs)
    if df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    df = df.set_index("date").sort_index()
    result = df[["open", "high", "low", "close", "volume"]]
    result.attrs['price_basis'] = adjust if node.get(key) else 'none'
    result.attrs['response_key'] = key if node.get(key) else 'day'
    return result


def fetch_daily_range(
    tencent_code: str,
    start: date,
    end: date,
    *,
    adjust: str = "none",
    count: int = 1500,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Fetch a bounded date range; Tencent supports more than 700 rows here."""
    param = (
        f"{tencent_code},day,{start.isoformat()},{end.isoformat()},"
        f"{int(count)},{adjust}"
    )
    sess = session or requests
    payload = _response(sess, param, (tencent_code,_ADJUST_KEY[adjust]) if adjust in {'qfq','hfq'} else None)
    if payload.get("code") != 0:
        raise ValueError(
            f"tencent kline code={payload.get('code')} msg={payload.get('msg')}"
        )
    node = payload.get("data", {}).get(tencent_code, {})
    key = _ADJUST_KEY.get(adjust, "day")
    if adjust in {'qfq', 'hfq'} and not node.get(key):
        raise ValueError('explicit adjusted range required; no raw fallback')
    rows = node.get(key) or node.get("day") or []
    records = []
    for row in rows:
        if len(row) < 6:
            continue
        try:
            observed = pd.Timestamp(row[0])
            records.append({
                "date": observed,
                "open": float(row[1]),
                "close": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
                "volume": float(row[5]),
            })
        except (TypeError, ValueError):
            continue
    if not records:
        return pd.DataFrame(
            columns=["open", "high", "low", "close", "volume"]
        )
    frame = pd.DataFrame(records).set_index("date").sort_index()
    result = frame[["open", "high", "low", "close", "volume"]]
    result.attrs.update(price_basis=adjust if node.get(key) else 'none',
        response_key=key if node.get(key) else 'day', adjustment_request=adjust)
    return result


def latest_bar_date(df: pd.DataFrame) -> date | None:
    if df is None or df.empty:
        return None
    return df.index[-1].date()
