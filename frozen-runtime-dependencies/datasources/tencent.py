"""Tencent qt.gtimg direct provider (free, no key).

One batch HTTP request returns real-time snapshots for ALL requested symbols
(A-share + HK mixed), so 12 symbols cost a *single* call — the cheapest,
lowest-ban-risk snapshot source. Response is GBK-encoded, ``;``-separated lines
of the form:

    v_sz002594="51~比亚迪~002594~90.04~89.45~89.77~...";
    v_hk01810="100~小米集团-W~01810~27.060~26.860~26.660~...";

Field layouts differ between A-share and HK. We only rely on the stable
leading fields (name, code, last, prev_close, open) plus, where positionally
reliable, high/low. Everything is validated downstream by the quality gate, so
we parse defensively and never guess deep fields.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests

QT_URL = "http://qt.gtimg.cn/q={codes}"
_TIMEOUT = 15
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Referer": "https://gu.qq.com/",
}


@dataclass
class Snapshot:
    tencent_code: str
    name: str
    code: str
    last: float
    prev_close: float
    open: float
    high: float | None
    low: float | None
    change_pct: float | None
    raw_field_count: int

    def as_dict(self) -> dict:
        return {
            "tencent_code": self.tencent_code,
            "name": self.name,
            "code": self.code,
            "last": self.last,
            "prev_close": self.prev_close,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "change_pct": self.change_pct,
            "raw_field_count": self.raw_field_count,
        }


def _f(x: str) -> float | None:
    try:
        v = float(x)
        return v
    except (ValueError, TypeError):
        return None


def _parse_line(line: str) -> Snapshot | None:
    """Parse one ``v_xxx="...";`` line into a Snapshot.

    Common leading layout (both A & HK):
      [0]=market flag [1]=name [2]=code [3]=last [4]=prev_close [5]=open
      [6]=volume ...
    A-share high/low observed at index 33/34; HK high/low at index 33/34 as well
    in the sampled payloads, but HK layouts vary, so we treat high/low as
    best-effort and let the quality gate rely on OHLCV bars instead.
    """
    line = line.strip()
    if not line or "=" not in line:
        return None
    key, _, payload = line.partition("=")
    tencent_code = key.replace("v_", "").strip()
    payload = payload.strip().strip(";").strip('"')
    if not payload:
        return None
    parts = payload.split("~")
    if len(parts) < 6:
        return None

    last = _f(parts[3])
    prev_close = _f(parts[4])
    open_ = _f(parts[5])
    if last is None or prev_close is None:
        return None

    change_pct = None
    if prev_close and prev_close != 0:
        change_pct = round((last - prev_close) / prev_close * 100, 4)

    high = _f(parts[33]) if len(parts) > 34 else None
    low = _f(parts[34]) if len(parts) > 34 else None
    # sanity: high/low must bracket last, else discard (layout mismatch)
    if high is not None and low is not None:
        if not (low <= last <= high or low <= prev_close <= high):
            high, low = None, None

    return Snapshot(
        tencent_code=tencent_code,
        name=parts[1],
        code=parts[2],
        last=last,
        prev_close=prev_close,
        open=open_ if open_ is not None else last,
        high=high,
        low=low,
        change_pct=change_pct,
        raw_field_count=len(parts),
    )


def fetch_snapshots(tencent_codes: list[str], session: requests.Session | None = None) -> dict[str, Snapshot]:
    """Fetch real-time snapshots for the given tencent codes in ONE request.

    Args:
        tencent_codes: e.g. ["sz002594", "hk01810", ...] (the 12 universe codes).

    Returns:
        Mapping tencent_code -> Snapshot. Missing/blank entries are omitted; the
        caller (quality gate) decides whether an omission is fatal.

    Raises:
        requests.RequestException on network failure (caller handles fallback).
    """
    codes = ",".join(tencent_codes)
    url = QT_URL.format(codes=codes)
    sess = session or requests
    resp = sess.get(url, headers=_HEADERS, timeout=_TIMEOUT)
    resp.raise_for_status()
    resp.encoding = "gbk"
    text = resp.text

    out: dict[str, Snapshot] = {}
    for line in text.split(";"):
        snap = _parse_line(line)
        if snap is not None:
            out[snap.tencent_code] = snap
    return out
