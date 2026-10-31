"""Historical 60-minute Anker bars, hard-scoped to 300866.SZ."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests

from .. import config, paths
from ..schemas import Market
from . import calendar as qcal


_TARGET_SYMBOL = "300866.SZ"
_SOURCE_CODE = "sz300866"
_URL = (
    "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/"
    "CN_MarketDataService.getKLineData"
)
_ROOT = paths.CACHE_DIR / "anker_historical_intraday"
_BAR_PATH = _ROOT / "300866_60m.parquet"
_MANIFEST_PATH = _ROOT / "300866_60m_manifest.json"


def parse_sina_60m_payload(
    text: str,
    source_code: str = _SOURCE_CODE,
) -> pd.DataFrame:
    if source_code != _SOURCE_CODE:
        raise ValueError(
            f"Anker intraday symbol is not allowed: {source_code}"
        )
    try:
        payload = str(text).split("=(", 1)[1].rsplit(");", 1)[0]
        records = json.loads(payload)
    except (IndexError, json.JSONDecodeError) as exc:
        raise ValueError("invalid Sina 60-minute JSONP payload") from exc
    rows = []
    for record in records:
        try:
            timestamp = pd.Timestamp(
                str(record["day"]),
                tz="Asia/Shanghai",
            )
            row = {
                "symbol": _TARGET_SYMBOL,
                "timestamp": timestamp,
                "open": float(record["open"]),
                "high": float(record["high"]),
                "low": float(record["low"]),
                "close": float(record["close"]),
                "volume": float(record["volume"]),
                "amount": (
                    float(record["amount"])
                    if record.get("amount") not in (None, "")
                    else None
                ),
                "source": "Sina CN_MarketDataService 60-minute",
            }
        except (KeyError, TypeError, ValueError):
            continue
        if (
            min(row["open"], row["high"], row["low"], row["close"]) <= 0
            or row["volume"] < 0
            or row["high"] < max(row["open"], row["close"], row["low"])
            or row["low"] > min(row["open"], row["close"], row["high"])
        ):
            raise ValueError("invalid Anker 60-minute OHLCV row")
        rows.append(row)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    return (
        frame.drop_duplicates(["symbol", "timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def collect_anker_historical_intraday(
    *,
    force: bool = False,
    timeout_seconds: float = 30.0,
) -> dict:
    fixed_sixty = {
        stock.symbol for stock in config.load_extended_universe().stocks
    }
    if len(fixed_sixty) != 60 or _TARGET_SYMBOL not in fixed_sixty:
        raise ValueError("Anker collector requires the fixed 60 universe")
    if _BAR_PATH.exists() and not force:
        frame = pd.read_parquet(_BAR_PATH)
        return {
            "status": "CACHED",
            "symbol": _TARGET_SYMBOL,
            "rows": len(frame),
            "first_timestamp": str(frame["timestamp"].min()),
            "last_timestamp": str(frame["timestamp"].max()),
            "path": str(_BAR_PATH),
        }
    response = requests.get(
        _URL,
        params={
            "symbol": _SOURCE_CODE,
            "scale": "60",
            "ma": "no",
            "datalen": "1970",
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    frame = parse_sina_60m_payload(response.text)
    if frame.empty:
        raise ValueError("Sina returned no Anker 60-minute bars")
    captured_at = datetime.now(timezone.utc)
    source_hash = hashlib.sha256(response.content).hexdigest()
    frame["captured_at"] = captured_at
    frame["source_sha256"] = source_hash
    _ROOT.mkdir(parents=True, exist_ok=True)
    temporary = _BAR_PATH.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False)
    temporary.replace(_BAR_PATH)
    manifest = {
        "schema_version": 1,
        "symbol": _TARGET_SYMBOL,
        "source_code": _SOURCE_CODE,
        "source": "Sina CN_MarketDataService 60-minute",
        "captured_at": captured_at.isoformat(),
        "source_sha256": source_hash,
        "rows": len(frame),
        "first_timestamp": str(frame["timestamp"].min()),
        "last_timestamp": str(frame["timestamp"].max()),
        "unique_trade_days": int(
            pd.to_datetime(frame["timestamp"], utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.date.nunique()
        ),
        "path": str(_BAR_PATH),
        "collection_symbols": [_TARGET_SYMBOL],
    }
    _MANIFEST_PATH.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {"status": "OK", **manifest}


def load_anker_historical_intraday() -> pd.DataFrame:
    if not _BAR_PATH.exists():
        collect_anker_historical_intraday()
    frame = pd.read_parquet(_BAR_PATH)
    symbols = set(frame["symbol"].astype(str))
    if symbols != {_TARGET_SYMBOL}:
        raise ValueError("Anker intraday cache contains another symbol")
    return frame.sort_values("timestamp").reset_index(drop=True)


def anker_historical_intraday_status() -> dict:
    if not _BAR_PATH.exists():
        return {
            "status": "NOT_COLLECTED",
            "symbol": _TARGET_SYMBOL,
            "rows": 0,
        }
    frame = load_anker_historical_intraday()
    local = pd.to_datetime(
        frame["timestamp"],
        utc=True,
    ).dt.tz_convert("Asia/Shanghai")
    return {
        "status": "READY",
        "symbol": _TARGET_SYMBOL,
        "rows": len(frame),
        "first_timestamp": str(local.min()),
        "last_timestamp": str(local.max()),
        "unique_trade_days": int(local.dt.date.nunique()),
        "path": str(_BAR_PATH),
        "historical_only": True,
    }


def _compound(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(np.prod(1.0 + numeric.to_numpy(dtype=float)) - 1.0)


def build_historical_intraday_weekly_features(
    decision_weeks: list[str],
    frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    bars = (
        load_anker_historical_intraday()
        if frame is None else frame.copy()
    )
    if set(bars["symbol"].astype(str)) != {_TARGET_SYMBOL}:
        raise ValueError("weekly intraday features accept only Anker")
    working = bars.sort_values("timestamp").copy()
    local = pd.to_datetime(
        working["timestamp"],
        utc=True,
    ).dt.tz_convert("Asia/Shanghai")
    working["trade_date"] = local.dt.date
    working["local_time"] = local.dt.strftime("%H:%M")
    expected_times = {"10:30", "11:30", "14:00", "15:00"}
    daily_rows = []
    for trade_date, selected in working.groupby("trade_date", sort=True):
        selected = (
            selected.drop_duplicates("local_time", keep="last")
            .sort_values("timestamp")
        )
        if set(selected["local_time"]) != expected_times:
            continue
        by_time = selected.set_index("local_time")
        open_price = float(by_time.loc["10:30", "open"])
        close_price = float(by_time.loc["15:00", "close"])
        high = float(selected["high"].max())
        low = float(selected["low"].min())
        bar_returns = (
            pd.to_numeric(selected["close"], errors="coerce")
            / pd.to_numeric(selected["open"], errors="coerce")
            - 1.0
        )
        total_amount = float(pd.to_numeric(
            selected["amount"],
            errors="coerce",
        ).sum())
        daily_rows.append({
            "trade_date": trade_date,
            "intraday": close_price / open_price - 1.0,
            "realized_volatility": float(np.sqrt(
                np.square(bar_returns.to_numpy(dtype=float)).sum()
            )),
            "range": high / low - 1.0,
            "morning": (
                float(by_time.loc["11:30", "close"]) / open_price - 1.0
            ),
            "afternoon": (
                close_price / float(by_time.loc["14:00", "open"]) - 1.0
            ),
            "last_hour": (
                close_price / float(by_time.loc["15:00", "open"]) - 1.0
            ),
            "last_hour_share": (
                float(by_time.loc["15:00", "amount"]) / total_amount
                if total_amount > 0 else None
            ),
            "close_location": (
                (close_price - low) / (high - low)
                if high > low else 0.5
            ),
        })
    daily = pd.DataFrame(daily_rows)
    indexed = (
        daily.set_index("trade_date")
        if not daily.empty else pd.DataFrame()
    )
    rows = []
    for week in map(str, decision_weeks):
        decision = date.fromisoformat(week)
        expected = qcal.sessions_between(
            Market.A,
            decision - timedelta(days=4),
            decision,
        )
        observed = [
            session for session in expected
            if not indexed.empty and session in indexed.index
        ]
        complete = bool(expected) and len(observed) == len(expected)
        row = {
            "week_ending": week,
            "anker_hist60_available": float(complete),
            "anker_hist60_complete_sessions": len(observed),
            "anker_hist60_expected_sessions": len(expected),
            "anker_hist60_week_intraday_return": None,
            "anker_hist60_week_realized_volatility": None,
            "anker_hist60_week_range": None,
            "anker_hist60_last_hour_return": None,
            "anker_hist60_last_hour_amount_share": None,
            "anker_hist60_path_asymmetry": None,
            "anker_hist60_close_location": None,
        }
        if complete:
            selected = indexed.loc[observed].copy()
            if isinstance(selected, pd.Series):
                selected = selected.to_frame().T
            morning = _compound(selected["morning"])
            afternoon = _compound(selected["afternoon"])
            last = selected.iloc[-1]
            row.update({
                "anker_hist60_week_intraday_return": _compound(
                    selected["intraday"]
                ),
                "anker_hist60_week_realized_volatility": float(np.sqrt(
                    np.square(selected["realized_volatility"]).sum()
                )),
                "anker_hist60_week_range": float(np.sqrt(
                    np.square(selected["range"]).sum()
                )),
                "anker_hist60_last_hour_return": float(last["last_hour"]),
                "anker_hist60_last_hour_amount_share": float(
                    last["last_hour_share"]
                ),
                "anker_hist60_path_asymmetry": (
                    float(afternoon - morning)
                    if morning is not None and afternoon is not None
                    else None
                ),
                "anker_hist60_close_location": float(
                    last["close_location"]
                ),
            })
        rows.append(row)
    return pd.DataFrame(rows)
