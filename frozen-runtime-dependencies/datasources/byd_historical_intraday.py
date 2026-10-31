"""Historical 60-minute BYD bars from Sina, hard-scoped to the fixed 60."""

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


_TARGET_SYMBOL = "002594.SZ"
_SOURCE_CODE = "sz002594"
_URL = (
    "https://quotes.sina.cn/cn/api/jsonp_v2.php/=/"
    "CN_MarketDataService.getKLineData"
)
_ROOT = paths.CACHE_DIR / "byd_historical_intraday"
_BAR_PATH = _ROOT / "002594_60m.parquet"
_MANIFEST_PATH = _ROOT / "002594_60m_manifest.json"


def parse_sina_60m_payload(
    text: str,
    source_code: str = _SOURCE_CODE,
) -> pd.DataFrame:
    if source_code != _SOURCE_CODE:
        raise ValueError(f"historical intraday symbol is not allowed: {source_code}")
    raw = str(text)
    try:
        payload = raw.split("=(", 1)[1].rsplit(");", 1)[0]
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
            raise ValueError("invalid Sina 60-minute OHLCV row")
        rows.append(row)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame.drop_duplicates(
        ["symbol", "timestamp"],
        keep="last",
    ).sort_values("timestamp").reset_index(drop=True)
    if not frame["timestamp"].is_monotonic_increasing:
        raise ValueError("Sina 60-minute timestamps are not monotonic")
    return frame


def collect_byd_historical_intraday(
    *,
    force: bool = False,
    timeout_seconds: float = 20.0,
) -> dict:
    fixed_sixty = {
        stock.symbol for stock in config.load_extended_universe().stocks
    }
    if _TARGET_SYMBOL not in fixed_sixty or len(fixed_sixty) != 60:
        raise ValueError("BYD historical intraday collector requires fixed 60")
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
    frame = parse_sina_60m_payload(response.text, _SOURCE_CODE)
    if frame.empty:
        raise ValueError("Sina returned no BYD 60-minute bars")
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


def load_byd_historical_intraday() -> pd.DataFrame:
    if not _BAR_PATH.exists():
        collect_byd_historical_intraday()
    frame = pd.read_parquet(_BAR_PATH)
    symbols = set(frame["symbol"].astype(str))
    if symbols != {_TARGET_SYMBOL}:
        raise ValueError("historical intraday cache contains non-BYD symbols")
    return frame.sort_values("timestamp").reset_index(drop=True)


def byd_historical_intraday_status() -> dict:
    if not _BAR_PATH.exists():
        return {
            "status": "NOT_COLLECTED",
            "symbol": _TARGET_SYMBOL,
            "rows": 0,
        }
    frame = load_byd_historical_intraday()
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


def build_historical_intraday_daily_features(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    symbols = set(frame["symbol"].astype(str))
    if symbols != {_TARGET_SYMBOL}:
        raise ValueError("historical daily features accept only BYD")
    working = frame.copy().sort_values("timestamp")
    local = pd.to_datetime(
        working["timestamp"],
        utc=True,
    ).dt.tz_convert("Asia/Shanghai")
    working["trade_date"] = local.dt.date
    working["local_time"] = local.dt.strftime("%H:%M")
    expected_times = {"10:30", "11:30", "14:00", "15:00"}
    rows = []
    for trade_date, selected in working.groupby("trade_date", sort=True):
        selected = selected.drop_duplicates(
            "local_time",
            keep="last",
        ).sort_values("timestamp")
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
        last_amount = float(by_time.loc["15:00", "amount"])
        rows.append({
            "symbol": _TARGET_SYMBOL,
            "trade_date": trade_date.isoformat(),
            "hist60_intraday_return": close_price / open_price - 1.0,
            "hist60_realized_volatility": float(np.sqrt(
                np.square(bar_returns.to_numpy(dtype=float)).sum()
            )),
            "hist60_range": high / low - 1.0,
            "hist60_morning_return": (
                float(by_time.loc["11:30", "close"]) / open_price - 1.0
            ),
            "hist60_afternoon_return": (
                close_price / float(by_time.loc["14:00", "open"]) - 1.0
            ),
            "hist60_last_hour_return": (
                close_price / float(by_time.loc["15:00", "open"]) - 1.0
            ),
            "hist60_last_hour_amount_share": (
                last_amount / total_amount if total_amount > 0 else None
            ),
            "hist60_close_location": (
                (close_price - low) / (high - low)
                if high > low else 0.5
            ),
            "hist60_total_amount": total_amount,
        })
    return pd.DataFrame(rows)


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
        load_byd_historical_intraday()
        if frame is None else frame.copy()
    )
    daily = build_historical_intraday_daily_features(bars)
    if daily.empty:
        daily = pd.DataFrame(columns=["trade_date"])
    daily["trade_date"] = pd.to_datetime(
        daily["trade_date"]
    ).dt.date
    indexed = daily.set_index("trade_date")
    rows = []
    for week in map(str, decision_weeks):
        decision = date.fromisoformat(week)
        expected = qcal.sessions_between(
            Market.A,
            decision - timedelta(days=4),
            decision,
        )
        observed = [value for value in expected if value in indexed.index]
        complete = bool(expected) and len(observed) == len(expected)
        row = {
            "week_ending": week,
            "hist60_available": float(complete),
            "hist60_complete_sessions": len(observed),
            "hist60_expected_sessions": len(expected),
        }
        if complete:
            selected = indexed.loc[observed].copy()
            if isinstance(selected, pd.Series):
                selected = selected.to_frame().T
            morning = _compound(selected["hist60_morning_return"])
            afternoon = _compound(selected["hist60_afternoon_return"])
            last = selected.iloc[-1]
            row.update({
                "hist60_week_intraday_return": _compound(
                    selected["hist60_intraday_return"]
                ),
                "hist60_week_realized_volatility": float(np.sqrt(
                    np.square(pd.to_numeric(
                        selected["hist60_realized_volatility"],
                        errors="coerce",
                    ).to_numpy(dtype=float)).sum()
                )),
                "hist60_week_range": float(np.sqrt(
                    np.square(pd.to_numeric(
                        selected["hist60_range"],
                        errors="coerce",
                    ).to_numpy(dtype=float)).sum()
                )),
                "hist60_last_hour_return": float(
                    last["hist60_last_hour_return"]
                ),
                "hist60_last_hour_amount_share": float(
                    last["hist60_last_hour_amount_share"]
                ),
                "hist60_path_asymmetry": (
                    float(afternoon - morning)
                    if afternoon is not None and morning is not None
                    else None
                ),
                "hist60_close_location": float(
                    last["hist60_close_location"]
                ),
            })
        else:
            row.update({
                "hist60_week_intraday_return": None,
                "hist60_week_realized_volatility": None,
                "hist60_week_range": None,
                "hist60_last_hour_return": None,
                "hist60_last_hour_amount_share": None,
                "hist60_path_asymmetry": None,
                "hist60_close_location": None,
            })
        rows.append(row)
    return pd.DataFrame(rows)
