"""Free forward-only minute capture for BYD within the fixed 60."""

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


_SYMBOLS = {"sz002594": "002594.SZ"}
_ROOT = paths.STATE_DIR / "forecast_forward"
_MINUTE_PATH = _ROOT / "minute_bars.parquet"
_FLOW_PATH = _ROOT / "order_flow_snapshots.parquet"
_DAILY_FEATURE_PATH = _ROOT / "daily_features.parquet"
_WEEKLY_FEATURE_PATH = _ROOT / "weekly_features.parquet"


def parse_tencent_minute_payload(
    payload: dict,
    source_code: str,
) -> pd.DataFrame:
    if source_code not in _SYMBOLS:
        raise ValueError(f"forecast symbol is not allowed: {source_code}")
    try:
        data = payload["data"][source_code]["data"]
        observed_date = str(data["date"])
        rows = list(data["data"])
    except (KeyError, TypeError) as exc:
        raise ValueError("invalid Tencent minute payload") from exc
    output = []
    for raw in rows:
        fields = str(raw).split()
        if len(fields) < 4:
            continue
        time_value, price, cumulative_volume, cumulative_amount = fields[:4]
        if len(time_value) != 4 or len(observed_date) != 8:
            continue
        timestamp = pd.Timestamp(
            f"{observed_date[:4]}-{observed_date[4:6]}-"
            f"{observed_date[6:]} {time_value[:2]}:{time_value[2:]}",
            tz="Asia/Shanghai",
        )
        output.append({
            "symbol": _SYMBOLS[source_code],
            "timestamp": timestamp,
            "price": float(price),
            "cumulative_volume": float(cumulative_volume),
            "cumulative_amount": float(cumulative_amount),
            "source": "Tencent minute query",
        })
    frame = pd.DataFrame(output)
    if frame.empty:
        return frame
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    frame["minute_volume"] = frame["cumulative_volume"].diff().fillna(
        frame["cumulative_volume"]
    )
    frame["minute_amount"] = frame["cumulative_amount"].diff().fillna(
        frame["cumulative_amount"]
    )
    if (
        (frame["minute_volume"] < 0).any()
        or (frame["minute_amount"] < 0).any()
    ):
        raise ValueError("Tencent minute cumulative fields are not monotonic")
    return frame


def parse_tencent_flow_response(text: str) -> dict:
    prefix = "v_s_pksz002594="
    row = next(
        (
            line for line in str(text).splitlines()
            if line.startswith(prefix)
        ),
        None,
    )
    if row is None:
        raise ValueError("Tencent A-share flow snapshot is missing")
    raw = row.split("=", 1)[1].strip().strip('";')
    values = [float(value) for value in raw.split("~")]
    if len(values) != 4:
        raise ValueError("Tencent flow snapshot must have four values")
    return {
        "symbol": "002594.SZ",
        "metric_0": values[0],
        "metric_1": values[1],
        "metric_2": values[2],
        "metric_3": values[3],
        "raw_response": row,
        "source": "Tencent s_pk snapshot",
    }


def _append_parquet(
    path,
    frame: pd.DataFrame,
    natural_key: list[str],
) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        prior = pd.read_parquet(path)
        frame = pd.concat([prior, frame], ignore_index=True)
    frame = frame.drop_duplicates(natural_key, keep="last").sort_values(
        natural_key
    )
    temporary = path.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)
    return len(frame)


def aggregate_complete_a_share_day(frame: pd.DataFrame) -> dict:
    """Aggregate one complete continuous-auction day; exclude post-close rows."""
    symbols = set(frame["symbol"].astype(str))
    if symbols != {"002594.SZ"}:
        raise ValueError("daily aggregation is restricted to BYD in fixed 60")
    working = frame.copy().sort_values("timestamp")
    local = pd.to_datetime(working["timestamp"], utc=True).dt.tz_convert(
        "Asia/Shanghai"
    )
    minute_of_day = local.dt.hour * 60 + local.dt.minute
    session = (
        ((minute_of_day >= 9 * 60 + 30) & (minute_of_day <= 11 * 60 + 30))
        | ((minute_of_day >= 13 * 60) & (minute_of_day <= 15 * 60))
    )
    working = working.loc[session].copy()
    local = local.loc[session]
    unique_minutes = int(local.dt.strftime("%H:%M").nunique())
    has_close = bool((local.dt.strftime("%H:%M") == "15:00").any())
    if unique_minutes < 235 or not has_close:
        raise ValueError("incomplete A-share continuous-auction day")
    working["local_timestamp"] = local
    working = working.drop_duplicates("local_timestamp", keep="last")
    open_price = float(working.iloc[0]["price"])
    close_row = working[
        working["local_timestamp"].dt.strftime("%H:%M") == "15:00"
    ].iloc[-1]
    close_price = float(close_row["price"])
    high = float(working["price"].max())
    low = float(working["price"].min())
    amount = pd.to_numeric(working["minute_amount"], errors="coerce").clip(
        lower=0.0
    )
    volume_lots = pd.to_numeric(
        working["minute_volume"], errors="coerce"
    ).clip(lower=0.0)
    total_amount = float(amount.sum())
    total_shares = float(volume_lots.sum() * 100.0)
    vwap = (
        total_amount / total_shares
        if total_shares > 0 else float("nan")
    )
    price = pd.to_numeric(working["price"], errors="coerce")
    log_returns = np.log(price / price.shift(1)).replace(
        [np.inf, -np.inf], np.nan
    ).dropna()
    last_30 = working[
        working["local_timestamp"].dt.hour * 60
        + working["local_timestamp"].dt.minute >= 14 * 60 + 30
    ]
    morning = working[
        working["local_timestamp"].dt.hour * 60
        + working["local_timestamp"].dt.minute <= 11 * 60 + 30
    ]
    afternoon = working[
        working["local_timestamp"].dt.hour * 60
        + working["local_timestamp"].dt.minute >= 13 * 60
    ]
    close_location = (
        (close_price - low) / (high - low)
        if high > low else 0.5
    )
    return {
        "symbol": "002594.SZ",
        "trade_date": local.iloc[0].date().isoformat(),
        "complete_session": True,
        "minute_count": unique_minutes,
        "open": open_price,
        "close": close_price,
        "intraday_return": close_price / open_price - 1.0,
        "high_low_range": high / low - 1.0 if low > 0 else None,
        "vwap": vwap,
        "close_vwap_gap": (
            close_price / vwap - 1.0
            if np.isfinite(vwap) and vwap > 0 else None
        ),
        "realized_volatility": float(np.sqrt(np.square(log_returns).sum())),
        "last_30m_return": (
            float(last_30.iloc[-1]["price"])
            / float(last_30.iloc[0]["price"])
            - 1.0
        ),
        "last_30m_amount_share": (
            float(last_30["minute_amount"].sum()) / total_amount
            if total_amount > 0 else None
        ),
        "morning_return": (
            float(morning.iloc[-1]["price"])
            / float(morning.iloc[0]["price"])
            - 1.0
        ),
        "afternoon_return": (
            float(afternoon.iloc[-1]["price"])
            / float(afternoon.iloc[0]["price"])
            - 1.0
        ),
        "close_location": close_location,
        "total_amount": total_amount,
        "captured_at": working["captured_at"].max(),
        "source": "Tencent minute query",
    }


def rebuild_forward_daily_features() -> dict:
    if not _MINUTE_PATH.exists():
        return {"status": "NO_MINUTE_DATA", "daily_rows": 0}
    minute = pd.read_parquet(_MINUTE_PATH)
    rows = []
    rejected = []
    local_dates = pd.to_datetime(
        minute["timestamp"], utc=True
    ).dt.tz_convert("Asia/Shanghai").dt.date
    for (symbol, trade_date), indices in minute.groupby(
        [minute["symbol"], local_dates]
    ).groups.items():
        selected = minute.loc[indices].copy()
        try:
            rows.append(aggregate_complete_a_share_day(selected))
        except ValueError as exc:
            rejected.append({
                "symbol": str(symbol),
                "trade_date": str(trade_date),
                "reason": str(exc),
            })
    daily = pd.DataFrame(rows)
    if not daily.empty:
        _append_parquet(
            _DAILY_FEATURE_PATH, daily, ["symbol", "trade_date"]
        )
    return {
        "status": "OK" if rows else "NO_COMPLETE_DAYS",
        "daily_rows_built": len(rows),
        "rejected": rejected,
        "path": str(_DAILY_FEATURE_PATH),
    }


def _compound(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(np.prod(1.0 + numeric.to_numpy(dtype=float)) - 1.0)


def aggregate_complete_a_share_week(
    frame: pd.DataFrame,
    decision_week: date,
) -> dict:
    """Aggregate a week only when every scheduled XSHG session was captured."""
    symbols = set(frame["symbol"].astype(str))
    if symbols != {"002594.SZ"}:
        raise ValueError("weekly aggregation is restricted to BYD in fixed 60")
    if decision_week.weekday() != 4:
        raise ValueError("decision_week must be a Friday anchor")
    expected = qcal.sessions_between(
        Market.A,
        decision_week - timedelta(days=4),
        decision_week,
    )
    working = frame.copy()
    working["trade_date"] = pd.to_datetime(
        working["trade_date"]
    ).dt.date
    working = working.sort_values("trade_date").drop_duplicates(
        "trade_date",
        keep="last",
    )
    observed = set(working["trade_date"])
    expected_set = set(expected)
    if not expected or observed != expected_set:
        missing = sorted(value.isoformat() for value in expected_set - observed)
        extra = sorted(value.isoformat() for value in observed - expected_set)
        raise ValueError(
            "incomplete A-share week"
            f"; missing={','.join(missing)}; extra={','.join(extra)}"
        )
    total_amount = pd.to_numeric(
        working["total_amount"],
        errors="coerce",
    ).clip(lower=0.0)
    late_share = pd.to_numeric(
        working["last_30m_amount_share"],
        errors="coerce",
    )
    valid_late = total_amount.notna() & late_share.notna()
    weekly_late_share = (
        float(np.average(
            late_share[valid_late].to_numpy(dtype=float),
            weights=total_amount[valid_late].to_numpy(dtype=float),
        ))
        if valid_late.any()
        and float(total_amount[valid_late].sum()) > 0
        else None
    )
    morning = _compound(working["morning_return"])
    afternoon = _compound(working["afternoon_return"])
    last = working.iloc[-1]
    last_30m = float(last["last_30m_return"])
    close_vwap = float(last["close_vwap_gap"])
    realized = pd.to_numeric(
        working["realized_volatility"],
        errors="coerce",
    ).dropna()
    ranges = pd.to_numeric(
        working["high_low_range"],
        errors="coerce",
    ).dropna()
    return {
        "symbol": "002594.SZ",
        "decision_week": decision_week.isoformat(),
        "first_session": expected[0].isoformat(),
        "last_session": expected[-1].isoformat(),
        "complete_session_count": len(expected),
        "weekly_intraday_return": _compound(working["intraday_return"]),
        "weekly_realized_volatility": (
            float(np.sqrt(np.square(realized).sum()))
            if len(realized) else None
        ),
        "weekly_range_pressure": (
            float(np.sqrt(np.square(ranges).sum()))
            if len(ranges) else None
        ),
        "weekly_close_pressure": 0.5 * (last_30m + close_vwap),
        "weekly_path_asymmetry": (
            float(afternoon - morning)
            if afternoon is not None and morning is not None else None
        ),
        "weekly_late_amount_share": weekly_late_share,
        "weekly_close_location": float(last["close_location"]),
        "weekly_total_amount": float(total_amount.sum()),
        "captured_at": working["captured_at"].max(),
        "source": "Tencent minute query; XSHG-complete week",
    }


def rebuild_forward_weekly_features() -> dict:
    if not _DAILY_FEATURE_PATH.exists():
        return {"status": "NO_DAILY_FEATURES", "weekly_rows": 0}
    daily = pd.read_parquet(_DAILY_FEATURE_PATH)
    if daily.empty:
        return {"status": "NO_DAILY_FEATURES", "weekly_rows": 0}
    dates = pd.to_datetime(daily["trade_date"])
    periods = dates.dt.to_period("W-FRI")
    rows = []
    rejected = []
    for period, indices in daily.groupby(periods).groups.items():
        selected = daily.loc[indices].copy()
        decision_week = period.end_time.date()
        try:
            rows.append(aggregate_complete_a_share_week(
                selected,
                decision_week,
            ))
        except ValueError as exc:
            rejected.append({
                "decision_week": decision_week.isoformat(),
                "reason": str(exc),
            })
    weekly = pd.DataFrame(rows)
    if not weekly.empty:
        _append_parquet(
            _WEEKLY_FEATURE_PATH,
            weekly,
            ["symbol", "decision_week"],
        )
    return {
        "status": "OK" if rows else "NO_COMPLETE_WEEKS",
        "weekly_rows_built": len(rows),
        "rejected": rejected,
        "path": str(_WEEKLY_FEATURE_PATH),
    }


def forward_feature_status() -> dict:
    daily = (
        pd.read_parquet(_DAILY_FEATURE_PATH)
        if _DAILY_FEATURE_PATH.exists() else pd.DataFrame()
    )
    if daily.empty:
        return {
            "status": "NOT_READY",
            "complete_days": 0,
            "complete_weeks": 0,
        }
    rebuild = rebuild_forward_weekly_features()
    weekly = (
        pd.read_parquet(_WEEKLY_FEATURE_PATH)
        if _WEEKLY_FEATURE_PATH.exists() else pd.DataFrame()
    )
    complete_weeks = len(weekly)
    return {
        "status": "READY_FOR_FEATURE_RESEARCH" if complete_weeks else "CAPTURING",
        "complete_days": len(daily),
        "first_day": str(daily["trade_date"].min()),
        "last_day": str(daily["trade_date"].max()),
        "complete_weeks": complete_weeks,
        "first_week": (
            str(weekly["decision_week"].min())
            if complete_weeks else None
        ),
        "last_week": (
            str(weekly["decision_week"].max())
            if complete_weeks else None
        ),
        "weekly_rebuild": rebuild,
        "required_review_weeks": [13, 26, 52],
    }


def capture_byd_forward_intraday() -> dict:
    """Capture only today's observations; this function never backfills."""
    fixed_sixty = {
        stock.symbol for stock in config.load_extended_universe().stocks
    }
    leaked = sorted(set(_SYMBOLS.values()) - fixed_sixty)
    if leaked:
        raise ValueError(
            "forward capture contains symbols outside fixed 60: "
            + ",".join(leaked)
        )
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    captured_at = datetime.now(timezone.utc)
    minute_frames = []
    source_hashes = {}
    for source_code in _SYMBOLS:
        response = session.get(
            "https://web.ifzq.gtimg.cn/appstock/app/minute/query",
            params={"code": source_code},
            timeout=30,
        )
        response.raise_for_status()
        source_hashes[source_code] = hashlib.sha256(
            response.content
        ).hexdigest()
        payload = response.json()
        frame = parse_tencent_minute_payload(payload, source_code)
        if frame.empty:
            continue
        frame["captured_at"] = captured_at
        frame["source_sha256"] = source_hashes[source_code]
        minute_frames.append(frame)
    if not minute_frames:
        return {
            "status": "NO_DATA",
            "captured_at": captured_at.isoformat(),
        }
    minute = pd.concat(minute_frames, ignore_index=True)
    minute_rows = _append_parquet(
        _MINUTE_PATH, minute, ["symbol", "timestamp"]
    )

    flow_error = None
    flow_rows = (
        len(pd.read_parquet(_FLOW_PATH))
        if _FLOW_PATH.exists() else 0
    )
    try:
        flow_response = session.get(
            "https://qt.gtimg.cn/q=s_pksz002594",
            timeout=30,
        )
        flow_response.raise_for_status()
        flow = parse_tencent_flow_response(flow_response.text)
        flow.update({
            "captured_at": captured_at,
            "source_sha256": hashlib.sha256(
                flow_response.content
            ).hexdigest(),
        })
        flow_frame = pd.DataFrame([flow])
        flow_rows = _append_parquet(
            _FLOW_PATH, flow_frame, ["symbol", "captured_at"]
        )
    except (requests.RequestException, ValueError) as exc:
        flow_error = f"{type(exc).__name__}: {str(exc)[:240]}"
    daily_features = rebuild_forward_daily_features()
    weekly_features = rebuild_forward_weekly_features()
    manifest = {
        "captured_at": captured_at.isoformat(),
        "minute_file": str(_MINUTE_PATH),
        "minute_rows_total": minute_rows,
        "minute_rows_captured": len(minute),
        "minute_symbols": sorted(minute["symbol"].unique()),
        "flow_file": str(_FLOW_PATH),
        "flow_rows_total": flow_rows,
        "flow_error": flow_error,
        "source_hashes": source_hashes,
    }
    (_ROOT / "latest_capture.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "status": "PARTIAL" if flow_error else "OK",
        **manifest,
        "daily_features": daily_features,
        "weekly_features": weekly_features,
    }


def forward_capture_status() -> dict:
    minute = (
        pd.read_parquet(_MINUTE_PATH)
        if _MINUTE_PATH.exists() else pd.DataFrame()
    )
    flow = (
        pd.read_parquet(_FLOW_PATH)
        if _FLOW_PATH.exists() else pd.DataFrame()
    )
    return {
        "status": "CAPTURING" if not minute.empty else "NOT_STARTED",
        "minute_rows": len(minute),
        "minute_symbols": (
            sorted(minute["symbol"].astype(str).unique())
            if not minute.empty else []
        ),
        "first_minute": (
            str(minute["timestamp"].min()) if not minute.empty else None
        ),
        "last_minute": (
            str(minute["timestamp"].max()) if not minute.empty else None
        ),
        "flow_snapshots": len(flow),
        "historical_confirmation_allowed": False,
        "counts_only_as_forward_data": True,
        "feature_status": forward_feature_status(),
    }
