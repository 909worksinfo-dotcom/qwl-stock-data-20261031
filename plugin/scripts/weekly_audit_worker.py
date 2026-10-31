"""Run canonical auditors offline against an isolated materialized formal view."""
import json
import socket
import sys
from datetime import date, timedelta
from pathlib import Path

def no_network(*args, **kwargs):
    raise RuntimeError("network forbidden during formal acceptance")

socket.socket.connect = no_network
socket.create_connection = no_network

from qwl import config, paths
from qwl.datasources import extended
from qwl.datasources import calendar as qcal
from qwl.datasources.margin_contract import day as daily_date, FULL_FIELDS, SSE_FIELDS
from qwl.datasources.controlled_benchmarks import audit_benchmarks
import pandas as pd
import numpy as np

as_of = date.fromisoformat(sys.argv[1])
start = as_of - timedelta(days=as_of.weekday())
universe = config.load_extended_universe()
coverage = extended.audit_data_coverage(universe, as_of_date=as_of)
issues = []
margin_pending = []
for stock in universe.stocks:
    safe = stock.symbol.replace(".", "_")
    # Canonical calendar includes market holidays; native listing/suspension
    # handling determines the last expected session.
    expected = set()
    day = start
    last = extended._expected_symbol_session(stock, as_of)
    while day <= min(last, as_of):
        if extended._expected_symbol_session(stock, day) == day:
            expected.add(day.isoformat())
        day += timedelta(days=1)
    for suffix in ("", "_raw"):
        f = paths.STATE_DIR / "history" / f"{safe}{suffix}.parquet"
        try:
            frame = pd.read_parquet(f)
            idx = pd.DatetimeIndex(pd.to_datetime(frame.index)).normalize()
            current = frame[(idx.date >= start) & (idx.date <= as_of)]
            found = {d.date().isoformat() for d in idx}
            missing = expected - found
            if missing:
                issues.extend({"domain": "history", "symbol": stock.symbol, "date": d,
                               "reason": "missing_session", "blocking": False} for d in sorted(missing))
            for _, row in current.iterrows():
                numbers = pd.to_numeric(row.reindex(["open", "high", "low", "close", "volume"]), errors="coerce")
                if (not np.isfinite(numbers.to_numpy(dtype=float)).all() or (numbers.iloc[:4] <= 0).any()
                        or numbers["volume"] < 0 or numbers["low"] > min(numbers["open"], numbers["close"])
                        or numbers["high"] < max(numbers["open"], numbers["close"])):
                    raise ValueError("invalid OHLCV")
        except Exception as exc:
            issues.append({"domain": "history", "symbol": stock.symbol,
                           "reason": "invalid_formal_history", "blocking": True, "details": str(exc)})
    for domain, field in (("microstructure", "records"), ("margin", "records"),
                          ("fund_flow_ths", "daily_history"), ("southbound", "records")):
        item = coverage.get("freshness", {}).get(domain, {}).get("items", {}).get(stock.symbol)
        if not item or item.get("status") == "exempt":
            continue
        f = paths.CACHE_DIR / domain / f"{stock.ak_symbol}.json"
        try:
            doc = json.loads(f.read_text())
            found = {daily_date(r.get("date", r.get("trade_date", ""))) for r in doc.get(field, [])}
            if domain == "margin":
                for row in doc.get(field, []):
                    observed = daily_date(row.get("date"))
                    if not start.isoformat() <= observed <= as_of.isoformat():
                        continue
                    missing_fields = sorted(FULL_FIELDS - row.keys())
                    if missing_fields:
                        issues.append({"domain": domain, "symbol": stock.symbol, "date": observed,
                                       "reason": "missing_original_margin_fields", "fields": missing_fields, "blocking": False})
                    for key in FULL_FIELDS & row.keys():
                        value = row.get(key)
                        if value is None and row.get(key + "_status"):
                            continue
                        if value is None or isinstance(value, bool) or not np.isfinite(float(value)):
                            raise ValueError(f"invalid required margin value: {observed}:{key}")
            (margin_pending if domain == "margin" else issues).extend(
                {"domain": domain, "symbol": stock.symbol, "date": d,
                 "reason": "missing_session", "blocking": False}
                for d in sorted(expected - found))
        except FileNotFoundError as exc:
            finding = {"domain": domain, "symbol": stock.symbol,
                       "reason": "missing_formal_json", "blocking": False,
                       "details": str(exc)}
            (margin_pending if domain == "margin" else issues).append(finding)
        except Exception as exc:
            issues.append({"domain": domain, "symbol": stock.symbol,
                           "reason": "invalid_formal_json", "blocking": True, "details": str(exc)})
    if stock.symbol in {"002594.SZ", "300866.SZ"}:
        folder = "byd_historical_intraday" if stock.symbol == "002594.SZ" else "anker_historical_intraday"
        f = paths.CACHE_DIR / folder / f"{stock.ak_symbol}_60m.parquet"
        try:
            intraday = pd.read_parquet(f)
            stamps = pd.to_datetime(intraday["timestamp"], utc=True).dt.tz_convert("Asia/Shanghai")
            slots = set(stamps.dt.strftime("%Y-%m-%d %H:%M"))
            if stamps.duplicated().any():
                raise ValueError("duplicate intraday timestamp")
            for d in sorted(expected):
                missing = [t for t in ("10:30", "11:30", "14:00", "15:00") if f"{d} {t}" not in slots]
                if missing:
                    issues.append({"domain": "specialized", "symbol": stock.symbol, "date": d,
                                   "reason": "missing_intraday_bars", "slots": missing, "blocking": False})
            current = intraday[(stamps.dt.date >= start) & (stamps.dt.date <= as_of)]
            numbers = current[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
            if (not np.isfinite(numbers.to_numpy(dtype=float)).all()
                    or (numbers[["open", "high", "low", "close"]] <= 0).any().any()
                    or (numbers["volume"] < 0).any()
                    or (numbers["low"] > numbers[["open", "close"]].min(axis=1)).any()
                    or (numbers["high"] < numbers[["open", "close"]].max(axis=1)).any()):
                raise ValueError("invalid intraday OHLCV")
        except Exception as exc:
            issues.append({"domain": "specialized", "symbol": stock.symbol,
                           "reason": "invalid_intraday", "blocking": True, "details": str(exc)})
issues.extend(audit_benchmarks(universe, as_of.isoformat(), paths.ROOT))
(paths.ROOT / "weekly_acceptance.json").write_text(json.dumps(
    {"coverage": coverage, "weekly_issues": issues, "margin_pending": margin_pending},
    ensure_ascii=False, default=str))
