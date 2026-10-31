"""Fundamentals cache (TTL-based).

Fundamentals are QUARTERLY data — fetching them on every weekly run is wasteful
and, over the flaky eastmoney endpoints, the main source of latency. So we cache
per-symbol fundamentals to disk with a TTL (default 14 days) and keep the live
fetch OFF the weekly critical path:

  - ``load_fresh(ak_symbol, ttl_days)``  -> cached dict or None (instant).
  - ``store(ak_symbol, data)``           -> write cache (called by refresh path).
  - ``refresh_universe(...)``            -> explicit, budgeted bulk refresh used
                                            by the ``refresh_fundamentals`` tool.

weekly_analyze reads the cache only (instant); if an entry is missing/stale the
quality gate emits a best-effort WARN and the factor layer imputes it — never a
fatal failure.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog

from .. import paths
from ..schemas import Market, Universe
from . import akshare_provider as akp

log = structlog.get_logger("qwl.fundamentals")


def _dir() -> Path:
    d = paths.CACHE_DIR / "fundamentals"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _path(ak_symbol: str) -> Path:
    return _dir() / f"{ak_symbol}.json"


def load_fresh(ak_symbol: str, ttl_days: int = 14) -> Optional[dict]:
    """Return cached fundamentals if present and younger than ttl_days, else None."""
    p = _path(ak_symbol)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            blob = json.load(f)
        ts = blob.get("_cached_at", 0)
        if (time.time() - ts) > ttl_days * 86400:
            return None
        return blob.get("data")
    except Exception:  # noqa: BLE001
        return None


def store(ak_symbol: str, data: dict | None) -> None:
    if data is None:
        return
    blob = {"_cached_at": time.time(), "_cached_iso": datetime.now(timezone.utc).isoformat(), "data": data}
    with open(_path(ak_symbol), "w", encoding="utf-8") as f:
        json.dump(blob, f, ensure_ascii=False, indent=2)


def refresh_universe(universe: Universe, budget_s: float = 120.0, force: bool = False, ttl_days: int = 14) -> dict:
    """Fetch + cache fundamentals for all universe members (explicit, budgeted).

    Runs concurrently with a wall-clock budget. Intended to be called
    occasionally (e.g. after earnings season), NOT on every weekly run.

    Returns a summary dict {symbol: "cached"|"fresh"|"failed"}.
    """
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

    summary: dict[str, str] = {}
    todo = []
    for s in universe.stocks:
        if not force and load_fresh(s.ak_symbol, ttl_days) is not None:
            summary[s.symbol] = "fresh"
        else:
            todo.append(s)

    if not todo:
        return summary

    def _fetch(ref):
        if ref.market == Market.A:
            return ref, akp.fetch_a_fundamentals(ref.ak_symbol)
        return ref, akp.fetch_hk_fundamentals(ref.ak_symbol)

    ex = ThreadPoolExecutor(max_workers=6)
    futs = {ex.submit(_fetch, s): s for s in todo}
    deadline = time.time() + budget_s
    pending = set(futs.keys())
    while pending and time.time() < deadline:
        done, pending = wait(pending, timeout=max(0.5, deadline - time.time()), return_when=FIRST_COMPLETED)
        for fut in done:
            ref = futs[fut]
            try:
                _, data = fut.result()
                if data is not None:
                    store(ref.ak_symbol, data)
                    summary[ref.symbol] = "cached"
                else:
                    summary[ref.symbol] = "failed"
            except Exception:  # noqa: BLE001
                summary[ref.symbol] = "failed"
    for fut, ref in futs.items():
        if ref.symbol not in summary:
            summary[ref.symbol] = "timeout"
    ex.shutdown(wait=False, cancel_futures=True)
    from .financial_versions import archive_stock

    for ref in universe.stocks:
        if summary.get(ref.symbol) in {"fresh", "cached"}:
            archive_stock(ref)
    log.info("fundamentals_refresh_done", summary=summary)
    return summary
