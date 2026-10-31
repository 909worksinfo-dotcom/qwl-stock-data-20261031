"""Collection pipeline (M1 capstone).

Symbol-scoped collection with strict fallback + hard quality gate + immutable
Parquet snapshot. One weekly run costs ≈ 1 batch snapshot call + 12 K-line
calls + best-effort per-symbol fundamentals/flow — comfortably inside the
anti-ban safe zone.

Fallback chains (validated by live probing 2026-08):
  OHLCV:
    A-share : tencent_kline (web.ifzq) -> akshare stock_zh_a_hist_tx
    HK      : tencent_kline (web.ifzq) -> akshare stock_hk_hist (eastmoney)
  Snapshot:
    ALL     : tencent qt.gtimg batch (single request for all 12)
  Fundamentals / flow: akshare per-symbol (best-effort; guarded, never blocks).
"""

from __future__ import annotations

import json
import hashlib
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import structlog

from .. import paths
from ..schemas import Market, StockRef, Universe
from . import akshare_provider as akp
from . import calendar as qcal
from . import evidence
from . import fundamentals_cache
from . import quality
from . import tencent, tencent_kline

log = structlog.get_logger("qwl.collect")


@dataclass
class SymbolData:
    ref: StockRef
    ohlcv: pd.DataFrame
    ohlcv_source: str
    snapshot: dict | None
    fundamentals: dict | None
    fund_flow: list | dict | None
    week: dict  # WeekWindow.as_dict()
    evidence: dict = field(default_factory=dict)
    standardized_ohlcv: pd.DataFrame | None = None
    standardized_source: str = "none"


@dataclass
class DataBundle:
    run_id: str
    as_of: str
    universe_version: str
    symbols: dict[str, SymbolData] = field(default_factory=dict)
    gate: dict | None = None
    snapshot_dir: str | None = None


def _market_prefix(ref: StockRef) -> str:
    """akshare stock_individual_fund_flow needs 'sh'/'sz'."""
    return ref.tencent_code[:2]


def _collect_ohlcv(ref: StockRef, count: int, start: str, end: str,
                   adjust: str = "none") -> tuple[pd.DataFrame, str]:
    """Try primary Tencent K-line, then market-specific akshare fallback."""
    cutoff = pd.Timestamp(end)
    if adjust not in {"none", "qfq"}:
        raise ValueError('unsupported price basis')

    def bounded(frame: pd.DataFrame) -> pd.DataFrame:
        # Cropping a qfq response cannot undo a later corporate-action rebase
        if adjust == 'qfq' and pd.to_datetime(frame.index).max() > cutoff:
            raise ValueError('qfq adjustment anchor exceeds requested as_of')
        result = frame.loc[frame.index <= cutoff].copy()
        result.attrs['price_basis'] = adjust
        return result

    # Primary: Tencent web fqkline (works for both markets)
    try:
        kwargs = {"strict_adjustment": True} if adjust == 'qfq' else {}
        df = tencent_kline.fetch_daily(ref.tencent_code, count=count, adjust=adjust, **kwargs)
        if df is not None and not df.empty:
            return bounded(df), "tencent_kline_qfq" if adjust == 'qfq' else "tencent_kline_raw"
    except (requests.RequestException, ValueError) as e:
        log.warning("ohlcv_primary_failed", symbol=ref.symbol, err=str(e)[:80])

    if adjust == 'qfq':
        # These APIs receive an explicit qfq request. Accept their response
        # only when the provider adapter preserves that basis marker; an
        # unadjusted or ambiguous response remains a hard failure.
        if ref.market == Market.A:
            df = akp.fetch_a_daily_tx(ref.tencent_code, start=start, end=end, adjust='qfq')
            source = "akshare_tx_qfq"
        else:
            df = akp.fetch_hk_daily(ref.ak_symbol, start=start, end=end, adjust='qfq')
            source = "akshare_hk_qfq"
            if (df is None or df.empty
                    or df.attrs.get('price_basis') != 'qfq'):
                df = akp.fetch_hk_daily_sina(
                    ref.ak_symbol, start=start, end=end, adjust='qfq')
                source = "akshare_hk_sina_qfq"
        if (df is not None and not df.empty
                and df.attrs.get('price_basis') == 'qfq'):
            return bounded(df), source
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"]), "none"
    # Raw fallback
    if ref.market == Market.A:
        df = akp.fetch_a_daily_tx(ref.tencent_code, start=start, end=end, adjust="")
        if df is not None and not df.empty:
            return bounded(df), "akshare_tx_raw"
    else:
        df = akp.fetch_hk_daily(ref.ak_symbol, start=start, end=end, adjust="")
        if df is not None and not df.empty:
            return bounded(df), "akshare_hk_raw"

    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"]), "none"


def collect(
    universe: Universe,
    as_of: date,
    ohlcv_count: int = 320,
    polite_sleep: tuple[float, float] = (0.4, 0.9),
    best_effort: bool = True,
    best_effort_budget_s: float = 20.0,
    fundamentals_ttl_days: int = 14,
    include_standardized: bool = False,
    series_cache: Path | None = None,
) -> DataBundle:
    """Run a full symbol-scoped collection for ``as_of``.

    Raises nothing for network issues; instead records gate issues. The caller
    (weekly_analyze) inspects ``bundle.gate['passed']`` and refuses to predict
    if fatal issues exist.

    Best-effort fundamentals/flow (unreliable eastmoney) are fetched CONCURRENTLY
    with a total wall-clock ``best_effort_budget_s`` deadline, so a few slow
    endpoints can't stall the whole run. Anything not back in time is simply
    absent (a warn, never fatal).
    """
    import random
    from concurrent.futures import ThreadPoolExecutor, as_completed

    run_id = uuid.uuid4().hex
    log.info("collect_start", run_id=run_id, as_of=as_of.isoformat(), n=len(universe.stocks))

    # Snapshot: ONE batch request for all 12 symbols.
    sess = requests.Session()
    snapshots: dict = {}
    try:
        snapshots = tencent.fetch_snapshots([s.tencent_code for s in universe.stocks], session=sess)
    except requests.RequestException as e:
        log.warning("snapshot_batch_failed", err=str(e)[:100])

    all_issues: list = []
    bundle = DataBundle(run_id=run_id, as_of=as_of.isoformat(), universe_version=universe.universe_version)

    # --- Core path: OHLCV per symbol (fast, reliable Tencent primary) ---
    ohlcv_by_sym: dict[str, tuple] = {}
    standardized_by_sym: dict[str, tuple] = {}
    if series_cache is not None:
        series_cache = Path(series_cache).resolve()
        expected = paths.ROOT.resolve() / 'state/snapshots' / as_of.isoformat() / 'series'
        if series_cache != expected or not paths.ROOT.name.startswith('qwl-item-'):
            raise ValueError('SERIES_INTEGRITY: non-private cache rejected')
    def one(ref, start, end, basis):
        fetch = lambda: _collect_ohlcv(ref, ohlcv_count, start, end, adjust=basis)
        if series_cache is None:
            return fetch()
        from .weekly_series import collect as cached_collect
        signature = hashlib.sha256(b''.join(Path(__file__).with_name(name).read_bytes() for name in
            ('collect.py','weekly_series.py','tencent_kline.py','akshare_provider.py'))).hexdigest()
        return cached_collect(series_cache, ref, as_of, basis,
            signature, fetch)
    for ref in universe.stocks:
        window = qcal.resolve_week(ref.market, as_of)
        start = window.lookback_start.strftime("%Y%m%d")
        end = as_of.strftime("%Y%m%d")
        ohlcv, src = one(ref, start, end, 'none')
        ohlcv_by_sym[ref.symbol] = (ohlcv, src, window)
        all_issues += quality.check_ohlcv(ref.symbol, ref.market, ohlcv, as_of, src)
        if include_standardized:
            adjusted, adjusted_source = one(ref, start, end, 'qfq')
            standardized_by_sym[ref.symbol] = (adjusted, adjusted_source)
            all_issues += quality.check_ohlcv(ref.symbol, ref.market, adjusted, as_of, adjusted_source)
        snap = snapshots.get(ref.tencent_code)
        all_issues += quality.check_snapshot(ref.symbol, snap is not None, "tencent")
        time.sleep(random.uniform(*polite_sleep))

    # --- Best-effort path ---
    # Fundamentals and flow evidence are read from the local PIT caches. Network
    # refreshes stay outside the weekly critical path.
    funds_by_sym: dict[str, dict | None] = {s.symbol: None for s in universe.stocks}
    flow_by_sym: dict[str, dict | None] = {s.symbol: None for s in universe.stocks}
    evidence_by_sym: dict[str, dict] = {s.symbol: {} for s in universe.stocks}
    if best_effort:
        for ref in universe.stocks:
            funds_by_sym[ref.symbol] = evidence.load_fundamentals(ref)
            evidence_by_sym[ref.symbol] = evidence.symbol_evidence(ref, as_of)
            flow_by_sym[ref.symbol] = evidence_by_sym[ref.symbol].get("fund_flow")

    # --- Assemble bundle + best-effort gate warnings ---
    for ref in universe.stocks:
        ohlcv, src, window = ohlcv_by_sym[ref.symbol]
        snap = snapshots.get(ref.tencent_code)
        snap_dict = snap.as_dict() if snap is not None else None
        funds = funds_by_sym[ref.symbol]
        flow = flow_by_sym[ref.symbol]
        if best_effort:
            all_issues += quality.check_best_effort(ref.symbol, "fundamentals", funds is not None, "akshare")
            if ref.market == Market.A:
                all_issues += quality.check_best_effort(ref.symbol, "fund_flow", flow is not None, "akshare")
        bundle.symbols[ref.symbol] = SymbolData(
            ref=ref, ohlcv=ohlcv, ohlcv_source=src, snapshot=snap_dict,
            fundamentals=funds, fund_flow=flow, week=window.as_dict(),
            evidence=evidence_by_sym[ref.symbol],
            standardized_ohlcv=standardized_by_sym.get(ref.symbol, (None, 'none'))[0],
            standardized_source=standardized_by_sym.get(ref.symbol, (None, 'none'))[1],
        )
        log.info("collected_symbol", symbol=ref.symbol, ohlcv_rows=len(ohlcv), ohlcv_src=src,
                 has_snap=snap_dict is not None, has_funds=funds is not None)

    if any(ref.market == Market.HK for ref in universe.stocks):
        expected_fx_on = qcal.latest_expected_session(Market.HK, as_of)
        all_issues += quality.check_hkd_cny(
            evidence.load_hkd_cny_rate(expected_fx_on), expected_fx_on
        )

    gate = quality.finalize(all_issues)
    bundle.gate = gate.as_dict()
    log.info("collect_gate", passed=gate.passed, fatal=len(gate.fatals), warn=len(gate.warnings))
    return bundle


def persist_snapshot(bundle: DataBundle) -> Path:
    """Persist the bundle as an immutable snapshot under state/snapshots/<as_of>/<run_id>/.

    OHLCV -> one parquet per symbol; meta/gate/snapshots -> JSON. Reproducible:
    re-running analysis on this snapshot yields identical model inputs.
    """
    paths.ensure_state_dirs()
    out = paths.SNAPSHOTS_DIR / bundle.as_of / bundle.run_id
    out.mkdir(parents=True, exist_ok=False)
    (out / "ohlcv").mkdir()
    (out / "ohlcv_standardized").mkdir()

    meta = {
        "run_id": bundle.run_id,
        "as_of": bundle.as_of,
        "universe_version": bundle.universe_version,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "gate": bundle.gate,
        "symbols": {},
    }
    for sym, sd in bundle.symbols.items():
        safe = sym.replace(".", "_")
        if sd.ohlcv is not None and not sd.ohlcv.empty:
            sd.ohlcv.to_parquet(out / "ohlcv" / f"{safe}.parquet")
        if sd.standardized_ohlcv is not None and not sd.standardized_ohlcv.empty:
            sd.standardized_ohlcv.to_parquet(out / "ohlcv_standardized" / f"{safe}.parquet")
        meta["symbols"][sym] = {
            "name": sd.ref.name,
            "market": sd.ref.market.value,
            "ohlcv_source": sd.ohlcv_source,
            "ohlcv_rows": int(len(sd.ohlcv)) if sd.ohlcv is not None else 0,
            "ohlcv_basis": "none",
            "standardized_source": sd.standardized_source,
            "standardized_basis": (
                sd.standardized_ohlcv.attrs.get("price_basis")
                if sd.standardized_ohlcv is not None else None
            ),
            "standardized_anchor_date": (sd.standardized_ohlcv.index.max().date().isoformat()
                                          if sd.standardized_ohlcv is not None and not sd.standardized_ohlcv.empty else None),
            "snapshot": sd.snapshot,
            "fundamentals": sd.fundamentals,
            "fund_flow": sd.fund_flow,
            "week": sd.week,
            "evidence": sd.evidence,
        }

    with open(out / "meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2, default=str)

    validation = validate_snapshot_dir(out, persist=True)
    from .. import audit
    audit.record_snapshot_validation(validation)

    bundle.snapshot_dir = str(out)
    log.info(
        "snapshot_persisted",
        dir=str(out),
        replay_status=validation["status"],
    )
    return out


def _append_only_history_file(
    path: Path,
    incoming: pd.DataFrame,
) -> dict:
    """Append unseen sessions while preserving every existing observation.

    Provider revisions on an already stored date are reported as conflicts and
    never overwrite the historical value. This makes a refresh safe for PIT
    research while still allowing new sessions to reach the canonical history.
    """
    columns = [
        column
        for column in ("open", "high", "low", "close", "volume")
        if column in incoming.columns
    ]
    incoming = incoming.loc[:, columns].copy().sort_index()
    incoming.index = pd.to_datetime(incoming.index).normalize()
    incoming = incoming[~incoming.index.duplicated(keep="last")]

    if path.exists():
        existing = pd.read_parquet(path).sort_index()
        existing.index = pd.to_datetime(existing.index).normalize()
        existing = existing[~existing.index.duplicated(keep="first")]
    else:
        existing = pd.DataFrame(columns=columns)

    common_columns = [
        column for column in columns if column in existing.columns
    ]
    overlap = existing.index.intersection(incoming.index)
    conflicts = []
    for observed in overlap:
        changed = []
        for column in common_columns:
            old = existing.at[observed, column]
            new = incoming.at[observed, column]
            if pd.isna(old) and pd.isna(new):
                continue
            try:
                equal = math.isclose(
                    float(old), float(new), rel_tol=1e-9, abs_tol=1e-8
                )
            except (TypeError, ValueError):
                equal = old == new
            if not equal:
                changed.append(column)
        if changed:
            conflicts.append({
                "date": observed.date().isoformat(),
                "fields": changed,
            })

    additions = incoming.loc[~incoming.index.isin(existing.index)]
    if additions.empty and path.exists():
        return {
            "appended_rows": 0,
            "conflict_rows": len(conflicts),
            "conflicts": conflicts,
            "latest_date": (
                existing.index.max().date().isoformat()
                if not existing.empty else None
            ),
        }

    merged = pd.concat([existing, additions]).sort_index()
    temporary = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(temporary)
    temporary.replace(path)
    return {
        "appended_rows": len(additions),
        "conflict_rows": len(conflicts),
        "conflicts": conflicts,
        "latest_date": (
            merged.index.max().date().isoformat()
            if not merged.empty else None
        ),
    }


def _action_records_for(symbol: str) -> list[dict]:
    market = symbol.rsplit(".", 1)[-1]
    dataset = "dividends_hk" if market == "HK" else "dividends"
    path = paths.CACHE_DIR / dataset / f"{symbol.split('.', 1)[0]}.json"
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    records = document.get("records", []) if isinstance(document, dict) else []
    return records if isinstance(records, list) else []


def _persist_basis_transition(symbol: str, source: str, audit: dict) -> Path | None:
    if not audit.get("historical_overlap_differences", {}).get("cells"):
        return None
    directory = paths.CACHE_DIR / "qfq_basis_evidence"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{symbol.split('.', 1)[0]}.json"
    temporary = target.with_suffix(".json.tmp")
    payload = {
        **audit,
        "symbol": symbol,
        "source": source,
        "source_basis": "qfq",
        "authorization_required": True,
        "formal_history_modified": False,
    }
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
    return target


def sync_bundle_history(bundle: DataBundle) -> dict:
    """Append a quality-gated market bundle to both canonical history views."""
    if not bundle.gate or not bundle.gate.get("passed"):
        return {
            "status": "SKIPPED_GATE_FAILED",
            "symbols": {},
            "appended_rows": 0,
            "conflict_rows": 0,
        }

    history_dir = paths.STATE_DIR / "history"
    results = {}
    appended_rows = 0
    conflict_rows = 0
    failures = []
    price_basis_blocked = False
    transition_counts = {
        "UNCHANGED_QFQ_OVERLAP": 0,
        "SAFE_CURRENT_WEEK_APPEND": 0,
        "ACTION_AWARE_CHAIN_BRIDGE": 0,
        "CARRIED_QFQ_CHAIN_BRIDGE": 0,
        "CARRIED_ADDITIVE_QFQ_BRIDGE": 0,
    }
    week_end = pd.Timestamp(bundle.as_of).normalize()
    week_start = week_end - pd.Timedelta(days=week_end.weekday())
    for symbol, item in bundle.symbols.items():
        safe = symbol.replace(".", "_")
        symbol_result = {}
        for view, filename in (
            ("raw", f"{safe}_raw.parquet"),
            ("standardized", f"{safe}.parquet"),
        ):
            try:
                incoming = item.ohlcv
                if view == 'standardized':
                    from .price_basis import prepare_standardized_append, validated_prior_chain
                    existing_path = history_dir / filename
                    existing = pd.read_parquet(existing_path) if existing_path.exists() else None
                    parameters = dict(week_start=week_start, as_of=week_end,
                                      action_records=_action_records_for(symbol))
                    try:
                        incoming, transition = prepare_standardized_append(
                            existing, item.standardized_ohlcv, item.ohlcv, **parameters)
                    except ValueError as exc:
                        if str(exc) != 'unexplained qfq overlap drift; keep historical candidate isolated':
                            raise
                        database = os.environ.get('QWL_BRIDGE_FORMAL_READONLY')
                        prior_job = os.environ.get('QWL_BRIDGE_PRIOR_JOB_READONLY')
                        if not database or not prior_job:
                            raise
                        prior_chain = validated_prior_chain(
                            database, prior_job, symbol, item.standardized_ohlcv,
                            item.ohlcv, week_start=week_start,
                            source=getattr(item, 'standardized_source', ''),
                        )
                        incoming, transition = prepare_standardized_append(
                            existing, item.standardized_ohlcv, item.ohlcv,
                            **parameters, prior_chain=prior_chain)
                    transition_counts[transition["classification"]] += 1
                    evidence_path = _persist_basis_transition(
                        symbol, getattr(item, "standardized_source", "explicit_qfq"), transition,
                    )
                result = _append_only_history_file(
                    history_dir / filename, incoming
                )
                if view == 'standardized':
                    result["basis_transition"] = transition
                    result["basis_evidence"] = str(evidence_path) if evidence_path else None
            except Exception as exc:  # noqa: BLE001
                if view == 'standardized':
                    price_basis_blocked = True
                result = {
                    "error": f"{type(exc).__name__}: {str(exc)[:300]}"
                }
                failures.append(f"{symbol}:{view}")
            symbol_result[view] = result
            appended_rows += int(result.get("appended_rows") or 0)
            conflict_rows += int(result.get("conflict_rows") or 0)
        results[symbol] = symbol_result

    status = "OK"
    if failures:
        status = "FAILED"
    elif conflict_rows:
        status = "CONFLICT"
    return {
        "status": status,
        "symbols": results,
        "appended_rows": appended_rows,
        "conflict_rows": conflict_rows,
        "failures": failures,
        "price_basis_blocked": price_basis_blocked,
        "basis_transition_counts": transition_counts,
        "policy": "append_current_week_preserve_history_qfq_chain_v1",
    }


def validate_snapshot_dir(
    snapshot_dir: Path | str,
    *,
    persist: bool = False,
) -> dict:
    """Validate snapshot chronology without rewriting immutable source files."""
    directory = Path(snapshot_dir)
    meta_path = directory / "meta.json"
    with open(meta_path, encoding="utf-8") as handle:
        meta = json.load(handle)
    as_of = date.fromisoformat(str(meta["as_of"])[:10])
    future_rows = 0
    affected = {}
    max_observed_on = None
    content_digest = hashlib.sha256(meta_path.read_bytes())
    for path in sorted([*(directory / "ohlcv").glob("*.parquet"),
                        *(directory / "ohlcv_standardized").glob("*.parquet")]):
        content_digest.update(path.name.encode())
        content_digest.update(hashlib.sha256(path.read_bytes()).digest())
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        observed = pd.to_datetime(frame.index)
        maximum = observed.max().date()
        if max_observed_on is None or maximum > max_observed_on:
            max_observed_on = maximum
        count = int((observed.date > as_of).sum())
        if count:
            affected[path.stem.replace("_", ".")] = {
                "future_rows": count,
                "max_observed_on": maximum.isoformat(),
            }
            future_rows += count
    status = "invalid_for_replay" if future_rows else "valid_for_replay"
    validation = {
        "schema_version": 1,
        "validation_id": hashlib.sha256(
            (
                f"{meta['run_id']}|{content_digest.hexdigest()}|"
                f"{status}|{future_rows}"
            ).encode()
        ).hexdigest()[:24],
        "run_id": meta["run_id"],
        "snapshot_dir": str(directory),
        "as_of": as_of.isoformat(),
        "status": status,
        "valid_for_replay": future_rows == 0,
        "future_row_count": future_rows,
        "affected_symbols": affected,
        "max_observed_on": (
            max_observed_on.isoformat() if max_observed_on else None
        ),
        "snapshot_content_hash": content_digest.hexdigest(),
        "reason": (
            "snapshot contains OHLCV observations after as_of"
            if future_rows else None
        ),
        "validated_at": datetime.now(timezone.utc).isoformat(),
    }
    if persist:
        with open(
            directory / "validation.json", "w", encoding="utf-8"
        ) as handle:
            json.dump(validation, handle, ensure_ascii=False, indent=2)
    return validation


def assert_snapshot_replayable(snapshot_dir: Path | str) -> dict:
    """Return validation evidence or refuse replay of a contaminated snapshot."""
    directory = Path(snapshot_dir)
    validation_path = directory / "validation.json"
    if validation_path.exists():
        with open(validation_path, encoding="utf-8") as handle:
            validation = json.load(handle)
    else:
        validation = validate_snapshot_dir(directory, persist=False)
    if not validation.get("valid_for_replay"):
        raise ValueError(
            "snapshot is invalid for replay: "
            f"{validation.get('reason')}; "
            f"future_rows={validation.get('future_row_count')}"
        )
    return validation
