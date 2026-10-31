"""Data-only MCP adapter for the canonical QWL fixed-60 runtime."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


def _plugin_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _valid_qwl_home(path: Path) -> bool:
    return (
        (path / "src/qwl/datasources").is_dir()
        and (path / "config/universe.yaml").is_file()
        and (path / "config/comparables.yaml").is_file()
    )


def _resolve_qwl_home() -> Path:
    candidates: list[Path] = []
    if os.environ.get("QWL_HOME"):
        candidates.append(Path(os.environ["QWL_HOME"]).expanduser())
    runtime_path = _plugin_root() / "config/runtime.json"
    try:
        configured = json.loads(runtime_path.read_text(encoding="utf-8")).get(
            "qwl_home"
        )
        if configured:
            candidates.append(Path(configured).expanduser())
    except (OSError, json.JSONDecodeError):
        pass
    for ancestor in Path(__file__).resolve().parents:
        candidates.append(ancestor / "quant-watchlist-plugin")
    for candidate in candidates:
        resolved = candidate.resolve()
        if _valid_qwl_home(resolved):
            return resolved
    raise RuntimeError(
        "Canonical QWL runtime not found. Set QWL_HOME or update "
        "config/runtime.json to a quant-watchlist-plugin directory."
    )


QWL_HOME = _resolve_qwl_home()
QWL_PYTHON = QWL_HOME / ".venv/bin/python"

if (
    os.environ.get("QWL_DATA_MCP_REEXEC") != "1"
    and QWL_PYTHON.is_file()
    and Path(sys.executable).resolve() != QWL_PYTHON.resolve()
):
    env = dict(os.environ)
    env["QWL_HOME"] = str(QWL_HOME)
    env["QWL_DATA_MCP_REEXEC"] = "1"
    os.execve(
        str(QWL_PYTHON),
        [str(QWL_PYTHON), str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )

os.environ["QWL_HOME"] = str(QWL_HOME)
os.environ.setdefault(
    "FASTMCP_HOME",
    str(QWL_HOME / "state/fastmcp"),
)
os.environ.setdefault("FASTMCP_SHOW_SERVER_BANNER", "false")
os.environ.setdefault("FASTMCP_CHECK_FOR_UPDATES", "off")
sys.path.insert(0, str(_plugin_root()))
sys.path.insert(0, str(QWL_HOME / "src"))

from fastmcp import FastMCP  # noqa: E402

from qwl import config, paths  # noqa: E402
from qwl.datasources import collect, extended, extended_p2  # noqa: E402

from scripts.weekly_guard import (  # noqa: E402
    DEFAULT_DATABASE_ROOT,
    DEFAULT_FREEZE_MANIFEST,
    PUBLISH_TOKEN,
    publish_weekly,
    verify_freeze_manifest,
)
from scripts.weekly_acceptance import (
    audit_formal,
    coverage_gate,
    validate_qfq_basis_bridges,
)
from scripts.weekly_runner import run_job, status as weekly_job_status, REVISION
from scripts.weekly_tasks import make_tasks

FIXED_UNIVERSE_SHA256 = "787f8016081e6f13027b6cfc46245780cb69ebd61157bd90734514769a07ba20"

def _fixed_membership(universe):
    return hashlib.sha256("\n".join(sorted(s.symbol for s in universe.stocks)).encode()).hexdigest() == FIXED_UNIVERSE_SHA256


def _market_basis_acceptance(database: Path, as_of_date: str) -> dict | None:
    """Bind final acceptance to the exact successful immutable market receipt."""
    from scripts.weekly_runner import job_dir
    folder = job_dir(QWL_HOME / "state/weekly_jobs", database, as_of_date).resolve()
    checkpoint = folder / "job.json"
    try:
        state = json.loads(checkpoint.read_text(encoding="utf-8"))
        receipt = Path(state["tasks"]["market"]["receipt"]).resolve()
        if not receipt.is_relative_to(folder) or not receipt.is_file():
            return None
        document = json.loads(receipt.read_text(encoding="utf-8"))
        snapshot = Path(document["provider_snapshot"]).resolve()
        if not snapshot.is_relative_to(folder) or not snapshot.is_file():
            return None
        return {
            "receipt_path": str(receipt),
            "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
            "provider_snapshot": str(snapshot),
            "provider_snapshot_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
        }
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None

# Native provider logs must not corrupt the JSON-RPC stdout stream.
import structlog
structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))


mcp = FastMCP(
    "qwl-stock-data",
    instructions=(
        "Data-only access to the canonical QWL fixed-60 A/H stock runtime. "
        "Never add symbols, never treat missing data as zero, and always "
        "report PIT, quality, coverage, and known source gaps."
    ),
)


def _json_read(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _count_status(values: dict) -> dict:
    return dict(Counter(values.values()))


def _coverage(refresh: bool, as_of_date: str | None = None) -> dict:
    if refresh:
        return extended.audit_data_coverage(
            config.load_extended_universe(), as_of_date=as_of_date
        )
    return _json_read(paths.CACHE_DIR / "coverage/data_coverage.json")


def _refresh_market(as_of_date: str, persist_snapshot: bool) -> dict:
    as_of = date.fromisoformat(as_of_date)
    universe = config.load_extended_universe()
    bundle = collect.collect(universe, as_of, best_effort=True, include_standardized=True,
        series_cache=os.environ.get('QWL_WEEKLY_SERIES_CACHE'))
    snapshot_dir = (
        str(collect.persist_snapshot(bundle)) if persist_snapshot else None
    )
    history_sync = collect.sync_bundle_history(bundle)
    status = "OK"
    if not bundle.gate.get("passed") or history_sync.get("status") != "OK":
        status = "FAILED"
    return {
        "status": status,
        "run_id": bundle.run_id,
        "as_of": bundle.as_of,
        "universe_size": len(bundle.symbols),
        "gate": bundle.gate,
        "snapshot_dir": snapshot_dir,
        "history_sync": history_sync,
        "symbols": {
            symbol: {
                "ohlcv_rows": len(item.ohlcv),
                "ohlcv_source": item.ohlcv_source,
                "has_snapshot": item.snapshot is not None,
                "has_fundamentals": item.fundamentals is not None,
                "has_fund_flow": item.fund_flow is not None,
            }
            for symbol, item in bundle.symbols.items()
        },
    }


def _refresh_fundamentals(force: bool, budget_seconds: float) -> dict:
    from qwl.datasources import fundamentals_cache

    universe = config.load_extended_universe()
    summary = fundamentals_cache.refresh_universe(
        universe,
        budget_s=budget_seconds,
        force=force,
    )
    return _finalize_result({
        "status": "OK",
        "total": len(summary),
        "summary_counts": _count_status(summary),
        "symbols": summary,
    })


def _refresh_financial_events(
    force: bool,
    statements_budget: float,
    dividends_budget: float,
    announcement_weeks: int,
) -> dict:
    from qwl.datasources import financial_versions

    universe = config.load_extended_universe()
    statements_a = extended.collect_statements(
        universe, budget_s=statements_budget, force=force
    )
    statements_hk = extended.collect_hk_statements(
        universe, budget_s=statements_budget, force=force
    )
    dividends_a = extended.collect_dividends(
        universe, budget_s=dividends_budget, force=force
    )
    result = {
        "status": "OK",
        "statements_a": _count_status(statements_a),
        "statements_hk": _count_status(statements_hk),
        "dividends_a": _count_status(dividends_a),
        "splits": extended.derive_splits_from_dividends(universe),
        "industry": extended.collect_industry_map(universe),
        "fx": extended.collect_fx(universe=universe),
        "announcements_a": extended.collect_announcements(
            universe, weeks=announcement_weeks
        ),
        "announcements_hk": extended.collect_hk_announcements(
            universe, days=announcement_weeks * 7
        ),
        "lhb": extended.collect_lhb(universe),
        "margin_sse": extended.collect_margin_sse(universe),
        "northbound": _count_status(
            extended.collect_northbound(universe, force=force)
        ),
        "microstructure": _count_status(
            extended.collect_market_microstructure(
                universe, force=force
            )
        ),
        "financial_versions": financial_versions.archive_universe(universe),
        "hk_financial_version_repair": (
            financial_versions.repair_hk_statement_baselines(universe)
        ),
        "pit_normalization": extended_p2.normalize_p2_availability(universe),
    }
    return _finalize_result(result)


def _refresh_gap(
    force: bool,
    include_fund_flow: bool,
    include_share_changes: bool,
    southbound_days: int,
    as_of_date: str | None = None,
) -> dict:
    universe = config.load_extended_universe()
    fund_flow = (
        extended.collect_fund_flow_ths(universe, force=force)
        if include_fund_flow
        else None
    )
    fund_history = (
        extended.collect_fund_flow_history(universe, force=force)
        if include_fund_flow
        else None
    )
    share_changes = (
        extended.collect_share_changes(universe)
        if include_share_changes
        else None
    )
    result = {
        "status": "OK",
        "microstructure": _count_status(
            extended.collect_market_microstructure(
                universe, force=force
            )
        ),
        "fund_flow_ths": fund_flow,
        "fund_flow_history": (
            _count_status(fund_history) if fund_history else None
        ),
        "margin_history": _count_status(
            extended.collect_margin_history_em(universe, force=force)
        ),
        "margin_market": extended.collect_margin_market_aggregate(),
        "margin_eligibility": extended.collect_margin_eligibility(universe),
        "northbound": _count_status(
            extended.collect_northbound(universe, force=force)
        ),
        "connect_flows": extended.collect_connect_flows(),
        "southbound": extended.collect_southbound_holdings(
            universe, days=southbound_days
        ),
        "dividends_hk": _count_status(
            extended.collect_hk_dividends(universe)
        ),
        "hk_share_snapshot": _count_status(
            extended.collect_hk_share_snapshots(universe)
        ),
        "share_changes": (
            _count_status(share_changes) if share_changes else None
        ),
        "flow_normalization": extended.normalize_flow_anomalies(universe),
        "pit_normalization": extended_p2.normalize_p2_availability(universe),
        "coverage": extended.audit_data_coverage(
            universe, as_of_date=as_of_date
        ),
    }
    return _finalize_result(result)


def _refresh_historical(start_date: str, budget_seconds: float) -> dict:
    universe = config.load_extended_universe()
    announcements = extended.collect_historical_announcements(
        universe,
        start_date=start_date,
        budget_s=budget_seconds,
    )
    listing = extended.collect_listing_intervals(universe)
    suspensions = extended.collect_suspension_intervals(
        universe, start_date=start_date
    )
    actions = extended.collect_hk_action_evidence(universe)
    return _finalize_result({
        "status": "OK",
        "announcements": _count_status(announcements),
        "listing": _count_status(listing),
        "suspensions": _count_status(suspensions),
        "hk_action_evidence": _count_status(actions),
    })


def _refresh_specialized(
    force: bool,
    include_research_text: bool,
) -> dict:
    from qwl.datasources.anker_historical_intraday import (
        collect_anker_historical_intraday,
    )
    from qwl.datasources.byd_historical_intraday import (
        collect_byd_historical_intraday,
    )
    from qwl.datasources.byd_operating import collect_byd_operating_history
    from qwl.datasources.byd_research_text import collect_byd_research_text
    from qwl.datasources.ev_operating import collect_ev_operating_history

    return _finalize_result({
        "status": "OK",
        "byd_intraday": collect_byd_historical_intraday(force=force),
        "anker_intraday": collect_anker_historical_intraday(force=force),
        "byd_operating": collect_byd_operating_history(force=force),
        "ev_operating": collect_ev_operating_history(force=force),
        "byd_research_text": (
            collect_byd_research_text(force=force)
            if include_research_text
            else None
        ),
    })


def _failure_reasons(value: Any, path: str = "$") -> list[str]:
    """Find nested operational failures without misclassifying empty/ineligible."""
    reasons = []
    if isinstance(value, dict):
        status = value.get("status")
        if isinstance(status, str) and status.lower() in {
            "failed", "failure", "partial", "timeout", "stale",
            "conflict", "invalid", "invalid_for_replay",
        }:
            reasons.append(f"{path}.status={status}")
        if value.get("passed") is False:
            reasons.append(f"{path}.passed=false")
        for key, nested in value.items():
            lowered = str(key).lower()
            if lowered in {
                "failed", "failures", "timeout", "timeouts", "stale",
                "conflict", "conflicts", "error", "errors",
                "provider_errors", "freshness_errors",
                "history_consistency_errors",
            } and nested:
                reasons.append(f"{path}.{key}={str(nested)[:240]}")
                continue
            if lowered.endswith("_complete") and nested is False:
                reasons.append(f"{path}.{key}=false")
                continue
            reasons.extend(_failure_reasons(nested, f"{path}.{key}"))
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            reasons.extend(_failure_reasons(nested, f"{path}[{index}]"))
    return list(dict.fromkeys(reasons))


def _finalize_result(result: dict) -> dict:
    output = dict(result)
    output.pop("failure_reasons", None)
    failures = _failure_reasons({
        key: value for key, value in output.items() if key != "status"
    })
    output["status"] = "PARTIAL" if failures else "OK"
    output["failure_reasons"] = failures
    return output


def _stage(name: str, fn, *args, **kwargs) -> dict:
    try:
        value = fn(*args, **kwargs)
        failures = _failure_reasons(value)
        return {
            "stage": name,
            "ok": not failures,
            "status": "OK" if not failures else "PARTIAL",
            "failure_reasons": failures,
            "result": value,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "stage": name,
            "ok": False,
            "status": "FAILED",
            "error": f"{type(exc).__name__}: {str(exc)[:500]}",
        }


def _refresh_all_data(
    as_of_date: str,
    force: bool,
    historical_start_date: str,
    include_specialized: bool,
) -> dict:
    stages = [
        _stage("market", _refresh_market, as_of_date, True),
        _stage("fundamentals", _refresh_fundamentals, force, 180.0),
        _stage(
            "financial_events",
            _refresh_financial_events,
            force,
            360.0,
            180.0,
            8,
        ),
        _stage(
            "gaps",
            _refresh_gap,
            force,
            True,
            True,
            1000,
            as_of_date,
        ),
        _stage(
            "historical_evidence",
            _refresh_historical,
            historical_start_date,
            1200.0,
        ),
        _stage(
            "p2",
            extended_p2.collect_p2,
            config.load_extended_universe(),
        ),
    ]
    if include_specialized:
        stages.append(
            _stage("specialized", _refresh_specialized, force, True)
        )
    coverage = _stage(
        "coverage",
        extended.audit_data_coverage,
        config.load_extended_universe(),
        as_of_date,
    )
    stages.append(coverage)
    critical_failed = any(
        stage["stage"] in {"market", "coverage"} and not stage["ok"]
        for stage in stages
    )
    return {
        "status": (
            "FAILED" if critical_failed
            else "OK" if all(stage["ok"] for stage in stages)
            else "PARTIAL"
        ),
        "stages": stages,
        "known_irreducible_gaps": (
            coverage.get("result", {}).get("known_irreducible_gaps", [])
        ),
    }


def _weekly_publication_gate(refresh: dict) -> dict:
    stages = {
        stage.get("stage"): stage
        for stage in refresh.get("stages", [])
        if isinstance(stage, dict)
    }
    market = stages.get("market", {}).get("result", {})
    coverage = stages.get("coverage", {}).get("result", {})
    checks = {
        "market_quality_gate": market.get("gate", {}).get("passed") is True,
        "fixed_60_scope_clean": coverage.get("scoped_directories_clean") is True,
        "record_integrity_complete": coverage.get("record_integrity_complete") is True,
        "freshness_complete": coverage.get("freshness_complete") is True,
        "refresh_complete": refresh.get("status") == "OK",
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "note": (
            "Legacy whole-refresh gate fails closed; resumable weekly jobs "
            "use explicit current-week acceptance and preserve historical warnings"
        ),
    }
@mcp.tool
def qwl_data_health() -> dict:
    """Check canonical runtime, fixed-60 boundary, dependencies, and coverage."""
    universe = config.load_extended_universe()
    markets = Counter(stock.market.value for stock in universe.stocks)
    coverage = _coverage(refresh=False)
    required = (
        "fastmcp",
        "pandas",
        "pyarrow",
        "duckdb",
        "akshare",
        "curl_cffi",
        "bs4",
        "pdfminer",
        "exchange_calendars",
    )
    dependencies = {}
    for name in required:
        try:
            __import__(name)
            dependencies[name] = True
        except Exception:  # noqa: BLE001
            dependencies[name] = False
    frozen_sources = verify_freeze_manifest(DEFAULT_FREEZE_MANIFEST)
    return {
        "ok": (
            len(universe.stocks) == 60
            and _fixed_membership(universe)
            and markets == Counter({"A": 45, "HK": 15})
            and all(dependencies.values())
            and frozen_sources.get("ok") is True
        ),
        "qwl_home": str(QWL_HOME),
        "runtime_revision": REVISION,
        "maintenance_revision": 'weekly-recovery.20261024',
        "python": sys.executable,
        "universe_size": len(universe.stocks),
        "markets": dict(markets),
        "dependencies": dependencies,
        "coverage_generated_at": coverage.get("generated_at"),
        "coverage": {
            key: coverage.get(key)
            for key in (
                "core_dense_complete",
                "optional_extended_complete",
                "scoped_directories_clean",
                "record_integrity_complete",
                "freshness_complete",
                "history_consistency_complete",
                "all_domains_complete",
            )
        },
        "known_irreducible_gaps": coverage.get(
            "known_irreducible_gaps", []
        ),
        "frozen_sources": frozen_sources,
    }


@mcp.tool
def qwl_list_fixed_60() -> dict:
    """Return the only symbols this plugin may collect or persist."""
    universe = config.load_extended_universe()
    return {
        "universe_version": universe.universe_version,
        "count": len(universe.stocks),
        "a_share_count": sum(
            stock.market.value == "A" for stock in universe.stocks
        ),
        "hk_count": sum(
            stock.market.value == "HK" for stock in universe.stocks
        ),
        "stocks": [
            stock.model_dump(mode="json") for stock in universe.stocks
        ],
    }


@mcp.tool
def qwl_data_coverage_status(
    refresh: bool = False,
    as_of_date: str | None = None,
) -> dict:
    """Read or recompute all-domain coverage, integrity, and leakage status."""
    value = _coverage(refresh=refresh, as_of_date=as_of_date)
    status = "NOT_AVAILABLE"
    if value:
        status = "OK" if value.get("all_domains_complete") else "INCOMPLETE"
    return {
        "status": status,
        **value,
    }


@mcp.tool
def qwl_refresh_market_data(
    as_of_date: str,
    persist_snapshot: bool = True,
) -> dict:
    """Refresh 60-stock daily bars and snapshots with the hard quality gate."""
    return _refresh_market(as_of_date, persist_snapshot)


@mcp.tool
def qwl_refresh_fundamentals(
    force: bool = False,
    budget_seconds: float = 120.0,
) -> dict:
    """Refresh fixed-60 fundamentals and archive changed report versions."""
    return _refresh_fundamentals(force, budget_seconds)


@mcp.tool
def qwl_refresh_financial_events(
    force: bool = False,
    statements_budget: float = 240.0,
    dividends_budget: float = 120.0,
    announcement_weeks: int = 8,
) -> dict:
    """Refresh statements, dividends, announcements, FX, and event evidence."""
    return _refresh_financial_events(
        force,
        statements_budget,
        dividends_budget,
        announcement_weeks,
    )


@mcp.tool
def qwl_refresh_gap_data(
    force: bool = False,
    include_fund_flow: bool = True,
    include_share_changes: bool = True,
    southbound_days: int = 1000,
    as_of_date: str | None = None,
) -> dict:
    """Refresh flows, margin, connect holdings, capital, and microstructure."""
    return _refresh_gap(
        force,
        include_fund_flow,
        include_share_changes,
        southbound_days,
        as_of_date,
    )


@mcp.tool
def qwl_refresh_historical_evidence(
    start_date: str = "20230101",
    budget_seconds: float = 1200.0,
) -> dict:
    """Backfill official announcements, listing, suspensions, and HK actions."""
    return _refresh_historical(start_date, budget_seconds)


@mcp.tool
def qwl_refresh_p2_data(as_of_date: str | None = None) -> dict:
    """Refresh ownership, consensus, news, benchmarks, macro, and HK shorts."""
    universe = config.load_extended_universe()
    result = {
        "status": "OK",
        **extended_p2.collect_p2(universe),
        "coverage": extended.audit_data_coverage(
            universe, as_of_date=as_of_date
        ),
    }
    return _finalize_result(result)


@mcp.tool
def qwl_refresh_specialized_data(
    force: bool = False,
    include_research_text: bool = True,
) -> dict:
    """Refresh source-hashed intraday, operating, and optional report text."""
    return _refresh_specialized(force, include_research_text)


@mcp.tool
def qwl_refresh_all_data(
    as_of_date: str,
    force: bool = False,
    historical_start_date: str = "20230101",
    include_specialized: bool = True,
) -> dict:
    """Run all fixed-60 refresh stages and finish with a fresh coverage audit."""
    return _refresh_all_data(
        as_of_date,
        force,
        historical_start_date,
        include_specialized,
    )


@mcp.tool
def qwl_publish_weekly_data(
    as_of_date: str,
    database_root: str = str(DEFAULT_DATABASE_ROOT),
    publish: bool = False,
    authorization: str = "",
    collected_after: str | None = None,
) -> dict:
    """Read-only publication preview; writes require qwl_weekly_update."""
    if publish:
        return {"status": "BLOCKED_USE_QWL_WEEKLY_UPDATE", "complete": False}
    universe = config.load_extended_universe()
    allowed_stems = {
        value
        for stock in universe.stocks
        for value in (stock.ak_symbol, stock.symbol.replace(".", "_"))
    }
    return publish_weekly(
        QWL_HOME,
        Path(database_root).expanduser().resolve(),
        as_of_date,
        publish=publish,
        authorization=authorization,
        collected_after=collected_after,
        freeze_manifest=DEFAULT_FREEZE_MANIFEST,
        allowed_stems=allowed_stems,
    )


@mcp.tool
def qwl_weekly_update(
    as_of_date: str,
    database_root: str = str(DEFAULT_DATABASE_ROOT),
    publish: bool = False,
    authorization: str = "",
    include_specialized: bool = True,
) -> dict:
    """Resume one weekend job; COMPLETE requires formal database acceptance."""
    freeze = verify_freeze_manifest(DEFAULT_FREEZE_MANIFEST)
    if not freeze.get("ok"):
        return {
            "status": "BLOCKED_PLUGIN_OR_SKILL_DRIFT",
            "freeze": freeze,
            "collection_started": False,
        }
    if publish and authorization != PUBLISH_TOKEN:
        return {
            "status": "BLOCKED_MISSING_CURRENT_WEEK_AUTHORIZATION",
            "freeze": freeze,
            "collection_started": False,
        }
    if not publish:
        return qwl_weekly_status(as_of_date, database_root)
    if not include_specialized:
        return {"status": "BLOCKED_REQUIRED_DIMENSIONS_DISABLED", "complete": False}
    universe = config.load_extended_universe()
    markets = Counter(str(s.market.value) for s in universe.stocks)
    if not _fixed_membership(universe) or len({s.symbol for s in universe.stocks}) != 60 or markets != {"A": 45, "HK": 15}:
        return {"status": "BLOCKED_OUTSIDE_FIXED_60", "complete": False}
    database = Path(database_root).expanduser().resolve()
    if database != DEFAULT_DATABASE_ROOT.resolve() or database == QWL_HOME.resolve():
        return {"status": "BLOCKED_DATABASE_ROOT", "complete": False}
    def source_audit():
        coverage = extended.audit_data_coverage(universe, as_of_date=as_of_date)
        bridges, bridge_errors = validate_qfq_basis_bridges(
            QWL_HOME, as_of_date, _market_basis_acceptance(database, as_of_date),
            database_root=database,
        )
        gate = coverage_gate(coverage, as_of_date, bridges)
        gate["issues"].extend(bridge_errors)
        gate["passed"] = not gate["issues"]
        gate["validated_qfq_basis_bridges"] = bridges
        return coverage, gate
    def publisher(started_at):
        from scripts.weekly_runner import window_allowed, job_dir
        job_path = job_dir(QWL_HOME / "state/weekly_jobs", database, as_of_date) / "job.json"
        def accept(root, staged):
            if not window_allowed(as_of_date):
                return {"passed": False, "issues": [{"reason": "cross_week_authorization_required", "blocking": True}]}
            return audit_formal(
                QWL_HOME, root, staged, as_of_date,
                basis_acceptance=_market_basis_acceptance(database, as_of_date),
                job_path=job_path,
            )
        return publish_weekly(
            QWL_HOME, database, as_of_date, publish=True, authorization=authorization,
            # Cached successful inputs may predate this job yet be absent from
            # the formal layer. Compare ALL scoped inputs; date/key guards
            # still quarantine every old backfill/revision.
            collected_after=None, freeze_manifest=DEFAULT_FREEZE_MANIFEST,
            allowed_stems={v for s in universe.stocks for v in (s.ak_symbol, s.symbol.replace(".", "_"))},
            acceptance=accept)
    from scripts.weekly_items import expand_tasks
    from scripts.weekly_isolation import collect_isolated
    from scripts.weekly_runner import window_allowed
    item_tasks = expand_tasks(make_tasks(sys.modules[__name__], as_of_date, include_specialized),universe,QWL_HOME,as_of_date)
    codes = {v for s in universe.stocks for v in (s.ak_symbol,s.symbol.replace('.', '_'))}
    return run_job(
        state_root=QWL_HOME / "state/weekly_jobs", runtime=QWL_HOME,
        database=database, as_of=as_of_date,
        tasks=item_tasks,
        freeze_manifest=DEFAULT_FREEZE_MANIFEST,
        audit_source=source_audit, publish=publisher,
        isolated_collect=lambda task,receipt: collect_isolated(task,QWL_HOME,as_of_date,receipt,codes,
            guard=lambda: window_allowed(as_of_date) and verify_freeze_manifest(DEFAULT_FREEZE_MANIFEST)["ok"], database=database))


@mcp.tool
def qwl_weekly_revalidate(as_of_date: str, resume: bool = False, authorization: str = '',
                          expected_checkpoint_sha: str = '',
                          database_root: str = str(DEFAULT_DATABASE_ROOT)) -> dict:
    """Explicit post-repair audit. Preview is read-only; resume needs separate authorization and SHA."""
    from scripts.weekly_revalidation import revalidate
    from scripts.weekly_dry_acceptance import assess
    database = Path(database_root).expanduser().resolve()
    universe = config.load_extended_universe()
    if database != DEFAULT_DATABASE_ROOT.resolve() or not _fixed_membership(universe):
        return {"status": "BLOCKED_DATABASE_OR_POOL", "checkpoint_modified": False}
    stems = {v for s in universe.stocks for v in (s.ak_symbol, s.symbol.replace('.', '_'))}
    return revalidate(state_root=QWL_HOME / 'state/weekly_jobs', database=database, as_of=as_of_date,
        freeze_manifest=DEFAULT_FREEZE_MANIFEST, resume=resume, authorization=authorization,
        expected_checkpoint_sha=expected_checkpoint_sha,
        assess=lambda: assess(
            QWL_HOME, database, as_of_date, DEFAULT_FREEZE_MANIFEST, stems,
            basis_acceptance=_market_basis_acceptance(database, as_of_date),
        ))


@mcp.tool
def qwl_weekly_resume_item_safety(
    as_of_date: str,
    quarantine_evidence_path: str,
    expected_checkpoint_sha: str,
    authorization: str = '',
    database_root: str = str(DEFAULT_DATABASE_ROOT),
) -> dict:
    """Resume one blocked item after exact, user-authorized quarantine evidence."""
    from scripts.weekly_revalidation import resume_item_safety
    database = Path(database_root).expanduser().resolve()
    universe = config.load_extended_universe()
    if database != DEFAULT_DATABASE_ROOT.resolve() or not _fixed_membership(universe):
        return {'status':'BLOCKED_DATABASE_OR_POOL','checkpoint_modified':False}
    return resume_item_safety(
        state_root=QWL_HOME / 'state/weekly_jobs', database=database, as_of=as_of_date,
        freeze_manifest=DEFAULT_FREEZE_MANIFEST,
        quarantine_evidence=quarantine_evidence_path,
        expected_checkpoint_sha=expected_checkpoint_sha,
        authorization=authorization)


@mcp.tool
def qwl_weekly_resume_market_safety(
    as_of_date: str,
    expected_checkpoint_sha: str,
    expected_provider_snapshot_sha: str,
    expected_validation_sha: str,
    authorization: str = '',
    database_root: str = str(DEFAULT_DATABASE_ROOT),
) -> dict:
    """Resume a blocked market task after an exact, read-only qfq snapshot replay."""
    from scripts.weekly_market_repair import validate_qfq_market_snapshot
    from scripts.weekly_revalidation import resume_market_safety
    database = Path(database_root).expanduser().resolve()
    universe = config.load_extended_universe()
    if database != DEFAULT_DATABASE_ROOT.resolve() or not _fixed_membership(universe):
        return {'status':'BLOCKED_DATABASE_OR_POOL','checkpoint_modified':False}
    return resume_market_safety(
        state_root=QWL_HOME / 'state/weekly_jobs', database=database, as_of=as_of_date,
        freeze_manifest=DEFAULT_FREEZE_MANIFEST,
        expected_checkpoint_sha=expected_checkpoint_sha,
        expected_provider_snapshot_sha=expected_provider_snapshot_sha,
        expected_validation_sha=expected_validation_sha,
        authorization=authorization,
        validate=lambda snapshot: validate_qfq_market_snapshot(
            provider_snapshot=snapshot, database=database,
            as_of=as_of_date, securities=universe.stocks,
            state_root=QWL_HOME / 'state/weekly_jobs',
        ),
    )


@mcp.tool
def qwl_weekly_status(as_of_date: str, database_root: str = str(DEFAULT_DATABASE_ROOT)) -> dict:
    """Read the durable checkpoint without collection, publication or file writes."""
    from scripts.weekly_runner import public_result, job_dir
    root = QWL_HOME / "state/weekly_jobs"
    database = Path(database_root).expanduser().resolve()
    result = weekly_job_status(root, database, as_of_date)
    older = []
    for checkpoint in job_dir(root, database, as_of_date).parent.glob("*/job.json"):
        previous = json.loads(checkpoint.read_text())
        if previous.get("as_of_date", "") < as_of_date and previous.get("status") != "COMPLETE":
            older.append({"as_of_date": previous.get("as_of_date"), "status": previous.get("status"),
                          "evidence_path": str(checkpoint), "authorization_required": True})
    return {"runtime_revision": REVISION, "older_pending_jobs": older,
            **public_result(result, job_dir(root, database, as_of_date) / "job.json")}


def _retry_weekly_selected(task, gaps):
    # Native collectors may need whole-market responses, but completed
    # collector tasks are skipped. Margin retries use an isolated subset
    # workspace to preserve the canonical full-directory whitelist checks.
    if task.name == "margin_history":
        from scripts.weekly_margin_retry import retry_margin
        return retry_margin(QWL_HOME, {g["symbol"] for g in gaps if g.get("symbol")})
    return task.call()


@mcp.tool
def qwl_symbol_data_status(symbol: str) -> dict:
    """List persisted source data for one allowed symbol."""
    universe = config.load_extended_universe()
    target = next(
        (
            stock
            for stock in universe.stocks
            if symbol in {
                stock.symbol,
                stock.ak_symbol,
                stock.tencent_code,
            }
        ),
        None,
    )
    if target is None:
        return {
            "status": "REJECTED_OUTSIDE_FIXED_60",
            "requested": symbol,
        }
    cache_files = []
    for directory in sorted(paths.CACHE_DIR.iterdir()):
        if not directory.is_dir() or directory.name == "pycache":
            continue
        for candidate in (
            directory / f"{target.ak_symbol}.json",
            directory / f"{target.ak_symbol}.parquet",
            directory / f"{target.ak_symbol}_60m.parquet",
        ):
            if candidate.is_file():
                cache_files.append({
                    "dataset": directory.name,
                    "path": str(candidate),
                    "bytes": candidate.stat().st_size,
                    "top_level_keys": list(_json_read(candidate))[:30]
                    if candidate.suffix == ".json"
                    else None,
                })
    safe = target.symbol.replace(".", "_")
    history_files = []
    for candidate in (
        paths.STATE_DIR / "history" / f"{safe}_raw.parquet",
        paths.STATE_DIR / "history" / f"{safe}.parquet",
    ):
        if candidate.is_file():
            history_files.append({
                "path": str(candidate),
                "bytes": candidate.stat().st_size,
                "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest(),
            })
    return {
        "status": "OK",
        "stock": target.model_dump(mode="json"),
        "history_files": history_files,
        "cache_files": cache_files,
        "cache_dataset_count": len(cache_files),
    }


@mcp.tool
def qwl_data_inventory() -> dict:
    """Return file counts and bytes for all source-data storage roots."""
    roots = {
        "history": paths.STATE_DIR / "history",
        "snapshots": paths.STATE_DIR / "snapshots",
        "forecast_forward": paths.STATE_DIR / "forecast_forward",
        "cache": paths.CACHE_DIR,
    }
    output: dict[str, Any] = {}
    for name, root in roots.items():
        files = [
            path for path in root.rglob("*")
            if path.is_file() and "pycache" not in path.parts
        ] if root.exists() else []
        output[name] = {
            "path": str(root),
            "files": len(files),
            "bytes": sum(path.stat().st_size for path in files),
        }
    return {"status": "OK", "roots": output}


@mcp.tool
def qwl_validate_snapshot(snapshot_dir: str) -> dict:
    """Validate snapshot chronology and return replay-safety evidence."""
    return collect.validate_snapshot_dir(snapshot_dir, persist=False)


def main() -> None:
    if "--http" in sys.argv:
        mcp.run(transport="http", host="127.0.0.1", port=8858)
    else:
        mcp.run()


if __name__ == "__main__":
    main()
