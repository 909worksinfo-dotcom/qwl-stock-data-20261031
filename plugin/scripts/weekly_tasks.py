"""Granular calls to existing canonical collectors, with no provider reimplementation."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Callable

STATEMENT_NATIVE_BUDGET_SECONDS = 400

@dataclass
class Task:
    name: str
    domains: tuple[str, ...]
    call: Callable
    parent: str = ""
    symbol: str = ""
    document: str = ""
    output: str = ""
    expected_date: str = ""
    timeout_seconds: float = 0


def make_tasks(adapter, as_of: str, specialized: bool = True) -> list[Task]:
    from qwl.datasources import financial_versions, fundamentals_cache
    e, p = adapter.extended, adapter.extended_p2
    u = adapter.config.load_extended_universe()
    start = (date.fromisoformat(as_of) - timedelta(days=date.fromisoformat(as_of).weekday())).strftime("%Y%m%d")
    tasks = []
    def add(name, domains, fn, *args, **kwargs):
        tasks.append(Task(name, tuple(domains.split()), lambda: fn(*args, **kwargs)))
    add("fundamentals", "fundamentals", fundamentals_cache.refresh_universe, u, budget_s=180, force=True)
    add("statements_a", "statements", e.collect_statements, u,
        budget_s=STATEMENT_NATIVE_BUDGET_SECONDS, force=True)
    add("statements_hk", "statements_hk", e.collect_hk_statements, u,
        budget_s=STATEMENT_NATIVE_BUDGET_SECONDS, force=True)
    add("dividends", "dividends", e.collect_dividends, u, budget_s=180, force=True)
    add("splits", "splits", e.derive_splits_from_dividends, u)
    add("industry", "industry", e.collect_industry_map, u)
    add("fx", "fx", e.collect_fx, universe=u)
    # FX also converts the freshly collected statements; keep those before it.
    # Market quality consumes FX, so it must not precede this prerequisite.
    tasks.append(Task("market", ("history",), lambda: adapter._refresh_market(as_of, True)))
    add("announcements_a", "announcements", e.collect_announcements, u, weeks=1)
    add("announcements_hk", "announcements", e.collect_hk_announcements, u, days=7)
    add("lhb", "lhb", e.collect_lhb, u, start_date=start)
    add("margin_sse", "margin", e.collect_margin_sse, u)
    add("northbound", "northbound", e.collect_northbound, u, force=False)
    add("microstructure", "microstructure", e.collect_market_microstructure, u, force=False)
    add("financial_versions", "financial_versions", financial_versions.archive_universe, u)
    add("hk_statement_baseline", "financial_versions", financial_versions.audit_hk_statement_baselines, u)
    add("fund_flow", "fund_flow_ths", e.collect_fund_flow_ths, u, force=False)
    add("fund_flow_history", "fund_flow_ths", e.collect_fund_flow_history, u, force=False)
    add("margin_history", "margin", e.collect_margin_history_em, u, force=False)
    add("margin_market", "margin_market", e.collect_margin_market_aggregate)
    # Native eligibility expects YYYYMMDD; the weekly/checkpoint API stays ISO
    add("margin_eligibility", "margin_eligibility", e.collect_margin_eligibility, u,
        as_of_date=date.fromisoformat(as_of).strftime("%Y%m%d"))
    add("connect_flows", "connect_flows", e.collect_connect_flows)
    add("southbound", "southbound", e.collect_southbound_holdings, u, days=1000)
    add("dividends_hk", "dividends_hk", e.collect_hk_dividends, u)
    add("hk_share_snapshot", "hk_share_snapshot", e.collect_hk_share_snapshots, u)
    add("share_changes", "share_changes", e.collect_share_changes, u, start_date=start)
    add("announcements_history", "announcements", e.collect_historical_announcements, u,
        start_date=start, end_date=as_of.replace("-", ""), budget_s=1200)
    add("listing", "listing", e.collect_listing_intervals, u)
    add("suspensions", "suspensions", e.collect_suspension_intervals, u, start_date=start)
    add("hk_actions", "hk_action_evidence", e.collect_hk_action_evidence, u)
    add("unlock_announcements", "announcements", p.backfill_a_unlock_announcements, u,
        start_date=start, end_date=as_of.replace("-", ""))
    add("hk_results", "announcements", p.backfill_hk_results_announcements, u)
    add("ownership_a", "ownership_a", p.collect_a_ownership, u)
    add("ownership_hk", "ownership_hk", p.collect_hk_ownership, u)
    add("hk_disclosures", "hk_market_disclosures", p.collect_hk_market_disclosures, u, start_date=start)
    add("market_events", "repurchases_a block_trades_a", p.collect_a_market_events, u, start_date=start, end_date=as_of.replace("-", ""))
    add("consensus", "consensus", p.collect_consensus, u)
    add("news", "news", p.collect_news, u)
    add("earnings", "earnings_calendar", p.collect_earnings_calendar, u)
    add("macro", "macro", p.collect_macro)
    add("short_selling", "hk_short_selling", p.collect_hk_short_selling, u)
    if specialized:
        add("specialized", "specialized", adapter._refresh_specialized, True, True)
    # Normalizers must run again if any collector is retried.
    add("flow_normalization", "normalization", e.normalize_flow_anomalies, u)
    add("pit_normalization", "normalization", p.normalize_p2_availability, u)
    # Derived inputs must be planned after the final PIT/flow normalization
    add("benchmarks", "benchmarks", p.collect_benchmarks, u)
    return tasks


def task_issues(value, path="$"):
    """Keep source status evidence; do not confuse an empty response with a zero."""
    bad = []
    failed = {"failed", "failure", "timeout", "stale", "partial", "conflict", "invalid",
              "error", "budget_exhausted", "invalid_for_replay"}
    if isinstance(value, dict):
        for k, v in value.items():
            key = str(k).lower()
            if (key in {"error", "errors", "provider_errors", "failed", "timeout", "stale", "failure_reasons"}
                    or key.endswith("_error")) and v:
                bad.append({"path": f"{path}.{k}", "value": str(v)[:1000]})
            elif (key == "passed" or key.endswith("_complete")) and v is False:
                bad.append({"path": f"{path}.{k}", "value": False})
            elif k == "status" and isinstance(v, str) and v.lower() in failed:
                bad.append({"path": f"{path}.{k}", "value": v})
            else:
                bad.extend(task_issues(v, f"{path}.{k}"))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            bad.extend(task_issues(v, f"{path}[{i}]"))
    elif isinstance(value, str) and (value.lower().split(":", 1)[0].strip() in failed
                                   or value.lower().startswith("failed_")):
        bad.append({"path": path, "value": value})
    return bad
