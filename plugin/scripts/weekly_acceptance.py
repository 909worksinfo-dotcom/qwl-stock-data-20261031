"""Offline acceptance of base + committed/staged versions, never quarantine."""
from __future__ import annotations

import json
import hashlib
import io
import os
import shutil
import subprocess
import tarfile
import tempfile
from datetime import date
from pathlib import Path

import pandas as pd

from scripts.weekly_guard import effective_json, effective_parquet, week_window

REQUIRED_DOMAINS = set("""fundamentals financial_versions statements statements_hk
announcements dividends dividends_hk hk_share_snapshot microstructure fund_flow_ths
northbound southbound listing suspensions hk_action_evidence margin margin_eligibility
share_changes ownership_a ownership_hk hk_market_disclosures consensus hk_short_selling
news earnings_calendar repurchases_a block_trades_a lhb splits benchmarks connect_flows
fx industry macro margin_market""".split())
DEFERRED_MARGIN_DOMAINS = frozenset({"margin", "margin_market"})


def coverage_gate(coverage: dict, as_of: str,
                  validated_basis_bridges: dict | None = None) -> dict:
    start, end = week_window(as_of)
    validated_basis_bridges = validated_basis_bridges or {}
    issues = []
    margin_pending = []
    history_warnings = []
    if coverage.get("as_of_date") != as_of or coverage.get("universe_size") != 60:
        issues.append({"domain": "scope", "reason": "wrong_date_or_universe", "blocking": True})
    for flag in ("scoped_directories_clean", "record_integrity_complete"):
        if coverage.get(flag) is not True:
            issues.append({"domain": flag, "reason": "integrity_gate", "blocking": True})
    datasets = coverage.get("datasets", {})
    for name in sorted(REQUIRED_DOMAINS | set(datasets)):
        item = datasets.get(name, {})
        if item.get("complete") is not True:
            finding = {"domain": name, "reason": "domain_incomplete",
                       "details": item, "blocking": bool(item.get("leaked"))}
            if (name in DEFERRED_MARGIN_DOMAINS and item
                    and not item.get("leaked") and not item.get("provider_errors")):
                margin_pending.append(finding)
            else:
                issues.append(finding)
    for domain, section in coverage.get("freshness", {}).items():
        for symbol, item in section.get("items", {}).items():
            if item.get("status") not in {"fresh", "exempt"}:
                finding = {"domain": domain, "symbol": symbol,
                           "date": item.get("expected_date"), "reason": "waiting_for_source",
                           "latest_date": item.get("latest_date"), "blocking": False}
                (margin_pending if domain in DEFERRED_MARGIN_DOMAINS else issues).append(finding)
    unexplained_freshness = [
        error for error in coverage.get("freshness_errors", [])
        if not any(str(error).startswith(f"{domain}/") for domain in coverage.get("freshness", {}))
    ]
    if (coverage.get("freshness_complete") is not True
            and (unexplained_freshness or not coverage.get("freshness_errors"))
            and not any(i["reason"] == "waiting_for_source" for i in issues)):
        issues.append({"domain": "freshness", "reason": "unclassified_freshness", "blocking": True})
    history = coverage.get("history_consistency", {})
    if not history.get("latest_snapshot") or len(history.get("items", {})) != 60:
        issues.append({"domain": "history", "reason": "missing_snapshot_or_symbols", "blocking": False})
    for symbol, views in history.get("items", {}).items():
        for view in ("raw", "standardized"):
            item = views.get(view, {})
            mismatch = item.get("snapshot_mismatch_dates", [])
            old_only = (item.get("status") in {"conflict", "basis_unverified"} and bool(mismatch)
                        and all(str(day) < start.isoformat() for day in mismatch)
                        and item.get("latest_date", "") >= item.get("expected_date", as_of)
                        and (item.get('status') != 'basis_unverified'
                             or item.get('current_week_raw_equivalent') is True))
            if old_only:
                history_warnings.append({"symbol": symbol, "view": view, **item})
            elif (view == "standardized" and symbol in validated_basis_bridges
                  and item.get("status") == "conflict"
                  and item.get("latest_date", "") >= item.get("expected_date", as_of)):
                history_warnings.append({
                    "symbol": symbol,
                    "view": view,
                    **item,
                    "classification": "ACTION_AWARE_CHAIN_BRIDGE",
                    "basis_bridge_validated": True,
                    "basis_bridge_proof": validated_basis_bridges[symbol],
                })
            elif item.get("status") != "fresh":
                issues.append({"domain": "history", "symbol": symbol, "view": view,
                               "date": item.get("expected_date"), "reason": "current_history_invalid",
                               "blocking": item.get("status") not in {"stale", "missing"},
                               "details": item})
    return {"passed": not issues, "issues": issues, "historical_conflicts": history_warnings,
            "margin_pending": margin_pending,
            "historical_basis_unverified": [w for w in history_warnings if w.get('status') == 'basis_unverified'],
            "historical_conflicts_waived_as_complete": False,
            "known_irreducible_gaps": coverage.get("known_irreducible_gaps", []),
            "all_domains_complete": coverage.get("all_domains_complete", False),
            "week_start": start.isoformat(), "week_end": end.isoformat()}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tar_frame(archive: tarfile.TarFile, names: set[str], suffix: str) -> pd.DataFrame:
    matches = sorted(name for name in names if name.endswith(suffix))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one market snapshot member ending {suffix}")
    handle = archive.extractfile(matches[0])
    if handle is None:
        raise ValueError(f"market snapshot member is not a file: {matches[0]}")
    return pd.read_parquet(io.BytesIO(handle.read()))


def validate_qfq_basis_bridges(candidate_root: Path, as_of: str,
                               basis_acceptance: dict | None,
                               database_root: Path | None = None) -> tuple[dict, list[dict]]:
    """Verify a chain-linked current-week tail against an immutable market receipt.

    A bridge may explain why the accepted qfq chain intentionally differs from
    the provider's latest full-vintage qfq series.  This never certifies or
    rewrites the historical prefix; it only proves the candidate week's values.
    """
    if not basis_acceptance:
        return {}, []
    errors = []
    try:
        receipt_path = Path(basis_acceptance["receipt_path"]).resolve()
        snapshot_path = Path(basis_acceptance["provider_snapshot"]).resolve()
        if (_sha(receipt_path) != basis_acceptance["receipt_sha256"]
                or _sha(snapshot_path) != basis_acceptance["provider_snapshot_sha256"]):
            raise ValueError("market receipt or immutable snapshot SHA drift")
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        result = receipt["result"]
        history = result["history_sync"]
        if (receipt.get("ok") is not True or receipt.get("issues")
                or result.get("gate", {}).get("passed") is not True
                or history.get("price_basis_blocked") is not False):
            raise ValueError("market receipt did not pass its native safety gate")
        transition_counts = history.get("basis_transition_counts", {})
        if sum(int(value) for value in transition_counts.values()) != 60:
            raise ValueError("market receipt does not cover the fixed 60-stock universe")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        return {}, [{"domain": "history", "reason": "invalid_qfq_bridge_receipt",
                     "blocking": True, "details": str(exc)}]

    start, end = week_window(as_of)
    validated = {}
    try:
        with tarfile.open(snapshot_path, mode="r:gz") as archive:
            names = set(archive.getnames())
            metas = [name for name in names if name.startswith('state/snapshots/')
                     and name.endswith('/meta.json')]
            meta = json.loads(archive.extractfile(metas[0]).read()) if len(metas) == 1 else None
            for symbol, views in history.get("symbols", {}).items():
                standard = views.get("standardized", {}) if isinstance(views, dict) else {}
                transition = standard.get("basis_transition", {})
                classification = transition.get("classification")
                if classification not in {"ACTION_AWARE_CHAIN_BRIDGE", "CARRIED_QFQ_CHAIN_BRIDGE",
                                           "CARRIED_ADDITIVE_QFQ_BRIDGE"}:
                    continue
                bridge = transition.get("bridge") or {}
                code_market = symbol.split(".", 1)
                if len(code_market) != 2:
                    raise ValueError(f"invalid bridge symbol: {symbol}")
                stem = symbol.replace(".", "_")
                qfq = _tar_frame(archive, names, f"/ohlcv_standardized/{stem}.parquet")
                raw = _tar_frame(archive, names, f"/ohlcv/{stem}.parquet")
                from qwl.datasources.price_basis import differences, frame, frame_sha256
                qfq, raw = frame(qfq), frame(raw)
                candidate = frame(pd.read_parquet(
                    candidate_root / "state/history" / f"{stem}.parquet"
                ))
                anchor = pd.Timestamp(transition["anchor_date"])
                target = qfq.loc[(qfq.index >= pd.Timestamp(start))
                                  & (qfq.index <= pd.Timestamp(end))]
                raw_target = raw.loc[(raw.index >= pd.Timestamp(start))
                                     & (raw.index <= pd.Timestamp(end))]
                actual = candidate.loc[(candidate.index >= pd.Timestamp(start))
                                       & (candidate.index <= pd.Timestamp(end))]
                if (transition.get("as_of") != as_of
                        or transition.get("week_start") != start.isoformat()
                        or transition.get("historical_records_published") != 0
                        or bridge.get("method") != ("cash_additive_oneoff"
                            if classification == "CARRIED_ADDITIVE_QFQ_BRIDGE" else "qfq_chain_v1")
                        or bridge.get("volume_scaled") is not False
                        or frame_sha256(qfq) != transition.get("incoming_qfq_sha256")
                        or frame_sha256(raw) != transition.get("raw_sha256")
                        or anchor not in candidate.index
                        or frame_sha256(candidate.loc[[anchor]])
                           != transition.get("formal_anchor_sha256")
                        or target.empty or target.index.max().date() != end
                        or not target.index.equals(raw_target.index)
                        or not target.index.equals(actual.index)):
                    raise ValueError(f"qfq bridge scope, hash, anchor or sessions invalid: {symbol}")
                current = differences(raw_target, target)
                reported = transition.get("current_week_qfq_raw_differences", {})
                if (current.get("cells") != reported.get("cells")
                        or current.get("dates") != reported.get("dates")
                        or current.get("field_counts") != reported.get("field_counts")
                        or current.get("field_counts", {}).get("volume") != 0):
                    raise ValueError(f"qfq bridge current-week difference proof changed: {symbol}")
                actions = bridge.get("actions") or []
                if classification in {"CARRIED_QFQ_CHAIN_BRIDGE", "CARRIED_ADDITIVE_QFQ_BRIDGE"}:
                    if (database_root is None or meta is None or current['cells']
                            or (classification == "CARRIED_QFQ_CHAIN_BRIDGE" and not actions)
                            or (classification == "CARRIED_ADDITIVE_QFQ_BRIDGE"
                                and (symbol != "603345.SH" or actions))):
                        raise ValueError(f"qfq carried bridge has no zero-drift prior proof: {symbol}")
                    from scripts.weekly_runner import job_dir
                    from qwl.datasources.price_basis import (
                        validated_prior_chain, current_week_actions,
                    )
                    prior_friday = start - __import__('datetime').timedelta(days=3)
                    prior_job = job_dir(receipt_path.parents[4], database_root,
                                        prior_friday.isoformat()) / 'job.json'
                    source = meta['symbols'][symbol]['standardized_source']
                    proven = validated_prior_chain(
                        database_root, prior_job, symbol, qfq, raw,
                        week_start=start, source=source,
                    )
                    if proven != bridge:
                        raise ValueError(f"qfq carried bridge evidence/hash drift: {symbol}")
                    code, market = symbol.split('.')
                    relative = Path('cache') / ('dividends_hk' if market == 'HK'
                                                else 'dividends') / f'{code}.json'
                    candidate_actions = effective_json(database_root, relative).get('records', [])
                    if current_week_actions(candidate_actions, anchor=anchor,
                                            as_of=pd.Timestamp(end)):
                        raise ValueError(f"qfq carried bridge crosses a new company action: {symbol}")
                else:
                    if not actions:
                        raise ValueError(f"qfq bridge lacks company-action evidence: {symbol}")
                    first_action = min(pd.Timestamp(item["ex_date"]) for item in actions)
                    if (first_action < pd.Timestamp(start) or first_action > pd.Timestamp(end)
                            or any(pd.Timestamp(item["available_on"])
                                   > pd.Timestamp(item["ex_date"]) for item in actions)
                            or any(pd.Timestamp(day) >= first_action
                                   for day in current.get("dates", []))):
                        raise ValueError(f"qfq bridge action is late or outside target week: {symbol}")
                expected = target.copy()
                if classification == "CARRIED_ADDITIVE_QFQ_BRIDGE":
                    expected.loc[:, ["open", "high", "low", "close"]] += float(bridge["offset"])
                else:
                    expected.loc[:, ["open", "high", "low", "close"]] *= float(bridge["scale"])
                # Parquet engines may materialize the same calendar sessions as
                # datetime64[ms] or datetime64[us].  Session equality is checked
                # above; align that already-proven index before comparing values.
                expected.index = actual.index
                pd.testing.assert_frame_equal(
                    actual.loc[:, ["open", "high", "low", "close", "volume"]],
                    expected.loc[:, ["open", "high", "low", "close", "volume"]],
                    check_exact=False, rtol=1e-10, atol=1e-8,
                )
                validated[symbol] = {
                    "receipt_sha256": basis_acceptance["receipt_sha256"],
                    "provider_snapshot_sha256": basis_acceptance["provider_snapshot_sha256"],
                    "candidate_week_rows": int(len(actual)),
                    "anchor_date": anchor.date().isoformat(),
                    "classification": classification,
                    "method": bridge["method"],
                    **({"offset": float(bridge["offset"])} if classification == "CARRIED_ADDITIVE_QFQ_BRIDGE"
                       else {"scale": float(bridge["scale"])}),
                    "volume_scaled": False,
                    "historical_records_published": 0,
                }
    except Exception as exc:  # includes parquet/tar/assertion failures
        errors.append({"domain": "history", "reason": "qfq_bridge_candidate_mismatch",
                       "blocking": True, "details": f"{type(exc).__name__}: {str(exc)[:1000]}"})
        return {}, errors
    expected_bridges = sum(int(transition_counts.get(kind, 0)) for kind in (
        "ACTION_AWARE_CHAIN_BRIDGE", "CARRIED_QFQ_CHAIN_BRIDGE", "CARRIED_ADDITIVE_QFQ_BRIDGE"))
    if len(validated) != expected_bridges:
        errors.append({"domain": "history", "reason": "qfq_bridge_count_mismatch",
                       "blocking": True,
                       "details": {"expected": expected_bridges, "validated": len(validated)}})
        return {}, errors
    return validated, []


def materialize(database: Path, staged: Path | None, destination: Path) -> None:
    """Use private copies, never hardlinks: the native auditor writes its report."""
    formal = destination / "formal"
    for relative in ("cache", "state/history", "state/published_versions"):
        src = database / relative
        if src.exists():
            shutil.copytree(src, formal / relative, dirs_exist_ok=True)
    if staged and (staged / "state/published_versions").exists():
        shutil.copytree(staged / "state/published_versions",
                        formal / "state/published_versions", dirs_exist_ok=True)
    relatives = set()
    for prefix in ("cache", "state/history"):
        for f in (formal / prefix).rglob("*"):
            if f.is_file():
                relatives.add(f.relative_to(formal))
    for f in (formal / "state/published_versions").rglob("*"):
        if f.is_file():
            relatives.add(f.parent.relative_to(formal / "state/published_versions"))
    for relative in sorted(relatives):
        output = destination / (Path(".cache") / Path(*relative.parts[1:])
                                if relative.parts[0] == "cache" else relative)
        output.parent.mkdir(parents=True, exist_ok=True)
        if relative.suffix == ".json":
            output.write_text(json.dumps(effective_json(formal, relative), ensure_ascii=False, default=str))
        elif relative.suffix == ".parquet":
            effective_parquet(formal / relative, formal / "state/published_versions" / relative).to_parquet(output)
        elif (formal / relative).is_file():
            shutil.copy2(formal / relative, output)


def audit_formal(runtime: Path, database: Path, staged: Path | None, as_of: str,
                 basis_acceptance: dict | None = None,
                 basis_provenance_root: Path | None = None,
                 job_path: Path | None = None) -> dict:
    with tempfile.TemporaryDirectory(prefix="qwl-formal-acceptance-") as temp:
        view = Path(temp)
        materialize(database, staged, view)
        shutil.copytree(runtime / "config", view / "config")
        snapshots = runtime / "state/snapshots" / as_of
        if snapshots.exists():
            shutil.copytree(snapshots, view / "state/snapshots" / as_of)
        env = {**os.environ, "QWL_HOME": str(view), "PYTHONPATH": str(runtime / "src"),
               "PYTHONDONTWRITEBYTECODE": "1"}
        process = subprocess.run(
            [str(runtime / ".venv/bin/python"), "-B", str(Path(__file__).with_name("weekly_audit_worker.py")), as_of],
            env=env, capture_output=True, text=True, timeout=600)
        report = view / "weekly_acceptance.json"
        if process.returncode or not report.is_file():
            return {"passed": False, "issues": [{"domain": "formal", "reason": "audit_worker_failed",
                    "blocking": True, "details": process.stderr[-1500:]}]}
        native = json.loads(report.read_text())
        bridges, bridge_errors = validate_qfq_basis_bridges(
            view, as_of, basis_acceptance,
            database_root=basis_provenance_root or database)
        result = coverage_gate(native["coverage"], as_of, bridges)
        result["issues"].extend(bridge_errors)
        result["issues"].extend(native["weekly_issues"])
        result["margin_pending"].extend(native.get("margin_pending", []))
        if job_path is not None:
            independent = Path("/Users/bytedance/Documents/ChatGPT/量化交易/weekly_formal_acceptance.py")
            if not independent.is_file():
                result["issues"].append({"domain":"formal","reason":"independent_gate_missing",
                                         "blocking":True})
            else:
                extra = subprocess.run(
                    [str(runtime / ".venv/bin/python"), "-B", str(independent),
                     "--view", str(view), "--runtime", str(runtime),
                     "--as-of", as_of, "--job-path", str(job_path)],
                    env={**os.environ, "PYTHONDONTWRITEBYTECODE":"1"},
                    capture_output=True, text=True, timeout=600)
                try:
                    report = json.loads(extra.stdout)
                    result["independent_formal_acceptance"] = report
                    result["margin_pending"].extend(report.get("margin_pending", []))
                    if extra.returncode or not report.get("passed"):
                        result["issues"].extend(report.get("issues") or [{
                            "domain":"formal","reason":"independent_gate_failed","blocking":True}])
                except (json.JSONDecodeError, TypeError):
                    result["issues"].append({"domain":"formal","reason":"independent_gate_exception",
                                             "blocking":True,"details":extra.stderr[-1500:]})
        result["passed"] = not result["issues"]
        result["validated_qfq_basis_bridges"] = bridges
        result["layer"] = "base+published_versions" + ("+staged" if staged else "")
        result["quarantine_used"] = False
        return result
