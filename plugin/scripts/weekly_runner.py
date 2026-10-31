"""Durable weekend jobs. Scheduler wakes the job; no unbounded in-call sleeps."""
from __future__ import annotations
import fcntl
import json
import os
import hashlib
import math
import re
import tarfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from scripts.weekly_guard import week_window, verify_freeze_manifest, file_inventory, verify_existing, _source_files
from scripts.weekly_tasks import task_issues

REVISION = "weekly-contract.20260906"
MAINTENANCE_REVISION = 'weekly-recovery.20261024'
PREVIOUS_REVISION = "weekly-resume.20260905"
TZ = ZoneInfo("Asia/Shanghai")
DEFERRED_MARGIN_DOMAINS = frozenset({"margin", "margin_market"})

def deferred_margin_task(task, item):
    """Only source-unavailable daily margin receipts may remain pending at publication."""
    if (not item or item.get("ok") or item.get("failure_class") != "RETRY_SOURCE"
            or not set(task.domains) <= DEFERRED_MARGIN_DOMAINS):
        return False
    detail = json.dumps(item.get("issues", []), ensure_ascii=False).lower()
    return not any(marker in detail for marker in (
        "conflict", "invalid", "unsafe", "schema", "integrity",
        "valueerror", "typeerror", "keyerror", "assertionerror", "parse", "format"))

def classify_task_failure(receipt):
    """Separate retryable source failures from deterministic access/contract failures."""
    if receipt.get("ok"):
        return "OK"
    result = receipt.get("result") or {}
    for event in result.get("cninfo_source_evidence", []):
        status = event.get("status")
        if status == "CNINFO_HTTP_DENIED":
            return "BLOCKED_SOURCE_ACCESS"
        if status in {"CNINFO_NON_JSON", "CNINFO_SCHEMA", "CNINFO_SOURCE_ERROR",
                      "CNINFO_REDIRECT_BLOCKED"}:
            return "BLOCKED_SOURCE_CONTRACT"
        if status in {"CNINFO_HTTP_FAILURE", "CNINFO_PAGINATION_INCOMPLETE"}:
            return "RETRY_SOURCE"
    message = str(result.get("error") or receipt.get("issues") or "")
    if 'SERIES_INTEGRITY' in message:
        return 'BLOCKED_ITEM_SAFETY'
    if "ProviderResponseError: CNINFO_HTTP_DENIED" in message:
        return "BLOCKED_SOURCE_ACCESS"
    if any(f"ProviderResponseError: {code}" in message for code in (
            "CNINFO_NON_JSON", "CNINFO_SCHEMA", "CNINFO_SOURCE_ERROR", "CNINFO_REDIRECT_BLOCKED")):
        return "BLOCKED_SOURCE_CONTRACT"
    return "RETRY_SOURCE"

def now():
    return datetime.now(TZ)

def window_allowed(as_of, instant=None):
    instant = instant or now()
    start, end = week_window(as_of)
    monday = instant.date() - timedelta(days=instant.weekday())
    return (start == monday and end == monday + timedelta(days=4)
            and instant.weekday() >= 5)

def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)

def job_dir(state_root, database, as_of):
    key = hashlib.sha256(str(database.resolve()).encode()).hexdigest()[:16]
    return state_root / key / as_of

def status(state_root, database, as_of):
    path = job_dir(state_root, database, as_of) / "job.json"
    if not path.is_file():
        return {"status": "NOT_STARTED", "as_of_date": as_of, "complete": False}
    return json.loads(path.read_text())

def public_result(state, path):
    return {**{k:v for k,v in state.items() if k not in {"baseline", "task_results"}},
            "evidence_path": str(path), "complete": state.get("status") == "COMPLETE"}

def queue_retry(state, tasks, instant):
    domains = {i.get("domain") for i in state.get("gaps", [])}
    for task in tasks:
        related = [g for g in state.get("gaps",[]) if g.get("domain") in task.domains
                   and (not task.symbol or not g.get("symbol") or g["symbol"] == task.symbol)]
        # Daily history gaps are not failed THS multi-horizon snapshots
        if (related and task.name not in {"margin_sse", "fund_flow"}):
            state["tasks"].setdefault(task.name, {})["ok"] = False
    delay = 60 * 2 ** (state["attempt"] - 1) if state["attempt"] < 6 else 3600
    state.update(status="WAITING_SOURCE" if domains else "WAITING_RETRY",
                 next_retry_at=(instant + timedelta(seconds=delay)).isoformat(),
                 fast_attempt_limit=6)

def market_fx_retry_evidence(state, folder, runtime, as_of, *, allow_pending=False):
    """Prove eligibility for collection retry, never eligibility for publication.

    Only the exact stale-FX fatal with skipped history sync qualifies. Old
    history differences may remain unresolved while its missing tail is
    collected; current/undated conflicts and every other hard gate stay hard.
    """
    try:
        if state.get('publication') or state.get('active_task'):
            return None
        market = state['tasks']['market']
        if market.get('ok') or market.get('failure_class') != 'RETRY_SOURCE':
            return None
        receipt_path = Path(market['receipt']).resolve()
        if not receipt_path.is_relative_to(folder.resolve()):
            return None
        receipt = json.loads(receipt_path.read_text())
        result = receipt['result']
        gate, history = result['gate'], result['history_sync']
        fatal = [i for i in gate.get('issues', []) if i.get('severity') == 'fatal']
        if (receipt.get('ok') is not False or result.get('status') != 'FAILED'
                or gate.get('passed') is not False or gate.get('fatal_count') != 1
                or len(fatal) != 1 or fatal[0].get('locus') != 'HKD/CNY:fx:SAFE_CFETS'
                or history.get('status') != 'SKIPPED_GATE_FAILED'
                or history.get('symbols') != {} or history.get('appended_rows') != 0
                or history.get('conflict_rows') != 0 or history.get('price_basis_blocked')):
            return None
        match = re.fullmatch(r'latest rate (\d{4}-\d{2}-\d{2}) is stale for expected session (\d{4}-\d{2}-\d{2})',
                             fatal[0].get('message', ''))
        start, end = week_window(as_of)
        if not match or not start.isoformat() <= match[2] <= end.isoformat() or match[1] >= match[2]:
            return None
        for issue in state.get('gaps', []):
            if not issue.get('blocking', False):
                continue
            if (issue.get('domain') == 'history' and issue.get('reason') == 'invalid_qfq_bridge_receipt'
                    and issue.get('details') == 'market receipt did not pass its native safety gate'):
                continue
            detail = issue.get('details') or {}
            dates = detail.get('snapshot_mismatch_dates') or []
            if not (issue.get('domain') == 'history' and issue.get('reason') == 'current_history_invalid'
                    and detail.get('status') in {'conflict', 'basis_unverified'} and dates
                    and all(datetime.fromisoformat(str(d)).date() < start for d in dates)
                    and detail.get('latest_date', '') < detail.get('expected_date', as_of)):
                return None
        evidence = {'scope':'market_fx_dependency_only', 'retry_allowed':True,
                    'publication_allowed':False, 'expected_fx_date':match[2],
                    'market_receipt':str(receipt_path),
                    'market_receipt_sha256':hashlib.sha256(receipt_path.read_bytes()).hexdigest()}
        snapshot = Path(receipt['provider_snapshot']).resolve()
        if not snapshot.is_relative_to(folder.resolve()) or not snapshot.is_file():
            return None
        evidence['market_provider_snapshot_sha256'] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        fx = state['tasks']['fx']
        if fx.get('ok') is not True:
            return {**evidence, 'fx_ready':False} if allow_pending and fx.get('failure_class') == 'RETRY_SOURCE' else None
        fx_receipt = Path(fx['receipt']).resolve()
        if not fx_receipt.is_relative_to(folder.resolve()):
            return None
        document = json.loads(fx_receipt.read_text())
        if (document.get('ok') is not True or document.get('issues')
                or document.get('result', {}).get('status') != 'OK'
                or document['result']['range'][-1] != match[2]):
            return None
        provider = Path(document['provider_snapshot']).resolve()
        prepared = json.loads(fx_receipt.with_suffix('.prepared.json').read_text())
        accepted = Path(prepared['snapshot']).resolve()
        if any(not p.is_relative_to(folder.resolve()) for p in (provider, accepted)):
            return None
        rel = 'cache/fx/HKDCNY.json'
        with tarfile.open(provider) as archive:
            data = archive.extractfile(rel).read()
        with tarfile.open(accepted) as archive:
            accepted_data = archive.extractfile(rel).read()
        sha = hashlib.sha256(data).hexdigest()
        current = runtime / '.cache/fx/HKDCNY.json'
        if (data != accepted_data or prepared['outputs'][rel]['sha256'] != sha
                or hashlib.sha256(current.read_bytes()).hexdigest() != sha
                or prepared['result'] != document['result']):
            return None
        payload = json.loads(data)
        dates = [datetime.fromisoformat(r['date']).date() for r in payload['records']]
        target = [r for r in payload['records'] if r['date'] == match[2]]
        sources = payload.get('source', [])
        if (payload.get('pair') != 'HKD/CNY' or payload.get('primary_field') != 'hkd_cny'
                or 'SAFE official CFETS RMB central parity archive' not in sources
                or len(dates) != len(set(dates)) or max(dates).isoformat() != match[2]
                or len(target) != 1 or isinstance(target[0].get('hkd_cny'), bool)
                or not math.isfinite(float(target[0]['hkd_cny'])) or float(target[0]['hkd_cny']) <= 0):
            return None
        return {**evidence, 'fx_ready':True, 'fx_receipt':str(fx_receipt),
                'fx_receipt_sha256':hashlib.sha256(fx_receipt.read_bytes()).hexdigest(),
                'fx_provider_snapshot_sha256':hashlib.sha256(provider.read_bytes()).hexdigest(),
                'fx_accepted_snapshot_sha256':hashlib.sha256(accepted.read_bytes()).hexdigest(),
                'fx_payload_sha256':sha}
    except (OSError, KeyError, TypeError, ValueError, IndexError, AttributeError, tarfile.TarError):
        return None

def run_job(*, state_root, runtime, database, as_of, tasks, freeze_manifest,
            audit_source, publish, clock=now, retry_selected=None, isolated_collect=None):
    freeze = verify_freeze_manifest(freeze_manifest)
    if not freeze["ok"]:
        return {"status": "BLOCKED_PLUGIN_OR_SKILL_DRIFT", "freeze": freeze, "complete": False}
    folder = job_dir(state_root, database, as_of)
    path = folder / "job.json"
    if not window_allowed(as_of, clock()):
        return {"status": "AUTHORIZATION_REQUIRED_CROSS_WEEK", "complete": False,
                "pending_job": str(path), "collection_started": False}
    # One shared lock protects canonical collectors AND database publication.
    state_root.mkdir(parents=True, exist_ok=True)
    with (state_root / "runtime.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "RUNNING", "complete": False, "evidence_path": str(path)}
        state = status(state_root, database, as_of)
        if state["status"].startswith("BLOCKED") or state["status"] == "COMPLETE":
            return public_result(state, path)
        if state.get("revision", REVISION) not in {REVISION, PREVIOUS_REVISION, 'weekly-items.20260906', 'weekly-basis.20260906'}:
            return {"status": "BLOCKED_CHECKPOINT_REVISION", "complete": False}
        if state.get("revision", REVISION) != REVISION and state.get("tasks"):
            return {"status": "BLOCKED_CHECKPOINT_CONTRACT_REVALIDATION_REQUIRED", "complete": False,
                    "evidence_path": str(path), "checkpoint_modified": False}
        if state.get("next_retry_at") and clock() < datetime.fromisoformat(state["next_retry_at"]):
            return public_result(state, path)
        if "baseline" not in state:
            state = {"revision": REVISION, "status": "RUNNING", "as_of_date": as_of,
                     "database": str(database), "started_at": clock().isoformat(),
                     "baseline": file_inventory(database), "tasks": {}, "attempt": 0, "gaps": []}
        drift = verify_existing(database, state["baseline"])
        if drift:
            state.update(status="BLOCKED_FORMAL_HISTORY_DRIFT", changed_files=drift)
            save(path, state)
            return public_result(state, path)
        if isolated_collect:
            from scripts.weekly_items import migrate_verified
            migrate_verified(state,tasks,folder,runtime,as_of)
            active = state.get("active_task")
            prior_receipt = folder/"attempts"/str(state.get("attempt",0))/(str(active)+".json")
            if active and prior_receipt.exists() and state.get("status") == "RUNNING":
                previous = json.loads(prior_receipt.read_text())
                if previous.get("ok"):
                    state["tasks"][active] = {"ok":True,"issues":[],"receipt":str(prior_receipt),
                                              **({"verified_artifact":previous["verified_artifact"]}
                                                 if previous.get("verified_artifact") else {})}
        state["revision"] = REVISION
        state["attempt"] += 1
        state["status"] = "RUNNING"
        save(path, state)
        group_used = {}
        for task in tasks:
            if state["tasks"].get(task.name, {}).get("ok"):
                if isolated_collect:
                    from scripts.weekly_items import verify_artifact
                    try:
                        verify_artifact(state["tasks"][task.name])
                    except Exception as exc:
                        state.update(status="BLOCKED_ITEM_ARTIFACT_DRIFT",error=str(exc),active_task=task.name)
                        save(path,state)
                        return public_result(state,path)
                continue
            if task.parent in {'fundamentals', 'listing'} and group_used.get(task.parent, 0) >= 1500:
                state['tasks'][task.name] = {'ok':False, 'issues':[{'reason':'group_budget_exhausted'}],
                    'failure_class':'RETRY_SOURCE'}
                continue
            if not verify_freeze_manifest(freeze_manifest)["ok"]:
                state["status"] = "BLOCKED_PLUGIN_OR_SKILL_DRIFT"
                save(path, state)
                return public_result(state, path)
            if not window_allowed(as_of, clock()):
                state["status"] = "AUTHORIZATION_REQUIRED_CROSS_WEEK"
                save(path, state)
                return public_result(state, path)
            state["active_task"] = task.name
            save(path, state)
            collected_after = datetime.now(TZ).isoformat()
            receipt_file = folder / "attempts" / str(state["attempt"]) / (task.name + ".json")
            snapshot = None
            print(f"[QWL item] attempt={state['attempt']} start={task.name}",file=__import__('sys').stderr,flush=True)
            try:
                selected = [g for g in state.get("gaps", []) if g.get("domain") in task.domains]
                if isolated_collect:
                    begun = time.monotonic()
                    if task.parent in {'fundamentals', 'listing'}:
                        task.timeout_seconds = min(180 if task.parent == 'fundamentals' else 300,
                            max(.1, 1500 - group_used.get(task.parent, 0)))
                    try:
                        value,snapshot = isolated_collect(task,receipt_file)
                    finally:
                        group_used[task.parent] = group_used.get(task.parent, 0) + time.monotonic() - begun
                        task.timeout_seconds = 0
                else:
                    value = retry_selected(task, selected) if selected and retry_selected else task.call()
                if value.get('history_sync', {}).get('price_basis_blocked'):
                    from scripts.weekly_isolation import IsolationSafetyError
                    raise IsolationSafetyError('standardized adjustment basis drift; do not retry to bypass')
                issues = task_issues(value)
                # Historical market differences are explicitly accepted only
                # after the offline audit proves they predate this week.
                if task.name == "market" and value.get("gate", {}).get("passed") is True:
                    history = value.get("history_sync", {})
                    if history.get("status") in {"OK", "CONFLICT"}:
                        issues = []
                receipt = {"ok": not issues, "issues": issues, "result": value}
            except Exception as exc:
                from scripts.weekly_isolation import IsolationSafetyError
                if isinstance(exc,IsolationSafetyError):
                    state.update(status="BLOCKED_ITEM_SAFETY",error=str(exc))
                    save(path,state)
                    return public_result(state,path)
                receipt = {"ok": False, "issues": [{"reason": type(exc).__name__, "value": str(exc)[:1000]}]}
            if snapshot is None:
                snapshot = receipt_file.with_suffix(".fallback.tar.gz")
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                with snapshot.open("xb") as handle:
                    with tarfile.open(fileobj=handle, mode="w:gz") as archive:
                        for source, relative in _source_files(runtime, collected_after):
                            archive.add(source, arcname=str(relative), recursive=False)
            receipt["provider_snapshot"] = str(snapshot)
            receipt["failure_class"] = classify_task_failure(receipt)
            state["tasks"][task.name] = {"ok": receipt["ok"], "issues": receipt["issues"],
                                          "receipt": str(receipt_file),
                                          "failure_class": receipt["failure_class"]}
            if receipt["ok"] and task.output:
                from scripts.weekly_items import digest
                artifact_snapshot = snapshot
                # Margin preparation may retain the historical SSE prefix while
                # the raw immutable response contains field enrichments. Bind
                # the accepted, crash-safe bytes, not those discarded candidates.
                prepared_path = receipt_file.with_suffix('.prepared.json')
                if task.parent == 'margin_history' and prepared_path.is_file():
                    prepared = json.loads(prepared_path.read_text())
                    artifact_snapshot = Path(prepared['snapshot'])
                with tarfile.open(artifact_snapshot) as archive:
                    data = archive.extractfile(task.output).read()
                if (task.parent == 'margin_history' and prepared_path.is_file()
                        and digest(data) != prepared['outputs'][task.output]['sha256']):
                    raise ValueError('accepted margin artifact SHA mismatch')
                state["tasks"][task.name]["verified_artifact"] = {
                    "archive":str(artifact_snapshot),"member":task.output,"sha256":digest(data)}
                receipt["verified_artifact"] = state["tasks"][task.name]["verified_artifact"]
            save(receipt_file, receipt)
            # Re-run normalizers only after actual upstream work, not every
            # waiting poll or a gap in an unrelated successful snapshot
            if "normalization" not in task.domains and task.name != "benchmarks":
                for later in tasks:
                    if "normalization" in later.domains:
                        state["tasks"].setdefault(later.name,{})["ok"] = False
            if task.name != "benchmarks" and set(task.domains) & {
                    'history', 'dividends', 'dividends_hk', 'announcements', 'hk_action_evidence', 'fx', 'normalization'}:
                for later in tasks:
                    if later.name == "benchmarks":
                        state["tasks"].setdefault(later.name,{})["ok"] = False
            print(f"[QWL item] finish={task.name} ok={receipt['ok']} issues={len(receipt['issues'])}",file=__import__('sys').stderr,flush=True)
            save(path, state)
            if receipt["failure_class"].startswith("BLOCKED_"):
                state.update(status=receipt["failure_class"], active_task=task.name,
                             error=receipt.get("result", {}).get("error", "source response rejected")[:1500])
                save(path, state)
                return public_result(state, path)
        try:
            coverage, gate = audit_source()
        except Exception as exc:
            state.update(status="BLOCKED_AUDIT_EXCEPTION", error=str(exc)[:1500])
            save(path, state)
            return public_result(state, path)
        save(folder / "attempts" / str(state["attempt"]) / "coverage.json", coverage)
        save(folder / "attempts" / str(state["attempt"]) / "acceptance.json", gate)
        state["gaps"] = gate.get("issues", [])
        state["margin_pending"] = gate.get("margin_pending", [])
        state["margin_pending_tasks"] = [
            {"task": task.name, "receipt": state["tasks"][task.name].get("receipt"),
             "issues": state["tasks"][task.name].get("issues", [])}
            for task in tasks
            if deferred_margin_task(task, state["tasks"].get(task.name))
        ]
        state["historical_conflicts"] = gate.get("historical_conflicts", [])
        state['historical_basis_unverified'] = gate.get('historical_basis_unverified', [])
        state["known_irreducible_gaps"] = gate.get("known_irreducible_gaps", [])
        drift = verify_existing(database, state["baseline"])
        if drift:
            state.update(status="BLOCKED_FORMAL_HISTORY_DRIFT", changed_files=drift)
            save(path, state)
            return public_result(state, path)
        # Old-only history warnings must not rerun successful market requests.
        pending = [task.name for task in tasks
                   if (task.name in state["tasks"] and not state["tasks"][task.name]["ok"]
                       and not deferred_margin_task(task, state["tasks"][task.name]))]
        state.pop('active_task', None)  # All planned worker processes have exited.
        dependency = market_fx_retry_evidence(state, folder, runtime, as_of, allow_pending=True)
        if dependency:
            # Keep failed receipts and unresolved findings, but retry collection.
            # The publisher is never reachable from this branch.
            state['dependency_retry'] = dependency
            queue_retry(state, tasks, clock())
        elif any(i.get("blocking") for i in state["gaps"]):
            state["status"] = "BLOCKED_ACCEPTANCE"
        elif state["gaps"] or pending:
            queue_retry(state, tasks, clock())
        else:
            if not window_allowed(as_of, clock()):
                state["status"] = "AUTHORIZATION_REQUIRED_CROSS_WEEK"
            else:
                state["status"] = "PUBLISHING"
                save(path, state)
                try:
                    publication = publish(state["started_at"])
                except Exception as exc:
                    publication = {"status": "FAILED", "error": str(exc)[:1500]}
                state["publication"] = publication
                if (publication.get("status") == "PUBLISHED"
                        and publication.get("formal_acceptance", {}).get("passed") is True
                        and all(publication.get(k) == 0 for k in (
                            "formal_existing_files_rewritten", "historical_revisions_published",
                            "historical_backfills_published"))):
                    state.update(status="COMPLETE", completed_at=clock().isoformat())
                else:
                    pre = publication.get("prepublication_acceptance", {})
                    recoverable = (pre.get("passed") is False and pre.get("issues")
                                   and not any(i.get("blocking", True) for i in pre["issues"])
                                   and publication.get("committed_files_remaining") == 0)
                    if recoverable:
                        state["gaps"] = pre["issues"]
                        queue_retry(state, tasks, clock())
                    else:
                        state["status"] = "BLOCKED_PUBLICATION"
        state.pop("active_task", None)
        save(path, state)
        return public_result(state, path)
