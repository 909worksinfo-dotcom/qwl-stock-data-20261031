"""Verified item checkpoints; provider implementations remain in canonical QWL."""
from __future__ import annotations

import hashlib
import io
import json
import math
import tarfile
from datetime import date, timedelta
from pathlib import Path

from scripts.weekly_tasks import Task, task_issues
from scripts.weekly_guard import _parse_day

# Only genuinely per-symbol native endpoints are split. Market-wide endpoints
# remain single requests and retain their native in-memory fixed-60 filter
SPLIT = {
    "fundamentals": (None, "fundamentals"),
    "listing": (None, "listing"),
    "statements_a": ("A", "statements"),
    "statements_hk": ("HK", "statements_hk"),
    "dividends": ("A", "dividends"),
    "share_changes": ("A", "share_changes"),
    "announcements_history": (None, "announcements"),
    "unlock_announcements": ("A", "announcements"),
    "fund_flow_history": ("A", "fund_flow_ths"),
    "margin_history": ("A", "margin"),
    "microstructure": (None, "microstructure"),
}
SPECIAL = {
    "byd_intraday": "cache/byd_historical_intraday/002594_60m.parquet",
    "anker_intraday": "cache/anker_historical_intraday/300866_60m.parquet",
    "byd_operating": "cache/byd_operating/002594.json",
    "ev_operating": "cache/ev_operating/byd_related.json",
}

def digest(data):
    return hashlib.sha256(data).hexdigest()

def working_path(root, relative):
    rel = Path(relative)
    if rel.is_absolute() or ".." in rel.parts or rel.parts[0] not in {"cache", "state"}:
        raise ValueError("unsafe artifact path")
    if rel.parts[0] == "state" and rel.parts[1] not in {"history", "snapshots"}:
        raise ValueError("non-data state output")
    return root / ((Path(".cache") / Path(*rel.parts[1:])) if rel.parts[0] == "cache" else rel)

def expand_tasks(tasks, universe, runtime, as_of=None):
    expanded = []
    for task in tasks:
        if task.name in SPLIT:
            market, dataset = SPLIT[task.name]
            for stock in universe.stocks:
                if market and str(stock.market.value) != market:
                    continue
                expected = ""
                if as_of and task.name in {"margin_history","fund_flow_history","microstructure"}:
                    from qwl.datasources.extended import _expected_symbol_session
                    expected = _expected_symbol_session(stock,date.fromisoformat(as_of)).isoformat()
                expanded.append(Task(task.name + "@" + stock.symbol, task.domains, task.call,
                    parent=task.name, symbol=stock.symbol,
                    output=f"cache/{dataset}/{stock.ak_symbol}.json",expected_date=expected))
        elif task.name == "specialized":
            for name, output in SPECIAL.items():
                expanded.append(Task(name, task.domains, task.call, parent="specialized", output=output))
            # Read canonical document identities, not another discovery source
            from qwl.datasources.byd_research_text import _source_records
            for source in _source_records():
                url = source["report_url"]
                expanded.append(Task("byd_research_text@" + digest(url.encode()), task.domains,
                    task.call, parent="specialized", symbol="002594.SZ", document=url,
                    output="cache/byd_research_text/002594.parquet"))
            expanded.append(Task("research_catalog",task.domains,task.call,parent="specialized"))
        else:
            expanded.append(task)
    return expanded

def read_payload(data, output):
    if output.endswith(".parquet"):
        import pandas as pd
        return pd.read_parquet(io.BytesIO(data))
    return json.loads(data)

def validate_item(task, data, as_of, *, allow_margin_pending=False):
    """Structural/source evidence check; final native/PIT/weekly gates still run."""
    value = read_payload(data, task.output)
    if task.document:
        rows = value[value.report_url == task.document]
        if len(rows) != 1:
            raise ValueError("missing/duplicate research identity")
        row = rows.iloc[0]
        if (row.parse_status != "parsed" or row.symbol != "002594.SZ"
                or len(str(row.document_sha256)) != 64 or int(row.document_bytes) <= 0
                or int(row.text_length) <= 0):
            raise ValueError("research content not parsed and hash-anchored")
        return rows
    if task.output.endswith(".parquet"):
        if value.empty:
            raise ValueError("empty intraday artifact")
        if "timestamp" in value:
            latest = str(value.timestamp.max())[:10]
            if latest != as_of:
                raise ValueError("stale/future intraday artifact")
        return value
    if not isinstance(value, dict):
        raise ValueError("non-object source artifact")
    if task.symbol and task.parent != 'fundamentals' and value.get("symbol") != task.symbol:
        raise ValueError("source symbol mismatch")
    if task_issues(value):
        raise ValueError("source artifact contains unresolved error")
    if task.parent == 'fundamentals':
        from datetime import datetime, timezone
        observed = datetime.fromisoformat(value['_cached_iso'])
        if (observed.tzinfo is None or not date.fromisoformat(as_of) - timedelta(days=4) <= observed.date()
                <= date.fromisoformat(as_of) + timedelta(days=2)
                or observed > datetime.now(timezone.utc)
                or not math.isfinite(float(value['_cached_at']))
                or abs(observed.timestamp() - float(value['_cached_at'])) > 2):
            raise ValueError('financial observation clock mismatch')
        body = value['data']
        expected = 'stock_financial_hk_analysis_indicator_em' if task.symbol.endswith('.HK') else 'stock_financial_analysis_indicator'
        if body.get('source') != expected or not body.get('reports'):
            raise ValueError('financial source or reports missing')
        days = []
        for row in body['reports']:
            days.append(date.fromisoformat(row['date']))
            if not row.get('fields') or any(isinstance(v, float) and not math.isfinite(v) for v in row['fields'].values()):
                raise ValueError('financial fields missing/nonfinite')
        if len(set(days)) != len(days) or max(days) > date.fromisoformat(as_of):
            raise ValueError('financial report date mismatch')
    if task.parent == 'listing':
        from datetime import datetime
        observed = datetime.fromisoformat(value['captured_at'])
        from zoneinfo import ZoneInfo
        observed = observed if observed.tzinfo else observed.replace(tzinfo=ZoneInfo('Asia/Shanghai'))
        market = 'HK' if task.symbol.endswith('.HK') else 'A'
        sources = {'stock_hk_daily(sina)'} if market == 'HK' else {'stock_zh_a_daily(sina)', 'ths_public_line'}
        if (value.get('market') != market or value.get('source') not in sources
                or value.get('authority') != 'observed_first_trade+current_tencent_quote'
                or value.get('currently_listed') is not True or value.get('effective_to') is not None
                or observed > datetime.now(ZoneInfo('Asia/Shanghai'))
                or not date.fromisoformat(as_of)-timedelta(days=4) <= observed.date() <= date.fromisoformat(as_of)+timedelta(days=2)):
            raise ValueError('listing identity, authority or observation mismatch')
        days = [date.fromisoformat(d) for d in value['positive_trade_dates_since_2023']]
        if (not days or days != sorted(set(days)) or days[-1] > date.fromisoformat(as_of)
                or date.fromisoformat(value['effective_from']) > days[0]):
            raise ValueError('listing trade-date range invalid')
    if task.parent in {"statements_a", "statements_hk"}:
        for section in ("income", "balance", "cashflow"):
            rows = value.get(section)
            if not isinstance(rows, list) or not rows:
                raise ValueError("missing statement section: " + section)
            seen = set()
            for row in rows:
                day = date.fromisoformat(row["report_date"][:10])
                if day > date.fromisoformat(as_of) or day in seen or not row.get("fields"):
                    raise ValueError("invalid/duplicate report period or missing fields")
                seen.add(day)
                if any(isinstance(x, float) and not math.isfinite(x) for x in row["fields"].values()):
                    raise ValueError("nonfinite financial field")
    if task.parent in {"fund_flow_history", "margin_history", "microstructure"}:
        field = "daily_history" if task.parent == "fund_flow_history" else "records"
        rows = value.get(field, [])
        if not rows and value.get("availability") == "no_reported_history":
            return value
        # Compare parsed dates only: never rewrite the supplier's date spelling
        # Native QWL accepts both compact and ISO dates, including in old caches
        days = [_parse_day(r.get("date")) for r in rows]
        expected = date.fromisoformat(task.expected_date or as_of)
        partial_margin = (allow_margin_pending and task.parent == 'margin_history'
                          and days and all(d is not None and d <= expected for d in days))
        if not partial_margin and (not days or any(d is None for d in days) or max(days) != expected):
            raise ValueError("current-week daily data still missing")
    return value

def prepare_margin_rows(task, data, previous, as_of):
    """Admit available daily rows, not a claim of complete source disclosure.

    Raw provider bytes remain immutable. Only documented SSE field extensions
    may differ from working rows; historical extensions are kept out of the
    accepted cache. All actual shared-value, date, PIT and schema changes fail.
    """
    from decimal import Decimal
    from qwl.datasources.margin_contract import FULL_FIELDS, SSE_FIELDS, compatible_subset, day
    import exchange_calendars
    import pandas as pd
    if task.parent != 'margin_history' or set(task.domains) != {'margin'}:
        raise ValueError('margin-only preparation scope')
    value, old = json.loads(data), json.loads(previous)
    if (value.get('symbol') != task.symbol or old.get('symbol') != task.symbol
            or value.get('source') != 'RPTA_WEB_RZRQ_GGMX'
            or value.get('data_shape') != 'per_stock_daily_history'
            or value.get('availability') != 'available' or task_issues(value)):
        raise ValueError('margin source contract invalid')
    end = date.fromisoformat(as_of)
    start = end - timedelta(days=end.weekday())
    expected = date.fromisoformat(task.expected_date or as_of)
    if not start <= expected <= end:
        raise ValueError('margin expected date outside week')
    def indexed(rows):
        if not isinstance(rows, list):
            raise ValueError('margin records schema invalid')
        result = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('margin row schema invalid')
            key = day(row.get('date'))
            if key in result or date.fromisoformat(key) > expected:
                raise ValueError('margin duplicate/future date invalid')
            if any(isinstance(x, float) and not math.isfinite(x) for x in row.values()):
                raise ValueError('margin nonfinite value invalid')
            result[key] = row
        return result
    incoming, prior = indexed(value.get('records')), indexed(old.get('records'))
    conflicts = value.get('history_conflicts', [])
    if not isinstance(conflicts, list):
        raise ValueError('margin conflict schema invalid')
    for conflict in conflicts:
        key = day(conflict.get('date'))
        if date.fromisoformat(key) >= start:
            raise ValueError('current-week margin conflict')
        if (not isinstance(conflict.get('previous'), dict)
                or not isinstance(conflict.get('candidate'), dict)
                or day(conflict['previous'].get('date')) != key
                or day(conflict['candidate'].get('date')) != key
                or incoming.get(key) != conflict['previous']):
            raise ValueError('margin historical conflict evidence invalid')
    restored = []
    for key, row in prior.items():
        candidate = incoming.get(key)
        if candidate == row:
            continue
        known_extension = (task.symbol.endswith('.SH')
            and old.get('source') in {'stock_margin_detail_sse', 'RPTA_WEB_RZRQ_GGMX'}
            and set(row) == SSE_FIELDS | {'date'} and isinstance(candidate, dict)
            and set(candidate) == FULL_FIELDS | {'date', 'market'}
            and candidate.get('market') == '融资融券_沪证'
            and compatible_subset(candidate, row))
        if not known_extension:
            raise ValueError('existing margin record changed or removed')
        if date.fromisoformat(key) < start:
            incoming[key] = row
            restored.append(key)
    signed = {'margin_net_buy', 'short_net_sell_volume'}
    for key, row in incoming.items():
        if date.fromisoformat(key) < start:
            continue
        if set(row) != FULL_FIELDS | {'date', 'market'}:
            raise ValueError('current margin full fields/schema/PIT invalid')
        market = '融资融券_沪证' if task.symbol.endswith('.SH') else '融资融券_深证'
        if row['market'] != market:
            raise ValueError('margin market/unit contract invalid')
        for field in FULL_FIELDS:
            number = row[field]
            if (isinstance(number, bool) or not isinstance(number, (int, float))
                    or not math.isfinite(number) or (field not in signed and number < 0)):
                raise ValueError('current margin numeric value invalid: ' + field)
        for total, left, right, sign in (
                ('margin_net_buy', 'margin_buy', 'margin_repay', -1),
                ('short_net_sell_volume', 'short_sell_volume', 'short_repay_volume', -1),
                ('margin_short_balance', 'margin_balance', 'short_balance', 1)):
            if abs(Decimal(str(row[total])) - Decimal(str(row[left]))
                   - sign * Decimal(str(row[right]))) > Decimal('0.02'):
                raise ValueError('current margin arithmetic value invalid: ' + total)
    value['records'] = list(incoming.values())
    prepared = json.dumps(value, ensure_ascii=False, indent=2).encode()
    validate_item(task, prepared, as_of, allow_margin_pending=True)
    required = exchange_calendars.get_calendar('XSHG').sessions_in_range(
        pd.Timestamp(start), pd.Timestamp(expected))
    pending = [{'domain':'margin', 'symbol':task.symbol, 'date':stamp.date().isoformat(),
                'reason':'waiting_for_source', 'blocking':False}
               for stamp in required if stamp.date().isoformat() not in incoming]
    return prepared, {'margin_pending':pending, 'historical_conflict_count':len(conflicts),
                      'historical_sse_extensions_preserved':restored,
                      'available_current_dates':[key for key in incoming if date.fromisoformat(key) >= start],
                      'historical_records_published':0}

def old_only_conflict(task, data, previous, as_of):
    if task.parent not in {"margin_history","fund_flow_history","microstructure"}:
        return False
    value = validate_item(task,data,as_of)
    field = "daily_history" if task.parent == "fund_flow_history" else "records"
    conflict_field = "daily_history_conflicts" if task.parent == "fund_flow_history" else "history_conflicts"
    conflicts = value.get(conflict_field,[])
    start = date.fromisoformat(as_of)-timedelta(days=4)
    if not conflicts or any(date.fromisoformat(str(r["date"])[:10]) >= start for r in conflicts):
        return False
    old = json.loads(previous).get(field,[])
    incoming = value.get(field,[])
    by_date = {r["date"]:r for r in incoming}
    return len(by_date)==len(incoming) and all(by_date.get(r["date"])==r for r in old)

def business_equal(task, a, b):
    def cleaned(value):
        if isinstance(value, dict):
            return {k: cleaned(v) for k,v in value.items() if k not in {
                "cached_at", "captured_at", "fetched_at", "updated_at",
                "available_on", "source_published_on", "source_published_at",
                "timestamp_precision", "availability_status", "fields_cny", "currency_conversion"}}
        if isinstance(value, list):
            return [cleaned(v) for v in value]
        return value
    if task.output.endswith(".parquet"):
        a, b = read_payload(a,task.output), read_payload(b,task.output)
        if task.document:
            a, b = (v[v.report_url == task.document] for v in (a,b))
        return a.reset_index(drop=True).equals(b.reset_index(drop=True))
    a,b = cleaned(json.loads(a)),cleaned(json.loads(b))
    if task.parent in {"announcements_history","unlock_announcements"}:
        # Another completed announcement subtask may have added records
        # Require all previously accepted evidence to remain unchanged
        old = {json.dumps(r,sort_keys=True,ensure_ascii=False) for r in a.get("announcements",[])}
        current = {json.dumps(r,sort_keys=True,ensure_ascii=False) for r in b.get("announcements",[])}
        return a.get("symbol") == b.get("symbol") and old <= current
    return a == b

def migrate_verified(state, tasks, folder, runtime, as_of):
    """Never accept a legacy 'cached' string without immutable matching content.

    Migration changes only external job bookkeeping, not working or formal data
    Unverifiable successes are left pending with an explicit migration reason
    """
    from scripts.weekly_runner import save
    if state.get("item_schema", 0) >= 2:
        # A later contract may split one previously batched task. Its failed
        # aggregate checkpoint must not remain pending after per-symbol items
        # pass; keep the old result for audit, never claim it as item success.
        split_parents = {task.parent for task in tasks if task.parent}
        for key in split_parents & state["tasks"].keys():
            state.setdefault("legacy_tasks", {})[key] = state["tasks"].pop(key)
        return
    folder.mkdir(parents=True,exist_ok=True)
    before = folder / "checkpoint.before-items.json"
    if not before.exists():
        with before.open("x") as handle:
            json.dump(state,handle,ensure_ascii=False,indent=2)
    legacy = dict(state["tasks"])
    receipts = {}
    payloads = {}
    evidence = []
    for task in tasks:
        if state['tasks'].get(task.name, {}).get('ok'):
            continue
        if not task.parent:
            continue
        key = task.parent
        if key not in receipts:
            receipts[key] = []
            for file in sorted((folder/"attempts").glob(f"*/{key}.json"),
                               key=lambda p:int(p.parent.name), reverse=True):
                try:
                    blob = json.loads(file.read_text())
                    archive = file.with_suffix(".tar.gz")
                    if archive.is_file():
                        receipts[key].append((file, blob, archive))
                except (ValueError, OSError):
                    continue
        reasons = []
        for file, receipt, archive in receipts[key]:
            result = receipt.get("result", {})
            item = result.get("byd_research_text" if task.document else task.name) if key == "specialized" else result.get(task.symbol)
            # A mixed PDF receipt can contain independently parsed documents
            if item is None or (not task.document and task_issues(item)):
                continue
            try:
                from datetime import datetime
                start = date.fromisoformat(as_of) - timedelta(days=4)
                observed = datetime.fromtimestamp(file.stat().st_mtime).date()
                if not start <= observed <= date.fromisoformat(as_of) + timedelta(days=2):
                    raise ValueError("receipt outside authorized week")
                cache_key = (str(archive),task.output)
                if cache_key not in payloads:
                    with tarfile.open(archive) as tar:
                        member = tar.getmember(task.output)
                        if not member.isfile():
                            raise ValueError("non-file artifact")
                        payloads[cache_key] = tar.extractfile(member).read()
                data = payloads[cache_key]
                validate_item(task,data,as_of)
                current = working_path(runtime,task.output).read_bytes()
                validate_item(task,current,as_of)
                if not business_equal(task,data,current):
                    raise ValueError("legacy snapshot differs from current working content; not imported")
                state["tasks"][task.name] = {"ok": True, "issues": [], "receipt":str(file),
                    "verified_artifact": {"archive":str(archive), "member":task.output,
                                          "sha256":digest(data)}, "migrated":True}
                evidence.append({"item":task.name,"status":"verified","receipt":str(file)})
                break
            except (ValueError, OSError, KeyError, TypeError) as exc:
                reasons.append(str(exc))
        else:
            evidence.append({"item":task.name,"status":"pending","reasons":reasons[:3]})
    split_parents = {task.parent for task in tasks if task.parent}
    state.setdefault('legacy_tasks', {}).update({k:v for k,v in legacy.items() if k in split_parents})
    for key in split_parents:
        state["tasks"].pop(key,None)
    # Old broad domain invalidation unnecessarily dirtied THS snapshots
    old = legacy.get("fund_flow",{})
    if not old.get("ok") and not old.get("issues") and old.get("receipt"):
        receipt = json.loads(Path(old["receipt"]).read_text())
        if receipt.get("ok"):
            state["tasks"]["fund_flow"] = {**old,"ok":True}
    state["item_schema"] = 2
    state["migration"] = {"verified":sum(i["status"]=="verified" for i in evidence),
                          "pending":sum(i["status"]=="pending" for i in evidence),
                          "evidence":str(folder/"item-migration.json")}
    save(folder/"item-migration.json",evidence)

def verify_artifact(entry):
    artifact = entry.get("verified_artifact")
    if not artifact:
        return
    with tarfile.open(artifact["archive"]) as archive:
        data = archive.extractfile(artifact["member"]).read()
    if digest(data) != artifact["sha256"]:
        raise ValueError("accepted item snapshot drift")
