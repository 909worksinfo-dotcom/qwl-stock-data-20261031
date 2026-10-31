"""Process-group deadlines and immutable artifacts before accepting worker data."""
from __future__ import annotations
import io
import json
import os
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime,timedelta
from numbers import Number
from pathlib import Path

from scripts.weekly_guard import _source_files, _SYMBOL_SCOPED_DATASETS
from scripts.weekly_items import digest, working_path, validate_item, old_only_conflict, prepare_margin_rows
from scripts.weekly_tasks import task_issues

class IsolationSafetyError(ValueError):
    pass

def item_deadline_seconds(task):
    """Keep ordinary items bounded while honoring the native statement budget."""
    if task.parent in {"statements_a", "statements_hk"}:
        return 420
    if getattr(task, 'timeout_seconds', 0):
        return task.timeout_seconds
    if task.parent == 'fundamentals':
        return 180
    if task.parent == 'listing':
        return 300
    return 150 if task.symbol else 1500

def document_rows_equal(prior, incoming):
    """Accept a repeated parsed document only when all non-null evidence agrees."""
    import pandas as pd
    if prior.empty or incoming.empty or "document_sha256" not in prior or "document_sha256" not in incoming:
        return False
    if not (prior.get("parse_status").eq("parsed").all()
            and incoming.get("parse_status").eq("parsed").all()):
        return False
    left_hashes = {str(value) for value in prior.document_sha256.dropna() if len(str(value)) == 64}
    right_hashes = {str(value) for value in incoming.document_sha256.dropna() if len(str(value)) == 64}
    if not left_hashes or left_hashes != right_hashes:
        return False
    def normalized(frame):
        records = []
        for raw in frame.to_dict(orient="records"):
            row = {}
            for key, value in raw.items():
                if bool(pd.isna(value)):
                    continue
                if hasattr(value, "item"):
                    value = value.item()
                if isinstance(value, Number) and not isinstance(value, bool):
                    try:
                        value = {"__number__": format(Decimal(str(value)).normalize(), "f")}
                    except InvalidOperation:
                        return None
                row[str(key)] = value
            records.append(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str))
        return sorted(records)
    return normalized(prior) == normalized(incoming)

def stop_group(process):
    # SIGKILL also stops late native threads and all non-detached descendants
    try:
        os.killpg(process.pid,signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait(timeout=10)

def wait_worker(process, receipt, deadline):
    end = time.monotonic() + deadline
    try:
        while not receipt.exists() and process.poll() is None and time.monotonic() < end:
            time.sleep(.2)
        if receipt.exists():
            return json.loads(receipt.read_text())
        return {"status":"timeout" if process.poll() is None else "FAILED",
                "error":"worker deadline/exit before durable receipt"}
    finally:
        stop_group(process)

def atomic_bytes(path, data):
    path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(data); handle.flush(); os.fsync(handle.fileno())
    os.replace(temporary,path)

def source_files(root):
    yield from _source_files(root,None)
    for path in (root/"state/snapshots").rglob("*"):
        if path.is_file():
            yield path,path.relative_to(root)

def allowed_relative(rel, codes):
    if rel.parts[0] == "cache" and len(rel.parts)>2 and rel.parts[1] in _SYMBOL_SCOPED_DATASETS:
        return rel.stem in codes
    return True

def seed_formal_market_history(workspace, database, all_codes):
    """Use the same formal base+versions for worker and bridge-proof replay.

    Only private copies are materialized; old runtime checkpoints, formal files,
    quarantined candidates and prior-week exception policies are never adopted.
    """
    from scripts.weekly_guard import effective_parquet
    from qwl.datasources.price_basis import frame
    stems = sorted(code for code in all_codes if '_' in code)
    if len(stems) != 60:
        raise IsolationSafetyError('formal market view requires exactly 60 fixed stems')
    for stem in stems:
        for suffix in ('', '_raw'):
            relative = Path('state/history') / f'{stem}{suffix}.parquet'
            current = frame(effective_parquet(database / relative,
                database / 'state/published_versions' / relative))
            if current.empty:
                raise IsolationSafetyError('missing formal market history: ' + str(relative))
            output = io.BytesIO()
            current.to_parquet(output)
            atomic_bytes(working_path(workspace, relative), output.getvalue())

def recover_prepared(runtime, prepared):
    """A crash mid working-cache promotion can be completed, never blind replayed."""
    with tarfile.open(prepared["snapshot"]) as archive:
        for rel, info in prepared["outputs"].items():
            path = working_path(runtime,rel)
            current = digest(path.read_bytes()) if path.exists() else None
            if current not in {info["before"],info["sha256"]}:
                raise IsolationSafetyError("working-cache promotion conflict: " + rel)
            data = archive.extractfile(rel).read()
            if digest(data) != info["sha256"]:
                raise IsolationSafetyError("prepared artifact hash mismatch")
        for rel, info in prepared["outputs"].items():
            path = working_path(runtime,rel)
            if not path.exists() or digest(path.read_bytes()) != info["sha256"]:
                atomic_bytes(path,archive.extractfile(rel).read())

def seed_partial_series(workspace, receipt_file, as_of, all_codes):
    """Restore only same-week private per-series artifacts, never formal rows"""
    prefix = f'state/snapshots/{as_of}/series/'
    for receipt in sorted(receipt_file.parent.parent.glob('*/market.json')):
        value = json.loads(receipt.read_text())
        snapshot = Path(value['provider_snapshot'])
        if not snapshot.resolve().is_relative_to(receipt_file.parent.parent.resolve()) or not snapshot.is_file() or snapshot.is_symlink():
            raise IsolationSafetyError('partial series snapshot outside job/missing')
        with tarfile.open(snapshot) as archive:
            names = set()
            for member in archive.getmembers():
                if not member.name.startswith(prefix):
                    continue
                rel = Path(member.name)
                tail = member.name[len(prefix):].split('/')
                if (not member.isreg() or member.name in names or '..' in rel.parts or len(tail) != 3
                        or tail[0].split('_')[0] not in all_codes or tail[1] not in {'none', 'qfq'}):
                    raise IsolationSafetyError('partial series member scope/duplicate')
                names.add(member.name)
                data = archive.extractfile(member).read()
                target = workspace / rel
                if target.exists():
                    if target.read_bytes() != data:
                        raise IsolationSafetyError('partial successful series evidence drift')
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with target.open('xb') as handle:
                        handle.write(data)


def collect_isolated(task, runtime, as_of, receipt_file, all_codes, guard=lambda:True, database=None):
    from scripts.weekly_runner import save
    snapshot = receipt_file.with_suffix(".tar.gz")
    prepared_path = receipt_file.with_suffix(".prepared.json")
    for prior in sorted(receipt_file.parent.parent.glob(task.name and "*/" + task.name + ".prepared.json"),
                        key=lambda p:int(p.parent.name),reverse=True):
        final_receipt = prior.with_name(task.name + ".json")
        if not final_receipt.exists():
            prepared = json.loads(prior.read_text())
            if not guard():
                raise IsolationSafetyError("guard changed before interrupted item recovery")
            recover_prepared(runtime,prepared)
            return prepared["result"],Path(prepared["snapshot"])
    if prepared_path.exists():
        prepared = json.loads(prepared_path.read_text())
        recover_prepared(runtime,prepared)
        return prepared["result"],snapshot
    codes = {task.symbol.split(".")[0]} if task.symbol and not task.document else all_codes
    plugin = Path(__file__).resolve().parents[1]
    receipt_file.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="qwl-item-") as temp:
        workspace = Path(temp)
        shutil.copytree(runtime/"config",workspace/"config")
        (workspace/"src").symlink_to(runtime/"src",target_is_directory=True)
        (workspace/".venv").symlink_to(runtime/".venv",target_is_directory=True)
        before = {}
        # Snapshot history is required only by market generation/normalization
        candidates = source_files(runtime) if not task.parent else _source_files(runtime,None)
        for source,rel in candidates:
            if not allowed_relative(rel,codes):
                continue
            target = working_path(workspace,rel)
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(source,target)
            before[str(rel)] = digest(source.read_bytes())
        if task.name == 'market' and database is not None:
            seed_formal_market_history(workspace, database, all_codes)
        if task.name == 'market':
            seed_partial_series(workspace, receipt_file, as_of, all_codes)
        spec = {"name":task.name,"parent":task.parent,"symbol":task.symbol,
                "document":task.document,"as_of":as_of,"parent_pid":os.getpid()}
        save(workspace/"spec.json",spec)
        env = {**os.environ,"QWL_HOME":str(workspace),"QWL_DATA_MCP_REEXEC":"1",
               "PYTHONDONTWRITEBYTECODE":"1",
               "PYTHONPATH":os.pathsep.join(filter(None,[str(plugin),str(runtime/"src"),os.environ.get("PYTHONPATH")]))}
        # Never inherit a caller-supplied/quarantine-derived input view
        env.pop("QWL_BENCHMARK_INPUT_HOME", None)
        env.pop("QWL_BENCHMARK_AS_OF", None)
        env.pop("QWL_BRIDGE_FORMAL_READONLY", None)
        env.pop("QWL_BRIDGE_PRIOR_JOB_READONLY", None)
        if task.name == "market" and database is not None:
            from scripts.weekly_runner import job_dir
            prior_friday = datetime.fromisoformat(as_of).date() - timedelta(days=7)
            env["QWL_BRIDGE_FORMAL_READONLY"] = str(database.resolve())
            env["QWL_BRIDGE_PRIOR_JOB_READONLY"] = str(
                job_dir(runtime / "state/weekly_jobs", database, prior_friday.isoformat()) / "job.json")
        if task.name == "benchmarks":
            if database is None:
                raise IsolationSafetyError("formal database required for controlled benchmarks")
            from scripts.weekly_benchmark_inputs import prepare
            inputs = prepare(runtime, database, workspace / "benchmark_inputs", as_of)
            env.update(QWL_BENCHMARK_INPUT_HOME=str(inputs), QWL_BENCHMARK_AS_OF=as_of)
        with (receipt_file.with_suffix(".log")).open("x") as log:
            process = subprocess.Popen([str(runtime/".venv/bin/python"),"-B","-m",
                "scripts.weekly_worker",str(workspace/"spec.json")],cwd=workspace,env=env,
                stdout=log,stderr=log,start_new_session=True)
            from scripts.weekly_runner import TZ
            instant=datetime.now(TZ)
            midnight=(instant+timedelta(days=7-instant.weekday())).replace(hour=0,minute=0,second=0,microsecond=0)
            deadline = min(item_deadline_seconds(task),max(.1,(midnight-instant).total_seconds()))
            value = wait_worker(process,workspace/"result.json",deadline)
        # All writes have stopped before inspecting or archiving any output
        changed = {}
        for source,rel in source_files(workspace):
            if not allowed_relative(rel,codes):
                raise IsolationSafetyError("outside fixed item scope: " + str(rel))
            data = source.read_bytes()
            if (digest(data) != before.get(str(rel)) or str(rel)==task.output
                    or str(rel).startswith(f'state/snapshots/{as_of}/series/')):
                changed[str(rel)] = data
        if task.document:
            # The filtered discovery input belongs solely to the disposable
            # worker, never replace the full fixed-pool consensus cache
            changed.pop("cache/consensus/002594.json",None)
        with snapshot.open("xb") as handle, tarfile.open(fileobj=handle,mode="w:gz") as archive:
            for rel,data in changed.items():
                member = tarfile.TarInfo(rel); member.size=len(data)
                archive.addfile(member,io.BytesIO(data))
        with receipt_file.with_suffix(".provider.json").open("x") as handle:
            json.dump(value,handle,ensure_ascii=False,default=str)
        if value.get('history_sync', {}).get('price_basis_blocked'):
            raise IsolationSafetyError('standardized adjustment basis drift; immutable receipt: ' + str(snapshot))
        margin_preparation = None
        if (task.parent == 'margin_history' and task.output in changed
                and value in ({task.symbol:'conflict'}, {task.symbol:'stale'})):
            try:
                changed[task.output], margin_preparation = prepare_margin_rows(
                    task, changed[task.output], working_path(runtime, task.output).read_bytes(), as_of)
            except (ValueError, KeyError, TypeError) as exc:
                raise IsolationSafetyError('margin candidate safety: ' + str(exc)) from exc
            value = {'status':'OK', 'source_outcome':'available_margin_rows_verified',
                     'symbol':task.symbol, 'native_receipt':str(receipt_file.with_suffix('.provider.json')),
                     **margin_preparation}
        elif task.symbol and value == {task.symbol:"conflict"} and task.output in changed:
            try:
                previous=working_path(runtime,task.output).read_bytes()
                if old_only_conflict(task,changed[task.output],previous,as_of):
                    value={"status":"OK","source_outcome":"historical_conflict_preserved",
                           "symbol":task.symbol,
                           "native_receipt":str(receipt_file.with_suffix('.provider.json'))}
                    value["historical_warning"]="old conflicts remain unresolved; prior records unchanged"
            except (ValueError,KeyError,TypeError):
                pass
        issues = task_issues(value)
        if task.name == "market" and value.get("gate",{}).get("passed") and value.get("history_sync",{}).get("status") in {"OK","CONFLICT"}:
            issues = []
        if issues:
            return value,snapshot
        if not guard():
            raise IsolationSafetyError("freeze/week guard changed before working-cache acceptance")
        if task.output:
            if task.output not in changed:
                return {"status":"FAILED","error":"missing item output","provider_result":value},snapshot
            try:
                validate_item(task,changed[task.output],as_of,
                              allow_margin_pending=margin_preparation is not None)
            except (ValueError,KeyError,TypeError) as exc:
                return {"status":"FAILED","error":str(exc),"provider_result":value},snapshot
        if task.document:
            import pandas as pd
            old_path = working_path(runtime,task.output)
            old = pd.read_parquet(old_path) if old_path.exists() else pd.DataFrame()
            incoming = pd.read_parquet(io.BytesIO(changed[task.output]))
            if not old.empty:
                prior = old[old.report_url == task.document]
                if not prior.empty and (prior.parse_status == "parsed").any():
                    if not document_rows_equal(prior,incoming):
                        raise IsolationSafetyError("already parsed document content differs")
                    # Preserve the accepted working bytes exactly. The raw
                    # repeated response remains in the immutable provider snapshot.
                    changed[task.output] = old_path.read_bytes()
                    if isinstance(value,dict):
                        value = {**value,"source_outcome":"document_unchanged"}
                else:
                    incoming = pd.concat([old[old.report_url != task.document],incoming],ignore_index=True)
                    buffer = io.BytesIO();incoming.to_parquet(buffer,index=False)
                    changed[task.output] = buffer.getvalue()
            else:
                buffer = io.BytesIO();incoming.to_parquet(buffer,index=False)
                changed[task.output] = buffer.getvalue()
            # Raw single-document response above is immutable; merged material
            # is independently archived for crash-safe working-cache promotion
        accepted_snapshot = receipt_file.with_suffix(".accepted.tar.gz")
        with accepted_snapshot.open("xb") as handle, tarfile.open(fileobj=handle,mode="w:gz") as archive:
            for rel,data in changed.items():
                member=tarfile.TarInfo(rel);member.size=len(data)
                archive.addfile(member,io.BytesIO(data))
        outputs = {rel:{"before":digest(working_path(runtime,rel).read_bytes()) if working_path(runtime,rel).exists() else None,
                        "sha256":digest(data)} for rel,data in changed.items()}
        prepared = {"snapshot":str(accepted_snapshot),"outputs":outputs,"result":value}
        save(prepared_path,prepared)
        recover_prepared(runtime,prepared)
        return value,snapshot
