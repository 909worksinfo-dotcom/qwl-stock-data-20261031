"""Explicit post-repair checkpoint revalidation; no collection or real publication."""
import fcntl
import hashlib
import json
import tarfile
from pathlib import Path
from scripts.weekly_guard import verify_freeze_manifest, verify_existing, _sha_bytes
from scripts.weekly_items import digest
from scripts.weekly_runner import REVISION, job_dir, window_allowed, save, now, public_result, market_fx_retry_evidence

TOKEN = 'REVALIDATE_AFTER_REPAIR'
ITEM_TOKEN = 'RESUME_AFTER_ITEM_QUARANTINE'
MARKET_TOKEN = 'RESUME_MARKET_AFTER_QFQ_REPAIR'

def _inside(root, path):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False

def resume_item_safety(*, state_root, database, as_of, freeze_manifest,
                       quarantine_evidence, expected_checkpoint_sha,
                       authorization='', clock=now):
    """Resume one item only after its exact candidate is audibly quarantined."""
    database = database.resolve()
    path = job_dir(state_root, database, as_of) / 'job.json'
    if not path.is_file():
        return {'status':'NOT_STARTED','complete':False,'checkpoint_modified':False}
    lock_path = state_root / 'runtime.lock'
    if not lock_path.is_file():
        return {'status':'BLOCKED_MISSING_EXISTING_LOCK','checkpoint_modified':False}
    with lock_path.open('r+') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status':'RUNNING','checkpoint_modified':False}
        original = path.read_bytes()
        checkpoint_sha = hashlib.sha256(original).hexdigest()
        state = json.loads(original)
        base = {'complete':False,'checkpoint_modified':False,
                'database_modified':False,'collection_started':False,
                'checkpoint_sha256':checkpoint_sha}
        if state.get('status') != 'BLOCKED_ITEM_SAFETY':
            return {**base,'status':'BLOCKED_INELIGIBLE_ITEM_RECOVERY_STATE',
                    'checkpoint_status':state.get('status')}
        if authorization != ITEM_TOKEN or expected_checkpoint_sha != checkpoint_sha:
            return {**base,'status':'BLOCKED_ITEM_RECOVERY_AUTHORIZATION_OR_SHA'}
        freeze = verify_freeze_manifest(freeze_manifest)
        if not freeze.get('ok'):
            return {**base,'status':'BLOCKED_PLUGIN_OR_SKILL_DRIFT','freeze':freeze}
        if not state.get('baseline') or verify_existing(database,state['baseline']):
            return {**base,'status':'BLOCKED_FORMAL_HISTORY_DRIFT'}
        if not window_allowed(as_of,clock()):
            return {**base,'status':'AUTHORIZATION_REQUIRED_CROSS_WEEK'}
        evidence_path = Path(quarantine_evidence).expanduser().resolve()
        quarantine_root = (database / 'state/quarantine').resolve()
        if not _inside(quarantine_root,evidence_path) or not evidence_path.is_file():
            return {**base,'status':'BLOCKED_INVALID_QUARANTINE_EVIDENCE_PATH'}
        try:
            evidence = json.loads(evidence_path.read_text())
            active = state['active_task']
            source_relative = evidence['source_relative']
            provenance = evidence['provenance']
            candidate_info = evidence['candidate_artifact']
            formal_info = evidence['formal_baseline']
            candidate_path = (evidence_path.parent / candidate_info['path']).resolve()
            provider_snapshot = Path(provenance['provider_snapshot']).expanduser().resolve()
            formal_path = Path(formal_info['path']).expanduser().resolve()
        except (KeyError,TypeError,ValueError,json.JSONDecodeError):
            return {**base,'status':'BLOCKED_INVALID_QUARANTINE_EVIDENCE'}
        authorized = evidence.get('authorization',{})
        if (evidence.get('authorization_required') is not True
                or authorized.get('scope') != 'quarantine_only'
                or authorized.get('user_authorized') is not True
                or authorized.get('formal_promotion_authorized') is not False
                or provenance.get('active_task') != active
                or Path(provenance.get('job_checkpoint','')).expanduser().resolve() != path.resolve()):
            return {**base,'status':'BLOCKED_QUARANTINE_SCOPE_OR_TASK'}
        expected_formal = (database / source_relative).resolve()
        if (not _inside(quarantine_root,candidate_path) or not candidate_path.is_file()
                or formal_path != expected_formal or not formal_path.is_file()
                or not provider_snapshot.is_file()):
            return {**base,'status':'BLOCKED_QUARANTINE_ARTIFACT_PATH'}
        candidate_sha = _sha_bytes(candidate_path.read_bytes())
        if (candidate_sha != candidate_info.get('sha256')
                or _sha_bytes(formal_path.read_bytes()) != formal_info.get('sha256')
                or _sha_bytes(provider_snapshot.read_bytes()) != provenance.get('provider_snapshot_sha256')):
            return {**base,'status':'BLOCKED_QUARANTINE_ARTIFACT_DRIFT'}
        try:
            with tarfile.open(provider_snapshot) as archive:
                member = archive.extractfile(source_relative)
                provider_data = member.read() if member else None
        except (KeyError,tarfile.TarError,OSError):
            provider_data = None
        if provider_data is None or digest(provider_data) != candidate_sha:
            return {**base,'status':'BLOCKED_QUARANTINE_PROVIDER_MISMATCH'}
        from scripts.weekly_items import verify_artifact
        for name,item in state.get('tasks',{}).items():
            if item.get('ok'):
                try:
                    verify_artifact(item)
                except Exception as exc:
                    return {**base,'status':'BLOCKED_ITEM_ARTIFACT_DRIFT','task':name,
                            'error':str(exc)}
        if (path.read_bytes() != original or not verify_freeze_manifest(freeze_manifest).get('ok')
                or verify_existing(database,state['baseline'])):
            return {**base,'status':'BLOCKED_ITEM_RECOVERY_INPUT_DRIFT'}
        recovery_root = path.parent / 'item_recoveries'
        checkpoint_archive = recovery_root / (checkpoint_sha + '.checkpoint.json')
        recovery_receipt = recovery_root / (checkpoint_sha + '.receipt.json')
        recovery_root.mkdir(parents=True,exist_ok=True)
        if not checkpoint_archive.exists():
            with checkpoint_archive.open('xb') as handle:
                handle.write(original)
        elif checkpoint_archive.read_bytes() != original:
            return {**base,'status':'BLOCKED_ITEM_RECOVERY_ARCHIVE_CONFLICT'}
        receipt = {'ok':True,'issues':[],'result':{
            'status':'OK','source_outcome':'historical_candidate_quarantined',
            'active_task':active,'quarantine_evidence':str(evidence_path)},
            'provider_snapshot':str(provider_snapshot),
            'verified_artifact':{'archive':str(provider_snapshot),
                                 'member':source_relative,'sha256':candidate_sha}}
        if recovery_receipt.exists() and json.loads(recovery_receipt.read_text()) != receipt:
            return {**base,'status':'BLOCKED_ITEM_RECOVERY_RECEIPT_CONFLICT'}
        if not recovery_receipt.exists():
            save(recovery_receipt,receipt)
        state.setdefault('tasks',{})[active] = {
            'ok':True,'issues':[],'receipt':str(recovery_receipt),
            'verified_artifact':receipt['verified_artifact'],
            'disposition':'historical_candidate_quarantined',
            'quarantine_evidence':str(evidence_path)}
        state.pop('active_task',None)
        state.pop('error',None)
        state.update(revision=REVISION,status='WAITING_RETRY',next_retry_at=clock().isoformat(),
                     item_safety_recovery={'prior_checkpoint':str(checkpoint_archive),
                                           'prior_sha256':checkpoint_sha,
                                           'receipt':str(recovery_receipt),
                                           'quarantine_evidence':str(evidence_path),
                                           'at':clock().isoformat()})
        save(path,state)
        return {**public_result(state,path),'checkpoint_modified':True,
                'database_modified':False,'collection_started':False}


def resume_market_safety(*, state_root, database, as_of, freeze_manifest,
                         expected_checkpoint_sha, expected_provider_snapshot_sha,
                         expected_validation_sha, validate, authorization='', clock=now):
    """Resume the market task only after an exact offline snapshot replay passes."""
    database = database.resolve()
    path = job_dir(state_root, database, as_of) / 'job.json'
    if not path.is_file():
        return {'status':'NOT_STARTED','complete':False,'checkpoint_modified':False}
    lock_path = state_root / 'runtime.lock'
    if not lock_path.is_file():
        return {'status':'BLOCKED_MISSING_EXISTING_LOCK','checkpoint_modified':False}
    with lock_path.open('r+') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status':'RUNNING','checkpoint_modified':False}
        original = path.read_bytes()
        checkpoint_sha = hashlib.sha256(original).hexdigest()
        state = json.loads(original)
        base = {'complete':False,'checkpoint_modified':False,
                'database_modified':False,'collection_started':False,
                'checkpoint_sha256':checkpoint_sha}
        if (state.get('status') != 'BLOCKED_ITEM_SAFETY'
                or state.get('active_task') != 'market'):
            return {**base,'status':'BLOCKED_INELIGIBLE_MARKET_RECOVERY_STATE',
                    'checkpoint_status':state.get('status'),
                    'active_task':state.get('active_task')}
        if authorization != MARKET_TOKEN or expected_checkpoint_sha != checkpoint_sha:
            return {**base,'status':'BLOCKED_MARKET_RECOVERY_AUTHORIZATION_OR_SHA'}
        freeze = verify_freeze_manifest(freeze_manifest)
        if not freeze.get('ok'):
            return {**base,'status':'BLOCKED_PLUGIN_OR_SKILL_DRIFT','freeze':freeze}
        if not state.get('baseline') or verify_existing(database,state['baseline']):
            return {**base,'status':'BLOCKED_FORMAL_HISTORY_DRIFT'}
        if not window_allowed(as_of,clock()):
            return {**base,'status':'AUTHORIZATION_REQUIRED_CROSS_WEEK'}
        snapshot = path.parent / 'attempts' / str(state.get('attempt')) / 'market.tar.gz'
        if (not snapshot.is_file()
                or _sha_bytes(snapshot.read_bytes()) != expected_provider_snapshot_sha):
            return {**base,'status':'BLOCKED_MARKET_PROVIDER_SNAPSHOT_DRIFT',
                    'provider_snapshot':str(snapshot)}
        try:
            validation = validate(snapshot)
        except Exception as exc:
            return {**base,'status':'BLOCKED_MARKET_REPAIR_VALIDATION',
                    'error':str(exc)[:1500]}
        if (validation.get('passed') is not True
                or validation.get('historical_records_published') != 0
                or validation.get('fixed_universe_count') != 60
                or validation.get('validation_sha256') != expected_validation_sha):
            return {**base,'status':'BLOCKED_MARKET_REPAIR_VALIDATION_SHA',
                    'validation':validation}
        from scripts.weekly_items import verify_artifact
        for name,item in state.get('tasks',{}).items():
            if item.get('ok'):
                try:
                    verify_artifact(item)
                except Exception as exc:
                    return {**base,'status':'BLOCKED_ITEM_ARTIFACT_DRIFT','task':name,
                            'error':str(exc)}
        if (path.read_bytes() != original
                or not verify_freeze_manifest(freeze_manifest).get('ok')
                or verify_existing(database,state['baseline'])
                or _sha_bytes(snapshot.read_bytes()) != expected_provider_snapshot_sha):
            return {**base,'status':'BLOCKED_MARKET_RECOVERY_INPUT_DRIFT'}
        recovery_root = path.parent / 'market_recoveries'
        checkpoint_archive = recovery_root / (checkpoint_sha + '.checkpoint.json')
        recovery_receipt = recovery_root / (checkpoint_sha + '.receipt.json')
        recovery_root.mkdir(parents=True,exist_ok=True)
        if not checkpoint_archive.exists():
            with checkpoint_archive.open('xb') as handle:
                handle.write(original)
        elif checkpoint_archive.read_bytes() != original:
            return {**base,'status':'BLOCKED_MARKET_RECOVERY_ARCHIVE_CONFLICT'}
        receipt = {
            'status':'MARKET_REPAIR_REVALIDATED',
            'prior_checkpoint_sha256':checkpoint_sha,
            'provider_snapshot':str(snapshot),
            'provider_snapshot_sha256':expected_provider_snapshot_sha,
            'validation':validation,
            'database_modified':False,
            'collection_started':False,
        }
        if recovery_receipt.exists() and json.loads(recovery_receipt.read_text()) != receipt:
            return {**base,'status':'BLOCKED_MARKET_RECOVERY_RECEIPT_CONFLICT'}
        if not recovery_receipt.exists():
            save(recovery_receipt,receipt)
        state.setdefault('tasks',{})['market'] = {
            'ok':False,
            'issues':[{'reason':'qfq_contract_repaired_rerun_required'}],
            'repair_revalidation_receipt':str(recovery_receipt),
        }
        state.pop('active_task',None)
        state.pop('error',None)
        state.update(
            revision=REVISION,status='WAITING_RETRY',next_retry_at=clock().isoformat(),
            market_safety_recovery={
                'prior_checkpoint':str(checkpoint_archive),
                'prior_sha256':checkpoint_sha,
                'receipt':str(recovery_receipt),
                'provider_snapshot_sha256':expected_provider_snapshot_sha,
                'validation_sha256':expected_validation_sha,
                'at':clock().isoformat(),
            },
        )
        save(path,state)
        return {**public_result(state,path),'checkpoint_modified':True,
                'database_modified':False,'collection_started':False}

def revalidate(*, state_root, database, as_of, freeze_manifest, assess, resume=False,
               authorization='', expected_checkpoint_sha='', clock=now):
    path = job_dir(state_root, database, as_of) / 'job.json'
    if not path.is_file():
        return {'status': 'NOT_STARTED', 'complete': False, 'checkpoint_modified': False}
    # A read-only preview does not create even a lock file
    lock_path = state_root / 'runtime.lock'
    if not lock_path.is_file():
        return {'status': 'BLOCKED_MISSING_EXISTING_LOCK', 'checkpoint_modified': False}
    with lock_path.open('r+') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status': 'RUNNING', 'checkpoint_modified': False}
        original = path.read_bytes()
        digest = hashlib.sha256(original).hexdigest()
        state = json.loads(original)
        if state.get('status') not in {'BLOCKED_PUBLICATION', 'BLOCKED_ACCEPTANCE'}:
            return {'status': 'BLOCKED_INELIGIBLE_REVALIDATION_STATE', 'checkpoint_status': state.get('status'),
                    'checkpoint_modified': False}
        freeze = verify_freeze_manifest(freeze_manifest)
        if not freeze['ok']:
            return {'status': 'BLOCKED_PLUGIN_OR_SKILL_DRIFT', 'freeze': freeze, 'checkpoint_modified': False}
        if not state.get('baseline') or verify_existing(database, state['baseline']):
            return {'status': 'BLOCKED_FORMAL_HISTORY_DRIFT', 'checkpoint_modified': False}
        from scripts.weekly_items import verify_artifact
        for name, item in state.get('tasks', {}).items():
            if item.get('ok'):
                try:
                    verify_artifact(item)
                except Exception as exc:
                    return {'status': 'BLOCKED_ITEM_ARTIFACT_DRIFT', 'task': name, 'error': str(exc),
                            'checkpoint_modified': False}
        dependency = (market_fx_retry_evidence(state, path.parent, state_root.parent.parent, as_of)
                      if state.get('status') == 'BLOCKED_ACCEPTANCE' else None)
        if dependency:
            # This is an explicitly authorized retry preview, NOT a passed
            # publication assessment. Every old finding remains in the job.
            evidence = {'status':'DEPENDENCY_RETRY_PREVIEW', 'complete':False,
                        'checkpoint_modified':False, 'checkpoint_sha256':digest,
                        'collection_started':False, 'database_modified':False,
                        'dependency':dependency, 'remaining_issues':state.get('gaps', [])}
            if not resume:
                return evidence
            if authorization != TOKEN or expected_checkpoint_sha != digest:
                return {**evidence, 'status':'BLOCKED_REVALIDATION_AUTHORIZATION_OR_SHA'}
            if not window_allowed(as_of, clock()):
                return {**evidence, 'status':'AUTHORIZATION_REQUIRED_CROSS_WEEK'}
            if (path.read_bytes() != original or not verify_freeze_manifest(freeze_manifest)['ok']
                    or verify_existing(database, state['baseline'])
                    or market_fx_retry_evidence(state, path.parent, state_root.parent.parent, as_of) != dependency):
                return {**evidence, 'status':'BLOCKED_REVALIDATION_INPUT_DRIFT'}
            archive = path.parent / 'revalidations' / (digest + '.json')
            archive.parent.mkdir(parents=True, exist_ok=True)
            if not archive.exists():
                with archive.open('xb') as handle:
                    handle.write(original)
            elif archive.read_bytes() != original:
                return {**evidence, 'status':'BLOCKED_REVALIDATION_ARCHIVE_CONFLICT'}
            proof = archive.with_suffix('.fx-dependency.json')
            if proof.exists() and json.loads(proof.read_text()) != dependency:
                return {**evidence, 'status':'BLOCKED_REVALIDATION_ARCHIVE_CONFLICT'}
            if not proof.exists():
                save(proof, dependency)
            state.update(status='WAITING_RETRY', next_retry_at=clock().isoformat(),
                         repair_revalidation={'prior_checkpoint':str(archive), 'prior_sha256':digest,
                                              'dependency_evidence':str(proof), 'at':clock().isoformat()})
            save(path, state)
            return {**public_result(state, path), 'checkpoint_modified':True,
                    'collection_started':False, 'database_modified':False}
        assessment = assess()
        evidence = {'status': 'REVALIDATION_PREVIEW', 'complete': False, 'checkpoint_modified': False,
                    'checkpoint_sha256': digest, 'assessment': assessment,
                    'collection_started': False, 'database_modified': False}
        issues = assessment.get('issues', [])
        if not isinstance(assessment.get('passed'), bool) or any(i.get('blocking', True) for i in issues) or (not assessment['passed'] and not issues):
            return {**evidence, 'status': 'BLOCKED_REVALIDATION'}
        if not resume:
            return evidence
        if authorization != TOKEN or expected_checkpoint_sha != digest:
            return {**evidence, 'status': 'BLOCKED_REVALIDATION_AUTHORIZATION_OR_SHA'}
        if not window_allowed(as_of, clock()):
            return {**evidence, 'status': 'AUTHORIZATION_REQUIRED_CROSS_WEEK'}
        if (path.read_bytes() != original or not verify_freeze_manifest(freeze_manifest)['ok']
                or verify_existing(database, state['baseline'])):
            return {**evidence, 'status': 'BLOCKED_REVALIDATION_INPUT_DRIFT'}
        archive = path.parent / 'revalidations' / (digest + '.json')
        archive.parent.mkdir(parents=True, exist_ok=True)
        if not archive.exists():
            with archive.open('xb') as handle:
                handle.write(original)
        elif archive.read_bytes() != original:
            return {**evidence, 'status': 'BLOCKED_REVALIDATION_ARCHIVE_CONFLICT'}
        # Preserve the full old checkpoint/waiver evidence, but carry no waiver
        # or former publication receipt as an active success authorization
        state = {k:v for k,v in state.items() if 'waiver' not in k.lower() and k not in {'active_task', 'publication'}}
        for name in ('benchmarks', 'financial_versions', 'hk_statement_baseline', 'flow_normalization', 'pit_normalization'):
            if name in state['tasks']:
                state['tasks'][name] = {**state['tasks'][name], 'ok': False, 'contract_revalidation_required': True}
        state.update(revision=REVISION, status='WAITING_RETRY', gaps=issues, next_retry_at=clock().isoformat(),
                     repair_revalidation={'prior_checkpoint': str(archive), 'prior_sha256': digest,
                                          'at': clock().isoformat(), 'assessment': assessment})
        save(path, state)
        return {**public_result(state, path), 'checkpoint_modified': True, 'collection_started': False,
                'database_modified': False}
