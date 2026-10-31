import hashlib
import io
import json
import tarfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from scripts.weekly_guard import build_freeze_manifest, file_inventory, publish_weekly
from scripts.weekly_revalidation import revalidate, TOKEN
from scripts.weekly_runner import market_fx_retry_evidence, run_job, save, job_dir, TZ
from scripts.weekly_tasks import Task, make_tasks

AS_OF = '2026-10-09'
INSTANT = datetime(2026, 10, 10, 16, tzinfo=TZ)

def sha(data):
    return hashlib.sha256(data).hexdigest()

def archive(path, contents):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, 'w:gz') as handle:
        for name, data in contents.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            handle.addfile(member, io.BytesIO(data))

def setup(tmp_path):
    runtime, db = tmp_path / 'runtime', tmp_path / 'db'
    (db / 'cache').mkdir(parents=True)
    (db / 'cache/existing.json').write_text('{"old":1}')
    sealed = tmp_path / 'sealed'
    sealed.mkdir()
    (sealed / 'code').write_text('frozen')
    freeze = tmp_path / 'freeze.json'
    build_freeze_manifest([('sealed', sealed)], freeze)
    jobs = runtime / 'state/weekly_jobs'
    jobs.mkdir(parents=True)
    (jobs / 'runtime.lock').touch()
    folder = job_dir(jobs, db, AS_OF)
    folder.mkdir(parents=True)
    path = folder / 'job.json'
    fx_data = json.dumps({'pair':'HKD/CNY', 'primary_field':'hkd_cny',
                         'source':['SAFE official CFETS RMB central parity archive'],
                         'records':[{'date':AS_OF, 'hkd_cny':.85798}]}).encode()
    current = runtime / '.cache/fx/HKDCNY.json'
    current.parent.mkdir(parents=True)
    current.write_bytes(fx_data)
    fx_receipt = folder / 'attempts/1/fx.json'
    provider, accepted = fx_receipt.with_suffix('.tar.gz'), fx_receipt.with_suffix('.accepted.tar.gz')
    archive(provider, {'cache/fx/HKDCNY.json':fx_data})
    archive(accepted, {'cache/fx/HKDCNY.json':fx_data})
    fx_result = {'status':'OK', 'range':['1994-01-01',AS_OF], 'rows':1}
    save(fx_receipt, {'ok':True, 'issues':[], 'result':fx_result, 'provider_snapshot':str(provider)})
    save(fx_receipt.with_suffix('.prepared.json'), {'snapshot':str(accepted), 'result':fx_result,
                                                 'outputs':{'cache/fx/HKDCNY.json':{'sha256':sha(fx_data)}}})
    market_receipt = folder / 'attempts/1/market.json'
    market_snapshot = market_receipt.with_suffix('.tar.gz')
    archive(market_snapshot, {})
    result = {'status':'FAILED', 'gate':{'passed':False, 'fatal_count':1, 'issues':[
        {'severity':'fatal','locus':'HKD/CNY:fx:SAFE_CFETS',
         'message':'latest rate 2026-09-18 is stale for expected session 2026-10-09'}]},
        'history_sync':{'status':'SKIPPED_GATE_FAILED','symbols':{},'appended_rows':0,'conflict_rows':0}}
    save(market_receipt, {'ok':False, 'issues':[{'path':'$.gate.passed','value':False}],
                          'result':result,'provider_snapshot':str(market_snapshot),'failure_class':'RETRY_SOURCE'})
    gaps = [{'domain':'history','reason':'invalid_qfq_bridge_receipt','blocking':True,
             'details':'market receipt did not pass its native safety gate'},
            {'domain':'history','reason':'current_history_invalid','blocking':True,
             'details':{'status':'conflict','snapshot_mismatch_dates':['2026-09-04'],
                        'latest_date':'2026-09-18','expected_date':AS_OF}}]
    state = {'status':'BLOCKED_ACCEPTANCE','attempt':1,'as_of_date':AS_OF,'database':str(db),
             'baseline':file_inventory(db),'gaps':gaps,'publication':None,
             'tasks':{'market':{'ok':False,'failure_class':'RETRY_SOURCE','receipt':str(market_receipt)},
                      'fx':{'ok':True,'receipt':str(fx_receipt)},'successful':{'ok':True}}}
    save(path,state)
    return runtime, db, freeze, jobs, folder, path, state

def options(parts):
    runtime, db, freeze, jobs, folder, path, state = parts
    return dict(state_root=jobs,database=db,as_of=AS_OF,freeze_manifest=freeze,
                assess=lambda: {'passed':False,'issues':[{'blocking':True,'reason':'not eligible'}]},
                clock=lambda:INSTANT)

def test_plan_keeps_statement_conversion_before_fx_and_market_after_fx():
    adapter = SimpleNamespace(extended=Mock(), extended_p2=Mock(), config=Mock(),
                              _refresh_market=Mock(),_refresh_specialized=Mock())
    names = [t.name for t in make_tasks(adapter, AS_OF)]
    assert names.index('statements_a') < names.index('fx') < names.index('market')
    assert names.index('statements_hk') < names.index('fx')
    assert names.index('market') < names.index('benchmarks')

def test_preview_and_explicit_recovery_never_pass_publication_or_change_successes(tmp_path):
    parts = setup(tmp_path)
    runtime, db, freeze, jobs, folder, path, state = parts
    old = path.read_bytes()
    before = file_inventory(db)
    preview = revalidate(**options(parts))
    assert preview['status'] == 'DEPENDENCY_RETRY_PREVIEW'
    assert not preview['dependency']['publication_allowed'] and not preview['complete']
    assert path.read_bytes() == old and file_inventory(db) == before
    result = revalidate(**options(parts), resume=True, authorization=TOKEN, expected_checkpoint_sha=sha(old))
    assert result['status'] == 'WAITING_RETRY' and result['attempt'] == 1
    current = json.loads(path.read_text())
    assert current['tasks'] == state['tasks'] and current['gaps'] == state['gaps']
    assert current['tasks']['market']['ok'] is False
    assert Path(current['repair_revalidation']['prior_checkpoint']).read_bytes() == old
    assert file_inventory(db) == before and not result['collection_started']

@pytest.mark.parametrize('case', ['price_fatal','qfq_block','current_conflict','undated_conflict',
                                'unknown_hard_gate','fx_payload_drift','fx_snapshot_drift','fx_source_drift'])
def test_non_dependency_safety_errors_are_not_unlockable(tmp_path, case):
    parts = setup(tmp_path)
    runtime, db, freeze, jobs, folder, path, state = parts
    receipt = Path(state['tasks']['market']['receipt'])
    doc = json.loads(receipt.read_text())
    if case == 'price_fatal':
        doc['result']['gate']['issues'][0]['locus'] = '00700.HK:price'
        save(receipt, doc)
    elif case == 'qfq_block':
        doc['result']['history_sync']['price_basis_blocked'] = True
        save(receipt, doc)
    elif case in {'current_conflict','undated_conflict'}:
        state['gaps'][1]['details']['snapshot_mismatch_dates'] = ['2026-10-08' if case == 'current_conflict' else 'unknown']
        save(path,state)
    elif case == 'unknown_hard_gate':
        state['gaps'].append({'domain':'scope','blocking':True})
        save(path,state)
    elif case == 'fx_payload_drift':
        (runtime / '.cache/fx/HKDCNY.json').write_text('{}')
    elif case == 'fx_snapshot_drift':
        archive(folder / 'attempts/1/fx.tar.gz', {'cache/fx/HKDCNY.json':b'{}'})
    elif case == 'fx_source_drift':
        data = json.loads((runtime / '.cache/fx/HKDCNY.json').read_text())
        data['source'] = ['unapproved source']
        data = json.dumps(data).encode()
        (runtime / '.cache/fx/HKDCNY.json').write_bytes(data)
        archive(folder / 'attempts/1/fx.tar.gz', {'cache/fx/HKDCNY.json':data})
        archive(folder / 'attempts/1/fx.accepted.tar.gz', {'cache/fx/HKDCNY.json':data})
        prepared = json.loads((folder / 'attempts/1/fx.prepared.json').read_text())
        prepared['outputs']['cache/fx/HKDCNY.json']['sha256'] = sha(data)
        save(folder / 'attempts/1/fx.prepared.json', prepared)
    before = path.read_bytes()
    result = revalidate(**options(parts), resume=True, authorization=TOKEN, expected_checkpoint_sha=sha(before))
    assert result['status'].startswith('BLOCKED') and path.read_bytes() == before

@pytest.mark.parametrize('case', ['wrong_sha','missing_auth','history_drift','freeze_drift','cross_week'])
def test_recovery_preserves_sha_authorization_freeze_history_and_week_boundaries(tmp_path,case):
    parts = setup(tmp_path)
    runtime, db, freeze, jobs, folder, path, state = parts
    original = path.read_bytes()
    kw = options(parts)
    if case == 'history_drift':
        (db / 'cache/existing.json').write_text('changed')
    if case == 'freeze_drift':
        (tmp_path / 'sealed/code').write_text('changed')
    if case == 'cross_week':
        kw['clock'] = lambda:datetime(2026,10,12,tzinfo=TZ)
    result = revalidate(**kw,resume=True,authorization='' if case=='missing_auth' else TOKEN,
                        expected_checkpoint_sha='bad' if case=='wrong_sha' else sha(original))
    assert result['status'].startswith(('BLOCKED','AUTHORIZATION_REQUIRED'))
    assert path.read_bytes() == original

def test_dependency_retry_is_before_final_gate_but_never_publishes_failed_market(tmp_path):
    parts = setup(tmp_path)
    runtime, db, freeze, jobs, folder, path, state = parts
    state['status']='WAITING_RETRY'
    state['started_at']=INSTANT.isoformat()
    save(path,state)
    calls=[]
    failed = json.loads(Path(state['tasks']['market']['receipt']).read_text())['result']
    kw = dict(state_root=jobs,runtime=runtime,database=db,as_of=AS_OF,freeze_manifest=freeze,
              tasks=[Task('fx',('fx',),lambda:pytest.fail('successful FX must not repeat')),
                     Task('market',('history',),lambda:failed),
                     Task('successful',('other',),lambda:pytest.fail('success must not repeat'))],
              audit_source=lambda:({}, {'passed':False,'issues':state['gaps']}),
              publish=lambda _:calls.append('publish'),clock=lambda:INSTANT)
    result=run_job(**kw)
    assert result['status']=='WAITING_SOURCE' and result['attempt']==2 and not calls
    assert result['dependency_retry']['fx_ready'] is True
    assert not result['dependency_retry']['publication_allowed']

def test_successful_retry_appends_once_and_preserves_all_original_database_bytes(tmp_path):
    parts=setup(tmp_path)
    runtime,db,freeze,jobs,folder,path,state=parts
    state.update(status='WAITING_RETRY',started_at=INSTANT.isoformat())
    state['gaps']=[]
    save(path,state)
    original=file_inventory(db)
    calls=[]
    def market():
        calls.append('market')
        p=runtime/'.cache/listing/00700.json'
        p.parent.mkdir(parents=True)
        p.write_text(json.dumps({'symbol':'00700.HK','records':[{'event_date':AS_OF,'available_on':AS_OF,'value':1}]}))
        return {'status':'OK'}
    def publish(_):
        return publish_weekly(runtime,db,AS_OF,publish=True,authorization='PUBLISH_CURRENT_WEEK',
                              freeze_manifest=freeze,acceptance=lambda *_:{'passed':True,'issues':[]})
    kw=dict(state_root=jobs,runtime=runtime,database=db,as_of=AS_OF,freeze_manifest=freeze,
            tasks=[Task('fx',('fx',),lambda:pytest.fail('FX repeated')),Task('market',('history',),market),
                   Task('successful',('other',),lambda:pytest.fail('success repeated'))],
            audit_source=lambda:({}, {'passed':True,'issues':[]}),publish=publish,clock=lambda:INSTANT)
    result=run_job(**kw)
    assert result['status']=='COMPLETE' and calls==['market']
    for key in ('formal_existing_files_rewritten','historical_revisions_published','historical_backfills_published'):
        assert result['publication'][key]==0
    assert all(file_inventory(db)[p]==value for p,value in original.items())
    versions=list((db/'state/published_versions').rglob('*.json'))
    run_job(**kw)
    assert calls==['market'] and list((db/'state/published_versions').rglob('*.json'))==versions
