import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import time
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.weekly_items import digest, expand_tasks, migrate_verified, validate_item, verify_artifact
from scripts.weekly_items import old_only_conflict
from scripts.weekly_isolation import (
    wait_worker, recover_prepared, IsolationSafetyError, document_rows_equal,
    item_deadline_seconds,
)
from scripts.weekly_runner import save, run_job, queue_retry, TZ
from scripts.weekly_tasks import Task, STATEMENT_NATIVE_BUDGET_SECONDS
from scripts.weekly_guard import build_freeze_manifest

def archive(path, files):
    path.parent.mkdir(parents=True,exist_ok=True)
    with tarfile.open(path,"w:gz") as tar:
        for name,data in files.items():
            member=tarfile.TarInfo(name);member.size=len(data)
            tar.addfile(member,io.BytesIO(data))

def test_statement_deadline_matches_native_budget_without_widening_other_items():
    statement = Task('statements_a@002594.SZ',('statements',),lambda:None,
                     parent='statements_a',symbol='002594.SZ')
    hk_statement = Task('statements_hk@00700.HK',('statements_hk',),lambda:None,
                        parent='statements_hk',symbol='00700.HK')
    ordinary = Task('dividends@002594.SZ',('dividends',),lambda:None,
                    parent='dividends',symbol='002594.SZ')
    document = Task('byd_research_text@x',('specialized',),lambda:None,
                    parent='specialized',symbol='002594.SZ',document='https://example.test/a.pdf')
    batch = Task('market',('history',),lambda:None)
    assert item_deadline_seconds(statement) == 420
    assert item_deadline_seconds(hk_statement) == 420
    assert STATEMENT_NATIVE_BUDGET_SECONDS == 400
    assert STATEMENT_NATIVE_BUDGET_SECONDS < item_deadline_seconds(statement)
    assert item_deadline_seconds(ordinary) == 150
    assert item_deadline_seconds(document) == 150
    assert item_deadline_seconds(batch) == 1500

def statement(symbol):
    return json.dumps({"symbol":symbol,**{s:[{"report_date":"2026-06-30","fields":{"amount":1}}]
                      for s in ("balance","income","cashflow")}}).encode()

def test_eighteen_successes_never_recollected_or_regress(tmp_path):
    runtime=tmp_path/"runtime";db=tmp_path/"db"
    sealed=tmp_path/"sealed";sealed.mkdir();(sealed/"x").write_text("fixed")
    freeze=tmp_path/"freeze.json";build_freeze_manifest([("sealed",sealed)],freeze)
    counts={str(i):0 for i in range(20)}
    tasks=[Task(str(i),("statements",),lambda:None) for i in range(20)]
    instant=[datetime(2026,9,5,10,tzinfo=TZ)]
    def collect(task,receipt):
        counts[task.name]+=1
        snapshot=receipt.with_suffix('.tar.gz');archive(snapshot,{})
        return {"status":"OK" if int(task.name)<18 else "timeout"},snapshot
    kw=dict(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
            tasks=tasks,freeze_manifest=freeze,audit_source=lambda:({},{"passed":True,"issues":[]}),
            publish=lambda _:pytest.fail("source still incomplete"),clock=lambda:instant[0],isolated_collect=collect)
    first=run_job(**kw);instant[0]=datetime.fromisoformat(first['next_retry_at'])
    second=run_job(**kw)
    assert all(counts[str(i)]==1 for i in range(18))
    assert counts['18']==counts['19']==2
    assert sum(v['ok'] for v in second['tasks'].values())==18
    assert not second['complete']

def test_history_gap_does_not_dirty_snapshot_or_other_symbols():
    tasks=[Task("fund_flow",("fund_flow_ths",),lambda:None),
           Task("fund_flow_history@A",("fund_flow_ths",),lambda:None,symbol="A"),
           Task("fund_flow_history@B",("fund_flow_ths",),lambda:None,symbol="B")]
    state={"attempt":1,"tasks":{t.name:{"ok":True} for t in tasks},
           "gaps":[{"domain":"fund_flow_ths","symbol":"A","date":"2026-09-04"}]}
    queue_retry(state,tasks,datetime(2026,9,5,10,tzinfo=TZ))
    assert state['tasks']['fund_flow']['ok']
    assert state['tasks']['fund_flow_history@B']['ok']
    assert not state['tasks']['fund_flow_history@A']['ok']

def test_migration_requires_snapshot_and_matching_complete_content(tmp_path):
    runtime=tmp_path/'runtime';folder=tmp_path/'job';folder.mkdir()
    task=Task('statements_a@002594.SZ',('statements',),lambda:None,parent='statements_a',
              symbol='002594.SZ',output='cache/statements/002594.json')
    data=statement(task.symbol)
    p=runtime/'.cache/statements/002594.json';p.parent.mkdir(parents=True);p.write_bytes(data)
    receipt=folder/'attempts/3/statements_a.json'
    save(receipt,{'ok':False,'result':{task.symbol:'cached','other':'timeout'}})
    when=datetime(2026,9,5,15,tzinfo=TZ).timestamp();os.utime(receipt,(when,when))
    archive(receipt.with_suffix('.tar.gz'),{task.output:data})
    state={'tasks':{'statements_a':{'ok':False}}}
    migrate_verified(state,[task],folder,runtime,'2026-09-04')
    assert state['tasks'][task.name]['ok'] and state['migration']['verified']==1
    verify_artifact(state['tasks'][task.name])
    archive(receipt.with_suffix('.tar.gz'),{task.output:b'changed'})
    with pytest.raises(ValueError,match='drift'):
        verify_artifact(state['tasks'][task.name])

def test_statement_partial_native_cached_is_not_accepted():
    task=Task('x',('statements',),lambda:None,parent='statements_a',symbol='002594.SZ',
              output='cache/statements/002594.json')
    blob=json.loads(statement(task.symbol));blob['cashflow']=[]
    with pytest.raises(ValueError,match='missing statement'):
        validate_item(task,json.dumps(blob).encode(),'2026-09-04')

@pytest.mark.parametrize('day',['2026-09-04','20260904','2026/09/04'])
def test_daily_date_spellings_compare_without_rewriting_source(day):
    task=Task('margin',('margin',),lambda:None,parent='margin_history',symbol='A',output='cache/margin/A.json')
    data=json.dumps({'symbol':'A','records':[{'date':day,'value':1}]}).encode()
    before=digest(data)
    assert validate_item(task,data,'2026-09-04')['records'][0]['date']==day
    assert digest(data)==before

def test_daily_expected_date_can_respect_native_suspension_calendar():
    task=Task('margin',('margin',),lambda:None,parent='margin_history',symbol='A',
              output='cache/margin/A.json',expected_date='2026-08-28')
    data=json.dumps({'symbol':'A','records':[{'date':'20260828','value':1}]}).encode()
    validate_item(task,data,'2026-09-04')
    task.expected_date='2026-09-04'
    with pytest.raises(ValueError,match='still missing'):
        validate_item(task,data,'2026-09-04')

def test_future_daily_record_is_not_a_fresh_success():
    task=Task('margin',('margin',),lambda:None,parent='margin_history',symbol='A',output='cache/margin/A.json')
    with pytest.raises(ValueError,match='still missing'):
        validate_item(task,json.dumps({'symbol':'A','records':[{'date':'20260907'}]}).encode(),'2026-09-04')

def test_one_failed_pdf_does_not_invalidate_parsed_documents():
    rows=[{'report_url':'good','parse_status':'parsed','symbol':'002594.SZ',
           'document_sha256':'a'*64,'document_bytes':123,'text_length':456},
          {'report_url':'bad','parse_status':'failed','symbol':'002594.SZ'}]
    buffer=io.BytesIO();pd.DataFrame(rows).to_parquet(buffer,index=False)
    task=Task('doc',('specialized',),lambda:None,document='good',output='cache/research.parquet')
    assert len(validate_item(task,buffer.getvalue(),'2026-09-04'))==1
    task.document='bad'
    with pytest.raises(ValueError,match='not parsed'):
        validate_item(task,buffer.getvalue(),'2026-09-04')

def test_repeated_parsed_document_requires_same_hash_and_fields():
    prior = pd.DataFrame([{'report_url':'same','parse_status':'parsed','document_sha256':'a'*64,
                           'document_bytes':10,'text_length':20,'score':1.0,'error':None}])
    incoming = pd.DataFrame([{'report_url':'same','parse_status':'parsed','document_sha256':'a'*64,
                              'document_bytes':10,'text_length':20,'score':1}])
    assert document_rows_equal(prior,incoming)
    changed = incoming.copy();changed.loc[0,'score'] = 2
    assert not document_rows_equal(prior,changed)
    changed = incoming.copy();changed.loc[0,'document_sha256'] = 'b'*64
    assert not document_rows_equal(prior,changed)

def test_process_deadline_stops_late_descendant_writes(tmp_path):
    target=tmp_path/'late'
    code="import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',sys.argv[1]]);time.sleep(20)"
    child=f"import time,pathlib;time.sleep(.8);pathlib.Path({str(target)!r}).write_text('late')"
    process=subprocess.Popen([sys.executable,'-c',code,child],start_new_session=True)
    result=wait_worker(process,tmp_path/'no-receipt',.25)
    time.sleep(.9)
    assert result['status']=='timeout' and process.poll() is not None and not target.exists()

def test_receipt_still_stops_late_threads(tmp_path):
    receipt=tmp_path/'result.json';target=tmp_path/'late'
    code=f"import pathlib,time;pathlib.Path({str(receipt)!r}).write_text('{{\"status\":\"OK\"}}');time.sleep(.7);pathlib.Path({str(target)!r}).write_text('late')"
    process=subprocess.Popen([sys.executable,'-c',code],start_new_session=True)
    assert wait_worker(process,receipt,2)['status']=='OK'
    time.sleep(.8);assert not target.exists()

def test_partial_working_commit_recovers_and_refuses_conflict(tmp_path):
    runtime=tmp_path/'runtime';snapshot=tmp_path/'accepted.tar.gz'
    archive(snapshot,{'cache/test/a.json':b'one','cache/test/b.json':b'two'})
    prepared={'snapshot':str(snapshot),'outputs':{
        'cache/test/a.json':{'before':None,'sha256':digest(b'one')},
        'cache/test/b.json':{'before':None,'sha256':digest(b'two')}}}
    a=runtime/'.cache/test/a.json';a.parent.mkdir(parents=True);a.write_bytes(b'one')
    recover_prepared(runtime,prepared)
    assert (a.parent/'b.json').read_bytes()==b'two'
    a.write_bytes(b'conflicting later write')
    with pytest.raises(IsolationSafetyError,match='conflict'):
        recover_prepared(runtime,prepared)

def test_old_conflict_only_accepts_current_tail_with_all_prior_rows_unchanged():
    task=Task('margin@A',('margin',),lambda:None,parent='margin_history',symbol='A',output='cache/margin/A.json')
    old={'symbol':'A','records':[{'date':'2026-08-28','value':1}]}
    new={**old,'records':[dict(r) for r in old['records']]+[{'date':'2026-09-04','value':2}],
         'history_conflicts':[{'date':'2026-08-28','incoming':{'value':9}}]}
    assert old_only_conflict(task,json.dumps(new).encode(),json.dumps(old).encode(),'2026-09-04')
    new['history_conflicts'][0]['date']='2026-09-01'
    assert not old_only_conflict(task,json.dumps(new).encode(),json.dumps(old).encode(),'2026-09-04')
    new['history_conflicts'][0]['date']='2026-08-28';new['records'][0]['value']=99
    assert not old_only_conflict(task,json.dumps(new).encode(),json.dumps(old).encode(),'2026-09-04')

def test_microstructure_splits_both_markets_into_independent_items():
    stocks=[SimpleNamespace(symbol='300866.SZ',ak_symbol='300866',market=SimpleNamespace(value='A')),
            SimpleNamespace(symbol='00700.HK',ak_symbol='00700',market=SimpleNamespace(value='HK'))]
    task=Task('microstructure',('microstructure',),lambda:None)
    items=expand_tasks([task],SimpleNamespace(stocks=stocks),None)
    assert [(item.name,item.output) for item in items]==[
        ('microstructure@300866.SZ','cache/microstructure/300866.json'),
        ('microstructure@00700.HK','cache/microstructure/00700.json')]

def test_new_split_archives_failed_aggregate_without_marking_items_success(tmp_path):
    state={'item_schema':1,'tasks':{'microstructure':{'ok':False},
                                  'market':{'ok':True}}}
    task=Task('microstructure@300866.SZ',('microstructure',),lambda:None,
              parent='microstructure',symbol='300866.SZ',
              output='cache/microstructure/300866.json')
    migrate_verified(state,[task],tmp_path,tmp_path,'2026-09-18')
    assert state['legacy_tasks']['microstructure']=={'ok':False}
    assert state['tasks']=={'market':{'ok':True}}

def test_microstructure_requires_current_date_and_preserves_old_conflicts():
    task=Task('microstructure@300866.SZ',('microstructure',),lambda:None,
              parent='microstructure',symbol='300866.SZ',
              output='cache/microstructure/300866.json',expected_date='2026-09-18')
    old={'symbol':'300866.SZ','records':[{'date':'2026-09-11','amount':1.0}]}
    stale=json.dumps(old).encode()
    with pytest.raises(ValueError,match='still missing'):
        validate_item(task,stale,'2026-09-18')
    new={**old,'records':old['records']+[{'date':'2026-09-18','amount':2.0}],
         'history_conflicts':[{'date':'2026-09-11','incoming':{'amount':9.0}}]}
    assert old_only_conflict(task,json.dumps(new).encode(),stale,'2026-09-18')
    new['records'][0]['amount']=9.0
    assert not old_only_conflict(task,json.dumps(new).encode(),stale,'2026-09-18')
    new['records'][0]['amount']=1.0
    new['history_conflicts'][0]['date']='2026-09-18'
    assert not old_only_conflict(task,json.dumps(new).encode(),stale,'2026-09-18')
