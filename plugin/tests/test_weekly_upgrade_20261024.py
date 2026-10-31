"""Offline regressions for recoverability, typed records and immutable inputs"""
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from scripts import weekly_guard as guard
from scripts.weekly_items import expand_tasks, validate_item
from scripts.weekly_tasks import Task
from scripts.weekly_retention import referenced_paths
from scripts.weekly_runner import classify_task_failure
from qwl.datasources import weekly_series as series
from qwl.schemas import Market

SKILL = Path('/Users/bytedance/.codex/skills/qwl-database-update-standard-workflow/scripts')

def load_module(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module

def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(value))

def test_new_batch_domains_are_exactly_fixed_members(tmp_path):
    refs=[SimpleNamespace(symbol='002594.SZ',ak_symbol='002594',market=Market.A),
          SimpleNamespace(symbol='02015.HK',ak_symbol='02015',market=Market.HK)]
    tasks=expand_tasks([Task('fundamentals',('fundamentals',),lambda:None),
        Task('listing',('listing',),lambda:None)],SimpleNamespace(stocks=refs),tmp_path,'2026-10-23')
    assert len(tasks)==4 and {t.symbol for t in tasks}=={r.symbol for r in refs}

def document(event_id, day, value=1):
    return {'events':[{'event':'announcement_evidence','published_at':day,'available_on':day,
        'title':'same generic title','url':f'https://www.cninfo.com.cn/new/disclosure/detail?announcementId={event_id}',
        'value':value}]}

def plan(tmp_path, base, candidate, relative):
    db=tmp_path/'db';target=db/relative;source=tmp_path/'candidate.json'
    write(target,base);write(source,candidate)
    return guard._plan_json(source,target,Path(relative),db,date(2026,10,19),date(2026,10,23))

def test_earnings_distinct_announcement_id_is_append_not_overwrite(tmp_path):
    old=document('123','2026-08-18');incoming=document('124','2026-10-19')
    changes=plan(tmp_path,old,incoming,'cache/earnings_calendar/300866.json')
    assert [c.kind for c in changes]==['weekly_append']
    assert json.loads((tmp_path/'db/cache/earnings_calendar/300866.json').read_text())==old

def test_same_announcement_id_changed_values_are_quarantined(tmp_path):
    changes=plan(tmp_path,document('123','2026-08-18'),document('123','2026-08-18',9),
        'cache/earnings_calendar/300866.json')
    assert [c.kind for c in changes]==['historical_revision']

@pytest.mark.parametrize('day',['2026-10-17','2026-10-30'])
def test_earnings_old_and_future_announcements_never_append(tmp_path,day):
    changes=plan(tmp_path,document('123','2026-08-18'),document('124',day),'cache/earnings_calendar/300866.json')
    assert not any(c.kind=='weekly_append' for c in changes)

def test_sfc_unverified_availability_is_visible_in_quarantine_plan(tmp_path):
    old={'historical_records':[{'date':'2026-10-09','reportable_short_shares':10}]}
    new={'historical_records':old['historical_records']+[{'date':'2026-10-23','reportable_short_shares':11}]}
    changes=plan(tmp_path,old,new,'cache/hk_short_selling/02015.json')
    assert not any(c.kind=='weekly_append' for c in changes)
    assert changes[0].payload['records'][0]['classification']=='availability_unverified'

def test_old_earnings_contract_is_not_reinterpreted(tmp_path):
    relative=Path('cache/earnings_calendar/300866.json');db=tmp_path/'db'
    old=document('123','2026-08-18');write(db/relative,old)
    delta={'schema_version':3,'record_key_contract':'typed-v2','append_records':{'events':document('124','2026-10-19')['events']}}
    old['events'][0]['event_date']='2026-08-18';write(db/relative,old)
    delta['append_records']['events'][0]['event_date']='2026-10-19'
    write(db/'state/published_versions'/relative/'old.json',delta)
    assert guard.effective_json(db,relative)['events']==old['events']+delta['append_records']['events']

def bars():
    values=pd.DataFrame({'open':10.,'high':11.,'low':9.,'close':10.,'volume':100.},
        index=pd.bdate_range(end='2026-10-23',periods=70))
    values.attrs={'price_basis':'qfq','response_key':'qfqday'}
    return values

def test_successful_series_is_reused_and_hash_drift_blocks(tmp_path):
    ref=SimpleNamespace(symbol='002594.SZ',market=Market.A);calls=[]
    def fetch(): calls.append(1);return bars(),'tencent_kline_qfq'
    a=series.collect(tmp_path,ref,date(2026,10,23),'qfq','signature',fetch)
    b=series.collect(tmp_path,ref,date(2026,10,23),'qfq','signature',fetch)
    assert len(calls)==1;pd.testing.assert_frame_equal(a[0],b[0],check_freq=False)
    path=next(tmp_path.rglob('*.parquet'));path.write_bytes(path.read_bytes()+b'drift')
    with pytest.raises(ValueError,match='SERIES_INTEGRITY'):
        series.collect(tmp_path,ref,date(2026,10,23),'qfq','signature',fetch)
    assert len(calls)==1

@pytest.mark.parametrize('defect',['basis','future','missing','nan'])
def test_bad_series_is_not_saved_as_success(tmp_path,defect):
    value=bars()
    if defect=='basis':value.attrs['price_basis']='none'
    if defect=='future':value.index=value.index+pd.Timedelta(days=7)
    if defect=='missing':value=value.iloc[:-1]
    if defect=='nan':value.iloc[-1,0]=float('nan')
    series.collect(tmp_path,SimpleNamespace(symbol='002594.SZ',market=Market.A),date(2026,10,23),
        'qfq','signature',lambda:(value,'tencent_kline_qfq'))
    assert not list(tmp_path.rglob('receipt.json'))

def test_series_safety_failure_not_blindly_retried():
    assert classify_task_failure({'ok':False,'result':{'error':'ValueError: SERIES_INTEGRITY: drift'}})=='BLOCKED_ITEM_SAFETY'

def test_reference_queue_follows_nested_cycle_and_archive_receipt(tmp_path):
    a,b=tmp_path/'a.json',tmp_path/'b.json';archive=tmp_path/'source.tar.gz'
    write(a,{'receipt':str(b)});write(b,{'receipt':str(a)})
    payload=json.dumps({'receipt':str(a)}).encode();member='state/snapshots/2026-10-23/batch.result.json'
    with tarfile.open(archive,'w:gz') as tar:
        record=tarfile.TarInfo(member);record.size=len(payload);tar.addfile(record,io.BytesIO(payload))
    root=tmp_path/'root.json'
    write(root,{'provider_snapshot':str(archive),'batches':[{'receipt':member,
        'receipt_sha256':hashlib.sha256(payload).hexdigest()}]})
    result=referenced_paths({'receipt':str(root)},tmp_path)
    assert {a,b,root,archive} <= result
    d=json.loads(root.read_text());d['batches'][0]['receipt_sha256']='0'*64;write(root,d)
    with pytest.raises(ValueError,match='SHA'):
        referenced_paths({'receipt':str(root)},tmp_path)

def test_metadata_contract_rejects_prices_and_preserves_old_operations():
    m=load_module('qwl_test_metadata',SKILL/'pit_source_lineage.py')
    class Fake:
        spec={'week_end':'2026-10-16'}
        def __init__(self,source,price):self.source,self.price=source,price
        def read_json(self,*a,**kw):return {'data':{'source':self.source,'records':[{'date':'2026-10-16','close':self.price}]}}
        def _document(self,*a,**kw):return {'source':self.source}
    old,new=Fake('tencent_raw',1),Fake('akshare_tx_index_raw',1)
    ops,lineage=m.prepare(old,new,{'created_at':'2026-10-24T01:00:00+00:00'},'2026-10-23')
    assert ops[0]['apply_on']=='0001-01-01' and ops[1]['apply_on']=='2026-10-24'
    assert not lineage['record_values_and_availability_changed']
    with pytest.raises(ValueError,match='prefix drift'):
        m.prepare(old,Fake('akshare_tx_index_raw',2),{},'2026-10-23')
    with pytest.raises(ValueError,match='prefix drift'):
        m.prepare(old,Fake('unknown',1),{},'2026-10-23')

def test_complete_database_does_not_invoke_update(monkeypatch,tmp_path):
    m=load_module('qwl_test_pipeline',SKILL/'run_weekly_end_to_end.py')
    calls=[]
    def invoke(command,**kw):
        calls.append(command)
        return {'ok':True,'frozen_sources':{'ok':True}} if command[-1]=='health' else {'status':'COMPLETE','complete':True}
    monkeypatch.setattr(m,'invoke',invoke)
    assert m.weekly_until_terminal(Path(sys.executable),tmp_path,'2026-10-23',datetime(2026,11,1,tzinfo=m.TZ))['complete']
    assert not any('update' in call for call in calls)

def test_tencent_verified_https_fallback_preserves_strict_qfq():
    from qwl.datasources import tencent_kline as t
    import requests
    urls=[]
    class Response:
        def raise_for_status(self):return None
        def json(self):return {'code':0,'data':{'sz002594':{'day':[['2026-10-23',1,1,1,1,10]]}}}
    class Session:
        def get(self,url,**kw):
            urls.append(url)
            if len(urls)==1:raise requests.HTTPError('501')
            return Response()
    with pytest.raises(ValueError,match='no raw fallback'):
        t.fetch_daily('sz002594',adjust='qfq',session=Session(),strict_adjustment=True)
    assert urls==[t._URL,t._BACKUP_URL] and all(url.startswith('https://') for url in urls)


def test_transport_recovery_is_capped_at_six_and_rechecks_status(monkeypatch,tmp_path):
    m=load_module('qwl_recovery_limit',SKILL/'run_weekly_end_to_end.py')
    calls=[]
    def invoke(command,**kw):
        calls.append(command[-1])
        if command[-1]=='health':return {'ok':True,'frozen_sources':{'ok':True}}
        raise m.TransientProcessFailure('429 Too Many Requests')
    monkeypatch.setattr(m,'invoke',invoke);monkeypatch.setattr(m.time,'sleep',lambda _:None)
    with pytest.raises(m.TransientProcessFailure):
        m.weekly_until_terminal(Path(sys.executable),tmp_path,'2026-10-23',datetime(2026,11,1,tzinfo=m.TZ))
    assert len(calls)==8  # health, first failure and at most six recoveries
    assert isinstance(m.process_failure(['weekly_cli.py'],'','SERIES_INTEGRITY: drift'),RuntimeError)
    assert not isinstance(m.process_failure(['weekly_cli.py'],'','SERIES_INTEGRITY: drift'),m.TransientProcessFailure)


def test_partial_series_source_recovery_is_scoped_to_this_week(tmp_path):
    from scripts.weekly_isolation import seed_partial_series, IsolationSafetyError
    root=tmp_path/'job/attempts';receipt=root/'1/market.json';receipt.parent.mkdir(parents=True)
    archive=receipt.with_suffix('.tar.gz');name='state/snapshots/2026-10-23/series/002594_SZ/qfq/receipt.json'
    with tarfile.open(archive,'w:gz') as tar:
        record=tarfile.TarInfo(name);record.size=2;tar.addfile(record,io.BytesIO(b'{}'))
    write(receipt,{'provider_snapshot':str(archive)})
    workspace=tmp_path/'workspace';workspace.mkdir()
    seed_partial_series(workspace,root/'2/market.json','2026-10-23',{'002594'})
    assert (workspace/name).read_bytes()==b'{}'
    (workspace/name).write_bytes(b'changed')
    with pytest.raises(IsolationSafetyError,match='drift'):
        seed_partial_series(workspace,root/'2/market.json','2026-10-23',{'002594'})


def test_explicit_qfq_fallback_does_not_trust_request_label(monkeypatch):
    from qwl.datasources import akshare_provider as a, tencent_kline as t
    value=bars();value.attrs={'price_basis':'none','response_key':'day'}
    monkeypatch.setattr(t,'fetch_daily_range',lambda *args,**kw:value)
    assert a.fetch_a_daily_tx('sz002594','20260101','20261023',adjust='qfq') is None


def test_budget_exhaustion_leaves_missing_items_pending(tmp_path,monkeypatch):
    from scripts import weekly_runner as runner
    from scripts.weekly_guard import build_freeze_manifest
    seal=tmp_path/'seal';seal.mkdir();(seal/'x').write_text('frozen')
    freeze=tmp_path/'freeze.json';build_freeze_manifest([('seal',seal)],freeze)
    tasks=[Task('fundamentals@'+str(i),('fundamentals',),lambda:None,parent='fundamentals',symbol=str(i)) for i in range(2)]
    elapsed=iter([0.,1500.]);monkeypatch.setattr(runner.time,'monotonic',lambda:next(elapsed))
    calls=[]
    def collect(task,receipt):
        calls.append(task.name);receipt.parent.mkdir(parents=True,exist_ok=True)
        snapshot=receipt.with_suffix('.tar.gz')
        with tarfile.open(snapshot,'w:gz'):pass
        return {'status':'timeout'},snapshot
    value=runner.run_job(state_root=tmp_path/'jobs',runtime=tmp_path/'runtime',database=tmp_path/'db',
        as_of='2026-10-23',tasks=tasks,freeze_manifest=freeze,
        audit_source=lambda:({}, {'passed':True,'issues':[]}),
        publish=lambda _:pytest.fail('incomplete data must not publish'),
        clock=lambda:datetime(2026,10,24,10,tzinfo=runner.TZ),isolated_collect=collect)
    assert len(calls)==1 and not value['complete']
    assert value['tasks'][tasks[1].name]['issues'][0]['reason']=='group_budget_exhausted'
