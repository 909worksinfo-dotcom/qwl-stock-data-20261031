import copy
from datetime import date, datetime
import hashlib
import io
import json
from pathlib import Path
import tarfile
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.weekly_guard import _plan_json, build_freeze_manifest, effective_json, publish_weekly
from scripts.weekly_acceptance import validate_qfq_basis_bridges
from scripts.weekly_revalidation import (MARKET_TOKEN, ITEM_TOKEN, TOKEN, revalidate,
                                         resume_item_safety, resume_market_safety)
from scripts.weekly_runner import job_dir, TZ
from qwl.datasources.margin_contract import SSE_FIELDS, FULL_FIELDS, compatible_subset, merge_working_records
from qwl.datasources.controlled_benchmarks import build_document, BenchmarkConflict
from qwl.schemas import Market

def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False))

def archive(path, files):
    path.parent.mkdir(parents=True,exist_ok=True)
    with tarfile.open(path,"w:gz") as tar:
        for name,data in files.items():
            member=tarfile.TarInfo(name);member.size=len(data)
            tar.addfile(member,io.BytesIO(data))

def plan(tmp, relative, old, new):
    rel = Path(relative)
    db, source = tmp / 'db', tmp / 'source.json'
    write(db / rel, old)
    write(source, new)
    return _plan_json(source, db / rel, rel, db, date(2026,8,31), date(2026,9,4))

def margin():
    return {'date':'2026-09-04', 'market':'SH', **{k:1.0 for k in FULL_FIELDS}}

@pytest.mark.parametrize('source', ['stock_margin_detail_sse', 'RPTA_WEB_RZRQ_GGMX'])
def test_sse_subset_is_no_change_and_does_not_drop_fields(tmp_path, source):
    old = margin()
    candidate = {'date':'20260904', **{k:1 for k in SSE_FIELDS}}
    changes = plan(tmp_path, 'cache/margin/600104.json', {'records':[old]},
                   {'source':source, 'records':[candidate]})
    assert changes == []
    rows, conflicts, appended = merge_working_records([old], [candidate])
    assert rows == [old] and not conflicts and appended == 0

@pytest.mark.parametrize('alter', ['value','null','missing','extra','pit','unit','invalid_date','nan'])
def test_sse_real_or_unknown_differences_not_waived(alter):
    prior = margin()
    candidate = {'date':'20260904', **{k:1 for k in SSE_FIELDS}}
    if alter == 'value': candidate['margin_balance'] = 2
    if alter == 'null': candidate['margin_balance'] = None
    if alter == 'missing': candidate.pop('margin_balance')
    if alter == 'extra': candidate['unknown'] = 1
    if alter == 'pit': candidate['available_on'] = '2026-09-03'
    if alter == 'unit': candidate['unit'] = 'wan'
    if alter == 'invalid_date': candidate['date'] = '20260931'
    if alter == 'nan': candidate['margin_balance'] = float('nan')
    assert not compatible_subset(prior, candidate)

def test_working_enrichment_only_when_shared_values_unchanged():
    full = margin()
    partial = {'date':'20260904', **{k:1 for k in SSE_FIELDS}}
    rows, conflicts, appended = merge_working_records([partial], [full], enrich=True)
    assert set(rows[0]) == set(full) and rows[0]['date'] == '2026-09-04'
    assert not conflicts and appended == 0
    bad = {**full, 'margin_balance':2}
    assert merge_working_records([partial], [bad], enrich=True)[1]

def test_existing_margin_date_aliases_collapse_only_when_equivalent():
    full = margin()
    partial = {'date':'20260904', **{k:1 for k in SSE_FIELDS}}
    rows, conflicts, appended = merge_working_records([full,partial], [])
    assert rows == [full] and not conflicts and appended == 0
    bad = {**partial,'margin_balance':2}
    with pytest.raises(ValueError,match='duplicate margin date conflict'):
        merge_working_records([full,bad], [])

def test_hk_same_title_different_url_are_distinct_and_future_is_not_published(tmp_path):
    old = {'title':'股份购回', 'url':'https://www.hkexnews.hk/a.pdf', 'published_at':'2026-09-01', 'action_type':'repurchase'}
    new = {**old, 'url':'https://www.hkexnews.hk/b.pdf', 'published_at':'2026-09-03', 'available_on':'2026-09-04'}
    future = {**new, 'url':'https://www.hkexnews.hk/c.pdf', 'available_on':'2026-09-07'}
    changes = plan(tmp_path, 'cache/hk_action_evidence/00700.json', {'records':[old]}, {'records':[old,new,future]})
    assert next(c for c in changes if c.kind=='weekly_append').payload['append_records']['records'] == [new]
    assert not any(c.kind=='historical_revision' for c in changes)
    assert next(c for c in changes if c.kind=='historical_backfill').payload['records'][0]['classification']=='future'

def test_same_hk_identity_changed_value_is_still_revision(tmp_path):
    old = {'url':'https://www.hkexnews.hk/a.pdf','title':'same','published_at':'2026-09-01','amount':1}
    changes = plan(tmp_path,'cache/hk_action_evidence/00700.json',{'records':[old]}, {'records':[{**old,'amount':2}]})
    assert changes[0].kind == 'historical_revision'

def test_announcements_same_day_same_title_different_urls_are_distinct(tmp_path):
    first = {'url':'https://www1.hkexnews.hk/a.pdf','title':'翌日披露報表',
             'source_published_on':'2026-09-03','source_published_at':'2026-09-03T08:00:00+08:00'}
    second = {**first, 'url':'https://www1.hkexnews.hk/b.pdf',
              'source_published_at':'2026-09-03T20:00:00+08:00'}
    changes = plan(tmp_path, 'cache/announcements/09888.json', {'announcements':[]},
                   {'announcements':[first, second]})
    append = next(c for c in changes if c.kind == 'weekly_append')
    assert append.payload['append_records']['announcements'] == [first, second]
    assert not any(c.kind == 'historical_revision' for c in changes)

def test_announcements_same_url_changed_value_is_still_revision(tmp_path):
    old = {'url':'https://www1.hkexnews.hk/a.pdf','title':'公告',
           'source_published_on':'2026-09-03','amount':1}
    changes = plan(tmp_path, 'cache/announcements/09888.json', {'announcements':[old]},
                   {'announcements':[{**old, 'amount':2}]})
    assert changes[0].kind == 'historical_revision'
    assert changes[0].payload['revisions'][0]['classification'] == 'current_week'

def test_legacy_announcement_versions_are_not_reinterpreted_by_new_url_key(tmp_path):
    rel = Path('cache/announcements/00175.json')
    db = tmp_path / 'db'
    write(db / rel, {'announcements':[]})
    old = {'url':'https://www1.hkexnews.hk/a.pdf','title':'翌日披露報表',
           'source_published_on':'2026-08-31'}
    enriched = {**old, 'category':'HKEX', 'source':'HKEXnews'}
    overlay = db / 'state/published_versions' / rel
    write(overlay / 'a.json', {'schema_version':1, 'kind':'weekly_append',
                               'append_records':{'announcements':[old]}})
    write(overlay / 'b.json', {'schema_version':3, 'kind':'weekly_append',
                               'append_records':{'announcements':[enriched]}})
    assert effective_json(db, rel)['announcements'] == [old, enriched]

def test_new_announcement_version_records_key_contract(tmp_path):
    source, db, seal = tmp_path/'source', tmp_path/'db', tmp_path/'seal'
    seal.mkdir(); (seal/'fixed').write_text('fixed')
    freeze = tmp_path/'freeze.json'; build_freeze_manifest([('seal',seal)],freeze)
    write(db/'cache/announcements/09888.json', {'announcements':[]})
    write(source/'.cache/announcements/09888.json', {'announcements':[{
        'url':'https://www1.hkexnews.hk/a.pdf','title':'公告',
        'source_published_on':'2026-09-03'}]})
    result = publish_weekly(source, db, '2026-09-04', publish=True,
                            authorization='PUBLISH_CURRENT_WEEK', freeze_manifest=freeze)
    assert result['status'] == 'PUBLISHED'
    version = next((db/'state/published_versions/cache/announcements/09888.json').glob('*.json'))
    assert json.loads(version.read_text())['record_key_contract'] == 'typed-v3'

def test_statement_publication_dates_old_revision_not_current(tmp_path):
    old = {'report_date':'2025-06-30', 'published_at':'2025-08-27', 'fields':{'revenue':1}}
    changes = plan(tmp_path, 'cache/statements/002397.json', {'income':[old]}, {'income':[{**old,'fields':{'revenue':2}}]})
    assert changes[0].kind == 'historical_revision'
    assert changes[0].payload['revisions'][0]['classification'] == 'historical'

def test_content_hash_never_waives_pit_revision(tmp_path):
    old = {'dataset':'statements','section':'income','report_date':'2020-06-30','content_hash':'same',
           'available_on':'2026-08-31','first_seen_at':'2026-08-28','version_basis':'first_seen_revision'}
    changes = plan(tmp_path,'cache/financial_versions/09868.json',{'versions':[old]},
                   {'versions':[{**old,'available_on':None,'first_seen_at':'2026-08-11','version_basis':'legacy_source_publication_baseline'}]})
    assert changes[0].kind=='historical_revision'
    assert changes[0].payload['revisions'][0]['classification']=='undated'

def benchmark_fixture(tmp_path):
    ref = SimpleNamespace(symbol='000651.SZ', ak_symbol='000651', market=Market.A)
    universe = SimpleNamespace(stocks=[ref])
    history = tmp_path / 'state/history/000651_SZ_raw.parquet'
    history.parent.mkdir(parents=True)
    pd.DataFrame({'close':[10.,9.,9.9]}, index=pd.to_datetime(['2026-08-28','2026-08-31','2026-09-01'])).to_parquet(history)
    write(tmp_path / '.cache/dividends/000651.json', {'records':[]})
    write(tmp_path / '.cache/announcements/000651.json', {'records':[]})
    base = {'symbols':['000651.SZ'],'records':[{'date':'2026-08-28','equal_weight_level':1000.,'constituent_count':1}]}
    return universe, base

def test_benchmark_anchor_tail_and_idempotency(tmp_path):
    universe, base = benchmark_fixture(tmp_path)
    result = build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    assert result['records'][0] == base['records'][0]
    assert result['records'][1]['equal_weight_level'] == pytest.approx(900)
    assert result['records'][2]['equal_weight_level'] == pytest.approx(990)
    assert result['records'][1]['derivation']['quarantine_used'] is False
    assert result == build_document(universe, base['symbols'], result, '2026-09-04', tmp_path)

def test_benchmark_real_difference_blocks_and_reports_values(tmp_path):
    universe, base = benchmark_fixture(tmp_path)
    base['records'].append({'date':'2026-08-31','equal_weight_level':1000.,'constituent_count':1})
    with pytest.raises(BenchmarkConflict) as failure:
        build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    assert failure.value.evidence['comparisons'][0]['recomputed']['equal_weight_level'] == pytest.approx(900)

def test_late_action_cannot_retroactively_zero_earlier_return(tmp_path, monkeypatch):
    from qwl.datasources import evidence
    universe, base = benchmark_fixture(tmp_path)
    def known(ref, cutoff, **kwargs):
        return [date(2026,8,31)] if cutoff >= date(2026,9,1) else []
    monkeypatch.setattr(evidence, 'corporate_action_dates', known)
    result = build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    assert result['records'][1]['equal_weight_level'] == pytest.approx(900)

def test_action_known_on_effective_date_is_neutralized(tmp_path, monkeypatch):
    from qwl.datasources import evidence
    universe, base = benchmark_fixture(tmp_path)
    monkeypatch.setattr(evidence, 'corporate_action_dates', lambda ref, cutoff, **kw: [date(2026,8,31)])
    result = build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    assert result['records'][1]['equal_weight_level'] == pytest.approx(1000)

def test_explicit_later_availability_never_backdates_action(tmp_path):
    universe, base = benchmark_fixture(tmp_path)
    write(tmp_path / '.cache/dividends/000651.json', {'records': [{
        '最新公告日期':'2026-08-27', '除权除息日':'2026-08-31',
        '现金分红-现金分红比例':1, 'available_on':'2026-09-01'}]})
    result = build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    assert result['records'][1]['equal_weight_level'] == pytest.approx(900)

def test_new_benchmark_without_provenance_cannot_publish(tmp_path):
    old = {'records':[{'date':'2026-08-28','equal_weight_level':1000}]}
    incoming = {'records':[{'date':'2026-09-04','equal_weight_level':1010}]}
    changes = plan(tmp_path, 'cache/benchmarks/industry_test.json', old, incoming)
    assert any(c.kind == 'invalid_source' for c in changes)
    assert not any(c.kind == 'weekly_append' for c in changes)

def test_benchmark_hash_and_quarantine_claim_are_verified(tmp_path):
    universe, base = benchmark_fixture(tmp_path)
    result = build_document(universe, base['symbols'], base, '2026-09-04', tmp_path)
    result['records'][1]['derivation']['quarantine_used'] = True
    with pytest.raises(BenchmarkConflict, match='benchmark_provenance_mismatch'):
        build_document(universe, base['symbols'], result, '2026-09-04', tmp_path)

def test_margin_current_shape_cannot_be_satisfied_by_older_full_rows():
    from qwl.datasources.margin_contract import current_shape_complete
    partial = {'date':'2026-09-04', **{k:1 for k in SSE_FIELDS}}
    assert not current_shape_complete([{**margin(),'date':'2026-09-03'},partial],date(2026,9,4))

def test_financial_archiving_preserves_existing_hash_metadata_and_duplicates(tmp_path, monkeypatch):
    from qwl import paths
    from qwl.datasources import financial_versions as fv, evidence
    monkeypatch.setattr(paths, 'CACHE_DIR', tmp_path)
    monkeypatch.setattr(evidence, '_load_fundamentals_current', lambda ref: {})
    monkeypatch.setattr(fv, '_observation_available_on', lambda ref: '2026-09-07')
    ref = SimpleNamespace(ak_symbol='09868', symbol='09868.HK', market=Market.HK)
    old = {'dataset':'statements','section':'income','report_date':'2020-06-30', 'content_hash':'legacy',
           'version_basis':'legacy_source_publication_baseline','payload':{'published_at':'2020-08-01'}}
    p = tmp_path / 'financial_versions/09868.json'
    write(p, {'versions':[old,copy.deepcopy(old)]})
    result = fv.archive_stock(ref)
    assert result['metadata_updated'] == result['duplicates_removed'] == 0
    assert json.loads(p.read_text())['versions'] == [old,old]
    before = p.read_bytes()
    fv.audit_hk_statement_baselines(SimpleNamespace(stocks=[ref]))
    assert p.read_bytes() == before

def test_revalidation_preserves_blocked_state_until_authorized(tmp_path):
    db, state_root, seal = tmp_path / 'db', tmp_path / 'jobs', tmp_path / 'seal'
    write(db / 'cache/fixed.json', {'x':1})
    write(seal / 'code.json', {'v':1})
    freeze = tmp_path / 'freeze.json'
    build_freeze_manifest([('code', seal)], freeze)
    from scripts.weekly_guard import file_inventory
    state = {'status':'BLOCKED_PUBLICATION','baseline':file_inventory(db),'tasks':{},'attempt':11,
             'revision':'weekly-oneoff.20260906','waiver':{'enabled':True},'as_of_date':'2026-09-04'}
    p = job_dir(state_root, db, '2026-09-04') / 'job.json'
    write(p, state)
    (state_root / 'runtime.lock').touch()
    original = p.read_bytes()
    kw = dict(state_root=state_root,database=db,as_of='2026-09-04',freeze_manifest=freeze,
              clock=lambda:datetime(2026,9,6,12,tzinfo=TZ),assess=lambda:{'passed':True,'issues':[]})
    assert revalidate(**kw)['status']=='REVALIDATION_PREVIEW'
    assert p.read_bytes()==original
    assert revalidate(**kw,resume=True)['status']=='BLOCKED_REVALIDATION_AUTHORIZATION_OR_SHA'
    assert p.read_bytes()==original
    kw['assess'] = lambda:{'passed':False,'issues':[{'blocking':True,'reason':'real_conflict'}]}
    assert revalidate(**kw,resume=True,authorization=TOKEN,expected_checkpoint_sha=hashlib.sha256(original).hexdigest())['status']=='BLOCKED_REVALIDATION'
    assert p.read_bytes()==original
    kw['assess'] = lambda:{'passed':True,'issues':[]}
    out = revalidate(**kw,resume=True,authorization=TOKEN,expected_checkpoint_sha=hashlib.sha256(original).hexdigest())
    new = json.loads(p.read_text())
    assert out['checkpoint_modified'] and not out['database_modified']
    assert new['attempt']==11 and 'waiver' not in new
    assert Path(new['repair_revalidation']['prior_checkpoint']).read_bytes()==original

def test_item_safety_resume_is_bound_to_checkpoint_and_quarantine(tmp_path):
    db, state_root, seal = tmp_path/'db',tmp_path/'jobs',tmp_path/'seal'
    source_relative = 'cache/test.json'
    formal = db/source_relative;write(formal,{'old':1})
    write(seal/'code.json',{'v':1})
    freeze=tmp_path/'freeze.json';build_freeze_manifest([('code',seal)],freeze)
    from scripts.weekly_guard import file_inventory, _sha_bytes
    state={'status':'BLOCKED_ITEM_SAFETY','baseline':file_inventory(db),'tasks':{},'attempt':2,
           'revision':'weekly-contract.20260906','as_of_date':'2026-09-11',
           'active_task':'doc@abc','error':'already parsed document changed unexpectedly'}
    checkpoint=job_dir(state_root,db,'2026-09-11')/'job.json';write(checkpoint,state)
    (state_root/'runtime.lock').touch()
    candidate=b'{"candidate":1}'
    snapshot=checkpoint.parent/'attempts/2/doc@abc.tar.gz'
    archive(snapshot,{source_relative:candidate})
    evidence_path=db/'state/quarantine/run/cache/test.json/evidence.json'
    candidate_path=evidence_path.parent/'candidate.json'
    candidate_path.parent.mkdir(parents=True,exist_ok=True);candidate_path.write_bytes(candidate)
    evidence={'source_relative':source_relative,'authorization_required':True,
              'authorization':{'scope':'quarantine_only','user_authorized':True,
                               'formal_promotion_authorized':False},
              'candidate_artifact':{'path':'candidate.json','sha256':_sha_bytes(candidate)},
              'formal_baseline':{'path':str(formal),'sha256':_sha_bytes(formal.read_bytes())},
              'provenance':{'active_task':'doc@abc','job_checkpoint':str(checkpoint),
                            'provider_snapshot':str(snapshot),
                            'provider_snapshot_sha256':_sha_bytes(snapshot.read_bytes())}}
    write(evidence_path,evidence)
    original=checkpoint.read_bytes();checkpoint_sha=hashlib.sha256(original).hexdigest()
    kw=dict(state_root=state_root,database=db,as_of='2026-09-11',freeze_manifest=freeze,
            quarantine_evidence=str(evidence_path),expected_checkpoint_sha=checkpoint_sha,
            clock=lambda:datetime(2026,9,13,12,tzinfo=TZ))
    assert resume_item_safety(**kw)['status']=='BLOCKED_ITEM_RECOVERY_AUTHORIZATION_OR_SHA'
    assert checkpoint.read_bytes()==original
    out=resume_item_safety(**kw,authorization=ITEM_TOKEN)
    resumed=json.loads(checkpoint.read_text())
    assert out['checkpoint_modified'] and not out['database_modified']
    assert resumed['status']=='WAITING_RETRY' and resumed['tasks']['doc@abc']['ok']
    assert formal.read_text()==json.dumps({'old':1},ensure_ascii=False)
    assert Path(resumed['item_safety_recovery']['prior_checkpoint']).read_bytes()==original


def test_market_safety_resume_requires_exact_snapshot_and_validation(tmp_path):
    db, state_root, seal = tmp_path/'db',tmp_path/'jobs',tmp_path/'seal'
    write(db/'cache/fixed.json', {'old':1})
    write(seal/'code.json', {'v':1})
    freeze=tmp_path/'freeze.json';build_freeze_manifest([('code',seal)],freeze)
    from scripts.weekly_guard import file_inventory, _sha_bytes
    state={'status':'BLOCKED_ITEM_SAFETY','baseline':file_inventory(db),'tasks':{},'attempt':5,
           'revision':'weekly-contract.20260906','as_of_date':'2026-09-11',
           'active_task':'market','error':'qfq overlap drift'}
    checkpoint=job_dir(state_root,db,'2026-09-11')/'job.json';write(checkpoint,state)
    (state_root/'runtime.lock').touch()
    snapshot=checkpoint.parent/'attempts/5/market.tar.gz'
    snapshot.parent.mkdir(parents=True,exist_ok=True);snapshot.write_bytes(b'provider')
    original=checkpoint.read_bytes();checkpoint_sha=hashlib.sha256(original).hexdigest()
    snapshot_sha=_sha_bytes(snapshot.read_bytes())
    validation={'passed':True,'historical_records_published':0,
                'fixed_universe_count':60,'validation_sha256':'proof'}
    kw=dict(state_root=state_root,database=db,as_of='2026-09-11',freeze_manifest=freeze,
            expected_checkpoint_sha=checkpoint_sha,
            expected_provider_snapshot_sha=snapshot_sha,
            expected_validation_sha='proof',validate=lambda value:validation,
            clock=lambda:datetime(2026,9,13,12,tzinfo=TZ))
    assert resume_market_safety(**kw)['status']=='BLOCKED_MARKET_RECOVERY_AUTHORIZATION_OR_SHA'
    assert checkpoint.read_bytes()==original
    assert resume_market_safety(**{**kw,'expected_provider_snapshot_sha':'bad'},
                                authorization=MARKET_TOKEN)['status']=='BLOCKED_MARKET_PROVIDER_SNAPSHOT_DRIFT'
    assert checkpoint.read_bytes()==original
    out=resume_market_safety(**kw,authorization=MARKET_TOKEN)
    resumed=json.loads(checkpoint.read_text())
    assert out['checkpoint_modified'] and not out['database_modified']
    assert resumed['status']=='WAITING_RETRY' and not resumed['tasks']['market']['ok']
    assert Path(resumed['market_safety_recovery']['prior_checkpoint']).read_bytes()==original


def test_market_snapshot_replay_is_read_only_and_fixed_60(tmp_path):
    from scripts.weekly_market_repair import validate_qfq_market_snapshot
    dates=pd.bdate_range(end='2026-09-11',periods=65)
    formal_dates=dates[dates <= pd.Timestamp('2026-09-04')]
    securities=[];members={}
    for number in range(60):
        symbol=f'{number:06d}.SZ';stem=symbol.replace('.','_')
        securities.append(SimpleNamespace(symbol=symbol))
        values=pd.DataFrame({name:range(1,66) for name in ('open','high','low','close','volume')},index=dates,dtype=float)
        qfq=values.copy();qfq.attrs['price_basis']='qfq'
        raw=values.copy();raw.attrs['price_basis']='none'
        target=tmp_path/'db/state/history'/f'{stem}.parquet';target.parent.mkdir(parents=True,exist_ok=True)
        qfq.loc[formal_dates].to_parquet(target)
        for folder,value in (('ohlcv_standardized',qfq),('ohlcv',raw)):
            buffer=io.BytesIO();value.to_parquet(buffer)
            members[f'snapshot/{folder}/{stem}.parquet']=buffer.getvalue()
    snapshot=tmp_path/'market.tar.gz';archive(snapshot,members)
    before={str(p.relative_to(tmp_path/'db')):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (tmp_path/'db').rglob('*') if p.is_file()}
    result=validate_qfq_market_snapshot(provider_snapshot=snapshot,database=tmp_path/'db',
                                        as_of='2026-09-11',securities=securities)
    after={str(p.relative_to(tmp_path/'db')):hashlib.sha256(p.read_bytes()).hexdigest()
           for p in (tmp_path/'db').rglob('*') if p.is_file()}
    assert result['passed'] and result['fixed_universe_count']==60
    assert result['classification_counts']=={'UNCHANGED_QFQ_OVERLAP':60}
    assert result['historical_records_published']==0 and before==after


def test_qfq_bridge_acceptance_recomputes_candidate_from_immutable_receipt(tmp_path):
    from qwl.datasources.price_basis import differences, frame_sha256
    dates=pd.to_datetime(['2026-09-04','2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11'])
    qfq=pd.DataFrame({name:[10.,5.,10.,11.,12.,13.] for name in ('open','high','low','close')},index=dates)
    qfq['volume']=[1.,2.,3.,4.,5.,6.];qfq.attrs['price_basis']='qfq'
    raw=qfq.copy();raw.loc[pd.Timestamp('2026-09-07'),list(('open','high','low','close'))]=10.
    raw.attrs['price_basis']='none'
    scale=2.0
    candidate=qfq.copy();candidate.loc[dates[1:],list(('open','high','low','close'))]*=scale
    candidate.loc[pd.Timestamp('2026-09-04'),list(('open','high','low','close'))]=20.
    path=tmp_path/'candidate/state/history/000001_SZ.parquet';path.parent.mkdir(parents=True)
    candidate.to_parquet(path)
    transition={'classification':'ACTION_AWARE_CHAIN_BRIDGE','week_start':'2026-09-07','as_of':'2026-09-11',
        'anchor_date':'2026-09-04','incoming_qfq_sha256':frame_sha256(qfq),'raw_sha256':frame_sha256(raw),
        'formal_anchor_sha256':frame_sha256(candidate.loc[[pd.Timestamp('2026-09-04')]]),
        'current_week_qfq_raw_differences':differences(raw.loc[dates[1:]],qfq.loc[dates[1:]]),
        'historical_records_published':0,'bridge':{'method':'qfq_chain_v1','scale':scale,'volume_scaled':False,
            'actions':[{'ex_date':'2026-09-08','available_on':'2026-09-01'}]}}
    qbuf=io.BytesIO();qfq.to_parquet(qbuf);rbuf=io.BytesIO();raw.to_parquet(rbuf)
    snapshot=tmp_path/'market.tar.gz';archive(snapshot,{
        'state/snapshots/x/ohlcv_standardized/000001_SZ.parquet':qbuf.getvalue(),
        'state/snapshots/x/ohlcv/000001_SZ.parquet':rbuf.getvalue()})
    receipt=tmp_path/'market.json'
    write(receipt,{'ok':True,'issues':[],'provider_snapshot':str(snapshot),'result':{
        'gate':{'passed':True},'history_sync':{'price_basis_blocked':False,
        'basis_transition_counts':{'UNCHANGED_QFQ_OVERLAP':59,'ACTION_AWARE_CHAIN_BRIDGE':1},
        'symbols':{'000001.SZ':{'standardized':{'basis_transition':transition}}}}}})
    proof={'receipt_path':str(receipt),'receipt_sha256':hashlib.sha256(receipt.read_bytes()).hexdigest(),
           'provider_snapshot':str(snapshot),'provider_snapshot_sha256':hashlib.sha256(snapshot.read_bytes()).hexdigest()}
    valid,errors=validate_qfq_basis_bridges(tmp_path/'candidate','2026-09-11',proof)
    assert not errors and valid['000001.SZ']['candidate_week_rows']==5
    candidate.loc[pd.Timestamp('2026-09-11'),'close']+=1;candidate.to_parquet(path)
    assert validate_qfq_basis_bridges(tmp_path/'candidate','2026-09-11',proof)[1][0]['blocking']
