"""Immutable formal proof compatibility; no previous checkpoints or waivers adopted."""
import hashlib
import io
import json
import tarfile
from pathlib import Path

import pandas as pd
import pytest

from qwl.datasources import approved_qfq_bridge as approved
from qwl.datasources.price_basis import (
    _record_sha256, frame_sha256, prepare_standardized_append, validated_prior_chain,
)


def bars(days, value=100., basis='qfq'):
    data = pd.DataFrame({name:[value+i for i in range(len(days))]
                         for name in ('open','high','low','close')},index=pd.to_datetime(days))
    data['volume'] = 100.
    data.attrs = {'price_basis':basis,'response_key':'qfqday' if basis=='qfq' else 'day'}
    return data


def write_json(path, value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value))
    return approved.sha(path)


def add_frame(archive, name, value):
    handle=io.BytesIO();value.to_parquet(handle);data=handle.getvalue()
    member=tarfile.TarInfo(name);member.size=len(data)
    archive.addfile(member,io.BytesIO(data))
    return {'member':name,'sha256':hashlib.sha256(data).hexdigest()}


@pytest.fixture
def formal_proof(tmp_path, monkeypatch):
    db=tmp_path/'db'; job=tmp_path/'jobs/key/2026-10-02/job.json'
    write_json(job,{'status':'RUNNING','as_of_date':'2026-10-02','tasks':{'market':{'ok':False}}})
    symbol='01579.HK';stem='01579_HK'
    days=['2026-09-28','2026-09-29','2026-09-30','2026-10-02']
    qfq=bars(days); raw=bars(days,basis='none'); scale=1.02
    run_id='2026-09-28_2026-10-02_fixture';run=db/'state/weekly_runs'/run_id
    archive_path=job.parent/'attempts/2/market.tar.gz';archive_path.parent.mkdir(parents=True)
    with tarfile.open(archive_path,'w:gz') as archive:
        qm=add_frame(archive,f'state/snapshots/2026-10-02/id/ohlcv_standardized/{stem}.parquet',qfq)
        rm=add_frame(archive,f'state/snapshots/2026-10-02/id/ohlcv/{stem}.parquet',raw)
        meta=json.dumps({'as_of':'2026-10-02','symbols':{symbol:{'standardized_source':'vendor-qfq'}}}).encode()
        m=tarfile.TarInfo('state/snapshots/2026-10-02/id/meta.json');m.size=len(meta)
        archive.addfile(m,io.BytesIO(meta))
    expected=qfq.copy();expected.loc[:,['open','high','low','close']]*=scale
    action={'ex_date':'2026-09-08','available_on':'2026-09-01'}
    bridge={'method':'qfq_chain_v1','scale':scale,'volume_scaled':False,'actions':[
        {**action,'record':action,'record_sha256':_record_sha256(action)}]}
    item={'audit':{'classification':'CARRIED_QFQ_CHAIN_BRIDGE','bridge':bridge,
                  'anchor_date':'2026-09-25','historical_records_published':0},
          'qfq_source':qm,'raw_source':rm,'expected_sessions':days,'historical_prefix_rows_changed':0}
    stage_path=tmp_path/'proof/MARKET_STAGE_PROOF.json'
    stage={'status':'PRIVATE_MARKET_120_VIEWS_VERIFIED','as_of_date':'2026-10-02',
           'symbols':{symbol:item,**{f'fixture{i}':{} for i in range(59)}},
           'provider_archive_sha256':approved.sha(archive_path),
           'formal_existing_files_rewritten':0,'historical_revisions_published':0,'historical_backfills_published':0}
    stage_sha=write_json(stage_path,stage)
    views={}
    catalog=[];manifest=[]
    versions=[]
    for suffix,value in (('',expected),('_raw',raw)):
        rel=f'state/published_versions/state/history/{stem}{suffix}.parquet/{run_id}.parquet'
        path=db/rel;path.parent.mkdir(parents=True);value.to_parquet(path);digest=approved.sha(path)
        catalog.append({'relative':rel,'sha256':digest})
        manifest.append(f'{digest}  {path.stat().st_size}  {rel}\n');versions.append(path)
        views[suffix]={'tail_business_sha256':frame_sha256(value),'rows':len(days)}
    run.mkdir(parents=True)
    (run/'MANIFEST_SHA256_SIZE.txt').write_text(''.join(manifest))
    write_json(run/'incremental_catalog.json',{'entries':catalog})
    recheck_path=stage_path.with_name('MARKET_CANDIDATE_RECHECK.json')
    recheck={'status':'MARKET_CANDIDATE_RECHECK_PASS','as_of_date':'2026-10-02',
             'symbols':{symbol:{'classification':'CARRIED_QFQ_CHAIN_BRIDGE','views':views,'expected_sessions':days},
                        **{f'fixture{i}':{} for i in range(59)}},
             'stage_proof_sha256':stage_sha,'source_archive_sha256':approved.sha(archive_path),
             'historical_prefix_rows_changed':0,'formal_database_files_written':0}
    recheck_sha=write_json(recheck_path,recheck)
    pinned={'path':str(recheck_path),'sha256':recheck_sha}
    publication={'status':'PUBLISHED','run_id':run_id,'formal_acceptance':{
        'passed':True,'manual_exception_proofs':{'market':pinned}},'formal_existing_files_rewritten':0,
        'historical_revisions_published':0,'historical_backfills_published':0}
    publication_sha=write_json(run/'publication.json',publication)
    relative='state/pit_gateway/releases/fixture/release.json'
    release={'weekly_run_id':run_id,'weekly_incremental_manifest_sha256':approved.sha(run/'MANIFEST_SHA256_SIZE.txt'),
             'known_exceptions':{'proofs':{'market':pinned},'holiday_waiver':{'ignore_all':True}}}
    release_sha=write_json(db/relative,release)
    spec={'relative':relative,'sha256':release_sha,'market_key':'proofs',
          'snapshot_relative':'attempts/2/market.tar.gz','publication_sha256':publication_sha,
          'catalog_sha256':approved.sha(run/'incremental_catalog.json')}
    monkeypatch.setattr(approved,'APPROVED_RELEASES',{'2026-10-02':spec})
    future=bars(['2026-10-05','2026-10-06','2026-10-07','2026-10-08','2026-10-09'],104.)
    current=pd.concat([qfq,future]);current.attrs=qfq.attrs.copy()
    current_raw=pd.concat([raw,future]);current_raw.attrs=raw.attrs.copy()
    return dict(db=db,job=job,symbol=symbol,qfq=current,raw=current_raw,formal=expected,
                stage=stage_path,recheck=recheck_path,release=db/relative,run=run,versions=versions,archive=archive_path)


def read_proof(f, **overrides):
    values=dict(database=f['db'],prior_job=f['job'],symbol=f['symbol'],incoming=f['qfq'],raw=f['raw'],
                week_start='2026-10-05',source='vendor-qfq')
    values.update(overrides)
    return validated_prior_chain(**values)


def test_approved_published_proof_no_checkpoint_or_history_changes(formal_proof):
    f=formal_proof;paths=[f['job'],*f['versions']];before={str(p):p.read_bytes() for p in paths}
    proof=read_proof(f)
    assert proof['prior_last_session']=='2026-10-02'
    assert proof['prior_source_proof']=='explicitly_approved_formal_publication'
    assert 'holiday_waiver' not in proof and 'known_exceptions' not in proof
    result,audit=prepare_standardized_append(f['formal'],f['qfq'],f['raw'],week_start='2026-10-05',
                                           as_of='2026-10-09',prior_chain=proof)
    assert audit['historical_records_published']==0
    assert result.loc['2026-10-09','close']==pytest.approx(108*1.02)
    assert result.volume.eq(100.).all()
    assert all(p.read_bytes()==before[str(p)] for p in paths)


@pytest.mark.parametrize('which',['stage','recheck','release','archive','manifest','catalog','publication','version'])
def test_any_evidence_hash_drift_blocks(formal_proof,which):
    f=formal_proof
    path={'manifest':f['run']/'MANIFEST_SHA256_SIZE.txt','catalog':f['run']/'incremental_catalog.json',
          'publication':f['run']/'publication.json','version':f['versions'][0]}.get(which,f.get(which))
    path.write_bytes(path.read_bytes()+b' ')
    with pytest.raises(ValueError,match='SHA'):
        read_proof(f)


@pytest.mark.parametrize('which',['source','vintage','attrs','symbol','week'])
def test_wrong_source_scope_or_vintage_blocks(formal_proof,which):
    f=formal_proof;args={}
    if which=='source':args['source']='other-feed'
    if which=='vintage':f['qfq'].iloc[0,0]+=.01
    if which=='attrs':f['qfq'].attrs['response_key']='day'
    if which=='symbol':args['symbol']='09988.HK'
    if which=='week':args['week_start']='2026-10-12'
    with pytest.raises((ValueError,KeyError)):
        read_proof(f,**args)


def test_wrong_coefficient_and_current_week_missing_are_not_waived(formal_proof):
    f=formal_proof;proof=read_proof(f)
    with pytest.raises(ValueError,match='scale'):
        prepare_standardized_append(f['formal'],f['qfq'],f['raw'],week_start='2026-10-05',as_of='2026-10-09',
                                    prior_chain={**proof,'scale':1.03})
    with pytest.raises(ValueError,match='sessions'):
        prepare_standardized_append(f['formal'],f['qfq'].iloc[:-1],f['raw'].iloc[:-1],
                                    week_start='2026-10-05',as_of='2026-10-09',prior_chain=proof)


def test_additive_origin_requires_exact_approved_hash_and_offset():
    with pytest.raises(ValueError,match='approved proof'):
        approved.validated_additive_origin({'method':'cash_additive_oneoff','offset':1.726,
                                            'volume_scaled':False,'original_proof_sha256':'wrong'})


def test_additive_cannot_be_used_without_symbol_and_approved_origin(formal_proof):
    f=formal_proof
    with pytest.raises(ValueError,match='additive bridge scope'):
        prepare_standardized_append(f['formal'],f['qfq'],f['raw'],week_start='2026-10-05',as_of='2026-10-09',
                                    prior_chain={'method':'cash_additive_oneoff','offset':1.726})
