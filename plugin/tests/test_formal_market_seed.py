"""Regression: online worker must not calculate bridges from a stale work cache."""
import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts.weekly_isolation import seed_formal_market_history, IsolationSafetyError
from qwl.datasources import collect
from qwl.datasources.price_basis import prepare_standardized_append


def bars(days, values):
    result=pd.DataFrame({f:values for f in ('open','high','low','close')},index=pd.to_datetime(days))
    result['volume']=100.
    return result


@pytest.fixture
def views(tmp_path):
    db=tmp_path/'formal';work=tmp_path/'private'
    stems={'01579_HK',*{f'{300000+i}_SZ' for i in range(59)}}
    for stem in stems:
        for suffix in ('','_raw'):
            base=db/'state/history'/f'{stem}{suffix}.parquet';base.parent.mkdir(parents=True,exist_ok=True)
            old=bars(['2026-09-25'],[100.]);old.to_parquet(base)
            stale=work/'state/history'/base.name;stale.parent.mkdir(parents=True,exist_ok=True);old.to_parquet(stale)
            version=db/'state/published_versions/state/history'/base.name/'previous.parquet'
            version.parent.mkdir(parents=True,exist_ok=True)
            bars(['2026-10-02'],[105.06 if not suffix else 103.]).to_parquet(version)
    return db,work,stems


def test_worker_seed_reads_formal_base_and_published_versions_not_stale_cache(views):
    db,work,stems=views
    formal_paths=[p for p in db.rglob('*.parquet') if p.is_file()]
    before={str(p):p.read_bytes() for p in formal_paths}
    stale=pd.read_parquet(work/'state/history/01579_HK.parquet')
    seed_formal_market_history(work,db,stems)
    seeded=pd.read_parquet(work/'state/history/01579_HK.parquet')
    assert len(stale)==1 and len(seeded)==2
    assert seeded.index.max()==pd.Timestamp('2026-10-02')
    assert seeded.loc['2026-10-02','close']==105.06
    assert len(list((work/'state/history').glob('*.parquet')))==120
    assert all(p.read_bytes()==before[str(p)] for p in formal_paths)


def test_real_sync_uses_seeded_formal_anchor_no_historical_write(views,monkeypatch):
    db,work,stems=views
    seed_formal_market_history(work,db,stems)
    qfq=bars(['2026-09-25','2026-10-02','2026-10-09'],[98.,103.,108.]);qfq.attrs['price_basis']='qfq'
    raw=bars(['2026-09-25','2026-10-02','2026-10-09'],[100.,103.,108.])
    prior={'method':'qfq_chain_v1','scale':1.02,'volume_scaled':False,
           'prior_as_of':'2026-10-02','actions':[]}
    monkeypatch.setattr('qwl.datasources.price_basis.validated_prior_chain',lambda *a,**kw:prior)
    monkeypatch.setattr(collect.paths,'STATE_DIR',work/'state')
    monkeypatch.setattr(collect.paths,'CACHE_DIR',work/'.cache')
    monkeypatch.setenv('QWL_BRIDGE_FORMAL_READONLY',str(db))
    monkeypatch.setenv('QWL_BRIDGE_PRIOR_JOB_READONLY',str(work/'job.json'))
    monkeypatch.setattr(collect,'_action_records_for',lambda symbol:[])
    symbol='01579.HK';old_formal=pd.read_parquet(work/'state/history/01579_HK.parquet')
    original={str(p):p.read_bytes() for p in db.rglob('*.parquet') if p.is_file()}
    item=SimpleNamespace(ohlcv=raw,standardized_ohlcv=qfq,standardized_source='approved-feed')
    result=collect.sync_bundle_history(collect.DataBundle('fixture','2026-10-09','fixture',{symbol:item},{'passed':True}))
    assert result['status']=='OK'
    assert result['basis_transition_counts']['CARRIED_QFQ_CHAIN_BRIDGE']==1
    current=pd.read_parquet(work/'state/history/01579_HK.parquet')
    pd.testing.assert_frame_equal(current.loc[old_formal.index],old_formal)
    assert current.loc['2026-10-09','close']==pytest.approx(108*1.02)
    assert all(p.read_bytes()==original[str(p)] for p in db.rglob('*.parquet') if p.is_file())


def test_seed_rejects_duplicate_formal_keys(views):
    db,work,stems=views
    duplicate=db/'state/published_versions/state/history/01579_HK.parquet/duplicate.parquet'
    bars(['2026-10-02'],[999.]).to_parquet(duplicate)
    with pytest.raises(ValueError,match='duplicate/conflicting'):
        seed_formal_market_history(work,db,stems)


def test_seed_missing_history_does_not_fall_back_to_cache(views):
    db,work,stems=views
    (db/'state/history/01579_HK_raw.parquet').unlink()
    for p in (db/'state/published_versions/state/history/01579_HK_raw.parquet').glob('*.parquet'):p.unlink()
    with pytest.raises(ValueError):seed_formal_market_history(work,db,stems)


def test_seed_requires_exact_fixed_count(views):
    db,work,stems=views
    with pytest.raises(IsolationSafetyError,match='exactly 60'):
        seed_formal_market_history(work,db,set(list(stems)[:-1]))
