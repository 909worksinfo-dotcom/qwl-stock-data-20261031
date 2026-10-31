"""Offline regression tests of canonical price semantics and weekly gates."""
import json
import gzip
import hashlib
import io
import tarfile
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
import sys

import pandas as pd
import pytest

RUNTIME = Path(json.loads((Path(__file__).parents[1] / 'config/runtime.json').read_text())['qwl_home'])
sys.path.insert(0, str(RUNTIME / 'src'))
from qwl.datasources import akshare_provider, collect, tencent_kline, extended
from qwl.schemas import Market
from qwl.datasources.price_basis import (
    comparison, differences, guarded_standardized_append,
    prepare_standardized_append,
    validated_prior_chain,
)
from scripts.weekly_acceptance import coverage_gate, REQUIRED_DOMAINS
from scripts.weekly_runner import run_job, TZ
from scripts.weekly_tasks import Task
from scripts.weekly_guard import build_freeze_manifest, _plan_json


def bars(values, dates=('2026-08-28', '2026-09-04'), basis=None):
    f = pd.DataFrame({c: values for c in ('open', 'high', 'low', 'close')}, index=pd.to_datetime(dates))
    f['volume'] = 100.
    if basis:
        f.attrs['price_basis'] = basis
    return f


def test_cross_basis_is_diagnostic_not_a_supplier_revision():
    result = comparison(bars([9., 11.]), bars([10., 11.]), None,
                        week_start='2026-08-31', as_of='2026-09-04')
    assert result['status'] == 'basis_unverified'
    assert not result['basis_verified']
    assert result['current_week_raw_equivalent']
    assert result['snapshot_mismatch_dates'] == ['2026-08-28']


def test_qfq_compares_with_qfq_not_raw():
    qfq = bars([9., 11.], basis='qfq')
    result = comparison(qfq, bars([10., 11.]), qfq,
                        week_start='2026-08-31', as_of='2026-09-04')
    assert result['status'] == 'fresh' and result['snapshot_mismatch_cells'] == 0
    assert result['basis_verified']


def test_true_same_basis_revision_is_not_waived():
    result = comparison(bars([9., 11.]), bars([10., 11.]), bars([8., 11.], basis='qfq'),
                        week_start='2026-08-31', as_of='2026-09-04')
    assert result['status'] == 'conflict'
    assert result['snapshot_mismatch_dates'] == ['2026-08-28']


@pytest.mark.parametrize('kind', ['raw', 'unknown', 'changed', 'no_overlap'])
def test_unsafe_standardized_tail_never_accepted(kind):
    old = bars([9.], dates=['2026-08-28'])
    new = bars([9., 11.], basis='qfq')
    if kind == 'raw': new.attrs['price_basis'] = 'none'
    if kind == 'unknown': new.attrs.clear()
    if kind == 'changed': new.iloc[0, 0] = 8.
    if kind == 'no_overlap': new = new.iloc[1:]
    with pytest.raises(ValueError):
        guarded_standardized_append(old, new)


def test_historical_only_qfq_drift_allows_unchanged_current_week_tail():
    dates = ['2026-08-28', '2026-09-04', '2026-09-07', '2026-09-08',
             '2026-09-09', '2026-09-10', '2026-09-11']
    old = bars([9., 11.], dates=dates[:2])
    raw = bars([10., 11., 12., 13., 14., 15., 16.], dates=dates)
    qfq = bars([8., 11., 12., 13., 14., 15., 16.], dates=dates, basis='qfq')
    prepared, audit = prepare_standardized_append(
        old, qfq, raw, week_start='2026-09-07', as_of='2026-09-11')
    assert audit['classification'] == 'SAFE_CURRENT_WEEK_APPEND'
    assert list(prepared.index.strftime('%Y-%m-%d')) == dates[1:]
    assert prepared.loc['2026-09-04', 'close'] == 11.
    assert prepared.loc['2026-09-11', 'close'] == 16.
    assert audit['historical_records_published'] == 0


def test_documented_ex_date_uses_qfq_chain_and_never_scales_volume():
    dates = ['2026-08-28', '2026-09-04', '2026-09-07', '2026-09-08',
             '2026-09-09', '2026-09-10', '2026-09-11']
    old = bars([90., 100.], dates=dates[:2])
    raw = bars([90., 100., 100., 99., 101., 102., 103.], dates=dates)
    qfq = bars([88., 98., 98., 99., 101., 102., 103.], dates=dates, basis='qfq')
    prepared, audit = prepare_standardized_append(
        old, qfq, raw, week_start='2026-09-07', as_of='2026-09-11',
        action_records=[{'ex_date': '2026-09-08', 'available_on': '2026-09-01'}])
    assert audit['classification'] == 'ACTION_AWARE_CHAIN_BRIDGE'
    assert audit['bridge']['method'] == 'qfq_chain_v1'
    assert audit['bridge']['scale'] == pytest.approx(100 / 98)
    assert prepared.loc['2026-09-07', 'close'] == pytest.approx(100.)
    assert prepared.loc['2026-09-08', 'close'] == pytest.approx(99 * 100 / 98)
    assert prepared['volume'].eq(100.).all()


def test_carry_previous_published_qfq_chain_sealed_source_and_history(tmp_path, monkeypatch):
    symbol = '01579.HK'
    prior_dates = ['2026-09-04', '2026-09-07', '2026-09-08',
                   '2026-09-09', '2026-09-10', '2026-09-11']
    raw = bars([100., 100., 99., 101., 102., 103.], dates=prior_dates)
    qfq = bars([98., 98., 99., 101., 102., 103.], dates=prior_dates, basis='qfq')
    prior_formal = bars([100.], dates=prior_dates[:1])
    action = {'ex_date': '2026-09-08', 'available_on': '2026-09-01'}
    prepared, audit = prepare_standardized_append(
        prior_formal, qfq, raw, week_start='2026-09-07',
        as_of='2026-09-11', action_records=[action])
    run_id = '2026-09-07_2026-09-11_fixture'
    db = tmp_path / 'db'
    run = db / 'state/weekly_runs' / run_id
    run.mkdir(parents=True)
    version_rel = (f'state/published_versions/state/history/01579_HK.parquet/'
                   f'{run_id}.parquet')
    evidence_rel = (f'state/weekly_runs/{run_id}/provider_full_responses/'
                    'cache/qfq_basis_evidence/01579.json.gz')
    version = db / version_rel
    version.parent.mkdir(parents=True)
    prepared.iloc[1:].to_parquet(version)
    old_version = version.read_bytes()
    archived = {**audit, 'symbol': symbol, 'source': 'akshare_hk_sina_qfq',
                'source_basis': 'qfq', 'authorization_required': True,
                'formal_history_modified': False}
    evidence = db / evidence_rel
    evidence.parent.mkdir(parents=True)
    evidence.write_bytes(gzip.compress(json.dumps(archived).encode()))
    sha = lambda data: hashlib.sha256(data).hexdigest()
    (run / 'MANIFEST_SHA256_SIZE.txt').write_text(''.join(
        f'{sha(path.read_bytes())}  {path.stat().st_size}  {relative}\n'
        for path, relative in ((version, version_rel), (evidence, evidence_rel))))
    (run / 'incremental_catalog.json').write_text(json.dumps({'entries': [
        {'relative': version_rel, 'sha256': sha(version.read_bytes())}]}))
    (run / 'publication.json').write_text(json.dumps({
        'status': 'PUBLISHED', 'run_id': run_id, 'formal_acceptance': {'passed': True},
        'formal_existing_files_rewritten': 0, 'historical_revisions_published': 0,
        'historical_backfills_published': 0}))
    job = tmp_path / 'jobs/key/2026-09-11/job.json'
    receipt = job.parent / 'attempts/1/market.json'
    receipt.parent.mkdir(parents=True)
    snapshot = receipt.with_suffix('.tar.gz')
    with tarfile.open(snapshot, 'w:gz') as archive:
        for name, value in (('ohlcv_standardized', qfq), ('ohlcv', raw)):
            output = io.BytesIO()
            value.to_parquet(output)
            data = output.getvalue()
            entry = tarfile.TarInfo(
                f'state/snapshots/2026-09-11/id/{name}/01579_HK.parquet')
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
    receipt.write_text(json.dumps({'ok': True, 'issues': [],
        'provider_snapshot': str(snapshot), 'result': {
            'gate': {'passed': True}, 'history_sync': {'symbols': {symbol: {
                'standardized': {'basis_transition': audit}}}}}}))
    job.write_text(json.dumps({'status': 'COMPLETE', 'as_of_date': '2026-09-11',
        'publication': {'status': 'PUBLISHED', 'run_id': run_id},
        'tasks': {'market': {'ok': True, 'receipt': str(receipt)}}}))
    week = ['2026-09-14', '2026-09-15', '2026-09-16', '2026-09-17', '2026-09-18']
    current_qfq = bars([98., 98., 99., 101., 102., 103.,
                        104., 105., 106., 107., 108.],
                       dates=prior_dates + week, basis='qfq')
    current_raw = raw.copy()
    current_raw = pd.concat([current_raw, bars([104.,105.,106.,107.,108.], dates=week)])
    formal = pd.concat([prior_formal, prepared.iloc[1:]])
    old_formal = formal.copy(deep=True)
    proof = validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                                  week_start='2026-09-14', source='akshare_hk_sina_qfq')
    new, transition = prepare_standardized_append(
        formal, current_qfq, current_raw, week_start='2026-09-14',
        as_of='2026-09-18', prior_chain=proof)
    assert transition['classification'] == 'CARRIED_QFQ_CHAIN_BRIDGE'
    assert transition['historical_records_published'] == 0
    assert len(new) == 6 and new['volume'].eq(100.).all()
    assert new.loc['2026-09-18','close'] == pytest.approx(108 * 100 / 98)
    pd.testing.assert_frame_equal(formal, old_formal)
    assert version.read_bytes() == old_version
    work = tmp_path / 'work'
    history = work / 'state/history'
    history.mkdir(parents=True)
    monkeypatch.setattr(collect.paths, 'STATE_DIR', work / 'state')
    monkeypatch.setattr(collect.paths, 'CACHE_DIR', work / '.cache')
    monkeypatch.setenv('QWL_BRIDGE_FORMAL_READONLY', str(db))
    monkeypatch.setenv('QWL_BRIDGE_PRIOR_JOB_READONLY', str(job))
    formal.to_parquet(history / '01579_HK.parquet')
    raw.to_parquet(history / '01579_HK_raw.parquet')
    item = SimpleNamespace(ohlcv=current_raw, standardized_ohlcv=current_qfq,
                           standardized_source='akshare_hk_sina_qfq')
    collected = collect.sync_bundle_history(collect.DataBundle(
        'test', '2026-09-18', 'test', {symbol: item}, {'passed': True}))
    assert collected['status'] == 'OK'
    assert collected['basis_transition_counts']['CARRIED_QFQ_CHAIN_BRIDGE'] == 1
    assert collected['symbols'][symbol]['standardized']['basis_transition'][
        'historical_records_published'] == 0
    pd.testing.assert_frame_equal(pd.read_parquet(history / '01579_HK.parquet').loc[formal.index], formal)
    assert version.read_bytes() == old_version
    with pytest.raises(ValueError, match='source'):
        validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                              week_start='2026-09-14', source='different_qfq_feed')
    changed_qfq = current_qfq.copy()
    changed_qfq.loc['2026-09-11', 'close'] += .01
    with pytest.raises(ValueError, match='vintage'):
        validated_prior_chain(db, job, symbol, changed_qfq, current_raw,
                              week_start='2026-09-14', source='akshare_hk_sina_qfq')
    with pytest.raises(ValueError, match='scale'):
        prepare_standardized_append(formal, current_qfq, current_raw,
                                    week_start='2026-09-14', as_of='2026-09-18',
                                    prior_chain={**proof, 'scale': proof['scale'] + .01})
    durable = tmp_path / 'snapshots/2026-09-11/id'
    (durable / 'ohlcv_standardized').mkdir(parents=True)
    (durable / 'ohlcv').mkdir()
    (durable / 'meta.json').write_text(json.dumps({
        'as_of': '2026-09-11', 'symbols': {symbol: {
            'standardized_source': 'akshare_hk_sina_qfq'}}}))
    qfq.to_parquet(durable / 'ohlcv_standardized/01579_HK.parquet')
    raw.to_parquet(durable / 'ohlcv/01579_HK.parquet')
    snapshot.unlink()
    recovered = validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                                      week_start='2026-09-14', source='akshare_hk_sina_qfq')
    assert recovered['prior_source_proof'] == 'manifest_bound_basis_and_matching_snapshot'
    assert recovered['prior_receipt_sha256'] == sha(receipt.read_bytes())
    receipt.unlink()
    recovered = validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                                      week_start='2026-09-14', source='akshare_hk_sina_qfq')
    assert recovered['prior_source_proof'] == 'manifest_bound_basis_and_matching_snapshot'
    assert 'prior_receipt_sha256' not in recovered
    corrupted = raw.copy()
    corrupted.loc['2026-09-11', 'close'] += 1
    corrupted.to_parquet(durable / 'ohlcv/01579_HK.parquet')
    with pytest.raises(ValueError, match='snapshot is missing/ambiguous'):
        validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                              week_start='2026-09-14', source='akshare_hk_sina_qfq')
    raw.to_parquet(durable / 'ohlcv/01579_HK.parquet')
    evidence.write_bytes(evidence.read_bytes() + b'tamper')
    with pytest.raises(ValueError, match='SHA'):
        validated_prior_chain(db, job, symbol, current_qfq, current_raw,
                              week_start='2026-09-14', source='akshare_hk_sina_qfq')


@pytest.mark.parametrize('record', [None, {'ex_date': '2026-09-08', 'available_on': '2026-09-09'}])
def test_unexplained_or_late_action_qfq_rebase_stays_blocked(record):
    dates = ['2026-08-28', '2026-09-04', '2026-09-07', '2026-09-08',
             '2026-09-09', '2026-09-10', '2026-09-11']
    old = bars([90., 100.], dates=dates[:2])
    raw = bars([90., 100., 100., 99., 101., 102., 103.], dates=dates)
    qfq = bars([88., 98., 98., 99., 101., 102., 103.], dates=dates, basis='qfq')
    with pytest.raises(ValueError, match='unexplained'):
        prepare_standardized_append(
            old, qfq, raw, week_start='2026-09-07', as_of='2026-09-11',
            action_records=[] if record is None else [record])


def test_qfq_overlap_evidence_is_always_quarantined(tmp_path):
    source = tmp_path / '01579.json'
    source.write_text(json.dumps({
        'symbol': '01579.HK', 'classification': 'ACTION_AWARE_CHAIN_BRIDGE',
        'historical_overlap_differences': {'cells': 3},
    }))
    changes = _plan_json(
        source, tmp_path / 'formal.json', Path('cache/qfq_basis_evidence/01579.json'),
        tmp_path, date(2026, 9, 7), date(2026, 9, 11),
    )
    assert len(changes) == 1 and changes[0].kind == 'qfq_overlap_drift'
    assert changes[0].payload['authorization_required']
    assert changes[0].payload['historical_records_published'] == 0


def test_matched_basis_tail_and_idempotence_preserve_prior_values(monkeypatch, tmp_path):
    monkeypatch.setattr(collect.paths, 'STATE_DIR', tmp_path)
    h = tmp_path / 'history'; h.mkdir()
    bars([9.], dates=['2026-08-28']).to_parquet(h / '002594_SZ.parquet')
    bars([10.], dates=['2026-08-28']).to_parquet(h / '002594_SZ_raw.parquet')
    item = SimpleNamespace(ohlcv=bars([10., 12.]), standardized_ohlcv=bars([9., 11.], basis='qfq'))
    bundle = collect.DataBundle('test', '2026-09-04', 'test', {'002594.SZ': item}, {'passed': True})
    result = collect.sync_bundle_history(bundle)
    assert result['status'] == 'OK' and result['appended_rows'] == 2
    assert pd.read_parquet(h / '002594_SZ.parquet')['close'].tolist() == [9., 11.]
    assert pd.read_parquet(h / '002594_SZ_raw.parquet')['close'].tolist() == [10., 12.]
    hashes = {p: p.read_bytes() for p in h.iterdir()}
    assert collect.sync_bundle_history(bundle)['appended_rows'] == 0
    assert all(p.read_bytes() == data for p, data in hashes.items())


def test_rebase_never_writes_existing_standardized_file(monkeypatch, tmp_path):
    monkeypatch.setattr(collect.paths, 'STATE_DIR', tmp_path)
    h = tmp_path / 'history'; h.mkdir()
    f = h / '002594_SZ.parquet'; bars([9.], dates=['2026-08-28']).to_parquet(f)
    before = f.read_bytes()
    item = SimpleNamespace(ohlcv=bars([10., 12.]), standardized_ohlcv=bars([8., 11.], basis='qfq'))
    bundle = collect.DataBundle('test', '2026-09-04', 'test', {'002594.SZ': item}, {'passed': True})
    result = collect.sync_bundle_history(bundle)
    assert result['status'] == 'FAILED' and result['price_basis_blocked']
    assert f.read_bytes() == before


def test_tencent_raw_fallback_cannot_be_labeled_qfq():
    response = SimpleNamespace(raise_for_status=lambda: None,
        json=lambda: {'code': 0, 'data': {'sz002594': {'day': [['2026-09-04', '10','10','10','10','100']]}}})
    session = SimpleNamespace(get=lambda *a, **k: response)
    with pytest.raises(ValueError, match='no raw fallback'):
        tencent_kline.fetch_daily('sz002594', adjust='qfq', session=session, strict_adjustment=True)


@pytest.mark.parametrize(
    ('market','symbol','ak_symbol','expected_source'),
    [(Market.A,'002594.SZ','002594','akshare_tx_qfq'),
     (Market.HK,'00700.HK','00700','akshare_hk_qfq')],
)
def test_explicit_qfq_fallback_is_accepted_with_preserved_basis(
        monkeypatch, market, symbol, ak_symbol, expected_source):
    monkeypatch.setattr(tencent_kline, 'fetch_daily',
                        lambda *a, **k: (_ for _ in ()).throw(ValueError('no qfqday')))
    adjusted = bars([9.,11.], basis='qfq')
    calls = []
    def fallback(*args, **kwargs):
        calls.append(kwargs.get('adjust'))
        return adjusted
    if market == Market.A:
        monkeypatch.setattr(collect.akp, 'fetch_a_daily_tx', fallback)
    else:
        monkeypatch.setattr(collect.akp, 'fetch_hk_daily', fallback)
    ref = SimpleNamespace(market=market,symbol=symbol,ak_symbol=ak_symbol,
                          tencent_code=('sh' if market == Market.A else 'hk') + ak_symbol)
    frame, source = collect._collect_ohlcv(ref,320,'20260101','20260904',adjust='qfq')
    assert source == expected_source and calls == ['qfq']
    assert frame.attrs['price_basis'] == 'qfq'


def test_hk_sina_explicit_qfq_is_used_after_empty_primary_fallback(monkeypatch):
    monkeypatch.setattr(tencent_kline, 'fetch_daily',
                        lambda *a, **k: (_ for _ in ()).throw(ValueError('no qfqday')))
    monkeypatch.setattr(collect.akp, 'fetch_hk_daily',lambda *a, **k: None)
    adjusted = bars([9.,11.],basis='qfq')
    calls=[]
    def sina(*args,**kwargs):
        calls.append(kwargs.get('adjust'))
        return adjusted
    monkeypatch.setattr(collect.akp, 'fetch_hk_daily_sina',sina)
    ref = SimpleNamespace(market=Market.HK,symbol='00700.HK',ak_symbol='00700',
                          tencent_code='hk00700')
    frame,source=collect._collect_ohlcv(ref,320,'20260101','20260904',adjust='qfq')
    assert source == 'akshare_hk_sina_qfq' and calls == ['qfq']
    assert frame.attrs['price_basis'] == 'qfq'


def test_hk_sina_adapter_preserves_explicit_basis_and_bounds_dates(monkeypatch):
    calls=[]
    source=pd.DataFrame({
        'date':['2026-08-28','2026-09-04','2026-09-07'],
        'open':[8.,9.,10.],'high':[9.,10.,11.],'low':[7.,8.,9.],
        'close':[8.5,9.5,10.5],'volume':[100.,110.,120.],
    })
    fake=SimpleNamespace(stock_hk_daily=lambda **kwargs:(calls.append(kwargs) or source))
    monkeypatch.setattr(akshare_provider,'_ak',lambda:fake)
    frame=akshare_provider.fetch_hk_daily_sina(
        '00700','20260831','20260904',adjust='qfq')
    assert calls == [{'symbol':'00700','adjust':'qfq'}]
    assert list(frame.index.strftime('%Y-%m-%d')) == ['2026-09-04']
    assert frame.attrs == {'price_basis':'qfq','adjustment_request':'qfq'}


def test_ambiguous_adjusted_fallback_remains_rejected(monkeypatch):
    monkeypatch.setattr(tencent_kline, 'fetch_daily',
                        lambda *a, **k: (_ for _ in ()).throw(ValueError('no qfqday')))
    ambiguous = bars([9.,11.], basis='none')
    monkeypatch.setattr(collect.akp, 'fetch_hk_daily',lambda *a, **k: ambiguous)
    monkeypatch.setattr(collect.akp, 'fetch_hk_daily_sina',lambda *a, **k: ambiguous)
    ref = SimpleNamespace(market=Market.HK,symbol='00700.HK',ak_symbol='00700',
                          tencent_code='hk00700')
    frame, source = collect._collect_ohlcv(ref,320,'20260101','20260904',adjust='qfq')
    assert frame.empty and source == 'none'


def test_qfq_future_anchor_is_not_fixed_by_cropping(monkeypatch):
    monkeypatch.setattr(tencent_kline, 'fetch_daily', lambda *a, **k:
                        bars([10., 11.], dates=['2026-09-04','2026-09-07'], basis='qfq'))
    monkeypatch.setattr(collect.akp, 'fetch_a_daily_tx',lambda *a, **k: None)
    frame, source = collect._collect_ohlcv(SimpleNamespace(tencent_code='sz002594', ak_symbol='002594',
                                         symbol='002594.SZ',market=Market.A), 320,
                                         '20260101', '20260904', adjust='qfq')
    assert frame.empty and source == 'none'


def test_future_adjusted_snapshot_is_rejected_and_hashed(tmp_path):
    (tmp_path / 'ohlcv').mkdir(); (tmp_path / 'ohlcv_standardized').mkdir()
    (tmp_path / 'meta.json').write_text(json.dumps({'run_id': 'test', 'as_of': '2026-09-04'}))
    bars([10.], dates=['2026-09-04']).to_parquet(tmp_path / 'ohlcv/002594_SZ.parquet')
    target = tmp_path / 'ohlcv_standardized/002594_SZ.parquet'
    bars([10.], dates=['2026-09-07'], basis='qfq').to_parquet(target)
    result = collect.validate_snapshot_dir(tmp_path)
    assert not result['valid_for_replay']
    assert result['future_row_count'] == 1
    before = result['snapshot_content_hash']
    bars([11.], dates=['2026-09-07'], basis='qfq').to_parquet(target)
    assert collect.validate_snapshot_dir(tmp_path)['snapshot_content_hash'] != before


@pytest.mark.parametrize('kind', ['duplicate', 'missing_field', 'nan'])
def test_bad_records_are_not_price_basis_explanations(kind):
    f = bars([9., 10.])
    if kind == 'duplicate': f.index = [f.index[0]] * 2
    if kind == 'missing_field': f = f.drop(columns=['volume'])
    if kind == 'nan': f.iloc[0, 0] = float('nan')
    with pytest.raises(ValueError): differences(f, bars([10., 11.]))


def report(item):
    c = {'as_of_date':'2026-09-04','universe_size':60,'scoped_directories_clean':True,
         'record_integrity_complete':True,'freshness_complete':True,
         'datasets':{k:{'complete':True} for k in REQUIRED_DOMAINS},
         'history_consistency':{'latest_snapshot':'reference','items':{
             str(i):{v:{'status':'fresh'} for v in ('raw','standardized')} for i in range(60)}}}
    c['history_consistency']['items']['0']['standardized'] = {
        **item,'latest_date':'2026-09-04','expected_date':'2026-09-04'}
    return c


def test_legacy_warning_does_not_become_complete_or_mask_current_mismatch():
    item = comparison(bars([9., 11.]), bars([10., 11.]), None,
                      week_start='2026-08-31', as_of='2026-09-04')
    gate = coverage_gate(report(item), '2026-09-04')
    assert gate['passed'] and len(gate['historical_basis_unverified']) == 1
    assert not gate['historical_conflicts_waived_as_complete']
    assert not gate['all_domains_complete']
    item['current_week_raw_equivalent'] = False
    gate = coverage_gate(report(item), '2026-09-04')
    assert not gate['passed'] and gate['issues'][0]['blocking']


def test_basis_drift_is_safety_block_not_retry(tmp_path):
    sealed = tmp_path / 'sealed'; sealed.mkdir(); (sealed / 'x').write_text('code')
    freeze = tmp_path / 'freeze.json'; build_freeze_manifest([('code', sealed)], freeze)
    task = Task('market', ('history',), lambda: {'history_sync': {'price_basis_blocked': True}})
    result = run_job(state_root=tmp_path/'jobs', runtime=tmp_path/'runtime', database=tmp_path/'db',
        as_of='2026-09-04',tasks=[task],freeze_manifest=freeze,
        audit_source=lambda: pytest.fail('must block before audit'),
        publish=lambda _: pytest.fail('must not publish'), clock=lambda: datetime(2026,9,6,10,tzinfo=TZ))
    assert result['status'] == 'BLOCKED_ITEM_SAFETY'
    assert not result['complete']


@pytest.mark.parametrize('mode', ['legacy', 'matched', 'revised', 'future_anchor', 'wrong_basis'])
def test_native_audit_routes_actual_snapshot_views(monkeypatch, tmp_path, mode):
    h = tmp_path / 'history'; h.mkdir()
    sp = tmp_path / 'snapshots/2026-09-04/test'; (sp / 'ohlcv').mkdir(parents=True)
    bars([10., 11.]).to_parquet(h / '002594_SZ_raw.parquet')
    bars([9., 11.]).to_parquet(h / '002594_SZ.parquet')
    bars([10., 11.]).to_parquet(sp / 'ohlcv/002594_SZ.parquet')
    meta = {'symbols': {'002594.SZ': {'ohlcv_source': 'tencent_kline_raw'}}}
    if mode != 'legacy':
        (sp / 'ohlcv_standardized').mkdir()
        adj = bars([8. if mode == 'revised' else 9., 11.], basis='qfq')
        adj.to_parquet(sp / 'ohlcv_standardized/002594_SZ.parquet')
        meta['symbols']['002594.SZ'].update(standardized_basis='none' if mode == 'wrong_basis' else 'qfq',
            standardized_anchor_date='2026-09-07' if mode == 'future_anchor' else '2026-09-04')
    (sp / 'meta.json').write_text(json.dumps(meta))
    monkeypatch.setattr(extended.paths, 'STATE_DIR', tmp_path)
    monkeypatch.setattr(extended.paths, 'SNAPSHOTS_DIR', tmp_path / 'snapshots')
    monkeypatch.setattr(extended, '_expected_symbol_session', lambda *a: date(2026,9,4))
    result, errors = extended._audit_history_consistency(
        SimpleNamespace(stocks=[SimpleNamespace(symbol='002594.SZ')]), date(2026,9,4))
    std = result['items']['002594.SZ']['standardized']
    if mode == 'matched':
        assert result['complete'] and not errors and std['status'] == 'fresh'
    elif mode == 'legacy':
        assert not result['complete'] and std['status'] == 'basis_unverified'
    elif mode == 'revised':
        assert not result['complete'] and std['status'] == 'conflict'
    else:
        assert not result['complete'] and std['status'] == 'invalid'


def test_no_overlap_cannot_certify_history():
    with pytest.raises(ValueError, match='overlap'):
        comparison(bars([9.], dates=['2026-08-28']), bars([10.,11.]),
                   bars([11.], dates=['2026-09-04'], basis='qfq'),
                   week_start='2026-08-31', as_of='2026-09-04')


def test_weekly_bridge_receipt_created_during_same_run_is_used_at_both_gates(monkeypatch, tmp_path):
    from scripts import qwl_data_mcp as adapter
    from scripts import weekly_items

    stocks = [SimpleNamespace(symbol=f'{index:06d}.SZ', ak_symbol=f'{index:06d}',
                              market=SimpleNamespace(value='A')) for index in range(45)]
    stocks += [SimpleNamespace(symbol=f'{index:05d}.HK', ak_symbol=f'{index:05d}',
                               market=SimpleNamespace(value='HK')) for index in range(15)]
    database = tmp_path / 'formal'
    proof = {'receipt_path': 'sealed-market-receipt', 'receipt_sha256': 'a' * 64}
    ready = {'market': False, 'lookups': 0, 'validated': [], 'formal': []}
    monkeypatch.setattr(adapter, 'DEFAULT_DATABASE_ROOT', database)
    monkeypatch.setattr(adapter, 'QWL_HOME', tmp_path / 'runtime')
    monkeypatch.setattr(adapter, 'verify_freeze_manifest', lambda _: {'ok': True})
    monkeypatch.setattr(adapter, '_fixed_membership', lambda _: True)
    monkeypatch.setattr(adapter.config, 'load_extended_universe',
                        lambda: SimpleNamespace(stocks=stocks))
    monkeypatch.setattr(adapter, 'make_tasks', lambda *args: [])
    monkeypatch.setattr(weekly_items, 'expand_tasks', lambda *args: [])
    monkeypatch.setattr(adapter.extended, 'audit_data_coverage', lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, 'coverage_gate',
                        lambda *args: {'passed': True, 'issues': []})

    def current_receipt(*args):
        ready['lookups'] += 1
        return proof if ready['market'] else None

    def validate(*args, **kwargs):
        ready['validated'].append(args[2])
        return {'01579.HK': {'classification': 'CARRIED_QFQ_CHAIN_BRIDGE'}}, []

    def formal(*args, **kwargs):
        ready['formal'].append(kwargs['basis_acceptance'])
        return {'passed': True}

    def publish(*args, **kwargs):
        assert kwargs['acceptance'](database, [])['passed']
        return {'status': 'PUBLISHED'}

    def run(*args, **kwargs):
        assert ready['lookups'] == 0  # market receipt is not yet created
        ready['market'] = True
        _, gate = kwargs['audit_source']()
        assert gate['validated_qfq_basis_bridges']['01579.HK']['classification'] == 'CARRIED_QFQ_CHAIN_BRIDGE'
        return kwargs['publish']('2026-09-19T10:00:00+08:00')

    monkeypatch.setattr(adapter, '_market_basis_acceptance', current_receipt)
    monkeypatch.setattr(adapter, 'validate_qfq_basis_bridges', validate)
    monkeypatch.setattr(adapter, 'audit_formal', formal)
    monkeypatch.setattr(adapter, 'publish_weekly', publish)
    monkeypatch.setattr(adapter, 'run_job', run)
    from scripts import weekly_runner
    monkeypatch.setattr(weekly_runner, 'window_allowed', lambda _: True)
    assert adapter.qwl_weekly_update('2026-09-18', str(database), publish=True,
                                    authorization=adapter.PUBLISH_TOKEN)['status'] == 'PUBLISHED'
    assert ready['lookups'] == 2
    assert ready['validated'] == [proof]
    assert ready['formal'] == [proof]


def test_private_preview_keeps_real_database_as_readonly_bridge_provenance(monkeypatch, tmp_path):
    from scripts import weekly_dry_acceptance as dry

    original = tmp_path / 'formal'
    runtime = tmp_path / 'runtime'
    proof = {'receipt_sha256': 'a' * 64}
    seen = []
    monkeypatch.setattr(dry.shutil, 'copytree', lambda *args, **kwargs: None)
    monkeypatch.setattr(dry, 'prepare', lambda *args: tmp_path / 'derived')
    monkeypatch.setattr(dry.subprocess, 'run', lambda *args, **kwargs:
                        SimpleNamespace(returncode=0, stdout='{"passed":true}', stderr=''))

    def formal(*args, **kwargs):
        seen.append((args[1], kwargs['basis_provenance_root'], kwargs['basis_acceptance']))
        return {'passed': True, 'issues': []}

    def publish(*args, **kwargs):
        return {'status': 'PUBLISHED', 'formal_acceptance':
                kwargs['acceptance'](args[1], None)}

    monkeypatch.setattr(dry, 'audit_formal', formal)
    monkeypatch.setattr(dry, 'publish_weekly', publish)
    result = dry.assess(runtime, original, '2026-09-18', tmp_path / 'freeze',
                        {'01579'}, basis_acceptance=proof)
    assert result['passed'] and result['real_database_modified'] is False
    assert len(seen) == 1 and seen[0][0] != original
    assert seen[0][1:] == (original, proof)
