"""Read only explicitly approved publications, never adopt their week exceptions."""
from __future__ import annotations

import hashlib
import io
import json
import math
import tarfile
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import pandas as pd

APPROVED_RELEASES = {
    '2026-10-02': {
        'relative': 'state/pit_gateway/releases/2026-10-02-holiday-all12-models-pit-v1/release.json',
        'sha256': '1df6a409fcdd1612ea4213d07577c88a62889aaab94cc2a2969a6d7763800bea',
        'market_key': 'weekly_20261002_authorized_external_proofs',
        'snapshot_relative': 'attempts/2/market.tar.gz',
        'publication_sha256': '9e0fde854d8c42b5e1d6e83464fcd305f9f6899309bd34f7c8a62dd15e8dd2cb',
        'catalog_sha256': '814662a5d2cb280264c3bb5570429bc3f4cb5acdf001f621ea5101cde13a8132',
    },
}
ADDITIVE_PROOF = Path('/Users/bytedance/Documents/ChatGPT/量化交易/weekly_evidence/qwl-20260925-anjing-additive-bridge/603345_SH.additive_bridge_evidence.json')
ADDITIVE_SHA = 'aa2825e018e443bf0062458790dcdc3ba8e7441787e7a4aa98d0df4760b460bd'
BRIDGE_SYMBOLS = {'002050.SZ', '01579.HK', '02020.HK', '300866.SZ', '603345.SH', '603529.SH'}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def checked(path, expected):
    if not expected or sha(path) != expected:
        raise ValueError('approved qfq evidence SHA mismatch: ' + str(path))
    return json.loads(Path(path).read_text())


def member_frame(archive, description):
    from .price_basis import frame
    name = description.get('member', description.get('path'))
    matches = [m for m in archive.getmembers() if m.name == name and m.isfile()]
    if len(matches) != 1:
        raise ValueError('approved qfq source member missing/ambiguous')
    data = archive.extractfile(matches[0]).read()
    if hashlib.sha256(data).hexdigest() != description['sha256']:
        raise ValueError('approved qfq source member SHA mismatch')
    original = pd.read_parquet(io.BytesIO(data))
    return frame(original), original.attrs


def validated_additive_origin(bridge):
    """Separate issuer arithmetic and OHLCV replay; never fit an offset."""
    from .price_basis import PRICE_FIELDS, differences, frame
    if (bridge.get('original_proof_sha256') != ADDITIVE_SHA
            or bridge.get('method') != 'cash_additive_oneoff'
            or bridge.get('volume_scaled') is not False):
        raise ValueError('Anjoy additive origin is not the approved proof')
    proof = checked(ADDITIVE_PROOF, ADDITIVE_SHA)
    issuer = proof['issuer_announcement']
    cash = (Decimal(issuer['participating_a_shares'])
            * Decimal(issuer['actual_cash_per_share_cny'])
            / Decimal(issuer['all_a_shares'])).quantize(Decimal('0.001'), rounding=ROUND_HALF_UP)
    if (proof['symbol'] != '603345.SH' or proof['ex_date'] != '2026-09-24'
            or issuer['published_on'] != '2026-09-16'
            or cash != Decimal('1.726')
            or Decimal(issuer['virtual_cash_per_share_cny']) != cash
            or Decimal(str(bridge['offset'])) != cash
            or proof['historical_existing_files_rewritten'] != 0
            or proof['historical_rows_in_candidate'] != 0):
        raise ValueError('Anjoy issuer/offset/history proof mismatch')
    action = proof['action_cache']['record']
    if (action['除权除息日'] != proof['ex_date']
            or action['available_on'] > proof['ex_date']
            or Decimal(str(action['现金分红-现金分红比例'])) / 10
               != Decimal(issuer['actual_cash_per_share_cny'])):
        raise ValueError('Anjoy action date/amount proof mismatch')
    source = proof['provider_archive']
    if sha(source['path']) != source['sha256']:
        raise ValueError('Anjoy original provider archive SHA mismatch')
    with tarfile.open(source['path'], 'r:gz') as archive:
        raw, _ = member_frame(archive, source['members']['raw'])
        qfq, attrs = member_frame(archive, source['members']['qfq'])
    if attrs.get('price_basis') != 'qfq':
        raise ValueError('Anjoy origin is not an explicit qfq response')
    days = pd.DatetimeIndex([r['date'] for r in proof['field_arithmetic']])
    if (days.has_duplicates or days.empty or days.min().date().isoformat() != proof['window'][0]
            or days.max().date().isoformat() != proof['window'][1]):
        raise ValueError('Anjoy original business keys mismatch')
    candidate = proof['candidate']
    if sha(candidate['path']) != candidate['sha256']:
        raise ValueError('Anjoy candidate SHA mismatch')
    actual = frame(pd.read_parquet(candidate['path']))
    expected = qfq.loc[days].copy()
    expected.loc[:, list(PRICE_FIELDS)] += float(cash)
    if not actual.index.equals(days) or differences(actual, expected)['cells']:
        raise ValueError('Anjoy original candidate arithmetic mismatch')
    for row in proof['field_arithmetic']:
        day = pd.Timestamp(row['date'])
        delta = float(cash) if day < pd.Timestamp(proof['ex_date']) else 0.0
        for field in PRICE_FIELDS:
            values = row['fields'][field]
            if any(not math.isclose(a, b, rel_tol=0, abs_tol=1e-8) for a, b in (
                (float(raw.at[day,field]), values['independent_raw']),
                (float(qfq.at[day,field]), values['supplier_qfq']),
                (float(raw.at[day,field])-float(qfq.at[day,field]), delta),
                (float(expected.at[day,field]), values['append_candidate']),
            )):
                raise ValueError('Anjoy per-field additive proof mismatch')
        if not (raw.at[day,'volume'] == qfq.at[day,'volume'] == actual.at[day,'volume']
                == row['volume']['append_candidate'] == row['volume']['independent_raw']
                == row['volume']['supplier_qfq']):
            raise ValueError('Anjoy additive proof changed volume')
    for name, expected_sha in proof['formal_prefix']['input_file_sha256'].items():
        if sha(name) != expected_sha:
            raise ValueError('Anjoy original formal prefix SHA mismatch')
    return {'original_proof_sha256': ADDITIVE_SHA, 'issuer_cash_formula_result': str(cash),
            'original_provider_archive_sha256': source['sha256'],
            'original_candidate_sha256': candidate['sha256'],
            'verified_price_cells': len(days)*4, 'volume_scaled': False}


def validated_approved_chain(database, prior_job, symbol, incoming, raw, *, week_start, source):
    """Only this pinned adjacent formal publication can replace a native receipt."""
    from .price_basis import PRICE_FIELDS, differences, frame, frame_sha256, _record_sha256
    database, prior_job = Path(database), Path(prior_job)
    start = pd.Timestamp(week_start).normalize()
    end = start - pd.Timedelta(days=3)
    spec = APPROVED_RELEASES.get(end.date().isoformat())
    if spec is None or symbol not in BRIDGE_SYMBOLS:
        raise ValueError('prior qfq bridge has no explicitly approved formal proof')
    release = checked(database / spec['relative'], spec['sha256'])
    run_id = release['weekly_run_id']
    if not run_id.startswith((end-pd.Timedelta(days=4)).date().isoformat()+'_'+end.date().isoformat()+'_'):
        raise ValueError('approved qfq publication is not the adjacent week')
    run = database / 'state/weekly_runs' / run_id
    if sha(run / 'MANIFEST_SHA256_SIZE.txt') != release['weekly_incremental_manifest_sha256']:
        raise ValueError('approved qfq formal MANIFEST SHA mismatch')
    publication = checked(run / 'publication.json', spec['publication_sha256'])
    pinned = release['known_exceptions'][spec['market_key']]['market']
    if (publication.get('status') != 'PUBLISHED' or publication.get('run_id') != run_id
            or publication.get('formal_acceptance',{}).get('passed') is not True
            or publication['formal_acceptance']['manual_exception_proofs']['market'] != pinned
            or any(publication.get(k) != 0 for k in ('formal_existing_files_rewritten',
                'historical_revisions_published','historical_backfills_published'))):
        raise ValueError('approved qfq publication is not accepted append-only data')
    recheck_path = Path(pinned['path'])
    recheck = checked(recheck_path, pinned['sha256'])
    if (recheck.get('status') != 'MARKET_CANDIDATE_RECHECK_PASS'
            or recheck.get('as_of_date') != end.date().isoformat()
            or len(recheck['symbols']) != 60
            or recheck.get('historical_prefix_rows_changed') != 0
            or recheck.get('formal_database_files_written') != 0):
        raise ValueError('approved qfq market proof scope/history mismatch')
    stage_path = recheck_path.with_name('MARKET_STAGE_PROOF.json')
    stage = checked(stage_path, recheck['stage_proof_sha256'])
    if (stage.get('status') != 'PRIVATE_MARKET_120_VIEWS_VERIFIED'
            or stage.get('as_of_date') != end.date().isoformat()
            or len(stage['symbols']) != 60
            or stage['provider_archive_sha256'] != recheck['source_archive_sha256']
            or any(stage.get(k) != 0 for k in ('formal_existing_files_rewritten',
                'historical_revisions_published','historical_backfills_published'))):
        raise ValueError('approved qfq stage proof scope/history mismatch')
    item, rechecked = stage['symbols'][symbol], recheck['symbols'][symbol]
    audit, bridge = item['audit'], item['audit']['bridge']
    if (audit.get('classification') != rechecked['classification']
            or audit.get('classification') not in {'ACTION_AWARE_CHAIN_BRIDGE',
                'CARRIED_QFQ_CHAIN_BRIDGE','AUTHORIZED_CARRIED_ADDITIVE_BRIDGE'}
            or audit.get('historical_records_published') != 0
            or bridge.get('volume_scaled') is not False
            or item['historical_prefix_rows_changed'] != 0):
        raise ValueError('approved qfq bridge method/history mismatch')
    snapshot = prior_job.parent / spec['snapshot_relative']
    if sha(snapshot) != recheck['source_archive_sha256']:
        raise ValueError('approved qfq source archive SHA mismatch')
    with tarfile.open(snapshot, 'r:gz') as archive:
        qfq_old, attrs = member_frame(archive, item['qfq_source'])
        raw_old, _ = member_frame(archive, item['raw_source'])
        metas = [m for m in archive.getmembers() if m.isfile() and m.name.endswith('/meta.json')
                 and m.name.startswith('state/snapshots/')]
        if len(metas) != 1:
            raise ValueError('approved qfq source metadata missing/ambiguous')
        meta = json.loads(archive.extractfile(metas[0]).read())
    if (attrs.get('price_basis') != 'qfq' or meta['as_of'] != end.date().isoformat()
            or meta['symbols'][symbol]['standardized_source'] != source
            or any(incoming.attrs.get(k) != attrs.get(k) for k in
                   ('price_basis','response_key','adjustment_request','provider','source'))):
        raise ValueError('approved qfq source/provider drift')
    days = pd.DatetimeIndex(item['expected_sessions'])
    previous_start = end - pd.Timedelta(days=4)
    if (days.has_duplicates or days.empty or not days.is_monotonic_increasing
            or days.min() < previous_start or days.max() > end
            or list(days.strftime('%Y-%m-%d')) != rechecked['expected_sessions']
            or not qfq_old.loc[(qfq_old.index>=previous_start)&(qfq_old.index<=end)].index.equals(days)
            or not raw_old.loc[(raw_old.index>=previous_start)&(raw_old.index<=end)].index.equals(days)
            or differences(qfq_old.loc[days], frame(incoming).reindex(days))['cells']
            or differences(raw_old.loc[days], frame(raw).reindex(days))['cells']):
        raise ValueError('approved qfq source vintage/sessions drift')
    entries = {}
    for line in (run / 'MANIFEST_SHA256_SIZE.txt').read_text().splitlines():
        digest, size, name = line.split('  ', 2)
        if name in entries:
            raise ValueError('approved qfq manifest duplicate member')
        entries[name] = (digest, int(size))
    catalog = checked(run / 'incremental_catalog.json', spec['catalog_sha256'])['entries']
    formal = {}
    for suffix in ('','_raw'):
        relative = f'state/published_versions/state/history/{symbol.replace(".","_")}{suffix}.parquet/{run_id}.parquet'
        path = database / relative
        digest = sha(path)
        rows = [r for r in catalog if r.get('relative') == relative]
        if entries.get(relative) != (digest,path.stat().st_size) or len(rows)!=1 or rows[0]['sha256']!=digest:
            raise ValueError('approved qfq formal version/catalog/MANIFEST SHA mismatch')
        tail = frame(pd.read_parquet(path))
        if (not tail.index.equals(days)
                or frame_sha256(tail) != rechecked['views'][suffix]['tail_business_sha256']
                or rechecked['views'][suffix]['rows'] != len(days)):
            raise ValueError('approved qfq published business values/sessions mismatch')
        formal[suffix] = (tail,digest)
    expected = qfq_old.loc[days].copy()
    actions = bridge.get('actions') or []
    result = {k:bridge[k] for k in ('method','volume_scaled')}
    if bridge['method'] == 'qfq_chain_v1':
        scale = float(bridge['scale'])
        if not math.isfinite(scale) or not 0.5 < scale < 2 or not actions:
            raise ValueError('approved qfq scale/company action invalid')
        for action in actions:
            if (_record_sha256(action['record']) != action['record_sha256']
                    or pd.Timestamp(action['available_on']) > pd.Timestamp(action['ex_date'])
                    or pd.Timestamp(action['ex_date']) > days.max()):
                raise ValueError('approved qfq company action SHA/date mismatch')
        expected.loc[:,list(PRICE_FIELDS)] *= scale
        result.update(scale=scale,actions=actions)
    elif symbol == '603345.SH' and audit['classification'] == 'AUTHORIZED_CARRIED_ADDITIVE_BRIDGE':
        result.update(offset=float(bridge['offset']),original_proof_sha256=bridge['original_proof_sha256'])
        result['additive_origin_verification'] = validated_additive_origin(bridge)
        expected.loc[:,list(PRICE_FIELDS)] += float(bridge['offset'])
    else:
        raise ValueError('unapproved qfq transform method')
    if differences(formal[''][0],expected)['cells'] or differences(formal['_raw'][0],raw_old.loc[days])['cells']:
        raise ValueError('approved qfq published transform/volume mismatch')
    # Return only price proof bindings, not known_exceptions, dates, waivers or model locks
    return {**result,'prior_as_of':end.date().isoformat(),'prior_last_session':days.max().date().isoformat(),
            'prior_source':source,'prior_run_id':run_id,'prior_version_sha256':formal[''][1],
            'prior_evidence_sha256':recheck['stage_proof_sha256'],
            'prior_snapshot_sha256':recheck['source_archive_sha256'],
            'approved_release_sha256':spec['sha256'],'approved_recheck_sha256':pinned['sha256'],
            'approved_publication_sha256':sha(run/'publication.json'),
            'prior_source_proof':'explicitly_approved_formal_publication',
            'prior_anchor_date':audit.get('anchor_date') or item['publication_binding']['prior_market_last_session'],
            'symbol':symbol}
