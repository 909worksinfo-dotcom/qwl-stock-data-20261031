"""Read-only price comparison and explicit adjustment-basis append guards.

A raw diagnostic difference is not evidence of a historical revision. Never
fit adjustment factors to the values being audited or infer a missing qfq feed
from a raw response. Legacy files keep their original bytes and metadata.
"""
from __future__ import annotations

import hashlib
import gzip
import io
import json
import math
import tarfile
from pathlib import Path

import numpy as np
import pandas as pd

FIELDS = ('open', 'high', 'low', 'close', 'volume')
PRICE_FIELDS = ('open', 'high', 'low', 'close')
ACTION_DATE_FIELDS = ('ex_date', '除净日', '除权除息日', '除权日')
AVAILABLE_DATE_FIELDS = ('available_on', 'source_published_on', '最新公告日期')


def frame(value):
    result = value.copy()
    result.index = pd.DatetimeIndex(pd.to_datetime(result.index)).normalize()
    if result.index.hasnans or result.index.has_duplicates:
        raise ValueError('invalid or duplicate daily business key')
    if not set(FIELDS) <= set(result.columns):
        raise ValueError('missing OHLCV fields')
    result = result.loc[:, list(FIELDS)].apply(pd.to_numeric, errors='raise').sort_index()
    if not np.isfinite(result.to_numpy(dtype=float)).all():
        raise ValueError('nonfinite OHLCV')
    return result


def differences(stored, reference):
    left, right = frame(stored), frame(reference)
    overlap = left.index.intersection(right.index)
    mask = ~np.isclose(left.loc[overlap], right.loc[overlap], rtol=1e-9, atol=1e-8)
    rows = []
    for i, day in enumerate(overlap):
        if not mask[i].any():
            continue
        fields = {name: {'stored': float(left.at[day, name]),
                         'reference': float(right.at[day, name])}
                  for j, name in enumerate(FIELDS) if mask[i, j]}
        rows.append({'date': day.date().isoformat(), 'fields': fields})
    return {'cells': int(mask.sum()), 'dates': [r['date'] for r in rows],
            'overlap_rows': len(overlap), 'rows': rows,
            'field_counts': dict(zip(FIELDS, map(int, mask.sum(axis=0))))}


def frame_sha256(value):
    """Hash normalized business values without depending on Parquet bytes."""
    checked = frame(value)
    payload = [
        {
            'date': day.date().isoformat(),
            **{name: float(checked.at[day, name]) for name in FIELDS},
        }
        for day in checked.index
    ]
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _record_sha256(value):
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str,
    ).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def _day(value):
    if value in (None, ''):
        return None
    text = str(value).strip()[:10].replace('/', '-')
    try:
        return pd.Timestamp(text).normalize()
    except (TypeError, ValueError):
        return None


def current_week_actions(records, *, anchor, as_of):
    """Return only explicit, timely company-action evidence for this transition."""
    accepted = []
    for record in records or []:
        if not isinstance(record, dict):
            continue
        ex_date = next((_day(record.get(name)) for name in ACTION_DATE_FIELDS
                        if _day(record.get(name)) is not None), None)
        available_on = next((_day(record.get(name)) for name in AVAILABLE_DATE_FIELDS
                             if _day(record.get(name)) is not None), None)
        if (ex_date is None or available_on is None
                or not anchor < ex_date <= as_of or available_on > ex_date):
            continue
        accepted.append({
            'ex_date': ex_date.date().isoformat(),
            'available_on': available_on.date().isoformat(),
            'record_sha256': _record_sha256(record),
            'record': record,
        })
    return accepted


def validated_prior_chain(database, prior_job, symbol, incoming, raw, *, week_start,
                          source):
    """Carry only a sealed, published previous-week chain with unchanged source values."""
    database, prior_job = Path(database), Path(prior_job)
    start = pd.Timestamp(week_start).normalize()
    previous_end = start - pd.Timedelta(days=3)
    previous_start = previous_end - pd.Timedelta(days=4)
    state = json.loads(prior_job.read_text(encoding='utf-8'))
    if (prior_job.parent.name != previous_end.date().isoformat()
            or state.get('as_of_date') != previous_end.date().isoformat()):
        raise ValueError('prior qfq bridge job is not the completed adjacent week')
    if state.get('status') != 'COMPLETE':
        from .approved_qfq_bridge import validated_approved_chain
        return validated_approved_chain(database, prior_job, symbol, incoming, raw,
                                        week_start=start, source=source)
    published = state.get('publication', {})
    run_id = published.get('run_id', '')
    run = database / 'state/weekly_runs' / run_id
    publication = json.loads((run / 'publication.json').read_text(encoding='utf-8'))
    if (not run_id.startswith(previous_start.date().isoformat() + '_'
                              + previous_end.date().isoformat() + '_')
            or published.get('status') != 'PUBLISHED'
            or publication.get('status') != 'PUBLISHED'
            or publication.get('run_id') != run_id
            or publication.get('formal_acceptance', {}).get('passed') is not True
            or any(publication.get(key) != 0 for key in (
                'formal_existing_files_rewritten', 'historical_revisions_published',
                'historical_backfills_published'))):
        raise ValueError('prior qfq bridge has no accepted append-only publication')
    receipt = Path(state['tasks']['market']['receipt'])
    if (not receipt.is_relative_to(prior_job.parent)
            or state['tasks']['market'].get('ok') is not True):
        raise ValueError('prior qfq bridge receipt is outside completed job')
    receipt_bytes = receipt.read_bytes() if receipt.is_file() else None
    snapshot = None
    prior = None
    if receipt_bytes is not None:
        document = json.loads(receipt_bytes)
        snapshot = Path(document['provider_snapshot'])
        if (not snapshot.is_relative_to(prior_job.parent)
                or document.get('ok') is not True or document.get('issues')
                or document.get('result', {}).get('gate', {}).get('passed') is not True):
            raise ValueError('prior qfq market receipt was not accepted')
        prior = document['result']['history_sync']['symbols'][symbol]['standardized']['basis_transition']
        if not snapshot.is_file():
            snapshot = None
    code, _market = symbol.split('.')
    entries = {}
    for line in (run / 'MANIFEST_SHA256_SIZE.txt').read_text().splitlines():
        columns = line.split('  ', 1)
        if len(columns) == 2:
            entries[columns[1].split('  ', 1)[-1].strip().split(' ', 1)[-1]] = columns[0]
    stem = symbol.replace('.', '_')
    version_rel = (f'state/published_versions/state/history/{stem}.parquet/'
                   f'{run_id}.parquet')
    evidence_rel = (f'state/weekly_runs/{run_id}/provider_full_responses/'
                    f'cache/qfq_basis_evidence/{code}.json.gz')
    version = database / version_rel
    evidence = database / evidence_rel
    def check_sha(path, relative):
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if entries.get(relative) != actual:
            raise ValueError('prior qfq published evidence SHA mismatch: ' + relative)
        return actual
    version_sha = check_sha(version, version_rel)
    evidence_sha = check_sha(evidence, evidence_rel)
    catalog = json.loads((run / 'incremental_catalog.json').read_text())
    if not any(row.get('relative') == version_rel and row.get('sha256') == version_sha
               for row in catalog.get('entries', [])):
        raise ValueError('prior qfq version is absent from published catalog')
    archived = json.loads(gzip.decompress(evidence.read_bytes()))
    archived_prior = {key: value for key, value in archived.items() if key not in {
        'symbol', 'source', 'source_basis', 'authorization_required',
        'formal_history_modified'}}
    if prior is None:
        prior = archived_prior
    old_source = archived.get('source')
    same_tencent_adapter = {old_source, source} <= {'tencent_kline_qfq', 'akshare_tx_qfq'}
    if (archived.get('symbol') != symbol or (old_source != source and not same_tencent_adapter)
            or archived.get('source_basis') != 'qfq'
            or archived.get('classification') != prior['classification']
            or archived_prior != prior):
        raise ValueError('prior qfq source or archived bridge evidence drift')
    bridge = prior.get('bridge') or {}
    additive = bridge.get('method') == 'cash_additive_oneoff'
    if additive:
        from .approved_qfq_bridge import validated_additive_origin
        origin = validated_additive_origin(bridge)
        accepted = publication['formal_acceptance'].get('validated_qfq_basis_bridges', {}).get(symbol, {})
        if (symbol != '603345.SH' or prior.get('classification') != 'CARRIED_ADDITIVE_QFQ_BRIDGE'
                or origin != bridge.get('additive_origin_verification')
                or accepted.get('offset') != bridge.get('offset')
                or accepted.get('historical_records_published') != 0):
            raise ValueError('prior additive approval/origin mismatch')
    if (prior.get('classification') not in {'ACTION_AWARE_CHAIN_BRIDGE',
                                           'CARRIED_QFQ_CHAIN_BRIDGE', 'CARRIED_ADDITIVE_QFQ_BRIDGE'}
            or prior.get('as_of') != previous_end.date().isoformat()
            or prior.get('week_start') != previous_start.date().isoformat()
            or prior.get('historical_records_published') != 0
            or bridge.get('method') not in {'qfq_chain_v1', 'cash_additive_oneoff'}
            or bridge.get('volume_scaled') is not False
            or (not additive and (not math.isfinite(float(bridge['scale']))
                or not 0.5 < float(bridge['scale']) < 2.0))):
        raise ValueError('prior qfq bridge scope or scale is invalid')
    actions = bridge.get('actions') or []
    if prior['classification'] == 'ACTION_AWARE_CHAIN_BRIDGE' and not actions:
        raise ValueError('prior qfq bridge has no company-action proof')
    for action in actions:
        if (_record_sha256(action['record']) != action['record_sha256']
                or _day(action['available_on']) > _day(action['ex_date'])):
            raise ValueError('prior qfq company-action evidence SHA/date mismatch')
    if snapshot is not None:
        with tarfile.open(snapshot, mode='r:gz') as archive:
            names = archive.getnames()
            def sealed_frame(kind):
                matches = [name for name in names if
                           f'/snapshots/{previous_end.date().isoformat()}/' in '/' + name
                           and name.endswith(f'/{kind}/{stem}.parquet')]
                if len(matches) != 1:
                    raise ValueError('prior qfq snapshot member is missing/ambiguous')
                return frame(pd.read_parquet(io.BytesIO(archive.extractfile(matches[0]).read())))
            old_qfq = sealed_frame('ohlcv_standardized')
            old_raw = sealed_frame('ohlcv')
    else:
        # A completed publication and its MANIFEST bind the bridge evidence.
        # Recover source values only from one snapshot whose normalized hashes
        # match that immutable evidence; never recreate a missing receipt.
        snapshot_root = prior_job.parents[3] / 'snapshots' / previous_end.date().isoformat()
        matches = []
        for candidate in sorted(snapshot_root.iterdir()) if snapshot_root.is_dir() else []:
            if not candidate.is_dir() or candidate.is_symlink():
                continue
            meta_path = candidate / 'meta.json'
            qfq_path = candidate / 'ohlcv_standardized' / f'{stem}.parquet'
            raw_path = candidate / 'ohlcv' / f'{stem}.parquet'
            if not all(path.is_file() and not path.is_symlink()
                       for path in (meta_path, qfq_path, raw_path)):
                continue
            meta = json.loads(meta_path.read_text(encoding='utf-8'))
            if (meta.get('as_of') != previous_end.date().isoformat()
                    or meta.get('symbols', {}).get(symbol, {}).get('standardized_source') != old_source):
                continue
            candidate_qfq = frame(pd.read_parquet(qfq_path))
            candidate_raw = frame(pd.read_parquet(raw_path))
            if (frame_sha256(candidate_qfq) == prior['incoming_qfq_sha256']
                    and frame_sha256(candidate_raw) == prior['raw_sha256']):
                matches.append((candidate_qfq, candidate_raw))
        if len(matches) != 1:
            raise ValueError('prior qfq source snapshot is missing/ambiguous')
        old_qfq, old_raw = matches[0]
    if (frame_sha256(old_qfq) != prior['incoming_qfq_sha256']
            or frame_sha256(old_raw) != prior['raw_sha256']):
        raise ValueError('prior qfq source snapshot content SHA mismatch')
    if old_source != source and (incoming.attrs.get('price_basis') != 'qfq'
            or incoming.attrs.get('response_key') != 'qfqday'):
        raise ValueError('prior qfq source equivalence lacks explicit response proof')
    old_tail = old_qfq.loc[(old_qfq.index >= previous_start)
                           & (old_qfq.index <= previous_end)]
    incoming_tail = frame(incoming).reindex(old_tail.index)
    raw_tail = frame(raw).reindex(old_tail.index)
    published_tail = frame(pd.read_parquet(version))
    from .calendar import sessions_between
    from ..schemas import Market
    expected_days = pd.DatetimeIndex(sessions_between(Market.HK if symbol.endswith('.HK') else Market.A,
        previous_start.date(), previous_end.date()))
    if (not old_tail.index.equals(expected_days) or old_tail.empty or old_tail.isna().any().any()
            or incoming_tail.isna().any().any() or raw_tail.isna().any().any()
            or differences(old_tail, incoming_tail)['cells']
            or differences(old_raw.loc[old_tail.index], raw_tail)['cells']
            or not published_tail.index.equals(old_tail.index)):
        raise ValueError('prior qfq source vintage or published sessions drift')
    expected = old_tail.copy()
    if additive:
        expected.loc[:, list(PRICE_FIELDS)] += float(bridge['offset'])
    else:
        expected.loc[:, list(PRICE_FIELDS)] *= float(bridge['scale'])
    if differences(published_tail, expected)['cells']:
        raise ValueError('prior qfq published tail does not match sealed bridge scale')
    source_proof = {}
    if receipt_bytes is not None:
        source_proof['prior_receipt_sha256'] = hashlib.sha256(receipt_bytes).hexdigest()
    if snapshot is not None:
        source_proof['prior_snapshot_sha256'] = hashlib.sha256(snapshot.read_bytes()).hexdigest()
    else:
        source_proof.update({
            'prior_source_proof': 'manifest_bound_basis_and_matching_snapshot',
            'prior_qfq_frame_sha256': prior['incoming_qfq_sha256'],
            'prior_raw_frame_sha256': prior['raw_sha256'],
        })
    transform = ({'method':'cash_additive_oneoff', 'offset':float(bridge['offset']),
        'original_proof_sha256':bridge['original_proof_sha256'], 'additive_origin_verification':origin,
        'symbol':symbol, 'prior_source_proof':'explicitly_approved_formal_publication'} if additive
        else {'method':'qfq_chain_v1', 'scale':float(bridge['scale'])})
    return {**transform,
            'volume_scaled': False, 'prior_as_of': previous_end.date().isoformat(),
            'prior_last_session':old_tail.index.max().date().isoformat(),
            'prior_source': source, 'prior_run_id': run_id,
            'prior_version_sha256': version_sha, 'prior_evidence_sha256': evidence_sha,
            'prior_anchor_date': prior['anchor_date'], 'actions': actions,
            **source_proof}


def prepare_standardized_append(existing, incoming, raw, *, week_start, as_of,
                                action_records=(), prior_chain=None):
    """Prepare a current-week qfq tail without changing frozen historical values.

    Historical-only drift is allowed only when the current-week qfq values equal
    independently collected raw OHLCV and the prior formal close is unchanged.
    A current-week qfq rebase requires timely company-action evidence and is
    chain-linked to the prior formal close.  Anything unexplained remains blocked.
    """
    if incoming is None or incoming.empty or incoming.attrs.get('price_basis') != 'qfq':
        raise ValueError('explicit qfq source required; raw cannot fill standardized history')
    if raw is None or raw.empty:
        raise ValueError('independent raw reference required for qfq transition')
    left, adjusted, unadjusted = frame(existing), frame(incoming), frame(raw)
    start, end = pd.Timestamp(week_start).normalize(), pd.Timestamp(as_of).normalize()
    if start > end or adjusted.index.max() > end or unadjusted.index.max() > end:
        raise ValueError('invalid or future qfq transition window')
    overlap = left.index.intersection(adjusted.index)
    if len(overlap) < min(60, len(left)):
        raise ValueError('insufficient overlap to validate adjustment transition')
    anchors = left.index[left.index < start]
    if anchors.empty:
        raise ValueError('missing prior formal qfq anchor')
    anchor = anchors.max()
    if anchor not in adjusted.index or anchor not in unadjusted.index:
        raise ValueError('qfq/raw source does not contain prior formal anchor')
    target_index = adjusted.index[(adjusted.index >= start) & (adjusted.index <= end)]
    raw_target_index = unadjusted.index[(unadjusted.index >= start) & (unadjusted.index <= end)]
    if (target_index.empty or target_index.max() != end
            or not target_index.equals(raw_target_index)):
        raise ValueError('qfq/raw current-week sessions are incomplete or inconsistent')

    drift = differences(left, adjusted)
    target_qfq = adjusted.loc[target_index]
    target_raw = unadjusted.loc[target_index]
    current = differences(target_raw, target_qfq)
    actions = current_week_actions(action_records, anchor=anchor, as_of=end)
    anchor_close_equal = math.isclose(
        float(left.at[anchor, 'close']), float(adjusted.at[anchor, 'close']),
        rel_tol=1e-9, abs_tol=1e-8,
    )
    if any(pd.Timestamp(value) >= start for value in drift['dates']):
        raise ValueError('qfq drift includes an existing current-week business key')

    classification = 'UNCHANGED_QFQ_OVERLAP'
    bridge = None
    prepared_tail = target_qfq.copy()
    if drift['cells']:
        if anchor_close_equal and current['cells'] == 0 and not actions:
            classification = 'SAFE_CURRENT_WEEK_APPEND'
        elif (prior_chain and prior_chain.get('method') == 'cash_additive_oneoff'
              and current['cells'] == 0 and not actions):
            from .approved_qfq_bridge import validated_additive_origin
            if (prior_chain.get('symbol') != '603345.SH'
                    or prior_chain.get('volume_scaled') is not False
                    or prior_chain.get('prior_source_proof') != 'explicitly_approved_formal_publication'
                    or prior_chain.get('prior_last_session') != anchor.date().isoformat()):
                raise ValueError('unapproved additive bridge scope')
            verified = validated_additive_origin(prior_chain)
            if verified != prior_chain.get('additive_origin_verification'):
                raise ValueError('additive origin verification drift')
            offset = float(prior_chain['offset'])
            for name in PRICE_FIELDS:
                if not math.isclose(float(left.at[anchor,name]),
                                    float(adjusted.at[anchor,name]) + offset,
                                    rel_tol=0,abs_tol=1e-8):
                    raise ValueError('prior additive bridge does not match formal OHLC anchor')
            if left.at[anchor,'volume'] != adjusted.at[anchor,'volume']:
                raise ValueError('prior additive bridge changes anchor volume')
            prepared_tail.loc[:,list(PRICE_FIELDS)] += offset
            classification = 'CARRIED_ADDITIVE_QFQ_BRIDGE'
            bridge = prior_chain
        elif prior_chain and current['cells'] == 0 and not actions:
            scale = float(prior_chain['scale'])
            if (prior_chain.get('method') != 'qfq_chain_v1'
                    or prior_chain.get('volume_scaled') is not False
                    or prior_chain.get('prior_last_session', prior_chain.get('prior_as_of'))
                       != anchor.date().isoformat()
                    or not math.isclose(float(left.at[anchor, 'close']),
                                        float(adjusted.at[anchor, 'close']) * scale,
                                        rel_tol=1e-9, abs_tol=1e-8)):
                raise ValueError('prior qfq bridge scale does not match formal anchor')
            prepared_tail.loc[:, list(PRICE_FIELDS)] *= scale
            classification = 'CARRIED_QFQ_CHAIN_BRIDGE'
            bridge = prior_chain
        elif actions and current['cells']:
            first_action = min(pd.Timestamp(item['ex_date']) for item in actions)
            mismatch_days = [pd.Timestamp(value) for value in current['dates']]
            if any(day >= first_action for day in mismatch_days):
                raise ValueError('qfq/raw mismatch persists on or after documented ex-date')
            scale = float(left.at[anchor, 'close']) / float(adjusted.at[anchor, 'close'])
            if not math.isfinite(scale) or not 0.5 < scale < 2.0:
                raise ValueError('unsafe qfq bridge scale')
            expected_ratio = 1.0 / scale
            for day in mismatch_days:
                for name in PRICE_FIELDS:
                    raw_value = float(unadjusted.at[day, name])
                    if raw_value == 0 or not math.isclose(
                            float(adjusted.at[day, name]) / raw_value,
                            expected_ratio, rel_tol=5e-4, abs_tol=5e-4):
                        raise ValueError('qfq bridge factor is not stable before ex-date')
            prepared_tail.loc[:, list(PRICE_FIELDS)] *= scale
            classification = 'ACTION_AWARE_CHAIN_BRIDGE'
            bridge = {
                'method': 'qfq_chain_v1',
                'anchor_date': anchor.date().isoformat(),
                'formal_anchor_close': float(left.at[anchor, 'close']),
                'incoming_qfq_anchor_close': float(adjusted.at[anchor, 'close']),
                'scale': scale,
                'volume_scaled': False,
                'actions': actions,
            }
        else:
            raise ValueError('unexplained qfq overlap drift; keep historical candidate isolated')

    # Include an exact formal anchor so downstream append logic sees no synthetic
    # revision. Only unseen current-week sessions can be written to the work view.
    prepared = pd.concat([left.loc[[anchor]], prepared_tail]).sort_index()
    prepared.attrs['price_basis'] = (
        'qfq_cash_additive_v1' if bridge and bridge.get('method') == 'cash_additive_oneoff'
        else 'qfq_chain_v1' if bridge else 'qfq'
    )
    audit = {
        'schema_version': 1,
        'classification': classification,
        'week_start': start.date().isoformat(),
        'as_of': end.date().isoformat(),
        'anchor_date': anchor.date().isoformat(),
        'incoming_qfq_sha256': frame_sha256(adjusted),
        'raw_sha256': frame_sha256(unadjusted),
        'formal_anchor_sha256': frame_sha256(left.loc[[anchor]]),
        'current_week_qfq_raw_differences': current,
        'historical_overlap_differences': drift,
        'bridge': bridge,
        'historical_records_published': 0,
    }
    return prepared, audit


def guarded_standardized_append(existing, incoming):
    """Qfq additions require a real qfq response and an unchanged overlap.

    A provider rebase/revision changes old qfq values. Appending only its new
    tail would splice adjustment anchors, so reject that entire append. The
    immutable provider snapshot remains available for separately authorized
    historical analysis; this function never generates a revised history.
    """
    if incoming is None or incoming.empty or incoming.attrs.get('price_basis') != 'qfq':
        raise ValueError('explicit qfq source required; raw cannot fill standardized history')
    checked = frame(incoming)
    if existing is None or existing.empty:
        return {'basis': 'qfq', 'overlap_rows': 0, 'new_series': True}
    diff = differences(existing, checked)
    if diff['overlap_rows'] < min(60, len(existing)):
        raise ValueError('insufficient overlap to validate unchanged adjustment anchor')
    if diff['cells']:
        raise ValueError('qfq overlap drift: no standardized append; historical authorization required')
    return {'basis': 'qfq', 'overlap_rows': diff['overlap_rows'],
            'anchor_validation': 'unchanged_all_observed_overlap',
            'unobserved_history_certified': False}


def comparison(stored, raw, adjusted, *, week_start, as_of):
    """Compare like references; retain legacy cross-basis differences as evidence."""
    if adjusted is not None:
        if adjusted.empty or adjusted.attrs.get('price_basis') != 'qfq':
            raise ValueError('standardized reference is not explicitly qfq')
        d = differences(stored, adjusted)
        if d['overlap_rows'] < min(60, len(stored)):
            raise ValueError('insufficient same-basis overlap for certification')
        return {'comparison_basis': 'qfq', 'basis_verified': True,
                'snapshot_mismatch_cells': d['cells'], 'snapshot_mismatch_dates': d['dates'],
                'status': 'conflict' if d['cells'] else 'fresh'}
    d = differences(stored, raw)
    left, reference = frame(stored), frame(raw)
    target = reference.loc[(reference.index >= pd.Timestamp(week_start))
                           & (reference.index <= pd.Timestamp(as_of))]
    current_matches = (not target.empty and target.index.isin(left.index).all()
                       and differences(left, target)['cells'] == 0)
    return {'comparison_basis': 'legacy_unknown_vs_raw_diagnostic_only',
            'basis_verified': False, 'classification': 'adjustment_basis_unverified',
            'snapshot_mismatch_cells': d['cells'], 'snapshot_mismatch_dates': d['dates'],
            'current_week_raw_equivalent': bool(current_matches),
            'status': 'basis_unverified' if d['cells'] else 'fresh'}
