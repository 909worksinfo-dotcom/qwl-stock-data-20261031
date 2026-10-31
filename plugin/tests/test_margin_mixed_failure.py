import copy
import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from qwl.datasources.margin_contract import FULL_FIELDS, SSE_FIELDS
from scripts.weekly_guard import _plan_json
from scripts.weekly_items import prepare_margin_rows, validate_item
from scripts.weekly_tasks import Task, task_issues

AS_OF = '2026-10-09'

def blob(value):
    return json.dumps(value).encode()

def row(day, market='融资融券_深证'):
    return {'date':day, 'market':market, **{k:10.0 for k in FULL_FIELDS},
            'margin_net_buy':0.0, 'short_net_sell_volume':0.0, 'margin_short_balance':20.0}

def documents(symbol='002594.SZ'):
    task = Task('margin_history@'+symbol, ('margin',), lambda:None,
                parent='margin_history', symbol=symbol,
                output='cache/margin/'+symbol.split('.')[0]+'.json', expected_date=AS_OF)
    market = '融资融券_沪证' if symbol.endswith('.SH') else '融资融券_深证'
    old = {'symbol':symbol, 'source':'RPTA_WEB_RZRQ_GGMX',
           'data_shape':'per_stock_daily_history', 'availability':'available',
           'records':[row('2026-09-30', market)]}
    new = {**old, 'records':copy.deepcopy(old['records'])+[row('2026-10-08', market)],
           'history_conflicts':[{'date':'2026-09-30', 'previous':copy.deepcopy(old['records'][0]),
                                'candidate':{**old['records'][0], 'float_market_cap':99}}]}
    return task, old, new

def test_mixed_old_conflict_and_pending_disclosure_preserve_prefix():
    task, old, new = documents()
    source, prefix = blob(new), blob(old)
    result, proof = prepare_margin_rows(task, source, prefix, AS_OF)
    assert json.loads(result)['records'][0] == old['records'][0]
    assert proof['historical_conflict_count'] == 1
    assert proof['margin_pending'] == [{'domain':'margin', 'symbol':task.symbol,
        'date':AS_OF, 'reason':'waiting_for_source', 'blocking':False}]
    assert not task_issues({'status':'OK', **proof})
    assert source == blob(new) and prefix == blob(old)
    with pytest.raises(ValueError, match='still missing'):
        validate_item(task, result, AS_OF)
    validate_item(task, result, AS_OF, allow_margin_pending=True)

@pytest.mark.parametrize('mutation', ['current_conflict','undated_conflict','old_value',
    'old_removed','old_pit','duplicate','future','symbol','source','unknown_field',
    'pit_future','unit','null','nan','boolean','negative','arithmetic','missing_field','market'])
def test_unsafe_candidates_never_waived(mutation):
    task, old, new = documents()
    current = new['records'][-1]
    if mutation == 'current_conflict':
        new['history_conflicts'][0] = {'date':'2026-10-08', 'previous':current, 'candidate':current}
    if mutation == 'undated_conflict': new['history_conflicts'][0]['date'] = None
    if mutation == 'old_value': new['records'][0]['margin_balance'] = 11
    if mutation == 'old_removed': new['records'] = new['records'][1:]
    if mutation == 'old_pit': new['records'][0]['available_on'] = '2026-09-30'
    if mutation == 'duplicate': new['records'].append(copy.deepcopy(current))
    if mutation == 'future': current['date'] = '2026-10-12'
    if mutation == 'symbol': new['symbol'] = '000001.SZ'
    if mutation == 'source': new['source'] = 'unknown'
    if mutation == 'unknown_field': current['extra'] = 1
    if mutation == 'pit_future': current['available_on'] = '2026-10-12'
    if mutation == 'unit': current['unit'] = 'wan'
    if mutation == 'null': current['margin_repay'] = None
    if mutation == 'nan': current['margin_repay'] = float('nan')
    if mutation == 'boolean': current['margin_repay'] = True
    if mutation == 'negative': current['margin_repay'] = -1
    if mutation == 'arithmetic': current['margin_net_buy'] = 99
    if mutation == 'missing_field': current.pop('short_volume')
    if mutation == 'market': current['market'] = 'SH'
    with pytest.raises((ValueError, KeyError, TypeError)):
        prepare_margin_rows(task, blob(new), blob(old), AS_OF)

def test_sse_known_extension_only_enriches_current_working_rows():
    task, old, new = documents('603228.SH')
    old['records'] = [{k:v for k,v in r.items() if k in SSE_FIELDS | {'date'}}
                      for r in [old['records'][0], new['records'][-1]]]
    new['history_conflicts'] = []
    result, proof = prepare_margin_rows(task, blob(new), blob(old), AS_OF)
    records = json.loads(result)['records']
    assert records[0] == old['records'][0]
    assert FULL_FIELDS <= records[1].keys()
    assert proof['historical_sse_extensions_preserved'] == ['2026-09-30']
    assert proof['historical_records_published'] == 0
    bad = copy.deepcopy(new); bad['records'][0]['margin_balance'] = 11
    with pytest.raises(ValueError): prepare_margin_rows(task, blob(bad), blob(old), AS_OF)

@pytest.mark.parametrize('mutation',['pit','unit','extra','shared_null','source'])
def test_sse_unknown_extensions_cannot_be_restored_and_waived(mutation):
    task, old, new = documents('603228.SH')
    old['records'][0] = {k:v for k,v in old['records'][0].items() if k in SSE_FIELDS | {'date'}}
    new['history_conflicts'] = []
    if mutation == 'pit': new['records'][0]['available_on'] = '2026-09-30'
    if mutation == 'unit': new['records'][0]['unit'] = 'wan'
    if mutation == 'extra': new['records'][0]['extra'] = 1
    if mutation == 'shared_null': new['records'][0]['margin_balance'] = None
    if mutation == 'source': old['source'] = 'unknown'
    with pytest.raises(ValueError): prepare_margin_rows(task, blob(new), blob(old), AS_OF)

def test_other_daily_domains_cannot_use_margin_freshness_waiver():
    task, old, new = documents()
    task.parent = 'microstructure'
    with pytest.raises(ValueError): prepare_margin_rows(task, blob(new), blob(old), AS_OF)
    with pytest.raises(ValueError, match='still missing'):
        validate_item(task, blob(new), AS_OF, allow_margin_pending=True)

def test_append_plan_no_history_rewrites_or_duplicate_current_publication(tmp_path):
    task, old, new = documents()
    data, _ = prepare_margin_rows(task, blob(new), blob(old), AS_OF)
    relative = Path(task.output)
    base = tmp_path/'db'/relative
    base.parent.mkdir(parents=True); base.write_bytes(blob(old))
    before = hashlib.sha256(base.read_bytes()).hexdigest()
    source = tmp_path/'source.json'; source.write_bytes(data)
    changes = _plan_json(source, base, relative, tmp_path/'db', date(2026,10,5), date(2026,10,9))
    appends = [c for c in changes if c.kind == 'weekly_append']
    assert len(appends) == 1
    assert [r['date'] for r in appends[0].payload['append_records']['records']] == ['2026-10-08']
    version = tmp_path/'db/state/published_versions'/relative/'first.json'
    version.parent.mkdir(parents=True); version.write_bytes(blob(appends[0].payload))
    repeated = _plan_json(source, base, relative, tmp_path/'db', date(2026,10,5), date(2026,10,9))
    assert not any(c.kind == 'weekly_append' and c.payload.get('append_records',{}).get('records') for c in repeated)
    assert hashlib.sha256(base.read_bytes()).hexdigest() == before
