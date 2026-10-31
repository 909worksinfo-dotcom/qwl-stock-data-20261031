"""Offline, anchored industry benchmark calculation from an approved input view."""
from contextlib import contextmanager
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path

from .. import paths

METHOD = 'selected_peer_equal_weight_cny.pit_daily.anchor.v1'

class BenchmarkConflict(ValueError):
    def __init__(self, reason, **evidence):
        self.evidence = {'reason': reason, **evidence}
        super().__init__(json.dumps(self.evidence, ensure_ascii=False, default=str))

@contextmanager
def input_view(root):
    # Only used within disposable workers/offline tests, never the MCP host
    old = paths.CACHE_DIR, paths.STATE_DIR
    paths.CACHE_DIR, paths.STATE_DIR = root / '.cache', root / 'state'
    try:
        yield
    finally:
        paths.CACHE_DIR, paths.STATE_DIR = old

def dependency_hashes(root, symbols):
    relatives = {Path('.cache/fx/HKDCNY.json')}
    for symbol in symbols:
        code = symbol.split('.')[0]
        relatives.add(Path('state/history') / f"{symbol.replace('.', '_')}_raw.parquet")
        for domain in ('dividends_hk', 'hk_action_evidence') if symbol.endswith('.HK') else ('dividends', 'announcements'):
            relatives.add(Path('.cache') / domain / f'{code}.json')
    return {str(rel): hashlib.sha256((root / rel).read_bytes()).hexdigest()
            if (root / rel).is_file() else None for rel in sorted(relatives)}

def build_document(universe, symbols, baseline, as_of, root):
    """Old records are returned verbatim; all current values are independently checked."""
    from .extended_p2 import _industry_benchmark_records
    end = date.fromisoformat(as_of)
    start = end - timedelta(days=end.weekday())
    if not symbols or len(set(symbols)) != len(symbols) or baseline.get('symbols') != symbols:
        raise BenchmarkConflict('benchmark_membership_or_baseline_requires_authorization', symbols=symbols)
    if not set(symbols) <= {s.symbol for s in universe.stocks}:
        raise BenchmarkConflict('benchmark_outside_fixed_pool')
    old = baseline.get('records', [])
    by_day = {}
    for row in old:
        day = date.fromisoformat(row['date'])
        if day in by_day or not math.isfinite(float(row['equal_weight_level'])) or row['equal_weight_level'] <= 0:
            raise BenchmarkConflict('invalid_formal_benchmark', date=row['date'])
        by_day[day] = row
    anchors = [d for d in by_day if d < start]
    if not anchors:
        raise BenchmarkConflict('benchmark_anchor_requires_authorization')
    anchor_day = max(anchors)
    anchor = by_day[anchor_day]
    dependencies = dependency_hashes(root, symbols)
    for symbol in symbols:
        rel = f"state/history/{symbol.replace('.', '_')}_raw.parquet"
        if not dependencies[rel]:
            raise BenchmarkConflict('missing_benchmark_member_input', symbol=symbol)
    with input_view(root):
        daily = _industry_benchmark_records(universe, symbols, pit_by_event=True, return_daily=True)
    observed = {date.fromisoformat(r['date']) for r in daily}
    if anchor_day not in observed:
        raise BenchmarkConflict('benchmark_anchor_not_in_price_inputs', date=str(anchor_day))
    level = float(anchor['equal_weight_level'])
    added, comparisons = [], []
    for row in daily:
        day = date.fromisoformat(row['date'])
        if not anchor_day < day <= end:
            continue
        if day < start:
            raise BenchmarkConflict('benchmark_anchor_has_historical_gap', date=str(day))
        if not math.isfinite(row['return']) or row['return'] <= -1 or row['constituent_count'] <= 0 or row.get('invalid_members'):
            raise BenchmarkConflict('invalid_benchmark_daily_return', date=str(day), details=row)
        level *= 1 + row['return']
        previous = by_day.get(day)
        expected = {'date': str(day), 'equal_weight_level': level, 'constituent_count': row['constituent_count']}
        derivation = {'method': METHOD,
                'input_layer': 'base+published_versions+eligible_current_append',
                'input_sha256': dependencies, 'symbols': symbols,
                'weight_policy': 'equal_weight_over_nonmissing_active_member_returns',
                'weights': row['weights'],
                'anchor': {k: anchor[k] for k in ('date', 'equal_weight_level', 'constituent_count')},
                'quarantine_used': False}
        if previous:
            if 'derivation' in previous and previous['derivation'] != derivation:
                raise BenchmarkConflict('benchmark_provenance_mismatch', date=str(day),
                                        stored=previous['derivation'], expected=derivation)
            if (not math.isclose(float(previous['equal_weight_level']), level, rel_tol=1e-12, abs_tol=1e-9)
                    or previous['constituent_count'] != row['constituent_count']):
                comparisons.append({'date': str(day), 'formal': previous, 'recomputed': expected})
        else:
            added.append({**expected, 'derivation': derivation})
    if comparisons:
        raise BenchmarkConflict('formal_benchmark_dependency_mismatch', comparisons=comparisons,
                                anchor=anchor, input_sha256=dependencies)
    absent = [str(d) for d in by_day if start <= d <= end and d not in observed]
    if absent:
        raise BenchmarkConflict('formal_benchmark_date_not_in_inputs', dates=absent)
    return {**baseline, 'records': [*old, *added]}

def groups_from_config(root):
    import yaml
    definitions = yaml.safe_load((root / 'config/comparables.yaml').read_text())
    groups = {}
    for item in definitions.get('stocks', []):
        groups.setdefault(item.get('group', 'unknown'), []).append(item['symbol'])
    return groups

def write_controlled(universe, as_of, input_root, output_root):
    """Offline calculation only, called in a disposable worker, not a collector."""
    documents = {}
    for group, symbols in groups_from_config(input_root).items():
        baseline = json.loads((input_root / '.cache/benchmarks' / f'industry_{group}.json').read_text())
        documents[group] = build_document(universe, symbols, baseline, as_of, input_root)
    for group, document in documents.items():
        target = output_root / '.cache/benchmarks' / f'industry_{group}.json'
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(document, ensure_ascii=False, indent=2))
    return {'groups': len(documents), 'network_used': False}

def audit_benchmarks(universe, as_of, root):
    groups = groups_from_config(root)
    issues = []
    for group, symbols in groups.items():
        try:
            path = root / '.cache/benchmarks' / f'industry_{group}.json'
            baseline = json.loads(path.read_text())
            expected = build_document(universe, symbols, baseline, as_of, root)
            if len(expected['records']) != len(baseline['records']):
                raise BenchmarkConflict('missing_formal_benchmark_dates', dates=[r['date'] for r in expected['records'][len(baseline['records']):]])
        except (ValueError, KeyError, OSError) as exc:
            issues.append({'domain': 'benchmarks', 'group': group, 'blocking': True,
                           **getattr(exc, 'evidence', {'reason': 'invalid_benchmark', 'details': str(exc)})})
    return issues
