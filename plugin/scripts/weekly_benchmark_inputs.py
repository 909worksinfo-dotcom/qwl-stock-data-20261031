"""Construct a private approved input view; never read quarantine as market input."""
import json
import shutil
from pathlib import Path
from scripts.weekly_guard import (RECORD_KEY_CONTRACT, _plan_json, _parquet_plan,
                                  _source_files, week_window)
from scripts.weekly_acceptance import materialize

INPUT_DOMAINS = {'dividends', 'dividends_hk', 'announcements', 'hk_action_evidence', 'fx'}

def prepare(runtime, database, destination, as_of):
    start, end = week_window(as_of)
    staged = destination / 'eligible'
    versions = staged / 'state/published_versions'
    excluded = []
    for source, relative in _source_files(runtime, None):
        is_history = relative.parts[:2] == ('state', 'history') and relative.name.endswith('_raw.parquet')
        is_input = relative.parts[0] == 'cache' and relative.parts[1] in INPUT_DOMAINS
        if not (is_history or is_input):
            continue
        output = versions / relative
        if source.suffix == '.json':
            changes = _plan_json(source, database / relative, relative, database, start, end)
            for change in changes:
                if change.kind not in {'weekly_append', 'document_version'}:
                    excluded.append({'relative': str(relative), 'kind': change.kind})
                    continue
                output.mkdir(parents=True, exist_ok=True)
                (output / 'zz_controlled_candidate.json').write_text(json.dumps({
                    'schema_version': 3, 'record_key_contract': RECORD_KEY_CONTRACT,
                    'kind': change.kind, 'week_start': str(start), 'week_end': str(end),
                    **change.payload}, ensure_ascii=False, default=str))
        elif source.suffix == '.parquet':
            plan = _parquet_plan(source, database / relative, start, end,
                                 database / 'state/published_versions' / relative)
            if plan.get('revisions') or plan.get('quarantine'):
                excluded.append({'relative': str(relative), 'kind': 'non_additive_parquet'})
            if plan.get('append_positions'):
                output.mkdir(parents=True, exist_ok=True)
                plan['incoming'].iloc[plan['append_positions']].to_parquet(output / 'zz_controlled_candidate.parquet')
    view = destination / 'view'
    materialize(database, staged, view)
    shutil.copytree(runtime / 'config', view / 'config')
    (destination / 'excluded.json').write_text(json.dumps(excluded, ensure_ascii=False, indent=2))
    return view
