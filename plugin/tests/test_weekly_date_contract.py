"""Exercise the weekly adapter against the native date parser, without I/O."""
import ast
from datetime import date, datetime
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from scripts.weekly_tasks import make_tasks


def native_date_parser():
    # Compile only the actual native date expression, never the collector body
    plugin = Path(__file__).resolve().parents[1]
    runtime = json.loads((plugin / 'config/runtime.json').read_text())
    source = Path(runtime['qwl_home']) / 'src/qwl/datasources/extended.py'
    tree = ast.parse(source.read_text())
    collector = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                     and n.name == 'collect_margin_eligibility')
    assignment = next(n for n in collector.body if isinstance(n, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == 'end' for t in n.targets))
    expression = compile(ast.Expression(assignment.value), str(source), 'eval')
    return lambda value: eval(expression, {'datetime': datetime, 'date': date,
                                         'as_of_date': value})


class InertCollectors:
    def __getattr__(self, name):
        def unexpected(*args, **kwargs):
            raise AssertionError('Unrelated collector invoked: ' + name)
        return unexpected


@pytest.mark.parametrize('as_of', ['2026-09-04', '2027-01-01', '2028-02-29'])
def test_weekly_eligibility_matches_actual_native_date_contract(monkeypatch, as_of):
    parse_native = native_date_parser()
    universe = object()
    calls = []
    collectors = InertCollectors()

    def eligibility(received_universe, *, as_of_date):
        assert received_universe is universe
        calls.append(as_of_date)
        return parse_native(as_of_date)

    collectors.collect_margin_eligibility = eligibility
    datasources = ModuleType('qwl.datasources')
    datasources.financial_versions = InertCollectors()
    datasources.fundamentals_cache = InertCollectors()
    monkeypatch.setitem(sys.modules, 'qwl.datasources', datasources)
    adapter = SimpleNamespace(extended=collectors, extended_p2=InertCollectors(),
        config=SimpleNamespace(load_extended_universe=lambda: universe),
        _refresh_market=lambda *a: pytest.fail('Market must not run'),
        _refresh_specialized=lambda *a: pytest.fail('Specialized must not run'))
    tasks = make_tasks(adapter, as_of)
    task = next(task for task in tasks if task.name == 'margin_eligibility')
    assert task.call() == date.fromisoformat(as_of)
    assert calls == [date.fromisoformat(as_of).strftime('%Y%m%d')]


def test_original_iso_argument_is_rejected_by_native_parser():
    with pytest.raises(ValueError, match='does not match format'):
        native_date_parser()('2026-09-04')


def test_announcement_collectors_receive_explicit_week_end(monkeypatch):
    universe=object()
    calls=[]
    collectors=InertCollectors()
    collectors.collect_historical_announcements=lambda *args,**kwargs: calls.append(kwargs)
    datasources=ModuleType('qwl.datasources')
    datasources.financial_versions=InertCollectors()
    datasources.fundamentals_cache=InertCollectors()
    monkeypatch.setitem(sys.modules,'qwl.datasources',datasources)
    adapter=SimpleNamespace(extended=collectors,extended_p2=InertCollectors(),
        config=SimpleNamespace(load_extended_universe=lambda:universe),
        _refresh_market=lambda *a:None,_refresh_specialized=lambda *a:None)
    task=next(t for t in make_tasks(adapter,'2026-09-25') if t.name=='announcements_history')
    task.call()
    assert calls==[{'start_date':'20260921','end_date':'20260925','budget_s':1200}]
