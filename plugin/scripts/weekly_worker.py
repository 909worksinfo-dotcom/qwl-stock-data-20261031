"""Disposable process entry point. No access path points at the formal database."""
from __future__ import annotations
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

def network_timeouts():
    # These defaults exist only in this disposable worker, never global settings
    import requests
    original = requests.sessions.Session.request
    def bounded(self, method, url, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = (10, 30)
        elif isinstance(timeout, (int,float)):
            kwargs["timeout"] = (min(timeout,10),min(timeout,45))
        return original(self,method,url,**kwargs)
    requests.sessions.Session.request = bounded

def watch_parent(parent):
    while True:
        if os.getppid() != parent:
            os.killpg(os.getpgrp(),signal.SIGKILL)
        time.sleep(.5)

def specialized(spec):
    name = spec["name"]
    if name == "byd_intraday":
        from qwl.datasources.byd_historical_intraday import collect_byd_historical_intraday
        return collect_byd_historical_intraday(force=True)
    if name == "anker_intraday":
        from qwl.datasources.anker_historical_intraday import collect_anker_historical_intraday
        return collect_anker_historical_intraday(force=True)
    if name == "byd_operating":
        from qwl.datasources.byd_operating import collect_byd_operating_history
        return collect_byd_operating_history(force=True)
    if name == "ev_operating":
        from qwl.datasources.ev_operating import collect_ev_operating_history
        return collect_ev_operating_history(force=True)
    from qwl.datasources import byd_research_text as research
    import pandas as pd
    if name == "research_catalog":
        sources = research._source_records()
        frame = pd.read_parquet(research._CACHE) if research._CACHE.exists() else pd.DataFrame()
        good = set(frame.loc[frame.parse_status == "parsed","report_url"]) if not frame.empty else set()
        missing = sorted({s["report_url"] for s in sources} - good)
        return {"status":"PARTIAL" if missing else "OK", "missing_documents":missing,
                "expected":len(sources),"parsed":len(good)}
    # Filter input identities in the isolated workspace, then use the original
    # PDF downloader/parser. A prior failed row must not trigger cache-hit
    source = research.paths.CACHE_DIR / "consensus/002594.json"
    blob = json.loads(source.read_text())
    blob["normalized_estimates"] = [r for r in blob.get("normalized_estimates",[])
                                   if r.get("report_url") == spec["document"]]
    if not blob["normalized_estimates"]:
        raise ValueError("document no longer belongs to canonical BYD source")
    source.write_text(json.dumps(blob,ensure_ascii=False))
    if research._CACHE.exists():
        frame = pd.read_parquet(research._CACHE)
        frame[frame.report_url == spec["document"]].to_parquet(research._CACHE,index=False)
    return research.collect_byd_research_text(force=True,budget_seconds=90)

def main():
    spec_path = Path(sys.argv[1])
    spec = json.loads(spec_path.read_text())
    threading.Thread(target=watch_parent,args=(spec["parent_pid"],),daemon=True).start()
    network_timeouts()
    os.environ.pop('QWL_WEEKLY_SERIES_CACHE', None)
    if spec['name'] == 'market':
        os.environ['QWL_WEEKLY_SERIES_CACHE'] = str(Path(os.environ['QWL_HOME']) / 'state/snapshots' / spec['as_of'] / 'series')
    from scripts import qwl_data_mcp as adapter
    from scripts.weekly_cninfo_compat import install
    cninfo_evidence=install()
    if spec["symbol"] and not spec["document"]:
        universe = adapter.config.load_extended_universe()
        if not adapter._fixed_membership(universe):
            raise ValueError("canonical universe drift")
        stocks = [s for s in universe.stocks if s.symbol == spec["symbol"]]
        if len(stocks) != 1:
            raise ValueError("unknown stock")
        from types import SimpleNamespace
        adapter.config = SimpleNamespace(load_extended_universe=lambda:universe.model_copy(update={"stocks":stocks}))
    try:
        if spec["parent"] == "specialized":
            result = specialized(spec)
        else:
            from scripts.weekly_tasks import make_tasks
            task = next(t for t in make_tasks(adapter,spec["as_of"],False)
                        if t.name == (spec["parent"] or spec["name"]))
            result = task.call()
        if cninfo_evidence and isinstance(result,dict):
            for event in cninfo_evidence:
                if event.get('status')!='verified_empty_source_window':
                    continue
                # Native results use exchange-suffixed symbols; the provider
                # takes the corresponding bare code
                for key in list(result):
                    if key.split('.')[0]==event['symbol'] and result[key]=='cached':
                        result[key]='verified_empty_source_window'
            result['cninfo_source_evidence']=cninfo_evidence
    except Exception as exc:
        result = {"status":"FAILED","error":f"{type(exc).__name__}: {str(exc)[:1500]}"}
        if cninfo_evidence:
            result['cninfo_source_evidence']=cninfo_evidence
    # Parent terminates our entire process group after this fsynced receipt
    # Native timeout threads can only write here until that termination
    result_path = spec_path.parent / "result.json"
    with result_path.with_suffix(".tmp").open("x") as handle:
        json.dump(result,handle,ensure_ascii=False,default=str)
        handle.flush(); os.fsync(handle.fileno())
    os.replace(result_path.with_suffix(".tmp"),result_path)
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
