from datetime import datetime
from pathlib import Path
import json
import pandas as pd
import pytest

from scripts.weekly_guard import (
    build_freeze_manifest, publish_weekly, _parquet_plan, _record_class,
    effective_json, effective_parquet,
)
from scripts.weekly_runner import run_job, window_allowed, classify_task_failure, deferred_margin_task, TZ
from scripts.weekly_tasks import Task, task_issues
from scripts.weekly_acceptance import coverage_gate, REQUIRED_DOMAINS


def setup(tmp_path):
    runtime, db, sealed = (tmp_path / p for p in ("runtime", "database", "sealed"))
    sealed.mkdir()
    (sealed / "fixed").write_text("fixed")
    freeze = tmp_path / "freeze.json"
    build_freeze_manifest([("sealed", sealed)], freeze)
    return runtime, db, freeze


def frame(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"close": [v for _, v in rows]},
                 index=pd.to_datetime([d for d, _ in rows])).to_parquet(path)


def test_parquet_second_publication_is_idempotent(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    rel = Path("state/history/00700_HK.parquet")
    frame(db / rel, [("2026-08-28", 1)])
    frame(runtime / rel, [("2026-08-28", 1), ("2026-09-04", 2)])
    kw = dict(publish=True, authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze)
    a = publish_weekly(runtime, db, "2026-09-04", **kw)
    b = publish_weekly(runtime, db, "2026-09-04", **kw)
    assert a["status"] == b["status"] == "PUBLISHED"
    versions = list((db / "state/published_versions" / rel).glob("*.parquet"))
    assert len(versions) == 1
    assert len(effective_parquet(db / rel, versions[0].parent)) == 2


def test_parquet_overlay_revision_is_not_republished(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    rel = Path("state/history/00700_HK.parquet")
    frame(db / rel, [("2026-08-28", 1)])
    frame(db / "state/published_versions" / rel / "v1.parquet", [("2026-09-04", 2)])
    frame(runtime / rel, [("2026-08-28", 1), ("2026-09-04", 9)])
    p = _parquet_plan(runtime / rel, db / rel,
                      datetime(2026, 8, 31).date(), datetime(2026, 9, 4).date(),
                      db / "state/published_versions" / rel)
    assert not p["append_positions"]
    assert p["revisions"][0]["previous"]["close"] == 2


def test_duplicate_input_rows_deduplicated_conflicts_rejected(tmp_path):
    f = tmp_path / "a.parquet"
    frame(f, [("2026-09-04", 1), ("2026-09-04", 1)])
    p = _parquet_plan(f, tmp_path / "absent", datetime(2026,8,31).date(), datetime(2026,9,4).date())
    assert p["append_positions"] == [0]
    frame(f, [("2026-09-04", 1), ("2026-09-04", 2)])
    with pytest.raises(ValueError, match="conflicting source"):
        _parquet_plan(f, tmp_path / "absent", datetime(2026,8,31).date(), datetime(2026,9,4).date())


def test_daily_history_json_and_metadata_resolve_without_duplicate(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    f = runtime / ".cache/fund_flow_ths/000333.json"
    f.parent.mkdir(parents=True)
    f.write_text(json.dumps({"symbol": "000333.SZ", "daily_history": [{"date":"2026-09-04","value":1}]}))
    kw = dict(publish=True, authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze)
    publish_weekly(runtime, db, "2026-09-04", **kw)
    second = publish_weekly(runtime, db, "2026-09-04", **kw)
    assert second["counts"].get("weekly_append", 0) == 0
    assert len(effective_json(db, Path("cache/fund_flow_ths/000333.json"))["daily_history"]) == 1


def test_pre_and_post_acceptance_failure_leave_no_published_files(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    frame(runtime / "state/history/00700_HK.parquet", [("2026-09-04", 2)])
    calls = []
    def accept(root, staged):
        calls.append(staged)
        return {"passed": staged is not None}
    result = publish_weekly(runtime, db, "2026-09-04", publish=True,
                           authorization="PUBLISH_CURRENT_WEEK", freeze_manifest=freeze,
                           acceptance=accept)
    assert result["status"] == "FAILED"
    assert len(calls) == 2
    assert not [p for p in (db / "state/published_versions").rglob("*") if p.is_file()]


def test_current_week_requires_event_day_and_available_day(tmp_path):
    start, end = datetime(2026,8,31).date(), datetime(2026,9,4).date()
    assert _record_class({"event_date":"2026-08-28","available_on":"2026-09-01"},start,end) == "historical"
    assert _record_class({"event_date":"2026-09-07","available_on":"2026-09-01"},start,end) == "future"
    assert _record_class({"available_on":"2026-09-01"},start,end) == "undated"
    assert _record_class({"event_date":"2026-09-04","available_on":"2026-09-07"},start,end) == "future"


def test_weekend_window_and_cross_week():
    assert not window_allowed("2026-09-04", datetime(2026,9,4,23,59,tzinfo=TZ))
    assert window_allowed("2026-09-04", datetime(2026,9,5,0,0,tzinfo=TZ))
    assert window_allowed("2026-09-04", datetime(2026,9,5,10,tzinfo=TZ))
    assert window_allowed("2026-09-04", datetime(2026,9,5,9,59,tzinfo=TZ))
    assert window_allowed("2026-09-04", datetime(2026,9,6,23,59,tzinfo=TZ))
    assert not window_allowed("2026-09-04", datetime(2026,9,7,0,tzinfo=TZ))


def test_waiting_job_resumes_skips_success_and_completes(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    counts = {"price":0, "margin":0, "publish":0}
    instant = [datetime(2026,9,5,10,tzinfo=TZ)]
    def price():
        counts["price"] += 1
        return {"status":"OK"}
    def margin():
        counts["margin"] += 1
        return {"status":"OK" if counts["margin"] > 1 else "timeout"}
    def audit():
        issues = [] if counts["margin"] > 1 else [{"domain":"margin","symbol":"000333.SZ","date":"2026-09-04","reason":"waiting_for_source","blocking":False}]
        return {}, {"passed":not issues,"issues":issues}
    def publish(_):
        counts["publish"] += 1
        return {"status":"PUBLISHED","formal_acceptance":{"passed":True},
                "formal_existing_files_rewritten":0,"historical_revisions_published":0,"historical_backfills_published":0}
    kw = dict(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
              tasks=[Task("price",("history",),price),Task("margin",("margin",),margin)],
              freeze_manifest=freeze,audit_source=audit,publish=publish,clock=lambda:instant[0])
    first = run_job(**kw)
    assert first["status"] == "WAITING_SOURCE" and not first["complete"]
    run_job(**kw) # Before next_retry_at: no repeated collection
    assert counts["margin"] == 1
    instant[0] = datetime(2026,9,5,11,tzinfo=TZ)
    second = run_job(**kw)
    assert second["status"] == "COMPLETE" and second["complete"]
    assert counts == {"price":1,"margin":2,"publish":1}
    run_job(**kw)
    assert counts["publish"] == 1


def test_freeze_drift_and_cross_week_never_collect(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    calls = []
    kw = dict(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
              tasks=[Task("x",("x",),lambda:calls.append(1))],freeze_manifest=freeze,
              audit_source=lambda:({},{"passed":True,"issues":[]}),publish=lambda _: {},
              clock=lambda:datetime(2026,9,7,tzinfo=TZ))
    assert run_job(**kw)["status"] == "AUTHORIZATION_REQUIRED_CROSS_WEEK"
    (tmp_path/"sealed/fixed").write_text("changed")
    assert run_job(**kw)["status"] == "BLOCKED_PLUGIN_OR_SKILL_DRIFT"
    assert not calls and not db.exists()


def native_report():
    return {"as_of_date":"2026-09-04","universe_size":60,"scoped_directories_clean":True,
            "record_integrity_complete":True,"freshness_complete":True,"freshness":{},
            "datasets":{k:{"complete":True} for k in REQUIRED_DOMAINS},
            "history_consistency":{"latest_snapshot":"reference","items":{
                str(i):{v:{"status":"fresh"} for v in ("raw","standardized")} for i in range(60)}}}


def test_old_conflicts_explicitly_warn_current_conflicts_block():
    c = native_report()
    c["history_consistency"]["items"]["0"]["standardized"] = {
        "status":"conflict","snapshot_mismatch_dates":["2026-07-03"],
        "latest_date":"2026-09-04","expected_date":"2026-09-04"}
    r = coverage_gate(c,"2026-09-04")
    assert r["passed"] and len(r["historical_conflicts"]) == 1
    assert not r["all_domains_complete"]
    c["history_consistency"]["items"]["0"]["standardized"]["snapshot_mismatch_dates"] += ["2026-09-01"]
    assert not coverage_gate(c,"2026-09-04")["passed"]


def test_current_qfq_chain_conflict_requires_exact_validated_bridge():
    c = native_report()
    c["history_consistency"]["items"]["0"]["standardized"] = {
        "status":"conflict", "snapshot_mismatch_dates":["2026-09-01"],
        "latest_date":"2026-09-04", "expected_date":"2026-09-04"}
    assert not coverage_gate(c,"2026-09-04")["passed"]
    proof = {"0":{"candidate_week_rows":5,"historical_records_published":0}}
    accepted = coverage_gate(c,"2026-09-04",proof)
    assert accepted["passed"]
    warning = accepted["historical_conflicts"][0]
    assert warning["basis_bridge_validated"] is True
    assert warning["classification"] == "ACTION_AWARE_CHAIN_BRIDGE"


def test_missing_domain_and_stale_data_never_pass():
    c = native_report()
    del c["datasets"]["margin"]
    assert not coverage_gate(c,"2026-09-04")["passed"]
    c = native_report()
    c["freshness_complete"] = False
    assert not coverage_gate(c,"2026-09-04")["passed"]


def test_margin_disclosure_lag_is_reported_but_other_gates_remain_hard():
    c = native_report()
    c["datasets"]["margin"]["complete"] = False
    c["datasets"]["margin"]["missing"] = ["000333"]
    c["datasets"]["margin_market"]["complete"] = False
    c["datasets"]["margin_market"]["missing"] = ["SZ"]
    c["freshness_complete"] = False
    c["freshness_errors"] = ["margin/000333.json:stale:latest=2026-09-03:expected=2026-09-04"]
    c["freshness"] = {"margin":{"items":{"000333.SZ":{
        "status":"stale","latest_date":"2026-09-03","expected_date":"2026-09-04"}}}}
    result = coverage_gate(c,"2026-09-04")
    assert result["passed"] and len(result["margin_pending"]) == 3
    assert result["all_domains_complete"] is False
    c["datasets"]["margin"]["provider_errors"] = ["000333"]
    assert not coverage_gate(c,"2026-09-04")["passed"]
    c["datasets"]["margin"]["provider_errors"] = []
    c["datasets"]["margin_eligibility"]["complete"] = False
    assert not coverage_gate(c,"2026-09-04")["passed"]
    c["datasets"]["margin_eligibility"]["complete"] = True
    c["record_integrity_complete"] = False
    assert not coverage_gate(c,"2026-09-04")["passed"]


def test_daily_margin_retry_source_can_publish_with_pending_receipt(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    calls = {"margin":0,"publish":0}
    def margin():
        calls["margin"] += 1
        return {"status":"stale"}
    def publish(_):
        calls["publish"] += 1
        return {"status":"PUBLISHED","formal_acceptance":{"passed":True},
                "formal_existing_files_rewritten":0,"historical_revisions_published":0,
                "historical_backfills_published":0}
    kw = dict(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
              tasks=[Task("margin_history@000333.SZ",("margin",),margin)],
              freeze_manifest=freeze,
              audit_source=lambda:({},{"passed":True,"issues":[],
                                       "margin_pending":[{"domain":"margin","reason":"waiting_for_source"}]}),
              publish=publish,clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    result = run_job(**kw)
    assert result["status"] == "COMPLETE"
    assert len(result["margin_pending_tasks"]) == 1
    assert result["tasks"]["margin_history@000333.SZ"]["ok"] is False
    assert calls == {"margin":1,"publish":1}
    assert run_job(**kw)["status"] == "COMPLETE"
    assert calls == {"margin":1,"publish":1}


def test_margin_safety_failure_and_eligibility_are_not_deferred():
    item = {"ok":False,"failure_class":"RETRY_SOURCE",
            "issues":[{"path":"$.status","value":"conflict"}]}
    assert not deferred_margin_task(Task("margin_history@000333.SZ",("margin",),lambda:None),item)
    item["issues"] = [{"path":"$.status","value":"timeout"}]
    assert not deferred_margin_task(Task("margin_eligibility",("margin_eligibility",),lambda:None),item)


def test_scalar_failure_and_nested_failure_are_detected():
    assert task_issues({"000333.SZ":"timeout"})
    assert task_issues({"stage":{"request_error":"network"}})
    assert not task_issues({"000333.SZ":"no_reported_history","count":0})


def test_cninfo_access_and_contract_failures_do_not_enter_blind_retry():
    for code, expected in [('CNINFO_HTTP_DENIED','BLOCKED_SOURCE_ACCESS'),
                           ('CNINFO_NON_JSON','BLOCKED_SOURCE_CONTRACT'),
                           ('CNINFO_SCHEMA','BLOCKED_SOURCE_CONTRACT'),
                           ('CNINFO_PAGINATION_INCOMPLETE','RETRY_SOURCE')]:
        receipt={'ok':False,'issues':[{'reason':'failed'}],
                 'result':{'cninfo_source_evidence':[{'status':code}]}}
        assert classify_task_failure(receipt)==expected


def test_cninfo_denied_keeps_snapshot_and_blocks_before_publication(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    published=[]
    result=run_job(state_root=tmp_path/"jobs",runtime=runtime,database=db,
        as_of="2026-09-04",tasks=[Task("announcements",("announcements",),
            lambda:{"status":"FAILED","cninfo_source_evidence":[{
                "status":"CNINFO_HTTP_DENIED","http_status":403}]})],
        freeze_manifest=freeze,audit_source=lambda:({}, {"passed":True,"issues":[]}),
        publish=lambda _:published.append(1),
        clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    assert result["status"]=="BLOCKED_SOURCE_ACCESS" and not result["complete"]
    assert not published
    receipt=Path(result["tasks"]["announcements"]["receipt"])
    assert receipt.is_file() and receipt.with_suffix(".fallback.tar.gz").is_file()


def test_intraday_bars_use_timestamp_not_day_as_key(tmp_path):
    frame(tmp_path/"a.parquet", [("2026-09-04 10:00",1),("2026-09-04 11:00",2)])
    assert len(effective_parquet(tmp_path/"a.parquet")) == 2
    p = _parquet_plan(tmp_path/"a.parquet",tmp_path/"missing",
                      datetime(2026,8,31).date(),datetime(2026,9,4).date())
    assert p["append_positions"] == [0,1]


def test_legacy_document_daily_history_is_visible_without_replacing_old(tmp_path):
    rel = Path("cache/fund_flow_ths/000333.json")
    (tmp_path/rel).parent.mkdir(parents=True)
    (tmp_path/rel).write_text(json.dumps({"daily_history":[{"date":"2026-08-28","value":1}]}))
    version = tmp_path/"state/published_versions"/rel/"v1.json"
    version.parent.mkdir(parents=True)
    version.write_text(json.dumps({"kind":"document_version","week_start":"2026-08-31",
        "document":{"daily_history":[{"date":"2026-08-28","value":99},{"date":"2026-09-04","value":2}]}}))
    assert effective_json(tmp_path,rel)["daily_history"] == [
        {"date":"2026-08-28","value":1},{"date":"2026-09-04","value":2}]


def test_failed_source_gate_never_calls_publish(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    called = []
    r = run_job(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
        tasks=[Task("price",("history",),lambda:{"status":"OK"})],freeze_manifest=freeze,
        audit_source=lambda:({},{"passed":False,"issues":[{"domain":"history","blocking":True}]}),
        publish=lambda _:called.append(1),clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    assert r["status"] == "BLOCKED_ACCEPTANCE" and not called


def test_formal_only_recoverable_gap_returns_to_waiting(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    r = run_job(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
        tasks=[Task("margin",("margin",),lambda:{"status":"OK"})],freeze_manifest=freeze,
        audit_source=lambda:({},{"passed":True,"issues":[]}),
        publish=lambda _:{"status":"FAILED","committed_files_remaining":0,
            "prepublication_acceptance":{"passed":False,"issues":[
                {"domain":"margin","symbol":"000333.SZ","date":"2026-09-04","blocking":False}]}},
        clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    assert r["status"] == "WAITING_SOURCE"
    assert not r["tasks"]["margin"]["ok"] and not r["complete"]


def test_prepublication_acceptance_failure_has_no_formal_versions(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    frame(runtime/"state/history/00700_HK.parquet",[("2026-09-04",2)])
    r=publish_weekly(runtime,db,"2026-09-04",publish=True,authorization="PUBLISH_CURRENT_WEEK",
        freeze_manifest=freeze,acceptance=lambda *_:{"passed":False,"issues":[{"blocking":False}]})
    assert r["status"] == "FAILED" and r["committed_files_remaining"] == 0
    assert not r["prepublication_acceptance"]["passed"]
    assert not (db/"state/published_versions").exists()


def test_interrupted_publication_is_not_replayed(tmp_path):
    runtime, db, freeze = setup(tmp_path)
    (db/"state/weekly_runs/interrupted").mkdir(parents=True)
    r=publish_weekly(runtime,db,"2026-09-04",publish=True,authorization="PUBLISH_CURRENT_WEEK",
        freeze_manifest=freeze)
    assert r["status"] == "BLOCKED_INTERRUPTED_PUBLICATION"


def test_existing_job_lock_prevents_duplicate_collectors(tmp_path):
    import fcntl
    runtime, db, freeze = setup(tmp_path)
    jobs=tmp_path/"jobs"
    jobs.mkdir()
    calls=[]
    with (jobs/"runtime.lock").open("a+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        r=run_job(state_root=jobs,runtime=runtime,database=db,as_of="2026-09-04",
            tasks=[Task("x",("x",),lambda:calls.append(1))],freeze_manifest=freeze,
            audit_source=lambda:None,publish=lambda _:None,
            clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    assert r["status"] == "RUNNING" and not calls


def test_caught_history_drift_never_collects(tmp_path):
    from scripts.weekly_runner import save,job_dir
    runtime, db, freeze = setup(tmp_path)
    db.mkdir()
    (db/"old").write_text("changed")
    jobs=tmp_path/"jobs"
    save(job_dir(jobs,db,"2026-09-04")/"job.json",
         {"status":"WAITING_RETRY","baseline":{"old":"not-the-current-hash"}})
    calls=[]
    r=run_job(state_root=jobs,runtime=runtime,database=db,as_of="2026-09-04",
        tasks=[Task("x",("x",),lambda:calls.append(1))],freeze_manifest=freeze,
        audit_source=lambda:None,publish=lambda _:None,
        clock=lambda:datetime(2026,9,5,10,tzinfo=TZ))
    assert r["status"] == "BLOCKED_FORMAL_HISTORY_DRIFT" and not calls


def test_current_history_missing_is_retryable_not_waived():
    c=native_report()
    c["history_consistency"]["items"]["0"]["raw"]={"status":"stale","expected_date":"2026-09-04"}
    r=coverage_gate(c,"2026-09-04")
    assert not r["passed"] and all(not i["blocking"] for i in r["issues"])


def test_capture_provenance_is_not_a_business_revision(tmp_path):
    f=tmp_path/"bars.parquet"
    old=tmp_path/"old.parquet"
    df=pd.DataFrame({"timestamp":pd.to_datetime(["2026-09-04 15:00"]),
                     "close":[1.0],"captured_at":["2026-09-05"],"source_sha256":["old"]})
    df.to_parquet(old,index=False)
    df["captured_at"]="2026-09-06"
    df["source_sha256"]="new"
    df.to_parquet(f,index=False)
    r=_parquet_plan(f,old,datetime(2026,8,31).date(),datetime(2026,9,4).date())
    assert not r["append_positions"] and not r["revisions"]


def test_new_json_versions_cannot_repeat_business_keys(tmp_path):
    rel=Path("cache/margin/000333.json")
    base=tmp_path/rel
    base.parent.mkdir(parents=True)
    base.write_text(json.dumps({"records":[{"date":"2026-09-04","value":1}]}))
    v=tmp_path/"state/published_versions"/rel/"new.json"
    v.parent.mkdir(parents=True)
    v.write_text(json.dumps({"schema_version":2,"kind":"weekly_append",
                            "append_records":{"records":[{"date":"2026-09-04","value":1}]}}))
    with pytest.raises(ValueError,match="duplicate/conflicting"):
        effective_json(tmp_path,rel)


def test_six_attempts_enter_hourly_wait_without_false_success(tmp_path):
    runtime,db,freeze=setup(tmp_path)
    instant=[datetime(2026,9,5,10,tzinfo=TZ)]
    from datetime import timedelta
    for attempt in range(1,7):
        r=run_job(state_root=tmp_path/"jobs",runtime=runtime,database=db,as_of="2026-09-04",
            tasks=[Task("x",("x",),lambda:{"status":"timeout"})],freeze_manifest=freeze,
            audit_source=lambda:({},{"passed":True,"issues":[]}),publish=lambda _:None,
            clock=lambda:instant[0])
        assert r["status"] == "WAITING_RETRY" and not r["complete"]
        instant[0]=datetime.fromisoformat(r["next_retry_at"])
    assert r["attempt"] == 6
    assert instant[0] == datetime(2026,9,5,11,31,tzinfo=TZ)


def test_provider_status_repair_does_not_republish_old_monthly_rows(tmp_path):
    runtime,db,freeze=setup(tmp_path)
    rel=Path("cache/macro/commodity_price_index.json")
    (db/rel).parent.mkdir(parents=True)
    old={"records":[{"date":"2026-08-01","value":1}],"error":"timeout"}
    (db/rel).write_text(json.dumps(old))
    source=runtime/".cache/macro/commodity_price_index.json"
    source.parent.mkdir(parents=True)
    source.write_text(json.dumps({"records":old["records"]}))
    result=publish_weekly(runtime,db,"2026-09-04",publish=True,authorization="PUBLISH_CURRENT_WEEK",
                         freeze_manifest=freeze)
    assert result["status"] == "PUBLISHED"
    view=effective_json(db,rel)
    assert view["records"] == old["records"] and view["error"] is None
    assert json.loads((db/rel).read_text()) == old
