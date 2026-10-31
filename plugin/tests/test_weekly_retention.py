import json
import pytest
from scripts import weekly_retention as retention


def fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(retention, "no_live_collector", lambda: True)
    job_dir=tmp_path/"runtime/state/weekly_jobs/key/2026-09-25"
    attempts=job_dir/"attempts/1"
    attempts.mkdir(parents=True)
    db=tmp_path/"database"
    run=db/"state/weekly_runs/run1"
    archive=run/"provider_full_responses"
    archive.mkdir(parents=True)
    (db/"MANIFEST_SHA256_SIZE.txt").write_text("fixed")
    (run/"publication.json").write_text(json.dumps({
        "status":"PUBLISHED","formal_acceptance":{"passed":True},
        "formal_existing_files_rewritten":0,"historical_revisions_published":0,
        "historical_backfills_published":0}))
    return job_dir,attempts,db,run,archive


def test_retention_never_deletes_referenced_attempt_receipt(tmp_path,monkeypatch):
    job_dir,attempts,db,run,archive=fixture(tmp_path,monkeypatch)
    receipt=attempts/"receipt.json"
    snapshot=attempts/"market.tar.gz"
    snapshot.write_bytes(b"sealed source")
    (archive/"copy.tar.gz").write_bytes(snapshot.read_bytes())
    receipt.write_text(json.dumps({"provider_snapshot":str(snapshot)}))
    (archive/"copy.json").write_text(receipt.read_text())
    (job_dir/"job.json").write_text(json.dumps({"as_of_date":"2026-09-25","status":"COMPLETE",
        "baseline":{},"tasks":{"one":{"receipt":str(receipt)}}}))
    report=retention.audit(job_dir=job_dir,database=db,run_dir=run,
        independent_report={"status":"FORMAL_WEEK_ACCEPTED"},apply=True)
    assert receipt.is_file() and snapshot.is_file() and not report["deleted_files"]
    assert {item["path"] for item in report["retained_files"]} == {str(receipt),str(snapshot)}
    assert all(item["reason"]=="CHECKPOINT_REFERENCED" for item in report["retained_files"])


def test_retention_refuses_missing_success_receipt(tmp_path,monkeypatch):
    job_dir,attempts,db,run,archive=fixture(tmp_path,monkeypatch)
    missing=attempts/"market.json"
    (job_dir/"job.json").write_text(json.dumps({"as_of_date":"2026-09-25","status":"COMPLETE",
        "baseline":{},"tasks":{"market":{"ok":True,"receipt":str(missing)}}}))
    with pytest.raises(ValueError,match="referenced receipt is missing"):
        retention.audit(job_dir=job_dir,database=db,run_dir=run,
            independent_report={"status":"FORMAL_WEEK_ACCEPTED"},apply=True)


def test_only_unreferenced_exact_formal_duplicate_can_be_deleted(tmp_path,monkeypatch):
    job_dir,attempts,db,run,archive=fixture(tmp_path,monkeypatch)
    candidate=attempts/"orphan.bin"
    candidate.write_bytes(b"identical")
    (archive/"copy.bin").write_bytes(b"identical")
    (job_dir/"job.json").write_text(json.dumps({"as_of_date":"2026-09-25",
        "status":"COMPLETE","baseline":{},"tasks":{}}))
    dry=retention.audit(job_dir=job_dir,database=db,run_dir=run,
        independent_report={"status":"FORMAL_WEEK_ACCEPTED"})
    assert candidate.is_file() and len(dry["candidate_files"])==1
    report=retention.audit(job_dir=job_dir,database=db,run_dir=run,
        independent_report={"status":"FORMAL_WEEK_ACCEPTED"},apply=True)
    assert not candidate.exists() and report["freed_bytes"]==9
