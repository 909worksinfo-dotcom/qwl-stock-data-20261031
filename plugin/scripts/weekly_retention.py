"""Conservative, post-acceptance QWL temporary-file retention audit."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tarfile
from collections import deque
from datetime import datetime, timezone
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def no_live_collector() -> bool:
    output = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True,
                            text=True, check=True).stdout
    return not any(any(term in line for term in (
        "weekly_cli.py update", "weekly_worker.py", "qwl_data_mcp.py"))
        for line in output.splitlines() if str(os.getpid()) not in line)


def referenced_paths(job: dict, job_dir: Path) -> set[Path]:
    result, visited, virtual = set(), set(), set()
    receipts = deque()
    def visit(value, context=None):
        if isinstance(value, dict):
            if isinstance(value.get('provider_snapshot'), str):
                context = Path(value['provider_snapshot']).resolve()
            for key, item in value.items():
                if key in {"receipt", "archive", "provider_snapshot", "evidence_path", "source_archive", "snapshot"} and isinstance(item, str):
                    if key == 'receipt' and not Path(item).is_absolute():
                        member = Path(item)
                        expected = value.get('receipt_sha256')
                        if (context is None or not context.is_relative_to(job_dir) or not context.is_file()
                                or context.is_symlink() or '..' in member.parts
                                or not item.startswith('state/snapshots/') or not expected):
                            raise ValueError('unbound archive receipt reference')
                        identity = (str(context), item, expected)
                        result.add(context)
                        if identity not in virtual:
                            with tarfile.open(context, 'r:gz') as archive:
                                matches = [m for m in archive.getmembers() if m.name == item]
                                if len(matches) != 1 or not matches[0].isreg() or matches[0].size > 16 * 1024 * 1024:
                                    raise ValueError('missing/ambiguous archive receipt')
                                data = archive.extractfile(matches[0]).read()
                            if hashlib.sha256(data).hexdigest() != expected:
                                raise ValueError('archive receipt SHA mismatch')
                            virtual.add(identity)
                            visit(json.loads(data), context)
                        continue
                    path = Path(item).resolve()
                    result.add(path)
                    if key == "receipt":
                        receipts.append(path)
                visit(item, context)
        elif isinstance(value, list):
            for item in value:
                visit(item, context)
    visit(job)
    while receipts:
        receipt = receipts.popleft()
        if receipt in visited:
            continue
        if not receipt.is_relative_to(job_dir) or not receipt.is_file() or receipt.is_symlink():
            raise ValueError("referenced receipt is missing or outside job")
        visited.add(receipt)
        visit(json.loads(receipt.read_text()))
    return result


def audit(*, job_dir: Path, database: Path, run_dir: Path,
          independent_report: dict, apply: bool = False) -> dict:
    from scripts.weekly_guard import verify_existing
    job_dir = job_dir.resolve()
    database = database.resolve()
    run_dir = run_dir.resolve()
    if job_dir.name != json.loads((job_dir / "job.json").read_text()).get("as_of_date"):
        raise ValueError("job directory/as_of mismatch")
    if not job_dir.is_relative_to(job_dir.parents[2] / "weekly_jobs"):
        raise ValueError("job outside weekly_jobs")
    if database == job_dir or run_dir == job_dir or not run_dir.is_relative_to(database / "state/weekly_runs"):
        raise ValueError("unsafe retention scope")
    if not no_live_collector():
        raise RuntimeError("collector still running")
    job = json.loads((job_dir / "job.json").read_text())
    publication_file = run_dir / "publication.json"
    publication = json.loads(publication_file.read_text())
    if (job.get("status") != "COMPLETE" or publication.get("status") != "PUBLISHED"
        or publication.get("formal_acceptance", {}).get("passed") is not True
        or independent_report.get("status") != "FORMAL_WEEK_ACCEPTED"
        or any(publication.get(key) != 0 for key in (
            "formal_existing_files_rewritten", "historical_revisions_published",
            "historical_backfills_published"))):
        raise ValueError("weekly formal/PIT acceptance is not complete")
    if verify_existing(database, job.get("baseline", {})):
        raise ValueError("historical baseline changed")
    attempts = job_dir / "attempts"
    if attempts.is_symlink():
        raise ValueError("attempts symlink forbidden")
    formal_archive = run_dir / "provider_full_responses"
    hashes = {}
    for path in formal_archive.rglob("*"):
        if path.is_file() and not path.is_symlink():
            hashes.setdefault(digest(path), path)
    references = referenced_paths(job, job_dir)
    for reference in references:
        if reference.is_relative_to(attempts) and not reference.is_file():
            raise ValueError("referenced attempt evidence is missing")
    candidates = []
    retained = []
    for path in attempts.rglob("*") if attempts.exists() else []:
        if not path.is_file():
            continue
        if path.is_symlink() or not path.resolve().is_relative_to(attempts):
            raise ValueError("attempt path escapes target")
        item = {"path": str(path), "bytes": path.stat().st_size, "sha256": digest(path)}
        if path.resolve() in references:
            retained.append({**item, "reason": "CHECKPOINT_REFERENCED"})
        elif item["sha256"] in hashes:
            candidates.append({**item, "recoverable_from": str(hashes[item["sha256"]])})
        else:
            retained.append({**item, "reason": "RETAINED_FOR_AUDIT"})
    before = {"manifest": digest(database / "MANIFEST_SHA256_SIZE.txt"),
              "publication": digest(publication_file)}
    deleted = []
    if apply:
        for item in candidates:
            path = Path(item["path"])
            replica = Path(item["recoverable_from"])
            if (path.is_symlink() or not path.is_file() or digest(path) != item["sha256"]
                or not replica.is_file() or digest(replica) != item["sha256"]):
                raise ValueError("retention candidate changed before deletion")
            path.unlink()
            deleted.append(item)
    after = {"manifest": digest(database / "MANIFEST_SHA256_SIZE.txt"),
             "publication": digest(publication_file)}
    if before != after or verify_existing(database, job.get("baseline", {})):
        raise RuntimeError("formal database drift after retention")
    return {"status": "CLEANUP_AUDITED", "at": datetime.now(timezone.utc).isoformat(),
            "apply": apply, "candidate_files": candidates, "retained_files": retained,
            "deleted_files": deleted, "freed_bytes": sum(item["bytes"] for item in deleted),
            "formal_hashes_before": before, "formal_hashes_after": after}


def main() -> int:
    parser = argparse.ArgumentParser()
    for name in ("job-dir", "database", "run-dir", "independent-report"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    report = audit(job_dir=Path(args.job_dir), database=Path(args.database),
        run_dir=Path(args.run_dir),
        independent_report=json.loads(Path(args.independent_report).read_text()),
        apply=args.apply)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
