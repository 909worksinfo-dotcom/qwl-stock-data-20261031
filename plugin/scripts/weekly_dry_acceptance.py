"""Exercise the real publication gate on private formal-layer copies only."""
import shutil
import tempfile
import subprocess
import os
import json
from pathlib import Path
from scripts.weekly_guard import publish_weekly
from scripts.weekly_acceptance import audit_formal
from scripts.weekly_benchmark_inputs import prepare

def assess(runtime, database, as_of, freeze_manifest, allowed_stems,
           basis_acceptance=None):
    with tempfile.TemporaryDirectory(prefix='qwl-readonly-revalidation-') as temp:
        copy = Path(temp) / 'database'
        candidate = Path(temp) / 'candidate'
        for rel in ('cache', 'state/history', 'state/published_versions'):
            if (database / rel).exists():
                shutil.copytree(database / rel, copy / rel)
        # Recompute on private working copies before revalidating the old job;
        # otherwise a stale pre-repair benchmark would prevent its own retry
        for rel in ('.cache', 'state/history', 'config'):
            if (runtime / rel).exists():
                shutil.copytree(runtime / rel, candidate / rel)
        view = prepare(runtime, database, Path(temp) / 'derived_inputs', as_of)
        process = subprocess.run([str(runtime / '.venv/bin/python'), '-B',
            str(Path(__file__).with_name('weekly_derived_worker.py')), as_of, str(view)],
            env={**os.environ, 'QWL_HOME':str(candidate), 'PYTHONPATH':str(runtime / 'src'),
                 'PYTHONDONTWRITEBYTECODE':'1'}, capture_output=True, text=True, timeout=180)
        try:
            derived = json.loads(process.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            derived = {'passed': False, 'issues':[{'domain':'benchmarks','reason':'derived_worker_failed',
                        'blocking': True, 'details':process.stderr[-1000:]}]}
        if process.returncode:
            return {'passed':False, 'issues':[{'domain':'benchmarks', 'reason':'derived_worker_exit',
                    'blocking':True, 'details':process.stderr[-1000:]}]}
        if not derived['passed']:
            return derived
        result = publish_weekly(candidate, copy, as_of, publish=True, authorization='PUBLISH_CURRENT_WEEK',
            freeze_manifest=freeze_manifest, allowed_stems=allowed_stems,
            acceptance=lambda root, staged: audit_formal(
                runtime, root, staged, as_of,
                basis_acceptance=basis_acceptance,
                basis_provenance_root=database,
            ))
        audit = result.get('formal_acceptance') or result.get('prepublication_acceptance')
        if not audit:
            return {'passed': False, 'issues': [{'domain': 'publication', 'reason': result['status'],
                                                'blocking': True, 'details': result.get('error')}]}
        return {**audit, 'private_publication_status': result['status'], 'real_database_modified': False}
