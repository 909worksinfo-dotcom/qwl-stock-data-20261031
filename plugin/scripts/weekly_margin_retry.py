"""Target only missing margin symbols using the unchanged native collector."""
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

def retry_margin(runtime: Path, symbols: set[str]):
    if not symbols:
        return {"status": "FAILED", "error": "no verified target symbols"}
    with tempfile.TemporaryDirectory(prefix="qwl-margin-retry-") as temp:
        workspace = Path(temp)
        shutil.copytree(runtime / "config", workspace / "config")
        for symbol in symbols:
            if not symbol.endswith((".SZ", ".SH")) or not symbol.split(".")[0].isdigit():
                raise ValueError("invalid margin target")
            source = runtime / ".cache/margin" / (symbol.split(".")[0] + ".json")
            target = workspace / ".cache/margin" / source.name
            target.parent.mkdir(parents=True, exist_ok=True)
            if source.is_file():
                shutil.copy2(source, target)
        env = {**os.environ, "QWL_HOME": str(workspace), "PYTHONPATH": str(runtime / "src"),
               "PYTHONDONTWRITEBYTECODE": "1"}
        code = """
import json,sys
from qwl import config,paths
from qwl.datasources import extended
u=config.load_extended_universe()
wanted=set(json.loads(sys.argv[1]))
assert wanted <= {s.symbol for s in u.stocks}
selected=u.model_copy(update={"stocks":[s for s in u.stocks if s.symbol in wanted]})
result=extended.collect_margin_history_em(selected, budget_s=600, force=False)
(paths.ROOT/'receipt.json').write_text(json.dumps(result,ensure_ascii=False))
"""
        process = subprocess.run([str(runtime / ".venv/bin/python"), "-B", "-c", code,
                                  json.dumps(sorted(symbols))], env=env, capture_output=True,
                                 text=True, timeout=900)
        if process.returncode:
            return {"status": "FAILED", "error": process.stderr[-1500:]}
        allowed = {s.split(".")[0] + ".json" for s in symbols}
        files = list((workspace / ".cache/margin").glob("*.json"))
        if {f.name for f in files} - allowed:
            raise ValueError("outside target margin symbols")
        for source in files:
            json.loads(source.read_text())  # parse before replacing working cache
            shutil.copy2(source, runtime / ".cache/margin" / source.name)
        return json.loads((workspace / "receipt.json").read_text())
