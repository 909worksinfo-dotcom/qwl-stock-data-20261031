"""Install the local QWL stock-data plugin and global Codex skill."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path


def valid_qwl_home(path: Path) -> bool:
    return (
        (path / "src/qwl/datasources").is_dir()
        and (path / "config/universe.yaml").is_file()
        and (path / "config/comparables.yaml").is_file()
        and (path / ".venv/bin/python").is_file()
    )


def append_codex_config(
    config_path: Path,
    marketplace_root: Path,
) -> Path | None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    text = config_path.read_text(encoding="utf-8") if config_path.exists() else ""
    backup = None
    additions = []
    if "[marketplaces.qwl-stock-data-local]" not in text:
        additions.append(
            "\n[marketplaces.qwl-stock-data-local]\n"
            f'last_updated = "{datetime.now().astimezone().isoformat()}"\n'
            'source_type = "local"\n'
            f'source = "{marketplace_root}"\n'
        )
    if '[plugins."qwl-stock-data@qwl-stock-data-local"]' not in text:
        additions.append(
            '\n[plugins."qwl-stock-data@qwl-stock-data-local"]\n'
            "enabled = true\n"
        )
    if additions:
        if config_path.exists():
            stamp = datetime.now().strftime("%Y%m%d%H%M%S")
            backup = config_path.with_name(f"{config_path.name}.bak.qwl.{stamp}")
            shutil.copy2(config_path, backup)
        config_path.write_text(
            text.rstrip() + "\n" + "".join(additions),
            encoding="utf-8",
        )
    return backup


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--qwl-home")
    parser.add_argument(
        "--codex-home",
        default=str(Path.home() / ".codex"),
    )
    args = parser.parse_args()

    plugin_root = Path(__file__).resolve().parents[1]
    marketplace_root = Path(__file__).resolve().parents[3]
    runtime_path = plugin_root / "config/runtime.json"
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
    qwl_home = Path(
        args.qwl_home or runtime.get("qwl_home") or ""
    ).expanduser().resolve()
    if not valid_qwl_home(qwl_home):
        raise SystemExit(
            f"Invalid QWL home: {qwl_home}. Expected source, config, and .venv."
        )

    runtime["qwl_home"] = str(qwl_home)
    runtime_path.write_text(
        json.dumps(runtime, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    codex_home = Path(args.codex_home).expanduser().resolve()
    global_skill = Path.home() / ".agents/skills/qwl-stock-data"
    source_skill = plugin_root / "skills/qwl-stock-data"
    shutil.copytree(source_skill, global_skill, dirs_exist_ok=True)

    backup = append_codex_config(
        codex_home / "config.toml",
        marketplace_root,
    )
    print(json.dumps({
        "status": "OK",
        "qwl_home": str(qwl_home),
        "plugin_marketplace": str(marketplace_root),
        "global_skill": str(global_skill),
        "codex_config": str(codex_home / "config.toml"),
        "config_backup": str(backup) if backup else None,
        "next_step": "Restart Codex Desktop to reload the local plugin.",
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
