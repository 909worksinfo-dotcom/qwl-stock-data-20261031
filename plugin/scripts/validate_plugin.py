"""Static and runtime validation for the QWL Codex plugin package."""

from __future__ import annotations

import ast
import importlib.util
import json
import os
import py_compile
import re
import sys
from collections import Counter
from pathlib import Path


def check(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def main() -> None:
    plugin_root = Path(__file__).resolve().parents[1]
    marketplace_root = Path(__file__).resolve().parents[3]
    errors: list[str] = []
    data_errors: list[str] = []

    plugin_path = plugin_root / ".codex-plugin/plugin.json"
    marketplace_path = marketplace_root / ".agents/plugins/marketplace.json"
    mcp_path = plugin_root / ".mcp.json"
    skill_path = plugin_root / "skills/qwl-stock-data/SKILL.md"
    runtime_path = plugin_root / "config/runtime.json"
    server_path = plugin_root / "scripts/qwl_data_mcp.py"
    weekly_guard_path = plugin_root / "scripts/weekly_guard.py"

    for path in (
        plugin_path,
        mcp_path,
        skill_path,
        runtime_path,
        server_path,
        weekly_guard_path,
    ):
        check(path.is_file(), f"missing file: {path}", errors)

    plugin = json.loads(plugin_path.read_text(encoding="utf-8"))
    marketplace = (
        json.loads(marketplace_path.read_text(encoding="utf-8"))
        if marketplace_path.is_file() else {}
    )
    mcp = json.loads(mcp_path.read_text(encoding="utf-8"))
    runtime = json.loads(runtime_path.read_text(encoding="utf-8"))

    check(plugin.get("name") == "qwl-stock-data", "plugin name mismatch", errors)
    check(plugin.get("skills") == "./skills/", "skills path mismatch", errors)
    check(plugin.get("mcpServers") == "./.mcp.json", "MCP path mismatch", errors)
    if marketplace:
        check(
            marketplace.get("plugins", [{}])[0].get("name")
            == "qwl-stock-data",
            "marketplace plugin mismatch",
            errors,
        )
    check(
        "qwl-stock-data" in mcp.get("mcpServers", {}),
        "MCP server entry missing",
        errors,
    )

    skill_text = skill_path.read_text(encoding="utf-8")
    frontmatter = re.match(r"\A---\n(.*?)\n---\n", skill_text, re.S)
    check(frontmatter is not None, "SKILL frontmatter missing", errors)
    if frontmatter:
        check(
            re.search(r"^name:\s*qwl-stock-data\s*$", frontmatter.group(1), re.M)
            is not None,
            "SKILL name mismatch",
            errors,
        )
        description = re.search(
            r"^description:\s*(.+)$", frontmatter.group(1), re.M
        )
        check(description is not None, "SKILL description missing", errors)
        if description:
            check(
                "Invoke" in description.group(1),
                "SKILL description lacks invocation trigger",
                errors,
            )
            check(
                len(description.group(1)) < 200,
                "SKILL description exceeds 200 characters",
                errors,
            )

    for reference in (
        "01-data-sources.md",
        "02-pit-quality.md",
        "03-operations.md",
        "04-known-gaps.md",
        "05-universe-storage.md",
        "06-weekly-publication.md",
    ):
        check(
            (skill_path.parent / "references" / reference).is_file(),
            f"missing reference: {reference}",
            errors,
        )

    py_compile.compile(str(server_path), doraise=True)
    py_compile.compile(str(weekly_guard_path), doraise=True)
    py_compile.compile(str(plugin_root / "scripts/install.py"), doraise=True)
    tree = ast.parse(server_path.read_text(encoding="utf-8"))
    tool_names = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(
            isinstance(decorator, ast.Attribute)
            and decorator.attr == "tool"
            for decorator in node.decorator_list
        ):
            tool_names.append(node.name)
    expected_tools = {
        "qwl_data_health",
        "qwl_list_fixed_60",
        "qwl_data_coverage_status",
        "qwl_refresh_market_data",
        "qwl_refresh_fundamentals",
        "qwl_refresh_financial_events",
        "qwl_refresh_gap_data",
        "qwl_refresh_historical_evidence",
        "qwl_refresh_p2_data",
        "qwl_refresh_specialized_data",
        "qwl_refresh_all_data",
        "qwl_symbol_data_status",
        "qwl_data_inventory",
        "qwl_validate_snapshot",
        "qwl_publish_weekly_data",
        "qwl_weekly_update",
        "qwl_weekly_revalidate",
        "qwl_weekly_resume_item_safety",
        "qwl_weekly_resume_market_safety",
        "qwl_weekly_status",
    }
    check(set(tool_names) == expected_tools, "MCP tool set mismatch", errors)

    qwl_home = Path(runtime["qwl_home"]).expanduser().resolve()
    check((qwl_home / "src/qwl").is_dir(), "QWL source missing", errors)
    check((qwl_home / ".venv/bin/python").is_file(), "QWL venv missing", errors)
    os.environ["QWL_HOME"] = str(qwl_home)
    sys.path.insert(0, str(qwl_home / "src"))
    from qwl import config  # noqa: E402

    universe = config.load_extended_universe()
    markets = Counter(stock.market.value for stock in universe.stocks)
    check(len(universe.stocks) == 60, "universe is not 60", errors)
    check(markets == Counter({"A": 45, "HK": 15}), "market split mismatch", errors)
    check(
        len({stock.symbol for stock in universe.stocks}) == 60,
        "duplicate symbols",
        errors,
    )
    spec = importlib.util.spec_from_file_location(
        "qwl_data_mcp_validation", server_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    failed_stage = module._stage(
        "synthetic", lambda: {"status": "FAILED"}
    )
    check(
        failed_stage["ok"] is False,
        "nested FAILED status does not fail its stage",
        errors,
    )
    timeout_stage = module._stage(
        "synthetic", lambda: {"summary": {"timeout": 1}}
    )
    check(
        timeout_stage["ok"] is False,
        "nested timeout count does not fail its stage",
        errors,
    )
    empty_stage = module._stage(
        "synthetic", lambda: {"status": "no_records"}
    )
    check(
        empty_stage["ok"] is True,
        "legitimate no_records status is misclassified as failure",
        errors,
    )
    weekly_gate = module._weekly_publication_gate({
        "stages": [
            {"stage": "market", "result": {"gate": {"passed": True}}},
            {"stage": "coverage", "result": {
                "scoped_directories_clean": True,
                "record_integrity_complete": True,
                "freshness_complete": False,
                "history_consistency_complete": False,
            }},
        ]
    })
    check(
        weekly_gate["passed"] is False,
        "legacy whole-refresh gate accepted incomplete current-week freshness",
        errors,
    )
    coverage_path = qwl_home / ".cache/coverage/data_coverage.json"
    check(coverage_path.is_file(), "coverage manifest missing", errors)
    coverage = (
        json.loads(coverage_path.read_text(encoding="utf-8"))
        if coverage_path.is_file()
        else {}
    )
    for key in (
        "core_dense_complete",
        "optional_extended_complete",
        "scoped_directories_clean",
        "record_integrity_complete",
        "all_domains_complete",
    ):
        check(
            coverage.get(key) is True,
            f"coverage check failed: {key}",
            data_errors,
        )
    for key in ("freshness_complete", "history_consistency_complete"):
        check(
            coverage.get(key) is True,
            f"coverage check failed: {key}",
            data_errors,
        )

    result = {
        "status": "PASS" if not errors else "FAIL",
        "plugin": plugin.get("name"),
        "version": plugin.get("version"),
        "mcp_tools": sorted(tool_names),
        "mcp_tool_count": len(tool_names),
        "universe_size": len(universe.stocks),
        "markets": dict(markets),
        "coverage_generated_at": coverage.get("generated_at"),
        "data_status": "READY" if not data_errors else "INCOMPLETE",
        "data_errors": data_errors,
        "known_irreducible_gaps": coverage.get("known_irreducible_gaps", []),
        "errors": errors,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
