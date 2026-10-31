"""Fresh stdio MCP connection for scheduled jobs stuck with an older tool session."""
from __future__ import annotations
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from fastmcp import Client
from fastmcp.client.transports import StdioTransport


async def invoke(action, as_of, expected_checkpoint_sha='', quarantine_evidence='',
                 expected_provider_snapshot_sha='', expected_validation_sha=''):
    plugin = Path(__file__).resolve().parents[1]
    transport = StdioTransport(
        command=sys.executable, args=["-B", str(plugin / "scripts/qwl_data_mcp.py")],
        cwd=str(plugin), env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    async with Client(transport, timeout=43200, init_timeout=60) as client:
        async def call(name, arguments=None):
            reply = await client.call_tool(name, arguments or {})
            if isinstance(reply.data, dict):
                return reply.data
            for block in reply.content:
                if getattr(block, "type", None) == "text":
                    return json.loads(block.text)
            raise RuntimeError("MCP returned no structured result")
        health = await call("qwl_data_health")
        if action == "health":
            return health
        if not health.get("ok") or health.get("runtime_revision") != "weekly-contract.20260906":
            return {"status": "BLOCKED_HEALTH_OR_REVISION", "complete": False, "health": health}
        if action == "status":
            return await call("qwl_weekly_status", {"as_of_date": as_of})
        if action == "resume-item-safety":
            return await call("qwl_weekly_resume_item_safety", {
                "as_of_date":as_of,
                "quarantine_evidence_path":quarantine_evidence,
                "expected_checkpoint_sha":expected_checkpoint_sha,
                "authorization":"RESUME_AFTER_ITEM_QUARANTINE"})
        if action == "resume-market-safety":
            return await call("qwl_weekly_resume_market_safety", {
                "as_of_date":as_of,
                "expected_checkpoint_sha":expected_checkpoint_sha,
                "expected_provider_snapshot_sha":expected_provider_snapshot_sha,
                "expected_validation_sha":expected_validation_sha,
                "authorization":"RESUME_MARKET_AFTER_QFQ_REPAIR"})
        return await call("qwl_weekly_update", {
            "as_of_date": as_of, "publish": True,
            "authorization": "PUBLISH_CURRENT_WEEK", "include_specialized": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("health", "status", "update", "resume-item-safety", "resume-market-safety"))
    parser.add_argument("--as-of")
    parser.add_argument("--expected-checkpoint-sha", default="")
    parser.add_argument("--quarantine-evidence", default="")
    parser.add_argument("--expected-provider-snapshot-sha", default="")
    parser.add_argument("--expected-validation-sha", default="")
    args = parser.parse_args()
    if args.action != "health" and not args.as_of:
        parser.error("--as-of is required for status/update")
    if args.action == "resume-item-safety" and (not args.expected_checkpoint_sha or not args.quarantine_evidence):
        parser.error("resume-item-safety requires --expected-checkpoint-sha and --quarantine-evidence")
    if args.action == "resume-market-safety" and (not args.expected_checkpoint_sha
            or not args.expected_provider_snapshot_sha or not args.expected_validation_sha):
        parser.error("resume-market-safety requires checkpoint, provider snapshot and validation SHAs")
    print(json.dumps(asyncio.run(invoke(args.action, args.as_of,
                                       args.expected_checkpoint_sha,args.quarantine_evidence,
                                       args.expected_provider_snapshot_sha,args.expected_validation_sha)),
                     ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
