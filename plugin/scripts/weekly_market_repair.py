"""Offline proof for resuming a market item after a qfq-contract repair."""
from __future__ import annotations

import hashlib
import io
import json
import tarfile
from pathlib import Path

import pandas as pd

from scripts.weekly_guard import effective_json, effective_parquet, week_window


def _canonical(value) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")


def _member_bytes(archive: tarfile.TarFile, names: set[str], suffix: str) -> bytes:
    matches = sorted(name for name in names if name.endswith(suffix))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one provider member ending {suffix}: {matches}")
    handle = archive.extractfile(matches[0])
    if handle is None:
        raise ValueError(f"provider member is not a file: {matches[0]}")
    return handle.read()


def validate_qfq_market_snapshot(*, provider_snapshot: Path, database: Path,
                                 as_of: str, securities, state_root: Path | None = None) -> dict:
    """Replay an immutable provider snapshot against the effective formal view.

    The function only reads the tar archive and formal base+published versions.
    It does not extract files, mutate the runtime, or publish any record.
    """
    from qwl.datasources.price_basis import (
        frame, prepare_standardized_append, validated_prior_chain,
    )

    week_start, week_end = week_window(as_of)
    if week_end.isoformat() != as_of:
        raise ValueError("as_of must be the weekly publication Friday")
    expected = []
    for security in securities:
        symbol = str(security.symbol)
        code, market = symbol.split(".", 1)
        stem = symbol.replace(".", "_")
        expected.append((symbol, code, market, stem))
    if len(expected) != 60 or len({item[0] for item in expected}) != 60:
        raise ValueError("market recovery is restricted to the fixed 60-stock universe")

    rows = []
    counts = {}
    with tarfile.open(provider_snapshot, mode="r:gz") as archive:
        names = set(archive.getnames())
        metas = [name for name in names if name.startswith('state/snapshots/')
                 and name.endswith('/meta.json')]
        meta = json.loads(archive.extractfile(metas[0]).read()) if len(metas) == 1 else None
        qfq_stems = {
            Path(name).stem
            for name in names
            if "/ohlcv_standardized/" in name and name.endswith(".parquet")
        }
        raw_stems = {
            Path(name).stem
            for name in names
            if "/ohlcv/" in name and name.endswith(".parquet")
        }
        expected_stems = {item[3] for item in expected}
        if qfq_stems != expected_stems or raw_stems != expected_stems:
            raise ValueError("provider snapshot does not contain exactly the fixed 60-stock OHLCV set")

        for symbol, code, market, stem in sorted(expected):
            qfq = pd.read_parquet(io.BytesIO(_member_bytes(
                archive, names, f"/ohlcv_standardized/{stem}.parquet",
            )))
            raw = pd.read_parquet(io.BytesIO(_member_bytes(
                archive, names, f"/ohlcv/{stem}.parquet",
            )))
            formal_relative = Path("state/history") / f"{stem}.parquet"
            formal = effective_parquet(
                database / formal_relative,
                database / "state/published_versions" / formal_relative,
            )
            if formal.empty:
                raise ValueError(f"missing effective formal qfq history: {symbol}")
            action_relative = Path("cache") / (
                "dividends_hk" if market == "HK" else "dividends"
            ) / f"{code}.json"
            action_document = effective_json(database, action_relative)
            parameters = dict(week_start=week_start, as_of=week_end,
                              action_records=action_document.get("records", []))
            try:
                prepared, audit = prepare_standardized_append(formal, qfq, raw, **parameters)
            except ValueError as exc:
                if (str(exc) != 'unexplained qfq overlap drift; keep historical candidate isolated'
                        or state_root is None):
                    raise
                from scripts.weekly_runner import job_dir
                prior_friday = week_start - __import__('datetime').timedelta(days=3)
                prior_job = job_dir(state_root, database, prior_friday.isoformat()) / 'job.json'
                if meta is None:
                    raise ValueError('missing or ambiguous market source metadata')
                source = meta['symbols'][symbol]['standardized_source']
                prior_chain = validated_prior_chain(
                    database, prior_job, symbol, qfq, raw,
                    week_start=week_start, source=source)
                prepared, audit = prepare_standardized_append(
                    formal, qfq, raw, **parameters, prior_chain=prior_chain)
            checked = frame(prepared)
            appended = checked.loc[
                (checked.index >= pd.Timestamp(week_start))
                & (checked.index <= pd.Timestamp(week_end))
            ]
            if appended.empty or appended.index.max().date() != week_end:
                raise ValueError(f"prepared current-week tail is incomplete: {symbol}")
            if audit.get("historical_records_published") != 0:
                raise ValueError(f"historical publication requested by repair proof: {symbol}")
            classification = audit["classification"]
            counts[classification] = counts.get(classification, 0) + 1
            rows.append({
                "symbol": symbol,
                "classification": classification,
                "formal_last_date": frame(formal).index.max().date().isoformat(),
                "incoming_qfq_last_date": frame(qfq).index.max().date().isoformat(),
                "incoming_raw_last_date": frame(raw).index.max().date().isoformat(),
                "prepared_current_week_rows": int(len(appended)),
                "audit_sha256": hashlib.sha256(_canonical(audit)).hexdigest(),
                "bridge": audit.get("bridge"),
            })

    result = {
        "passed": True,
        "as_of_date": as_of,
        "week_start": week_start.isoformat(),
        "fixed_universe_count": len(rows),
        "classification_counts": dict(sorted(counts.items())),
        "historical_records_published": 0,
        "symbols": rows,
    }
    result["validation_sha256"] = hashlib.sha256(_canonical(result)).hexdigest()
    return result
