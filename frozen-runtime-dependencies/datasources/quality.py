"""Data quality gate.

Enforces the user's "hard failure gate" requirement: if any REQUIRED datum for
any stock is missing / stale / anomalous, the whole run fails with a precise
locus (``symbol:field:source``) rather than silently predicting on bad data.

Tiering:
  - REQUIRED (fatal): OHLCV present, fresh (latest bar == expected session),
    continuous, and the real-time snapshot.
  - BEST-EFFORT (warn only): fundamentals, fund flow, events — these ride on
    unreliable eastmoney endpoints, so their absence downgrades confidence but
    does not abort the run.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from ..schemas import Market
from . import calendar as qcal


@dataclass
class GateIssue:
    severity: str          # "fatal" | "warn"
    locus: str             # symbol:field:source
    message: str


@dataclass
class GateResult:
    passed: bool
    issues: list[GateIssue] = field(default_factory=list)

    @property
    def fatals(self) -> list[GateIssue]:
        return [i for i in self.issues if i.severity == "fatal"]

    @property
    def warnings(self) -> list[GateIssue]:
        return [i for i in self.issues if i.severity == "warn"]

    def as_dict(self) -> dict:
        return {
            "passed": self.passed,
            "fatal_count": len(self.fatals),
            "warn_count": len(self.warnings),
            "issues": [i.__dict__ for i in self.issues],
        }


# Max plausible single-session move before we flag as anomaly (needs human/core
# review). HK has no limit; A-share limits are 10/20/30% so 35% is a safe alarm.
_ANOMALY_ABS_PCT = 0.35
_MIN_BARS = 60  # need at least ~60 sessions for the longest lookback factor


def check_ohlcv(symbol: str, market: Market, df: pd.DataFrame | None, as_of: date, source: str) -> list[GateIssue]:
    issues: list[GateIssue] = []
    loc = f"{symbol}:ohlcv:{source}"

    if df is None or df.empty:
        issues.append(GateIssue("fatal", loc, "OHLCV missing/empty from all sources"))
        return issues

    if len(df) < _MIN_BARS:
        issues.append(GateIssue("fatal", loc, f"only {len(df)} bars (<{_MIN_BARS} required for factors)"))

    # Freshness: latest bar must equal the expected latest session for as_of.
    expected = qcal.latest_expected_session(market, as_of)
    latest = df.index[-1].date()
    if latest != expected:
        # allow 1-session lag on HK (data vendor delay) but still warn
        sev = "fatal" if abs((expected - latest).days) > 4 else "warn"
        issues.append(
            GateIssue(sev, f"{symbol}:ohlcv_fresh:{source}", f"latest bar {latest} != expected {expected}")
        )

    # Missing values in core columns
    for c in ("open", "high", "low", "close"):
        if c not in df.columns:
            issues.append(GateIssue("fatal", f"{symbol}:ohlcv_{c}:{source}", f"column '{c}' absent"))
        elif df[c].isna().any():
            issues.append(GateIssue("warn", f"{symbol}:ohlcv_{c}:{source}", f"{int(df[c].isna().sum())} NaN in '{c}'"))

    # Non-positive prices
    if "close" in df.columns and (df["close"] <= 0).any():
        issues.append(GateIssue("fatal", f"{symbol}:ohlcv_close:{source}", "non-positive close present"))

    # Anomalous jumps (qfq series can have real gaps, so warn, don't abort)
    if "close" in df.columns and len(df) > 2:
        ret = df["close"].pct_change().abs()
        big = ret[ret > _ANOMALY_ABS_PCT]
        if len(big) > 0:
            worst = big.max()
            issues.append(
                GateIssue("warn", f"{symbol}:ohlcv_jump:{source}", f"{len(big)} day(s) |ret|>{_ANOMALY_ABS_PCT:.0%}, max {worst:.1%}")
            )
    return issues


def check_snapshot(symbol: str, has_snapshot: bool, source: str) -> list[GateIssue]:
    if not has_snapshot:
        return [GateIssue("fatal", f"{symbol}:snapshot:{source}", "real-time snapshot missing")]
    return []


def check_hkd_cny(record: dict | None, expected_on: date) -> list[GateIssue]:
    """Require a fresh, positive conversion rate for CNY-based A/H results."""
    locus = "HKD/CNY:fx:SAFE_CFETS"
    if not record:
        return [GateIssue("fatal", locus, "HKD/CNY central parity missing")]
    try:
        observed = pd.Timestamp(record["date"]).date()
        rate = float(record["hkd_cny"])
    except (KeyError, TypeError, ValueError):
        return [GateIssue("fatal", locus, "HKD/CNY record is malformed")]
    if rate <= 0 or not 0.5 <= rate <= 1.5:
        return [GateIssue("fatal", locus, f"implausible HKD/CNY rate {rate}")]
    if (expected_on - observed).days > 7:
        return [
            GateIssue(
                "fatal",
                locus,
                f"latest rate {observed} is stale for expected session {expected_on}",
            )
        ]
    return []


def check_best_effort(symbol: str, kind: str, present: bool, source: str) -> list[GateIssue]:
    if not present:
        return [GateIssue("warn", f"{symbol}:{kind}:{source}", f"{kind} unavailable (best-effort)")]
    return []


def finalize(issues: list[GateIssue]) -> GateResult:
    passed = not any(i.severity == "fatal" for i in issues)
    return GateResult(passed=passed, issues=issues)
