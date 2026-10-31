"""Trading-calendar utilities.

Defines the *week* the whole plugin reasons about, honouring the user's
decision: predict "next Friday close vs this Friday close", each market aligned
to its own trading calendar (A-share = XSHG, HK = XHKG).

Key definitions (given an ``as_of`` Sunday, e.g. 2026-08-09):
  - ``this_week_close``  : the last trading session on/before as_of (the Friday
                           that just closed, per each market's calendar).
  - ``next_week_close``  : the last trading session of the following ISO week.
  - The realized weekly return the model predicts is
        (close[next_week_close] / close[this_week_close] - 1) * 100
    computed per market on its own sessions.

We use half-open reasoning on ISO weeks so holidays (e.g. a Friday holiday)
degrade gracefully to Thursday, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache

import exchange_calendars as xcals
import pandas as pd

from ..schemas import Market

# Market -> exchange-calendars code
_CAL_CODE = {Market.A: "XSHG", Market.HK: "XHKG"}


@lru_cache(maxsize=4)
def _calendar(market: Market):
    return xcals.get_calendar(_CAL_CODE[market])


def _to_ts(d: date | str) -> pd.Timestamp:
    return pd.Timestamp(d).normalize()


@dataclass(frozen=True)
class WeekWindow:
    """Resolved week boundaries for a single market."""

    market: Market
    as_of: date
    this_week_close: date          # last session of the as_of ISO week (<= as_of)
    next_week_close: date          # last session of the following ISO week
    lookback_start: date           # start of the OHLCV pull window (for factors)

    def as_dict(self) -> dict:
        return {
            "market": self.market.value,
            "as_of": self.as_of.isoformat(),
            "this_week_close": self.this_week_close.isoformat(),
            "next_week_close": self.next_week_close.isoformat(),
            "lookback_start": self.lookback_start.isoformat(),
        }


def _iso_week_bounds(d: date) -> tuple[date, date]:
    """Return (Monday, Sunday) of the ISO week containing ``d``."""
    monday = d - timedelta(days=d.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def _last_session_in_range(market: Market, start: date, end: date) -> date | None:
    """Last trading session within [start, end] inclusive, or None."""
    cal = _calendar(market)
    sessions = cal.sessions_in_range(_to_ts(start), _to_ts(end))
    if len(sessions) == 0:
        return None
    return sessions[-1].date()


def resolve_week(market: Market, as_of: date, lookback_calendar_days: int = 420) -> WeekWindow:
    """Resolve the week window for ``market`` given an ``as_of`` date.

    ``as_of`` is normally a Sunday (analysis runs Sunday 22:30) but any date is
    accepted; we anchor on its ISO week.

    ``lookback_calendar_days`` controls how far back OHLCV is pulled for factor
    warm-up (default ~420 calendar days ≈ 60 weeks ≈ enough for 60-day momentum,
    52-week high, weekly trend). This is a calendar-day span; the actual number
    of sessions returned depends on the market.
    """
    this_monday, this_sunday = _iso_week_bounds(as_of)

    this_close = _last_session_in_range(market, this_monday, this_sunday)
    if this_close is None:
        # Whole week was a holiday (rare). Fall back to previous session.
        cal = _calendar(market)
        prev = cal.previous_session(_to_ts(this_sunday))
        this_close = prev.date()

    next_monday = this_monday + timedelta(days=7)
    next_sunday = this_sunday + timedelta(days=7)
    next_close = _last_session_in_range(market, next_monday, next_sunday)
    if next_close is None:
        next_close = next_sunday  # placeholder; realized return unavailable yet

    lookback_start = as_of - timedelta(days=lookback_calendar_days)

    return WeekWindow(
        market=market,
        as_of=as_of,
        this_week_close=this_close,
        next_week_close=next_close,
        lookback_start=lookback_start,
    )


def is_session(market: Market, d: date) -> bool:
    return _calendar(market).is_session(_to_ts(d))


def sessions_between(market: Market, start: date, end: date) -> list[date]:
    cal = _calendar(market)
    return [s.date() for s in cal.sessions_in_range(_to_ts(start), _to_ts(end))]


def latest_expected_session(market: Market, as_of: date) -> date:
    """The most recent trading session on or before ``as_of`` for the market.

    Used by the data-quality gate to assert OHLCV is not stale: the freshest bar
    we collect must equal this date.
    """
    cal = _calendar(market)
    ts = _to_ts(as_of)
    if cal.is_session(ts):
        return ts.date()
    # as_of is not a session (e.g. a Sunday): take the last session in a short
    # window ending at as_of. exchange-calendars' previous_session() requires
    # its argument to itself be a session, so we scan a range instead.
    start = as_of - timedelta(days=14)
    sessions = cal.sessions_in_range(_to_ts(start), ts)
    if len(sessions) == 0:
        # Extremely unlikely (2+ week gap); widen once.
        sessions = cal.sessions_in_range(_to_ts(as_of - timedelta(days=45)), ts)
    return sessions[-1].date()
