"""Market-session availability semantics for point-in-time evidence."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

import pandas as pd
from exchange_calendars.errors import DateOutOfBounds

from ..schemas import Market
from . import calendar as qcal

_MARKET_TZ = {
    Market.A: ZoneInfo("Asia/Shanghai"),
    Market.HK: ZoneInfo("Asia/Hong_Kong"),
}
_MARKET_CLOSE = {
    Market.A: time(15, 0),
    Market.HK: time(16, 15),
}
_HAS_EXPLICIT_TIME = re.compile(r"(?:T|\s)\d{1,2}:\d{2}(?::\d{2})?")
_MIDNIGHT_PLACEHOLDER = re.compile(
    r"(?:T|\s)00:00(?::00)?(?:[+-]\d{2}:\d{2}|Z)?$"
)


@lru_cache(maxsize=32768)
def _next_session(market: Market, value: date, *, strictly_after: bool) -> date:
    start = value + timedelta(days=1) if strictly_after else value
    try:
        sessions = qcal.sessions_between(market, start, start + timedelta(days=20))
    except DateOutOfBounds:
        sessions = []
        candidate = start
        while len(sessions) == 0 and candidate <= start + timedelta(days=7):
            if candidate.weekday() < 5:
                sessions.append(candidate)
            candidate += timedelta(days=1)
    if not sessions:
        raise ValueError(f"no {market.value} session after {value}")
    return sessions[0]


def publication_availability(
    value,
    market: Market,
    *,
    force_date_only: bool = False,
) -> dict | None:
    """Resolve a provider publication value into its first usable session.

    Date-only disclosures are conservatively available on the next session.
    Exact intraday timestamps are usable on the same session only when they
    occur no later than that market's regular close.
    """
    if value in (None, "", "None", "nan"):
        return None
    text = str(value).strip()
    explicit_time = bool(_HAS_EXPLICIT_TIME.search(text)) and not force_date_only
    dayfirst = bool(re.match(r"^\d{1,2}/\d{1,2}/\d{4}", text))
    try:
        parsed = pd.to_datetime(text, dayfirst=dayfirst).to_pydatetime()
    except Exception:  # noqa: BLE001
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_MARKET_TZ[market])
    else:
        parsed = parsed.astimezone(_MARKET_TZ[market])

    published_on = parsed.date()
    if not explicit_time:
        precision = "date_only"
        available_on = _next_session(market, published_on, strictly_after=True)
        source_published_at = published_on.isoformat()
    else:
        precision = "exact"
        source_published_at = parsed.isoformat()
        try:
            is_session = qcal.is_session(market, published_on)
        except DateOutOfBounds:
            is_session = published_on.weekday() < 5
        if not is_session:
            available_on = _next_session(market, published_on, strictly_after=False)
        elif parsed.time().replace(tzinfo=None) > _MARKET_CLOSE[market]:
            available_on = _next_session(market, published_on, strictly_after=True)
        else:
            available_on = published_on
    return {
        "source_published_at": source_published_at,
        "source_published_on": published_on.isoformat(),
        "timestamp_precision": precision,
        "available_on": available_on.isoformat(),
    }


def announcement_availability(record: dict, market: Market) -> dict | None:
    """Resolve the first provider timestamp present on an announcement."""
    for key in ("publish_datetime", "公告时间", "published_at", "publish_date"):
        value = record.get(key)
        if value not in (None, ""):
            # CNINFO/A-share adapters commonly serialize a date-only disclosure
            # as midnight. It is not evidence that the filing was available at
            # 00:00 and must remain unavailable until the next trading session.
            date_only_placeholder = (
                market == Market.A
                and bool(_MIDNIGHT_PLACEHOLDER.search(str(value).strip()))
            )
            return publication_availability(
                value,
                market,
                force_date_only=date_only_placeholder,
            )
    return None
