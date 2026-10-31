"""Strict-PIT text features from public BYD analyst-report PDFs."""

from __future__ import annotations

import hashlib
import json
import re
import time
from io import BytesIO

import pandas as pd
import requests
from pdfminer.high_level import extract_text

from .. import paths


_TARGET = "002594.SZ"
_CACHE = paths.CACHE_DIR / "byd_research_text" / "002594.parquet"
_POSITIVE = (
    "超预期", "好于预期", "亮眼", "高增", "改善", "强劲", "新高",
    "加速", "向上", "修复", "突破", "领先", "增长", "放量",
)
_NEGATIVE = (
    "低于预期", "不及预期", "承压", "下滑", "下降", "放缓", "减速",
    "竞争加剧", "风险", "弱于", "亏损", "恶化", "压力",
)
_UNCERTAINTY = (
    "预计", "预期", "有望", "或将", "可能", "假设", "若", "不确定",
)
_RISK = ("风险提示", "风险因素", "不及预期", "波动", "竞争加剧")
_EXPECTATION_PATTERNS = {
    "above_expectation": ("超预期", "好于预期", "大超预期"),
    "meet_expectation": ("符合预期", "基本符合预期"),
    "below_expectation": ("低于预期", "不及预期", "弱于预期"),
}
_TARGET_PRICE = re.compile(
    r"目标价(?:为|至|：|:)?\s*(\d{2,3}(?:\.\d+)?)\s*元"
)
_ANNUAL_SALES = re.compile(
    r"(?:预计|预期|目标)[^。\n]{0,40}?(?:全年|年度)?"
    r"(?:销量|销售量)[^0-9]{0,12}(\d{2,3}(?:\.\d+)?)\s*万"
)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def text_features(text: str) -> dict:
    compact = _normalize_text(text)
    length = len(compact)
    denominator = max(length / 1000.0, 1.0)
    positive = sum(compact.count(term) for term in _POSITIVE)
    negative = sum(compact.count(term) for term in _NEGATIVE)
    uncertainty = sum(compact.count(term) for term in _UNCERTAINTY)
    risk = sum(compact.count(term) for term in _RISK)
    expectation = {
        name: int(any(term in compact for term in terms))
        for name, terms in _EXPECTATION_PATTERNS.items()
    }
    target_prices = [
        float(value) for value in _TARGET_PRICE.findall(compact)
        if 10.0 <= float(value) <= 1000.0
    ]
    annual_sales = [
        float(value) for value in _ANNUAL_SALES.findall(compact)
        if 100.0 <= float(value) <= 1000.0
    ]
    return {
        "text_length": length,
        "positive_density": positive / denominator,
        "negative_density": negative / denominator,
        "net_sentiment_density": (positive - negative) / denominator,
        "uncertainty_density": uncertainty / denominator,
        "risk_density": risk / denominator,
        **expectation,
        "extracted_target_price": (
            float(pd.Series(target_prices).median())
            if target_prices else None
        ),
        "extracted_annual_sales_10k": (
            float(pd.Series(annual_sales).median())
            if annual_sales else None
        ),
    }


def _source_records() -> list[dict]:
    path = paths.CACHE_DIR / "consensus" / "002594.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    output = []
    for row in payload.get("normalized_estimates", []):
        url = row.get("report_url")
        available = row.get("available_on")
        updated = row.get("updated_at")
        if (
            not url
            or not available
            or not updated
            or str(updated) < "2022-09-01"
        ):
            continue
        output.append({
            "symbol": _TARGET,
            "report_url": str(url),
            "report_title": str(row.get("report_title") or ""),
            "broker": str(row.get("broker") or ""),
            "source_published_on": str(updated)[:10],
            "available_on": str(available)[:10],
            "timestamp_precision": str(
                row.get("timestamp_precision") or "date_only"
            ),
            "fiscal_year": row.get("fiscal_year"),
            "eps": row.get("eps"),
            "rating": row.get("rating"),
        })
    deduplicated = {
        row["report_url"]: row for row in output
    }
    return sorted(
        deduplicated.values(),
        key=lambda row: (row["available_on"], row["report_url"]),
    )


def collect_byd_research_text(
    *,
    force: bool = False,
    budget_seconds: float = 900.0,
) -> dict:
    """Download public BYD report PDFs; never accepts another symbol."""
    _CACHE.parent.mkdir(parents=True, exist_ok=True)
    if _CACHE.exists() and not force:
        frame = pd.read_parquet(_CACHE)
        return {
            "status": "cached",
            "records": len(frame),
            "parsed": int((frame["parse_status"] == "parsed").sum()),
            "path": str(_CACHE),
        }
    prior = (
        pd.read_parquet(_CACHE).to_dict("records")
        if _CACHE.exists() else []
    )
    by_url = {str(row["report_url"]): row for row in prior}
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    started = time.monotonic()
    for source in _source_records():
        if source["symbol"] != _TARGET:
            raise ValueError("BYD research text collector leaked its symbol")
        if (
            source["report_url"] in by_url
            and not force
        ):
            continue
        row = dict(source)
        if time.monotonic() - started > budget_seconds:
            row["parse_status"] = "timeout"
            by_url[source["report_url"]] = row
            continue
        try:
            response = session.get(source["report_url"], timeout=45)
            response.raise_for_status()
            if not response.content.startswith(b"%PDF"):
                raise ValueError("report attachment is not PDF")
            text = extract_text(BytesIO(response.content))
            row.update({
                "parse_status": "parsed",
                "document_sha256": hashlib.sha256(
                    response.content
                ).hexdigest(),
                "document_bytes": len(response.content),
                **text_features(text),
            })
        except Exception as exc:  # noqa: BLE001
            row.update({
                "parse_status": "failed",
                "error": f"{type(exc).__name__}: {str(exc)[:160]}",
            })
        by_url[source["report_url"]] = row
    frame = pd.DataFrame(sorted(
        by_url.values(),
        key=lambda row: (
            str(row.get("available_on") or ""),
            str(row.get("report_url") or ""),
        ),
    ))
    frame.to_parquet(_CACHE, index=False)
    parsed = int((frame["parse_status"] == "parsed").sum())
    return {
        "status": "OK",
        "records": len(frame),
        "parsed": parsed,
        "failed": int((frame["parse_status"] == "failed").sum()),
        "timeout": int((frame["parse_status"] == "timeout").sum()),
        "path": str(_CACHE),
    }


def load_byd_research_text() -> pd.DataFrame:
    if not _CACHE.exists():
        collect_byd_research_text()
    frame = pd.read_parquet(_CACHE)
    frame.loc[
        pd.to_numeric(
            frame.get("extracted_annual_sales_10k"), errors="coerce"
        ) < 100.0,
        "extracted_annual_sales_10k",
    ] = None
    return frame[
        (frame["symbol"] == _TARGET)
        & (frame["parse_status"] == "parsed")
    ].copy()
