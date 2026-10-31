"""Structured PIT operating evidence from BYD's official monthly filings."""

from __future__ import annotations

import hashlib
import json
import re
import time
from datetime import date
from io import BytesIO

import requests
from pdfminer.high_level import extract_text

from .. import paths


_TARGET_SYMBOL = "002594.SZ"
_TARGET_AK_SYMBOL = "002594"
_SALES_TITLE = re.compile(r"(\d{4})年(\d{1,2})月产销快报")


def _cache_path():
    directory = paths.CACHE_DIR / "byd_operating"
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{_TARGET_AK_SYMBOL}.json"


def _number(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        return float(value.replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None


def parse_byd_sales_text(text: str) -> dict:
    """Extract only stable fields explicitly printed in BYD's sales filing."""
    compact = re.sub(r"\s+", " ", text)
    segment = re.search(
        r"-纯电动\s+(.*?)\s+-插电式",
        compact,
    )
    tokens = re.findall(
        r"-?\d[\d,]*(?:\.\d+)?%?",
        segment.group(1) if segment else "",
    )
    phev_segment = re.search(
        r"-插电式混合动力\s+(.*?)\s+-商用车",
        compact,
    )
    phev_tokens = re.findall(
        r"-?\d[\d,]*(?:\.\d+)?%?",
        phev_segment.group(1) if phev_segment else "",
    )
    if len(tokens) < 7:
        return {
            "parse_status": "failed_table",
            "table_tokens": len(tokens),
        }
    production = _number(tokens[0])
    production_last_year = _number(tokens[1])
    sales = _number(tokens[5])
    sales_last_year = _number(tokens[6])
    phev_production = _number(phev_tokens[0]) if len(phev_tokens) >= 6 else None
    phev_sales = _number(phev_tokens[5]) if len(phev_tokens) >= 6 else None
    total_width = (
        10
        if len(tokens) > 9 and str(tokens[9]).endswith("%")
        else 9
    )
    candidate_production = (
        _number(tokens[total_width])
        if len(tokens) > total_width + 5 else None
    )
    candidate_sales = (
        _number(tokens[total_width + 5])
        if len(tokens) > total_width + 5 else None
    )
    passenger_production = passenger_sales = None
    bev_production = bev_sales = None
    if (
        candidate_production is not None
        and phev_production is not None
        and production is not None
    ):
        commercial = production - candidate_production
        if (
            candidate_production >= phev_production
            and 0 <= commercial <= max(production * 0.10, 10_000)
        ):
            passenger_production = candidate_production
            bev_production = candidate_production - phev_production
        else:
            bev_production = candidate_production
            passenger_production = candidate_production + phev_production
    if (
        candidate_sales is not None
        and phev_sales is not None
        and sales is not None
    ):
        commercial = sales - candidate_sales
        if (
            candidate_sales >= phev_sales
            and 0 <= commercial <= max(sales * 0.10, 10_000)
        ):
            passenger_sales = candidate_sales
            bev_sales = candidate_sales - phev_sales
        else:
            bev_sales = candidate_sales
            passenger_sales = candidate_sales + phev_sales
    overseas_match = re.search(
        r"海外销售(?:新能源)?(?:乘用车|汽车|新能源汽车)?"
        r"(?:合计)?\s*([\d,]+)\s*辆",
        compact,
    )
    battery_match = re.search(
        r"装机总量约为\s*([\d.]+)\s*GWh",
        compact,
        re.I,
    )
    if (
        production is None
        or production_last_year is None
        or sales is None
        or sales_last_year is None
        or min(production, production_last_year, sales, sales_last_year)
        < 0
    ):
        return {
            "parse_status": "failed_values",
            "table_tokens": len(tokens),
        }
    return {
        "parse_status": "parsed",
        "table_tokens": len(tokens),
        "production": int(round(production)),
        "production_last_year": int(round(production_last_year)),
        "sales": int(round(sales)),
        "sales_last_year": int(round(sales_last_year)),
        "sales_yoy": (
            sales / sales_last_year - 1.0
            if sales_last_year > 0 else None
        ),
        "bev_production": (
            int(round(bev_production))
            if bev_production is not None else None
        ),
        "bev_sales": (
            int(round(bev_sales)) if bev_sales is not None else None
        ),
        "passenger_production": (
            int(round(passenger_production))
            if passenger_production is not None else None
        ),
        "passenger_sales": (
            int(round(passenger_sales))
            if passenger_sales is not None else None
        ),
        "phev_production": (
            int(round(phev_production))
            if phev_production is not None else None
        ),
        "phev_sales": (
            int(round(phev_sales)) if phev_sales is not None else None
        ),
        "bev_sales_share": (
            bev_sales / sales
            if bev_sales is not None and sales > 0 else None
        ),
        "phev_sales_share": (
            phev_sales / sales
            if phev_sales is not None and sales > 0 else None
        ),
        "overseas_sales": (
            int(round(_number(overseas_match.group(1))))
            if overseas_match and _number(overseas_match.group(1)) is not None
            else None
        ),
        "battery_gwh": (
            _number(battery_match.group(1))
            if battery_match else None
        ),
    }


def _announcement_id(record: dict) -> str | None:
    match = re.search(
        r"announcementId=(\d+)",
        str(record.get("url") or ""),
    )
    return match.group(1) if match else None


def _pdf_url(
    session: requests.Session,
    announcement_id: str,
    announcement_time: str,
) -> str:
    response = session.post(
        "https://www.cninfo.com.cn/new/announcement/bulletin_detail",
        params={
            "announceId": announcement_id,
            "flag": "false",
            "announceTime": announcement_time,
        },
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.cninfo.com.cn/",
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    adjunct = (
        payload.get("announcement", {}).get("adjunctUrl")
    )
    if not adjunct:
        raise ValueError("CNINFO bulletin detail has no adjunctUrl")
    return f"https://static.cninfo.com.cn/{str(adjunct).lstrip('/')}"


def _derived_fields(records: list[dict]) -> list[dict]:
    by_month = {
        record["report_month"]: record
        for record in records
        if record.get("parse_status") == "parsed"
    }
    months = sorted(by_month)
    for index, month in enumerate(months):
        record = by_month[month]
        previous = by_month.get(months[index - 1]) if index else None
        prior_year = by_month.get(
            f"{int(month[:4]) - 1:04d}-{month[5:]}"
        )
        sales = record.get("sales")
        previous_sales = previous.get("sales") if previous else None
        record["sales_mom"] = (
            float(sales) / float(previous_sales) - 1.0
            if sales and previous_sales else None
        )
        previous_yoy = previous.get("sales_yoy") if previous else None
        record["sales_yoy_acceleration"] = (
            float(record["sales_yoy"]) - float(previous_yoy)
            if record.get("sales_yoy") is not None
            and previous_yoy is not None else None
        )
        for field in ("bev_sales_share", "phev_sales_share"):
            previous_value = previous.get(field) if previous else None
            record[f"{field}_change"] = (
                float(record[field]) - float(previous_value)
                if record.get(field) is not None
                and previous_value is not None else None
            )
        bev_change = record.get("bev_sales_share_change")
        phev_change = record.get("phev_sales_share_change")
        record["powertrain_mix_shift"] = (
            float(bev_change) - float(phev_change)
            if bev_change is not None and phev_change is not None
            else None
        )
        overseas = record.get("overseas_sales")
        record["overseas_share"] = (
            float(overseas) / float(sales)
            if overseas is not None and sales else None
        )
        previous_share = (
            previous.get("overseas_share") if previous else None
        )
        record["overseas_share_change"] = (
            float(record["overseas_share"]) - float(previous_share)
            if record.get("overseas_share") is not None
            and previous_share is not None else None
        )
        battery = record.get("battery_gwh")
        prior_battery = (
            prior_year.get("battery_gwh") if prior_year else None
        )
        record["battery_yoy"] = (
            float(battery) / float(prior_battery) - 1.0
            if battery is not None
            and prior_battery not in (None, 0) else None
        )
    return records


def collect_byd_operating_history(
    *,
    force: bool = False,
    budget_seconds: float = 300.0,
) -> dict:
    """Download and parse BYD sales filings already isolated in the 60-stock cache."""
    target = _cache_path()
    if target.exists() and not force:
        payload = json.loads(target.read_text(encoding="utf-8"))
        return {
            "status": "cached",
            "records": len(payload.get("records", [])),
            "parsed": sum(
                item.get("parse_status") == "parsed"
                for item in payload.get("records", [])
            ),
            "path": str(target),
        }
    announcement_path = (
        paths.CACHE_DIR
        / "announcements"
        / f"{_TARGET_AK_SYMBOL}.json"
    )
    blob = json.loads(announcement_path.read_text(encoding="utf-8"))
    announcements = [
        record
        for record in blob.get("announcements", [])
        if _SALES_TITLE.fullmatch(str(record.get("title") or "").strip())
    ]
    session = requests.Session()
    started = time.monotonic()
    records = []
    for announcement in announcements:
        title = str(announcement["title"]).strip()
        match = _SALES_TITLE.fullmatch(title)
        announcement_id = _announcement_id(announcement)
        record = {
            "symbol": _TARGET_SYMBOL,
            "report_month": (
                f"{int(match.group(1)):04d}-{int(match.group(2)):02d}"
            ),
            "title": title,
            "announcement_id": announcement_id,
            "source_published_at": announcement.get(
                "source_published_at"
            ),
            "source_published_on": announcement.get(
                "source_published_on"
            ),
            "timestamp_precision": announcement.get(
                "timestamp_precision"
            ),
            "available_on": announcement.get("available_on"),
            "source": "CNINFO official filing",
        }
        if time.monotonic() - started > budget_seconds:
            record["parse_status"] = "timeout"
            records.append(record)
            continue
        if not announcement_id or not record["available_on"]:
            record["parse_status"] = "invalid_announcement_metadata"
            records.append(record)
            continue
        try:
            pdf_url = _pdf_url(
                session,
                announcement_id,
                str(
                    announcement.get("publish_datetime")
                    or announcement.get("publish_date")
                    or ""
                ),
            )
            response = session.get(
                pdf_url,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=45,
            )
            response.raise_for_status()
            if "pdf" not in response.headers.get(
                "Content-Type", ""
            ).lower():
                raise ValueError("CNINFO attachment is not a PDF")
            content = response.content
            parsed = parse_byd_sales_text(
                extract_text(BytesIO(content))
            )
            record.update({
                "pdf_url": pdf_url,
                "document_sha256": hashlib.sha256(content).hexdigest(),
                "document_bytes": len(content),
                **parsed,
            })
        except Exception as exc:  # noqa: BLE001
            record.update({
                "parse_status": "failed_download_or_parse",
                "error": f"{type(exc).__name__}: {str(exc)[:160]}",
            })
        records.append(record)
    records = _derived_fields(records)
    payload = {
        "schema_version": 1,
        "symbol": _TARGET_SYMBOL,
        "source": "CNINFO official monthly production and sales filings",
        "generated_on": date.today().isoformat(),
        "records": sorted(
            records,
            key=lambda item: item["report_month"],
        ),
    }
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "status": "OK",
        "records": len(records),
        "parsed": sum(
            item.get("parse_status") == "parsed"
            for item in records
        ),
        "failed": sum(
            item.get("parse_status") != "parsed"
            for item in records
        ),
        "range": (
            [records[0]["report_month"], records[-1]["report_month"]]
            if records else None
        ),
        "path": str(target),
    }


def load_byd_operating_history() -> list[dict]:
    target = _cache_path()
    if not target.exists():
        collect_byd_operating_history()
    payload = json.loads(target.read_text(encoding="utf-8"))
    records = _derived_fields(payload.get("records", []))
    return [
        item for item in records
        if item.get("parse_status") == "parsed"
        and item.get("available_on")
    ]
