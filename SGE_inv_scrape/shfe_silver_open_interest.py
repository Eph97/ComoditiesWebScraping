#!/usr/bin/env python3
"""Download SHFE daily silver total open interest and write to CSV."""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


BASE_URL = "https://www.shfe.com.cn"
KX_PATH = "/data/tradedata/future/dailydata/kx{yyyymmdd}.dat"
USER_AGENT = "Mozilla/5.0"
REFERER = "https://www.shfe.com.cn/eng/reports/StatisticalData/DailyData/"


@dataclass
class DailySilverOI:
    trading_date: str
    total_open_interest: float
    source: str


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start: date, end: date) -> list[date]:
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def to_float(value: object) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").strip()
    if not text:
        return 0.0
    return float(text)


def fetch_json_via_curl(url: str, timeout: int) -> tuple[int, str]:
    cmd = [
        "curl",
        "-sS",
        "-L",
        "--max-time",
        str(timeout),
        "-H",
        f"User-Agent: {USER_AGENT}",
        "-H",
        f"Referer: {REFERER}",
        "-w",
        "\n%{http_code}",
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        return 0, ""
    if not proc.stdout:
        return 0, ""
    body, _, code_text = proc.stdout.rpartition("\n")
    try:
        code = int(code_text.strip())
    except ValueError:
        code = 0
    return code, body


def extract_silver_total_open_interest(payload: dict) -> float | None:
    rows = payload.get("o_curinstrument") or []
    silver_rows = [
        r
        for r in rows
        if str(r.get("PRODUCTID", "")).strip().lower() == "ag_f"
    ]
    if not silver_rows:
        return None

    for row in silver_rows:
        month = str(row.get("DELIVERYMONTH", "")).strip().lower()
        if "小计" in month or month.startswith("sub"):
            return to_float(row.get("OPENINTEREST"))

    total = 0.0
    count = 0
    for row in silver_rows:
        month = str(row.get("DELIVERYMONTH", "")).strip()
        if month.isdigit():
            total += to_float(row.get("OPENINTEREST"))
            count += 1
    if count:
        return total
    return None


def process_day(trading_day: date, timeout: int) -> DailySilverOI | None:
    yyyymmdd = trading_day.strftime("%Y%m%d")
    url = BASE_URL + KX_PATH.format(yyyymmdd=yyyymmdd)
    http_code, body = fetch_json_via_curl(url, timeout=timeout)

    if http_code != 200 or not body:
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None

    oi = extract_silver_total_open_interest(payload)
    if oi is None:
        return None

    return DailySilverOI(
        trading_date=trading_day.isoformat(),
        total_open_interest=oi,
        source=url,
    )


def write_csv(path: Path, rows: list[DailySilverOI]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["trading_date", "silver_total_open_interest", "source"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "trading_date": row.trading_date,
                    "silver_total_open_interest": row.total_open_interest,
                    "source": row.source,
                }
            )


def parse_args() -> argparse.Namespace:
    today = date.today()
    default_start = today - timedelta(days=365 * 2)
    parser = argparse.ArgumentParser(
        description="Fetch SHFE daily silver total open interest to CSV."
    )
    parser.add_argument(
        "--start-date",
        default=default_start.strftime("%Y-%m-%d"),
        help="Start date (YYYY-MM-DD), default is two years ago.",
    )
    parser.add_argument(
        "--end-date",
        default=today.strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD), default is today.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel download workers.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Per-request timeout seconds.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/shfe_silver_open_interest.csv"),
        help="Output CSV file path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start > end:
        raise SystemExit("--start-date must be <= --end-date")

    days = iter_dates(start, end)
    print(f"[info] scanning {len(days)} calendar days: {start} -> {end}")

    results: list[DailySilverOI] = []
    with ThreadPoolExecutor(max_workers=max(args.workers, 1)) as pool:
        futures = {pool.submit(process_day, d, args.timeout): d for d in days}
        for idx, future in enumerate(as_completed(futures), start=1):
            row = future.result()
            if row is not None:
                results.append(row)
            if idx % 100 == 0 or idx == len(days):
                print(f"[progress] {idx}/{len(days)} checked, {len(results)} records")

    results.sort(key=lambda r: r.trading_date)
    write_csv(args.out, results)
    print(f"[done] wrote {len(results)} rows -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
