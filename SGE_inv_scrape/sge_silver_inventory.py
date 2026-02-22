#!/usr/bin/env python3
"""Scrape SGE weekly PDF reports and extract silver inventory data."""

from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader


DEFAULT_START_URL = "https://www.sge.com.cn/sjzx/hqzb"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

NUMBER_RE = re.compile(r"[+-]?\d[\d,]*(?:\.\d+)?")
WEEK_RE = re.compile(r"(\d{8})-(\d{8})")


@dataclass
class ReportLink:
    title: str
    url: str
    source_page: str
    week_start: str | None
    week_end: str | None


@dataclass
class SilverInventoryRecord:
    title: str
    pdf_url: str
    source_page: str
    week_start: str | None
    week_end: str | None
    last_week_inventory: float
    weekly_change: float
    this_week_inventory: float
    this_week_inventory_kilograms: float
    pdf_path: str


def session_with_headers() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def parse_week_from_text(text: str) -> tuple[str | None, str | None]:
    match = WEEK_RE.search(text)
    if not match:
        return None, None
    return match.group(1), match.group(2)


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response.text


def extract_report_links(html: str, page_url: str) -> list[ReportLink]:
    soup = BeautifulSoup(html, "html.parser")
    items: list[ReportLink] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(page_url, href)
        if ".pdf" not in absolute.lower():
            continue
        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            title = Path(urlparse(absolute).path).name
        if absolute in seen_urls:
            continue
        seen_urls.add(absolute)
        week_start, week_end = parse_week_from_text(f"{title} {absolute}")
        items.append(
            ReportLink(
                title=title,
                url=absolute,
                source_page=page_url,
                week_start=week_start,
                week_end=week_end,
            )
        )

    return items


def extract_pagination_urls(html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    discovered: set[str] = set()

    def maybe_add(url_text: str) -> None:
        if not url_text:
            return
        absolute = urljoin(page_url, url_text.strip())
        parsed = urlparse(absolute)
        if "sge.com.cn" not in parsed.netloc:
            return
        if parsed.path != "/sjzx/hqzb":
            return
        if "/." in parsed.path:
            return
        if absolute.lower().endswith(".pdf"):
            return
        if parsed.query and not re.fullmatch(r"p=\d+", parsed.query):
            return
        discovered.add(absolute)

    for anchor in soup.find_all("a"):
        maybe_add(anchor.get("href") or "")

    for tag in soup.find_all(attrs={"onclick": True}):
        onclick = tag.get("onclick") or ""
        for hit in re.findall(r"""['"]([^'"]+)['"]""", onclick):
            maybe_add(hit)
        for hit in re.findall(r"/sjzx/hqzb[^'\" )]*", onclick):
            maybe_add(hit)

    for text_hit in re.findall(r"/sjzx/hqzb[^'\"<>\s]*", html):
        maybe_add(text_hit)

    total_page_match = re.search(r"totalPage\s*=\s*(\d+)", html)
    goto_base_match = re.search(r"gotoPage\('([^']*?/sjzx/hqzb\?p=)'", html)
    if total_page_match and goto_base_match:
        total_pages = int(total_page_match.group(1))
        base = goto_base_match.group(1)
        for i in range(1, total_pages + 1):
            maybe_add(f"{base}{i}")

    return sorted(discovered)


def crawl_report_links(
    session: requests.Session, start_url: str, max_pages: int
) -> list[ReportLink]:
    queue = [start_url]
    visited_pages: set[str] = set()
    reports_by_url: dict[str, ReportLink] = {}

    while queue and len(visited_pages) < max_pages:
        url = queue.pop(0)
        if url in visited_pages:
            continue
        visited_pages.add(url)
        print(f"[crawl] {url}")

        try:
            html = fetch_html(session, url)
        except Exception as exc:
            print(f"[warn] failed to fetch page: {url} ({exc})")
            continue

        for report in extract_report_links(html, page_url=url):
            reports_by_url.setdefault(report.url, report)

        for next_url in extract_pagination_urls(html, page_url=url):
            if next_url not in visited_pages and next_url not in queue:
                queue.append(next_url)

    reports = list(reports_by_url.values())
    reports.sort(
        key=lambda item: (item.week_end or "", item.title),
        reverse=True,
    )
    print(f"[crawl] pages_visited={len(visited_pages)} reports_found={len(reports)}")
    return reports


def filter_reports_by_date(
    reports: list[ReportLink],
    from_week: str | None,
    to_week: str | None,
) -> list[ReportLink]:
    filtered: list[ReportLink] = []
    for report in reports:
        report_date = report.week_end or report.week_start
        if not report_date:
            continue
        if from_week and report_date < from_week:
            continue
        if to_week and report_date > to_week:
            continue
        filtered.append(report)
    return filtered


def safe_filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name or "report.pdf"
    if not name.lower().endswith(".pdf"):
        name = f"{name}.pdf"
    return name


def download_pdf(
    session: requests.Session, url: str, out_dir: Path, timeout: int = 60
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / safe_filename_from_url(url)
    if target.exists() and target.stat().st_size > 0:
        return target

    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    target.write_bytes(response.content)
    return target


def normalize_number(value: str) -> float:
    return float(value.replace(",", "").replace("，", "").strip())


def extract_silver_inventory_from_text(text: str) -> tuple[float, float, float] | None:
    section_start = text.find("上交所交收库白银库存周度数据")
    search_text = text if section_start < 0 else text[section_start : section_start + 3000]

    for raw_line in search_text.splitlines():
        line = " ".join(raw_line.split())
        if not re.match(r"^白银\s+[+-]?\d", line):
            continue
        numbers = NUMBER_RE.findall(line)
        if len(numbers) < 3:
            continue
        try:
            last_week, weekly_change, this_week = (
                normalize_number(numbers[0]),
                normalize_number(numbers[1]),
                normalize_number(numbers[2]),
            )
            return last_week, weekly_change, this_week
        except ValueError:
            continue

    # Fallback for cases where line breaks split the table row.
    collapsed = " ".join(search_text.split())
    match = re.search(
        r"(?<!\S)白银\s+([+-]?\d[\d,]*(?:\.\d+)?)\s+([+-]?\d[\d,]*(?:\.\d+)?)\s+([+-]?\d[\d,]*(?:\.\d+)?)",
        collapsed,
    )
    if not match:
        return None
    return (
        normalize_number(match.group(1)),
        normalize_number(match.group(2)),
        normalize_number(match.group(3)),
    )


def extract_silver_inventory_from_pdf(pdf_path: Path) -> tuple[float, float, float] | None:
    reader = PdfReader(str(pdf_path))
    text_parts: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        text_parts.append(text)
    text = "\n".join(text_parts)
    return extract_silver_inventory_from_text(text)


def write_csv(path: Path, records: Iterable[SilverInventoryRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "title",
        "pdf_url",
        "source_page",
        "week_start",
        "week_end",
        "last_week_inventory",
        "weekly_change",
        "this_week_inventory",
        "this_week_inventory_kilograms",
        "pdf_path",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in records:
            writer.writerow(asdict(row))


def write_json(path: Path, records: Iterable[SilverInventoryRecord]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(row) for row in records]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape SGE weekly report PDFs and extract silver inventory data."
    )
    parser.add_argument("--start-url", default=DEFAULT_START_URL)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=300,
        help="Maximum listing pages to crawl from the start URL.",
    )
    parser.add_argument(
        "--max-reports",
        type=int,
        default=0,
        help="Optional cap on number of PDFs to process (0 = no cap).",
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("data/pdfs"),
        help="Directory where PDFs are cached.",
    )
    parser.add_argument(
        "--csv-out",
        type=Path,
        default=Path("data/sge_silver_inventory.csv"),
        help="CSV output path.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=Path("data/sge_silver_inventory.json"),
        help="JSON output path.",
    )
    parser.add_argument(
        "--from-week",
        default="20130101",
        help="Include reports from this week date (YYYYMMDD), default 20130101.",
    )
    parser.add_argument(
        "--to-week",
        default=datetime.now().strftime("%Y%m%d"),
        help="Include reports up to this week date (YYYYMMDD), default today.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session = session_with_headers()

    reports = crawl_report_links(
        session=session,
        start_url=args.start_url,
        max_pages=max(args.max_pages, 1),
    )
    reports = filter_reports_by_date(
        reports,
        from_week=args.from_week,
        to_week=args.to_week,
    )
    if args.max_reports > 0:
        reports = reports[: args.max_reports]

    if not reports:
        print("[error] no report PDFs found")
        return 1

    records: list[SilverInventoryRecord] = []
    for report in reports:
        try:
            pdf_path = download_pdf(session, report.url, out_dir=args.pdf_dir)
            values = extract_silver_inventory_from_pdf(pdf_path)
            if values is None:
                print(f"[warn] silver row not found: {report.url}")
                continue
            record = SilverInventoryRecord(
                title=report.title,
                pdf_url=report.url,
                source_page=report.source_page,
                week_start=report.week_start,
                week_end=report.week_end,
                last_week_inventory=values[0],
                weekly_change=values[1],
                this_week_inventory=values[2],
                this_week_inventory_kilograms=values[2],
                pdf_path=str(pdf_path),
            )
            records.append(record)
            print(
                "[ok] "
                f"{record.week_start}-{record.week_end} "
                f"last={record.last_week_inventory} "
                f"delta={record.weekly_change} "
                f"now={record.this_week_inventory}"
            )
        except Exception as exc:
            print(f"[warn] failed to process {report.url}: {exc}")

    if not records:
        print("[error] no silver inventory records extracted")
        return 1

    write_csv(args.csv_out, records)
    write_json(args.json_out, records)
    print(f"[done] wrote {len(records)} records -> {args.csv_out} and {args.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
