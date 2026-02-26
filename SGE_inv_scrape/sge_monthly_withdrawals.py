#!/usr/bin/env python3
"""Scrape SGE monthly report PDFs and extract Gold/Silver withdrawal data.

Source: https://en.sge.com.cn/data_MonthlyReport
Output: Excel file with monthly Gold and Silver withdrawal volumes (kg).

Note: Withdrawal data was added to the PDFs starting around November 2017.
      Earlier reports lack this field and are skipped automatically.
"""

from __future__ import annotations

import argparse
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://en.sge.com.cn"
LOAD_URL = "https://en.sge.com.cn/data_MonthlyReport_load"
ARTICLE_URL_RE = re.compile(r"/data_MonthlyReport/\d+")
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)
NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class WithdrawalRecord:
    date: str                           # "YYYY-MM"
    year: int
    month: int
    gold_withdrawal_kg: float | None
    silver_withdrawal_kg: float | None
    gold_withdrawal_ytd_kg: float | None
    silver_withdrawal_ytd_kg: float | None
    pdf_url: str


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def get_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


# ---------------------------------------------------------------------------
# Link discovery
# ---------------------------------------------------------------------------

def _pdf_links_from_soup(soup: BeautifulSoup) -> list[str]:
    """Return absolute PDF URLs found in an already-parsed HTML page."""
    urls = []
    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        if ".pdf" in href.lower():
            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)
            urls.append(href)
    return urls


def collect_pdf_links(session: requests.Session, max_pages: int = 15) -> list[str]:
    """
    Crawl the SGE monthly report listing pages and return all unique PDF URLs.

    The listing uses a `_load` endpoint with `?p=N` pagination.  Each listing
    page shows up to 10 reports.  For older items where the PDF is embedded
    inside an article page rather than linked directly, we follow the article
    link to fetch the PDF URL.
    """
    seen: set[str] = set()
    all_urls: list[str] = []

    for page in range(1, max_pages + 1):
        url = f"{LOAD_URL}?p={page}"
        print(f"[listing] page {page}: {url}")

        try:
            html = get_html(session, url)
        except Exception as exc:
            print(f"  [warn] Failed to fetch listing page {page}: {exc}")
            break

        soup = BeautifulSoup(html, "html.parser")

        # Direct PDF links
        page_pdfs = _pdf_links_from_soup(soup)

        # Article links that may embed a PDF (older reports)
        for a in soup.find_all("a", href=ARTICLE_URL_RE):
            title_span = a.find("span", class_="txt")
            title = title_span.get_text(strip=True) if title_span else ""
            # Only follow "Monthly Report of Data Highlights" articles
            if "Monthly Report" not in title and "Data Highlights" not in title:
                continue
            article_url = urljoin(BASE_URL, str(a["href"]))
            try:
                article_html = get_html(session, article_url)
                article_soup = BeautifulSoup(article_html, "html.parser")
                page_pdfs.extend(_pdf_links_from_soup(article_soup))
                time.sleep(0.3)
            except Exception as exc:
                print(f"  [warn] Failed to fetch article {article_url}: {exc}")

        # De-duplicate while preserving order
        new_count = 0
        for pdf_url in page_pdfs:
            if pdf_url not in seen:
                seen.add(pdf_url)
                all_urls.append(pdf_url)
                new_count += 1

        if new_count == 0 and not page_pdfs:
            # Page returned no PDFs at all — we've hit the end
            print(f"  [info] No PDFs on page {page}, stopping crawl.")
            break

        print(f"  Found {new_count} new PDFs (total so far: {len(all_urls)})")
        time.sleep(0.5)

    return all_urls


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _date_from_url(url: str) -> tuple[int, int] | None:
    """Try to extract (year, month) from a descriptive filename like
    'Data Highlights--January 2026.pdf'."""
    name = url.split("/")[-1]
    m = re.search(
        r"--\s*(\w+)[,\s]+(\d{4})\.pdf",
        name,
        re.IGNORECASE,
    )
    if m:
        month_str = m.group(1).lower()
        year = int(m.group(2))
        month = MONTH_MAP.get(month_str)
        if month:
            return year, month
    return None


def _date_from_text(text: str) -> tuple[int, int] | None:
    """Parse date from PDF text (e.g. 'November, 2017', 'Nov 2017', 'November2018')."""
    month_alts = "|".join(MONTH_MAP.keys())
    # Allow 0 or more separator chars between month and year to handle "November2018"
    m = re.search(
        rf"\b({month_alts})[,.\s]*(\d{{4}})\b",
        text,
        re.IGNORECASE,
    )
    if m:
        month = MONTH_MAP.get(m.group(1).lower())
        year = int(m.group(2))
        if month:
            return year, month
    return None


# ---------------------------------------------------------------------------
# PDF processing
# ---------------------------------------------------------------------------

def _parse_withdrawal_line(line: str) -> tuple[float, float] | None:
    """Extract (gold_kg, silver_kg) from a withdrawal data line."""
    nums = NUMBER_RE.findall(line)
    if len(nums) >= 2:
        try:
            return float(nums[0].replace(",", "")), float(nums[1].replace(",", ""))
        except ValueError:
            pass
    return None


def extract_withdrawal_data(
    text: str,
) -> tuple[float | None, float | None, float | None, float | None]:
    """
    Returns (gold_monthly_kg, silver_monthly_kg, gold_ytd_kg, silver_ytd_kg).
    Looks for the pattern:
        Withdrawal Volume (Present Month)   125,781.40  321,660.00
        Withdrawal Volume (Accumulative Total)  125,781.40  321,660.00
    """
    gold_m = silver_m = gold_ytd = silver_ytd = None

    # Collapse each line for easier matching
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split())

        if re.search(r"Withdrawal Volume\s*\(Present Month\)", line, re.IGNORECASE):
            parsed = _parse_withdrawal_line(line)
            if parsed:
                gold_m, silver_m = parsed

        elif re.search(r"Withdrawal Volume\s*\(Accumulative Total\)", line, re.IGNORECASE):
            parsed = _parse_withdrawal_line(line)
            if parsed:
                gold_ytd, silver_ytd = parsed

    # Fallback: sometimes values appear on the next line after the label
    if gold_m is None:
        collapsed = " ".join(text.split())
        m = re.search(
            r"Withdrawal Volume\s*\(Present Month\)\s*([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            collapsed,
            re.IGNORECASE,
        )
        if m:
            try:
                gold_m = float(m.group(1).replace(",", ""))
                silver_m = float(m.group(2).replace(",", ""))
            except ValueError:
                pass

    if gold_ytd is None:
        collapsed = " ".join(text.split())
        m = re.search(
            r"Withdrawal Volume\s*\(Accumulative Total\)\s*([\d,]+\.?\d*)\s+([\d,]+\.?\d*)",
            collapsed,
            re.IGNORECASE,
        )
        if m:
            try:
                gold_ytd = float(m.group(1).replace(",", ""))
                silver_ytd = float(m.group(2).replace(",", ""))
            except ValueError:
                pass

    return gold_m, silver_m, gold_ytd, silver_ytd


def _extract_text_by_coords(page) -> str:
    """
    Fallback text extraction for PDFs where characters are stored individually.
    Groups extracted words by Y position into lines.  Within each line, groups
    individual characters into words using X-coordinate gaps: a gap larger than
    1.5× the median intra-character gap is treated as a word boundary.
    """
    words = page.extract_words(x_tolerance=1, y_tolerance=3)
    if not words:
        return ""

    # Group by rounded Y coordinate
    rows: dict[int, list] = defaultdict(list)
    for w in words:
        y_key = round(float(w["top"]) / 2) * 2
        rows[y_key].append(w)

    lines = []
    for y_key in sorted(rows.keys()):
        row_words = sorted(rows[y_key], key=lambda w: float(w["x0"]))

        # Compute gaps between consecutive tokens
        gaps = []
        for i in range(1, len(row_words)):
            gap = float(row_words[i]["x0"]) - float(row_words[i - 1]["x1"])
            gaps.append(gap)

        # Use 1.5× the median gap as word-boundary threshold (robust to outliers)
        if gaps:
            sorted_gaps = sorted(gaps)
            median_gap = sorted_gaps[len(sorted_gaps) // 2]
            threshold = max(median_gap * 1.5, 2.0)
        else:
            threshold = 2.0

        # Build line: join chars within a word, separate words with a space
        word_buf = [row_words[0]["text"]]
        token_list: list[str] = []
        for i in range(1, len(row_words)):
            gap = float(row_words[i]["x0"]) - float(row_words[i - 1]["x1"])
            if gap > threshold:
                token_list.append("".join(word_buf))
                word_buf = [row_words[i]["text"]]
            else:
                word_buf.append(row_words[i]["text"])
        token_list.append("".join(word_buf))

        lines.append(" ".join(token_list))
    return "\n".join(lines)


def _safe_filename(url: str) -> str:
    name = url.split("/")[-1]
    # Replace characters that are problematic on most file systems
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


def process_pdf(
    session: requests.Session,
    url: str,
    pdf_dir: Path,
) -> WithdrawalRecord | None:
    pdf_path = pdf_dir / _safe_filename(url)

    # Download (skip if already cached)
    if not (pdf_path.exists() and pdf_path.stat().st_size > 0):
        try:
            r = session.get(url, timeout=60)
            r.raise_for_status()
            pdf_path.write_bytes(r.content)
        except Exception as exc:
            print(f"  [warn] Download failed {url}: {exc}")
            return None

    # Parse
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return None
            full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)

            # Some PDFs store each character as a separate object; fall back to
            # coordinate-based extraction which collapses them into proper words.
            if "Withdrawal" not in full_text:
                coord_text = "\n".join(_extract_text_by_coords(p) for p in pdf.pages)
                if "Withdrawal" in coord_text:
                    full_text = coord_text
    except Exception as exc:
        print(f"  [warn] PDF read failed {pdf_path.name}: {exc}")
        return None

    if "Withdrawal" not in full_text:
        return None  # Pre-2017 report without withdrawal data

    gold_m, silver_m, gold_ytd, silver_ytd = extract_withdrawal_data(full_text)
    if gold_m is None and silver_m is None:
        print(f"  [warn] Withdrawal section found but values not parsed: {pdf_path.name}")
        return None

    # Date — prefer URL filename (reliable for recent reports), fall back to text
    date_result = _date_from_url(url) or _date_from_text(full_text)
    if date_result is None:
        print(f"  [warn] Could not determine date: {pdf_path.name}")
        return None

    year, month = date_result
    return WithdrawalRecord(
        date=f"{year}-{month:02d}",
        year=year,
        month=month,
        gold_withdrawal_kg=gold_m,
        silver_withdrawal_kg=silver_m,
        gold_withdrawal_ytd_kg=gold_ytd,
        silver_withdrawal_ytd_kg=silver_ytd,
        pdf_url=url,
    )


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def write_excel(records: list[WithdrawalRecord], out_path: Path) -> None:
    rows = [
        {
            "Date": r.date,
            "Year": r.year,
            "Month": r.month,
            "Gold Withdrawal (kg)": r.gold_withdrawal_kg,
            "Silver Withdrawal (kg)": r.silver_withdrawal_kg,
            "Gold Withdrawal YTD (kg)": r.gold_withdrawal_ytd_kg,
            "Silver Withdrawal YTD (kg)": r.silver_withdrawal_ytd_kg,
            "PDF URL": r.pdf_url,
        }
        for r in records
    ]
    df = pd.DataFrame(rows)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Monthly Withdrawals")
        ws = writer.sheets["Monthly Withdrawals"]
        for col in ws.columns:
            max_len = max((len(str(cell.value or "")) for cell in col), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 55)

    print(f"[excel] Wrote {len(records)} rows to {out_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape SGE monthly PDFs and extract Gold/Silver withdrawal volumes."
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=15,
        help="Maximum listing pages to crawl (default 15, covers ~2016-present).",
    )
    parser.add_argument(
        "--pdf-dir",
        type=Path,
        default=Path("data/monthly_pdfs"),
        help="Directory to cache downloaded PDFs.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("data/sge_monthly_withdrawals.xlsx"),
        help="Output Excel file path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.pdf_dir.mkdir(parents=True, exist_ok=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    session = make_session()

    print("=" * 60)
    print("SGE Monthly Withdrawal Scraper")
    print("=" * 60)

    # Step 1: collect all PDF links
    print("\n--- Step 1: Collect PDF links ---")
    pdf_links = collect_pdf_links(session, max_pages=args.max_pages)
    print(f"Total PDFs discovered: {len(pdf_links)}")

    if not pdf_links:
        print("[error] No PDFs found.")
        return 1

    # Step 2: process each PDF
    print("\n--- Step 2: Download & extract withdrawal data ---")
    records: list[WithdrawalRecord] = []
    for i, url in enumerate(pdf_links, 1):
        print(f"[{i}/{len(pdf_links)}] {url[-65:]}")
        record = process_pdf(session, url, args.pdf_dir)
        if record:
            records.append(record)
            print(
                f"  -> {record.date}  Gold={record.gold_withdrawal_kg:,.2f} kg"
                f"  Silver={record.silver_withdrawal_kg:,.2f} kg"
            )
        else:
            print("  -> skipped (no withdrawal data or parse error)")
        time.sleep(0.3)

    if not records:
        print("[error] No withdrawal records extracted.")
        return 1

    # Sort chronologically
    records.sort(key=lambda r: (r.year, r.month))

    # Deduplicate: keep only the first record per (year, month)
    seen_dates: set[tuple[int, int]] = set()
    unique_records: list[WithdrawalRecord] = []
    for rec in records:
        key = (rec.year, rec.month)
        if key not in seen_dates:
            seen_dates.add(key)
            unique_records.append(rec)
        else:
            print(f"[dedup] Skipping duplicate for {rec.date}: {rec.pdf_url[-50:]}")
    records = unique_records

    # Step 3: write Excel
    print("\n--- Step 3: Write Excel ---")
    write_excel(records, args.out)

    print(f"\n[done] {len(records)} records extracted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
