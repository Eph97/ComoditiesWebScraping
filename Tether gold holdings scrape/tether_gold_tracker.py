#!/usr/bin/env python3
"""Scrape quarterly precious-metal reserves backing Tether's fiat tokens."""
import argparse
import csv
import hashlib
import io
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urljoin

import pdfplumber
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

INDEX = 'https://tether.to/page-data/en/transparency/page-data.json'
SUPPLY_URL = 'https://stablecoins.llama.fi/stablecoin/1'
OZ_PER_TONNE = Decimal('32150.7465686275')
NUMBER = r'\d{1,3}(?:,\d{3})+(?:\.\d+)?'
PROJECT = Path(__file__).resolve().parent
DEFAULT_EXCEL = PROJECT / 'outputs' / 'tether' / 'tether_tracker.xlsx'


def make_session():
    session = requests.Session()
    session.headers['User-Agent'] = 'TetherReserveResearch/2.0'
    session.mount('https://', HTTPAdapter(max_retries=Retry(
        total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])))
    return session


def discover_reports(session):
    response = session.get(INDEX, timeout=30)
    response.raise_for_status()
    reports = []
    for node in response.json()['result']['data']['reports']['nodes']:
        date = datetime.strptime(node['reportDate'], '%B %d, %Y').date()
        if (date.month, date.day) not in ((3, 31), (6, 30), (9, 30), (12, 31)):
            continue
        reports.append((date, urljoin(INDEX, node['reportFile']['file']['url'])))
    if not reports:
        raise ValueError('No quarterly reports in the transparency index')
    return sorted(set(reports), reverse=True)


def parse_supply(payload):
    """USDT circulating face-value supply, excluding authorized treasury inventory."""
    if str(payload.get('id')) != '1' or payload.get('symbol') != 'USDT':
        raise ValueError('Supply endpoint did not identify Tether USDT')
    history = {}
    for item in payload['tokens']:
        day = datetime.fromtimestamp(int(item['date']), timezone.utc).date().isoformat()
        value = Decimal(str(item['circulating']['peggedUSD']))
        if not value.is_finite() or value <= 0:
            raise ValueError(f'Invalid outstanding supply on {day}')
        if day in history and history[day] != value:
            raise ValueError(f'Conflicting supply observations on {day}')
        history[day] = value
    if not history:
        raise ValueError('Supply endpoint returned no history')
    return history


def fetch_supply(session):
    response = session.get(SUPPLY_URL, timeout=60)
    response.raise_for_status()
    payload = json.loads(response.text, parse_float=Decimal)
    return parse_supply(payload), response.content


def add_supply(rows, history):
    for row in rows:
        if row['date'] not in history:
            raise ValueError(f"No exact-date USDT supply observation for {row['date']}; refusing to use today's supply")
        row['outstanding_tether_supply_usd'] = str(history[row['date']])


def save_supply(history, content, output):
    output.mkdir(parents=True, exist_ok=True)
    (output / 'tether_supply_raw.json').write_bytes(content)
    with (output / 'tether_outstanding_supply_daily.csv').open('w', newline='') as handle:
        writer = csv.writer(handle, lineterminator='\n')
        writer.writerow(['Date', 'Tether - Outstanding supply ($)'])
        writer.writerows((day, str(value)) for day, value in sorted(history.items()))
    metadata = dict(source=SUPPLY_URL, provider='DefiLlama', asset='USDT',
                    metric='tokens[].circulating.peggedUSD',
                    definition='Circulating USDT at USD peg face value; excludes unreleased treasury tokens. Not market capitalization.',
                    alignment='Exact same UTC date as gold reporting date; daily snapshot, not weekly average.',
                    reference_comparability='Alternative provider; not an exact reproduction of the supplied Token Terminal export.',
                    first_date=min(history), latest_date=max(history),
                    retrieved_at=datetime.now(timezone.utc).isoformat(),
                    sha256=hashlib.sha256(content).hexdigest())
    (output / 'tether_supply_source.json').write_text(json.dumps(metadata, indent=2) + '\n')


def report_date(pages):
    compact = re.sub(r'\s+', '', ''.join(pages))
    match = re.search(r'(?:Assetsat|asat|asof)(\d{1,2}[A-Za-z]+20\d{2})', compact, re.I)
    if not match:
        raise ValueError('Cannot identify PDF reporting date')
    return datetime.strptime(match[1], '%d%B%Y').date()


def parse_report(pages, expected_date=None):
    date = report_date(pages)
    if expected_date and date != expected_date:
        raise ValueError(f'Index date {expected_date} differs from PDF date {date}')
    # Prior to Q1 2023 gold was not separately disclosed. Never call a mixed asset gold.
    if date < datetime(2023, 1, 1).date():
        return dict(date=date.isoformat(), precious_metals_usd='', gold_price_usd_oz='',
                    gold_troy_oz_est='', gold_tonnes_est='', pdf_page='',
                    disclosure_status='not separately disclosed',
                    note='Gold is not separately disclosed in this report. Blank means unavailable, not zero.')
    matches = []
    for page_number, text in enumerate(pages, 1):
        compact = re.sub(r'\s+', '', text)
        if not re.search(r'AssetCategory(?:US\$|AmountinUSD)(?![A-Za-z])', compact, re.I):
            continue
        rows = re.findall(r'^\s*(?:\d+\.\s*)?Precious\s*Metals\d*\s+(?:\d{1,2}\s+)?(' + NUMBER + r')\s*$', text, re.M | re.I)
        if len(rows) != 1:
            raise ValueError('Expected one unambiguous Precious Metals USD amount')
        flat = ' '.join(text.split())
        gold = re.search(r'The [“\"]Precious Metals[”\"] category comprises (.*?)(?=\s+\d+ The |$)', flat, re.I)
        price = None
        if gold and re.search(r'physical gold bars', gold[1], re.I):
            prices = re.findall(r'XAU price of US\$\s*(' + NUMBER + r')\s*/\s*Oz', gold[1], re.I)
            if len(prices) == 1:
                price = Decimal(prices[0].replace(',', ''))
                if price <= 0:
                    raise ValueError('Gold valuation price must be positive')
        value = Decimal(rows[0].replace(',', ''))
        is_gold = bool(re.search(r'The[“\"]PreciousMetals[”\"]categorycomprisesLBMAstandardphysicalgoldbars', compact, re.I))
        if not is_gold:
            raise ValueError('Precious metals footnote does not confirm physical gold')
        note = ('Gold quantity derived from reported USD fair value and report-disclosed XAU price; rounded inputs may affect quantity.'
                if price else 'Reported gold fair value. Tonnes unavailable: this report does not disclose the XAU valuation price.')
        if 'notyetreceivedintocustody' in compact.lower():
            note += ' Includes USD 496,632,928 of owned gold not yet received into custody at quarter end.'
        matches.append(dict(date=date.isoformat(), precious_metals_usd=str(value),
                            gold_price_usd_oz=str(price) if price else '',
                            gold_troy_oz_est=str(round(value / price, 6)) if price else '',
                            gold_tonnes_est=str(round(value / price / OZ_PER_TONNE, 6)) if price else '',
                            pdf_page=page_number,
                            disclosure_status='gold value and valuation price' if price else 'gold value only',
                            note=note))
    if len(matches) != 1:
        raise ValueError('Expected exactly one supported reserves table; PDF layout may have changed')
    return matches[0]


def scrape(session, url, output, expected_date=None):
    response = session.get(url, timeout=60)
    response.raise_for_status()
    content = response.content
    if not content.startswith(b'%PDF'):
        raise ValueError('Report URL did not return a PDF')
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        pages = [page.extract_text() or '' for page in pdf.pages]
    row = parse_report(pages, expected_date)
    digest = hashlib.sha256(content).hexdigest()
    row.update(source=url, retrieved_at=datetime.now(timezone.utc).isoformat(), pdf_sha256=digest)
    raw = output / 'reports'
    raw.mkdir(parents=True, exist_ok=True)
    stem = f"{row['date']}_{digest[:12]}"
    (raw / f'{stem}.pdf').write_bytes(content)
    (raw / f'{stem}.txt').write_text('\n\n'.join(pages))
    return row


def select_reports(reports, existing, all_reports=False):
    if all_reports:
        return reports
    # Bootstrap automatically, fill missed releases, and recheck the latest for revisions.
    newest = max(day for day, _ in reports)
    return [(day, url) for day, url in reports
            if day.isoformat() not in existing or day == newest
            or existing[day.isoformat()].get('source') != url]


def load_gold_prices(path):
    prices = {}
    if not path.exists():
        return prices
    with path.open(newline='') as handle:
        for row in csv.DictReader(handle):
            day = datetime.fromisoformat(row['date']).date()
            price_day = datetime.fromisoformat(row['price_date']).date()
            value = Decimal(row['gold_price_usd_oz'])
            if not value.is_finite() or value <= 0 or price_day > day:
                raise ValueError(f'Invalid gold price or price date for {day}')
            if str(day) in prices:
                raise ValueError(f'Duplicate gold price for {day}')
            prices[str(day)] = (value, str(price_day), row['source'])
    return prices


def workbook_price_overrides(path, existing):
    """Keep prices edited in Excel, comparing against the last exported CSV values."""
    if not path.exists():
        return {}
    from openpyxl import load_workbook
    workbook = load_workbook(path, read_only=True, data_only=False)
    overrides = {}
    try:
        if 'Quarterly Gold' not in workbook.sheetnames:
            return overrides
        for cells in workbook['Quarterly Gold'].iter_rows(min_row=2, values_only=True):
            if not isinstance(cells[0], datetime):
                continue
            day = cells[0].date().isoformat()
            old = existing.get(day, {})
            if cells[2] in ('', None):
                if old.get('gold_price_source', '').startswith('Workbook override'):
                    overrides[day] = None  # Clearing the cell resets a saved override.
                continue
            value = Decimal(str(cells[2]))
            if not value.is_finite() or value <= 0:
                raise ValueError(f'Invalid edited Excel gold price on {day}')
            previous = old.get('gold_price_usd_oz', '')
            edited = not previous or value != Decimal(previous)
            if edited or old.get('gold_price_source', '').startswith('Workbook override'):
                price_day = cells[10] if len(cells) > 10 else None
                price_day = price_day.date().isoformat() if isinstance(price_day, datetime) else day
                if price_day > day:
                    raise ValueError(f'Edited price date is after quarter end: {day}')
                overrides[day] = (value, price_day, 'Workbook override (user/Bloomberg)')
    finally:
        workbook.close()
    return overrides


def apply_gold_prices(rows, prices, overrides=None):
    overrides = overrides or {}
    for row in rows:
        report_price = row.setdefault('report_gold_price_usd_oz', row['gold_price_usd_oz'])
        chosen = overrides.get(row['date']) or prices.get(row['date'])
        if row.get('gold_price_source', '').startswith('Workbook override') and row['date'] not in overrides:
            chosen = (Decimal(row['gold_price_usd_oz']), row['gold_price_date'], row['gold_price_source'])
        if not chosen and report_price:
            chosen = (Decimal(report_price), row['date'], 'Tether report valuation price')
        price, price_day, source = chosen or (None, '', '')
        row['gold_price_usd_oz'] = str(price) if price else ''
        row['gold_price_date'] = price_day
        row['gold_price_source'] = source
        value = row['precious_metals_usd']
        row['gold_troy_oz_est'] = str(round(Decimal(value) / price, 6)) if price and value else ''
        row['gold_tonnes_est'] = str(round(Decimal(value) / price / OZ_PER_TONNE, 6)) if price and value else ''
        note = row['note'].split(' Price input:')[0]
        if price and value:
            note = note.replace('Tonnes unavailable: this report does not disclose the XAU valuation price.',
                                'This report does not disclose its XAU valuation price.')
            note += f' Price input: {source}, observation date {price_day}. Quantity is an estimate.'
        row['note'] = note


def export_excel(rows, history, output_path):
    """Write both data sheets directly in Python; no external runtime required."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill
    from openpyxl.workbook.properties import CalcProperties
    from openpyxl.worksheet.table import Table, TableStyleInfo

    def numeric(value):
        return float(value) if value not in ('', None) else None

    workbook = Workbook()
    gold = workbook.active
    gold.title = 'Quarterly Gold'
    gold.append(['Quarter end', 'Gold value (USD)', 'Gold price (USD/oz)',
                 'Gold (troy oz, est.)', 'Gold (tonnes, est.)', 'USDT supply (USD)',
                 'Gold disclosure', 'PDF page', 'Report source', 'Notes',
                 'Gold price date', 'Gold price source'])
    for row in rows:
        gold.append([datetime.fromisoformat(row['date']),
                     numeric(row['precious_metals_usd']), numeric(row['gold_price_usd_oz']),
                     numeric(row['gold_troy_oz_est']), numeric(row['gold_tonnes_est']),
                     numeric(row['outstanding_tether_supply_usd']),
                     row.get('disclosure_status', ''), numeric(row['pdf_page']), row['source'], row['note'],
                     datetime.fromisoformat(row['gold_price_date']) if row.get('gold_price_date') else None,
                     row.get('gold_price_source', '')])
        line = gold.max_row
        gold.cell(line, 4, f'=IF(OR(B{line}="",C{line}="",C{line}<=0),"",B{line}/C{line})')
        gold.cell(line, 5, f'=IF(D{line}="","",D{line}/{OZ_PER_TONNE})')
        if row['precious_metals_usd']:
            gold.cell(line, 3).fill = PatternFill('solid', fgColor='FFF2CC')
        gold.cell(line, 11).number_format = 'yyyy-mm-dd'
    workbook.calculation = CalcProperties(calcId=0, fullCalcOnLoad=True, forceFullCalc=True, calcMode='auto')
    supply = workbook.create_sheet('Daily Supply')
    supply.append(['Date', 'Outstanding USDT (USD)'])
    for day, value in sorted(history.items()):
        supply.append([datetime.fromisoformat(day), float(value)])

    for sheet, name, widths in [
        (gold, 'QuarterlyGold', [16, 23, 23, 23, 23, 23, 35, 12, 55, 110, 18, 45]),
        (supply, 'DailySupply', [16, 29]),
    ]:
        sheet.freeze_panes = 'A2'
        sheet.sheet_view.showGridLines = False
        for cell in sheet[1]:
            cell.font = Font(name='Calibri', bold=True, color='FFFFFF')
            cell.fill = PatternFill('solid', fgColor='24364B')
        for column, width in zip(sheet.columns, widths):
            sheet.column_dimensions[column[0].column_letter].width = width
        for cells in sheet.iter_rows(min_row=2):
            cells[0].number_format = 'yyyy-mm-dd'
            for cell in cells[1:]:
                if cell.data_type == 'n':
                    cell.number_format = '#,##0.00'
        table = Table(displayName=name, ref=sheet.dimensions)
        table.tableStyleInfo = TableStyleInfo(name='TableStyleMedium2', showRowStripes=True)
        sheet.add_table(table)
    for cells in gold.iter_rows(min_row=2):
        cells[1].number_format = '#,##0'
        cells[4].number_format = '#,##0.000'
        cells[5].number_format = '#,##0'
        cells[7].number_format = '0'
        cells[10].number_format = 'yyyy-mm-dd'

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_name(output_path.stem + '.tmp.xlsx')
    try:
        workbook.save(temporary)
        if output_path.exists():
            shutil.copy2(output_path, str(output_path) + '.bak')
        temporary.replace(output_path)
    finally:
        workbook.close()
        temporary.unlink(missing_ok=True)
    print(f'Excel updated: {output_path} ({len(rows)} quarters, {len(history)} daily observations)')


def save(rows, output):
    path = output / 'tether_usdt_gold_holdings.csv'
    existing = {}
    if path.exists():
        with path.open(newline='') as handle:
            existing = {row['date']: row for row in csv.DictReader(handle)}
    existing.update({row['date']: row for row in rows})
    ordered = sorted(existing.values(), key=lambda row: row['date'])
    fields = list(rows[0])
    with path.open('w', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction='ignore', lineterminator='\n')
        writer.writeheader()
        writer.writerows(ordered)
    (output / 'latest.json').write_text(json.dumps(ordered[-1], indent=2) + '\n')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pdf-url', help='Explicit report URL; bypass discovery')
    parser.add_argument('--all', action='store_true', help='Re-download and reparse all historical quarterly reports')
    parser.add_argument('--output-dir', type=Path, default=PROJECT / 'data')
    parser.add_argument('--excel-path', type=Path, default=DEFAULT_EXCEL, help='Managed two-sheet workbook output; rebuilt on refresh')
    parser.add_argument('--no-excel', action='store_true', help='Update CSV/JSON only')
    parser.add_argument('--gold-prices', type=Path, default=PROJECT / 'data' / 'gold_prices.csv',
                        help='Quarterly gold price inputs (screenshot or Bloomberg replacement prices)')
    args = parser.parse_args()
    session = make_session()
    try:
        old_path = args.output_dir / 'tether_usdt_gold_holdings.csv'
        existing = {}
        if old_path.exists():
            with old_path.open(newline='') as handle:
                existing = {row['date']: row for row in csv.DictReader(handle)}
        overrides = workbook_price_overrides(args.excel_path.resolve(), existing) if not args.no_excel else {}
        prices = load_gold_prices(args.gold_prices)
        indexed = discover_reports(session) if not args.pdf_url else []
        reports = [(None, args.pdf_url)] if args.pdf_url else select_reports(indexed, existing, args.all)
        print(f'Fetching {len(reports)} quarterly report(s); retaining {len(existing)} existing quarter(s).', flush=True)
        rows = [scrape(session, url, args.output_dir, date) for date, url in reports]
        existing.update({row['date']: row for row in rows})
        rows = sorted(existing.values(), key=lambda row: row['date'])
        history, supply_content = fetch_supply(session)
        add_supply(rows, history)
        apply_gold_prices(rows, prices, overrides)
        if not args.no_excel:
            export_excel(rows, history, args.excel_path.resolve())
        save_supply(history, supply_content, args.output_dir)
        path = save(rows, args.output_dir)
        print(json.dumps(max(rows, key=lambda row: row['date']), indent=2))
        print(f'Saved {len(rows)} quarter(s) to {path}')
    except (requests.RequestException, ValueError, KeyError, TypeError, OSError, ImportError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
