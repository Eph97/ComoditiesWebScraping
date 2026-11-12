import unittest
import tempfile
from pathlib import Path
from datetime import date
from decimal import Decimal
from tether_gold_tracker import (parse_report, parse_supply, add_supply, select_reports,
                                 export_excel, apply_gold_prices, workbook_price_overrides, load_gold_prices)

TABLE = '''Assets at 30 September 2026
Asset Category US$
3. Precious Metals10 19,084,651,736
10 The “Precious Metals” category comprises LBMA standard physical gold bars owned by the Company valued using an XAU
price of US$ 4,157.41/Oz closing price as published by Bloomberg on 30 September 2026.
11 The “Bitcoin” category comprises Bitcoins.
'''


class ParserTests(unittest.TestCase):
    def test_report_value_price_and_units(self):
        result = parse_report(['In US$ Millions\nFinancial figures 191,451', TABLE], date(2026, 9, 30))
        self.assertEqual(result['precious_metals_usd'], '19084651736')
        self.assertEqual(result['gold_price_usd_oz'], '4157.41')
        self.assertEqual(result['gold_tonnes_est'], '142.780968')
        self.assertEqual(result['pdf_page'], 2)

    def test_index_pdf_date_mismatch(self):
        with self.assertRaises(ValueError):
            parse_report([TABLE], date(2026, 6, 30))

    def test_unknown_units_fail(self):
        with self.assertRaises(ValueError):
            parse_report([TABLE.replace('Asset Category US$', 'Asset Category USD millions')])

    def test_no_gold_confirmation_no_quantity(self):
        with self.assertRaises(ValueError):
            parse_report([TABLE.replace('physical gold bars', 'mixed precious metals')])

    def test_ambiguous_table_fails(self):
        with self.assertRaises(ValueError):
            parse_report([TABLE, TABLE])

    def test_older_multiline_table_and_missing_price(self):
        text = '''Consolidated Reserves Report as of 31 December 2023
Asset Category Amount in USD
3. Precious Metals
12
3,514,935,634
4. Bitcoins
13
2,820,162,484
12
The “Precious Metals” category comprises LBMA standard physical gold bars owned by the Group.
'''
        result = parse_report([text], date(2023, 12, 31))
        self.assertEqual(result['precious_metals_usd'], '3514935634')
        self.assertEqual(result['gold_tonnes_est'], '')
        self.assertEqual(result['disclosure_status'], 'gold value only')

    def test_missing_spaces_in_pdf_text(self):
        text = '''FinancialFiguresandReservesReportasof30September2024
AssetCategory AmountinUSD
3. PreciousMetals14 4,962,116,508
14The“PreciousMetals”categorycomprisesLBMAstandardphysicalgoldbarsownedbyTetherInternationalLimited.
'''
        self.assertEqual(parse_report([text])['precious_metals_usd'], '4962116508')

    def test_early_combined_category_is_not_gold(self):
        text = '''Consolidated Reserves Report as of 31 December 2022
Asset Category Amount in USD
2. Corporate Bonds, Funds & Precious Metals 3,444,097,599
'''
        result = parse_report([text])
        self.assertEqual(result['precious_metals_usd'], '')
        self.assertEqual(result['disclosure_status'], 'not separately disclosed')

    def test_gold_in_transit_is_explained(self):
        text = TABLE.replace('closing price', 'closing price') + '\nGold not yet received into custody.'
        self.assertIn('not yet received into custody', parse_report([text])['note'])


class RefreshTests(unittest.TestCase):
    def setUp(self):
        self.reports = [(date(2026, 9, 30), 'new.pdf'), (date(2026, 6, 30), 'june.pdf'),
                        (date(2026, 3, 31), 'march.pdf')]

    def test_first_run_bootstraps_every_report(self):
        self.assertEqual(select_reports(self.reports, {}), self.reports)

    def test_routine_run_only_rechecks_latest(self):
        existing = {str(d): {'source': url} for d, url in self.reports}
        self.assertEqual(select_reports(self.reports, existing), self.reports[:1])

    def test_missed_quarter_is_added(self):
        existing = {'2026-03-31': {'source': 'march.pdf'}}
        self.assertEqual(select_reports(self.reports, existing), self.reports[:2])

    def test_changed_source_is_refetched(self):
        existing = {str(d): {'source': url} for d, url in self.reports}
        existing['2026-03-31']['source'] = 'superseded.pdf'
        self.assertEqual(select_reports(self.reports, existing), [self.reports[0], self.reports[2]])


class ExcelTests(unittest.TestCase):
    def test_python_workbook_preserves_numeric_data_and_missing_gold(self):
        from openpyxl import load_workbook
        row = parse_report([TABLE])
        row.update(source='https://example.com/report.pdf', outstanding_tether_supply_usd='183839917293')
        early = dict(row, date='2022-12-31', precious_metals_usd='', gold_price_usd_oz='',
                     gold_troy_oz_est='', gold_tonnes_est='', pdf_page='',
                     disclosure_status='not separately disclosed')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'tracker.xlsx'
            history = {'2026-09-30': Decimal('183839917293.125')}
            export_excel([early, row], history, path)
            with path.open('rb') as handle:
                workbook = load_workbook(handle)
                self.assertEqual(workbook.sheetnames, ['Quarterly Gold', 'Daily Supply'])
                self.assertIsNone(workbook['Quarterly Gold']['B2'].value)
                self.assertEqual(workbook['Quarterly Gold']['E2'].value, '=IF(D2="","",D2/32150.7465686275)')
                self.assertEqual(workbook['Quarterly Gold']['B3'].value, 19084651736)
                self.assertEqual(workbook['Quarterly Gold']['D3'].value, '=IF(OR(B3="",C3="",C3<=0),"",B3/C3)')
                self.assertEqual(workbook['Quarterly Gold']['E3'].value, '=IF(D3="","",D3/32150.7465686275)')
                self.assertEqual(workbook['Daily Supply']['A2'].value.date(), date(2026, 9, 30))
                self.assertEqual(workbook['Daily Supply']['B2'].value, 183839917293.125)
                workbook.close()
            original = path.read_bytes()
            export_excel([early, row], history, path)
            self.assertEqual(Path(str(path) + '.bak').read_bytes(), original)


class GoldPriceTests(unittest.TestCase):
    def test_screenshot_history_calculated_from_exact_report_value(self):
        prices = load_gold_prices(Path(__file__).resolve().parents[1] / 'data/gold_prices.csv')
        self.assertEqual(len(prices), 12)
        row = dict(parse_report([TABLE]), date='2025-12-31', precious_metals_usd='17450435088',
                   gold_price_usd_oz='')
        apply_gold_prices([row], prices)
        self.assertEqual(row['gold_price_usd_oz'], '4319.37')
        self.assertEqual(row['gold_tonnes_est'], str(round(Decimal('17450435088') / Decimal('4319.37') / Decimal('32150.7465686275'), 6)))
        self.assertEqual(prices['2024-03-31'][1], '2024-03-28')

    def test_workbook_price_edit_survives_refresh(self):
        from openpyxl import load_workbook
        row = parse_report([TABLE])
        row.update(source='https://example.com/report.pdf', outstanding_tether_supply_usd='183839917293')
        apply_gold_prices([row], {})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'tracker.xlsx'
            export_excel([row], {'2026-09-30': Decimal('183839917293')}, path)
            workbook = load_workbook(path)
            workbook['Quarterly Gold']['C2'] = 4200
            workbook.save(path)
            workbook.close()
            overrides = workbook_price_overrides(path, {row['date']: row})
            apply_gold_prices([row], {}, overrides)
            self.assertEqual(row['gold_price_usd_oz'], '4200')
            self.assertEqual(row['report_gold_price_usd_oz'], '4157.41')
            apply_gold_prices([row], {})
            self.assertEqual(row['gold_price_usd_oz'], '4200')
            workbook = load_workbook(path)
            workbook['Quarterly Gold']['C2'] = None
            workbook.save(path)
            workbook.close()
            overrides = workbook_price_overrides(path, {row['date']: row})
            apply_gold_prices([row], {}, overrides)
            self.assertEqual(row['gold_price_usd_oz'], '4157.41')


class SupplyTests(unittest.TestCase):
    def payload(self):
        return {'id': '1', 'symbol': 'USDT', 'tokens': [
            {'date': 1790726400, 'circulating': {'peggedUSD': '183839917293'},
             'unreleased': {'peggedUSD': '10000000000'}}]}

    def test_supply_excludes_unreleased_and_preserves_precision(self):
        payload = self.payload()
        payload['tokens'][0]['circulating']['peggedUSD'] = '183839917293.123456'
        history = parse_supply(payload)
        self.assertEqual(history['2026-09-30'], Decimal('183839917293.123456'))

    def test_exact_date_join(self):
        rows = [{'date': '2026-09-30'}]
        add_supply(rows, parse_supply(self.payload()))
        self.assertEqual(rows[0]['outstanding_tether_supply_usd'], '183839917293')

    def test_missing_date_does_not_use_current_supply(self):
        with self.assertRaises(ValueError):
            add_supply([{'date': '2026-06-30'}], parse_supply(self.payload()))

    def test_wrong_asset_rejected(self):
        payload = self.payload()
        payload['symbol'] = 'USDC'
        with self.assertRaises(ValueError):
            parse_supply(payload)

    def test_invalid_supply_rejected(self):
        for value in ['NaN', '-1', 'Infinity']:
            payload = self.payload()
            payload['tokens'][0]['circulating']['peggedUSD'] = value
            with self.assertRaises(ValueError):
                parse_supply(payload)


if __name__ == '__main__':
    unittest.main()
