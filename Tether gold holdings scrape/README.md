# Tether quarterly gold and daily supply

Run one command to update the two-sheet Excel workbook:

```sh
python3 tether_gold_tracker.py
```

The managed workbook is `outputs/tether/tether_tracker.xlsx`:

- **Quarterly Gold**: historical gold values backing fiat-denominated Tether tokens, estimated ounces and tonnes using the screenshot prices or report valuation prices, and same-date USDT supply. Each row links to its official source PDF.
- **Daily Supply**: full daily USDT circulating supply, refreshed from DefiLlama.

The workbook is regenerated on refresh. Gold prices edited in column C are preserved on later refreshes. The prior generated Excel file is saved as `tether_tracker.xlsx.bak`. Keep personal analysis in your own workbook and import the CSVs below, rather than editing this generated file. No existing personal workbook was supplied or modified.

## Historical coverage

The November 12, 2026 backfill reviewed all **23 quarterly reports** indexed by Tether, from **March 31, 2021 through September 30, 2026**.

| Coverage | Treatment |
| --- | --- |
| Q1 2021–Q4 2022 | Gold not separately disclosed. Blank cells, not zeros. Later reports in this interval combine precious metals with bonds and funds. |
| Q1 2023–Q4 2025 | Separately reported gold USD value. Ounces and tonnes estimated using the 12 XAU Curncy prices transcribed from the supplied screenshot. |
| Q1 2026–Q3 2026 | Gold value and XAU valuation price disclosed. Ounces and tonnes calculated from those inputs. |

This gives **15 consecutive quarters of reported gold values** from Q1 2023. The 12 screenshot prices fill the previously missing historical estimates, giving 15 consecutive quarterly tonnes estimates from Q1 2023 through Q3 2026. Screenshot prices are user-supplied inputs and have not been independently verified against Bloomberg. The exact reported USD gold values are used rather than the screenshot's rounded billion-dollar values, so calculated weights may differ slightly from its displayed rounded figures.

Q3 2025 gold includes $496,632,928 of owned gold not yet received into custody at quarter end, identified in the row note. Values follow each quarter's reporting entity and consolidation scope, which have changed over time. These are reserve snapshots, not all gold across every Tether entity, and not the separate XAU₮ product.

Latest quarter, September 30, 2026: gold value $19,084,651,736 at $4,157.41/troy oz, implying 142.781 metric tonnes. Same-date USDT supply: $183,839,917,293.

Daily supply has 3,271 consecutive observations from November 29, 2017 through November 12, 2026. Coverage and values are those returned by the provider, not independently reconstructed blockchain balances. The current day's observation may change during the day.

## Routine updates

The first run backfills every indexed quarter. Later runs fetch missing quarters, changed report URLs and the latest quarter again to capture revisions. Historical rows are preserved and upserted by date without duplicates. The full supply history is pulled each time, including provider revisions, and matched to quarter-end by exact UTC date.

Older PDFs at unchanged URLs are not downloaded again during a routine run. Use `--all` to recheck them. No recurring schedule is installed.

```sh
# Normal update: both sheets
python3 tether_gold_tracker.py
# Re-download and reparse all quarterly reports
python3 tether_gold_tracker.py --all
# CSV/JSON only
python3 tether_gold_tracker.py --no-excel
# Choose a dedicated generated workbook destination
python3 tether_gold_tracker.py --excel-path '/path/to/generated_tether_data.xlsx'
# Validation tests
python3 -m unittest discover -s tests -v
```

`--excel-path` is a generated output destination, not an in-place editor for a personal workbook. Do not point it at a file with other content you want to retain. Close the generated file in Excel before refreshing it. Excel is exported to a temporary file and replaced after export succeeds.

## Connect your existing Excel workbook

Two stable CSV files support separate Excel queries:

- `data/tether_usdt_gold_holdings.csv`
- `data/tether_outstanding_supply_daily.csv`

In Excel versions with Power Query, use **Data → Get Data → From Text/CSV** to import each into a separate worksheet. After running the scraper, choose **Data → Refresh All** in your workbook. Set the date column to Date and numeric columns to numbers. This preserves your own charts, formatting, formulas and other sheets. The generated Excel workbook can also be opened directly.

## Sources and definitions

Quarterly discovery uses [Tether's transparency page data](https://tether.to/page-data/en/transparency/page-data.json), which contains the PDF links loaded by the [transparency page](https://tether.to/en/transparency/). Records are ordered by reporting date, not filenames or publication dates. The PDF date is checked against the index, including for September 2025, whose filename contains its October publication date.

The parser supports USD reserves tables, multiline rows, footnotes and PDF text without word spacing. It verifies that the precious-metals footnote identifies physical gold. Unknown layouts, ambiguous rows, date mismatches and missing same-date supply cause an error rather than guessing. Gold CSV/JSON files are not updated after such a failure.

`gold tonnes = reported gold USD / report gold price USD per troy ounce / 32150.7465686275`

Python calculates ounces and tonnes for the CSV. Excel uses live formulas in columns D and E, so editing the gold price in column C recalculates holdings when opened in Excel. These are derived quantities subject to valuation and rounding, not directly disclosed vault weights.

Supply uses [DefiLlama's public USDT endpoint](https://stablecoins.llama.fi/stablecoin/1), documented in its [free API reference](https://github.com/DefiLlama/api-docs/blob/main/llms-free.txt). The metric is `tokens[].circulating.peggedUSD`, excluding unreleased treasury inventory. This is supply at $1 peg face value, not supply multiplied by traded USDT price. Observations are UTC date snapshots, not weekly averages or necessarily the report's exact 23:59 time. No stale or future-date supply is substituted.

The supplied weekly CSV resembles a Token Terminal export. DefiLlama is an alternative provider and does not reproduce its historical values exactly. The attachment is not spliced into the refreshed series. Token Terminal's direct API requires authentication; the source used here requires no API key.

PDFs and extracted text are kept in `data/reports/`, with content hashes identifying revisions. Other supporting outputs: `data/latest.json`, `data/tether_supply_raw.json`, and `data/tether_supply_source.json` (provider, definition, retrieval timestamp, response hash).

## Setup and portability

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Everything runs in `tether_gold_tracker.py`. The only dependencies are requests (downloads), pdfplumber (PDF extraction), and openpyxl (Excel writing). No JavaScript, Node.js, or Codex runtime is required. Default output paths are relative to the project, regardless of your shell's current directory.

## Replace the screenshot prices with Bloomberg

The 12 screenshot prices (Q1 2023–Q4 2025) are in `data/gold_prices.csv`, keyed by quarter end, with the actual price observation dates retained (for example March 28, 2024 for Q1 2024). For 2026, prices still come from each report because the screenshot does not cover those quarters.

To update on your work computer, edit **column C, Gold price (USD/oz)** in Quarterly Gold. Columns D and E calculate ounces and tonnes automatically. Column K records the price date. The next Python refresh detects your changed prices and preserves them, recording the source as a workbook override. Price cells are shaded yellow. The original report price is retained separately in the CSV for provenance.

Alternatively, replace values in `data/gold_prices.csv` with Bloomberg prices and rerun the script. You can point `--gold-prices` to another CSV with the same columns. Priority is: saved workbook edits, price-input CSV, then report valuation price. To discard an existing workbook override, clear its column C cell before running the script; it will revert to the CSV or report price.

The daily supply sheet is unchanged. Excel formula results populate when Excel opens/recalculates the workbook; Python does not include an Excel calculation engine.
