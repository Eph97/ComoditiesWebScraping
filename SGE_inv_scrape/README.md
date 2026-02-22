# SGE Silver Inventory Scraper

Scrapes weekly PDF reports from [SGE market reports](https://www.sge.com.cn/sjzx/hqzb) and extracts:

- `上周库存`
- `本周增减`
- `本周库存`

for the `白银` row in the table `上交所交收库白银库存周度数据（千克）`.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python sge_silver_inventory.py
```

Optional flags:

```bash
python sge_silver_inventory.py \
  --max-pages 20 \
  --max-reports 100 \
  --from-week 20130101 \
  --to-week 20260212 \
  --pdf-dir data/pdfs \
  --csv-out data/sge_silver_inventory.csv \
  --json-out data/sge_silver_inventory.json
```

## Output

- CSV: `data/sge_silver_inventory.csv`
- JSON: `data/sge_silver_inventory.json`

Each row includes report metadata (`title`, `pdf_url`, `week_start`, `week_end`) and silver inventory values, including `this_week_inventory_kilograms`.

## SHFE Daily Silver Open Interest

Fetch SHFE daily silver total open interest (from `kxYYYYMMDD.dat`) and write CSV:

```bash
.venv/bin/python shfe_silver_open_interest.py
```

Optional flags:

```bash
.venv/bin/python shfe_silver_open_interest.py \
  --start-date 2024-02-14 \
  --end-date 2026-02-13 \
  --workers 8 \
  --out data/shfe_silver_open_interest.csv
```

Output:

- CSV: `data/shfe_silver_open_interest.csv`
