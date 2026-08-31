#!/usr/bin/env python3
"""Download Banco de la Republica auction data into a pandas DataFrame.

The dashboard is rendered by Oracle Analytics, but the page provides an
official CSV export.  Using that export is faster and less brittle than
scraping the rendered grid with a browser.
"""

from __future__ import annotations

import argparse
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests


EXPORT_URL = (
    "https://suameca.banrep.gov.co/estadisticas-economicas-back/rest/"
    "estadisticaEconomicaRestService/exportarReporteExcel"
)

# This is the table selected by default at the supplied URL:
# "TES corto plazo en pesos colombianos".
DEFAULT_REPORT_PATH = (
    "/shared/Estadisticas_Banco_de_la_Republica/"
    "6_Sector_Publico_y_Deuda_Publica/5_Subastas_administradas(TES)/"
    "1_Subastas_administradas_TES_corto_plazo_en_pesos_colombianos_iqy"
)


def scrape_subastas(
    report_path: str = DEFAULT_REPORT_PATH,
    *,
    timeout: int = 120,
) -> pd.DataFrame:
    """Return the complete BanRep report as a DataFrame."""
    response = requests.get(
        EXPORT_URL,
        params={"reportPath": report_path, "formato": "csv"},
        headers={
            "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (compatible; BanRepDataDownloader/1.0)",
        },
        timeout=timeout,
    )
    response.raise_for_status()

    if not response.content:
        raise RuntimeError("BanRep returned an empty response.")

    # The export is UTF-8 with a BOM. utf-8-sig removes the BOM from the
    # first column name; thousands separators are parsed as numeric values.
    df = pd.read_csv(
        BytesIO(response.content),
        encoding="utf-8-sig",
        thousands=",",
        low_memory=False,
    )
    df.columns = df.columns.str.strip()

    if df.empty:
        raise RuntimeError("BanRep returned a CSV, but it contained no rows.")

    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download BanRep's administered TES auctions table."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("banrep_subastas_administradas.csv"),
        help="Output CSV path (default: %(default)s)",
    )
    parser.add_argument(
        "--report-path",
        default=DEFAULT_REPORT_PATH,
        help="Oracle report path, if you want a different dashboard table",
    )
    args = parser.parse_args()

    df = scrape_subastas(args.report_path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"Downloaded {len(df):,} rows and {len(df.columns):,} columns")
    print(f"Saved to: {args.output.resolve()}")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
