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

REPORT_DIRECTORY = (
    "/shared/Estadisticas_Banco_de_la_Republica/"
    "6_Sector_Publico_y_Deuda_Publica/5_Subastas_administradas(TES)/"
)

# One official CSV export per tab shown along the bottom of the dashboard.
REPORT_PATHS = {
    "TES corto plazo en pesos colombianos": (
        REPORT_DIRECTORY
        + "1_Subastas_administradas_TES_corto_plazo_en_pesos_colombianos_iqy"
    ),
    "TES largo plazo en pesos colombianos": (
        REPORT_DIRECTORY
        + "1_Subastas_administradas_TES_largo_plazo_en_pesos_colombianos_iqy"
    ),
    "TES Verdes": REPORT_DIRECTORY + "1_Subastas_administradas_TES_Verdes_iqy",
    "TES en UVR": REPORT_DIRECTORY + "1_Subastas_administradas_TES_en_UVR_iqy",
    "TES de regulación de liquidez": (
        REPORT_DIRECTORY
        + "1_Subastas_administradas_TES_de_regulación_de_liquidez_iqy"
    ),
}

DEFAULT_REPORT_PATH = REPORT_PATHS["TES corto plazo en pesos colombianos"]


def scrape_subastas(
    report_path: str = DEFAULT_REPORT_PATH,
    *,
    timeout: int = 120,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """Return one complete BanRep report as a DataFrame."""
    client = session or requests
    response = client.get(
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


def scrape_all_subastas(*, timeout: int = 120) -> pd.DataFrame:
    """Download all five dashboard tabs into one combined DataFrame.

    Since the tabs do not have exactly the same columns, pandas creates the
    union of their schemas and fills non-applicable values with blanks.
    """
    frames = []
    with requests.Session() as session:
        for tab_name, report_path in REPORT_PATHS.items():
            print(f"Downloading: {tab_name}")
            tab_df = scrape_subastas(
                report_path,
                timeout=timeout,
                session=session,
            )
            tab_df.insert(0, "Dashboard tab", tab_name)
            frames.append(tab_df)

    return pd.concat(frames, ignore_index=True, sort=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download every tab in BanRep's administered TES auctions report."
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
        help="Download only this Oracle report path instead of all dashboard tabs",
    )
    args = parser.parse_args()

    if args.report_path:
        df = scrape_subastas(args.report_path)
    else:
        df = scrape_all_subastas()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    print(f"Downloaded {len(df):,} rows and {len(df.columns):,} columns")
    if "Dashboard tab" in df.columns:
        print("Rows by tab:")
        print(df["Dashboard tab"].value_counts(sort=False).to_string())
    print(f"Saved to: {args.output.resolve()}")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
