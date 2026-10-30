"""Keep a growing Excel collection of the official UKMTO rolling API feed."""
from pathlib import Path

import pandas as pd
import requests

URL = "https://sccd.royalnavy.mod.uk/api/ukmto/all"
FILE = Path(__file__).with_name("ukmto_api_incidents.xlsx")

response = requests.get(URL, timeout=45, headers={
    "User-Agent": "UKMTO-Historical-Research/1.0 (public incident analysis)"
})
response.raise_for_status()
records = response.json()
if not isinstance(records, list) or not records:
    raise ValueError("API did not return a nonempty incident list; workbook unchanged")
fresh = pd.DataFrame(records)
if "sitecoreId" not in fresh or fresh.sitecoreId.isna().any() or fresh.sitecoreId.duplicated().any():
    raise ValueError("Missing or duplicate API incident IDs; workbook unchanged")
old = pd.read_excel(FILE, dtype={"sitecoreId": str}) if FILE.exists() else pd.DataFrame()
new_count = len(set(fresh.sitecoreId) - set(old.get("sitecoreId", [])))
merged = pd.concat([old, fresh], ignore_index=True).drop_duplicates("sitecoreId", keep="last")
merged = merged.sort_values("utcDateOfIncident")
temporary = FILE.with_suffix(".tmp.xlsx")
merged.to_excel(temporary, index=False)
temporary.replace(FILE)
print(f"Fetched {len(fresh)} incidents; {new_count} new; {len(merged)} saved to {FILE}")
print(f"Incident dates: {merged.utcDateOfIncident.min()} to {merged.utcDateOfIncident.max()}")
