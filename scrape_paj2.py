import requests, pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin

GUIDELINES_URL = "https://stats.paj.gr.jp/en/guidelines"
TARGET_URL     = "https://stats.paj.gr.jp/en/pub/current_en_n2.html"
HEADERS        = {"User-Agent": "Mozilla/5.0"}

def _accept_terms(session: requests.Session, soup: BeautifulSoup | None = None) -> bool:
    if soup is None:
        r = session.get(GUIDELINES_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
    form = soup.select_one('form[action="/en/accept_guidelines"]')
    if not form:  # nothing to accept
        return False
    token = form.select_one('input[name="authenticity_token"]')['value']
    session.post(
        urljoin(GUIDELINES_URL, form['action']),
        data={"authenticity_token": token, "accept_terms": "1", "commit": "Agree"},
        headers={"Referer": GUIDELINES_URL, **HEADERS},
        timeout=30,
        allow_redirects=True,
    ).raise_for_status()
    return True

def fetch_paj_current_tables() -> list[pd.DataFrame]:
    with requests.Session() as s:
        s.headers.update(HEADERS)

        # 1) Try the target
        r = s.get(TARGET_URL, timeout=30, allow_redirects=True)
        r.raise_for_status()

        # 2) If we landed on guidelines or the accept form is present, accept and retry
        landed_on_guidelines = "/en/guidelines" in r.url
        soup = BeautifulSoup(r.text, "html.parser")
        has_accept_form = bool(soup.select_one('form[action="/en/accept_guidelines"]'))
        if landed_on_guidelines or has_accept_form:
            _accept_terms(s, soup if has_accept_form else None)
            r = s.get(TARGET_URL, timeout=30, allow_redirects=True)
            r.raise_for_status()

        # 3) Parse tables from the current page
        return pd.read_html(r.text)  # list of DataFrames

# --- example usage ---
if __name__ == "__main__":
    tables = fetch_paj_current_tables()
    print(f"Found {len(tables)} tables")
    for i, df in enumerate(tables, 1):
        print(f"\nTable {i} preview:")
        print(df.head())

crude = tables[4]
crude.columns = ['index', 'Current Week', 'Last Week', 'Change from Last Week']
crude = crude.iloc[1:]
crude.set_index('index')

prod = tables[3]
prod.columns = ['index', 'sulfur', 'Current Week', 'Last Week', 'Change from Last Week']
prod = prod.iloc[1:]
prod.set_index('index')

