import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from datetime import date

URL = "https://en.wikipedia.org/wiki/List_of_Academy_Award_winners_and_nominees_for_Best_Picture"

def fetch_oscar_best_picture():
    print(f"Fetching data from {URL} ...")
    response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    all_data = []

    # Each wikitable on the page contains a decade’s worth of nominees
    tables = soup.select("table.wikitable")
    for table in tables:
        rows = table.select("tr")
        year = None
        for row in rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue

            # Try to detect the year column — first cell often contains it
            year_text = cells[0].get_text(strip=True)
            if year_text:
                for token in year_text.split():
                    if token.isdigit() and len(token) == 4:
                        year = int(token)
                        break

            # Find nominee and result columns
            # Usually: Year | Film | Producer(s) | Result
            if len(cells) >= 4:
                film = cells[1].get_text(" ", strip=True)
                producers = cells[2].get_text(" ", strip=True)
                result = cells[3].get_text(" ", strip=True).lower()
                winner = "won" in result or "winner" in result
            elif len(cells) == 3:
                film = c
